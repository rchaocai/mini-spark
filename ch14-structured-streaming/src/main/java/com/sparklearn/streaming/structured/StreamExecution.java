package com.sparklearn.streaming.structured;

import com.sparklearn.sql.DataFrame;
import com.sparklearn.sql.Row;
import com.sparklearn.sql.SQLContext;
import com.sparklearn.sql.catalyst.plans.logical.LogicalPlan;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 微批流式执行引擎。
 * <p>
 * 核心逻辑：将逻辑计划中的 StreamingRelation 替换为 Source 提供的实际数据，
 * 然后通过 SQL 引擎（优化器 + 物理执行器）计算结果，写入 Sink。
 * <p>
 * 参考 Spark 源码：{@code org.apache.spark.sql.execution.streaming.StreamExecution}
 *
 * <p><b>偏移量持久化</b>：通过 {@link MetadataLog}（{@link LocalFileMetadataLog}）
 * 在每个微批处理前将可用偏移量写入 WAL（write-ahead log），对应 Spark 的
 * {@code offsetLog}（{@code HDFSMetadataLog[CompositeOffset]}）。重启时从 WAL
 * 恢复 {@code availableOffsets}（最新批次）和 {@code committedOffsets}（上一批次）。
 *
 * <p><b>Source commit 时机</b>：不在当前批次处理完后立即 commit，而是在
 * <b>下一批次写入 WAL 后</b>才对上一批次调用 {@code source.commit(prevOffset)}。
 * 这样即使下一批次处理失败重启，Source 仍保留上一批次的数据可供重试。
 * 对应 Spark {@code constructNextBatch} 中的 {@code prevBatchOff} 逻辑。
 *
 * <p><b>后台线程自动执行</b>：{@link #start()} 启动一个守护线程持续调用
 * {@link #advance()} 处理微批，对应 Spark 中 {@code queryExecutionThread}
 * 运行 {@code runStream()} 循环。用户添加数据后无需手动触发，后台线程会自动拉取处理。
 * {@link #processAllAvailable()} 阻塞当前线程直到所有已添加的数据处理完毕，
 * 常用于测试场景确保 {@code show()} 前结果已就绪。
 *
 * <p><b>输出模式</b>：通过 {@link OutputMode} 控制结果输出行为：
 * <ul>
 *   <li>Append：无状态，每批独立计算后输出</li>
 *   <li>Update：有状态，通过 StateStore 维护跨批次聚合状态，只输出更新的行</li>
 *   <li>Complete：有状态，通过 StateStore 维护跨批次聚合状态，输出全量结果</li>
 * </ul>
 *
 * <p>Update / Complete 模式使用 {@link IncrementalExecution} 替代普通的
 * {@code QueryExecution}，在物理计划中注入状态管理参数。
 * 对应 Spark 中 {@code StreamExecution.runBatch()} 创建 {@code IncrementalExecution}：
 * <pre>
 *   lastExecution = new IncrementalExecution(
 *       sparkSession, triggerLogicalPlan, outputMode,
 *       checkpointFile("state"), currentBatchId)
 * </pre>
 */
public class StreamExecution {

    private final SQLContext sqlContext;
    private final LogicalPlan logicalPlan;
    private final Sink sink;
    private final OutputMode outputMode;
    private final String queryName;

    /** 状态检查点路径标识 */
    private final String checkpointLocation;

    /**
     * 偏移量 WAL，持久化每个微批的可用偏移量。
     * <p>
     * 对应 Spark {@code offsetLog = new HDFSMetadataLog[CompositeOffset](sparkSession, checkpointFile("offsets"))}。
     * 第 N 条记录表示"正在处理"的偏移量，第 N-1 条表示"已持久化到 Sink"的偏移量。
     */
    private final MetadataLog<OffsetSeq> offsetLog;

    /**
     * 已提交到 Sink 的偏移量（等价于 Spark {@code committedOffsets}）。
     * <p>
     * 每次 {@code runBatch} 完成后更新为 {@code availableOffsets}。
     */
    private final Map<Source, Offset> committedOffsets = new ConcurrentHashMap<>();

    /**
     * 可用但尚未提交的偏移量（等价于 Spark {@code availableOffsets}）。
     * <p>
     * 在 {@code constructNextBatch} 阶段从 {@code source.getCurrentOffset()} 获取，
     * 写入 {@code offsetLog}，在 {@code runBatch} 完成后提交到 {@code committedOffsets}。
     */
    private final Map<Source, Offset> availableOffsets = new ConcurrentHashMap<>();

    /** 当前批次 ID */
    private long currentBatchId = 0;

    /** 是否已停止 */
    private volatile boolean stopped = false;

    /** 已执行的批次数（后台线程写，主线程读） */
    private volatile int batchesExecuted = 0;

    /** 后台执行线程，循环调用 advance() 处理微批 */
    private final Thread queryExecutionThread;

    /** 进度锁：后台线程无数据时等待，processAllAvailable 等待处理完成 */
    private final Object progressLock = new Object();

    /** 后台线程是否已启动 */
    private volatile boolean started = false;

    /**
     * 使用指定输出模式和查询名构造流式执行引擎。
     *
     * @param queryName 查询名；非 null 时，每次 advance() 后将 Sink 当前数据
     *                  注册为同名临时视图，可通过 {@code sql.table(queryName)} 读取结果
     */
    public StreamExecution(SQLContext sqlContext, LogicalPlan logicalPlan,
                           Sink sink, OutputMode outputMode, String queryName) {
        this.sqlContext = Objects.requireNonNull(sqlContext, "sqlContext");
        this.logicalPlan = Objects.requireNonNull(logicalPlan, "logicalPlan");
        this.sink = Objects.requireNonNull(sink, "sink");
        this.outputMode = Objects.requireNonNull(outputMode, "outputMode");
        this.queryName = queryName;
        this.checkpointLocation = "mini-spark-state-" + System.identityHashCode(this);

        // 初始化偏移量 WAL（对应 Spark offsetLog）
        Path offsetLogPath = Path.of(System.getProperty("java.io.tmpdir"),
                checkpointLocation, "offsets");
        this.offsetLog = new LocalFileMetadataLog<>(offsetLogPath);

        String threadName = "stream-execution-"
                + (queryName != null ? queryName : Integer.toHexString(System.identityHashCode(this)));
        this.queryExecutionThread = new Thread(this::runStream, threadName);
        this.queryExecutionThread.setDaemon(true);
    }

    /**
     * 使用指定输出模式构造流式执行引擎（不注册查询名视图）。
     */
    public StreamExecution(SQLContext sqlContext, LogicalPlan logicalPlan,
                           Sink sink, OutputMode outputMode) {
        this(sqlContext, logicalPlan, sink, outputMode, null);
    }

    /**
     * 默认使用 Append 模式（向后兼容）。
     */
    public StreamExecution(SQLContext sqlContext, LogicalPlan logicalPlan, Sink sink) {
        this(sqlContext, logicalPlan, sink, OutputMode.Append, null);
    }

    /**
     * 启动后台执行线程，自动循环处理微批。
     * <p>
     * 对应 Spark 的 {@code StreamExecution.start()} 启动 {@code queryExecutionThread}。
     * 启动后后台线程会持续调用 {@link #advance()}，无需用户手动触发。
     */
    public void start() {
        if (started) {
            return;
        }
        started = true;
        queryExecutionThread.start();
    }

    /**
     * 后台线程循环体：持续处理微批，无数据时短暂等待。
     * <p>
     * 对应 Spark 的 {@code StreamExecution.runStream()}。
     */
    private void runStream() {
        while (!stopped) {
            try {
                boolean processed = advanceInternal();
                if (processed) {
                    // 处理完一批，唤醒可能在等待的 processAllAvailable()
                    synchronized (progressLock) {
                        progressLock.notifyAll();
                    }
                } else {
                    // 无新数据，短暂等待（模拟 ProcessingTime(0) trigger）
                    synchronized (progressLock) {
                        if (!stopped) {
                            progressLock.wait(10);
                        }
                    }
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            } catch (Exception e) {
                // 后台线程异常不中断整个进程，打印后继续
                e.printStackTrace();
            }
        }
    }

    /**
     * 阻塞直到所有已添加的数据被处理并写入 Sink。
     * <p>
     * 与 Spark 的 {@code processAllAvailable()} 语义一致：等待后台线程处理完
     * 所有 Source 中当前可用的数据，常用于测试场景确保读取结果前数据已就绪。
     * <p>
     * 如果后台线程未启动（未调用 {@link #start()}），则在当前线程同步处理所有数据。
     */
    public void processAllAvailable() {
        // 后台线程未启动时，退化为同步处理（兼容旧用法）
        if (!started) {
            while (advanceInternal()) {
                // 同步循环处理所有数据
            }
            return;
        }

        // 等待后台线程处理完所有已添加的数据
        while (!stopped && !noNewData()) {
            synchronized (progressLock) {
                if (!stopped && !noNewData()) {
                    try {
                        progressLock.wait(50);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        return;
                    }
                }
            }
        }
    }

    /**
     * 手动推进一个微批：检查数据源是否有新数据，有则执行查询并写入 Sink。
     * <p>
     * 供测试代码直接调用；正常使用时由后台线程自动调用。
     *
     * @return 本批次是否处理了新数据
     */
    public boolean advance() {
        if (stopped) {
            return false;
        }
        return advanceInternal();
    }

    /**
     * 一个微批的完整处理流程：{@link #constructNextBatch()} + {@link #runBatch()}。
     * <p>
     * 对应 Spark {@code runBatches()} 循环体中的 {@code constructNextBatch() + runBatch()}。
     */
    private boolean advanceInternal() {
        // 1. 构建下一批：获取可用偏移量 → 写 WAL → commit 上一批
        if (!constructNextBatch()) {
            return false;
        }

        // 2. 执行批次：取数据 → 替换流式节点 → 执行 → 写 Sink → 更新 committedOffsets
        runBatch();

        currentBatchId++;
        return true;
    }

    /**
     * 构建下一批：获取可用偏移量，写入 WAL，commit 上一批次。
     * <p>
     * 对应 Spark {@code StreamExecution.constructNextBatch()}：
     * <ol>
     *   <li>调用 {@code source.getOffset} 获取最新偏移量，更新 {@code availableOffsets}</li>
     *   <li>如果有新数据，{@code offsetLog.add(currentBatchId, availableOffsets.toCompositeOffset(sources))}</li>
     *   <li>取出 {@code offsetLog.get(currentBatchId - 1)}（上一批次），对每个 source 调用 {@code source.commit(prevOffset)}</li>
     *   <li>{@code offsetLog.purge(currentBatchId - 1)} 清理更早的 WAL</li>
     * </ol>
     *
     * @return 是否有新数据需要处理
     */
    private boolean constructNextBatch() {
        List<Source> sources = collectSources(logicalPlan);

        // 1. 获取所有 Source 的最新偏移量，更新 availableOffsets
        for (Source source : sources) {
            Offset current = source.getCurrentOffset();
            if (current instanceof LongOffset lo && lo.offset() >= 0) {
                availableOffsets.put(source, current);
            }
        }

        // 2. 检查是否有新数据
        if (!dataAvailable(sources)) {
            return false;
        }

        // 3. 写入 WAL（处理前先持久化，对应 Spark offsetLog.add）
        OffsetSeq offsetSeq = OffsetSeq.fromStreamProgress(availableOffsets, sources);
        offsetLog.add(currentBatchId, offsetSeq);

        // 4. commit 上一批次（对应 Spark constructNextBatch 中的 prevBatchOff 逻辑）
        //    写完当前批次 WAL 后，上一批次的数据已安全持久化到 Sink，可以通知 Source 回收。
        //    注意：第一批次（currentBatchId = 0）没有上一批次，不 commit。
        if (currentBatchId > 0) {
            Optional<OffsetSeq> prevBatchOpt = offsetLog.get(currentBatchId - 1);
            if (prevBatchOpt.isPresent()) {
                Map<Source, Offset> prevOffsets = prevBatchOpt.get().toStreamProgress(sources);
                for (Map.Entry<Source, Offset> entry : prevOffsets.entrySet()) {
                    entry.getKey().commit(entry.getValue());
                }
            }
        }

        // 5. 清理更早的 WAL（对应 Spark offsetLog.purge(currentBatchId - 1)）
        offsetLog.purge(currentBatchId - 1);

        return true;
    }

    /**
     * 执行当前批次：取数据 → 替换流式节点 → 执行查询 → 写 Sink → 更新 committedOffsets。
     * <p>
     * 对应 Spark {@code StreamExecution.runBatch()}：
     * <ol>
     *   <li>用 {@code (committedOffsets, availableOffsets)} 作为 {@code (start, end)} 调用 {@code source.getBatch}</li>
     *   <li>替换 StreamingRelation 为实际数据的 Scan</li>
     *   <li>通过 IncrementalExecution 执行（注入状态管理参数）</li>
     *   <li>{@code sink.addBatch(currentBatchId, nextBatch)}</li>
     *   <li>{@code committedOffsets ++= availableOffsets}</li>
     * </ol>
     */
    private void runBatch() {
        // 1. 替换 StreamingRelation 为实际数据（用 committedOffsets 作为 start）
        ReplacementResult result = replaceAndCollect(logicalPlan);

        // 2. 通过 IncrementalExecution 执行（注入状态管理参数）
        IncrementalExecution exec = new IncrementalExecution(
                sqlContext, result.plan(), outputMode,
                checkpointLocation, batchesExecuted);
        List<Row> rows = exec.executed().execute().collect();

        // 3. 将结果写入 Sink（即使 rows 为空也写入，保持 offset 进度）
        Offset batchEnd = result.newOffsets().values().iterator().next();
        if (rows.isEmpty()) {
            sink.addBatch(new Batch(batchEnd,
                    sqlContext.createDataFrame("stream_result", List.of(), result.plan().schema(), 1)));
        } else {
            DataFrame resultDf = sqlContext.createDataFrame("stream_result", rows, result.plan().schema(), 1);
            sink.addBatch(new Batch(batchEnd, resultDf));
        }

        // 4. 如果指定了 queryName，将 Sink 当前数据注册为同名临时视图
        if (queryName != null && sink instanceof MemorySink ms) {
            List<Row> allRows = ms.allData();
            sqlContext.createDataFrame(queryName, allRows, result.plan().schema(), 1)
                    .createOrReplaceTempView(queryName);
        }

        // 5. 更新 committedOffsets（对应 Spark committedOffsets ++= availableOffsets）
        //    必须在 sink.addBatch 之后更新，保证 committedOffsets 变为"已处理"时，
        //    本批结果已经对读取方可见。
        committedOffsets.clear();
        committedOffsets.putAll(availableOffsets);

        batchesExecuted++;
    }

    /**
     * 检查是否有新数据需要处理。
     * <p>
     * 对应 Spark {@code dataAvailable}：比较 {@code availableOffsets} 与 {@code committedOffsets}，
     * 任意 Source 的可用偏移量大于已提交偏移量即返回 true。
     */
    private boolean dataAvailable(List<Source> sources) {
        for (Source source : sources) {
            Offset available = availableOffsets.get(source);
            if (available == null) {
                continue;
            }
            Offset committed = committedOffsets.get(source);
            if (committed == null || !committed.equals(available)) {
                return true;
            }
        }
        return false;
    }

    /**
     * 检查是否所有 Source 的当前数据都已处理完毕。
     * <p>
     * 对应 Spark 的 {@code noNewData()}：遍历所有 Source，
     * 比较其当前偏移量与已提交偏移量，全部一致才返回 true。
     */
    private boolean noNewData() {
        List<Source> sources = collectSources(logicalPlan);
        for (Source source : sources) {
            Offset current = source.getCurrentOffset();
            // currentOffset 为初始值（-1）表示从未添加数据，视为无新数据
            if (current instanceof LongOffset lo && lo.offset() < 0) {
                continue;
            }
            if (current == null) {
                continue;
            }
            Offset committed = committedOffsets.get(source);
            if (committed == null || !committed.equals(current)) {
                return false;
            }
        }
        return true;
    }

    /**
     * 递归收集逻辑计划中所有 StreamingRelation 引用的 Source。
     */
    private List<Source> collectSources(LogicalPlan plan) {
        List<Source> sources = new ArrayList<>();
        collectSources(plan, sources);
        return sources;
    }

    private void collectSources(LogicalPlan plan, List<Source> sources) {
        if (plan instanceof StreamingRelation sr) {
            sources.add(sr.source());
            return;
        }
        for (LogicalPlan child : plan.children()) {
            collectSources(child, sources);
        }
    }

    /**
     * 替换并收集新偏移量。
     */
    private ReplacementResult replaceAndCollect(LogicalPlan plan) {
        return replaceAndCollect(plan, new HashMap<>());
    }

    private ReplacementResult replaceAndCollect(LogicalPlan plan, Map<Source, Offset> newOffsets) {
        if (plan instanceof StreamingRelation sr) {
            Source source = sr.source();
            // 用 committedOffsets 作为 start（等价于 Spark 的 committedOffsets.get(source)）
            Offset prevOffset = committedOffsets.get(source);
            Optional<Batch> batchOpt = source.getNextBatch(
                    prevOffset != null ? Optional.of(prevOffset) : Optional.empty());

            if (batchOpt.isPresent()) {
                Batch batch = batchOpt.get();
                newOffsets.put(source, batch.end());
                return new ReplacementResult(batch.data().logicalPlan(), newOffsets, true);
            }
            // 没有新数据，保留原 StreamingRelation 节点
            return new ReplacementResult(plan, newOffsets, false);
        }

        List<LogicalPlan> children = plan.children();
        if (children.isEmpty()) {
            return new ReplacementResult(plan, newOffsets, false);
        }

        boolean hasNewData = false;
        List<LogicalPlan> newChildren = new ArrayList<>();
        for (LogicalPlan child : children) {
            ReplacementResult childResult = replaceAndCollect(child, newOffsets);
            newChildren.add(childResult.plan());
            if (childResult.hasNewData()) {
                hasNewData = true;
            }
        }

        if (newChildren.equals(children)) {
            return new ReplacementResult(plan, newOffsets, hasNewData);
        }
        return new ReplacementResult(plan.withNewChildren(newChildren), newOffsets, hasNewData);
    }

    /**
     * 停止查询，唤醒后台线程并等待其退出。
     */
    public void stop() {
        stopped = true;
        synchronized (progressLock) {
            progressLock.notifyAll();
        }
        // 等待后台线程退出（避免在主线程退出时后台线程仍在写入 Sink）
        if (started && Thread.currentThread() != queryExecutionThread) {
            try {
                queryExecutionThread.join(2000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    public int batchesExecuted() {
        return batchesExecuted;
    }

    public boolean isStopped() {
        return stopped;
    }

    public OutputMode outputMode() {
        return outputMode;
    }

    /**
     * 替换结果：新的逻辑计划 + 新偏移量 + 是否有新数据
     */
    private record ReplacementResult(
            LogicalPlan plan,
            Map<Source, Offset> newOffsets,
            boolean hasNewData) {
    }
}
