package com.sparklearn.streaming.structured;

import com.sparklearn.sql.DataFrame;
import com.sparklearn.sql.QueryExecution;
import com.sparklearn.sql.Row;
import com.sparklearn.sql.SQLContext;
import com.sparklearn.sql.catalyst.plans.logical.LogicalPlan;
import com.sparklearn.sql.catalyst.plans.logical.Scan;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * 微批流式执行引擎。
 * <p>
 * 核心逻辑：将逻辑计划中的 StreamingRelation 替换为 Source 提供的实际数据，
 * 然后通过 SQL 引擎（优化器 + 物理执行器）计算结果，写入 Sink。
 * <p>
 * 参考 Spark 源码：{@code org.apache.spark.sql.execution.streaming.StreamExecution}
 */
public class StreamExecution {

    private final SQLContext sqlContext;
    private final LogicalPlan logicalPlan;
    private final Sink sink;

    /** 每个 Source 已处理到的偏移量 */
    private final Map<Source, Offset> streamProgress = new HashMap<>();

    /** 是否已停止 */
    private volatile boolean stopped = false;

    /** 已执行的批次数 */
    private int batchesExecuted = 0;

    public StreamExecution(SQLContext sqlContext, LogicalPlan logicalPlan, Sink sink) {
        this.sqlContext = Objects.requireNonNull(sqlContext, "sqlContext");
        this.logicalPlan = Objects.requireNonNull(logicalPlan, "logicalPlan");
        this.sink = Objects.requireNonNull(sink, "sink");
    }

    /**
     * 手动推进一个微批：检查数据源是否有新数据，有则执行查询并写入 Sink。
     *
     * @return 本批次是否处理了新数据
     */
    public boolean advance() {
        if (stopped) {
            return false;
        }

        // 1. 替换 StreamingRelation 为实际数据，收集新偏移量
        ReplacementResult result = replaceAndCollect(logicalPlan);
        if (!result.hasNewData()) {
            return false;
        }

        // 2. 通过 SQL 引擎优化并执行
        QueryExecution queryExecution = sqlContext.executePlan(result.plan());
        List<Row> rows = queryExecution.executed().execute().collect();

        // 3. 更新进度
        for (Map.Entry<Source, Offset> entry : result.newOffsets().entrySet()) {
            streamProgress.put(entry.getKey(), entry.getValue());
        }

        // 4. 将结果写入 Sink（即使 rows 为空也写入，保持 offset 进度）
        Offset batchEnd = result.newOffsets().values().iterator().next();
        if (rows.isEmpty()) {
            sink.addBatch(new Batch(batchEnd,
                    sqlContext.createDataFrame("stream_result", List.of(Row.of()), 1)));
        } else {
            DataFrame resultDf = sqlContext.createDataFrame("stream_result", rows, 1);
            sink.addBatch(new Batch(batchEnd, resultDf));
        }

        batchesExecuted++;
        return true;
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
            Offset prevOffset = streamProgress.get(source);
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
        List<LogicalPlan> newChildren = new java.util.ArrayList<>();
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

    public void stop() {
        stopped = true;
    }

    public int batchesExecuted() {
        return batchesExecuted;
    }

    public boolean isStopped() {
        return stopped;
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