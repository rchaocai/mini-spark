package com.sparklearn.streaming.structured;

import com.sparklearn.sql.Row;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * 基于内存的流式输出接收器，用于教学和测试。
 * 将所有批次的结果保存在内存中，方便验证。
 * 参考 Spark 源码：{@code org.apache.spark.sql.execution.streaming.MemorySink}
 *
 * <p>根据 {@link OutputMode} 的不同，{@link #addBatch} 的行为不同：
 * <ul>
 *   <li>Append / Update：追加新批次到列表末尾</li>
 *   <li>Complete：清空已有批次，再添加新批次（因为每次输出的是全量结果）</li>
 * </ul>
 *
 * <p>ch15 教学版目前主要使用 Append 模式，其余模式保留以对齐 API。
 */
public class MemorySink implements Sink {

    private final OutputMode outputMode;
    private final List<Batch> batches = new ArrayList<>();

    /**
     * 使用指定输出模式构造 MemorySink。
     */
    public MemorySink(OutputMode outputMode) {
        this.outputMode = outputMode;
    }

    /**
     * 默认使用 Append 模式（向后兼容）。
     */
    public MemorySink() {
        this(OutputMode.Append);
    }

    @Override
    public synchronized void addBatch(Batch batch) {
        if (outputMode == OutputMode.Complete) {
            batches.clear();
        }
        batches.add(batch);
    }

    @Override
    public synchronized Optional<Offset> currentOffset() {
        if (batches.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(batches.get(batches.size() - 1).end());
    }

    /**
     * 返回所有已写入批次的所有行。
     */
    public synchronized List<Row> allData() {
        List<Row> all = new ArrayList<>();
        for (Batch batch : batches) {
            all.addAll(batch.data().collect());
        }
        return all;
    }

    /**
     * 返回最新一批的数据。
     */
    public synchronized List<Row> latestBatchData() {
        if (batches.isEmpty()) {
            return List.of();
        }
        return batches.get(batches.size() - 1).data().collect();
    }

    /**
     * 返回已写入的批次数量。
     */
    public synchronized int batchCount() {
        return batches.size();
    }

    @Override
    public synchronized String toString() {
        StringBuilder sb = new StringBuilder("MemorySink[\n");
        for (Batch batch : batches) {
            sb.append("  batch@").append(batch.end()).append(": ");
            sb.append(batch.data().collect());
            sb.append("\n");
        }
        sb.append("]");
        return sb.toString();
    }
}