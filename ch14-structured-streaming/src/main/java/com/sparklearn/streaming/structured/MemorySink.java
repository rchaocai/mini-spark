package com.sparklearn.streaming.structured;

import com.sparklearn.sql.Row;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * 基于内存的流式输出接收器，用于教学和测试。
 * 将所有批次的结果保存在内存中，方便验证。
 * 参考 Spark 源码：{@code org.apache.spark.sql.execution.streaming.MemorySink}
 */
public class MemorySink implements Sink {

    private final List<Batch> batches = new ArrayList<>();

    @Override
    public synchronized void addBatch(Batch batch) {
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