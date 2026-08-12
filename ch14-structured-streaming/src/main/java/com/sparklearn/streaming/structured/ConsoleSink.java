package com.sparklearn.streaming.structured;

import java.util.Optional;

/**
 * 将每个微批的结果打印到控制台，用于调试和演示。
 * <p>
 * 后台微批线程每处理完一批数据，{@link StreamExecution} 就会调用 {@link #addBatch}，
 * 本 sink 直接把结果打印到标准输出——用户无需手动调用 {@code show()} 查看结果。
 * <p>
 * 参考 Spark 源码：{@code org.apache.spark.sql.execution.streaming.ConsoleSink}
 *（sql/core/.../execution/streaming/console.scala）。
 */
public class ConsoleSink implements Sink {

    private long lastBatchId = -1;
    private volatile Offset lastOffset;

    @Override
    public synchronized void addBatch(Batch batch) {
        lastBatchId++;
        lastOffset = batch.end();
        System.out.println("-------------------------------------------");
        System.out.println("Batch: " + lastBatchId);
        System.out.println("-------------------------------------------");
        batch.data().show();
    }

    @Override
    public synchronized Optional<Offset> currentOffset() {
        if (lastOffset == null) {
            return Optional.empty();
        }
        return Optional.of(lastOffset);
    }
}
