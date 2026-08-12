package com.sparklearn.streaming.structured;

import java.util.Optional;

/**
 * 流式输出接收器：接收微批计算结果。
 * 参考 Spark 源码：{@code org.apache.spark.sql.execution.streaming.Sink}
 */
public interface Sink {

    /**
     * 写入一个批次的数据。
     */
    void addBatch(Batch batch);

    /**
     * 获取当前已写入的最新偏移量。
     */
    Optional<Offset> currentOffset();
}