package com.sparklearn.streaming.structured;

import com.sparklearn.sql.Schema;

import java.util.Optional;

/**
 * 流式数据源：提供持续到达的数据。
 * 参考 Spark 源码：{@code org.apache.spark.sql.execution.streaming.Source}
 */
public interface Source {

    /**
     * 数据源的 schema，用于构建流式查询的逻辑计划。
     */
    Schema schema();

    /**
     * 从上一个偏移量开始，获取下一个批次的数据。
     *
     * @param start 起始偏移量；空表示从头开始
     * @return 新批次数据；空表示当前没有新数据
     */
    Optional<Batch> getNextBatch(Optional<Offset> start);

    /**
     * 获取当前数据源的最新偏移量。
     */
    Offset getCurrentOffset();
}