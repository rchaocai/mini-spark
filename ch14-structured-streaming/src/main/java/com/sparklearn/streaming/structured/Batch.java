package com.sparklearn.streaming.structured;

import com.sparklearn.sql.DataFrame;

import java.util.Objects;

/**
 * 一个微批数据：DataFrame + 结束 Offset。
 * 参考 Spark 源码：{@code org.apache.spark.sql.execution.streaming.Batch}
 */
public record Batch(Offset end, DataFrame data) {

    public Batch {
        Objects.requireNonNull(end, "end");
        Objects.requireNonNull(data, "data");
    }
}