package com.sparklearn.streaming.structured;

/**
 * 单调递增的进度标记，表示数据流中已处理到的位置。
 * 参考 Spark 源码：{@code org.apache.spark.sql.execution.streaming.Offset}
 */
public interface Offset extends Comparable<Offset> {
}