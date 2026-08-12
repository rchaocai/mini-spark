package com.sparklearn.streaming.structured;

/**
 * 流式查询的输出模式。
 *
 * <p>参考 Spark 源码：{@code org.apache.spark.sql.streaming.OutputMode}
 * 和 {@code org.apache.spark.sql.InternalOutputModes}。
 *
 * <p>三种模式的语义：
 * <ul>
 *   <li>{@link #Append}：只输出新行。不维护跨批次状态，每个微批独立计算后输出。</li>
 *   <li>{@link #Update}：只输出本批次中被更新的行。需要 StateStore 维护跨批次聚合状态。</li>
 *   <li>{@link #Complete}：每次输出全部聚合结果。需要 StateStore 维护跨批次聚合状态。</li>
 * </ul>
 *
 * <p>Append 模式适用于无聚合的查询（如 map/filter）；
 * Update 和 Complete 模式适用于含聚合的查询（如 groupBy().count()）。
 */
public enum OutputMode {

    /**
     * 只输出新行。
     *
     * <p>对应 Spark 的 {@code InternalOutputModes.Append}。
     * 不使用状态存储，每个微批的结果直接写入 Sink。
     */
    Append,

    /**
     * 只输出本批次中被更新的行。
     *
     * <p>对应 Spark 的 {@code InternalOutputModes.Update}。
     * 通过 StateStore 维护跨批次聚合状态，每个微批只输出有变化的分组。
     */
    Update,

    /**
     * 每次输出全部聚合结果。
     *
     * <p>对应 Spark 的 {@code InternalOutputModes.Complete}。
     * 通过 StateStore 维护跨批次聚合状态，每个微批输出所有分组的最新聚合值。
     */
    Complete
}
