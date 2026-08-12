package com.sparklearn.sql.streaming;

/**
 * 流式查询句柄，由 {@link DataStreamWriter#start()} 返回。
 * 用于控制查询的执行、停止与进度查看。
 * <p>
 * {@code start()} 返回时后台线程已启动，会自动循环处理微批，
 * 用户添加数据后无需手动触发执行。
 * <p>
 * 参考 Spark 源码：{@code org.apache.spark.sql.streaming.StreamingQuery}
 */
public interface StreamingQuery {

    /**
     * 阻塞当前线程，直到数据源中所有已添加的数据都被后台线程处理并写入 Sink。
     * <p>
     * 与 Spark 的 {@code processAllAvailable()} 语义一致：仅等待执行完成，不触发执行。
     * 常用于测试场景确保读取结果前数据已就绪。
     * <p>
     * 典型用法：
     * <pre>{@code
     * source.addData(...);
     * query.processAllAvailable();  // 等待后台线程处理完
     * sql.table("wordCounts").show();
     * }</pre>
     */
    void processAllAvailable();

    /** 停止查询，释放资源。 */
    void stop();

    /** 查询是否已停止。 */
    boolean isStopped();

    /** 已执行的微批次数。 */
    int batchesExecuted();
}
