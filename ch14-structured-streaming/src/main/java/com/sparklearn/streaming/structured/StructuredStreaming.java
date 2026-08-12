package com.sparklearn.streaming.structured;

import com.sparklearn.sql.DataFrame;
import com.sparklearn.sql.SQLContext;

import java.util.Objects;

/**
 * Structured Streaming 入口，类似于 DStream 中的 StreamingContext。
 * <p>
 * 负责将 DataFrame 流式查询与 Sink 绑定，创建 StreamExecution 引擎。
 */
public class StructuredStreaming {

    private final SQLContext sqlContext;

    public StructuredStreaming(SQLContext sqlContext) {
        this.sqlContext = Objects.requireNonNull(sqlContext, "sqlContext");
    }

    /**
     * 启动一个流式查询：将 DataFrame 的结果持续写入 Sink。
     * <p>
     * 创建 StreamExecution 后立即调用 {@link StreamExecution#start()} 启动后台线程，
     * 后台线程会自动循环处理微批，无需用户手动触发。
     * 对应 Spark 中 {@code DataStreamWriter.start()} 最终调用
     * {@code StreamExecution.start()} 启动 {@code queryExecutionThread}。
     *
     * @param resultDataFrame 用户的流式查询 DataFrame（根节点包含 StreamingRelation）
     * @param sink            输出接收器
     * @param outputMode      输出模式（Append / Update / Complete）
     * @param queryName       查询名；非 null 时，每次微批后将 Sink 数据注册为同名临时视图
     * @return 流式执行引擎，后台线程已启动；通过 {@link StreamExecution#processAllAvailable()}
     *         等待数据处理完成，{@link StreamExecution#stop()} 停止
     */
    public StreamExecution startQuery(DataFrame resultDataFrame, Sink sink,
                                       OutputMode outputMode, String queryName) {
        Objects.requireNonNull(resultDataFrame, "resultDataFrame");
        Objects.requireNonNull(sink, "sink");
        Objects.requireNonNull(outputMode, "outputMode");
        StreamExecution execution = new StreamExecution(
                sqlContext, resultDataFrame.logicalPlan(), sink, outputMode, queryName);
        execution.start();
        return execution;
    }

    /**
     * 启动一个流式查询（不注册查询名视图）。
     */
    public StreamExecution startQuery(DataFrame resultDataFrame, Sink sink, OutputMode outputMode) {
        return startQuery(resultDataFrame, sink, outputMode, null);
    }

    /**
     * 启动一个流式查询（默认 Append 模式）。
     */
    public StreamExecution startQuery(DataFrame resultDataFrame, Sink sink) {
        return startQuery(resultDataFrame, sink, OutputMode.Append, null);
    }

    public SQLContext sqlContext() {
        return sqlContext;
    }
}