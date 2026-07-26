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
     *
     * @param resultDataFrame 用户的流式查询 DataFrame（根节点包含 StreamingRelation）
     * @param sink            输出接收器
     * @return 流式执行引擎，通过 {@link StreamExecution#advance()} 手动推进
     */
    public StreamExecution startQuery(DataFrame resultDataFrame, Sink sink) {
        Objects.requireNonNull(resultDataFrame, "resultDataFrame");
        Objects.requireNonNull(sink, "sink");
        return new StreamExecution(sqlContext, resultDataFrame.logicalPlan(), sink);
    }

    public SQLContext sqlContext() {
        return sqlContext;
    }
}