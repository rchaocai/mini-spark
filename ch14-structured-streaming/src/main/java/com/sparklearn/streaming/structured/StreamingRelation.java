package com.sparklearn.streaming.structured;

import com.sparklearn.sql.Schema;
import com.sparklearn.sql.catalyst.plans.logical.LogicalPlan;

import java.util.List;
import java.util.Objects;

/**
 * 将流式数据源连接到逻辑计划的叶子节点。
 * 类似于 {@link com.sparklearn.sql.catalyst.plans.logical.Scan}，但数据来自 Source 而非静态 RDD。
 * 参考 Spark 源码：{@code org.apache.spark.sql.execution.streaming.StreamingRelation}
 */
public record StreamingRelation(Source source) implements LogicalPlan {

    public StreamingRelation {
        Objects.requireNonNull(source, "source");
    }

    @Override
    public List<LogicalPlan> children() {
        return List.of();
    }

    @Override
    public LogicalPlan withNewChildren(List<LogicalPlan> children) {
        if (!children.isEmpty()) {
            throw new IllegalArgumentException("StreamingRelation cannot have children");
        }
        return this;
    }

    /**
     * 流式数据源的 schema 由 Source 提供。
     */
    @Override
    public Schema schema() {
        return source.schema();
    }

    @Override
    public String nodeName() {
        return "StreamingRelation";
    }

    @Override
    public String detailString() {
        return source.toString();
    }
}