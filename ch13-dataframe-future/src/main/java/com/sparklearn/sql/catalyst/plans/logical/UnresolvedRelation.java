package com.sparklearn.sql.catalyst.plans.logical;

import com.sparklearn.sql.Schema;

import java.util.List;

/**
 * 未解析的关系：解析阶段只记下表名，具体的逻辑计划和 schema 留给 Analyzer 绑定。
 *
 * <p>SQL 文本里的 {@code FROM employees} 在解析时还不知道 {@code employees} 对应
 * 哪棵逻辑计划——这件事要等 Analyzer 查 Catalog 之后才能确定。所以解析器先产出
 * {@code UnresolvedRelation("employees")}，Analyzer 再把它换成注册过的
 * {@link Scan}（或其他逻辑计划）。
 *
 * <p>未解析状态下没有 schema，也不能有孩子节点，否则说明 Analyzer 漏掉了这一步，
 * 因此 {@link #schema()} 直接抛异常。
 */
public record UnresolvedRelation(String tableName) implements LogicalPlan {

    @Override
    public List<LogicalPlan> children() {
        return List.of();
    }

    @Override
    public LogicalPlan withNewChildren(List<LogicalPlan> children) {
        if (!children.isEmpty()) {
            throw new IllegalArgumentException("UnresolvedRelation cannot have children");
        }
        return this;
    }

    @Override
    public Schema schema() {
        throw new UnsupportedOperationException("UnresolvedRelation 在 Analyzer 解析前没有 schema: " + tableName);
    }

    @Override
    public String nodeName() {
        return "UnresolvedRelation";
    }

    @Override
    public String detailString() {
        return tableName;
    }
}
