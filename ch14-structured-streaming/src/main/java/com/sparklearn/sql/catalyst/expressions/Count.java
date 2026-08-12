package com.sparklearn.sql.catalyst.expressions;

import com.sparklearn.sql.DataType;
import com.sparklearn.sql.Row;

import java.util.Set;

/**
 * COUNT 聚合函数。
 *
 * <p>参考 Spark 的 {@code Count}（sql/catalyst/.../expressions/aggregate/AggregationFunctions.scala）。
 *
 * <p>两种形态：
 * <ul>
 *   <li>{@code count(*)}：child 为 null，统计行数</li>
 *   <li>{@code count(column)}：child 为具体表达式，统计非空值数</li>
 * </ul>
 *
 * <p>聚合状态用 {@code Long} 表示。{@code initialize} 每行返回 1L（或 0L 如果 count 的列为 null），
 * {@code merge} 做累加。{@code reduceByKey} 内部的 map-side combine 反复 merge 就完成了
 * Partial 聚合，shuffle 后的 reduce 阶段再做 Final 聚合。
 */
public record Count(Expression child) implements AggregateFunction {

    /** count(*) 的便捷构造。 */
    public Count() {
        this(null);
    }

    @Override
    public String name() {
        return "count";
    }

    @Override
    public DataType dataType() {
        return DataType.LONG;
    }

    @Override
    public Set<String> references() {
        return child == null ? Set.of() : child.references();
    }

    @Override
    public String sql() {
        return child == null ? "count(*)" : "count(" + child.sql() + ")";
    }

    @Override
    public Object initialize(Row row) {
        if (child == null) {
            return 1L;
        }
        return child.eval(row) != null ? 1L : 0L;
    }

    @Override
    public Object merge(Object state1, Object state2) {
        return (Long) state1 + (Long) state2;
    }

    @Override
    public Object evaluate(Object state) {
        return state;
    }

    @Override
    public Expression transform(ExpressionRule rule) {
        Expression newChild = child == null ? null : child.transform(rule);
        return rule.apply(new Count(newChild));
    }
}
