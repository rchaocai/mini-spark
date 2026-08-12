package com.sparklearn.sql.catalyst.expressions;

import com.sparklearn.sql.DataType;
import com.sparklearn.sql.Row;

import java.util.Set;

/**
 * SUM 聚合函数。
 *
 * <p>参考 Spark 的 {@code Sum}（sql/catalyst/.../expressions/aggregate/AggregationFunctions.scala）。
 *
 * <p>聚合状态用 {@code Double} 表示。{@code initialize} 每行返回该列的值（null 视为 0），
 * {@code merge} 做累加，{@code evaluate} 直接输出累加结果。
 */
public record Sum(Expression child) implements AggregateFunction {

    @Override
    public String name() {
        return "sum";
    }

    @Override
    public DataType dataType() {
        return DataType.DOUBLE;
    }

    @Override
    public Set<String> references() {
        return child.references();
    }

    @Override
    public String sql() {
        return "sum(" + child.sql() + ")";
    }

    @Override
    public Object initialize(Row row) {
        Object value = child.eval(row);
        return value != null ? ((Number) value).doubleValue() : 0.0;
    }

    @Override
    public Object merge(Object state1, Object state2) {
        return (Double) state1 + (Double) state2;
    }

    @Override
    public Object evaluate(Object state) {
        return state;
    }

    @Override
    public Expression transform(ExpressionRule rule) {
        return rule.apply(new Sum(child.transform(rule)));
    }
}
