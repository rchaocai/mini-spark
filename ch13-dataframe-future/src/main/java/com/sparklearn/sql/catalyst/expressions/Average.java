package com.sparklearn.sql.catalyst.expressions;

import com.sparklearn.sql.DataType;
import com.sparklearn.sql.Row;

import java.util.Set;

/**
 * AVG 聚合函数。
 *
 * <p>参考 Spark 的 {@code Average}（sql/catalyst/.../expressions/aggregate/AggregationFunctions.scala）。
 *
 * <p>Spark 的 {@code Average} 用 {@code DeclarativeAggregate}，维护两个 buffer 字段
 * （{@code sum} 和 {@code count}），通过 Catalyst 表达式描述 update / merge / evaluate。
 *
 * <p>mini-spark 用 {@code double[]} 作为聚合状态：{@code {sum, count}}。
 * {@code initialize} 每行返回 {@code {value, 1}}（null 视为 {@code {0, 0}}），
 * {@code merge} 逐字段相加，{@code evaluate} 返回 {@code sum / count}。
 */
public record Average(Expression child) implements AggregateFunction {

    @Override
    public String name() {
        return "avg";
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
        return "avg(" + child.sql() + ")";
    }

    @Override
    public Object initialize(Row row) {
        Object value = child.eval(row);
        if (value == null) {
            return new double[]{0.0, 0.0};
        }
        return new double[]{((Number) value).doubleValue(), 1.0};
    }

    @Override
    public Object merge(Object state1, Object state2) {
        double[] s1 = (double[]) state1;
        double[] s2 = (double[]) state2;
        return new double[]{s1[0] + s2[0], s1[1] + s2[1]};
    }

    @Override
    public Object evaluate(Object state) {
        double[] s = (double[]) state;
        if (s[1] == 0.0) {
            return 0.0;
        }
        return s[0] / s[1];
    }

    @Override
    public Expression transform(ExpressionRule rule) {
        return rule.apply(new Average(child.transform(rule)));
    }
}
