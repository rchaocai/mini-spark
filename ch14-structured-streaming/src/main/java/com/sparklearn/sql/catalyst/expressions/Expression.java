package com.sparklearn.sql.catalyst.expressions;

import com.sparklearn.sql.DataType;
import com.sparklearn.sql.Row;

import java.io.Serializable;
import java.util.Set;

/**
 * Catalyst 表达式树上的一个节点。
 */
public sealed interface Expression extends Serializable
        permits And, EqualTo, GreaterThan, Literal, Multiply, NamedExpression {

    Object eval(Row row);

    Set<String> references();

    String sql();

    /**
     * 返回表达式的结果类型。
     * <p>
     * 在 Spark 中，类型推断由 Catalyst 的 Analyzer 阶段完成。
     * 本教学实现简化为在每个表达式中直接推断。
     */
    DataType dataType();

    default Expression transform(ExpressionRule rule) {
        return rule.apply(this);
    }

    default GreaterThan gt(Object value) {
        return new GreaterThan(this, Expressions.lit(value));
    }

    default EqualTo eqTo(Object value) {
        return new EqualTo(this, Expressions.lit(value));
    }

    default Multiply multiply(Object value) {
        return new Multiply(this, Expressions.lit(value));
    }

    default Alias as(String name) {
        return new Alias(this, name);
    }
}
