package com.sparklearn.sql.catalyst.expressions;

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
