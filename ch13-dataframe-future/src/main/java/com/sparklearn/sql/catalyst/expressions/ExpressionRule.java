package com.sparklearn.sql.catalyst.expressions;

import java.io.Serializable;

/**
 * 表达式树变换规则。
 */
@FunctionalInterface
public interface ExpressionRule extends Serializable {

    Expression apply(Expression expression);
}
