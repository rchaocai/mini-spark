package com.sparklearn.sql.catalyst.expressions;

/**
 * 会出现在 Project 输出里的表达式。
 */
public sealed interface NamedExpression extends Expression
        permits Alias, Attribute {

    String name();
}
