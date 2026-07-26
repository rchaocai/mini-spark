package com.sparklearn.sql.catalyst.expressions;

import com.sparklearn.sql.DataType;

/**
 * 会出现在 Project 输出里的表达式。
 */
public sealed interface NamedExpression extends Expression
        permits Alias, Attribute {

    String name();

    /**
     * 返回表达式的结果类型。
     * <p>
     * Attribute：从数据源的 Schema 推断类型
     * Alias：继承子表达式的类型
     */
    DataType dataType();
}
