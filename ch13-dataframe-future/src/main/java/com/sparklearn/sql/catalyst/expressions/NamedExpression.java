package com.sparklearn.sql.catalyst.expressions;

import com.sparklearn.sql.DataType;

/**
 * 会出现在 Project 输出里的表达式。
 *
 * <p>参考 Spark 的 {@code NamedExpression}（sql/catalyst/.../expressions/namedExpressions.scala）：
 * 每个命名表达式都有一个全局唯一的 {@link ExprId}，用来区分同名但不同来源的列引用
 *（典型场景：self-join 时两张表都有 name 列，靠 exprId 区分）。
 */
public sealed interface NamedExpression extends Expression
        permits Alias, Attribute, UnresolvedAttribute {

    String name();

    /**
     * 返回表达式的结果类型。
     * <p>
     * Attribute：从数据源的 Schema 推断类型
     * Alias：继承子表达式的类型
     */
    DataType dataType();

    /**
     * 返回此表达式的全局唯一标识。
     * <p>
     * Attribute：构造时自动分配
     * Alias：构造时自动分配
     */
    ExprId exprId();
}
