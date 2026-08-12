package com.sparklearn.sql.catalyst.expressions;

import com.sparklearn.sql.DataType;
import com.sparklearn.sql.Row;
import com.sparklearn.sql.execution.CodegenContext;

import java.io.Serializable;
import java.util.List;
import java.util.Set;

/**
 * Catalyst 表达式树上的一个节点。
 */
public sealed interface Expression extends Serializable
        permits And, EqualTo, GreaterThan, GreaterThanOrEqual, LessThan, LessThanOrEqual,
                Literal, Multiply, NamedExpression, NotEqualTo, Or, AggregateFunction, UserDefinedFunction {

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

    default EqualTo equalTo(Expression other) {
        return new EqualTo(this, other);
    }

    default Multiply multiply(Object value) {
        return new Multiply(this, Expressions.lit(value));
    }

    default Alias as(String name) {
        return new Alias(this, name);
    }

    /**
     * 生成对一行数据求值的 Java 表达式字符串。
     *
     * <p>这是 whole-stage codegen 的表达式端入口：每个表达式把自己翻译成一段 Java 代码，
     * 拼进生成的 processNext 循环里。rowVar 是当前行变量名，inputFields 是该行的字段名列表，
     * 用来把 Attribute 的列名解析成位置索引。
     *
     * @param ctx          代码生成上下文
     * @param rowVar       当前行的变量名，例如 "scanRow"
     * @param inputFields  当前行各字段的名称，用来解析列索引
     * @return 一段 Java 表达式，例如 "(((Number) (scanRow.get(3))).doubleValue() > 50000.0)"
     */
    String genCode(CodegenContext ctx, String rowVar, List<String> inputFields);
}
