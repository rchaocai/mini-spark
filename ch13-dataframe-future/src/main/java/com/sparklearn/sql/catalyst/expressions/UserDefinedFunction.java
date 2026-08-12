package com.sparklearn.sql.catalyst.expressions;

import com.sparklearn.sql.DataType;
import com.sparklearn.sql.Row;
import com.sparklearn.sql.execution.CodegenContext;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * 用户自定义标量函数（UDF）的表达式节点。
 *
 * <p>参考 Spark 的 {@code ScalaUDF}（sql/catalyst/.../expressions/ScalaUDF.scala）：
 * 用户通过 {@code spark.udf.register(name, func)} 注册一个 Lambda 函数，
 * 解析器在 SQL 中遇到该函数名时，通过 {@link FunctionRegistry} 查找到对应的
 * {@link FunctionBuilder}，构造出 {@code UserDefinedFunction} 实例。
 *
 * <p>{@code UserDefinedFunction} 持有用户函数的引用（{@code function} 字段），在 {@link #eval(Row)}
 * 中对每个子表达式求值后，按参数数量调用用户函数：
 * <ul>
 *   <li>1 个参数：cast 为 {@code Function<Object, Object>} 并调用 {@code apply(arg0)}</li>
 *   <li>2 个参数：cast 为 {@code BiFunction<Object, Object, Object>} 并调用 {@code apply(arg0, arg1)}</li>
 * </ul>
 *
 * <p>教学简化：
 * <ul>
 *   <li>只支持 1~2 个参数（Spark 支持 0~22 个）</li>
 *   <li>不做 Catalyst 类型与 Java 类型之间的转换（Spark 用 {@code CatalystTypeConverters}）</li>
 *   <li>不参与 whole-stage codegen——UDF 调用是黑盒，无法内联到生成的代码中</li>
 * </ul>
 */
public record UserDefinedFunction(
        String name,
        Object function,
        DataType resultType,
        List<Expression> children) implements Expression {

    @Override
    public Object eval(Row row) {
        return switch (children.size()) {
            case 1 -> {
                Object arg0 = children.get(0).eval(row);
                @SuppressWarnings("unchecked")
                Function<Object, Object> func1 = (Function<Object, Object>) function;
                yield func1.apply(arg0);
            }
            case 2 -> {
                Object arg0 = children.get(0).eval(row);
                Object arg1 = children.get(1).eval(row);
                @SuppressWarnings("unchecked")
                BiFunction<Object, Object, Object> func2 =
                        (BiFunction<Object, Object, Object>) function;
                yield func2.apply(arg0, arg1);
            }
            default -> throw new UnsupportedOperationException(
                    "UDF only supports 1-2 arguments, got: " + children.size());
        };
    }

    @Override
    public Set<String> references() {
        LinkedHashSet<String> refs = new LinkedHashSet<>();
        for (Expression child : children) {
            refs.addAll(child.references());
        }
        return Set.copyOf(refs);
    }

    @Override
    public String sql() {
        return name + "(" + String.join(", ",
                children.stream().map(Expression::sql).toList()) + ")";
    }

    @Override
    public DataType dataType() {
        return resultType;
    }

    @Override
    public Expression transform(ExpressionRule rule) {
        List<Expression> newChildren = children.stream()
                .map(c -> c.transform(rule))
                .toList();
        return rule.apply(new UserDefinedFunction(name, function, resultType, newChildren));
    }

    @Override
    public String genCode(CodegenContext ctx, String rowVar, List<String> inputFields) {
        throw new UnsupportedOperationException(
                "UDF 不参与 whole-stage codegen，请在聚合查询中使用");
    }
}
