package com.sparklearn.sql.catalyst.expressions;

import java.util.List;

/**
 * 函数构造器：根据参数列表构造一个表达式。
 *
 * <p>参考 Spark 的 {@code FunctionRegistry.FunctionBuilder}：
 * <pre>
 * type FunctionBuilder = Seq[Expression] => Expression
 * </pre>
 *
 * <p>注册函数时传入此构造器，后续 SQL 中出现该函数名时，
 * 解析器调用 {@link #apply(List)} 传入实际参数，得到对应的表达式。
 */
@FunctionalInterface
public interface FunctionBuilder {

    /**
     * 根据参数表达式列表构造函数表达式。
     *
     * @param children 函数参数（已解析为 Expression）
     * @return 函数对应的表达式（如 {@link Count}）
     */
    Expression apply(List<Expression> children);
}
