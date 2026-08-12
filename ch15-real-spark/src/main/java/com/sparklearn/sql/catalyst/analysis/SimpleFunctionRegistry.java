package com.sparklearn.sql.catalyst.analysis;

import com.sparklearn.sql.catalyst.expressions.*;

import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * 内存函数注册表，管理内置函数和用户注册的临时函数。
 *
 * <p>参考 Spark 的 {@code SimpleFunctionRegistry}（sql/catalyst/.../analysis/FunctionRegistry.scala）：
 * 内部用一个 {@code Map[String, FunctionBuilder]} 存储函数名到构造器的映射，
 * 函数名统一小写以实现大小写不敏感。
 *
 * <p>内置函数在构造时自动注册。当前注册了 {@code count}、{@code sum}、{@code avg}——
 * 后续扩展更多函数时，只需在构造器里追加一行 {@link #registerFunction} 调用。
 */
public final class SimpleFunctionRegistry implements FunctionRegistry {

    private final Map<String, FunctionBuilder> functionBuilders = new HashMap<>();

    public SimpleFunctionRegistry() {
        registerBuiltInFunctions();
    }

    private void registerBuiltInFunctions() {
        // count(*) → Count()（无参，统计行数）
        // count(col) → Count(col)（统计非空值数）
        // count(1) 等价于 count(*)，解析器把 * 翻译成 Literal(1)
        registerFunction("count", children -> {
            if (children.isEmpty()) {
                return new Count();
            }
            Expression arg = children.get(0);
            if (arg instanceof Literal lit
                    && lit.value() instanceof Integer i && i == 1) {
                return new Count();
            }
            return new Count(arg);
        });

        // sum(col) → Sum(col)（数值累加）
        registerFunction("sum", children -> {
            if (children.isEmpty()) {
                throw new IllegalArgumentException("sum requires at least 1 argument");
            }
            return new Sum(children.get(0));
        });

        // avg(col) → Average(col)（数值平均）
        registerFunction("avg", children -> {
            if (children.isEmpty()) {
                throw new IllegalArgumentException("avg requires at least 1 argument");
            }
            return new Average(children.get(0));
        });
    }

    @Override
    public void registerFunction(String name, FunctionBuilder builder) {
        functionBuilders.put(name.toLowerCase(Locale.ROOT), builder);
    }

    @Override
    public Expression lookupFunction(String name, List<Expression> children) {
        FunctionBuilder builder = functionBuilders.get(name.toLowerCase(Locale.ROOT));
        if (builder == null) {
            throw new IllegalArgumentException("undefined function: " + name);
        }
        return builder.apply(children);
    }

    @Override
    public boolean functionExists(String name) {
        return functionBuilders.containsKey(name.toLowerCase(Locale.ROOT));
    }

    @Override
    public boolean dropFunction(String name) {
        return functionBuilders.remove(name.toLowerCase(Locale.ROOT)) != null;
    }

    @Override
    public List<String> listFunctions() {
        return functionBuilders.keySet().stream().sorted().toList();
    }
}
