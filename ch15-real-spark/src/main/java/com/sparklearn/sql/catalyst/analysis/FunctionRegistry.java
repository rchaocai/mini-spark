package com.sparklearn.sql.catalyst.analysis;

import com.sparklearn.sql.catalyst.expressions.Expression;
import com.sparklearn.sql.catalyst.expressions.FunctionBuilder;

import java.util.List;

/**
 * 函数注册表：管理函数名到构造器的映射，供解析器查找。
 *
 * <p>参考 Spark 的 {@code FunctionRegistry}（sql/catalyst/.../analysis/FunctionRegistry.scala）。
 * Spark 有三类函数：
 * <ul>
 *   <li>内置函数（如 {@code count}、{@code sum}）：启动时注册，全局可用</li>
 *   <li>临时函数（{@code CREATE TEMPORARY FUNCTION}）：会话级别，注册后即可使用</li>
 *   <li>永久函数（{@code CREATE FUNCTION}）：元数据存在 Hive Metastore 中，跨会话共享</li>
 * </ul>
 *
 * <p>mini-spark 不接 Hive Metastore，内置函数和临时函数都在内存中管理，
 * 不需要外部元数据存储。用户通过 {@link #registerFunction} 注册自定义函数后，
 * SQL 中即可使用该函数名。
 */
public interface FunctionRegistry {

    /**
     * 注册函数：把函数名绑定到构造器。
     *
     * <p>同名函数再次注册会覆盖之前的注册（等价于 CREATE OR REPLACE）。
     *
     * @param name    函数名（不区分大小写）
     * @param builder 函数构造器
     */
    void registerFunction(String name, FunctionBuilder builder);

    /**
     * 查找函数并构造表达式。
     *
     * @param name     函数名
     * @param children 函数参数表达式列表
     * @return 函数对应的表达式
     * @throws IllegalArgumentException 函数未注册时抛出
     */
    Expression lookupFunction(String name, List<Expression> children);

    /** 函数是否已注册。 */
    boolean functionExists(String name);

    /** 注销函数，返回函数是否曾存在。 */
    boolean dropFunction(String name);

    /** 列出所有已注册的函数名（按字母序）。 */
    List<String> listFunctions();
}
