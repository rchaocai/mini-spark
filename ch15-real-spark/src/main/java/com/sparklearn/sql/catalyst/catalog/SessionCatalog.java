package com.sparklearn.sql.catalyst.catalog;

import com.sparklearn.sql.catalyst.analysis.FunctionRegistry;
import com.sparklearn.sql.catalyst.expressions.Expression;
import com.sparklearn.sql.catalyst.expressions.FunctionBuilder;
import com.sparklearn.sql.catalyst.plans.logical.LogicalPlan;
import com.sparklearn.sql.catalyst.plans.logical.Scan;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 会话级 Catalog：管理临时视图和函数注册表。
 *
 * <p>参考 Spark 的 {@code SessionCatalog}（sql/catalyst/.../catalog/SessionCatalog.scala）。
 * Spark 的 SessionCatalog 是 ExternalCatalog（Hive Metastore）的代理，同时管理
 * 会话级别的临时表和临时函数。mini-spark 不接 Hive Metastore，没有 CREATE TABLE
 * 持久化需求，因此只保留临时视图和函数注册两部分。
 *
 * <h2>临时视图</h2>
 * <p>{@link #createTempView} 把表名和逻辑计划绑定，存入内存 Map。
 * 后续 SQL 中出现 {@code FROM tableName} 时，Analyzer 调用
 * {@link #lookupRelation} 取回对应的逻辑计划。
 *
 * <p>如果注册的计划是一个 {@link Scan}，会把 Scan 的 {@code relationName}
 * 更新为视图名——这样 JOIN 中按表名查找 schema
 *（{@code ON employees.col = departments.col}）才能正确匹配。
 *
 * <h2>函数注册</h2>
 * <p>函数管理委托给 {@link FunctionRegistry}。内置函数（如 {@code count}）
 * 在构造时自动注册；用户可通过 {@link #registerFunction} 注册自定义函数。
 * 不需要 Hive 元数据——内置函数和临时函数都在内存中管理。
 */
public final class SessionCatalog {

    /** 临时视图：表名 → 逻辑计划 */
    private final Map<String, LogicalPlan> tempViews = new HashMap<>();

    /** 函数注册表 */
    private final FunctionRegistry functionRegistry;

    public SessionCatalog(FunctionRegistry functionRegistry) {
        this.functionRegistry = functionRegistry;
    }

    // -------------------------------------------------------
    //  临时视图管理
    // -------------------------------------------------------

    /**
     * 注册临时视图。
     *
     * <p>参考 Spark 的 {@code SessionCatalog.createTempTable}。
     * 如果计划是 {@link Scan}，会更新 {@code relationName} 为视图名，
     * 使 JOIN 的限定列名（{@code table.col}）能正确匹配。
     *
     * @param name             视图名
     * @param plan             逻辑计划
     * @param overrideIfExists 已存在时是否覆盖
     */
    public void createTempView(String name, LogicalPlan plan, boolean overrideIfExists) {
        if (tempViews.containsKey(name) && !overrideIfExists) {
            throw new IllegalStateException("Temp view already exists: " + name);
        }
        LogicalPlan toRegister = plan;
        if (plan instanceof Scan scan) {
            toRegister = new Scan(name, scan.sourceSchema(), scan.rdd(),
                    scan.requiredColumns(), scan.pushedFilters());
        }
        tempViews.put(name, toRegister);
    }

    /**
     * 按表名查找关系，返回对应的逻辑计划。
     *
     * <p>参考 Spark 的 {@code SessionCatalog.lookupRelation}。
     * Spark 会先查临时表，再查 ExternalCatalog（永久表）。
     * mini-spark 没有永久表，只查临时视图。
     *
     * @param name 表名
     * @return 对应的逻辑计划
     * @throws IllegalArgumentException 表名未注册时抛出
     */
    public LogicalPlan lookupRelation(String name) {
        LogicalPlan plan = tempViews.get(name);
        if (plan == null) {
            throw new IllegalArgumentException("table not found: " + name);
        }
        return plan;
    }

    /** 表是否已注册为临时视图。 */
    public boolean tableExists(String name) {
        return tempViews.containsKey(name);
    }

    /** 删除临时视图。 */
    public void dropTempView(String name) {
        tempViews.remove(name);
    }

    /** 清除所有临时视图。 */
    public void clearTempViews() {
        tempViews.clear();
    }

    // -------------------------------------------------------
    //  函数管理（委托给 FunctionRegistry）
    // -------------------------------------------------------

    /**
     * 注册函数。
     *
     * <p>参考 Spark 的 {@code SessionCatalog.registerFunction}。
     * 内置函数和临时函数都在内存中管理，不需要 Hive Metastore。
     */
    public void registerFunction(String name, FunctionBuilder builder) {
        functionRegistry.registerFunction(name, builder);
    }

    /**
     * 查找函数并构造表达式。
     *
     * <p>参考 Spark 的 {@code SessionCatalog.lookupFunction}。
     */
    public Expression lookupFunction(String name, List<Expression> children) {
        return functionRegistry.lookupFunction(name, children);
    }

    /** 函数是否已注册。 */
    public boolean functionExists(String name) {
        return functionRegistry.functionExists(name);
    }

    /** 获取底层函数注册表（主要供测试断言）。 */
    public FunctionRegistry functionRegistry() {
        return functionRegistry;
    }
}
