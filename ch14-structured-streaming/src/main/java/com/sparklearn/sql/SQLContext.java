package com.sparklearn.sql;

import com.sparklearn.core.SparkContext;
import com.sparklearn.core.rdd.RDD;
import com.sparklearn.sql.catalyst.analysis.Analyzer;
import com.sparklearn.sql.catalyst.analysis.FunctionRegistry;
import com.sparklearn.sql.catalyst.analysis.SimpleFunctionRegistry;
import com.sparklearn.sql.catalyst.catalog.SessionCatalog;
import com.sparklearn.sql.catalyst.expressions.FunctionBuilder;
import com.sparklearn.sql.catalyst.expressions.UserDefinedFunction;
import com.sparklearn.sql.catalyst.optimizer.Optimizer;
import com.sparklearn.sql.catalyst.parser.LegacySqlParser;
import com.sparklearn.sql.catalyst.parser.ParserInterface;
import com.sparklearn.sql.catalyst.parser.SparkSqlParser;
import com.sparklearn.sql.catalyst.plans.logical.LogicalPlan;
import com.sparklearn.sql.catalyst.plans.logical.Scan;
import com.sparklearn.sql.execution.PhysicalPlanner;
import com.sparklearn.sql.streaming.DataStreamReader;

import java.util.List;
import java.util.Objects;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * 结构化查询入口：负责把 RDD 包成 DataFrame，并驱动分析、优化与物理规划。
 *
 * <p>SQL 解析器默认使用 {@link SparkSqlParser}（ANTLR4 版），
 * 也可通过 {@link #useLegacyParser()} 切换回 {@link LegacySqlParser}（手写版）。
 * 两种解析器都产出"未解析"的计划，由 {@link Analyzer} 绑定表和列的元数据。
 *
 * <p>表和函数的注册管理委托给 {@link SessionCatalog}，
 * 参考 Spark 的 {@code SparkSession} 持有 {@code SessionCatalog} 的设计。
 */
public final class SQLContext {

    private final SparkContext sparkContext;
    private final Optimizer optimizer = new Optimizer();
    private final PhysicalPlanner physicalPlanner = new PhysicalPlanner();
    private final SessionCatalog catalog;
    private final Analyzer analyzer;
    private ParserInterface sqlParser;

    public SQLContext(SparkContext sparkContext) {
        this.sparkContext = Objects.requireNonNull(sparkContext, "sparkContext");
        this.catalog = new SessionCatalog(new SimpleFunctionRegistry());
        this.analyzer = new Analyzer(catalog);
        this.sqlParser = new SparkSqlParser(catalog.functionRegistry());
    }

    public DataFrame createDataFrame(String relationName, List<Row> rows, int numberOfPartitions) {
        if (rows.isEmpty()) {
            throw new IllegalArgumentException("rows must not be empty");
        }
        return createDataFrame(
                relationName,
                sparkContext.parallelize(rows, numberOfPartitions),
                Schema.inferFrom(rows.get(0)));
    }

    /**
     * 用显式 schema 构造 DataFrame，支持空数据（流式微批结果可能为空）。
     * 按位置构造的行（无字段名）会被 schema 的字段名重新标记，使其可按字段名访问。
     */
    public DataFrame createDataFrame(String relationName, List<Row> rows, Schema schema, int numberOfPartitions) {
        List<Row> bound = rows.stream()
                .map(r -> r.fieldNames().isEmpty() ? r.withSchema(schema) : r)
                .toList();
        return createDataFrame(
                relationName,
                sparkContext.parallelize(bound, numberOfPartitions),
                schema);
    }

    public DataFrame createDataFrame(String relationName, RDD<Row> rdd, Schema schema) {
        return new DataFrame(this, new Scan(relationName, schema, rdd));
    }

   public QueryExecution executePlan(LogicalPlan logicalPlan) {
        LogicalPlan optimized = optimizer.optimize(logicalPlan);
        return new QueryExecution(logicalPlan, optimized, physicalPlanner.plan(optimized));
    }

    /**
     * 执行 SQL 查询，返回 DataFrame。
     *
     * <p>流程：解析器把 SQL 文本翻译成"未解析"的逻辑计划 → Analyzer 绑定表和列的元数据
     * → 包装成 DataFrame。之后和 DataFrame API 走完全相同的优化与物理规划链路。
     */
    public DataFrame sql(String sqlText) {
        LogicalPlan unresolved = sqlParser.parsePlan(sqlText);
        LogicalPlan analyzed = analyzer.analyze(unresolved);
        return new DataFrame(this, analyzed);
    }

    /**
     * 注册临时视图，供 SQL 查询通过表名引用。
     *
     * <p>委托给 {@link SessionCatalog#createTempView}，
     * 如果计划是 {@link Scan}，会把 Scan 的 relationName 更新为 tableName。
     */
    public void registerTable(String tableName, LogicalPlan plan) {
        catalog.createTempView(tableName, plan, true);
    }

    /**
     * 注册自定义函数，注册后即可在 SQL 中使用。
     *
     * <p>参考 Spark 的 {@code sqlContext.udf.register(name, builder)}。
     * 内置函数和临时函数都在内存中管理，不需要 Hive Metastore。
     */
    public void registerFunction(String name, FunctionBuilder builder) {
        catalog.registerFunction(name, builder);
    }

    /**
     * 注册单参数 UDF，注册后即可在 SQL 中使用。
     *
     * <p>参考 Spark 的 {@code spark.udf.register(name, func: Function1[A, RT])}。
     * 用户传入一个 {@code Function<Object, Object>}，系统包装成
     * {@link UserDefinedFunction} 表达式节点。
     *
     * @param name       函数名（SQL 中通过此名称调用）
     * @param func       用户函数
     * @param resultType 返回值类型
     */
    public void registerFunction(String name, Function<Object, Object> func, DataType resultType) {
        catalog.registerFunction(name, children ->
                new UserDefinedFunction(name, func, resultType, children));
    }

    /**
     * 注册双参数 UDF，注册后即可在 SQL 中使用。
     *
     * <p>参考 Spark 的 {@code spark.udf.register(name, func: Function2[A1, A2, RT])}。
     * 用户传入一个 {@code BiFunction<Object, Object, Object>}，系统包装成
     * {@link UserDefinedFunction} 表达式节点。
     *
     * @param name       函数名（SQL 中通过此名称调用）
     * @param func       用户函数
     * @param resultType 返回值类型
     */
    public void registerFunction(String name, BiFunction<Object, Object, Object> func,
                                  DataType resultType) {
        catalog.registerFunction(name, children ->
                new UserDefinedFunction(name, func, resultType, children));
    }

    /** 获取会话级 Catalog（主要供内部组件和测试使用）。 */
    public SessionCatalog catalog() {
        return catalog;
    }

    /**
     * 切换到手写解析器 {@link LegacySqlParser}（教学对比用）。
     *
     * @return this，方便链式调用
     */
    public SQLContext useLegacyParser() {
        this.sqlParser = new LegacySqlParser();
        return this;
    }

    /**
     * 切换到 ANTLR4 解析器 {@link SparkSqlParser}（默认）。
     *
     * @return this，方便链式调用
     */
    public SQLContext useSparkParser() {
        this.sqlParser = new SparkSqlParser(catalog.functionRegistry());
        return this;
    }

    /** 当前使用的 SQL 解析器（主要供测试断言）。 */
    public ParserInterface parser() {
        return sqlParser;
    }

    /**
     * 通过表名获取已注册的临时视图，等价于 {@code sql("SELECT * FROM tableName")}。
     * <p>
     * 参考 Spark 的 {@code SparkSession.table(tableName)}。
     * 常用于读取 memory sink 的查询结果：流式查询指定 {@code queryName} 后，
     * 可通过此方法读取 Sink 中的最新数据。
     */
    public DataFrame table(String tableName) {
        return sql("SELECT * FROM " + tableName);
    }

    /**
     * 进入流式读取构建器，以链式方式配置数据源并加载流式 DataFrame。
     * <p>
     * 参考 Spark 的 {@code SparkSession.readStream()}。
     *
     * <h3>示例</h3>
     * <pre>{@code
     * DataStreamReader reader = sql.readStream()
     *         .format("memory")
     *         .schema(wordSchema);
     * DataFrame streamDF = reader.load();
     * MemoryStream source = reader.memoryStream();  // 教学模式手动推送数据
     * }</pre>
     */
    public DataStreamReader readStream() {
        return new DataStreamReader(this);
    }
}

