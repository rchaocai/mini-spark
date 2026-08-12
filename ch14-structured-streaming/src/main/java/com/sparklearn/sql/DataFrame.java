package com.sparklearn.sql;

import com.sparklearn.core.rdd.RDD;
import com.sparklearn.sql.catalyst.expressions.Count;
import com.sparklearn.sql.catalyst.expressions.Expression;
import com.sparklearn.sql.catalyst.expressions.Expressions;
import com.sparklearn.sql.catalyst.expressions.NamedExpression;
import com.sparklearn.sql.catalyst.plans.logical.*;
import com.sparklearn.sql.execution.PhysicalPlan;
import com.sparklearn.sql.streaming.DataStreamWriter;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * 带 schema 的惰性逻辑计划。
 */
public final class DataFrame {

    private final SQLContext sqlContext;
    private final LogicalPlan logicalPlan;

    public DataFrame(SQLContext sqlContext, LogicalPlan logicalPlan) {
        this.sqlContext = Objects.requireNonNull(sqlContext, "sqlContext");
        this.logicalPlan = Objects.requireNonNull(logicalPlan, "logicalPlan");
    }

    /**
     * 把当前 DataFrame 注册为临时视图，供 SQL 查询通过表名引用。
     *
     * <p>委托给 {@link SQLContext#catalog()} 的 {@code createTempView}，
     * 把逻辑计划绑定到视图名，存入 {@code SessionCatalog} 的临时视图表。
     */
    public void createOrReplaceTempView(String viewName) {
        sqlContext.catalog().createTempView(viewName, logicalPlan, true);
    }

    public DataFrame where(Expression condition) {
        return new DataFrame(sqlContext, new Filter(condition, logicalPlan));
    }

    public DataFrame filter(Expression condition) {
        return where(condition);
    }

    public DataFrame select(NamedExpression... expressions) {
        return new DataFrame(sqlContext, new Project(List.of(expressions), logicalPlan));
    }

    public DataFrame join(DataFrame right, Expression condition) {
        return join(right, condition, JoinType.INNER);
    }

    public DataFrame join(DataFrame right, Expression condition, JoinType joinType) {
        return new DataFrame(sqlContext,
                new Join(logicalPlan, right.logicalPlan(), joinType, condition));
    }

    public GroupedDataFrame groupBy(String... columnNames) {
        List<NamedExpression> groupingExpressions = new ArrayList<>();
        for (String columnName : columnNames) {
            groupingExpressions.add(Expressions.col(columnName));
        }
        return new GroupedDataFrame(sqlContext, logicalPlan, groupingExpressions);
    }

    public List<Row> collect() {
        QueryExecution queryExecution = queryExecution();
        PhysicalPlan physicalPlan = queryExecution.executed();
        RDD<Row> rdd = physicalPlan.execute();
        return rdd.collect();
    }

    public long count() {
        QueryExecution queryExecution = queryExecution();
        PhysicalPlan physicalPlan = queryExecution.executed();
        RDD<Row> rdd = physicalPlan.execute();
        return rdd.count();
    }

    public QueryExecution queryExecution() {
        return sqlContext.executePlan(logicalPlan);
    }

    public LogicalPlan logicalPlan() {
        return logicalPlan;
    }

    public Schema schema() {
        return logicalPlan.schema();
    }

    public String explainString() {
        return queryExecution().explainString();
    }

    public void show() {
        List<Row> rows = collect();
        for (Row row : rows) {
            System.out.println(row);
        }
    }

    /**
     * 进入流式写出构建器，以链式方式配置输出模式、Sink 类型并启动查询。
     * <p>
     * 参考 Spark 的 {@code Dataset.writeStream()}。
     *
     * <h3>示例</h3>
     * <pre>{@code
     * StreamingQuery query = resultDF.writeStream()
     *         .outputMode(OutputMode.Complete)
     *         .format("memory")
     *         .queryName("wordCounts")
     *         .start();
     * }</pre>
     */
    public DataStreamWriter writeStream() {
        return new DataStreamWriter(this);
    }

    /** 供流式写出构建器访问上下文。 */
    public SQLContext sqlContext() {
        return sqlContext;
    }

    public record GroupedDataFrame(
            SQLContext sqlContext,
            LogicalPlan child,
            List<NamedExpression> groupingExpressions) {

        public DataFrame count() {
            return new DataFrame(sqlContext, new Aggregate(
                    groupingExpressions, List.of(new Count()), child));
        }
    }
}
