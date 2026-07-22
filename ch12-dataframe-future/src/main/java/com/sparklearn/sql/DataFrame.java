package com.sparklearn.sql;

import com.sparklearn.sql.catalyst.expressions.Attribute;
import com.sparklearn.sql.catalyst.expressions.Expression;
import com.sparklearn.sql.catalyst.expressions.Expressions;
import com.sparklearn.sql.catalyst.expressions.NamedExpression;
import com.sparklearn.sql.catalyst.plans.logical.Aggregate;
import com.sparklearn.sql.catalyst.plans.logical.Filter;
import com.sparklearn.sql.catalyst.plans.logical.LogicalPlan;
import com.sparklearn.sql.catalyst.plans.logical.Project;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * 带 schema 的惰性逻辑计划。
 */
public final class DataFrame {

    private final SQLContext sqlContext;
    private final LogicalPlan logicalPlan;

    DataFrame(SQLContext sqlContext, LogicalPlan logicalPlan) {
        this.sqlContext = Objects.requireNonNull(sqlContext, "sqlContext");
        this.logicalPlan = Objects.requireNonNull(logicalPlan, "logicalPlan");
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

    public GroupedDataFrame groupBy(String... columnNames) {
        List<Attribute> groupingExpressions = new ArrayList<>();
        for (String columnName : columnNames) {
            groupingExpressions.add(Expressions.col(columnName));
        }
        return new GroupedDataFrame(sqlContext, logicalPlan, groupingExpressions);
    }

    public List<Row> collect() {
        return queryExecution().executed().execute().collect();
    }

    public long count() {
        return queryExecution().executed().execute().count();
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

    public record GroupedDataFrame(
            SQLContext sqlContext,
            LogicalPlan child,
            List<Attribute> groupingExpressions) {

        public DataFrame count() {
            return new DataFrame(sqlContext, new Aggregate(groupingExpressions, child));
        }
    }
}
