package com.sparklearn.sql;

import com.sparklearn.core.RDD;
import com.sparklearn.core.SparkContext;
import com.sparklearn.sql.catalyst.optimizer.Optimizer;
import com.sparklearn.sql.catalyst.plans.logical.LogicalPlan;
import com.sparklearn.sql.catalyst.plans.logical.Scan;
import com.sparklearn.sql.execution.PhysicalPlanner;

import java.util.List;
import java.util.Objects;

/**
 * 结构化查询入口：负责把 RDD 包成 DataFrame，并驱动优化与物理规划。
 */
public final class SQLContext {

    private final SparkContext sparkContext;
    private final Optimizer optimizer = new Optimizer();
    private final PhysicalPlanner physicalPlanner = new PhysicalPlanner();

    public SQLContext(SparkContext sparkContext) {
        this.sparkContext = Objects.requireNonNull(sparkContext, "sparkContext");
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

    public DataFrame createDataFrame(String relationName, RDD<Row> rdd, Schema schema) {
        return new DataFrame(this, new Scan(relationName, schema, rdd));
    }

    QueryExecution executePlan(LogicalPlan logicalPlan) {
        LogicalPlan optimized = optimizer.optimize(logicalPlan);
        return new QueryExecution(logicalPlan, optimized, physicalPlanner.plan(optimized));
    }
}
