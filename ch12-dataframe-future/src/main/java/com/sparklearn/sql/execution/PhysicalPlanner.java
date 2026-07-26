package com.sparklearn.sql.execution;

import com.sparklearn.sql.catalyst.plans.logical.Aggregate;
import com.sparklearn.sql.catalyst.plans.logical.Filter;
import com.sparklearn.sql.catalyst.plans.logical.LogicalPlan;
import com.sparklearn.sql.catalyst.plans.logical.Project;
import com.sparklearn.sql.catalyst.plans.logical.Scan;

/**
 * 把优化后的逻辑计划翻译成物理计划。
 */
public final class PhysicalPlanner {

    public PhysicalPlan plan(LogicalPlan logicalPlan) {
        if (logicalPlan instanceof Scan scan) {
            return new ScanExec(
                    scan.relationName(),
                    scan.rdd(),
                    scan.requiredColumns(),
                    scan.pushedFilters());
        }
        if (logicalPlan instanceof Filter filter) {
            return new FilterExec(filter.condition(), plan(filter.child()));
        }
        if (logicalPlan instanceof Project project) {
            return new ProjectExec(project.projectList(), plan(project.child()));
        }
        if (logicalPlan instanceof Aggregate aggregate) {
            return new HashAggregateExec(
                    aggregate.groupingExpressions(),
                    plan(aggregate.child()));
        }
        throw new IllegalArgumentException("unknown logical plan: " + logicalPlan);
    }
}
