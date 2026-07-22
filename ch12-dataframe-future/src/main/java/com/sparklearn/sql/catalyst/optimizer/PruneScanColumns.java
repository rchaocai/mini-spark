package com.sparklearn.sql.catalyst.optimizer;

import com.sparklearn.sql.catalyst.expressions.Expression;
import com.sparklearn.sql.catalyst.expressions.NamedExpression;
import com.sparklearn.sql.catalyst.plans.logical.Aggregate;
import com.sparklearn.sql.catalyst.plans.logical.LogicalPlan;
import com.sparklearn.sql.catalyst.plans.logical.Project;
import com.sparklearn.sql.catalyst.plans.logical.Scan;
import com.sparklearn.sql.catalyst.rules.PlanRule;

import java.util.LinkedHashSet;
import java.util.Set;

/**
 * 尽量让 Scan 只读后续真正需要的列。
 */
public final class PruneScanColumns implements PlanRule {

    @Override
    public LogicalPlan apply(LogicalPlan plan) {
        if (plan instanceof Project project && project.child() instanceof Scan scan) {
            Set<String> columns = new LinkedHashSet<>();
            for (NamedExpression expression : project.projectList()) {
                columns.addAll(expression.references());
            }
            return new Project(project.projectList(), scan.withRequiredColumns(columns.stream().toList()));
        }
        if (plan instanceof Aggregate aggregate && aggregate.child() instanceof Scan scan) {
            Set<String> columns = new LinkedHashSet<>();
            for (Expression expression : aggregate.groupingExpressions()) {
                columns.addAll(expression.references());
            }
            return new Aggregate(
                    aggregate.groupingExpressions(),
                    scan.withRequiredColumns(columns.stream().toList()));
        }
        return plan;
    }
}
