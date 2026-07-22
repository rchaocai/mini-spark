package com.sparklearn.sql.catalyst.optimizer;

import com.sparklearn.sql.catalyst.expressions.And;
import com.sparklearn.sql.catalyst.plans.logical.Filter;
import com.sparklearn.sql.catalyst.plans.logical.LogicalPlan;
import com.sparklearn.sql.catalyst.rules.PlanRule;

/**
 * 合并相邻 Filter，给后续下推规则创造更简单的形状。
 */
public final class CombineFilters implements PlanRule {

    @Override
    public LogicalPlan apply(LogicalPlan plan) {
        if (plan instanceof Filter upper && upper.child() instanceof Filter lower) {
            return new Filter(
                    new And(upper.condition(), lower.condition()),
                    lower.child());
        }
        return plan;
    }
}
