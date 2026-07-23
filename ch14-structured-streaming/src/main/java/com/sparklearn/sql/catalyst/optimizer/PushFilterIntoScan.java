package com.sparklearn.sql.catalyst.optimizer;

import com.sparklearn.sql.catalyst.plans.logical.Filter;
import com.sparklearn.sql.catalyst.plans.logical.LogicalPlan;
import com.sparklearn.sql.catalyst.plans.logical.Scan;
import com.sparklearn.sql.catalyst.rules.PlanRule;

/**
 * 把 Filter 推进支持过滤的数据源扫描。
 */
public final class PushFilterIntoScan implements PlanRule {

    @Override
    public LogicalPlan apply(LogicalPlan plan) {
        if (plan instanceof Filter filter && filter.child() instanceof Scan scan) {
            return scan.withPushedFilter(filter.condition());
        }
        return plan;
    }
}
