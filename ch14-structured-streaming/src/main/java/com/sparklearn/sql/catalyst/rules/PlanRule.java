package com.sparklearn.sql.catalyst.rules;

import com.sparklearn.sql.catalyst.plans.logical.LogicalPlan;

import java.io.Serializable;

/**
 * 逻辑计划变换规则。
 */
@FunctionalInterface
public interface PlanRule extends Serializable {

    LogicalPlan apply(LogicalPlan plan);

    default String ruleName() {
        return getClass().getSimpleName();
    }
}
