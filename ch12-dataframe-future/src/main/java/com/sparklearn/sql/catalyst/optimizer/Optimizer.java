package com.sparklearn.sql.catalyst.optimizer;

import com.sparklearn.sql.catalyst.plans.logical.LogicalPlan;
import com.sparklearn.sql.catalyst.rules.RuleExecutor;

import java.util.List;

/**
 * 教学版 Catalyst 优化器。
 */
public final class Optimizer {

    private final RuleExecutor ruleExecutor = new RuleExecutor(List.of(
            new RuleExecutor.Batch("Operator Pushdown", List.of(
                    new CombineFilters(),
                    new PushFilterIntoScan())),
            new RuleExecutor.Batch("Column Pruning", List.of(
                    new PruneScanColumns()))
    ));

    public LogicalPlan optimize(LogicalPlan plan) {
        return ruleExecutor.execute(plan);
    }

    public List<RuleExecutor.Batch> batches() {
        return ruleExecutor.batches();
    }
}
