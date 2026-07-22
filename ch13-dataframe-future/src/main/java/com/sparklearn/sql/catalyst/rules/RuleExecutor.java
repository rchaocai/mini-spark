package com.sparklearn.sql.catalyst.rules;

import com.sparklearn.sql.catalyst.plans.logical.LogicalPlan;

import java.util.List;

/**
 * 教学版 RuleExecutor：按 batch 顺序应用规则，直到 fixed point。
 */
public final class RuleExecutor {

    private static final int MAX_ITERATIONS = 20;

    private final List<Batch> batches;

    public RuleExecutor(List<Batch> batches) {
        this.batches = List.copyOf(batches);
    }

    public LogicalPlan execute(LogicalPlan plan) {
        LogicalPlan current = plan;
        for (Batch batch : batches) {
            int iteration = 0;
            boolean changed = true;
            while (changed && iteration < MAX_ITERATIONS) {
                LogicalPlan before = current;
                for (PlanRule rule : batch.rules()) {
                    current = current.transformUp(rule);
                }
                changed = !current.equals(before);
                iteration++;
            }
        }
        return current;
    }

    public List<Batch> batches() {
        return batches;
    }

    public record Batch(String name, List<PlanRule> rules) {
        public Batch {
            rules = List.copyOf(rules);
        }
    }
}
