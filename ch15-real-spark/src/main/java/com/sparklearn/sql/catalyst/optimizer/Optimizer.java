package com.sparklearn.sql.catalyst.optimizer;

import com.sparklearn.sql.catalyst.plans.logical.LogicalPlan;
import com.sparklearn.sql.catalyst.rules.RuleExecutor;

import java.util.List;

/**
 * 教学版 Catalyst 优化器。
 *
 * <p>参考 Spark 的 {@code Optimizer}（sql/catalyst/.../optimizer/Optimizer.scala）：
 * 规则按批次组织，每个批次带一个 {@link RuleExecutor.Strategy}。
 *
 * <ul>
 *   <li>{@code Operator Pushdown} 用 FixedPoint——谓词下推和 Filter 合并可能互相触发，
 *       要跑到收敛</li>
 *   <li>{@code Column Pruning} 用 FixedPoint——列裁剪依赖上游投影，可能级联</li>
 * </ul>
 *
 * <p>Spark 里还有用 {@code Once} 的批次（如 {@code Finish Analysis}、{@code OptimizeCodegen}），
 * 本章没有这类规则，所以全部用 FixedPoint。
 */
public final class Optimizer {

    private final RuleExecutor ruleExecutor = new RuleExecutor(List.of(
            new RuleExecutor.Batch(
                    "Operator Pushdown",
                    RuleExecutor.FixedPoint.defaultFixedPoint(),
                    List.of(
                            new CombineFilters(),
                            new PushFilterIntoScan())),
            new RuleExecutor.Batch(
                    "Column Pruning",
                    RuleExecutor.FixedPoint.defaultFixedPoint(),
                    List.of(
                            new PruneScanColumns()))
    ));

    public LogicalPlan optimize(LogicalPlan plan) {
        return ruleExecutor.execute(plan);
    }

    public List<RuleExecutor.Batch> batches() {
        return ruleExecutor.batches();
    }
}
