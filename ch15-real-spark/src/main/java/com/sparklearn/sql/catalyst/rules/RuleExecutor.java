package com.sparklearn.sql.catalyst.rules;

import com.sparklearn.sql.catalyst.plans.logical.LogicalPlan;

import java.util.List;

/**
 * 教学版 RuleExecutor：按 batch 顺序应用规则，直到 fixed point。
 *
 * <p>参考 Spark 的 {@code RuleExecutor}（sql/catalyst/.../rules/RuleExecutor.scala）：
 * <ul>
 *   <li>每个 {@link Batch} 带一个 {@link Strategy}，决定跑一次还是跑到收敛</li>
 *   <li>{@link Strategy#ONCE} 适合"只做一次就够"的规则（如 Spark 的 Finish Analysis）</li>
 *   <li>{@link Strategy#fixedPoint(int)} 适合"可能触发连锁改写"的规则（如谓词下推、列裁剪）</li>
 * </ul>
 */
public final class RuleExecutor {

    private static final int DEFAULT_MAX_ITERATIONS = 20;

    private final List<Batch> batches;

    public RuleExecutor(List<Batch> batches) {
        this.batches = List.copyOf(batches);
    }

    public LogicalPlan execute(LogicalPlan plan) {
        LogicalPlan current = plan;
        for (Batch batch : batches) {
            current = batch.apply(current);
        }
        return current;
    }

    public List<Batch> batches() {
        return batches;
    }

    /**
     * 一个批次的执行策略：决定规则集合跑一次还是跑到收敛。
     *
     * <p>对应 Spark 的 {@code RuleExecutor.Strategy}：
     * <pre>
     * case object Once extends Strategy { val maxIterations = 1 }
     * case class FixedPoint(maxIterations: Int) extends Strategy
     * </pre>
     */
    public sealed interface Strategy permits Once, FixedPoint {

        int maxIterations();
    }

    /**
     * 只跑一次。适合不会触发连锁改写的规则。
     */
    public record Once() implements Strategy {

        public static final Once INSTANCE = new Once();

        @Override
        public int maxIterations() {
            return 1;
        }
    }

    /**
     * 跑到收敛或达到 maxIterations，取先到者。
     */
    public record FixedPoint(int maxIterations) implements Strategy {

        public FixedPoint {
            if (maxIterations <= 0) {
                throw new IllegalArgumentException("maxIterations must be positive: " + maxIterations);
            }
        }

        public static FixedPoint defaultFixedPoint() {
            return new FixedPoint(DEFAULT_MAX_ITERATIONS);
        }
    }

    /**
     * 一组规则 + 一个执行策略。
     */
    public record Batch(String name, Strategy strategy, List<PlanRule> rules) {

        public Batch {
            rules = List.copyOf(rules);
        }

        /**
         * 旧版兼容构造：默认 FixedPoint。
         *
         * <p>已标记废弃，新调用应显式传入 {@link Strategy}，让读者一眼看清每个批次的执行方式。
         */
        @Deprecated
        public Batch(String name, List<PlanRule> rules) {
            this(name, FixedPoint.defaultFixedPoint(), rules);
        }

        LogicalPlan apply(LogicalPlan plan) {
            LogicalPlan current = plan;
            int iteration = 0;
            int limit = strategy.maxIterations();
            LogicalPlan last = current;
            while (iteration < limit) {
                for (PlanRule rule : rules) {
                    current = current.transformUp(rule);
                }
                if (strategy instanceof Once) {
                    break;
                }
                if (current.equals(last)) {
                    break;
                }
                last = current;
                iteration++;
            }
            return current;
        }
    }
}
