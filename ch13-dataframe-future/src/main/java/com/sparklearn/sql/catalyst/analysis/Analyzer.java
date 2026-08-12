package com.sparklearn.sql.catalyst.analysis;

import com.sparklearn.sql.catalyst.catalog.SessionCatalog;
import com.sparklearn.sql.catalyst.plans.logical.LogicalPlan;
import com.sparklearn.sql.catalyst.rules.PlanRule;
import com.sparklearn.sql.catalyst.rules.RuleExecutor;

import java.util.List;

/**
 * 分析器：把"未解析"的逻辑计划绑定为"已解析"的逻辑计划。
 *
 * <p>SQL 解析器（{@link com.sparklearn.sql.catalyst.parser.ParserInterface}）产出的计划
 * 里，表名只是字符串（{@link com.sparklearn.sql.catalyst.plans.logical.UnresolvedRelation}），
 * 列名也只是字符串（{@link com.sparklearn.sql.catalyst.expressions.UnresolvedAttribute}）。
 * Analyzer 的职责是把这些字符串绑定到 Catalog 里的真实元数据：
 *
 * <ul>
 *   <li>{@link ResolveRelations}：{@code UnresolvedRelation(表名)} → 注册过的 {@code LogicalPlan}</li>
 *   <li>{@link ResolveAttributes}：{@code UnresolvedAttribute(列名)} → 带类型的 {@code Attribute}</li>
 * </ul>
 *
 * <p>两条规则放在同一个 batch 里，用 {@link RuleExecutor.Strategy.Once} 策略跑一次：
 * {@code ResolveRelations} 先把 {@code UnresolvedRelation} 换成带 schema 的 {@code Scan}，
 * {@code ResolveAttributes} 再从 child 的 schema 里查出列的类型。{@code RuleExecutor.transformUp}
 * 自底向上保证：当 {@code ResolveAttributes} 处理一个 {@code Filter} 时，它的 child 已经被
 * {@code ResolveRelations} 解析过了。
 *
 * <p>解析通过后，计划才是 "resolved" 的，才能交给优化器改写。
 */
public final class Analyzer {

    private final RuleExecutor ruleExecutor;

    public Analyzer(SessionCatalog catalog) {
        List<PlanRule> rules = List.of(
                new ResolveRelations(catalog),
                new ResolveAttributes());
        this.ruleExecutor = new RuleExecutor(List.of(
                new RuleExecutor.Batch(
                        "Analysis",
                        RuleExecutor.Once.INSTANCE,
                        rules)));
    }

    /**
     * 对未解析的计划应用解析规则，返回已解析的计划。
     *
     * @param plan 解析器产出的未解析计划
     * @return 绑定元数据后的已解析计划
     */
    public LogicalPlan analyze(LogicalPlan plan) {
        return ruleExecutor.execute(plan);
    }
}
