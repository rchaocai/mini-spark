package com.sparklearn.sql.catalyst.optimizer;

import com.sparklearn.sql.catalyst.expressions.And;
import com.sparklearn.sql.catalyst.plans.logical.Filter;
import com.sparklearn.sql.catalyst.plans.logical.LogicalPlan;
import com.sparklearn.sql.catalyst.rules.PlanRule;

/**
 * Filter 合并规则：合并相邻 Filter，给后续下推规则创造更简单的形状。
 *
 * <p>优化收益：
 * <ul>
 *   <li>简化逻辑计划结构：把多个 Filter 合并成一个，减少计划节点数量</li>
 *   <li>促进谓词下推：合并后的单一 Filter 更容易被推进数据源</li>
 *   <li>减少执行开销：只需要一次过滤操作，而不是多次</li>
 * </ul>
 *
 * <p>优化前的逻辑计划：
 * <pre>
 *   Filter (salary > 50000)
 *     └── Filter (department = 'eng')
 *           └── Scan
 * </pre>
 *
 * <p>优化后的逻辑计划：
 * <pre>
 *   Filter ((salary > 50000) AND (department = 'eng'))
 *     └── Scan
 * </pre>
 *
 * <p>实现思路：
 * <ol>
 *   <li>检查是否存在相邻的两个 Filter 节点</li>
 *   <li>用 And 表达式把两个条件合并</li>
 *   <li>生成新的 Filter 节点，条件为合并后的 And 表达式</li>
 * </ol>
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