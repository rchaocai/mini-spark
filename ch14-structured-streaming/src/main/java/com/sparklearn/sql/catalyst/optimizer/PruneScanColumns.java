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
 * 列裁剪规则：尽量让 Scan 只读后续真正需要的列。
 *
 * <p>优化收益：
 * <ul>
 *   <li>减少数据读取量：只读取后续操作真正用到的列</li>
 *   <li>减少内存占用：每行数据只存储需要的列值</li>
 *   <li>减少网络传输：如果数据源在远程存储，只传输需要的列</li>
 * </ul>
 *
 * <p>优化前的逻辑计划：
 * <pre>
 *   Project (name, salary)
 *     └── Scan (读取 id, name, department, salary 所有列)
 * </pre>
 *
 * <p>优化后的逻辑计划：
 * <pre>
 *   Project (name, salary)
 *     └── Scan (只读 name, salary 两列)
 * </pre>
 *
 * <p>实现思路：
 * <ol>
 *   <li>遍历 Project 的所有表达式，收集引用的字段名</li>
 *   <li>修改 Scan，只读取这些字段</li>
 * </ol>
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