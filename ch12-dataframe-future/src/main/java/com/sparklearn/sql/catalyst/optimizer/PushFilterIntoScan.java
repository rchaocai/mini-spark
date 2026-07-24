package com.sparklearn.sql.catalyst.optimizer;

import com.sparklearn.sql.catalyst.plans.logical.Filter;
import com.sparklearn.sql.catalyst.plans.logical.LogicalPlan;
import com.sparklearn.sql.catalyst.plans.logical.Scan;
import com.sparklearn.sql.catalyst.rules.PlanRule;

/**
 * 谓词下推规则：把 Filter 推进支持过滤的数据源扫描。
 *
 * <p>优化收益：
 * <ul>
 *   <li>减少数据读取量：在数据源层面过滤，只读取符合条件的数据</li>
 *   <li>减少网络传输：如果数据源在远程存储（如 HDFS），只传输过滤后的数据</li>
 *   <li>减少内存占用：内存中只处理符合条件的数据</li>
 * </ul>
 *
 * <p>优化前的逻辑计划：
 * <pre>
 *   Filter (salary > 50000)
 *     └── Scan (读取所有列)
 * </pre>
 *
 * <p>优化后的逻辑计划：
 * <pre>
 *   Scan (读取所有列, 过滤 salary > 50000)
 * </pre>
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