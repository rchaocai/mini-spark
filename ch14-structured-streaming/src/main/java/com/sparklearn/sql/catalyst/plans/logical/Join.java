package com.sparklearn.sql.catalyst.plans.logical;

import com.sparklearn.sql.Field;
import com.sparklearn.sql.Schema;
import com.sparklearn.sql.catalyst.expressions.Expression;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * JOIN 逻辑节点。
 *
 * <p>参考 Spark 的 {@code Join}（sql/catalyst/.../plans/logical/basicLogicalOperators.scala）：
 * <pre>
 * case class Join(left: LogicalPlan, right: LogicalPlan, joinType: JoinType,
 *                 condition: Option[Expression])
 * </pre>
 *
 * <p>mini-spark 简化：
 * <ul>
 *   <li>{@code condition} 直接是 {@link Expression}（非 Option），要求 ON 子句必须有</li>
 *   <li>只支持 {@link JoinType#INNER} 和 {@link JoinType#LEFT_OUTER}</li>
 * </ul>
 */
public record Join(
        LogicalPlan left,
        LogicalPlan right,
        JoinType joinType,
        Expression condition) implements LogicalPlan {

    public Join {
        Objects.requireNonNull(left, "left");
        Objects.requireNonNull(right, "right");
        Objects.requireNonNull(joinType, "joinType");
        Objects.requireNonNull(condition, "condition");
    }

    @Override
    public List<LogicalPlan> children() {
        return List.of(left, right);
    }

    @Override
    public LogicalPlan withNewChildren(List<LogicalPlan> children) {
        return new Join(children.get(0), children.get(1), joinType, condition);
    }

    /**
     * 合并左右 schema：{@code left.output ++ right.output}，两表字段全部保留。
     *
     * <p>参考 Spark 的 {@code Join.output = left.output ++ right.output}
     *（basicLogicalOperators.scala）。INNER JOIN 直接拼接；Spark 在 LEFT_OUTER /
     * RIGHT_OUTER / FULL_OUTER 时会把"可空的一侧"列标 nullable，mini-spark 暂不
     * 处理 nullable，统一拼接。
     *
     * <p>同名列共存（例如 self-join 的 {@code a.name, b.name}），靠 {@link Field#exprId()}
     * 在表达式层区分。ResolveAttributes 在分析阶段会给 self-join 的右表 {@link Scan}
     * 调 {@code newInstance()} 分配新 ExprId（参考 Spark 的 {@code dedupRight}），
     * 保证左右同名 Attribute 的 ExprId 不同。
     */
    @Override
    public Schema schema() {
        List<Field> fields = new ArrayList<>(left.schema().fields());
        fields.addAll(right.schema().fields());
        return new Schema(fields);
    }

    @Override
    public String nodeName() {
        return "Join";
    }

    @Override
    public String detailString() {
        return joinType.sql() + ", " + condition.sql();
    }
}
