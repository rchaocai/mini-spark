package com.sparklearn.sql.catalyst.plans.logical;

import com.sparklearn.sql.DataType;
import com.sparklearn.sql.Field;
import com.sparklearn.sql.Schema;
import com.sparklearn.sql.catalyst.expressions.AggregateFunction;
import com.sparklearn.sql.catalyst.expressions.Attribute;
import com.sparklearn.sql.catalyst.expressions.NamedExpression;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * 聚合逻辑节点。
 *
 * <p>参考 Spark 的 {@code Aggregate}（sql/catalyst/.../plans/logical/basicLogicalOperators.scala）：
 * <pre>
 * case class Aggregate(
 *     groupingExpressions: Seq[Expression],
 *     aggregateExpressions: Seq[NamedExpression],
 *     child: LogicalPlan)
 * </pre>
 *
 * <p>Spark 把"按什么分组"（{@code groupingExpressions}）和"算什么聚合函数"
 * （{@code aggregateExpressions}）分开。{@code aggregateExpressions} 里的每个元素
 * 是 {@code AggregateExpression(AggregateFunction, mode)}，{@code mode} 取
 * {@code Partial} / {@code Final} / {@code Complete}。
 *
 * <p>mini-spark 简化：{@code aggregateExpressions} 直接是 {@link AggregateFunction} 列表
 *（不包 AggregateExpression），模式默认 Complete（由 {@code reduceByKey} 隐式拆成
 * Partial + Final 两阶段）。
 */
public record Aggregate(
        List<NamedExpression> groupingExpressions,
        List<AggregateFunction> aggregateExpressions,
        LogicalPlan child) implements LogicalPlan {

    public Aggregate {
        groupingExpressions = List.copyOf(groupingExpressions);
        aggregateExpressions = List.copyOf(aggregateExpressions);
        Objects.requireNonNull(child, "child");
    }

    @Override
    public List<LogicalPlan> children() {
        return List.of(child);
    }

    @Override
    public LogicalPlan withNewChildren(List<LogicalPlan> children) {
        return new Aggregate(groupingExpressions, aggregateExpressions, children.get(0));
    }

    /**
     * 输出 schema：分组列 + 聚合列。
     *
     * <p>分组列如果本身是 {@link Attribute}（例如 {@code GROUP BY department}），复用它的
     * ExprId，保持与输入列的引用关系。聚合函数产生的列（{@code count}、{@code sum} 等）
     * 是新派生的列，自动分配新 ExprId。
     */
    @Override
    public Schema schema() {
        List<Field> fields = new ArrayList<>();
        for (NamedExpression attribute : groupingExpressions) {
            if (attribute instanceof Attribute attr) {
                fields.add(new Field(attr.name(), attr.dataType(), attr.exprId()));
            } else {
                fields.add(new Field(attribute.name(), DataType.OBJECT));
            }
        }
        for (AggregateFunction aggFunc : aggregateExpressions) {
            fields.add(new Field(aggFunc.name(), aggFunc.dataType()));
        }
        return new Schema(fields);
    }

    @Override
    public String nodeName() {
        return "Aggregate";
    }

    @Override
    public String detailString() {
        String groupBy = "groupBy=[" + String.join(", ",
                groupingExpressions.stream().map(NamedExpression::sql).toList()) + "]";
        String aggs = String.join(", ",
                aggregateExpressions.stream().map(AggregateFunction::sql).toList());
        return groupBy + ", " + aggs;
    }
}
