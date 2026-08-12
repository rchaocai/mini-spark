package com.sparklearn.sql.execution;

import com.sparklearn.core.KeyValuePair;
import com.sparklearn.core.rdd.RDD;
import com.sparklearn.sql.Row;
import com.sparklearn.sql.catalyst.expressions.AggregateFunction;
import com.sparklearn.sql.catalyst.expressions.NamedExpression;

import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Objects;

/**
 * 聚合物理算子。
 *
 * <p>参考 Spark 的 {@code HashAggregateExec}（sql/core/.../execution/aggregate/HashAggregateExec.scala）。
 *
 * <p>Spark 的 {@code HashAggregateExec} 根据 {@code AggregateMode}（Partial / Final / Complete）
 * 决定执行方式：Partial 在 map 端做局部聚合，Final 在 reduce 端做全局聚合。
 *
 * <p>mini-spark 用 {@code reduceByKey} 一步完成：{@code initialize} 产生单行状态，
 * {@code reduceByKey} 内部的 map-side combine 反复 {@code merge} 完成 Partial 聚合，
 * shuffle 后的 reduce 阶段再做 Final 聚合。语义等价于 Partial → Shuffle → Final。
 */
public record HashAggregateExec(
        List<NamedExpression> groupingExpressions,
        List<AggregateFunction> aggregateExpressions,
        PhysicalPlan child) implements PhysicalPlan {

    private static final int DEFAULT_REDUCE_PARTITIONS = 2;

    public HashAggregateExec {
        groupingExpressions = List.copyOf(groupingExpressions);
        aggregateExpressions = List.copyOf(aggregateExpressions);
        Objects.requireNonNull(child, "child");
    }

    @Override
    public RDD<Row> execute() {
        // 每行 → (groupKey, aggStates)
        // aggStates[i] = 第 i 个聚合函数对这行的初始状态
        RDD<KeyValuePair<GroupKey, Object[]>> keyed = child.execute()
                .map(row -> {
                    GroupKey key = GroupKey.from(row, groupingExpressions);
                    Object[] states = new Object[aggregateExpressions.size()];
                    for (int i = 0; i < aggregateExpressions.size(); i++) {
                        states[i] = aggregateExpressions.get(i).initialize(row);
                    }
                    return new KeyValuePair<>(key, states);
                });

        // reduceByKey：map-side combine + shuffle + reduce
        // 对每个聚合函数应用 merge，等价于 Spark 的 Partial → Final 两阶段聚合
        RDD<KeyValuePair<GroupKey, Object[]>> reduced =
                keyed.reduceByKey((acc, input) -> {
                    Object[] result = new Object[acc.length];
                    for (int i = 0; i < result.length; i++) {
                        result[i] = aggregateExpressions.get(i).merge(acc[i], input[i]);
                    }
                    return result;
                }, DEFAULT_REDUCE_PARTITIONS);

        return reduced.map(pair -> pair.key().toRow(pair.value(), aggregateExpressions));
    }

    @Override
    public List<PhysicalPlan> children() {
        return List.of(child);
    }

    public PhysicalPlan withNewChildren(List<PhysicalPlan> children) {
        return new HashAggregateExec(groupingExpressions, aggregateExpressions, children.get(0));
    }

    @Override
    public String nodeName() {
        return "HashAggregateExec";
    }

    @Override
    public String detailString() {
        String groupBy = "groupBy=[" + String.join(", ",
                groupingExpressions.stream().map(NamedExpression::sql).toList()) + "]";
        String aggs = String.join(", ",
                aggregateExpressions.stream().map(AggregateFunction::sql).toList());
        return groupBy + ", " + aggs;
    }

    /**
     * 分组键：一组列值，用作 reduceByKey 的 key。
     */
    private record GroupKey(List<String> names, List<Object> values) implements Serializable {

        static GroupKey from(Row row, List<NamedExpression> groupingExpressions) {
            List<String> names = groupingExpressions.stream().map(NamedExpression::sql).toList();
            List<Object> values = groupingExpressions.stream()
                    .map(attr -> attr.eval(row))
                    .toList();
            return new GroupKey(names, values);
        }

        Row toRow(Object[] aggStates, List<AggregateFunction> aggFuncs) {
            LinkedHashMap<String, Object> result = new LinkedHashMap<>();
            for (int index = 0; index < names.size(); index++) {
                result.put(names.get(index), values.get(index));
            }
            for (int i = 0; i < aggFuncs.size(); i++) {
                result.put(aggFuncs.get(i).name(), aggFuncs.get(i).evaluate(aggStates[i]));
            }
            return Row.of(result);
        }
    }
}
