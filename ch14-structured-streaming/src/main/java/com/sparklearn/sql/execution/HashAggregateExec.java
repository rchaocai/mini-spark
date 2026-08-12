package com.sparklearn.sql.execution;

import com.sparklearn.core.KeyValuePair;
import com.sparklearn.core.SparkContext;
import com.sparklearn.core.rdd.RDD;
import com.sparklearn.sql.Row;
import com.sparklearn.sql.catalyst.expressions.AggregateFunction;
import com.sparklearn.sql.catalyst.expressions.NamedExpression;
import com.sparklearn.streaming.structured.state.StateStore;
import com.sparklearn.streaming.structured.state.StateStoreId;
import com.sparklearn.streaming.structured.state.StateStoreManager;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

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
 *
 * <p><b>状态管理</b>：当 {@code stateId} 不为 null 时（流式聚合模式），算子在 reduceByKey
 * 之后增加三个阶段：
 * <ol>
 *   <li>Restore：从 StateStore 恢复上一批次的聚合状态</li>
 *   <li>Merge：将本批次状态与历史状态合并</li>
 *   <li>Save：将合并后的状态保存回 StateStore</li>
 * </ol>
 *
 * <p>这三个阶段对应 Spark 中三个独立的物理算子：
 * <ul>
 *   <li>{@code StateStoreRestoreExec}：恢复历史状态，附加到每行数据后</li>
 *   <li>{@code PartialMerge Aggregate}：将新数据与历史状态合并</li>
 *   <li>{@code StateStoreSaveExec}：保存状态，根据 {@code returnAllStates} 决定输出</li>
 * </ul>
 *
 * <p>mini-spark 将这三个阶段集成在 HashAggregateExec 内部，简化了物理计划结构。
 * {@code returnAllStates} 对应 Spark {@code StateStoreSaveExec} 的同名参数：
 * <ul>
 *   <li>{@code false}（Update 模式）：只输出本批次更新的行</li>
 *   <li>{@code true}（Complete 模式）：输出 StateStore 中的全部行</li>
 * </ul>
 */
public record HashAggregateExec(
        List<NamedExpression> groupingExpressions,
        List<AggregateFunction> aggregateExpressions,
        PhysicalPlan child,
        StateStoreId stateId,
        boolean returnAllStates) implements PhysicalPlan {

    private static final int DEFAULT_REDUCE_PARTITIONS = 2;

    /**
     * 紧凑构造器：复制集合字段，校验非空。
     */
    public HashAggregateExec {
        groupingExpressions = List.copyOf(groupingExpressions);
        aggregateExpressions = List.copyOf(aggregateExpressions);
        Objects.requireNonNull(child, "child");
        // stateId 可以为 null（批处理模式，不使用状态）
    }

    /**
     * 批处理模式构造器（无状态管理）。
     *
     * <p>供 {@link PhysicalPlanner} 在非流式查询中使用。
     */
    public HashAggregateExec(
            List<NamedExpression> groupingExpressions,
            List<AggregateFunction> aggregateExpressions,
            PhysicalPlan child) {
        this(groupingExpressions, aggregateExpressions, child, null, false);
    }

    @Override
    public RDD<Row> execute() {
        // Phase 1: 每行 → (groupKey, aggStates)
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

        // Phase 2: reduceByKey — map-side combine + shuffle + reduce
        // 对每个聚合函数应用 merge，等价于 Spark 的 Partial → Final 两阶段聚合
        RDD<KeyValuePair<GroupKey, Object[]>> reduced =
                keyed.reduceByKey((acc, input) -> {
                    Object[] result = new Object[acc.length];
                    for (int i = 0; i < result.length; i++) {
                        result[i] = aggregateExpressions.get(i).merge(acc[i], input[i]);
                    }
                    return result;
                }, DEFAULT_REDUCE_PARTITIONS);

        // Phase 3: 输出
        if (stateId == null) {
            // 批处理模式：直接 evaluate 并输出
            return reduced.map(pair -> pair.key().toRow(pair.value(), aggregateExpressions));
        } else {
            // 流式模式：状态管理（Restore → Merge → Save → Output）
            return executeWithState(reduced);
        }
    }

    /**
     * 带状态管理的执行路径。
     *
     * <p>对应 Spark 的物理计划：
     * <pre>
     *   StateStoreSaveExec(returnAllStates)
     *     └── PartialMerge Aggregate      ← merge 新数据与历史状态
     *           └── StateStoreRestoreExec  ← 恢复历史状态
     *                 └── PartialMerge Aggregate (shuffle 后)
     *                       └── Partial Aggregate (本批数据)
     * </pre>
     *
     * <p>mini-spark 将上述结构扁平化为 reduceByKey 后的 mapPartitions：
     * <ol>
     *   <li>遍历 reduceByKey 的结果 (key, batchState)</li>
     *   <li>从 StateStore 恢复该 key 的历史状态（对应 StateStoreRestoreExec）</li>
     *   <li>合并历史状态与本批状态（对应 PartialMerge）</li>
     *   <li>保存合并后的状态到 StateStore（对应 StateStoreSaveExec）</li>
     *   <li>根据 returnAllStates 决定输出内容</li>
     * </ol>
     */
    private RDD<Row> executeWithState(RDD<KeyValuePair<GroupKey, Object[]>> reduced) {
        if (returnAllStates) {
            return executeCompleteMode(reduced);
        } else {
            return executeUpdateMode(reduced);
        }
    }

    /**
     * Update 模式：每个分区独立处理自己负责的 key，只输出本批次更新的行。
     *
     * <p>reduceByKey 保证同一 key 只出现在一个分区中，因此各分区输出的行不会重复。
     */
    private RDD<Row> executeUpdateMode(RDD<KeyValuePair<GroupKey, Object[]>> reduced) {
        final StateStoreId finalStateId = stateId;
        final List<AggregateFunction> finalAggFuncs = aggregateExpressions;

        return reduced.mapPartitions(iter -> {
            StateStore store = StateStoreManager.getOrCreate(finalStateId);
            List<Row> output = new ArrayList<>();

            while (iter.hasNext()) {
                KeyValuePair<GroupKey, Object[]> pair = iter.next();
                Object[] batchState = pair.value();
                Row keyRow = pair.key().toKeyRow();

                Optional<Row> prevStateOpt = store.get(keyRow);
                Object[] mergedState = batchState;
                if (prevStateOpt.isPresent()) {
                    Row prevStateRow = prevStateOpt.get();
                    mergedState = new Object[batchState.length];
                    for (int i = 0; i < mergedState.length; i++) {
                        Object prevState = prevStateRow.get(i);
                        mergedState[i] = finalAggFuncs.get(i).merge(prevState, batchState[i]);
                    }
                }

                store.put(keyRow, GroupKey.toStateRow(mergedState));
                output.add(pair.key().toRow(mergedState, finalAggFuncs));
            }

            store.commit();
            return output.iterator();
        });
    }

    /**
     * Complete 模式：先收集所有分区的聚合结果到 Driver，统一做 Restore / Merge / Save，
     * 然后遍历 StateStore 输出全部状态。
     *
     * <p>Complete 模式需要输出 StateStore 中的所有 key，而 StateStore 是跨分区共享的。
     * 如果在 mapPartitions 中遍历 StateStore，每个分区都会输出全部 key，导致重复。
     * 因此 Complete 模式在 Driver 端集中处理，确保 StateStore 只被遍历一次。
     */
    private RDD<Row> executeCompleteMode(RDD<KeyValuePair<GroupKey, Object[]>> reduced) {
        final StateStoreId finalStateId = stateId;
        final List<NamedExpression> finalGroupingExprs = groupingExpressions;
        final List<AggregateFunction> finalAggFuncs = aggregateExpressions;

        List<KeyValuePair<GroupKey, Object[]>> collected = reduced.collect();
        StateStore store = StateStoreManager.getOrCreate(finalStateId);

        for (KeyValuePair<GroupKey, Object[]> pair : collected) {
            Object[] batchState = pair.value();
            Row keyRow = pair.key().toKeyRow();

            Optional<Row> prevStateOpt = store.get(keyRow);
            Object[] mergedState = batchState;
            if (prevStateOpt.isPresent()) {
                Row prevStateRow = prevStateOpt.get();
                mergedState = new Object[batchState.length];
                for (int i = 0; i < mergedState.length; i++) {
                    Object prevState = prevStateRow.get(i);
                    mergedState[i] = finalAggFuncs.get(i).merge(prevState, batchState[i]);
                }
            }
            store.put(keyRow, GroupKey.toStateRow(mergedState));
        }
        store.commit();

        List<Row> output = new ArrayList<>();
        Iterator<Map.Entry<Row, Row>> stateIter = store.iterator();
        while (stateIter.hasNext()) {
            Map.Entry<Row, Row> entry = stateIter.next();
            Row keyRow = entry.getKey();
            Row stateRow = entry.getValue();
            Object[] state = new Object[stateRow.length()];
            for (int i = 0; i < state.length; i++) {
                state[i] = stateRow.get(i);
            }
            output.add(GroupKey.toOutputRow(keyRow, state, finalGroupingExprs, finalAggFuncs));
        }

        SparkContext sc = reduced.sparkContext();
        return sc.parallelize(output, 1);
    }

    @Override
    public List<PhysicalPlan> children() {
        return List.of(child);
    }

    public PhysicalPlan withNewChildren(List<PhysicalPlan> children) {
        return new HashAggregateExec(
                groupingExpressions, aggregateExpressions,
                children.get(0), stateId, returnAllStates);
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
        String state = stateId != null
                ? ", state=" + stateId + ", returnAll=" + returnAllStates
                : "";
        return groupBy + ", " + aggs + state;
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

        /**
         * 转换为 StateStore 的 key Row（仅包含分组列的值）。
         */
        Row toKeyRow() {
            return Row.apply(values.toArray());
        }

        /**
         * 将聚合状态数组包装为 StateStore 的 value Row。
         */
        static Row toStateRow(Object[] states) {
            return Row.apply(states);
        }

        /**
         * 输出行：分组列 + 聚合函数 evaluate 后的值。
         */
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

        /**
         * 从 StateStore 的 key Row 和状态数组构造输出行（Complete 模式用）。
         */
        static Row toOutputRow(Row keyRow, Object[] states,
                               List<NamedExpression> groupingExprs,
                               List<AggregateFunction> aggFuncs) {
            LinkedHashMap<String, Object> result = new LinkedHashMap<>();
            for (int i = 0; i < groupingExprs.size(); i++) {
                result.put(groupingExprs.get(i).sql(), keyRow.get(i));
            }
            for (int i = 0; i < aggFuncs.size(); i++) {
                result.put(aggFuncs.get(i).name(), aggFuncs.get(i).evaluate(states[i]));
            }
            return Row.of(result);
        }
    }
}
