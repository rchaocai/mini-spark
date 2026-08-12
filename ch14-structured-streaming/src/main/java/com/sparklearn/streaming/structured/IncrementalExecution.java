package com.sparklearn.streaming.structured;

import com.sparklearn.sql.SQLContext;
import com.sparklearn.sql.catalyst.plans.logical.LogicalPlan;
import com.sparklearn.sql.execution.CodegenSupport;
import com.sparklearn.sql.execution.FilterExec;
import com.sparklearn.sql.execution.HashAggregateExec;
import com.sparklearn.sql.execution.HashJoinExec;
import com.sparklearn.sql.execution.PhysicalPlan;
import com.sparklearn.sql.execution.ProjectExec;
import com.sparklearn.sql.QueryExecution;
import com.sparklearn.sql.execution.ScanExec;
import com.sparklearn.sql.execution.WholeStageCodegenExec;
import com.sparklearn.streaming.structured.state.StateStoreId;

import java.util.List;

/**
 * 流式查询的增量执行器：在普通物理计划上注入状态管理参数。
 *
 * <p>参考 Spark 源码：{@code org.apache.spark.sql.execution.streaming.IncrementalExecution}
 *（sql/core/.../execution/streaming/IncrementalExecution.scala）。
 *
 * <p>Spark 的 IncrementalExecution 继承 {@code QueryExecution}，做两件额外的事：
 * <ol>
 *   <li>注册 {@code StatefulAggregationStrategy}：将逻辑 Aggregate 翻译为带
 *       StateStoreRestoreExec / StateStoreSaveExec 的物理计划</li>
 *   <li>应用 {@code state} 规则：为状态算子注入 {@code stateId}（检查点路径 + 算子编号 + 批次号）
 *       和 {@code returnAllStates}（由 OutputMode 决定）</li>
 * </ol>
 *
 * <p>mini-spark 的实现简化为：先用 {@link SQLContext#executePlan} 生成普通物理计划
 *（Aggregate → HashAggregateExec），再通过 {@link #injectState} 遍历物理计划树，
 * 为每个 HashAggregateExec 注入 {@code stateId} 和 {@code returnAllStates}。
 *
 * <p>{@code returnAllStates} 的取值逻辑与 Spark 完全一致：
 * <pre>
 *   val returnAllStates = if (outputMode == InternalOutputModes.Complete) true else false
 * </pre>
 */
public class IncrementalExecution {

    private final QueryExecution queryExecution;
    private final OutputMode outputMode;
    private final String checkpointLocation;
    private final long batchId;

    /** 状态算子编号计数器，每个批次从 0 开始 */
    private int operatorId = 0;

    /** 注入状态后的物理计划（惰性初始化） */
    private final PhysicalPlan statefulPlan;

    public IncrementalExecution(
            SQLContext sqlContext,
            LogicalPlan logicalPlan,
            OutputMode outputMode,
            String checkpointLocation,
            long batchId) {
        this.queryExecution = sqlContext.executePlan(logicalPlan);
        this.outputMode = outputMode;
        this.checkpointLocation = checkpointLocation;
        this.batchId = batchId;
        this.statefulPlan = injectState(queryExecution.executed());
    }

    /**
     * 返回注入状态后的物理计划。
     */
    public PhysicalPlan executed() {
        return statefulPlan;
    }

    /**
     * 返回原始逻辑计划。
     */
    public LogicalPlan logical() {
        return queryExecution.logical();
    }

    /**
     * 返回优化后的逻辑计划。
     */
    public LogicalPlan optimized() {
        return queryExecution.optimized();
    }

    /**
     * 状态注入规则：遍历物理计划树，为 HashAggregateExec 注入状态参数。
     *
     * <p>对应 Spark IncrementalExecution 中的 {@code state} 规则：
     * <pre>
     * case StateStoreSaveExec(keys, None, None, ...) =>
     *   val stateId = OperatorStateId(checkpointLocation, operatorId, currentBatchId)
     *   val returnAllStates = if (outputMode == Complete) true else false
     *   operatorId += 1
     *   StateStoreSaveExec(keys, Some(stateId), Some(returnAllStates), ...)
     * </pre>
     *
     * <p>Append 模式不注入状态（无聚合状态管理），直接返回原始计划。
     */
    private PhysicalPlan injectState(PhysicalPlan plan) {
        if (outputMode == OutputMode.Append) {
            return plan;
        }
        return transformPlan(plan);
    }

    /**
     * 递归遍历物理计划树，为 HashAggregateExec 注入 stateId 和 returnAllStates。
     */
    private PhysicalPlan transformPlan(PhysicalPlan plan) {
        if (plan instanceof HashAggregateExec hae && hae.stateId() == null) {
            StateStoreId stateId = new StateStoreId(checkpointLocation, operatorId, batchId);
            operatorId++;
            boolean returnAllStates = (outputMode == OutputMode.Complete);

            return new HashAggregateExec(
                    hae.groupingExpressions(),
                    hae.aggregateExpressions(),
                    transformPlan(hae.child()),
                    stateId,
                    returnAllStates);
        }

        List<PhysicalPlan> oldChildren = plan.children();
        if (oldChildren.isEmpty()) {
            return plan;
        }

        List<PhysicalPlan> newChildren = oldChildren.stream()
                .map(this::transformPlan)
                .toList();

        if (newChildren.equals(oldChildren)) {
            return plan;
        }
        return withNewChildren(plan, newChildren);
    }

    /**
     * 用新的子节点重建物理计划节点。
     * 逻辑与 {@link com.sparklearn.sql.execution.PhysicalPlanner#withNewChildren} 一致。
     */
    private PhysicalPlan withNewChildren(PhysicalPlan plan, List<PhysicalPlan> newChildren) {
        if (plan instanceof ScanExec) {
            return plan;
        }
        if (plan instanceof FilterExec filter) {
            return new FilterExec(filter.condition(), newChildren.get(0));
        }
        if (plan instanceof ProjectExec project) {
            return new ProjectExec(project.projectList(), newChildren.get(0));
        }
        if (plan instanceof HashAggregateExec agg) {
            return new HashAggregateExec(agg.groupingExpressions(), agg.aggregateExpressions(),
                    newChildren.get(0), agg.stateId(), agg.returnAllStates());
        }
        if (plan instanceof HashJoinExec join) {
            return new HashJoinExec(join.condition(), join.joinType(),
                    join.leftSchema(), join.rightSchema(),
                    newChildren.get(0), newChildren.get(1));
        }
        if (plan instanceof WholeStageCodegenExec) {
            return new WholeStageCodegenExec((CodegenSupport) newChildren.get(0));
        }
        throw new IllegalArgumentException("unknown plan: " + plan);
    }
}
