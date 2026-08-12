package com.sparklearn.sql.execution;

import com.sparklearn.sql.catalyst.plans.logical.*;

import java.util.List;

/**
 * 把优化后的逻辑计划翻译成物理计划，再把连续的可 codegen 算子融合成一个 WholeStageCodegenExec。
 */
public final class PhysicalPlanner {

    public PhysicalPlan plan(LogicalPlan logicalPlan) {
        PhysicalPlan physicalPlan = doPlan(logicalPlan);
        return collapseCodegenStages(physicalPlan);
    }

    private PhysicalPlan doPlan(LogicalPlan logicalPlan) {
        if (logicalPlan instanceof Scan scan) {
            return new ScanExec(
                    scan.relationName(),
                    scan.rdd(),
                    scan.sourceSchema(),
                    scan.requiredColumns(),
                    scan.pushedFilters());
        }
        if (logicalPlan instanceof Filter filter) {
            return new FilterExec(filter.condition(), doPlan(filter.child()));
        }
        if (logicalPlan instanceof Project project) {
            return new ProjectExec(project.projectList(), doPlan(project.child()));
        }
        if (logicalPlan instanceof Aggregate aggregate) {
            return new HashAggregateExec(
                    aggregate.groupingExpressions(),
                    aggregate.aggregateExpressions(),
                    doPlan(aggregate.child()));
        }
        if (logicalPlan instanceof Join join) {
            return new HashJoinExec(
                    join.condition(),
                    join.joinType(),
                    join.left().schema(),
                    join.right().schema(),
                    doPlan(join.left()),
                    doPlan(join.right()));
        }
        throw new IllegalArgumentException("unknown logical plan: " + logicalPlan);
    }

    /**
     * CollapseCodegenStages：把相邻的、都支持 codegen 的物理算子包进一个 WholeStageCodegenExec。
     *
     * <p>规则：从根往下走，遇到支持 codegen 的算子，且它的 child 也支持 codegen，
     * 就把两者放进同一个 stage（不单独包）；遇到不支持 codegen 的算子（如 HashAggregateExec），
     * 就切断 stage，递归处理 child 时另开一个新 stage。
     */
    private PhysicalPlan collapseCodegenStages(PhysicalPlan plan) {
        return collapse(plan, false);
    }

    private PhysicalPlan collapse(PhysicalPlan plan, boolean inStage) {
        if (plan instanceof CodegenSupport codegen && codegen.supportCodegen()
                && allChildrenCodegen(plan)) {
            PhysicalPlan withCollapsedChildren = withNewChildren(
                    plan, plan.children().stream().map(c -> collapse(c, true)).toList());
            if (inStage) {
                // 在 stage 中间，不单独包
                return withCollapsedChildren;
            }
            // stage 根：包一层 WholeStageCodegenExec
            return new WholeStageCodegenExec((CodegenSupport) withCollapsedChildren);
        }
        // 不支持 codegen（或 child 不支持），切断 stage，child 另开 stage
        return withNewChildren(
                plan, plan.children().stream().map(c -> collapse(c, false)).toList());
    }

    private boolean allChildrenCodegen(PhysicalPlan plan) {
        List<PhysicalPlan> children = plan.children();
        if (children.isEmpty()) {
            return true;
        }
        for (PhysicalPlan child : children) {
            if (!(child instanceof CodegenSupport cs) || !cs.supportCodegen()) {
                return false;
            }
            // 递归检查：child 的整棵子树都要能参与 codegen，
            // 否则中间遇到不支持的算子（如 HashJoinExec）会把 stage 切断
            if (!allChildrenCodegen(child)) {
                return false;
            }
        }
        return true;
    }

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
            return new HashAggregateExec(agg.groupingExpressions(), agg.aggregateExpressions(), newChildren.get(0));
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
