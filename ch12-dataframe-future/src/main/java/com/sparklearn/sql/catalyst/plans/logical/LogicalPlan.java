package com.sparklearn.sql.catalyst.plans.logical;

import com.sparklearn.sql.Schema;
import com.sparklearn.sql.catalyst.rules.PlanRule;

import java.io.Serializable;
import java.util.List;

/**
 * 逻辑计划：DataFrame 在 action 之前保存的那棵树。
 */
public sealed interface LogicalPlan extends Serializable
        permits Aggregate, Filter, Project, Scan {

    List<LogicalPlan> children();

    LogicalPlan withNewChildren(List<LogicalPlan> children);

    Schema schema();

    String nodeName();

    String detailString();

    default LogicalPlan transformUp(PlanRule rule) {
        List<LogicalPlan> newChildren = children().stream()
                .map(child -> child.transformUp(rule))
                .toList();
        LogicalPlan afterChildren = withNewChildren(newChildren);
        return rule.apply(afterChildren);
    }

    default String treeString() {
        StringBuilder builder = new StringBuilder();
        appendTree(builder, "", true);
        return builder.toString();
    }

    private void appendTree(StringBuilder builder, String indent, boolean root) {
        if (root) {
            builder.append(nodeName()).append("(").append(detailString()).append(")");
        } else {
            builder.append(indent)
                    .append("└── ")
                    .append(nodeName())
                    .append("(")
                    .append(detailString())
                    .append(")");
        }
        List<LogicalPlan> planChildren = children();
        for (LogicalPlan child : planChildren) {
            builder.append(System.lineSeparator());
            child.appendTree(builder, root ? "  " : indent + "    ", false);
        }
    }
}
