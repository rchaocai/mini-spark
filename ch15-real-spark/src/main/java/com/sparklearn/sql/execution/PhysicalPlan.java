package com.sparklearn.sql.execution;

import com.sparklearn.core.rdd.RDD;
import com.sparklearn.sql.Row;

import java.io.Serializable;
import java.util.List;

/**
 * 物理计划：真正知道怎么落到 RDD 执行的节点。
 */
public sealed interface PhysicalPlan extends Serializable
        permits HashAggregateExec, HashJoinExec, CodegenSupport, WholeStageCodegenExec {

    RDD<Row> execute();

    List<PhysicalPlan> children();

    String nodeName();

    String detailString();

    default String treeString() {
        StringBuilder builder = new StringBuilder();
        appendTree(builder, "", true);
        return builder.toString();
    }

    private void appendTree(StringBuilder builder, String indent, boolean root) {
        String detail = detailString();
        String nodeText = detail.isEmpty()
                ? nodeName()
                : nodeName() + "(" + detail + ")";
        if (root) {
            builder.append(nodeText);
        } else {
            builder.append(indent)
                    .append("└── ")
                    .append(nodeText);
        }
        for (PhysicalPlan child : children()) {
            builder.append(System.lineSeparator());
            child.appendTree(builder, root ? "  " : indent + "    ", false);
        }
    }
}
