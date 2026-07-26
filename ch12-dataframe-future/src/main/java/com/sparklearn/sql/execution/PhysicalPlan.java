package com.sparklearn.sql.execution;

import com.sparklearn.core.RDD;
import com.sparklearn.sql.Row;

import java.io.Serializable;
import java.util.List;

/**
 * 物理计划：真正知道怎么落到 RDD 执行的节点。
 */
public sealed interface PhysicalPlan extends Serializable
        permits HashAggregateExec, FilterExec, ProjectExec, ScanExec {

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
        for (PhysicalPlan child : children()) {
            builder.append(System.lineSeparator());
            child.appendTree(builder, root ? "  " : indent + "    ", false);
        }
    }
}
