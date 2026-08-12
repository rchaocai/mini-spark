package com.sparklearn.sql.catalyst.plans.logical;

import com.sparklearn.sql.Schema;
import com.sparklearn.sql.catalyst.expressions.Expression;

import java.util.List;
import java.util.Objects;

/**
 * WHERE / filter 条件。
 */
public record Filter(Expression condition, LogicalPlan child) implements LogicalPlan {

    public Filter {
        Objects.requireNonNull(condition, "condition");
        Objects.requireNonNull(child, "child");
    }

    @Override
    public List<LogicalPlan> children() {
        return List.of(child);
    }

    @Override
    public LogicalPlan withNewChildren(List<LogicalPlan> children) {
        return new Filter(condition, children.get(0));
    }

    @Override
    public Schema schema() {
        return child.schema();
    }

    @Override
    public String nodeName() {
        return "Filter";
    }

    @Override
    public String detailString() {
        return condition.sql();
    }
}
