package com.sparklearn.sql.catalyst.plans.logical;

import com.sparklearn.sql.DataType;
import com.sparklearn.sql.Field;
import com.sparklearn.sql.Schema;
import com.sparklearn.sql.catalyst.expressions.NamedExpression;

import java.util.List;
import java.util.Objects;

/**
 * SELECT / select 投影。
 */
public record Project(List<NamedExpression> projectList, LogicalPlan child) implements LogicalPlan {

    public Project {
        projectList = List.copyOf(projectList);
        Objects.requireNonNull(child, "child");
    }

    @Override
    public List<LogicalPlan> children() {
        return List.of(child);
    }

    @Override
    public LogicalPlan withNewChildren(List<LogicalPlan> children) {
        return new Project(projectList, children.get(0));
    }

    @Override
    public Schema schema() {
        return new Schema(projectList.stream()
                .map(expression -> new Field(expression.name(), DataType.OBJECT))
                .toList());
    }

    @Override
    public String nodeName() {
        return "Project";
    }

    @Override
    public String detailString() {
        return String.join(", ", projectList.stream().map(NamedExpression::sql).toList());
    }
}
