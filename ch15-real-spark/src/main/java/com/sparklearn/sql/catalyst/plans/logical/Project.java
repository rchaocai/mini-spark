package com.sparklearn.sql.catalyst.plans.logical;

import com.sparklearn.sql.Field;
import com.sparklearn.sql.Schema;
import com.sparklearn.sql.catalyst.expressions.Attribute;
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

    /**
     * 输出 schema：每个投影表达式对应一个 Field。
     *
     * <p>如果表达式是 {@link Attribute}，复用它的 ExprId——这样下游解析时能精确定位
     * 到投影前的列（典型场景：{@code SELECT a.name, b.name FROM ... JOIN ...}，
     * 两个 {@code name} 列各有不同 ExprId）。其他表达式（如 {@code Alias}、
     * 聚合函数等）自动分配新 ExprId。
     */
    @Override
    public Schema schema() {
        return new Schema(projectList.stream()
                .map(expression -> {
                    if (expression instanceof Attribute attr) {
                        return new Field(attr.name(), attr.dataType(), attr.exprId());
                    }
                    return new Field(expression.name(), expression.dataType());
                })
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
