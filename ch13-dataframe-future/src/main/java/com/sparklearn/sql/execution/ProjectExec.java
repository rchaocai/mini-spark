package com.sparklearn.sql.execution;

import com.sparklearn.core.RDD;
import com.sparklearn.sql.Row;
import com.sparklearn.sql.catalyst.expressions.NamedExpression;

import java.util.LinkedHashMap;
import java.util.List;

/**
 * 投影执行，落成 RDD.map。
 */
public record ProjectExec(List<NamedExpression> projectList, PhysicalPlan child)
        implements PhysicalPlan {

    public ProjectExec {
        projectList = List.copyOf(projectList);
    }

    @Override
    public RDD<Row> execute() {
        return child.execute().map(row -> {
            LinkedHashMap<String, Object> values = new LinkedHashMap<>();
            for (NamedExpression expression : projectList) {
                values.put(expression.name(), expression.eval(row));
            }
            return new Row(values);
        });
    }

    @Override
    public List<PhysicalPlan> children() {
        return List.of(child);
    }

    @Override
    public String nodeName() {
        return "ProjectExec";
    }

    @Override
    public String detailString() {
        return String.join(", ", projectList.stream().map(NamedExpression::sql).toList());
    }
}
