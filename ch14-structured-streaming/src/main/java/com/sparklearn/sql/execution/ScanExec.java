package com.sparklearn.sql.execution;

import com.sparklearn.core.RDD;
import com.sparklearn.sql.Row;
import com.sparklearn.sql.catalyst.expressions.Expression;

import java.util.List;

/**
 * 结构化数据源扫描。
 */
public record ScanExec(
        String relationName,
        RDD<Row> rdd,
        List<String> requiredColumns,
        List<Expression> pushedFilters) implements PhysicalPlan {

    public ScanExec {
        requiredColumns = List.copyOf(requiredColumns);
        pushedFilters = List.copyOf(pushedFilters);
    }

    @Override
    public RDD<Row> execute() {
        RDD<Row> current = rdd;
        for (Expression filter : pushedFilters) {
            current = current.filter(row -> Boolean.TRUE.equals(filter.eval(row)));
        }
        if (!requiredColumns.isEmpty()) {
            current = current.map(row -> row.select(requiredColumns));
        }
        return current;
    }

    @Override
    public List<PhysicalPlan> children() {
        return List.of();
    }

    @Override
    public String nodeName() {
        return "ScanExec";
    }

    @Override
    public String detailString() {
        String columns = requiredColumns.isEmpty()
                ? "*"
                : String.join(", ", requiredColumns);
        String filters = pushedFilters.isEmpty()
                ? ""
                : ", pushedFilters=["
                + String.join(", ", pushedFilters.stream().map(Expression::sql).toList())
                + "]";
        return relationName + ", columns=[" + columns + "]" + filters;
    }
}
