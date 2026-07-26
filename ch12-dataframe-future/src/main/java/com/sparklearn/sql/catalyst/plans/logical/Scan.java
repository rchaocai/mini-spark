package com.sparklearn.sql.catalyst.plans.logical;

import com.sparklearn.core.RDD;
import com.sparklearn.sql.Field;
import com.sparklearn.sql.Row;
import com.sparklearn.sql.Schema;
import com.sparklearn.sql.catalyst.expressions.Expression;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * 读一个结构化数据源。
 */
public record Scan(
        String relationName,
        Schema sourceSchema,
        RDD<Row> rdd,
        List<String> requiredColumns,
        List<Expression> pushedFilters) implements LogicalPlan {

    public Scan {
        Objects.requireNonNull(relationName, "relationName");
        Objects.requireNonNull(sourceSchema, "sourceSchema");
        Objects.requireNonNull(rdd, "rdd");
        requiredColumns = List.copyOf(requiredColumns);
        pushedFilters = List.copyOf(pushedFilters);
    }

    public Scan(String relationName, Schema sourceSchema, RDD<Row> rdd) {
        this(relationName, sourceSchema, rdd, List.of(), List.of());
    }

    @Override
    public List<LogicalPlan> children() {
        return List.of();
    }

    @Override
    public LogicalPlan withNewChildren(List<LogicalPlan> children) {
        if (!children.isEmpty()) {
            throw new IllegalArgumentException("Scan cannot have children");
        }
        return this;
    }

    @Override
    public Schema schema() {
        if (requiredColumns.isEmpty()) {
            return sourceSchema;
        }
        return sourceSchema.select(requiredColumns);
    }

    @Override
    public String nodeName() {
        return "Scan";
    }

    @Override
    public String detailString() {
        String columns = requiredColumns.isEmpty()
                ? String.join(", ", sourceSchema.fieldNames())
                : String.join(", ", requiredColumns);
        String filters = pushedFilters.isEmpty()
                ? ""
                : ", pushedFilters=["
                + String.join(", ", pushedFilters.stream().map(Expression::sql).toList())
                + "]";
        return relationName + ", columns=[" + columns + "]" + filters;
    }

    public Scan withRequiredColumns(List<String> columns) {
        LinkedHashSet<String> merged = new LinkedHashSet<>(columns);
        for (Expression filter : pushedFilters) {
            merged.addAll(filter.references());
        }
        return new Scan(relationName, sourceSchema, rdd, List.copyOf(merged), pushedFilters);
    }

    public Scan withPushedFilter(Expression condition) {
        List<Expression> filters = new ArrayList<>(pushedFilters);
        filters.add(condition);

        Set<String> columns = new LinkedHashSet<>(requiredColumns);
        columns.addAll(condition.references());
        return new Scan(relationName, sourceSchema, rdd, List.copyOf(columns), filters);
    }

    public Field sourceField(String name) {
        return sourceSchema.field(name);
    }
}
