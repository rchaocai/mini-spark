package com.sparklearn.sql.catalyst.plans.logical;

import com.sparklearn.core.rdd.RDD;
import com.sparklearn.sql.Field;
import com.sparklearn.sql.Row;
import com.sparklearn.sql.Schema;
import com.sparklearn.sql.catalyst.expressions.Expression;

import java.util.*;

/**
 * 读一个结构化数据源。
 *
 * <p>实现 {@code MultiInstanceRelation} 语义——self-join 场景下左右子树会引用同一个 {@code Scan}
 * 实例（同一个表两次出现在 SQL 里）。Spark 的 {@code dedupRight} 会给右表调用
 * {@code newInstance()} 分配新的 {@code ExprId}，避免左右两表的列引用冲突。
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

    /**
     * 创建一个"新实例"：同 relationName / rdd，但 sourceSchema 里每个 Field 换新 ExprId。
     *
     * <p>参考 Spark 的 {@code MultiInstanceRelation.newInstance()}（Analyzer.scala 的
     * {@code dedupRight} 调用）：self-join 时左右表引用同一个 {@code Scan}，ExprId 相同会导致
     * Join 的 {@code duplicateResolved} 检查失败（{@code left.outputSet ∩ right.outputSet} 非空）。
     * 给右表调 {@code newInstance()}，让它的 Field 重新分配 ExprId，左右两表同名列的 ExprId 不同，
     * 才能在 {@code Join.output = left ++ right} 后靠 ExprId 区分。
     */
    public Scan newInstance() {
        List<Field> newFields = sourceSchema.fields().stream()
                .map(Field::newInstance)
                .toList();
        Schema newSchema = new Schema(newFields);
        return new Scan(relationName, newSchema, rdd, requiredColumns, pushedFilters);
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
        return "columns=[" + columns + "]" + filters;
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
