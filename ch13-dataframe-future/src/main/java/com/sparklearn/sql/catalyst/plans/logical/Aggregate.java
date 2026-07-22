package com.sparklearn.sql.catalyst.plans.logical;

import com.sparklearn.sql.DataType;
import com.sparklearn.sql.Field;
import com.sparklearn.sql.Schema;
import com.sparklearn.sql.catalyst.expressions.Attribute;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * 教学版聚合节点，只实现 groupBy(...).count()。
 */
public record Aggregate(List<Attribute> groupingExpressions, LogicalPlan child)
        implements LogicalPlan {

    public Aggregate {
        groupingExpressions = List.copyOf(groupingExpressions);
        Objects.requireNonNull(child, "child");
    }

    @Override
    public List<LogicalPlan> children() {
        return List.of(child);
    }

    @Override
    public LogicalPlan withNewChildren(List<LogicalPlan> children) {
        return new Aggregate(groupingExpressions, children.get(0));
    }

    @Override
    public Schema schema() {
        List<Field> fields = new ArrayList<>();
        for (Attribute attribute : groupingExpressions) {
            fields.add(new Field(attribute.name(), DataType.OBJECT));
        }
        fields.add(new Field("count", DataType.LONG));
        return new Schema(fields);
    }

    @Override
    public String nodeName() {
        return "Aggregate";
    }

    @Override
    public String detailString() {
        return "groupBy=[" + String.join(", ",
                groupingExpressions.stream().map(Attribute::sql).toList()) + "], count(*)";
    }
}
