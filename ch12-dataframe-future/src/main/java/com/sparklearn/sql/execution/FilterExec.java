package com.sparklearn.sql.execution;

import com.sparklearn.core.RDD;
import com.sparklearn.sql.Row;
import com.sparklearn.sql.catalyst.expressions.Expression;

import java.util.List;

/**
 * 没有被下推的数据过滤。
 */
public record FilterExec(Expression condition, PhysicalPlan child) implements PhysicalPlan {

    @Override
    public RDD<Row> execute() {
        return child.execute().filter(row -> Boolean.TRUE.equals(condition.eval(row)));
    }

    @Override
    public List<PhysicalPlan> children() {
        return List.of(child);
    }

    @Override
    public String nodeName() {
        return "FilterExec";
    }

    @Override
    public String detailString() {
        return condition.sql();
    }
}
