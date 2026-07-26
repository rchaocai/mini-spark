package com.sparklearn.sql.catalyst.expressions;

import com.sparklearn.sql.DataType;
import com.sparklearn.sql.Row;

import java.util.LinkedHashSet;
import java.util.Objects;
import java.util.Set;

/**
 * 相等比较。
 */
public record EqualTo(Expression left, Expression right) implements Expression {

    @Override
    public Object eval(Row row) {
        return Objects.equals(left.eval(row), right.eval(row));
    }

    @Override
    public Set<String> references() {
        LinkedHashSet<String> refs = new LinkedHashSet<>(left.references());
        refs.addAll(right.references());
        return Set.copyOf(refs);
    }

    @Override
    public String sql() {
        return left.sql() + " = " + right.sql();
    }

    @Override
    public DataType dataType() {
        return DataType.BOOLEAN;
    }

    @Override
    public Expression transform(ExpressionRule rule) {
        return rule.apply(new EqualTo(left.transform(rule), right.transform(rule)));
    }
}
