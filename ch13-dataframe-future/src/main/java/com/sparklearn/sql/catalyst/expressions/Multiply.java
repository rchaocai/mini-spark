package com.sparklearn.sql.catalyst.expressions;

import com.sparklearn.sql.Row;

import java.util.LinkedHashSet;
import java.util.Set;

/**
 * 乘法表达式。
 */
public record Multiply(Expression left, Expression right) implements Expression {

    @Override
    public Object eval(Row row) {
        Object leftValue = left.eval(row);
        Object rightValue = right.eval(row);
        if (!(leftValue instanceof Number leftNumber)
                || !(rightValue instanceof Number rightNumber)) {
            throw new IllegalArgumentException("multiply requires numbers");
        }
        return leftNumber.doubleValue() * rightNumber.doubleValue();
    }

    @Override
    public Set<String> references() {
        LinkedHashSet<String> refs = new LinkedHashSet<>(left.references());
        refs.addAll(right.references());
        return Set.copyOf(refs);
    }

    @Override
    public String sql() {
        return left.sql() + " * " + right.sql();
    }

    @Override
    public Expression transform(ExpressionRule rule) {
        return rule.apply(new Multiply(left.transform(rule), right.transform(rule)));
    }
}
