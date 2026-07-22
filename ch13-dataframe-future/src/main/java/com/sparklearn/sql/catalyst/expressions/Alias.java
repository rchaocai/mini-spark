package com.sparklearn.sql.catalyst.expressions;

import com.sparklearn.sql.Row;

import java.util.Set;

/**
 * 给表达式起一个输出列名。
 */
public record Alias(Expression child, String name) implements NamedExpression {

    @Override
    public Object eval(Row row) {
        return child.eval(row);
    }

    @Override
    public Set<String> references() {
        return child.references();
    }

    @Override
    public String sql() {
        return child.sql() + " AS " + name;
    }

    @Override
    public Expression transform(ExpressionRule rule) {
        Expression newChild = child.transform(rule);
        return rule.apply(new Alias(newChild, name));
    }
}
