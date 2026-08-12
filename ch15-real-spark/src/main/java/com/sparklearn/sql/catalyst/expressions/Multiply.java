package com.sparklearn.sql.catalyst.expressions;

import com.sparklearn.sql.DataType;
import com.sparklearn.sql.Row;
import com.sparklearn.sql.execution.CodegenContext;

import java.util.LinkedHashSet;
import java.util.List;
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
    public DataType dataType() {
        return DataType.DOUBLE;
    }

    @Override
    public Expression transform(ExpressionRule rule) {
        return rule.apply(new Multiply(left.transform(rule), right.transform(rule)));
    }

    @Override
    public String genCode(CodegenContext ctx, String rowVar, List<String> inputFields) {
        String leftCode = left.genCode(ctx, rowVar, inputFields);
        String rightCode = right.genCode(ctx, rowVar, inputFields);
        return "(((Number) (" + leftCode + ")).doubleValue() * "
                + "((Number) (" + rightCode + ")).doubleValue())";
    }
}
