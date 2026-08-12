package com.sparklearn.sql.catalyst.expressions;

import com.sparklearn.sql.DataType;
import com.sparklearn.sql.Row;
import com.sparklearn.sql.execution.CodegenContext;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * 布尔与，用于合并相邻 Filter。
 */
public record And(Expression left, Expression right) implements Expression {

    @Override
    public Object eval(Row row) {
        return Boolean.TRUE.equals(left.eval(row))
                && Boolean.TRUE.equals(right.eval(row));
    }

    @Override
    public Set<String> references() {
        LinkedHashSet<String> refs = new LinkedHashSet<>(left.references());
        refs.addAll(right.references());
        return Set.copyOf(refs);
    }

    @Override
    public String sql() {
        return "(" + left.sql() + ") AND (" + right.sql() + ")";
    }

    @Override
    public DataType dataType() {
        return DataType.BOOLEAN;
    }

    @Override
    public Expression transform(ExpressionRule rule) {
        return rule.apply(new And(left.transform(rule), right.transform(rule)));
    }

    @Override
    public String genCode(CodegenContext ctx, String rowVar, List<String> inputFields) {
        String leftCode = left.genCode(ctx, rowVar, inputFields);
        String rightCode = right.genCode(ctx, rowVar, inputFields);
        return "(" + leftCode + " && " + rightCode + ")";
    }
}
