package com.sparklearn.sql.catalyst.expressions;

import com.sparklearn.sql.DataType;
import com.sparklearn.sql.Row;
import com.sparklearn.sql.execution.CodegenContext;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * 不等于比较。
 *
 * <p>对应 Spark 的 {@code Not(EqualTo(...))}（sql/catalyst/.../expressions/predicates.scala）。
 * Spark 用 {@code Not} 包装 {@code EqualTo}，mini-spark 直接实现一个 record 简化代码。
 */
public record NotEqualTo(Expression left, Expression right) implements Expression {

    @Override
    public Object eval(Row row) {
        return !Objects.equals(left.eval(row), right.eval(row));
    }

    @Override
    public Set<String> references() {
        LinkedHashSet<String> refs = new LinkedHashSet<>(left.references());
        refs.addAll(right.references());
        return Set.copyOf(refs);
    }

    @Override
    public String sql() {
        return left.sql() + " <> " + right.sql();
    }

    @Override
    public DataType dataType() {
        return DataType.BOOLEAN;
    }

    @Override
    public Expression transform(ExpressionRule rule) {
        return rule.apply(new NotEqualTo(left.transform(rule), right.transform(rule)));
    }

    @Override
    public String genCode(CodegenContext ctx, String rowVar, List<String> inputFields) {
        String leftCode = left.genCode(ctx, rowVar, inputFields);
        String rightCode = right.genCode(ctx, rowVar, inputFields);
        return "(!java.util.Objects.equals(" + leftCode + ", " + rightCode + "))";
    }
}
