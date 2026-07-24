package com.sparklearn.sql.catalyst.expressions;

import com.sparklearn.sql.DataType;
import com.sparklearn.sql.Row;

import java.util.Set;

/**
 * 常量表达式。
 */
public record Literal(Object value) implements Expression {

    @Override
    public Object eval(Row row) {
        return value;
    }

    @Override
    public Set<String> references() {
        return Set.of();
    }

    @Override
    public String sql() {
        if (value instanceof String text) {
            return "'" + text + "'";
        }
        return String.valueOf(value);
    }

    @Override
    public DataType dataType() {
        return DataType.infer(value);
    }
}
