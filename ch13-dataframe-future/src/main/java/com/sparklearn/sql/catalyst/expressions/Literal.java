package com.sparklearn.sql.catalyst.expressions;

import com.sparklearn.sql.DataType;
import com.sparklearn.sql.Row;
import com.sparklearn.sql.execution.CodegenContext;

import java.util.List;
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

    @Override
    public String genCode(CodegenContext ctx, String rowVar, List<String> inputFields) {
        if (value instanceof String text) {
            return "\"" + text + "\"";
        }
        if (value instanceof Boolean booleanValue) {
            return String.valueOf(booleanValue);
        }
        // 数字直接写字面量，由调用方在外层包 (Number) 强转
        if (value instanceof Number) {
            return String.valueOf(value);
        }
        return String.valueOf(value);
    }
}
