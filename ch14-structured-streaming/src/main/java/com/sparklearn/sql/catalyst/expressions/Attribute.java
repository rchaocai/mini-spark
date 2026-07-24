package com.sparklearn.sql.catalyst.expressions;

import com.sparklearn.sql.DataType;
import com.sparklearn.sql.Row;

import java.util.Set;

/**
 * 输入行里的一个列引用。
 */
public record Attribute(String name, DataType dataType) implements NamedExpression {

    public Attribute(String name) {
        this(name, DataType.OBJECT);
    }

    @Override
    public Object eval(Row row) {
        return row.get(name);
    }

    @Override
    public Set<String> references() {
        return Set.of(name);
    }

    @Override
    public String sql() {
        return name;
    }

    @Override
    public DataType dataType() {
        return dataType;
    }
}