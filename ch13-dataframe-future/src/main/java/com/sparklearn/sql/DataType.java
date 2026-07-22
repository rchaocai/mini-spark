package com.sparklearn.sql;

/**
 * 教学版 SQL 类型，只保留本章示例会用到的几种。
 */
public enum DataType {
    INTEGER("int"),
    LONG("long"),
    DOUBLE("double"),
    STRING("string"),
    BOOLEAN("boolean"),
    OBJECT("object");

    private final String simpleName;

    DataType(String simpleName) {
        this.simpleName = simpleName;
    }

    public String simpleName() {
        return simpleName;
    }

    public static DataType infer(Object value) {
        if (value instanceof Integer) {
            return INTEGER;
        }
        if (value instanceof Long) {
            return LONG;
        }
        if (value instanceof Float || value instanceof Double) {
            return DOUBLE;
        }
        if (value instanceof Boolean) {
            return BOOLEAN;
        }
        if (value instanceof String) {
            return STRING;
        }
        return OBJECT;
    }
}
