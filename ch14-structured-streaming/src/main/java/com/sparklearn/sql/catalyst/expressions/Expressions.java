package com.sparklearn.sql.catalyst.expressions;

/**
 * DataFrame DSL 入口。
 */
public final class Expressions {

    private Expressions() {
    }

    public static Attribute col(String name) {
        return new Attribute(name);
    }

    public static Literal lit(Object value) {
        return new Literal(value);
    }
}
