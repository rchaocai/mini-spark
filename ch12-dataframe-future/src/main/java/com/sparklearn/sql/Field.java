package com.sparklearn.sql;

import java.io.Serializable;
import java.util.Objects;

/**
 * Schema 里的一个字段。
 */
public record Field(String name, DataType dataType) implements Serializable {

    public Field {
        Objects.requireNonNull(name, "name");
        Objects.requireNonNull(dataType, "dataType");
    }

    public String simpleString() {
        return name + ": " + dataType.simpleName();
    }
}
