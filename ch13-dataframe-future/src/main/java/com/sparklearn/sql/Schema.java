package com.sparklearn.sql;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * DataFrame 的列结构。
 */
public final class Schema implements Serializable {

    private final List<Field> fields;

    public Schema(List<Field> fields) {
        Objects.requireNonNull(fields, "fields");
        Set<String> names = new LinkedHashSet<>();
        for (Field field : fields) {
            if (!names.add(field.name())) {
                throw new IllegalArgumentException("duplicate field: " + field.name());
            }
        }
        this.fields = List.copyOf(fields);
    }

    public static Schema of(Field... fields) {
        return new Schema(List.of(fields));
    }

    public static Schema inferFrom(Row row) {
        List<Field> fields = new ArrayList<>();
        for (String name : row.fieldNames()) {
            fields.add(new Field(name, DataType.infer(row.get(name))));
        }
        return new Schema(fields);
    }

    public List<Field> fields() {
        return fields;
    }

    public List<String> fieldNames() {
        return fields.stream().map(Field::name).toList();
    }

    public Field field(String name) {
        for (Field field : fields) {
            if (field.name().equals(name)) {
                return field;
            }
        }
        throw new IllegalArgumentException("unknown field: " + name);
    }

    public Schema select(List<String> names) {
        List<Field> selected = new ArrayList<>();
        for (String name : names) {
            selected.add(field(name));
        }
        return new Schema(selected);
    }

    public String simpleString() {
        return fields.stream().map(Field::simpleString).toList().toString();
    }
}
