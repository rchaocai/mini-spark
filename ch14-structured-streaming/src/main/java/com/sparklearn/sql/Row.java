package com.sparklearn.sql;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * 一行结构化数据。字段顺序由插入顺序决定，便于打印。
 */
public final class Row implements Serializable {

    private final LinkedHashMap<String, Object> values;

    public Row(Map<String, Object> values) {
        Objects.requireNonNull(values, "values");
        this.values = new LinkedHashMap<>(values);
    }

    public static Row of(Object... nameValues) {
        if (nameValues.length % 2 != 0) {
            throw new IllegalArgumentException("nameValues must be name/value pairs");
        }
        LinkedHashMap<String, Object> values = new LinkedHashMap<>();
        for (int index = 0; index < nameValues.length; index += 2) {
            values.put((String) nameValues[index], nameValues[index + 1]);
        }
        return new Row(values);
    }

    public Object get(String name) {
        if (!values.containsKey(name)) {
            throw new IllegalArgumentException("unknown field: " + name);
        }
        return values.get(name);
    }

    public Row select(List<String> names) {
        if (names.isEmpty()) {
            return this;
        }
        LinkedHashMap<String, Object> selected = new LinkedHashMap<>();
        for (String name : names) {
            selected.put(name, get(name));
        }
        return new Row(selected);
    }

    public List<String> fieldNames() {
        return List.copyOf(values.keySet());
    }

    public List<Object> values() {
        return new ArrayList<>(values.values());
    }

    public Map<String, Object> asMap() {
        return Map.copyOf(values);
    }

    @Override
    public String toString() {
        return values.toString();
    }
}
