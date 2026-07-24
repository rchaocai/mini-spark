package com.sparklearn.sql;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * 一行结构化数据。
 *
 * <p>参考 Spark 源码设计：
 * <ul>
 *   <li>Spark 内部使用 {@code InternalRow}，是纯值数组 {@code Object[]}</li>
 *   <li>用户 API 使用 {@code Row}，可以按位置或字段名访问</li>
 *   <li>Schema 是外部描述，与 Row 分离</li>
 * </ul>
 *
 * <p>本教学实现采用混合设计：
 * <ul>
 *   <li>内部存储：{@code Object[]} 数组，符合 Spark 高性能设计</li>
 *   <li>字段名映射：同时维护字段名到索引的映射，便于教学演示</li>
 *   <li>API 兼容：支持 {@code get(int)} 和 {@code get(String)} 两种访问方式</li>
 * </ul>
 */
public final class Row implements Serializable {

    private final Object[] values;
    private final Map<String, Integer> nameToIndex;

    private Row(Object[] values, Map<String, Integer> nameToIndex) {
        this.values = values;
        this.nameToIndex = nameToIndex;
    }

    /**
     * 用位置索引的值序列构造 Row，符合 Spark 源码的核心设计。
     *
     * <p>Spark 源码：{@code Row.apply(values: Any*)}
     * <p>Java API：{@code RowFactory.create(values)}
     *
     * @param values 按位置排列的值，顺序必须与 Schema 的字段顺序一致
     * @return Row 实例
     */
    public static Row apply(Object... values) {
        Objects.requireNonNull(values, "values");
        Map<String, Integer> nameToIndex = new LinkedHashMap<>();
        return new Row(values.clone(), nameToIndex);
    }

    /**
     * 从 List 构造 Row。
     *
     * <p>Spark 源码：{@code Row.fromSeq(values: Seq[Any])}
     */
    public static Row fromSeq(List<Object> values) {
        Objects.requireNonNull(values, "values");
        Map<String, Integer> nameToIndex = new LinkedHashMap<>();
        return new Row(values.toArray(), nameToIndex);
    }

    /**
     * 从 Map 构造 Row，字段顺序由 Map 的迭代顺序决定。
     *
     * <p>这是一个教学便利方法，实际生产中建议用 {@link #apply(Object...)}
     * 配合显式 Schema，避免隐式依赖 Map 顺序。
     */
    public static Row of(Map<String, Object> values) {
        Objects.requireNonNull(values, "values");
        LinkedHashMap<String, Integer> nameToIndex = new LinkedHashMap<>();
        Object[] valueArray = new Object[values.size()];
        int index = 0;
        for (Map.Entry<String, Object> entry : values.entrySet()) {
            nameToIndex.put(entry.getKey(), index);
            valueArray[index] = entry.getValue();
            index++;
        }
        return new Row(valueArray, nameToIndex);
    }

    /**
     * 交替的 name/value 对构造 Row（教学便利方法）。
     *
     * <p>示例：{@code Row.of("id", 1, "name", "Alice")}
     * <p>注意：此方法依赖插入顺序推断字段名，生产中应配合显式 Schema 使用。
     */
    public static Row of(Object... nameValues) {
        if (nameValues.length % 2 != 0) {
            throw new IllegalArgumentException("nameValues must be name/value pairs");
        }
        LinkedHashMap<String, Integer> nameToIndex = new LinkedHashMap<>();
        Object[] valueArray = new Object[nameValues.length / 2];
        for (int index = 0; index < nameValues.length; index += 2) {
            String name = (String) nameValues[index];
            Object value = nameValues[index + 1];
            nameToIndex.put(name, index / 2);
            valueArray[index / 2] = value;
        }
        return new Row(valueArray, nameToIndex);
    }

    /**
     * 按位置索引获取值，符合 Spark 原生 API。
     *
     * <p>Spark 源码：{@code Row.apply(i: Int)}
     * <p>这是最高效的访问方式，优化器最终都会把字段名解析为位置索引。
     */
    public Object get(int index) {
        if (index < 0 || index >= values.length) {
            throw new IndexOutOfBoundsException("index: " + index + ", size: " + values.length);
        }
        return values[index];
    }

    /**
     * 按字段名获取值。
     *
     * <p>教学便利方法。实际执行时，优化器会把字段名解析为位置索引，
     * 最终还是调用 {@link #get(int)}。
     */
    public Object get(String name) {
        Integer index = nameToIndex.get(name);
        if (index == null) {
            throw new IllegalArgumentException("unknown field: " + name);
        }
        return values[index];
    }

    /**
     * 获取字段数量。
     *
     * <p>Spark 源码：{@code Row.length}
     */
    public int length() {
        return values.length;
    }

    /**
     * 获取字段名列表。
     */
    public List<String> fieldNames() {
        return List.copyOf(nameToIndex.keySet());
    }

    /**
     * 获取所有值的 List 视图。
     */
    public List<Object> values() {
        return List.of(values);
    }

    /**
     * 按字段名投影出指定的字段。
     */
    public Row select(List<String> names) {
        if (names.isEmpty()) {
            return this;
        }
        LinkedHashMap<String, Integer> newNameToIndex = new LinkedHashMap<>();
        Object[] selected = new Object[names.size()];
        for (int i = 0; i < names.size(); i++) {
            String name = names.get(i);
            Integer index = nameToIndex.get(name);
            if (index == null) {
                throw new IllegalArgumentException("unknown field: " + name);
            }
            newNameToIndex.put(name, i);
            selected[i] = values[index];
        }
        return new Row(selected, newNameToIndex);
    }

    /**
     * 转换为 Map。
     */
    public Map<String, Object> asMap() {
        LinkedHashMap<String, Object> map = new LinkedHashMap<>();
        for (Map.Entry<String, Integer> entry : nameToIndex.entrySet()) {
            map.put(entry.getKey(), values[entry.getValue()]);
        }
        return Collections.unmodifiableMap(map);
    }

    /**
     * 转换为字符串表示。
     */
    @Override
    public String toString() {
        return asMap().toString();
    }
}
