package com.sparklearn.sql;

import com.sparklearn.sql.catalyst.expressions.ExprId;

import java.io.Serializable;
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
 *   <li>字段名映射：维护 {@code name → index} 和 {@code ExprId → index} 两种映射</li>
 *   <li>API 兼容：支持 {@code get(int)} / {@code get(String)} / {@code get(ExprId)} 三种访问方式</li>
 * </ul>
 *
 * <p><b>ExprId 映射</b>：JOIN 输出是 {@code left ++ right}，两表同名列都保留
 *（参考 Spark {@code Join.output}）。同名按位置访问会歧义，按字段名访问在多个同名时
 * 也无法区分，必须用 {@link ExprId} 精确指向。{@link #withSchema(Schema)} 把 schema 的
 * ExprId 绑定到当前行的位置索引，之后 {@link #get(ExprId)} 就能精确取值。
 */
public final class Row implements Serializable {

    private final Object[] values;
    private final Map<String, Integer> nameToIndex;
    private final Map<ExprId, Integer> exprIdToIndex;

    private Row(Object[] values,
                Map<String, Integer> nameToIndex,
                Map<ExprId, Integer> exprIdToIndex) {
        this.values = values;
        this.nameToIndex = nameToIndex;
        this.exprIdToIndex = exprIdToIndex;
    }

    /**
     * 用位置索引的值序列构造 Row，符合 Spark 源码的核心设计。
     *
     * <p>Spark 源码：{@code Row.apply(values: Any*)}
     * <p>Java API：{@code RowFactory.create(values)}
     *
     * <p>构造时只有值，没有字段名/ExprId 映射。后续可通过 {@link #withSchema(Schema)}
     * 绑定 schema，建立 {@code name→index} 和 {@code ExprId→index} 映射。
     *
     * @param values 按位置排列的值，顺序必须与 Schema 的字段顺序一致
     * @return Row 实例
     */
    public static Row apply(Object... values) {
        Objects.requireNonNull(values, "values");
        return new Row(values.clone(), new LinkedHashMap<>(), new LinkedHashMap<>());
    }

    /**
     * 从 List 构造 Row。
     *
     * <p>Spark 源码：{@code Row.fromSeq(values: Seq[Any])}
     */
    public static Row fromSeq(List<Object> values) {
        Objects.requireNonNull(values, "values");
        return new Row(values.toArray(), new LinkedHashMap<>(), new LinkedHashMap<>());
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
        return new Row(valueArray, nameToIndex, new LinkedHashMap<>());
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
        return new Row(valueArray, nameToIndex, new LinkedHashMap<>());
    }

    /**
     * 用 schema 绑定当前行：建立 {@code name→index} 和 {@code ExprId→index} 映射。
     *
     * <p>值数组不变，只是给行"挂上" schema 的字段元数据。要求 schema 的字段顺序
     * 与本行值顺序一致（Scan 读出的行天然满足）。
     *
     * <p>参考 Spark 的 {@code Row.getStruct(i).schema}——Spark 的 Row 持有 schema 引用，
     * 字段访问通过 schema 解析。mini-spark 把映射直接存在 Row 里，避免反复查 schema。
     */
    public Row withSchema(Schema schema) {
        Objects.requireNonNull(schema, "schema");
        List<Field> fields = schema.fields();
        LinkedHashMap<String, Integer> newNameToIndex = new LinkedHashMap<>();
        LinkedHashMap<ExprId, Integer> newExprIdToIndex = new LinkedHashMap<>();
        for (int i = 0; i < fields.size(); i++) {
            Field field = fields.get(i);
            // 同名时保留第一个位置（name 查找在歧义时本就该报错，这里只是兜底）
            newNameToIndex.putIfAbsent(field.name(), i);
            newExprIdToIndex.put(field.exprId(), i);
        }
        return new Row(values, newNameToIndex, newExprIdToIndex);
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
     * <p>教学便利方法。实际执行时，优化器会把字段名解析为位置索引或 ExprId，
     * 最终还是调用 {@link #get(int)} 或 {@link #get(ExprId)}。
     *
     * <p>注意：JOIN 后的行可能有同名列，此时按名取值会拿到第一个匹配。
     * 用户层应通过 {@code table.col} 限定列名避免歧义（由 {@link Schema#field(String)}
     * 在分析阶段报错）。
     */
    public Object get(String name) {
        Integer index = nameToIndex.get(name);
        if (index == null) {
            throw new IllegalArgumentException("unknown field: " + name);
        }
        return values[index];
    }

    /**
     * 按 ExprId 获取值。
     *
     * <p>ExprId 全局唯一，精确指向某一列。JOIN 输出里的同名列靠 ExprId 区分：
     * <pre>
     *   SELECT a.name, b.name FROM employees a JOIN employees b ON ...
     * </pre>
     * 两个 {@code name} 列各有不同 ExprId，{@code Attribute.eval(row)} 用 ExprId 取值。
     */
    public Object get(ExprId exprId) {
        Integer index = exprIdToIndex.get(exprId);
        if (index == null) {
            throw new IllegalArgumentException("unknown exprId: " + exprId);
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
     *
     * <p>投影后的行只保留 nameToIndex，ExprId 映射丢失。需要 ExprId 的场景
     *（如 JOIN 后再投影）应在投影后调用 {@link #withSchema(Schema)} 重新绑定。
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
        return new Row(selected, newNameToIndex, new LinkedHashMap<>());
    }

    /**
     * 合并两行为一行（JOIN 输出用）。
     *
     * <p>参考 Spark {@code Join.output = left.output ++ right.output}：
     * 左表所有字段 + 右表所有字段全部保留，<b>不去重</b>。同名列靠 ExprId 区分。
     *
     * <p>本方法拼接：
     * <ul>
     *   <li>值数组：{@code left.values ++ right.values}</li>
     *   <li>nameToIndex：同名时右表覆盖左表位置（name 查找本就有歧义风险，
     *       分析阶段会要求限定列名）</li>
     *   <li>exprIdToIndex：左右表所有 ExprId 都保留，右表位置加左表长度偏移</li>
     * </ul>
     *
     * @param left  左表行
     * @param right 右表行
     * @return 合并后的行，字段为左表全部字段 + 右表全部字段
     */
    public static Row merge(Row left, Row right) {
        Objects.requireNonNull(left, "left");
        Objects.requireNonNull(right, "right");
        int offset = left.values.length;
        Object[] newValues = new Object[offset + right.values.length];
        System.arraycopy(left.values, 0, newValues, 0, offset);
        System.arraycopy(right.values, 0, newValues, offset, right.values.length);

        LinkedHashMap<String, Integer> newNameToIndex = new LinkedHashMap<>(left.nameToIndex);
        for (Map.Entry<String, Integer> entry : right.nameToIndex.entrySet()) {
            newNameToIndex.put(entry.getKey(), offset + entry.getValue());
        }

        LinkedHashMap<ExprId, Integer> newExprIdToIndex = new LinkedHashMap<>(left.exprIdToIndex);
        for (Map.Entry<ExprId, Integer> entry : right.exprIdToIndex.entrySet()) {
            newExprIdToIndex.put(entry.getKey(), offset + entry.getValue());
        }
        return new Row(newValues, newNameToIndex, newExprIdToIndex);
    }

    /**
     * 构造全 NULL 的行（LEFT OUTER JOIN 右表无匹配时用）。
     *
     * <p>用 schema 的字段元数据建立 nameToIndex 和 exprIdToIndex，
     * 这样左表行 merge nullRight 后，右表列的 ExprId 也能在合并行里查到（值为 null）。
     *
     * @param schema 提供字段名、ExprId 和字段数
     * @return 所有字段为 null 的行
     */
    public static Row nullRow(Schema schema) {
        Objects.requireNonNull(schema, "schema");
        List<Field> fields = schema.fields();
        Object[] nulls = new Object[fields.size()];
        LinkedHashMap<String, Integer> nameToIndex = new LinkedHashMap<>();
        LinkedHashMap<ExprId, Integer> exprIdToIndex = new LinkedHashMap<>();
        for (int i = 0; i < fields.size(); i++) {
            Field field = fields.get(i);
            nulls[i] = null;
            nameToIndex.putIfAbsent(field.name(), i);
            exprIdToIndex.put(field.exprId(), i);
        }
        return new Row(nulls, nameToIndex, exprIdToIndex);
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
