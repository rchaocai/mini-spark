package com.sparklearn.sql;

import com.sparklearn.sql.catalyst.expressions.ExprId;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * DataFrame 的列结构。
 *
 * <p>允许同名列共存——参考 Spark 的 {@code StructType}：schema 是字段列表，
 * 不强制列名唯一。同名区分由 {@link Field#exprId()} 在表达式层完成。
 * 用户层 API（{@link #field(String)}）按列名查找时，若多个同名列存在则报歧义，
 * 要求用 {@code table.col} 限定。
 */
public final class Schema implements Serializable {

    private final List<Field> fields;

    public Schema(List<Field> fields) {
        Objects.requireNonNull(fields, "fields");
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

    /**
     * 按 ExprId 精确查找字段。
     *
     * <p>ExprId 全局唯一，不会有歧义。JOIN 后的 schema 拼接了左右两表所有字段，
     * 同名列靠 ExprId 区分。
     */
    public Field field(ExprId exprId) {
        Objects.requireNonNull(exprId, "exprId");
        for (Field field : fields) {
            if (field.exprId().equals(exprId)) {
                return field;
            }
        }
        throw new IllegalArgumentException("unknown exprId: " + exprId);
    }

    /**
     * 按字段名查找。
     *
     * <p>多个同名列存在时报歧义，要求用 {@code table.col} 限定
     *（参考 Spark 的 {@code AnalysisException ambiguous reference}）。
     */
    public Field field(String name) {
        List<Field> matched = fieldsWithName(name);
        if (matched.isEmpty()) {
            throw new IllegalArgumentException("unknown field: " + name);
        }
        if (matched.size() > 1) {
            throw new IllegalArgumentException(
                    "ambiguous field '" + name + "'; use table-qualified name like 'table." + name + "'");
        }
        return matched.get(0);
    }

    /**
     * 返回所有同名 Field（可能为空、单个或多个）。
     */
    public List<Field> fieldsWithName(String name) {
        List<Field> matched = new ArrayList<>();
        for (Field field : fields) {
            if (field.name().equals(name)) {
                matched.add(field);
            }
        }
        return matched;
    }

    /**
     * 按字段名投影出子 schema。
     *
     * <p>若某个名字在 schema 里出现多次，报歧义。
     */
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
