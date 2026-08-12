package com.sparklearn.sql;

import com.sparklearn.sql.catalyst.expressions.ExprId;

import java.io.Serializable;
import java.util.Objects;

/**
 * Schema 里的一个字段。
 *
 * <p>每个字段带一个全局唯一的 {@link ExprId}，用于区分同名列。
 * 参考 Spark 的 {@code AttributeReference.exprId}——JOIN 输出是
 * {@code left.output ++ right.output}，两表的列全部保留，靠 ExprId 区分。
 */
public record Field(String name, DataType dataType, ExprId exprId) implements Serializable {

    public Field {
        Objects.requireNonNull(name, "name");
        Objects.requireNonNull(dataType, "dataType");
        Objects.requireNonNull(exprId, "exprId");
    }

    /**
     * 便利构造：自动分配新的 ExprId。
     */
    public Field(String name, DataType dataType) {
        this(name, dataType, ExprId.newId());
    }

    /**
     * 创建同名列、同类型但带新 ExprId 的副本（self-join 去重用）。
     */
    public Field newInstance() {
        return new Field(name, dataType, ExprId.newId());
    }

    public String simpleString() {
        return name + ": " + dataType.simpleName();
    }
}
