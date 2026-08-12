package com.sparklearn.sql.catalyst.expressions;

import java.io.Serializable;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 表达式的全局唯一标识。
 *
 * <p>参考 Spark 的 {@code ExprId}（sql/catalyst/.../expressions/namedExpressions.scala）：
 * <pre>
 * case class ExprId(id: Long, jvmId: UUID)
 * </pre>
 *
 * <p>Spark 用 {@code jvmId} 区分不同 JVM 生成的 ExprId（分布式环境下多台机器各自自增）。
 * mini-spark 是单机实现，只用一个自增 {@code long} 就够。
 *
 * <p>为什么需要 exprId？考虑 self-join：
 * <pre>
 * SELECT a.name, b.name
 * FROM employees a JOIN employees b ON a.department = b.department
 * </pre>
 * 两张表都叫 {@code employees}，都有 {@code name} 列。光靠字段名字符串无法区分
 * "a.name" 和 "b.name"。给每个 {@code AttributeReference} 分配全局唯一的
 * {@code ExprId}，这样即使名字相同，{@code exprId} 不同也能区分。
 *
 * <p>{@code ExprId} 存在于表达式层（{@link Attribute} 和 {@link Alias}）和
 * {@code Schema} 层（{@link com.sparklearn.sql.Field Field}），用于区分同名列引用。
 * JOIN 输出是 {@code left.output ++ right.output}，两表同名列都保留，靠 {@code ExprId} 区分。
 */
public record ExprId(long id) implements Serializable {

    private static final AtomicLong NEXT = new AtomicLong(0);

    /** 分配一个新的全局唯一 ExprId。 */
    public static ExprId newId() {
        return new ExprId(NEXT.getAndIncrement());
    }

    @Override
    public String toString() {
        return "#" + id;
    }
}
