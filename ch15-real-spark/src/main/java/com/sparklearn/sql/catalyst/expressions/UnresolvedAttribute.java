package com.sparklearn.sql.catalyst.expressions;

import com.sparklearn.sql.DataType;
import com.sparklearn.sql.Row;
import com.sparklearn.sql.execution.CodegenContext;

import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * 未解析的列引用：解析阶段只记下列名（可带表名限定），类型和唯一标识留给 Analyzer 绑定。
 *
 * <p>参考 Spark 的 {@code UnresolvedAttribute}（sql/catalyst/.../expressions/namedExpressions.scala）：
 * <pre>
 * case class UnresolvedAttribute(nameParts: Seq[String])
 * </pre>
 * Spark 用 {@code nameParts} 序列建模 {@code table.column} 这种限定列名——
 * {@code Seq("employees", "dept_id")} 表示「employees 表的 dept_id 列」。
 *
 * <p>教学版沿用同样的设计：
 * <ul>
 *   <li>{@code nameParts = ["salary"]}：无限定列名，Analyzer 在 child schema 里按名查找</li>
 *   <li>{@code nameParts = ["employees", "salary"]}：限定列名，Analyzer 按 qualifier
 *       匹配表名后从对应 schema 解析（JOIN 同名列歧义时必需）</li>
 * </ul>
 *
 * <p>SQL 文本里的列名（例如 {@code SELECT salary FROM employees} 中的 {@code salary}）
 * 在解析时还不知道它属于哪张表、是什么类型——这些信息要等 Analyzer 查到表的
 * schema 之后才能确定。所以解析器先产出 {@code UnresolvedAttribute(["salary"])}，
 * Analyzer 再把它换成带类型的 {@link Attribute}。
 *
 * <p>未解析状态下不能求值、不能生成代码，否则说明 Analyzer 漏掉了这一步，
 * 因此相关方法直接抛异常。
 */
public record UnresolvedAttribute(List<String> nameParts) implements NamedExpression {

    /**
     * 紧凑构造：单段列名（最常见情况，{@code SELECT salary}）。
     */
    public UnresolvedAttribute(String name) {
        this(List.of(name));
    }

    public UnresolvedAttribute {
        Objects.requireNonNull(nameParts, "nameParts");
        if (nameParts.isEmpty()) {
            throw new IllegalArgumentException("nameParts must not be empty");
        }
        nameParts = List.copyOf(nameParts);
    }

    /**
     * 列名：{@code nameParts} 的最后一段。
     *
     * <p>{@code ["employees", "salary"]} 的 name 是 {@code "salary"}，
     * {@code ["salary"]} 的 name 也是 {@code "salary"}。
     */
    @Override
    public String name() {
        return nameParts.get(nameParts.size() - 1);
    }

    /**
     * 限定符：{@code nameParts} 的倒数第二段，没有则返回 {@code null}。
     *
     * <p>{@code ["employees", "salary"]} 的 qualifier 是 {@code "employees"}，
     * 表示「employees 表的 salary 列」。{@code ["salary"]} 没有 qualifier。
     *
     * <p>JOIN 场景下同名列歧义时，用户用 {@code table.col} 限定到具体表，
     * Analyzer 据此选对应的 schema 解析。
     */
    public String qualifier() {
        return nameParts.size() >= 2 ? nameParts.get(nameParts.size() - 2) : null;
    }

    @Override
    public DataType dataType() {
        throw new UnsupportedOperationException("UnresolvedAttribute 在 Analyzer 解析前没有类型: " + name());
    }

    @Override
    public ExprId exprId() {
        throw new UnsupportedOperationException("UnresolvedAttribute 在 Analyzer 解析前没有 exprId: " + name());
    }

    @Override
    public Object eval(Row row) {
        throw new UnsupportedOperationException("UnresolvedAttribute 在 Analyzer 解析前不能求值: " + name());
    }

    @Override
    public Set<String> references() {
        return Set.of(name());
    }

    @Override
    public String sql() {
        return String.join(".", nameParts);
    }

    @Override
    public Expression transform(ExpressionRule rule) {
        return rule.apply(this);
    }

    @Override
    public String genCode(CodegenContext ctx, String rowVar, List<String> inputFields) {
        throw new UnsupportedOperationException("UnresolvedAttribute 在 Analyzer 解析前不能生成代码: " + name());
    }
}
