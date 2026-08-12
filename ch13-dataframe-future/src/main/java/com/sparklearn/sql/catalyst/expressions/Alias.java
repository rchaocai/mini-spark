package com.sparklearn.sql.catalyst.expressions;

import com.sparklearn.sql.DataType;
import com.sparklearn.sql.Row;
import com.sparklearn.sql.execution.CodegenContext;

import java.util.List;
import java.util.Set;

/**
 * 给表达式起一个输出列名。
 *
 * <p>参考 Spark 的 {@code Alias}（sql/catalyst/.../expressions/namedExpressions.scala）：
 * <pre>
 * case class Alias(child: Expression, name: String)(
 *     override val exprId: ExprId = NamedExpression.newExprId,
 *     qualifier: Option[String] = None)
 *   extends UnaryExpression with NamedExpression
 * </pre>
 *
 * <p>Spark 的 Alias 构造时自动分配 exprId，mini-spark 同理。
 */
public record Alias(Expression child, String name, ExprId exprId) implements NamedExpression {

    /** 常用构造：给子表达式起名，自动分配 exprId。 */
    public Alias(Expression child, String name) {
        this(child, name, ExprId.newId());
    }

    @Override
    public Object eval(Row row) {
        return child.eval(row);
    }

    @Override
    public Set<String> references() {
        return child.references();
    }

    @Override
    public String sql() {
        return child.sql() + " AS " + name;
    }

    @Override
    public DataType dataType() {
        return child.dataType();
    }

    @Override
    public Expression transform(ExpressionRule rule) {
        Expression newChild = child.transform(rule);
        return rule.apply(new Alias(newChild, name, exprId));
    }

    @Override
    public String genCode(CodegenContext ctx, String rowVar, List<String> inputFields) {
        // Alias 只是给子表达式起名，生成的求值代码就是子表达式的代码
        return child.genCode(ctx, rowVar, inputFields);
    }
}
