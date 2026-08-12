package com.sparklearn.sql.catalyst.expressions;

import com.sparklearn.sql.DataType;
import com.sparklearn.sql.Row;
import com.sparklearn.sql.execution.CodegenContext;

import java.util.List;
import java.util.Set;

/**
 * 表达式树的叶子节点：引用输入行中的某一列。
 *
 * <p>参考 Spark 的 {@code AttributeReference}（sql/catalyst/.../expressions/namedExpressions.scala）：
 * <pre>
 * case class AttributeReference(name: String, dataType: DataType, nullable: Boolean = true,
 *                               override val exprId: ExprId = NamedExpression.newExprId)
 *   extends Attribute with LeafExpression
 * </pre>
 *
 * <p>Spark 用 {@code exprId} 做引用解析和列区分（self-join 场景关键）。
 * JOIN 输出是 {@code left.output ++ right.output}，两表同名列都保留，
 * {@code eval} 必须用 {@code exprId} 精确取值，否则同名时会取到错误的列。
 *
 * <p>{@code references()} 仍按字段名匹配——优化器做列裁剪、谓词下推时按列名传递
 *（参考 Spark 的 {@code references} 实现），单表内列名唯一，无需 ExprId。
 */
public record Attribute(String name, DataType dataType, ExprId exprId) implements NamedExpression {

    /**
     * 推断类型的构造：从字段名和数据类型构造，自动分配 exprId。
     */
    public Attribute(String name, DataType dataType) {
        this(name, dataType, ExprId.newId());
    }

    /**
     * 最常用的构造：只给字段名，数据类型默认 OBJECT（本章不做强类型检查）。
     */
    public Attribute(String name) {
        this(name, DataType.OBJECT);
    }

    /**
     * 用 ExprId 精确取值；ExprId 未绑定时退回按字段名取值（兼容未走 {@code withSchema} 的行）。
     */
    @Override
    public Object eval(Row row) {
        try {
            return row.get(exprId);
        } catch (IllegalArgumentException notBound) {
            return row.get(name);
        }
    }

    @Override
    public Set<String> references() {
        return Set.of(name);
    }

    @Override
    public String sql() {
        return name;
    }

    @Override
    public Expression transform(ExpressionRule rule) {
        return rule.apply(this);
    }

    /**
     * 生成按位置索引取值的代码。
     *
     * <p>codegen 路径不直接用 ExprId——inputFields 是上游算子传下来的列名列表，
     * 按列名查位置索引。同名列场景下 codegen 不参与（{@code HashJoinExec} 不实现
     * {@code CodegenSupport}，会切断 codegen stage），所以这里按列名查找是安全的。
     */
    @Override
    public String genCode(CodegenContext ctx, String rowVar, List<String> inputFields) {
        int index = inputFields.indexOf(name);
        if (index < 0) {
            // 兜底：按列名从 Row 取值（codegen 路径不应走到这里）
            return rowVar + ".get(\"" + name + "\")";
        }
        return rowVar + ".get(" + index + ")";
    }
}
