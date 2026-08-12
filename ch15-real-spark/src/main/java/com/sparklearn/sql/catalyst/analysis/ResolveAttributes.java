package com.sparklearn.sql.catalyst.analysis;

import com.sparklearn.sql.Field;
import com.sparklearn.sql.Schema;
import com.sparklearn.sql.catalyst.expressions.*;
import com.sparklearn.sql.catalyst.plans.logical.*;
import com.sparklearn.sql.catalyst.rules.PlanRule;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * 解析属性规则：把表达式里的 {@link UnresolvedAttribute}（只有列名）换成带类型的 {@link Attribute}。
 *
 * <p>这条规则在 {@link ResolveRelations} 之后运行——先让 {@code UnresolvedRelation}
 * 变成带 schema 的 {@code Scan}，再从 child 的 schema 里查出列的类型和 ExprId，把
 * {@code UnresolvedAttribute("salary")} 换成 {@code Attribute("salary", INTEGER, field.exprId())}。
 *
 * <p>解析范围覆盖四种携带表达式的逻辑节点：
 * <ul>
 *   <li>{@link Filter}：解析 {@code condition}</li>
 *   <li>{@link Project}：解析 {@code projectList} 里的每个 NamedExpression</li>
 *   <li>{@link Aggregate}：解析 {@code groupingExpressions} 和 {@code aggregateExpressions}</li>
 *   <li>{@link Join}：解析 ON {@code condition}，按 qualifier 或列名查找绑定到对应表的 schema；
 *       若左右子树 ExprId 交集非空（self-join），先调 {@code dedupRight} 给右表分配新 ExprId</li>
 * </ul>
 *
 * <p>表达式树的递归由 {@link Expression#transform} 完成：每个表达式节点先变换子表达式，
 * 再把规则应用到自身。规则只关心 {@code UnresolvedAttribute} 这一种节点，其他节点原样返回。
 *
 * <p><b>JOIN 条件解析</b>（参考 Spark 的 {@code ResolveReferences}）：
 * <ul>
 *   <li>限定列名 {@code table.col}：按 qualifier 匹配表名，从对应 schema 解析并绑定 ExprId</li>
 *   <li>无限定列名 {@code col}：先查左表、再查右表；同名列出现在两表时报歧义错，
 *       要求用 {@code table.col} 限定（和 Spark 行为一致）</li>
 * </ul>
 *
 * <p><b>self-join 去重</b>（参考 Spark 的 {@code dedupRight}，Analyzer.scala）：
 * 同一个表自己 join 自己时，左右子树引用同一个 {@link Scan}，ExprId 完全相同。
 * 直接 {@code Join.output = left ++ right} 会导致同一 ExprId 出现两次。
 * 解决办法是给右子树的 {@link Scan} 调 {@link Scan#newInstance()} 重新分配 ExprId，
 * 这样左右两表同名列的 ExprId 不同，可以在 JOIN 输出里区分。
 */
public final class ResolveAttributes implements PlanRule {

    @Override
    public LogicalPlan apply(LogicalPlan plan) {
        if (plan instanceof Filter filter) {
            Schema schema = filter.child().schema();
            Expression resolved = resolveExpression(filter.condition(), schema);
            return new Filter(resolved, filter.child());
        }
        if (plan instanceof Project project) {
            Schema schema = project.child().schema();
            List<NamedExpression> resolved = project.projectList().stream()
                    .map(e -> (NamedExpression) resolveExpression(e, schema))
                    .toList();
            return new Project(resolved, project.child());
        }
        if (plan instanceof Aggregate aggregate) {
            Schema schema = aggregate.child().schema();
            List<NamedExpression> resolvedGrouping = aggregate.groupingExpressions().stream()
                    .map(e -> (NamedExpression) resolveExpression(e, schema))
                    .toList();
            List<AggregateFunction> resolvedAggs = aggregate.aggregateExpressions().stream()
                    .map(e -> (AggregateFunction) resolveExpression(e, schema))
                    .toList();
            return new Aggregate(resolvedGrouping, resolvedAggs, aggregate.child());
        }
        if (plan instanceof Join join) {
            // self-join 检测：左右 schema 的 ExprId 交集非空时，给右子树重新分配 ExprId
            LogicalPlan left = join.left();
            LogicalPlan right = dedupRight(left, join.right());
            Expression resolved = resolveJoinCondition(join.condition(), left, right);
            return new Join(left, right, join.joinType(), resolved);
        }
        return plan;
    }

    /**
     * 在表达式树里把 {@link UnresolvedAttribute} 换成带类型的 {@link Attribute}，
     * 绑定 schema 里对应 Field 的 ExprId。
     *
     * <p>列名在 schema 里出现多次时报歧义（参考 Spark 的 {@code ambiguous reference}）。
     *
     * @param expr   待解析的表达式
     * @param schema child 节点的 schema，用来查列的类型和 ExprId
     * @return 解析后的表达式树
     * @throws IllegalArgumentException 列名在 schema 里找不到 / 出现多次（歧义）时
     */
    private Expression resolveExpression(Expression expr, Schema schema) {
        return expr.transform(e -> {
            if (e instanceof UnresolvedAttribute ua) {
                Field field = schema.field(ua.name());  // 多个同名时报歧义
                return new Attribute(ua.name(), field.dataType(), field.exprId());
            }
            return e;
        });
    }

    /**
     * 解析 JOIN 的 ON 条件：对条件表达式里的每个 {@link UnresolvedAttribute} 独立绑定。
     *
     * <p>绑定策略（参考 Spark {@code ResolveReferences} 按 qualifier 解析的思路）：
     * <ul>
     *   <li>限定列名 {@code employees.dept_id}：按 qualifier {@code "employees"} 在
     *       左右子树找匹配的 {@link Scan}，从其 schema 解析列并绑定 ExprId</li>
     *   <li>无限定列名 {@code dept_id}：先查左表 schema、再查右表 schema；
     *       若两表都有该列，抛歧义异常要求用限定列名</li>
     * </ul>
     *
     * <p>与早期「等号左=左表、右=右表」的位置约定相比，按列名独立解析不依赖等号位置，
     * {@code ON b.x = a.x}（反序）也能正确工作。
     *
     * @param expr      ON 条件表达式
     * @param leftPlan  左子计划（已解析，通常是 {@link Scan}）
     * @param rightPlan 右子计划（已解析，通常是 {@link Scan}）
     * @return 解析后的条件表达式
     */
    private Expression resolveJoinCondition(Expression expr, LogicalPlan leftPlan, LogicalPlan rightPlan) {
        Schema leftSchema = leftPlan.schema();
        Schema rightSchema = rightPlan.schema();
        return expr.transform(e -> {
            if (e instanceof UnresolvedAttribute ua) {
                return resolveJoinAttribute(ua, leftPlan, rightPlan, leftSchema, rightSchema);
            }
            return e;
        });
    }

    /**
     * 解析 JOIN 条件里的单个列引用。
     */
    private Expression resolveJoinAttribute(
            UnresolvedAttribute ua,
            LogicalPlan leftPlan, LogicalPlan rightPlan,
            Schema leftSchema, Schema rightSchema) {
        String qualifier = ua.qualifier();
        String name = ua.name();

        if (qualifier != null) {
            // 限定列名 table.col：按 qualifier 匹配表名选 schema
            Schema matched = findSchemaByTableName(leftPlan, qualifier);
            String side = "left";
            if (matched == null) {
                matched = findSchemaByTableName(rightPlan, qualifier);
                side = "right";
            }
            if (matched == null) {
                throw new IllegalArgumentException(
                        "unknown table qualifier '" + qualifier + "' in: " + ua.sql());
            }
            if (!hasField(matched, name)) {
                throw new IllegalArgumentException(
                        "column '" + name + "' not found in " + side + " table '" + qualifier + "'");
            }
            Field field = matched.field(name);  // 多个同名时报歧义
            return new Attribute(name, field.dataType(), field.exprId());
        }

        // 无限定列名：先查左表、再查右表，同名列报歧义
        boolean inLeft = hasField(leftSchema, name);
        boolean inRight = hasField(rightSchema, name);
        if (inLeft && inRight) {
            throw new IllegalArgumentException(
                    "ambiguous column '" + name + "' in JOIN condition; "
                            + "use table-qualified name like 'table." + name + "'");
        }
        if (inLeft) {
            Field field = leftSchema.field(name);
            return new Attribute(name, field.dataType(), field.exprId());
        }
        if (inRight) {
            Field field = rightSchema.field(name);
            return new Attribute(name, field.dataType(), field.exprId());
        }
        throw new IllegalArgumentException("unknown column '" + name + "' in JOIN condition");
    }

    /**
     * self-join 去重：如果右子树 schema 的 ExprId 与左子树有交集，给右子树所有 {@link Scan}
     * 调 {@link Scan#newInstance()} 重新分配 ExprId。
     *
     * <p>参考 Spark 的 {@code dedupRight}（Analyzer.scala）：
     * <pre>
     * private def dedupRight(left: LogicalPlan, right: LogicalPlan): LogicalPlan = {
     *   val rightDuplication = right.outputSet.intersect(left.outputSet)
     *   if (rightDuplication.nonEmpty) {
     *     right.transformUp { case m: MultiInstanceRelation => m.newInstance() }
     *   } else { right }
     * }
     * </pre>
     *
     * <p>典型场景：{@code SELECT a.name, b.name FROM employees a JOIN employees b ON ...}
     * 左右两侧的 {@code employees} 是同一个 {@link Scan} 实例，ExprId 完全相同。
     * 直接拼到 {@code Join.output} 会出现 ExprId 重复，Attribute.eval(exprId) 无法区分。
     * 调 {@code newInstance()} 后右表 Field 换新 ExprId，左右两表同名列的 ExprId 不同。
     */
    private LogicalPlan dedupRight(LogicalPlan left, LogicalPlan right) {
        Set<ExprId> leftExprIds = collectExprIds(left.schema());
        Set<ExprId> rightExprIds = collectExprIds(right.schema());
        if (Collections.disjoint(leftExprIds, rightExprIds)) {
            return right;
        }
        // 交集非空 → 右子树有重复 ExprId（self-join），递归替换所有 Scan
        return right.transformUp(plan -> {
            if (plan instanceof Scan scan) {
                return scan.newInstance();
            }
            return plan;
        });
    }

    private Set<ExprId> collectExprIds(Schema schema) {
        Set<ExprId> ids = new HashSet<>();
        for (Field field : schema.fields()) {
            ids.add(field.exprId());
        }
        return ids;
    }

    /**
     * 在计划子树里按表名找 {@link Scan} 的 schema。
     *
     * <p>JION 链 {@code FROM a JOIN b JOIN c} 的左子树是 {@code Join(Join(a,b), c)}，
     * 递归遍历找到 {@code relationName} 匹配的 {@link Scan}。
     */
    private Schema findSchemaByTableName(LogicalPlan plan, String tableName) {
        if (plan instanceof Scan scan && scan.relationName().equals(tableName)) {
            return scan.sourceSchema();
        }
        if (plan instanceof Join join) {
            Schema left = findSchemaByTableName(join.left(), tableName);
            if (left != null) {
                return left;
            }
            return findSchemaByTableName(join.right(), tableName);
        }
        return null;
    }

    /** 非抛出式字段查找：schema 里有该字段返回 true。 */
    private boolean hasField(Schema schema, String name) {
        return !schema.fieldsWithName(name).isEmpty();
    }
}
