package com.sparklearn.sql.catalyst.parser;

import com.sparklearn.sql.catalyst.analysis.FunctionRegistry;
import com.sparklearn.sql.catalyst.expressions.AggregateFunction;
import com.sparklearn.sql.catalyst.expressions.Alias;
import com.sparklearn.sql.catalyst.expressions.And;
import com.sparklearn.sql.catalyst.expressions.EqualTo;
import com.sparklearn.sql.catalyst.expressions.Expression;
import com.sparklearn.sql.catalyst.expressions.GreaterThan;
import com.sparklearn.sql.catalyst.expressions.GreaterThanOrEqual;
import com.sparklearn.sql.catalyst.expressions.LessThan;
import com.sparklearn.sql.catalyst.expressions.LessThanOrEqual;
import com.sparklearn.sql.catalyst.expressions.Literal;
import com.sparklearn.sql.catalyst.expressions.Multiply;
import com.sparklearn.sql.catalyst.expressions.NamedExpression;
import com.sparklearn.sql.catalyst.expressions.NotEqualTo;
import com.sparklearn.sql.catalyst.expressions.Or;
import com.sparklearn.sql.catalyst.expressions.UnresolvedAttribute;
import com.sparklearn.sql.catalyst.plans.logical.Aggregate;
import com.sparklearn.sql.catalyst.plans.logical.Filter;
import com.sparklearn.sql.catalyst.plans.logical.Join;
import com.sparklearn.sql.catalyst.plans.logical.JoinType;
import com.sparklearn.sql.catalyst.plans.logical.LogicalPlan;
import com.sparklearn.sql.catalyst.plans.logical.Project;
import com.sparklearn.sql.catalyst.plans.logical.UnresolvedRelation;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.RuleNode;
import org.antlr.v4.runtime.tree.TerminalNode;

import java.util.ArrayList;
import java.util.List;

/**
 * ANTLR ParseTree → Catalyst AST 的翻译器。
 *
 * <p>每个 {@code visitXxx} 方法对应一个语法节点，把 ANTLR 生成的 ParseTree 翻译成
 * Catalyst 的 {@link LogicalPlan} / {@link Expression}。翻译阶段只做语法到语义结构的映射，
 * <b>不</b>绑定表和列的真实元数据——FROM 子句产出 {@link UnresolvedRelation}（只有表名），
 * 列引用产出 {@link UnresolvedAttribute}（只有列名），绑定工作交给后面的
 * {@link com.sparklearn.sql.catalyst.analysis.Analyzer Analyzer} 完成。
 *
 * <p>教学简化：
 * <ul>
 *   <li>不支持子查询、CTE、UNION 等复杂语法（JOIN 已支持 INNER / LEFT OUTER）</li>
 *   <li>表达式只覆盖比较/逻辑/算术/字面量/列引用/函数调用</li>
 * </ul>
 */
public class AstBuilder extends SqlBaseBaseVisitor<Object> {

    private final FunctionRegistry functionRegistry;

    public AstBuilder(FunctionRegistry functionRegistry) {
        this.functionRegistry = functionRegistry;
    }

    // ------------------------------------------------------------------------
    // 顶层入口
    // ------------------------------------------------------------------------

    @Override
    public LogicalPlan visitSingleStatement(SqlBaseParser.SingleStatementContext ctx) {
        return (LogicalPlan) visit(ctx.statement());
    }

    @Override
    public Expression visitSingleExpression(SqlBaseParser.SingleExpressionContext ctx) {
        return (Expression) visit(ctx.expression());
    }

    @Override
    public Object visitChildren(RuleNode node) {
        // 默认行为：只有一个子节点时透传，否则返回 null（与 Spark AstBuilder 一致）
        if (node.getChildCount() == 1) {
            return node.getChild(0).accept(this);
        }
        return null;
    }

    // ------------------------------------------------------------------------
    // 语句 / 查询
    // ------------------------------------------------------------------------

    @Override
    public LogicalPlan visitStatement(SqlBaseParser.StatementContext ctx) {
        return (LogicalPlan) visit(ctx.query());
    }

    @Override
    public LogicalPlan visitQuery(SqlBaseParser.QueryContext ctx) {
        return (LogicalPlan) visit(ctx.querySpecification());
    }

    /**
     * 解析 FROM 子句：一个表名后面可以跟任意多个 JOIN。
     *
     * <p>单表产出 {@link UnresolvedRelation}；带 JOIN 时，左侧累积成
     * {@link Join} 节点链（左结合）：
     * <pre>
     * FROM a JOIN b ON ... JOIN c ON ...
     *   → Join(Join(a, b), c)
     * </pre>
     * 每个 JOIN 的 ON 条件在解析阶段只做语法到结构的映射，列类型绑定交给 Analyzer。
     */
    @Override
    public LogicalPlan visitRelation(SqlBaseParser.RelationContext ctx) {
        LogicalPlan left = new UnresolvedRelation(ctx.left.getText());
        for (SqlBaseParser.JoinRelationContext joinCtx : ctx.joinRelation()) {
            LogicalPlan right = new UnresolvedRelation(joinCtx.right.getText());
            Expression condition = (Expression) visit(joinCtx.condition);
            JoinType joinType = joinCtx.joinType() == null
                    ? JoinType.INNER
                    : parseJoinType(joinCtx.joinType());
            left = new Join(left, right, joinType, condition);
        }
        return left;
    }

    private JoinType parseJoinType(SqlBaseParser.JoinTypeContext ctx) {
        if (ctx.LEFT() != null) {
            return JoinType.LEFT_OUTER;
        }
        return JoinType.INNER;
    }

    /**
     * 解析 SELECT ... FROM ... [WHERE ...] [GROUP BY ...]。
     *
     * <p>FROM 子句产出 {@link UnresolvedRelation}（只有表名）或 {@link Join}（JOIN 链），
     * 列引用产出 {@link UnresolvedAttribute}（只有列名）——绑定到真实元数据的工作
     * 交给后面的 {@link com.sparklearn.sql.catalyst.analysis.Analyzer Analyzer}。
     */
    @Override
    public LogicalPlan visitQuerySpecification(SqlBaseParser.QuerySpecificationContext ctx) {
        // 1. FROM：产出未解析的关系（单表或 JOIN 链），表名绑定留给 Analyzer
        LogicalPlan relation = (LogicalPlan) visit(ctx.relation());

        // 2. WHERE
        LogicalPlan withFilter = relation;
        if (ctx.where != null) {
            Expression condition = (Expression) visit(ctx.where);
            withFilter = new Filter(condition, withFilter);
        }

        // 3. SELECT 列表
        List<NamedExpression> selectItems = new ArrayList<>();
        List<AggregateFunction> aggregateFunctions = new ArrayList<>();
        boolean hasStar = false;
        for (SqlBaseParser.NamedExpressionContext namedCtx : ctx.namedExpressionSeq().namedExpression()) {
            Object item = visit(namedCtx);
            if (item instanceof StarMarker) {
                hasStar = true;
            } else if (item instanceof AggregateFunction agg) {
                aggregateFunctions.add(agg);
                // 聚合函数也作为命名表达式出现在 Project 列表里（对应 Spark 的 aggregateExpressions）
                selectItems.add(asNamed(agg));
            } else if (item instanceof NamedExpression ne) {
                selectItems.add(ne);
            } else if (item instanceof Expression expr) {
                selectItems.add(asNamed(expr));
            }
        }

        // 4. GROUP BY
        SqlBaseParser.AggregationContext aggregation = ctx.aggregation();
        if (aggregation != null) {
            // 聚合分支：groupingExpressions 来自 GROUP BY，aggregateExpressions 来自 SELECT
            List<NamedExpression> groupingExpressions = new ArrayList<>();
            for (SqlBaseParser.ExpressionContext groupCtx : aggregation.groupingExpressions) {
                Expression groupExpr = (Expression) visit(groupCtx);
                groupingExpressions.add(asNamed(groupExpr));
            }
            if (aggregateFunctions.isEmpty()) {
                throw new ParseException("GROUP BY requires aggregate functions in SELECT list");
            }
            return new Aggregate(groupingExpressions, aggregateFunctions, withFilter);
        }

        // 5. 非聚合分支
        if (!aggregateFunctions.isEmpty()) {
            throw new ParseException("aggregate functions require GROUP BY in this chapter");
        }

        if (hasStar && selectItems.isEmpty()) {
            // SELECT * —— 直接返回表计划，不包 Project
            return withFilter;
        }

        if (selectItems.isEmpty()) {
            return withFilter;
        }

        return new Project(selectItems, withFilter);
    }

    /** 把任意表达式包成 NamedExpression（如果是 NamedExpression 则直接返回）。 */
    private NamedExpression asNamed(Expression expr) {
        if (expr instanceof NamedExpression ne) {
            return ne;
        }
        return new Alias(expr, expr.sql());
    }

    // ------------------------------------------------------------------------
    // SELECT 项（带别名）
    // ------------------------------------------------------------------------

    @Override
    public Object visitNamedExpression(SqlBaseParser.NamedExpressionContext ctx) {
        Object expr = visit(ctx.expression());
        if (ctx.alias != null) {
            String aliasName = ctx.alias.getText();
            if (expr instanceof Expression e) {
                return new Alias(e, aliasName);
            }
        }
        return expr;
    }

    // ------------------------------------------------------------------------
    // 表达式
    // ------------------------------------------------------------------------

    @Override
    public Expression visitLogicalBinary(SqlBaseParser.LogicalBinaryContext ctx) {
        Expression left = (Expression) visit(ctx.left);
        Expression right = (Expression) visit(ctx.right);
        return switch (ctx.operator.getType()) {
            case SqlBaseParser.AND -> new And(left, right);
            case SqlBaseParser.OR -> new Or(left, right);
            default -> throw new ParseException("unsupported logical operator: " + ctx.operator.getText());
        };
    }

    @Override
    public Expression visitLogicalNot(SqlBaseParser.LogicalNotContext ctx) {
        throw new ParseException("NOT is not supported in this chapter");
    }

    @Override
    public Expression visitComparison(SqlBaseParser.ComparisonContext ctx) {
        Expression left = (Expression) visit(ctx.left);
        Expression right = (Expression) visit(ctx.right);
        TerminalNode opNode = (TerminalNode) ctx.comparisonOperator().getChild(0);
        return switch (opNode.getSymbol().getType()) {
            case SqlBaseParser.EQ -> new EqualTo(left, right);
            case SqlBaseParser.NEQ, SqlBaseParser.NEQJ -> new NotEqualTo(left, right);
            case SqlBaseParser.LT -> new LessThan(left, right);
            case SqlBaseParser.LTE -> new LessThanOrEqual(left, right);
            case SqlBaseParser.GT -> new GreaterThan(left, right);
            case SqlBaseParser.GTE -> new GreaterThanOrEqual(left, right);
            default -> throw new ParseException("unsupported comparison: " + ctx.getText());
        };
    }

    @Override
    public Expression visitArithmeticBinary(SqlBaseParser.ArithmeticBinaryContext ctx) {
        Expression left = (Expression) visit(ctx.left);
        Expression right = (Expression) visit(ctx.right);
        return switch (ctx.operator.getType()) {
            case SqlBaseParser.ASTERISK -> new Multiply(left, right);
            // 教学版只实现了 Multiply，其他算术运算先抛异常
            case SqlBaseParser.PLUS, SqlBaseParser.MINUS, SqlBaseParser.SLASH, SqlBaseParser.PERCENT ->
                    throw new ParseException("arithmetic operator '" + ctx.operator.getText()
                            + "' is not implemented yet (only '*' is supported)");
            default -> throw new ParseException("unsupported arithmetic: " + ctx.getText());
        };
    }

    @Override
    public Expression visitArithmeticUnary(SqlBaseParser.ArithmeticUnaryContext ctx) {
        throw new ParseException("unary arithmetic is not supported in this chapter");
    }

    @Override
    public Object visitStar(SqlBaseParser.StarContext ctx) {
        // SELECT * —— 用一个标记对象表示，visitQuerySpecification 会特殊处理
        return new StarMarker();
    }

    @Override
    public Object visitFunctionCall(SqlBaseParser.FunctionCallContext ctx) {
        String funcName = ctx.functionName.getText().toLowerCase();
        List<Expression> args = new ArrayList<>();
        if (ctx.expression() != null) {
            for (ParseTree argCtx : ctx.expression()) {
                Object arg = visit(argCtx);
                if (arg instanceof StarMarker) {
                    // count(*) —— 等价于 count(1)，用 Literal(1) 作为占位参数
                    args.add(new Literal(1));
                } else if (arg instanceof Expression e) {
                    args.add(e);
                }
            }
        }
        // 通过 FunctionRegistry 查找函数构造器，而非硬编码 switch
        return functionRegistry.lookupFunction(funcName, args);
    }

    @Override
    public Expression visitColumnReference(SqlBaseParser.ColumnReferenceContext ctx) {
        // 解析限定列名：单段 "salary" 或多段 "employees.salary"
        // 每段 identifier 复用已有的 visitUnquotedIdentifier / visitQuotedIdentifier
        List<String> nameParts = new ArrayList<>();
        for (SqlBaseParser.IdentifierContext idCtx : ctx.multipartIdentifier().identifier()) {
            nameParts.add((String) visit(idCtx));
        }
        return new UnresolvedAttribute(nameParts);
    }

    @Override
    public Object visitParenthesizedExpression(SqlBaseParser.ParenthesizedExpressionContext ctx) {
        return visit(ctx.expression());
    }

    // ------------------------------------------------------------------------
    // 字面量
    // ------------------------------------------------------------------------

    @Override
    public Expression visitConstantDefault(SqlBaseParser.ConstantDefaultContext ctx) {
        return (Expression) visit(ctx.constant());
    }

    @Override
    public Expression visitNullLiteral(SqlBaseParser.NullLiteralContext ctx) {
        return new Literal(null);
    }

    @Override
    public Expression visitBooleanLiteral(SqlBaseParser.BooleanLiteralContext ctx) {
        boolean value = ctx.booleanValue().getText().equalsIgnoreCase("TRUE");
        return new Literal(value);
    }

    @Override
    public Expression visitStringLiteral(SqlBaseParser.StringLiteralContext ctx) {
        String text = ctx.STRING().getText();
        // 去掉外层引号（支持单引号和双引号）
        String unquoted = text.substring(1, text.length() - 1);
        return new Literal(unquoted);
    }

    @Override
    public Expression visitNumericLiteral(SqlBaseParser.NumericLiteralContext ctx) {
        return (Expression) visit(ctx.number());
    }

    @Override
    public Expression visitIntegerLiteral(SqlBaseParser.IntegerLiteralContext ctx) {
        return new Literal(Integer.parseInt(ctx.INTEGER_VALUE().getText()));
    }

    @Override
    public Expression visitDecimalLiteral(SqlBaseParser.DecimalLiteralContext ctx) {
        return new Literal(Double.parseDouble(ctx.DECIMAL_VALUE().getText()));
    }

    // ------------------------------------------------------------------------
    // 标识符 / 工具
    // ------------------------------------------------------------------------

    @Override
    public Object visitUnquotedIdentifier(SqlBaseParser.UnquotedIdentifierContext ctx) {
        return ctx.getText();
    }

    @Override
    public Object visitQuotedIdentifierAlternative(SqlBaseParser.QuotedIdentifierAlternativeContext ctx) {
        return visit(ctx.quotedIdentifier());
    }

    @Override
    public Object visitQuotedIdentifier(SqlBaseParser.QuotedIdentifierContext ctx) {
        // 去掉反引号
        return ctx.BACKQUOTED_IDENTIFIER().getText().replaceAll("^`|`$", "");
    }

    /** 内部标记：SELECT * 中的星号。 */
    private static final class StarMarker {
    }
}
