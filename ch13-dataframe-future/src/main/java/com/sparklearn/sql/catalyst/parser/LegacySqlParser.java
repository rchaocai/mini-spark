package com.sparklearn.sql.catalyst.parser;

import com.sparklearn.sql.catalyst.expressions.Count;
import com.sparklearn.sql.catalyst.expressions.EqualTo;
import com.sparklearn.sql.catalyst.expressions.Expression;
import com.sparklearn.sql.catalyst.expressions.GreaterThan;
import com.sparklearn.sql.catalyst.expressions.Literal;
import com.sparklearn.sql.catalyst.expressions.NamedExpression;
import com.sparklearn.sql.catalyst.expressions.UnresolvedAttribute;
import com.sparklearn.sql.catalyst.plans.logical.Aggregate;
import com.sparklearn.sql.catalyst.plans.logical.Filter;
import com.sparklearn.sql.catalyst.plans.logical.LogicalPlan;
import com.sparklearn.sql.catalyst.plans.logical.Project;
import com.sparklearn.sql.catalyst.plans.logical.UnresolvedRelation;

import java.util.ArrayList;
import java.util.List;

/**
 * 手写最小 SQL 解析器（保留兼容用）。
 *
 * <p>与 {@link SparkSqlParser}（ANTLR4 版）并存，产出完全相同的"未解析"计划——
 * FROM 子句产出 {@link UnresolvedRelation}，列引用产出 {@link UnresolvedAttribute}，
 * 元数据绑定交给后面的
 * {@link com.sparklearn.sql.catalyst.analysis.Analyzer Analyzer}。
 *
 * <p>支持 SELECT ... FROM ... [WHERE ...] [GROUP BY ...] + count(*)。
 * 本实现用 Java 手写递归下降，回避外部依赖，覆盖教学所需的子集。
 */
public final class LegacySqlParser implements ParserInterface {

    @Override
    public LogicalPlan parsePlan(String sql) {
        List<String> tokens = tokenize(sql);
        if (tokens.stream().anyMatch(t -> t.equalsIgnoreCase("JOIN"))) {
            throw new ParseException(
                    "LegacySqlParser does not support JOIN; use SparkSqlParser (default) instead");
        }
        return parseSelect(tokens);
    }

    @Override
    public Expression parseExpression(String sql) {
        // 手写版只支持简单条件，不暴露独立表达式解析
        throw new ParseException("LegacySqlParser does not support standalone expression parsing");
    }

    // ---- tokenizer ----

    private List<String> tokenize(String sql) {
        List<String> tokens = new ArrayList<>();
        int i = 0;
        while (i < sql.length()) {
            char c = sql.charAt(i);
            if (Character.isWhitespace(c)) {
                i++;
                continue;
            }
            if (c == ',' || c == '(' || c == ')' || c == '*' || c == '=' || c == '>') {
                tokens.add(String.valueOf(c));
                i++;
                continue;
            }
            if (c == '\'' || c == '"') {
                char quote = c;
                int j = i + 1;
                while (j < sql.length() && sql.charAt(j) != quote) {
                    j++;
                }
                tokens.add(sql.substring(i, j + 1));
                i = j + 1;
                continue;
            }
            int j = i;
            while (j < sql.length() && !isDelimiter(sql.charAt(j))) {
                j++;
            }
            tokens.add(sql.substring(i, j));
            i = j;
        }
        return tokens;
    }

    private boolean isDelimiter(char c) {
        return Character.isWhitespace(c) || c == ',' || c == '(' || c == ')' || c == '*' || c == '=' || c == '>';
    }

    // ---- recursive descent parser ----

    private int pos;

    private LogicalPlan parseSelect(List<String> tokens) {
        pos = 0;
        expect(tokens, "SELECT");

        // 解析 SELECT 列表
        List<String> selectCols = new ArrayList<>();
        boolean hasCountStar = false;
        while (pos < tokens.size() && !isKeyword(tokens.get(pos), "FROM")) {
            String token = tokens.get(pos);
            if (isKeyword(token, "COUNT")) {
                pos++;
                expect(tokens, "(");
                expect(tokens, "*");
                expect(tokens, ")");
                hasCountStar = true;
            } else {
                selectCols.add(token);
                pos++;
            }
            if (pos < tokens.size() && tokens.get(pos).equals(",")) {
                pos++;
            }
        }

        expect(tokens, "FROM");
        String tableName = tokens.get(pos++);

        // 解析 WHERE 条件
        Expression whereCondition = null;
        if (pos < tokens.size() && isKeyword(tokens.get(pos), "WHERE")) {
            pos++;
            whereCondition = parseCondition(tokens);
        }

        // 解析 GROUP BY
        List<String> groupByCols = new ArrayList<>();
        if (pos < tokens.size() && isKeyword(tokens.get(pos), "GROUP")) {
            pos++;
            expect(tokens, "BY");
            while (pos < tokens.size()) {
                groupByCols.add(tokens.get(pos++));
                if (pos < tokens.size() && tokens.get(pos).equals(",")) {
                    pos++;
                }
            }
        }

        return buildPlan(tableName, selectCols, hasCountStar, groupByCols, whereCondition);
    }

    /**
     * 解析简单条件表达式。
     *
     * <p>支持的条件：
     * <ul>
     *   <li>column = value（支持字符串和数字）</li>
     *   <li>column > value（支持数字比较）</li>
     * </ul>
     */
    private Expression parseCondition(List<String> tokens) {
        String left = tokens.get(pos++);
        String op = tokens.get(pos++);

        String right = tokens.get(pos++);
        Object value;
        if (right.startsWith("'") && right.endsWith("'")) {
            value = right.substring(1, right.length() - 1);
        } else {
            try {
                value = Integer.parseInt(right);
            } catch (NumberFormatException e) {
                try {
                    value = Double.parseDouble(right);
                } catch (NumberFormatException e2) {
                    value = right;
                }
            }
        }

        UnresolvedAttribute leftAttr = new UnresolvedAttribute(left);
        Literal rightLiteral = new Literal(value);

        return switch (op) {
            case "=" -> new EqualTo(leftAttr, rightLiteral);
            case ">" -> new GreaterThan(leftAttr, rightLiteral);
            default -> throw new ParseException("unsupported operator: " + op);
        };
    }

    private LogicalPlan buildPlan(String tableName, List<String> selectCols,
                                   boolean hasCountStar, List<String> groupByCols,
                                   Expression whereCondition) {
        // FROM：产出未解析的关系，表名绑定留给 Analyzer
        LogicalPlan plan = new UnresolvedRelation(tableName);

        if (whereCondition != null) {
            plan = new Filter(whereCondition, plan);
        }

        if (!groupByCols.isEmpty() && hasCountStar) {
            List<NamedExpression> groupingAttrs = new ArrayList<>();
            for (String col : groupByCols) {
                groupingAttrs.add(new UnresolvedAttribute(col));
            }
            return new Aggregate(groupingAttrs, List.of(new Count()), plan);
        }

        if (hasCountStar) {
            throw new ParseException("count(*) without GROUP BY is not supported in this chapter");
        }

        if (!selectCols.isEmpty()) {
            List<NamedExpression> projectList = new ArrayList<>();
            for (String col : selectCols) {
                projectList.add(new UnresolvedAttribute(col));
            }
            return new Project(projectList, plan);
        }

        return plan;
    }

    // ---- helpers ----

    private void expect(List<String> tokens, String expected) {
        if (pos >= tokens.size()) {
            throw new ParseException(
                    "expected '" + expected + "' but reached end of input");
        }
        String actual = tokens.get(pos);
        if (!actual.equalsIgnoreCase(expected)) {
            throw new ParseException(
                    "expected '" + expected + "' but got '" + actual + "'");
        }
        pos++;
    }

    private boolean isKeyword(String token, String keyword) {
        return token.equalsIgnoreCase(keyword);
    }
}
