package com.sparklearn.sql;

import com.sparklearn.sql.catalyst.expressions.Attribute;
import com.sparklearn.sql.catalyst.expressions.EqualTo;
import com.sparklearn.sql.catalyst.expressions.Expression;
import com.sparklearn.sql.catalyst.expressions.GreaterThan;
import com.sparklearn.sql.catalyst.expressions.Literal;
import com.sparklearn.sql.catalyst.plans.logical.Aggregate;
import com.sparklearn.sql.catalyst.plans.logical.Filter;
import com.sparklearn.sql.catalyst.plans.logical.LogicalPlan;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * 手写最小 SQL 解析器，支持 SELECT ... FROM ... [WHERE ...] [GROUP BY ...] + count(*)。
 * <p>
 * Spark 1.3 起用 Scala 的 parser combinator（{@code StandardTokenParsers}）解析 SQL，
 * 核心入口在 {@code SqlParser.scala}。本实现用 Java 手写递归下降，回避外部依赖，
 * 覆盖教学所需的子集。
 * <p>
 * 参考 Spark 源码：
 * <ul>
 *   <li>branch-1.3: {@code sql/catalyst/.../SqlParser.scala}</li>
 *   <li>branch-2.0: Structured Streaming 引入，SQL 解析沿用同一套基础</li>
 * </ul>
 */
public final class SqlParser {

    private final Map<String, LogicalPlan> tableRegistry;

    public SqlParser(Map<String, LogicalPlan> tableRegistry) {
        this.tableRegistry = tableRegistry;
    }

    /**
     * 解析 SQL 字符串，返回 LogicalPlan。
     * <p>
     * 支持的语法：
     * <pre>
     * SELECT column [, column] FROM tableName
     * SELECT column [, column] FROM tableName WHERE condition
     * SELECT column [, count(*)] FROM tableName GROUP BY column
     * SELECT column [, count(*)] FROM tableName WHERE condition GROUP BY column
     * </pre>
     *
     * <p>支持的 WHERE 条件：
     * <ul>
     *   <li>column = value（字符串用单引号）</li>
     *   <li>column > value（数字比较）</li>
     * </ul>
     */
    public LogicalPlan parse(String sql) {
        List<String> tokens = tokenize(sql);
        return parseSelect(tokens);
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
     * <p>
     * 支持的条件：
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

        Attribute leftAttr = new Attribute(left);
        Literal rightLiteral = new Literal(value);

        return switch (op) {
            case "=" -> new EqualTo(leftAttr, rightLiteral);
            case ">" -> new GreaterThan(leftAttr, rightLiteral);
            default -> throw new UnsupportedOperationException("unsupported operator: " + op);
        };
    }

    private LogicalPlan buildPlan(String tableName, List<String> selectCols,
                                   boolean hasCountStar, List<String> groupByCols,
                                   Expression whereCondition) {
        LogicalPlan tablePlan = tableRegistry.get(tableName);
        if (tablePlan == null) {
            throw new IllegalArgumentException("table not found: " + tableName);
        }

        LogicalPlan plan = tablePlan;

        if (whereCondition != null) {
            plan = new Filter(whereCondition, plan);
        }

        if (!groupByCols.isEmpty() && hasCountStar) {
            List<Attribute> groupingAttrs = new ArrayList<>();
            for (String col : groupByCols) {
                groupingAttrs.add(new Attribute(col));
            }
            return new Aggregate(groupingAttrs, plan);
        }

        return plan;
    }

    // ---- helpers ----

    private void expect(List<String> tokens, String expected) {
        if (pos >= tokens.size()) {
            throw new IllegalArgumentException(
                    "expected '" + expected + "' but reached end of input");
        }
        String actual = tokens.get(pos);
        if (!actual.equalsIgnoreCase(expected)) {
            throw new IllegalArgumentException(
                    "expected '" + expected + "' but got '" + actual + "'");
        }
        pos++;
    }

    private boolean isKeyword(String token, String keyword) {
        return token.equalsIgnoreCase(keyword);
    }
}