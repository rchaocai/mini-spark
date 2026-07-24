package com.sparklearn.sql;

import com.sparklearn.sql.catalyst.expressions.Attribute;
import com.sparklearn.sql.catalyst.plans.logical.Aggregate;
import com.sparklearn.sql.catalyst.plans.logical.LogicalPlan;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * 手写最小 SQL 解析器，只支持 SELECT ... FROM ... GROUP BY ... + count(*)。
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
     * SELECT column [, count(*)] FROM tableName GROUP BY column
     * </pre>
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
            if (c == ',' || c == '(' || c == ')' || c == '*') {
                tokens.add(String.valueOf(c));
                i++;
                continue;
            }
            if (c == '\'' || c == '"') {
                // 字符串字面量
                char quote = c;
                int j = i + 1;
                while (j < sql.length() && sql.charAt(j) != quote) {
                    j++;
                }
                tokens.add(sql.substring(i, j + 1));
                i = j + 1;
                continue;
            }
            // 标识符或关键字
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
        return Character.isWhitespace(c) || c == ',' || c == '(' || c == ')' || c == '*';
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
                pos++; // skip COUNT
                expect(tokens, "(");
                expect(tokens, "*");
                expect(tokens, ")");
                hasCountStar = true;
            } else {
                selectCols.add(token);
                pos++;
            }
            if (pos < tokens.size() && tokens.get(pos).equals(",")) {
                pos++; // skip comma
            }
        }

        expect(tokens, "FROM");
        String tableName = tokens.get(pos++);

        // 解析 GROUP BY
        List<String> groupByCols = new ArrayList<>();
        if (pos < tokens.size() && isKeyword(tokens.get(pos), "GROUP")) {
            pos++; // skip GROUP
            expect(tokens, "BY");
            while (pos < tokens.size()) {
                groupByCols.add(tokens.get(pos++));
                if (pos < tokens.size() && tokens.get(pos).equals(",")) {
                    pos++;
                }
            }
        }

        // 解析 WHERE（暂不支持，但识别语法）
        if (pos < tokens.size() && isKeyword(tokens.get(pos), "WHERE")) {
            throw new UnsupportedOperationException("WHERE clause is not supported yet");
        }

        // 构建 LogicalPlan
        return buildPlan(tableName, selectCols, hasCountStar, groupByCols);
    }

    private LogicalPlan buildPlan(String tableName, List<String> selectCols,
                                   boolean hasCountStar, List<String> groupByCols) {
        LogicalPlan tablePlan = tableRegistry.get(tableName);
        if (tablePlan == null) {
            throw new IllegalArgumentException("table not found: " + tableName);
        }

        if (!groupByCols.isEmpty() && hasCountStar) {
            // SELECT col, count(*) FROM table GROUP BY col
            List<Attribute> groupingAttrs = new ArrayList<>();
            for (String col : groupByCols) {
                groupingAttrs.add(new Attribute(col));
            }
            return new Aggregate(groupingAttrs, tablePlan);
        }

        return tablePlan;
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