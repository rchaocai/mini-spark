package com.sparklearn.sql.catalyst.parser;

import com.sparklearn.sql.catalyst.expressions.Expression;
import com.sparklearn.sql.catalyst.plans.logical.LogicalPlan;

/**
 * SQL 解析器接口：把 SQL 字符串翻译成 Catalyst 逻辑计划或表达式。
 *
 * <p>参考 Spark 源码：
 * <pre>
 * // sql/catalyst/.../parser/ParserInterface.scala
 * trait ParserInterface {
 *   def parsePlan(sqlText: String): LogicalPlan
 *   def parseExpression(sqlText: String): Expression
 *   def parseTableIdentifier(sqlText: String): TableIdentifier
 * }
 * </pre>
 *
 * <p>Spark 的 trait 在 Java 里天然对应 interface。本教学版只需要解析 plan 和 expression，
 * 不解析 TableIdentifier（mini-spark 没有库表的概念，FROM 后面直接是注册的表名），
 * 所以接口比 Spark 少一个方法。
 *
 * <p>本接口提供两种实现并存：
 * <ul>
 *   <li>{@link LegacySqlParser}：原手写递归下降解析器，保留教学连贯性</li>
 *   <li>{@link SparkSqlParser}：基于 ANTLR4 的解析器，对齐 Spark 2.0+ 架构</li>
 * </ul>
 */
public interface ParserInterface {

    /**
     * 把 SQL 字符串解析成 {@link LogicalPlan}。
     *
     * <p>对应 Spark 的 {@code ParserInterface.parsePlan}。
     *
     * @param sqlText SQL 文本，例如 {@code "SELECT name FROM employees WHERE salary > 50000"}
     * @return 解析后的逻辑计划树
     * @throws ParseException SQL 语法错误时抛出
     */
    LogicalPlan parsePlan(String sqlText);

    /**
     * 把 SQL 字符串解析成 {@link Expression}。
     *
     * <p>对应 Spark 的 {@code ParserInterface.parseExpression}。本教学版主要用于调试，
     * DataFrame DSL 走的是 {@code Expressions.col(...)} 而不是 SQL 解析。
     *
     * @param sqlText 表达式文本，例如 {@code "salary > 50000"}
     * @return 解析后的表达式树
     * @throws ParseException SQL 语法错误时抛出
     */
    Expression parseExpression(String sqlText);
}
