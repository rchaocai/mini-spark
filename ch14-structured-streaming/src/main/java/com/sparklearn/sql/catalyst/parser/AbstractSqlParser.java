package com.sparklearn.sql.catalyst.parser;

import com.sparklearn.sql.catalyst.expressions.Expression;
import com.sparklearn.sql.catalyst.plans.logical.LogicalPlan;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.atn.PredictionMode;
import org.antlr.v4.runtime.misc.ParseCancellationException;

import java.util.function.Function;

/**
 * ANTLR4 SQL 解析器的抽象基类，封装"lexer → tokenStream → parser → AstBuilder"的模板流程。
 *
 * <p>参考 Spark 源码：
 * <pre>
 * // sql/catalyst/.../parser/ParseDriver.scala
 * abstract class AbstractSqlParser extends ParserInterface with Logging {
 *   override def parsePlan(sqlText: String): LogicalPlan = parse(sqlText) { parser =>
 *     astBuilder.visitSingleStatement(parser.singleStatement())
 *   }
 *   protected def astBuilder: AstBuilder
 *   protected def parse[T](command: String)(toResult: SqlBaseParser => T): T = {
 *     val lexer = new SqlBaseLexer(new ANTLRNoCaseStringStream(command))
 *     ...
 *     try {
 *       try {
 *         parser.getInterpreter.setPredictionMode(PredictionMode.SLL)  // 先快速 SLL
 *         toResult(parser)
 *       } catch {
 *         case e: ParseCancellationException =>
 *           tokenStream.reset(); parser.reset()
 *           parser.getInterpreter.setPredictionMode(PredictionMode.LL)  // 回退 LL
 *           toResult(parser)
 *       }
 *     } catch { ... }
 *   }
 * }
 * </pre>
 *
 * <p>关键设计：
 * <ul>
 *   <li>关键词大小写不敏感：通过 {@link UpperCaseCharStream} 把 LA() 返回的字符大写化，
 *       但 token 文本仍保留原样（与 Spark 的 ANTLRNoCaseStringStream 同思路）</li>
 *   <li>SLL → LL 回退：先用快速 SLL 模式尝试，失败再回退到精确但更慢的 LL 模式</li>
 *   <li>子类只需要提供 {@link #astBuilder()}，把 ANTLR ParseTree 翻译成 Catalyst AST</li>
 * </ul>
 */
public abstract class AbstractSqlParser implements ParserInterface {

    /** 子类提供 ANTLR ParseTree → Catalyst AST 的翻译器。 */
    protected abstract AstBuilder astBuilder();

    @Override
    public LogicalPlan parsePlan(String sqlText) {
        return parse(sqlText, parser -> {
            Object result = astBuilder().visitSingleStatement(parser.singleStatement());
            if (result instanceof LogicalPlan plan) {
                return plan;
            }
            throw new ParseException("Unsupported SQL statement", sqlText);
        });
    }

    @Override
    public Expression parseExpression(String sqlText) {
        return parse(sqlText, parser -> astBuilder().visitSingleExpression(parser.singleExpression()));
    }

    /**
     * 模板方法：构造 lexer / tokenStream / parser，先用 SLL 模式尝试解析，
     * 失败则回退到 LL 模式重试。
     *
     * <p>对应 Spark 的 {@code AbstractSqlParser.parse[T](command)(toResult)}。
     *
     * @param sqlText  SQL 文本
     * @param toResult 把 SqlBaseParser 应用到 singleStatement/singleExpression 等入口的函数
     * @param <T>      返回类型（LogicalPlan 或 Expression）
     */
    protected <T> T parse(String sqlText, Function<SqlBaseParser, T> toResult) {
        // 1. 词法：Upper-case stream 让关键字大小写不敏感
        SqlBaseLexer lexer = new SqlBaseLexer(new UpperCaseCharStream(sqlText));
        lexer.removeErrorListeners();
        lexer.addErrorListener(ParseErrorListener.INSTANCE);

        // 2. token 流
        CommonTokenStream tokenStream = new CommonTokenStream(lexer);

        // 3. 语法
        SqlBaseParser parser = new SqlBaseParser(tokenStream);
        parser.removeErrorListeners();
        parser.addErrorListener(ParseErrorListener.INSTANCE);

        try {
            try {
                // 先尝试快速 SLL 模式（不进行完整 LL 分析，速度快但精度略低）
                parser.getInterpreter().setPredictionMode(PredictionMode.SLL);
                return toResult.apply(parser);
            } catch (ParseCancellationException e) {
                // SLL 失败：回退到精确的 LL 模式重试
                tokenStream.reset();
                parser.reset();
                parser.getInterpreter().setPredictionMode(PredictionMode.LL);
                return toResult.apply(parser);
            }
        } catch (ParseException e) {
            // 已经是 ParseException：补上 SQL 文本后重新抛
            if (e.command() != null) {
                throw e;
            }
            throw new ParseException(e.getMessage(), sqlText, e);
        } catch (Exception e) {
            throw new ParseException("解析失败: " + e.getMessage(), sqlText, e);
        }
    }
}
