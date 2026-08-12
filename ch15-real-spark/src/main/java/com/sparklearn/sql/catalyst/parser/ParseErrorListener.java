package com.sparklearn.sql.catalyst.parser;

import org.antlr.v4.runtime.BaseErrorListener;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.Recognizer;

/**
 * ANTLR 错误监听器：把词法/语法错误翻译成 {@link ParseException}。
 *
 * <p>参考 Spark 源码：
 * <pre>
 * // sql/catalyst/.../parser/ParseDriver.scala
 * case object ParseErrorListener extends BaseErrorListener {
 *   override def syntaxError(
 *       recognizer: Recognizer[_, _],
 *       offendingSymbol: scala.Any,
 *       line: Int,
 *       charPositionInLine: Int,
 *       msg: String,
 *       e: RecognitionException): Unit = {
 *     val position = Origin(Some(line), Some(charPositionInLine))
 *     throw new ParseException(None, msg, position, position)
 *   }
 * }
 * </pre>
 *
 * <p>ANTLR 默认的错误行为是打印到 stderr 并继续解析，对本教学版不合适——
 * 翻译成异常直接终止，让上层（{@link AbstractSqlParser#parse}）统一处理。
 */
final class ParseErrorListener extends BaseErrorListener {

    static final ParseErrorListener INSTANCE = new ParseErrorListener();

    private ParseErrorListener() {
    }

    @Override
    public void syntaxError(Recognizer<?, ?> recognizer,
                            Object offendingSymbol,
                            int line,
                            int charPositionInLine,
                            String msg,
                            RecognitionException e) {
        throw new ParseException("line " + line + ":" + charPositionInLine + " " + msg);
    }
}
