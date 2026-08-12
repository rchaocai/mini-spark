package com.sparklearn.sql.catalyst.parser;

/**
 * SQL 解析异常。
 *
 * <p>参考 Spark 源码：
 * <pre>
 * // sql/catalyst/.../parser/ParseDriver.scala
 * class ParseException(
 *     val command: Option[String],
 *     message: String,
 *     val start: Origin,
 *     val stop: Origin) extends AnalysisException(message, start.line, start.startPosition)
 * </pre>
 *
 * <p>Spark 的 ParseException 继承 AnalysisException，并带 line/position 上下文。
 * 本教学版简化为 RuntimeException + 消息，保留可选的 SQL 文本便于排错。
 */
public class ParseException extends RuntimeException {

    private final String command;

    public ParseException(String message) {
        super(message);
        this.command = null;
    }

    public ParseException(String message, String command) {
        super(message);
        this.command = command;
    }

    public ParseException(String message, Throwable cause) {
        super(message, cause);
        this.command = null;
    }

    public ParseException(String message, String command, Throwable cause) {
        super(message, cause);
        this.command = command;
    }

    /** 触发解析错误的 SQL 文本，可能为 null。 */
    public String command() {
        return command;
    }

    @Override
    public String getMessage() {
        if (command != null) {
            return super.getMessage() + "\n== SQL ==\n" + command;
        }
        return super.getMessage();
    }
}
