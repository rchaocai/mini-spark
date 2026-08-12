package com.sparklearn.sql.catalyst.parser;

import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.IntStream;

/**
 * 大小写不敏感的字符流：让 Lexer 在匹配关键字时大小写不敏感，但 token 文本保留原样。
 *
 * <p>参考 Spark 源码：
 * <pre>
 * // sql/catalyst/.../parser/ParseDriver.scala
 * private[parser] class ANTLRNoCaseStringStream(input: String) extends ANTLRInputStream(input) {
 *   override def LA(i: Int): Int = {
 *     val la = super.LA(i)
 *     if (la == 0 || la == IntStream.EOF) la
 *     else Character.toUpperCase(la)
 *   }
 * }
 * </pre>
 *
 * <p>关键点：{@link #LA(int)} 是 lexer 的"向前看"函数，仅用于匹配 lexer 规则。
 * ANTLR 创建 token 时调用的是底层的 {@code consume()}，token 文本取自原始输入流，
 * 因此即使 LA() 把字符大写化，token 的 text 仍是用户原始输入。
 *
 * <p>这样 {@code select}、{@code Select}、{@code SELECT} 都能匹配 {@code SELECT: 'SELECT'}
 * 规则，但列名 {@code myColumn} 的 token 文本仍是 {@code myColumn}，不会变成 {@code MYCOLUMN}。
 */
final class UpperCaseCharStream extends ANTLRInputStream {

    UpperCaseCharStream(String input) {
        super(input);
    }

    @Override
    public int LA(int i) {
        int la = super.LA(i);
        if (la == 0 || la == IntStream.EOF) {
            return la;
        }
        return Character.toUpperCase(la);
    }
}
