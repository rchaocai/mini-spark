package com.sparklearn.tools;

import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.Vocabulary;
import org.antlr.v4.runtime.tree.ParseTree;

/**
 * 附录 B · ANTLR4 演示：把算术表达式字符串解析成结构化数据并计算。
 *
 * <p>流程：字符串 → 词法分析（token 流）→ 语法分析（parse tree）→ Visitor 计算结果。
 */
public final class AntlrDemo {

    private AntlrDemo() {
    }

    public static void demo() {
        String input = "1 + 2 * 3";
        System.out.println("输入: " + input);

        // 1. 词法分析：把字符串切成 token
        ExprLexer lexer = new ExprLexer(CharStreams.fromString(input));
        CommonTokenStream tokens = new CommonTokenStream(lexer);
        tokens.fill();

        Vocabulary vocab = lexer.getVocabulary();
        System.out.print("Token 流: ");
        for (Token token : tokens.getTokens()) {
            String text = token.getText();
            if (token.getType() == Token.EOF) {
                break;
            }
            String typeName = vocab.getSymbolicName(token.getType());
            System.out.print(text + "(" + typeName + ") ");
        }
        System.out.println();

        // 2. 语法分析：构建 parse tree
        ExprParser parser = new ExprParser(tokens);
        ParseTree tree = parser.prog();
        System.out.println("Parse Tree: " + tree.toStringTree(parser));

        // 3. Visitor 计算结果
        int result = new EvalVisitor().visit(tree);
        System.out.println("计算结果: " + result);
    }
}
