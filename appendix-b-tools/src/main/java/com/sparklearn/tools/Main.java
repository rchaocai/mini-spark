package com.sparklearn.tools;

/**
 * 附录 B · ANTLR4 与 Janino 入门 —— 演示入口。
 *
 * <p>运行：
 * <pre>
 * mvn -pl appendix-b-tools compile exec:java -Dexec.mainClass=com.sparklearn.tools.Main
 * </pre>
 */
public final class Main {

    private Main() {
    }

    public static void main(String[] args) {
        System.out.println("=== 附录 B · ANTLR4 与 Janino 入门 ===");
        System.out.println();

        System.out.println("--- B.1 ANTLR4：从文本到结构化数据 ---");
        AntlrDemo.demo();
        System.out.println();

        System.out.println("--- B.2 Janino：运行时编译代码 ---");
        JaninoDemo.demo();
    }
}
