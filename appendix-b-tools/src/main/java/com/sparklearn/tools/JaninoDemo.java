package com.sparklearn.tools;

import org.codehaus.janino.SimpleCompiler;

import java.io.StringReader;

/**
 * 附录 B · Janino 演示：在运行时把 Java 源码字符串编译成 Class 并执行。
 *
 * <p>分两步演示：
 * <ol>
 *   <li>基础：编译一个简单的类，反射调用它的方法</li>
 *   <li>进阶：模拟 codegen——动态生成一个实现 Calculator 接口的类</li>
 * </ol>
 */
public final class JaninoDemo {

    private JaninoDemo() {
    }

    /**
     * 一个接口，后面用 Janino 动态生成它的实现类。
     *
     * <p>这是代码生成的常见模式：定义一个接口，
     * 运行时生成实现该接口的代码，编译后用接口类型调用。
     */
    public interface Calculator {
        int compute(int x);
    }

    public static void demo() {
        basicDemo();
        codegenDemo();
    }

    // 演示 1：编译一个简单的类，反射调用
    private static void basicDemo() {
        System.out.println("=== Janino 基础：编译字符串代码 ===");

        String source = """
                package generated;
                public class Hello {
                    public String greet(String name) {
                        return "Hello, " + name + "!";
                    }
                }
                """;

        System.out.println("源码:");
        System.out.println(source);

        try {
            // 编译：SimpleCompiler 把字符串源码编译成字节码，加载进当前 JVM
            SimpleCompiler compiler = new SimpleCompiler();
            compiler.setParentClassLoader(JaninoDemo.class.getClassLoader());
            compiler.cook(new StringReader(source));
            ClassLoader loader = compiler.getClassLoader();

            // 反射实例化和调用
            Class<?> clazz = loader.loadClass("generated.Hello");
            Object obj = clazz.getDeclaredConstructor().newInstance();
            String result = (String) clazz.getMethod("greet", String.class)
                    .invoke(obj, "World");
            System.out.println("调用 greet(\"World\"): " + result);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        System.out.println();
    }

    // 演示 2：模拟 codegen——动态生成实现 Calculator 接口的类
    private static void codegenDemo() {
        System.out.println("=== Janino 进阶：模拟 codegen ===");

        // 假设要计算 x * 2 + 1
        // 正常流程：解析表达式 → 生成 AST → 遍历 AST 生成代码字符串
        // 这里直接写死代码字符串，聚焦演示 Janino 的编译能力
        String source = """
                package generated;
                public class DoublePlusOne implements com.sparklearn.tools.JaninoDemo.Calculator {
                    public int compute(int x) {
                        return x * 2 + 1;
                    }
                }
                """;

        System.out.println("生成的源码:");
        System.out.println(source);

        try {
            // 编译
            SimpleCompiler compiler = new SimpleCompiler();
            compiler.setParentClassLoader(JaninoDemo.class.getClassLoader());
            compiler.cook(new StringReader(source));
            ClassLoader loader = compiler.getClassLoader();

            // 实例化并调用——不需要反射，直接用接口类型
            Class<?> clazz = loader.loadClass("generated.DoublePlusOne");
            Calculator calc = (Calculator) clazz.getDeclaredConstructor().newInstance();
            System.out.println("compute(5)  = " + calc.compute(5));    // 11
            System.out.println("compute(10) = " + calc.compute(10));   // 21
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        System.out.println();
    }
}
