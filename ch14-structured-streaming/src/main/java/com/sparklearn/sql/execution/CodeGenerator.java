package com.sparklearn.sql.execution;

import org.codehaus.janino.SimpleCompiler;

import java.io.StringReader;
import java.util.Iterator;

/**
 * 用 Janino 把一段 Java 源码编译成 Class，再反射实例化。
 *
 * <p>Janino 是一个轻量级 Java 编译器，能在运行时把字符串源码编译成字节码并加载进当前 JVM。
 * Spark 生产环境用更底层的 ASM + Janino 组合，这里为了教学简洁，直接用 SimpleCompiler。
 */
public final class CodeGenerator {

    private CodeGenerator() {
    }

    /**
     * 编译一个实现了 Iterator<Row> 的类，并返回它的 Class 对象。
     *
     * @param className 类名（不含包名，固定在 generated 包下）
     * @param source    完整的 Java 源码
     * @return 编译后的 Class
     */
    public static Class<?> compile(String className, String source) {
        try {
            SimpleCompiler compiler = new SimpleCompiler();
            compiler.setParentClassLoader(CodeGenerator.class.getClassLoader());
            compiler.cook(new StringReader(source));
            ClassLoader loader = compiler.getClassLoader();
            return loader.loadClass("generated." + className);
        } catch (Exception e) {
            throw new RuntimeException(
                    "Failed to compile generated code:\n" + source, e);
        }
    }

    /**
     * 编译并实例化一个 Iterator<Row>。
     */
    @SuppressWarnings("unchecked")
    public static Iterator<com.sparklearn.sql.Row> compileAndInstantiate(
            String className, String source, Iterator<com.sparklearn.sql.Row> input) {
        Class<?> clazz = compile(className, source);
        try {
            return (Iterator<com.sparklearn.sql.Row>)
                    clazz.getDeclaredConstructor(Iterator.class).newInstance(input);
        } catch (Exception e) {
            throw new RuntimeException(
                    "Failed to instantiate generated class " + className, e);
        }
    }
}
