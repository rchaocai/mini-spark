package com.sparklearn;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;

/**
 * 第 1 章 · Java 特性预习 —— 可运行的演示。
 *
 * <p>对应 1.1 节讲的 Java 特性。每个特性配一段可运行代码，
 * 跑一下就能看到"传统写法"和"简化写法"的对比。
 *
 * <p>运行：{@code java -cp ch01-wordcount/target/classes com.sparklearn.JavaFeatures}
 */
public final class JavaFeatures {

    private JavaFeatures() {
    }

    public static void main(String[] args) {
        lambdaDemo();
        methodReferenceDemo();
        functionalInterfaceDemo();
        genericDemo();
        optionalDemo();
        listOfDemo();
        tryWithResourcesDemo();
        recordSealedDemo();
    }

    // 1.1.1 Lambda：匿名内部类的简化
    private static void lambdaDemo() {
        System.out.println("=== 1.1.1 Lambda：匿名内部类的简化 ===");

        Map<String, Integer> counts = new HashMap<>();
        counts.put("spark", 3);
        counts.put("hello", 2);

        // Java 8 之前：匿名内部类
        System.out.println("[匿名内部类]");
        counts.forEach(new BiConsumer<String, Integer>() {
            @Override
            public void accept(String word, Integer n) {
                System.out.println("  " + word + " -> " + n);
            }
        });

        // Java 8+：lambda
        System.out.println("[lambda]");
        counts.forEach((word, n) -> System.out.println("  " + word + " -> " + n));
        System.out.println();
    }

    // 1.1.1 方法引用：lambda 的进一步简写
    private static void methodReferenceDemo() {
        System.out.println("=== 1.1.1 方法引用 ===");

        List<String> words = List.of("spark", "hello", "world");

        System.out.println("[lambda]");
        words.forEach(w -> System.out.println("  " + w));

        System.out.println("[方法引用 System.out::println]");
        words.forEach(System.out::println);

        System.out.println("[Integer::sum]");
        System.out.println("  Integer.sum(3, 5) = " + Integer.sum(3, 5));
        System.out.println();
    }

    // 1.1.1 函数式接口：lambda 的类型
    private static void functionalInterfaceDemo() {
        System.out.println("=== 1.1.1 函数式接口 ===");

        // Function<T, R>：接收 T，返回 R
        Function<String, Integer> length = s -> s.length();
        System.out.println("Function: length(\"spark\") = " + length.apply("spark"));

        // Predicate<T>：接收 T，返回 true/false
        Predicate<String> isLong = s -> s.length() > 3;
        System.out.println("Predicate: \"spark\".length() > 3 ? " + isLong.test("spark"));

        // Supplier<T>：不接收参数，返回 T
        Supplier<String> greeting = () -> "hello";
        System.out.println("Supplier: " + greeting.get());

        // Consumer<T>：接收 T，不返回
        Consumer<String> printer = s -> System.out.println("  consumed: " + s);
        System.out.println("Consumer:");
        printer.accept("spark");
        System.out.println();
    }

    // 1.1.2 泛型：类型参数化
    private static void genericDemo() {
        System.out.println("=== 1.1.2 泛型 ===");

        // 泛型类：同一个 Box 能装不同类型
        Box<String> strBox = new Box<>("hello");
        Box<Integer> intBox = new Box<>(42);
        System.out.println("Box<String>: " + strBox.get());
        System.out.println("Box<Integer>: " + intBox.get());

        // 泛型方法：返回列表的第一个元素
        List<String> words = List.of("spark", "hello", "world");
        String first = firstOf(words);
        System.out.println("firstOf(words): " + first);
        System.out.println();
    }

    // 1.1.3 Optional：明确表示"可能没有"
    private static void optionalDemo() {
        System.out.println("=== 1.1.3 Optional ===");

        Map<String, Integer> counts = new HashMap<>();
        counts.put("spark", 3);

        // 存在的键
        Optional<Integer> found = Optional.ofNullable(counts.get("spark"));
        found.map(n -> n * 2)
                .ifPresent(n -> System.out.println("spark * 2 = " + n));

        // 不存在的键
        Optional<Integer> missing = Optional.ofNullable(counts.get("world"));
        System.out.println("world 存在? " + missing.isPresent());
        System.out.println("world 默认值: " + missing.orElse(0));
        System.out.println();
    }

    // 1.1.4 List.of：创建不可变集合
    private static void listOfDemo() {
        System.out.println("=== 1.1.4 List.of / Map.of ===");

        List<String> words = List.of("spark", "hello", "world");
        System.out.println("List.of: " + words);

        Map<String, Integer> freq = Map.of("spark", 3, "hello", 2);
        System.out.println("Map.of: " + freq);

        // List.of 完全不可变：add/set/remove 都抛异常
        System.out.println("List.of 尝试修改：");
        try { words.add("new"); } catch (UnsupportedOperationException e) {
            System.out.println("  add → " + e.getClass().getSimpleName());
        }
        try { words.set(0, "changed"); } catch (UnsupportedOperationException e) {
            System.out.println("  set → " + e.getClass().getSimpleName());
        }
        try { words.remove(0); } catch (UnsupportedOperationException e) {
            System.out.println("  remove → " + e.getClass().getSimpleName());
        }

        // Arrays.asList 允许 set 但不能 add
        List<String> asList = Arrays.asList("a", "b", "c");
        System.out.println("Arrays.asList 尝试修改：");
        asList.set(0, "changed");
        System.out.println("  set(0, \"changed\") → 成功: " + asList);
        try { asList.add("new"); } catch (UnsupportedOperationException e) {
            System.out.println("  add → " + e.getClass().getSimpleName());
        }
        System.out.println();
    }

    // 1.1.5 try-with-resources：自动关闭资源
    private static void tryWithResourcesDemo() {
        System.out.println("=== 1.1.5 try-with-resources ===");

        // 正常结束：资源自动关闭
        System.out.println("[正常结束]");
        try (Resource res = new Resource("资源A")) {
            res.use();
        }
        System.out.println("  资源已关闭");

        // 异常结束：资源仍然自动关闭
        System.out.println("[异常结束]");
        try (Resource res = new Resource("资源B")) {
            res.use();
            throw new RuntimeException("模拟异常");
        } catch (RuntimeException e) {
            System.out.println("  捕获异常: " + e.getMessage());
        }
        System.out.println();
    }

    // 1.1.6 record 与 sealed
    private static void recordSealedDemo() {
        System.out.println("=== 1.1.6 record 与 sealed ===");

        // record：一行定义一个"只装数据"的类
        Pair p = new Pair("spark", 3);
        System.out.println("record Pair:");
        System.out.println("  key() = " + p.key());
        System.out.println("  value() = " + p.value());
        System.out.println("  toString() = " + p);
        System.out.println("  equals(new Pair(\"spark\", 3)) = " + p.equals(new Pair("spark", 3)));

        // sealed：限制谁可以实现接口
        System.out.println("sealed Shape:");
        Shape circle = new Circle(5.0);
        Shape square = new Square(4.0);
        describe(circle);
        describe(square);
        System.out.println();
    }

    /** 对 Shape 做 instanceof 模式匹配。编译器知道 Shape 只有 Circle 和 Square。 */
    private static void describe(Shape shape) {
        if (shape instanceof Circle c) {
            System.out.println("  圆，半径=" + c.r());
        } else if (shape instanceof Square s) {
            System.out.println("  方，边长=" + s.side());
        }
    }

    // ---- 辅助类 ----

    /** 泛型类示例。 */
    static final class Box<T> {
        private final T value;

        Box(T value) {
            this.value = value;
        }

        T get() {
            return value;
        }
    }

    /** 泛型方法示例：返回列表的第一个元素。 */
    private static <T> T firstOf(List<T> list) {
        return list.get(0);
    }

    /** AutoCloseable 资源示例，用于演示 try-with-resources。 */
    static final class Resource implements AutoCloseable {
        private final String name;

        Resource(String name) {
            this.name = name;
            System.out.println("  打开资源: " + name);
        }

        void use() {
            System.out.println("  使用资源: " + name);
        }

        @Override
        public void close() {
            System.out.println("  关闭资源: " + name);
        }
    }

    // ---- record 示例 ----

    /** record：编译器自动生成构造器、getter、equals、hashCode、toString。 */
    record Pair(String key, int value) {}

    // ---- sealed 示例 ----

    /** sealed 接口：只允许 Circle 和 Square 实现。 */
    sealed interface Shape permits Circle, Square {}

    /** record 可以直接实现 sealed 接口（record 隐式是 final 的）。 */
    record Circle(double r) implements Shape {}

    record Square(double side) implements Shape {}
}
