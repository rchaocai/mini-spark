package com.sparklearn;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * 第 3 章演示入口。
 */
public final class Main {

    private Main() {
    }

    public static void main(String[] args) {
        demonstrateMappingIterator();
        demonstrateLazyMap();
        demonstratePipeline();
        demonstrateFilterAndFlatMap();
    }

    private static void demonstrateMappingIterator() {
        System.out.println("=== 1. MappingIterator：边读取边变换 ===");
        Iterator<Integer> parent = Arrays.asList(1, 2, 3).iterator();
        Iterator<Integer> mapped = new MappingIterator<>(parent, number -> {
            int result = number + 10;
            System.out.println("  apply: " + number + " -> " + result);
            return result;
        });

        System.out.println("构造完成，函数还没有执行。");
        while (mapped.hasNext()) {
            System.out.println("  next() 得到 " + mapped.next());
        }
        System.out.println();
    }

    private static void demonstrateLazyMap() {
        System.out.println("=== 2. MapPartitionsRDD：map 不会立刻计算 ===");
        RDD<Integer> plusOne = new ListRDD<>(Arrays.asList(1, 2, 3))
                .map(number -> {
                    System.out.println("  map(+1): " + number);
                    return number + 1;
                });

        System.out.println("map 已构造；到这里没有 map 日志。");
        System.out.println("collect() 结果: " + plusOne.collect());
        System.out.println();
    }

    private static void demonstratePipeline() {
        System.out.println("=== 3. 流水线：元素逐个穿透两层 map ===");
        RDD<Integer> pipeline = new ListRDD<>(Arrays.asList(1, 2, 3))
                .map(number -> {
                    System.out.println("  [+1] " + number);
                    return number + 1;
                })
                .map(number -> {
                    System.out.println("  [*2] " + number);
                    return number * 2;
                });

        System.out.println("流水线已构造；现在调用 collect()：");
        List<Integer> result = pipeline.collect();
        System.out.println("结果: " + result);
        System.out.println();
    }

    private static void demonstrateFilterAndFlatMap() {
        System.out.println("=== 4. filter、flatMap 与链式组合 ===");
        List<Integer> filtered = new ListRDD<>(Arrays.asList(1, 2, 3, 4, 5, 6))
                .map(number -> number * 3)
                .filter(number -> number > 10)
                .collect();
        System.out.println("map(*3).filter(>10): " + filtered);

        List<String> words = new ListRDD<>(Arrays.asList(
                "hello world",
                "spark java"))
                .flatMap(line -> Arrays.asList(line.split(" ")))
                .collect();
        System.out.println("flatMap(按空格拆词): " + words);
    }
}
