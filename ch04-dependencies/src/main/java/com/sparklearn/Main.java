package com.sparklearn;

import java.util.Arrays;
import java.util.List;

/**
 * 第 4 章演示入口。
 */
public final class Main {

    private Main() {
    }

    public static void main(String[] args) {
        demonstratePartitionsAndDependencies();
        demonstrateLineage();
        demonstrateOneToOneDependency();
        demonstrateActions();
    }

    private static void demonstratePartitionsAndDependencies() {
        System.out.println("=== 1. ListRDD：源头 RDD 也有分区 ===");
        ListRDD<Integer> rdd = new ListRDD<>(Arrays.asList(1, 2, 3, 4, 5));

        System.out.println("分区: " + rdd.partitions());
        System.out.println("依赖: " + rdd.dependencies());
        System.out.println();
    }

    private static void demonstrateLineage() {
        System.out.println("=== 2. 血缘链：沿 dependencies() 回到源头 ===");
        RDD<Integer> rdd = new ListRDD<>(Arrays.asList(1, 2, 3, 4, 5))
                .map(number -> number + 1)
                .filter(number -> number % 2 == 0)
                .map(number -> number * 10);

        printLineage(rdd, "");
        System.out.println();
    }

    private static void demonstrateOneToOneDependency() {
        System.out.println("=== 3. OneToOneDependency：子分区 i 依赖父分区 i ===");
        RDD<Integer> rdd = new ListRDD<>(Arrays.asList(1, 2, 3)).map(number -> number * 2);
        Dependency<?> dependency = rdd.dependencies().get(0);

        System.out.println("依赖类型: " + dependency.getClass().getSimpleName());
        if (dependency instanceof OneToOneDependency<?> oneToOneDependency) {
            System.out.println("子分区 0 的父分区: " + oneToOneDependency.getParents(0));
        }
        System.out.println();
    }

    private static void demonstrateActions() {
        System.out.println("=== 4. Action：遍历所有分区并归并结果 ===");
        ListRDD<Integer> numbers = new ListRDD<>(Arrays.asList(1, 2, 3, 4, 5));
        RDD<Integer> doubled = numbers.map(number -> {
            System.out.println("  [map *2] " + number);
            return number * 2;
        });

        System.out.println("collect(): " + doubled.collect());
        System.out.println("count(): " + numbers.count());
        System.out.println("reduce(sum): " + numbers.reduce(Integer::sum));
    }

    private static void printLineage(RDD<?> rdd, String indent) {
        List<Dependency<?>> dependencies = rdd.dependencies();
        if (dependencies.isEmpty()) {
            System.out.println(indent + rdd.getClass().getSimpleName() + "  <- 源头");
            return;
        }

        System.out.println(indent + rdd.getClass().getSimpleName());
        for (Dependency<?> dependency : dependencies) {
            System.out.println(indent + "  依赖: " + dependency.getClass().getSimpleName());
            printLineage(dependency.rdd(), indent + "    ");
        }
    }
}
