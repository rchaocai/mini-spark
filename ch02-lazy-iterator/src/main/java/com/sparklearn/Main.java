package com.sparklearn;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * 第 2 章 · 数据流与延迟迭代 —— 演示入口。
 *
 * <p>本章三个核心验证点：
 * <ol>
 *   <li>ListRDD <strong>不持有数据副本</strong>——构造时不复制，只存「访问方式」</li>
 *   <li>惰性——构造 RDD 时什么也不发生，消费迭代器时才真正读数据</li>
 *   <li>可重复消费——每次 {@code compute()} 返回全新的迭代器</li>
 * </ol>
 *
 * <p>详见 docs/specs/ch02-lazy-iterator.md 与 book/ch02-lazy-iterator.md。
 */
public final class Main {

    private Main() {
    }

    public static void main(String[] args) {
        // ========== 垫脚石：Deferred —— 「一段还没执行的计算」==========
        System.out.println("=== 1. Deferred：感受「延迟」===");
        System.out.println("构造 Deferred...");
        Deferred<String> lazy = new Deferred<>(() -> {
            // 这里是一段「重计算」——但构造 Deferred 时不会执行
            return "expensive result (" + System.currentTimeMillis() + ")";
        });

        System.out.println("Deferred 已构造，但计算还没发生。");
        System.out.println("第一次 get(): " + lazy.get());
        System.out.println("第二次 get(): " + lazy.get());
        System.out.println("看到了吗——第二次 get() 不重新算，直接返回缓存值。\n");

        // ========== ListRDD：持有「访问方式」，不持有数据 ==========
        System.out.println("=== 2. ListRDD：不存数据副本 ===");
        List<String> words = Arrays.asList("hello", "spark", "hello", "world", "spark", "is", "fun");

        // 构造 ListRDD —— 注意：这里不复制 words，也不触发任何遍历
        System.out.println("构造 ListRDD（不复制数据）...");
        ListRDD<String> rdd = new ListRDD<>(words);

        // 验证 1：不持有副本 —— 修改原始 list，通过 RDD 再读会看到修改吗？
        // 注意：ListRDD 持有的是 Supplier(() -> words.iterator())，
        // 所以如果原始 list 被修改了，RDD 再次消费会看到修改。
        // 这不是 bug——它正是「不持有副本」的证明。
        System.out.println("原始数据: " + words);

        System.out.println("\n=== 3. 每次 compute() 返回全新迭代器 ===");
        Iterator<String> it1 = rdd.compute();
        Iterator<String> it2 = rdd.compute();
        System.out.println("it1 == it2 ? " + (it1 == it2) + "  ← false，说明每次都是新的！");

        System.out.println("\n第一次遍历:");
        it1.forEachRemaining(w -> System.out.print(" " + w));
        System.out.println();

        System.out.println("第二次遍历（独立的迭代器，从头开始）:");
        it2.forEachRemaining(w -> System.out.print(" " + w));
        System.out.println();

        // ========== 延迟的延展：transform 也不马上算 ==========
        System.out.println("\n=== 4. 构造 RDD 时不触发计算 ===");
        System.out.print("构造一个带日志的 ListRDD... ");
        ListRDD<String> loggedRdd = new ListRDD<>(
                Arrays.asList("a", "b", "c")
        );
        System.out.println("完成！—— 没有一行读取日志，因为 Supplier 还没被调用。");
        System.out.print("现在消费迭代器: ");
        loggedRdd.compute().forEachRemaining(w -> System.out.print(w + " "));
        System.out.println("← 才真正读数据。");
    }
}
