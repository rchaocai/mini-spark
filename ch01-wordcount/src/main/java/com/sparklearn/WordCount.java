package com.sparklearn;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 第 1 章 · 从 WordCount 开始 —— 朴素单机版 WordCount。
 *
 * <p>用最朴素的 Java，在单机内存里统计词频。它是一个基准：当数据量涨到
 * 50GB 时，这段代码会撞上两个瓶颈——内存装不下、单核算得慢——而这正是
 * 分布式计算要解决的问题（分区 Partition、把算子发送到数据所在处）。
 */
public final class WordCount {

    private WordCount() {
    }

    public static void main(String[] args) {
        List<String> lines = Arrays.asList(
                "hello spark hello world",
                "spark is fast spark is fun"
        );

        Map<String, Integer> counts = new HashMap<>();
        for (String line : lines) {
            for (String word : line.split(" ")) {
                counts.put(word, counts.getOrDefault(word, 0) + 1);
            }
        }

        System.out.println("word\tcount");
        counts.forEach((word, n) -> System.out.println(word + "\t" + n));
    }
}
