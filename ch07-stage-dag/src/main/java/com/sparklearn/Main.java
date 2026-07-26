package com.sparklearn;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;

/**
 * Stage 与 DAG 演示入口。
 */
public final class Main {

    private Main() {
    }

    public static void main(String[] args) {
        demonstrateStageDag();
    }

    private static void demonstrateStageDag() {
        System.out.println("=== 1. 输入数据 ===");
        List<KeyValuePair<String, Integer>> words = Arrays.asList(
                new KeyValuePair<>("hello", 1),
                new KeyValuePair<>("world", 1),
                new KeyValuePair<>("hello", 1),
                new KeyValuePair<>("spark", 1),
                new KeyValuePair<>("world", 1),
                new KeyValuePair<>("hello", 1),
                new KeyValuePair<>("java", 1),
                new KeyValuePair<>("spark", 1),
                new KeyValuePair<>("hello", 1));
        System.out.println("原始数据: " + words);
        System.out.println("期望: hello→4, world→2, spark→2, java→1\n");

        int numReducePartitions = 2;
        try (SparkContext sc = new SparkContext(numReducePartitions, true)) {
            runDemo(sc, words, numReducePartitions);
        }
    }

    private static void runDemo(
            SparkContext sc,
            List<KeyValuePair<String, Integer>> words,
            int numReducePartitions) {
        System.out.println("=== 2. Map 分区 ===");
        RDD<KeyValuePair<String, Integer>> rdd = sc.parallelize(words, 3);
        for (Partition partition : rdd.partitions()) {
            List<KeyValuePair<String, Integer>> partData = new ArrayList<>();
            rdd.iterator(partition).forEachRemaining(partData::add);
            System.out.println("分区 " + partition.index() + ": " + partData);
        }
        System.out.println("—— 同一个 key 已经散到多个分区里了，光看局部结果不够\n");

        System.out.println("=== 3. RDD 血缘 ===");
        MapPartitionsRDD<KeyValuePair<String, Integer>, KeyValuePair<String, Integer>> mapped =
                rdd.map(Function.identity());
        System.out.println("ListRDD");
        System.out.println("  └─ MapPartitionsRDD（窄依赖，不切 Stage）");

        ShuffledRDD<String, Integer> shuffled = mapped.reduceByKey(
                Integer::sum, numReducePartitions);
        System.out.println("       └─ ShuffledRDD（宽依赖，切 Stage）");
        System.out.println("—— 这里只是构造结果 RDD，还没有真正写文件\n");

        System.out.println("=== 4. DAGScheduler 执行 collect ===");
        System.out.println("reduceByKey 结果: " + shuffled.collect());
    }
}
