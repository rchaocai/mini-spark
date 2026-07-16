package com.sparklearn;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * 第 6 章演示入口。
 */
public final class Main {

    private Main() {
    }

    public static void main(String[] args) {
        demonstrateShuffle();
    }

    private static void demonstrateShuffle() {
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

        System.out.println("=== 2. Map 分区 ===");
        ListRDD<KeyValuePair<String, Integer>> rdd = new ListRDD<>(words, 3);
        for (Partition partition : rdd.partitions()) {
            List<KeyValuePair<String, Integer>> partData = new ArrayList<>();
            rdd.iterator(partition).forEachRemaining(partData::add);
            System.out.println("分区 " + partition.index() + ": " + partData);
        }
        System.out.println("—— 同一个 key 已经散到多个分区里了，光看局部结果不够\n");

        System.out.println("=== 3. reduceByKey ===");
        int numReducePartitions = 2;
        ShuffledRDD<String, Integer> shuffled = rdd.reduceByKey(
                Integer::sum, numReducePartitions);
        System.out.println("Reduce 分区数: " + numReducePartitions);
        System.out.println("—— 这里只是构造结果 RDD，还没有真正写文件\n");

        System.out.println("=== 4. 并行 collect ===");
        try (TaskScheduler scheduler = new TaskScheduler(numReducePartitions, true)) {
            System.out.println("reduceByKey 结果: " + scheduler.collect(shuffled));
        }
        System.out.println();

        System.out.println("=== 5. 中间文件 ===");
        File dir = shuffled.shuffleDir();
        System.out.println("Shuffle 目录: " + dir.getAbsolutePath());
        File[] files = dir.listFiles();
        if (files != null) {
            Arrays.sort(files);
            for (File file : files) {
                System.out.println("  " + file.getName() + "  (" + file.length() + " bytes)");
            }
        }
        System.out.println("—— 3 个 Map 分区 × 2 个 Reduce 分区 = 6 个中间文件\n");

        System.out.println("=== 6. 再次 collect ===");
        try (TaskScheduler scheduler = new TaskScheduler(numReducePartitions)) {
            System.out.println("再次 collect 成功: " + scheduler.collect(shuffled));
        }
        System.out.println();

        System.out.println("=== 7. 删除中间文件后再次 collect ===");
        if (files != null) {
            for (File file : files) {
                file.delete();
            }
        }
        dir.delete();
        try (TaskScheduler scheduler = new TaskScheduler(numReducePartitions)) {
            scheduler.collect(shuffled);
            System.out.println("（不应该走到这里）");
        } catch (RuntimeException e) {
            String message = e.getCause() != null ? e.getCause().getMessage() : e.getMessage();
            System.out.println("失败: " + message);
            System.out.println("—— 这说明中间文件不是装饰，它就是这次计算的物理边界。");
        }
    }
}
