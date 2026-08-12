package com.sparklearn;

import com.sparklearn.rdd.RDD;
import com.sparklearn.storage.StorageLevel;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Locale;

/**
 * Cache 与 checkpoint 的演示入口。
 */
public final class Main {

    private Main() {
    }

    public static void main(String[] args) throws IOException {
        List<Integer> input = List.of(1, 2, 3, 4, 5, 6, 7, 8, 9);
        Path checkpointDir = Files.createTempDirectory("ch11-checkpoint-");
        try (SparkContext sc = new SparkContext(3, false)) {
            sc.setCheckpointDir(checkpointDir.toString());
            System.out.println("=== 第 11 章 · Cache 与 Checkpoint ===");
            System.out.println("输入数据: " + input + "，分区数: 3");
            runWithoutCache(sc, input);
            runWithCache(sc, input);
            runWithDiskOnly(sc, input);
            runWithCheckpoint(sc, input);
        } finally {
             deleteTree(checkpointDir);
        }
    }

    private static void runWithoutCache(
            SparkContext sc,
            List<Integer> input) {
        System.out.println();
        System.out.println("Part A: 没有 cache，两次 action 都会重算上游");

        RDD<Integer> source = sc.parallelize(input, 3);
        RDD<Integer> chain = buildLongLineage(source);
        RDD<Integer> cachedPoint = traceUp(chain, 3);

        List<Integer> first = chain.collect();
        System.out.println("第一次 collect: " + first);
        printComputeCount(source, cachedPoint, chain);

        source.resetComputeCount();
        cachedPoint.resetComputeCount();
        chain.resetComputeCount();
        List<Integer> second = chain.collect();
        System.out.println("第二次 collect: " + second);
        printComputeCount(source, cachedPoint, chain);
    }

    private static void runWithCache(
            SparkContext sc,
            List<Integer> input) throws IOException {
        System.out.println();
        System.out.println("Part B: cache 中间 RDD，第二次 action 从缓存点继续");

        RDD<Integer> source = sc.parallelize(input, 3);
        RDD<Integer> chain = buildLongLineage(source);
        RDD<Integer> cachedPoint = traceUp(chain, 3);
        cachedPoint.cache();

        List<Integer> first = chain.collect();
        System.out.println("第一次 collect，填充缓存: " + first);
        printComputeCount(source, cachedPoint, chain);

        source.resetComputeCount();
        cachedPoint.resetComputeCount();
        chain.resetComputeCount();

        List<Integer> second = chain.collect();
        System.out.println("第二次 collect，命中缓存: " + second);
        printComputeCount(source, cachedPoint, chain);

        runFileSourceTrainingDemo(sc);
    }

    private static void runWithDiskOnly(
            SparkContext sc,
            List<Integer> input) {
        System.out.println();
        System.out.println("Part B2: DISK_ONLY 缓存中间 RDD，缓存块落盘到本地目录");

        RDD<Integer> source = sc.parallelize(input, 3);
        RDD<Integer> chain = buildLongLineage(source);
        RDD<Integer> cachedPoint = traceUp(chain, 3);
        cachedPoint.persist(StorageLevel.DISK_ONLY);

        List<Integer> first = chain.collect();
        System.out.println("第一次 collect: " + first);
        printComputeCount(source, cachedPoint, chain);

        source.resetComputeCount();
        cachedPoint.resetComputeCount();
        chain.resetComputeCount();

        List<Integer> second = chain.collect();
        System.out.println("第二次 collect: " + second);
        printComputeCount(source, cachedPoint, chain);
    }

    private static void runWithCheckpoint(
            SparkContext sc,
            List<Integer> input) {
        System.out.println();
        System.out.println("Part C: checkpoint 中间 RDD，切断它的父依赖");

        RDD<Integer> source = sc.parallelize(input, 3);
        RDD<Integer> chain = buildLongLineage(source);
        RDD<Integer> checkpointPoint = traceUp(chain, 3);

        System.out.println("checkpoint 前依赖数: " + checkpointPoint.dependencies().size());
        checkpointPoint.checkpoint();
        System.out.println("checkpoint 请求后依赖数: " + checkpointPoint.dependencies().size());
        System.out.println("isCheckpointed: " + checkpointPoint.isCheckpointed());

        System.out.println("触发 checkpoint 的 collect: " + checkpointPoint.collect());
        System.out.println("checkpoint 物化后依赖数: " + checkpointPoint.dependencies().size());
        System.out.println("isCheckpointed: " + checkpointPoint.isCheckpointed());

        source.resetComputeCount();
        checkpointPoint.resetComputeCount();

        RDD<Integer> downstream = checkpointPoint
                .map(value -> value * 10)
                .filter(value -> value > 200);
        System.out.println("checkpoint 后继续往下计算: " + downstream.collect());
        System.out.println("源头 compute 次数: " + source.getComputeCount());
        System.out.println("checkpoint 点 compute 次数: " + checkpointPoint.getComputeCount());
    }

    private static RDD<Integer> buildLongLineage(RDD<Integer> source) {
        return source
                .map(value -> value * 2)
                .filter(value -> value > 5)
                .map(value -> value + 10)
                .filter(value -> value < 30)
                .map(value -> value * 3)
                .filter(value -> value > 30)
                .map(value -> value - 5)
                .map(value -> value + 1);
    }

    private static RDD<Integer> traceUp(RDD<?> rdd, int steps) {
        RDD<?> current = rdd;
        for (int index = 0; index < steps; index++) {
            current = current.dependencies().get(0).rdd();
        }
        @SuppressWarnings("unchecked")
        RDD<Integer> result = (RDD<Integer>) current;
        return result;
    }

    private static void printComputeCount(
            RDD<?> source,
            RDD<?> middle,
            RDD<?> last) {
        System.out.printf(
                "compute 次数: 源头=%d，中间点=%d，末端=%d%n",
                source.getComputeCount(),
                middle.getComputeCount(),
                last.getComputeCount());
    }

    private static final int SAMPLE_COUNT = 6;

    private static void runFileSourceTrainingDemo(SparkContext sc) throws IOException {
        System.out.println();
        System.out.println("小例子: 从文件加载一维逻辑回归训练数据，看同一批数据会不会被反复读");
        Path trainingFile = writeTrainingFile();
        try {
            runFileTrainingCase(sc, trainingFile, false);
            runFileTrainingCase(sc, trainingFile, true);
        } finally {
            Files.deleteIfExists(trainingFile);
        }
    }

    private static void runFileTrainingCase(
            SparkContext sc,
            Path trainingFile,
            boolean cacheSource) {
        String title = cacheSource ? "cache 训练数据" : "不缓存训练数据";
        System.out.println(title + ":");

        RDD<String> fileRDD = sc.textFile(trainingFile.toString(), 3);
        RDD<Sample> source = fileRDD.map(Main::parseSample);
        if (cacheSource) {
            source.cache();
        }

        double weight = 0.0;
        double learningRate = 0.1;
        for (int epoch = 1; epoch <= 3; epoch++) {
            fileRDD.resetComputeCount();
            double currentWeight = weight;
            double gradient = source
                    .map(sample -> (sigmoid(currentWeight * sample.x()) - sample.y()) * sample.x())
                    .reduce(Double::sum);
            weight -= learningRate * gradient / SAMPLE_COUNT;
            System.out.printf(
                    Locale.ROOT,
                    "  第 %d 轮: w=%.4f, gradient=%.4f, FileRDD.compute=%d%n",
                    epoch,
                    weight,
                    gradient,
                    fileRDD.getComputeCount());
        }
    }

    private static Path writeTrainingFile() throws IOException {
        Path tmp = Files.createTempFile("ch11-training", ".csv");
        Files.write(tmp, List.of(
                "0.5,0.0",
                "1.0,0.0",
                "2.0,0.0",
                "3.0,1.0",
                "4.0,1.0",
                "5.0,1.0"));
        return tmp;
    }

    private static void deleteTree(Path root) throws IOException {
        if (!Files.exists(root)) {
            return;
        }
        try (var paths = Files.walk(root)) {
            for (Path path : paths.sorted(java.util.Comparator.reverseOrder()).toList()) {
                Files.deleteIfExists(path);
            }
        }
    }

    private static Sample parseSample(String line) {
        String[] parts = line.split(",");
        return new Sample(Double.parseDouble(parts[0]), Double.parseDouble(parts[1]));
    }

    private record Sample(double x, double y) {
    }

    private static double sigmoid(double z) {
        return 1.0 / (1.0 + Math.exp(-z));
    }
}
