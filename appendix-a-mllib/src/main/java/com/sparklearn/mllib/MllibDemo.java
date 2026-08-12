package com.sparklearn.mllib;

import com.sparklearn.core.KeyValuePair;
import com.sparklearn.core.rdd.RDD;
import com.sparklearn.core.SparkContext;
import com.sparklearn.core.scheduler.NetworkTaskScheduler;
import com.sparklearn.core.scheduler.TaskScheduler;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * 附录 A · mini-spark 机器学习实战演示。
 *
 * <p>包含两个经典机器学习算法 + cache 对比实验：
 * <ol>
 *   <li>Cache 对比：用 FileRDD 读取数据，对比有/无 cache 的训练耗时</li>
 *   <li>逻辑回归（Logistic Regression）—— 使用 Iris 数据集训练二分类模型</li>
 *   <li>PageRank —— 使用小型网页图演示链接分析</li>
 * </ol>
 *
 * <p>运行模式：
 * <ul>
 *   <li>无参：使用 LocalTaskScheduler（单机线程池），直接 {@code mvn exec:java} 即可运行。</li>
 *   <li>{@code network <executor1> <executor2> ...}：使用 NetworkTaskScheduler，Task 通过
 *       Socket 发到独立 JVM 的 Executor 执行——和第 10/15 章演示的部署模式一致。
 *       使用前需先在独立终端启动 Executor 进程，例如：
 *       <pre>
 * java -cp &lt;classpath&gt; com.sparklearn.core.executor.Executor 40000 localhost 2
 * java -cp &lt;classpath&gt; com.sparklearn.core.executor.Executor 40001 localhost 2
 *       </pre>
 *       然后运行：
 *       <pre>
 * mvn exec:java -Dexec.mainClass=com.sparklearn.mllib.MllibDemo \
 *     -Dexec.args="network localhost:40000 localhost:40001"
 *       </pre>
 *   </li>
 * </ul>
 */
public final class MllibDemo {

    private MllibDemo() {
    }

    public static void main(String[] args) {
        System.out.println("=".repeat(78));
        System.out.println("附录 A · mini-spark 机器学习实战");
        System.out.println("=".repeat(78));

        boolean networkMode = args.length > 0 && "network".equals(args[0]);
        List<String> executorAddresses = networkMode
                ? new ArrayList<>(Arrays.asList(args).subList(1, args.length))
                : List.of();

        System.out.println(networkMode
                ? "运行模式: network（Executor 地址: " + executorAddresses + "）"
                : "运行模式: local（单机线程池）");
        System.out.println("提示: network 模式需先在独立终端启动 Executor 进程");

        runCacheComparison(networkMode, executorAddresses);
        runLogisticRegression(networkMode, executorAddresses);
        runPageRank(networkMode, executorAddresses);
    }

    /**
     * 根据运行模式构造 SparkContext。
     *
     * <p>network 模式下使用 {@link NetworkTaskScheduler}，Task 序列化后通过 Socket
     * 发到 Executor JVM 执行；local 模式下使用 {@code LocalTaskScheduler}，Task 在
     * Driver 同 JVM 的线程池里执行。两种模式下 RDD 算子语义完全一致。
     */
    private static SparkContext createSparkContext(
            boolean networkMode,
            List<String> executorAddresses) {
        if (networkMode) {
            if (executorAddresses.isEmpty()) {
                throw new IllegalArgumentException(
                        "network 模式需要至少一个 Executor 地址，例如: network localhost:40000");
            }
            TaskScheduler scheduler = new NetworkTaskScheduler(executorAddresses);
            return new SparkContext(scheduler, true);
        }
        return new SparkContext(2, true);
    }

    /**
     * Cache 对比实验：用 FileRDD 加载数据，对比有/无 cache 的训练耗时。
     *
     * <p>训练开始会执行 count()、map+reduce 各一次，随后每轮迭代执行梯度 collect、
     * 损失 reduce 各一次——每个 action 都会触发 FileRDD.compute()。无 cache 时，
     * 这些 action 每次都重新读文件；cache 后，第一次读取会填充缓存，后续直接走内存。
     */
    private static void runCacheComparison(boolean networkMode, List<String> executorAddresses) {
        System.out.println();
        System.out.println("-".repeat(78));
        System.out.println("【实验】Cache 对机器学习训练的加速效果");
        System.out.println("-".repeat(78));

        Path dataFile = prepareLargeDataset();
        System.out.printf("%n数据文件: %s%n", dataFile.toAbsolutePath());

        try (SparkContext sc = createSparkContext(networkMode, executorAddresses)) {
            // ========== 实验 1：无 Cache ==========
            System.out.println("\n--- 实验 1：无 Cache（每次迭代重读文件）---");

            RDD<String> fileRDD1 = sc.textFile(dataFile.toString(), 2);
            RDD<LabeledPoint> dataNoCache = fileRDD1.map(MllibDemo::parseLine);

            long startNoCache = System.nanoTime();
            LogisticRegression model1 = new LogisticRegression(0.1, 50, 1e-6);
            model1.train(sc, dataNoCache);
            long elapsedNoCache = System.nanoTime() - startNoCache;

            // ========== 实验 2：有 Cache ==========
            System.out.println("\n--- 实验 2：有 Cache（第一次读文件，后续走内存）---");

            RDD<String> fileRDD2 = sc.textFile(dataFile.toString(), 2);
            RDD<LabeledPoint> dataWithCache = fileRDD2.map(MllibDemo::parseLine);
            dataWithCache.cache();

            long startCache = System.nanoTime();
            LogisticRegression model2 = new LogisticRegression(0.1, 50, 1e-6);
            model2.train(sc, dataWithCache);
            long elapsedCache = System.nanoTime() - startCache;

            // ========== 对比结果 ==========
            System.out.println("\n--- 对比结果 ---");
            System.out.printf("  无 Cache:  %8.2f ms%n", elapsedNoCache / 1_000_000.0);
            System.out.printf("  有 Cache:  %8.2f ms%n", elapsedCache / 1_000_000.0);
            double speedup = (double) elapsedNoCache / elapsedCache;
            System.out.printf("  加速比:    %8.2fx%n", speedup);

            if (speedup > 1.5) {
                System.out.println("\n  Cache 效果显著，文件 I/O 被内存访问替代。");
            } else {
                System.out.println("\n  Cache 有一定加速效果，数据量增大后差异会更明显。");
            }
        }
    }

    /**
     * 逻辑回归：使用 Iris 数据集训练二分类模型。
     */
    private static void runLogisticRegression(boolean networkMode, List<String> executorAddresses) {
        System.out.println();
        System.out.println("-".repeat(78));
        System.out.println("【算法 1】逻辑回归（Logistic Regression）");
        System.out.println("-".repeat(78));

        List<LabeledPoint> irisData = loadIrisData();
        System.out.println("\nIris 二分类数据集:");
        System.out.println("  样本数量: " + irisData.size());
        System.out.println("  特征维度: " + irisData.get(0).numFeatures());
        System.out.println("  类别 0 (setosa): " + irisData.stream().filter(p -> p.label() == 0).count());
        System.out.println("  类别 1 (versicolor/virginica): " + irisData.stream().filter(p -> p.label() == 1).count());

        List<LabeledPoint> trainData = new ArrayList<>();
        List<LabeledPoint> testData = new ArrayList<>();
        for (int i = 0; i < irisData.size(); i++) {
            if (i % 5 == 4) {
                testData.add(irisData.get(i));
            } else {
                trainData.add(irisData.get(i));
            }
        }
        System.out.println("  训练集: " + trainData.size() + " 样本");
        System.out.println("  测试集: " + testData.size() + " 样本");

        try (SparkContext sc = createSparkContext(networkMode, executorAddresses)) {
            RDD<LabeledPoint> trainRDD = sc.parallelize(trainData, 2);

            LogisticRegression model = new LogisticRegression(0.1, 100, 1e-6);
            model.train(sc, trainRDD);

            System.out.println("\n--- 模型评估 ---");
            int correct = 0;
            int total = testData.size();
            for (LabeledPoint point : testData) {
                int predicted = model.predict(point.features());
                if (predicted == (int) point.label()) {
                    correct++;
                }
            }
            double accuracy = (double) correct / total * 100;
            System.out.printf("  准确率: %d/%d = %.1f%%%n", correct, total, accuracy);

            System.out.println("\n--- 预测示例 ---");
            double[][] sampleFeatures = {
                    {5.0, 3.5, 1.5, 0.2},
                    {6.0, 2.7, 4.0, 1.2},
                    {7.5, 3.8, 6.4, 2.0},
            };
            String[] expectedLabels = {"setosa (0)", "versicolor (1)", "virginica (1)"};
            for (int i = 0; i < sampleFeatures.length; i++) {
                double prob = model.predictProbability(sampleFeatures[i]);
                int pred = model.predict(sampleFeatures[i]);
                System.out.printf("  样本%d: 特征=%s, P(y=1)=%.4f, 预测=%d, 期望=%s%n",
                        i + 1,
                        Arrays.toString(sampleFeatures[i]),
                        prob, pred, expectedLabels[i]);
            }
        }
    }

    /**
     * PageRank：使用小型网页链接图演示算法。
     */
    private static void runPageRank(boolean networkMode, List<String> executorAddresses) {
        System.out.println();
        System.out.println("-".repeat(78));
        System.out.println("【算法 2】PageRank 链接分析");
        System.out.println("-".repeat(78));

        List<KeyValuePair<String, List<String>>> graph = List.of(
                new KeyValuePair<>("A", List.of("B", "C")),
                new KeyValuePair<>("B", List.of("C")),
                new KeyValuePair<>("C", List.of("A")),
                new KeyValuePair<>("D", List.of("A", "C")),
                new KeyValuePair<>("E", List.of("B")),
                new KeyValuePair<>("F", List.of("D"))
        );

        System.out.println("\n示例网页图:");
        System.out.println("  节点数: " + graph.size());
        for (KeyValuePair<String, List<String>> entry : graph) {
            System.out.println("  " + entry.key() + " → " + entry.value());
        }

        try (SparkContext sc = createSparkContext(networkMode, executorAddresses)) {
            RDD<KeyValuePair<String, List<String>>> graphRDD = sc.parallelize(graph, 2);

            PageRank pageRank = new PageRank(0.85, 50, 1e-8);
            Map<String, Double> ranks = pageRank.run(sc, graphRDD);

            System.out.println("\n--- PageRank 排名结果 ---");
            List<Map.Entry<String, Double>> sortedRanks = new ArrayList<>(ranks.entrySet());
            sortedRanks.sort(Map.Entry.<String, Double>comparingByValue().reversed());

            int rank = 1;
            for (Map.Entry<String, Double> entry : sortedRanks) {
                System.out.printf("  #%d %s: %.6f%n", rank++, entry.getKey(), entry.getValue());
            }

            double total = ranks.values().stream().mapToDouble(Double::doubleValue).sum();
            System.out.printf("%n  Rank 总和: %.6f (应接近 1.0)%n", total);

            System.out.println("\n--- 结果解读 ---");
            System.out.println("  网页 C 排名最高: 被 A、B、D 三个网页指向");
            System.out.println("  网页 A 排名第二: 被 C、D 指向");
            System.out.println("  网页 E、F 排名最低: 出链少且没有被其他高质量网页指向");
        }
    }

    private static LabeledPoint parseLine(String line) {
        String[] parts = line.split(",");
        if (parts.length != 5) {
            throw new IllegalArgumentException("Invalid line: " + line);
        }
        double[] features = new double[4];
        for (int i = 0; i < 4; i++) {
            features[i] = Double.parseDouble(parts[i]);
        }
        return new LabeledPoint(features, Double.parseDouble(parts[4]));
    }

    /**
     * 准备 500 行的扩展数据集（50 条 Iris 数据重复 10 次），写到临时文件。
     *
     * <p>使用临时文件而非 src/main/resources，是因为 network 模式下 Executor 子进程
     * 的工作目录不一定是 appendix-a-mllib，用绝对路径才能保证两边都能读到。
     */
    private static Path prepareLargeDataset() {
        try {
            List<String> lines = new ArrayList<>();
            try (InputStream is = MllibDemo.class.getClassLoader()
                    .getResourceAsStream("iris_binary.csv");
                 BufferedReader reader = new BufferedReader(
                         new InputStreamReader(is, StandardCharsets.UTF_8))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    line = line.trim();
                    if (!line.isEmpty()) {
                        lines.add(line);
                    }
                }
            }

            List<String> expanded = new ArrayList<>();
            for (int r = 0; r < 10; r++) {
                expanded.addAll(lines);
            }

            Path tempFile = Files.createTempFile("iris-binary-large-", ".csv");
            Files.write(tempFile, expanded, StandardCharsets.UTF_8);
            tempFile.toFile().deleteOnExit();
            return tempFile;
        } catch (Exception e) {
            throw new RuntimeException("准备扩展数据集失败", e);
        }
    }

    private static List<LabeledPoint> loadIrisData() {
        List<LabeledPoint> data = new ArrayList<>();
        try (InputStream is = MllibDemo.class.getClassLoader().getResourceAsStream("iris_binary.csv");
             BufferedReader reader = new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8))) {
            String line;
            while ((line = reader.readLine()) != null) {
                line = line.trim();
                if (line.isEmpty()) continue;

                String[] parts = line.split(",");
                if (parts.length != 5) continue;

                double[] features = new double[4];
                for (int i = 0; i < 4; i++) {
                    features[i] = Double.parseDouble(parts[i]);
                }
                double label = Double.parseDouble(parts[4]);
                data.add(new LabeledPoint(features, label));
            }
        } catch (Exception e) {
            throw new RuntimeException("加载 Iris 数据集失败", e);
        }
        return data;
    }
}
