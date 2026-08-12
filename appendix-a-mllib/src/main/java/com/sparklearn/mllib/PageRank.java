package com.sparklearn.mllib;

import com.sparklearn.core.KeyValuePair;
import com.sparklearn.core.rdd.RDD;
import com.sparklearn.core.SparkContext;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * PageRank 算法——使用 mini-spark 的 RDD API 实现迭代式链接分析。
 *
 * <p>核心公式: PR(A) = (1 - d) / N + d × (danglingMass / N + Σ(PR(Ti) / C(Ti)))
 * <ul>
 *   <li>d: 阻尼系数（通常 0.85）</li>
 *   <li>N: 所有网页总数</li>
 *   <li>danglingMass: 所有没有出链网页的 PageRank 总和</li>
 *   <li>Ti: 指向 A 的所有网页</li>
 *   <li>C(Ti): 网页 Ti 的出链数量</li>
 * </ul>
 *
 * <p>实现步骤:
 * <ol>
 *   <li>初始化每个网页的 PageRank 为 1/N</li>
 *   <li>迭代: 有出链网页将自己的 rank 平均分配给出链</li>
 *   <li>通过 reduceByKey 汇总每个网页的入链 rank</li>
 *   <li>把悬挂网页的 rank 均分给所有网页，再加上随机跳转项</li>
 * </ol>
 */
public class PageRank {

    private final double dampingFactor;
    private final int maxIterations;
    private final double convergenceTol;

    /**
     * @param dampingFactor 阻尼系数 d，一般取 0.85
     * @param maxIterations 最大迭代次数
     * @param convergenceTol 收敛阈值
     */
    public PageRank(double dampingFactor, int maxIterations, double convergenceTol) {
        this.dampingFactor = dampingFactor;
        this.maxIterations = maxIterations;
        this.convergenceTol = convergenceTol;
    }

    /**
     * 运行 PageRank 算法。
     *
     * @param sc   SparkContext
     * @param graph 网页链接图 RDD，每个键值对表示 (网页, 出链列表)
     * @return 每个网页的 PageRank 值
     */
    public Map<String, Double> run(SparkContext sc, RDD<KeyValuePair<String, List<String>>> graph) {
        List<KeyValuePair<String, List<String>>> graphData = graph.collect();
        if (graphData.isEmpty()) {
            throw new IllegalArgumentException("网页图不能为空");
        }

        // 收集所有网页：既包括源网页，也包括只出现在目标位置的网页。
        List<String> allPages = new ArrayList<>();
        for (KeyValuePair<String, List<String>> entry : graphData) {
            if (!allPages.contains(entry.key())) {
                allPages.add(entry.key());
            }
            for (String target : entry.value()) {
                if (!allPages.contains(target)) {
                    allPages.add(target);
                }
            }
        }
        int pageCount = allPages.size();

        // 初始时不知道谁更重要，所以每个网页都从 1/N 开始。
        Map<String, Double> pageRank = new HashMap<>();
        for (String page : allPages) {
            pageRank.put(page, 1.0 / pageCount);
        }

        System.out.println("开始 PageRank 迭代...");
        System.out.println("  网页总数: " + pageCount);
        System.out.println("  阻尼系数: " + dampingFactor);
        System.out.println("  最大迭代次数: " + maxIterations);

        // 链接拓扑在迭代过程中不变，收集一次后作为只读查找表被每轮任务复用。
        final Map<String, List<String>> graphMap = new HashMap<>();
        for (KeyValuePair<String, List<String>> entry : graphData) {
            graphMap.put(entry.key(), entry.value());
        }

        for (int iteration = 0; iteration < maxIterations; iteration++) {
            final Map<String, Double> currentRank = new HashMap<>(pageRank);

            // 把当前 rank 数据转为 RDD 用于分布式计算
            List<KeyValuePair<String, Double>> rankList = new ArrayList<>();
            for (Map.Entry<String, Double> entry : currentRank.entrySet()) {
                rankList.add(new KeyValuePair<>(entry.getKey(), entry.getValue()));
            }

            RDD<KeyValuePair<String, Double>> rankRDD = sc.parallelize(rankList, 2);

            // 没有出链的网页称为悬挂网页。标准 PageRank 会把它们的 rank
            // 均匀分给所有网页，否则这部分概率质量会在迭代中丢失。
            double danglingMass = 0.0;
            for (Map.Entry<String, Double> entry : currentRank.entrySet()) {
                List<String> outgoingLinks = graphMap.getOrDefault(entry.getKey(), List.of());
                if (outgoingLinks.isEmpty()) {
                    danglingMass += entry.getValue();
                }
            }

            // 每个网页把自己的 rank 分配给出链
            RDD<KeyValuePair<String, Double>> contributionRDD = rankRDD
                    .flatMap(pair -> {
                        List<KeyValuePair<String, Double>> result = new ArrayList<>();
                        String page = pair.key();
                        double rank = pair.value();
                        List<String> outgoingLinks = graphMap.getOrDefault(page, List.of());

                        if (outgoingLinks.isEmpty()) {
                            // 悬挂网页的 rank 在 Driver 端统一均分，这里不发贡献。
                            return result;
                        } else {
                            double share = rank / outgoingLinks.size();
                            for (String target : outgoingLinks) {
                                result.add(new KeyValuePair<>(target, share));
                            }
                        }
                        return result;
                    });

            // 用 reduceByKey 汇总每个网页收到的 rank 贡献
            RDD<KeyValuePair<String, Double>> summedRDD = contributionRDD.reduceByKey(
                    (Double a, Double b) -> a + b, 2);
            List<KeyValuePair<String, Double>> summedContributions = summedRDD.collect();

            Map<String, Double> incomingSums = new HashMap<>();
            for (KeyValuePair<String, Double> entry : summedContributions) {
                incomingSums.put(entry.key(), entry.value());
            }

            // 更新所有网页，包括本轮没有任何入链贡献的网页。
            double maxChange = 0.0;
            Map<String, Double> nextRank = new HashMap<>();
            double randomJump = (1.0 - dampingFactor) / pageCount;
            double danglingShare = danglingMass / pageCount;
            for (String page : allPages) {
                double incoming = incomingSums.getOrDefault(page, 0.0);
                double newRank = randomJump + dampingFactor * (incoming + danglingShare);
                double oldRank = currentRank.getOrDefault(page, 0.0);
                maxChange = Math.max(maxChange, Math.abs(newRank - oldRank));
                nextRank.put(page, newRank);
            }
            pageRank.clear();
            pageRank.putAll(nextRank);

            if (iteration % 5 == 0 || iteration == maxIterations - 1) {
                System.out.printf("  迭代 %3d/%d | 最大变化: %.8f%n",
                        iteration + 1, maxIterations, maxChange);
            }

            if (maxChange < convergenceTol) {
                System.out.printf("  收敛！最大变化: %.8f < 阈值: %.8f%n", maxChange, convergenceTol);
                break;
            }
        }

        // 归一化
        double total = pageRank.values().stream().mapToDouble(Double::doubleValue).sum();
        if (total > 0) {
            pageRank.replaceAll((k, v) -> v / total);
        }

        System.out.println("PageRank 计算完成！");
        return pageRank;
    }
}
