package com.sparklearn.mllib;

import com.sparklearn.core.KeyValuePair;
import com.sparklearn.core.rdd.RDD;
import com.sparklearn.core.SparkContext;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import java.util.List;
import java.util.Map;

class MllibTest {

    @Test
    void testLogisticRegressionPrediction() {
        List<LabeledPoint> irisData = MllibTestHelper.loadIrisData();
        assertFalse(irisData.isEmpty());

        try (SparkContext sc = new SparkContext(2)) {
            RDD<LabeledPoint> dataRDD = sc.parallelize(irisData, 2);

            LogisticRegression model = new LogisticRegression(0.1, 50, 1e-6);
            model.train(sc, dataRDD);

            // 预测 setosa 样本
            double[] setosaFeatures = {5.0, 3.5, 1.5, 0.2};
            int prediction = model.predict(setosaFeatures);
            assertEquals(0, prediction, "Setosa 样本应预测为 0");

            // 预测 versicolor 样本
            double[] versicolorFeatures = {6.0, 2.7, 4.0, 1.2};
            prediction = model.predict(versicolorFeatures);
            assertEquals(1, prediction, "Versicolor 样本应预测为 1");
        }
    }

    @Test
    void testPageRankConvergence() {
        List<KeyValuePair<String, List<String>>> graph = List.of(
                new KeyValuePair<>("A", List.of("B", "C")),
                new KeyValuePair<>("B", List.of("C")),
                new KeyValuePair<>("C", List.of("A")),
                new KeyValuePair<>("D", List.of("A", "C")),
                new KeyValuePair<>("E", List.of("B")),
                new KeyValuePair<>("F", List.of("D"))
        );

        try (SparkContext sc = new SparkContext(2)) {
            RDD<KeyValuePair<String, List<String>>> graphRDD = sc.parallelize(graph, 2);

            PageRank pageRank = new PageRank(0.85, 50, 1e-8);
            Map<String, Double> ranks = pageRank.run(sc, graphRDD);

            // 验证所有网页都有排名
            assertEquals(6, ranks.size());

            // 验证排名总和接近 1
            double total = ranks.values().stream().mapToDouble(Double::doubleValue).sum();
            assertTrue(Math.abs(total - 1.0) < 0.001, "排名总和应接近 1.0");

            // 验证 C 排名最高（被 A、B、D 三个网页指向）
            assertTrue(ranks.get("C") > ranks.get("A"),
                    "C 应排名高于 A");
            assertTrue(ranks.get("C") > ranks.get("B"),
                    "C 应排名高于 B");

            // E、F 没有入链，只保留随机跳转基础分 (1 - 0.85) / 6 = 0.025。
            assertEquals(0.025, ranks.get("E"), 1e-6);
            assertEquals(0.025, ranks.get("F"), 1e-6);
        }
    }

    @Test
    void testPageRankRedistributesDanglingMass() {
        List<KeyValuePair<String, List<String>>> graph = List.of(
                new KeyValuePair<>("A", List.of("B")),
                new KeyValuePair<>("B", List.of())
        );

        try (SparkContext sc = new SparkContext(2)) {
            RDD<KeyValuePair<String, List<String>>> graphRDD = sc.parallelize(graph, 2);
            Map<String, Double> ranks = new PageRank(0.85, 100, 1e-10).run(sc, graphRDD);

            double total = ranks.values().stream().mapToDouble(Double::doubleValue).sum();
            assertEquals(1.0, total, 1e-9, "悬挂网页的 rank 不应丢失");
            assertTrue(ranks.get("B") > ranks.get("A"), "A 指向 B，B 应获得更高排名");
        }
    }
}
