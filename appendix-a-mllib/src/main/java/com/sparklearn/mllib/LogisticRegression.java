package com.sparklearn.mllib;

import com.sparklearn.core.KeyValuePair;
import com.sparklearn.core.rdd.RDD;
import com.sparklearn.core.SparkContext;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * 逻辑回归（Logistic Regression）梯度下降训练器。
 *
 * <p>使用 mini-spark 的 RDD API 实现批量梯度下降：
 * <ol>
 *   <li>在每个分区计算梯度向量的局部和</li>
 *   <li>通过 reduceByKey 汇总所有分区的梯度</li>
 *   <li>在 Driver 端更新权重向量</li>
 * </ol>
 *
 * <p>模型: P(y=1|x) = σ(w·x + b)，其中 σ 是 sigmoid 函数。
 */
public class LogisticRegression {

    private final double learningRate;
    private final int numIterations;
    private final double convergenceTol;

    private double[] weights;
    private double bias;
    private int featureCount;

    /**
     * @param learningRate   学习率 α
     * @param numIterations  最大迭代次数
     * @param convergenceTol 收敛阈值（权重与偏置的总变化量低于此值时提前停止）
     */
    public LogisticRegression(double learningRate, int numIterations, double convergenceTol) {
        this.learningRate = learningRate;
        this.numIterations = numIterations;
        this.convergenceTol = convergenceTol;
    }

    /**
     * 用 mini-spark 的 RDD API 训练逻辑回归模型。
     *
     * <p>训练全程不把样本收集到 Driver：样本数走分布式 {@code count()}，
     * 特征维度走分布式 {@code map + reduce}，每轮损失也走分布式 {@code map + reduce}。
     * Driver 只持有当前这轮的参数 {@code w}/{@code b}，以及每轮拉回的「特征数 + 1」个梯度聚合值。
     *
     * @param sc   SparkContext
     * @param data 训练数据 RDD
     */
    public void train(SparkContext sc, RDD<LabeledPoint> data) {
        // 样本数：分布式计数，Driver 只拿到一个 long。
        long totalSamples = data.count();
        if (totalSamples == 0) {
            throw new IllegalArgumentException("训练数据不能为空");
        }

        // 特征维度：所有样本维度相同，取任意一个即可；用 reduce 而不是 collect
        // 避免 Driver 端持有整份数据。
        this.featureCount = data.map(LabeledPoint::numFeatures).reduce((a, b) -> a);
        this.weights = new double[featureCount];
        this.bias = 0.0;

        System.out.println("开始训练逻辑回归...");
        System.out.println("  特征维度: " + featureCount);
        System.out.println("  样本数量: " + totalSamples);
        System.out.println("  学习率: " + learningRate);
        System.out.println("  最大迭代次数: " + numIterations);

        List<Double> previousWeights = null;

        final int numFeatures = featureCount;

        for (int iteration = 0; iteration < numIterations; iteration++) {
            final double[] currentWeights = weights;
            final double currentBias = bias;

            // 每个样本计算梯度向量，再用 reduceByKey 汇总
            RDD<KeyValuePair<Integer, Double>> gradsRDD = data
                    .flatMap(point -> {
                        List<KeyValuePair<Integer, Double>> grads = new ArrayList<>();
                        double prediction = sigmoid(dot(currentWeights, point.features()) + currentBias);
                        double error = prediction - point.label();

                        for (int j = 0; j < numFeatures; j++) {
                            grads.add(new KeyValuePair<>(j, error * point.feature(j)));
                        }
                        grads.add(new KeyValuePair<>(numFeatures, error));
                        return grads;
                    });

            RDD<KeyValuePair<Integer, Double>> summedGrads = gradsRDD.reduceByKey(
                    (Double a, Double b) -> a + b, 2);

            List<KeyValuePair<Integer, Double>> gradients = summedGrads.collect();

            // 计算梯度的平均值并更新权重
            double maxGradientNorm = 0.0;

            for (KeyValuePair<Integer, Double> grad : gradients) {
                double avgGrad = grad.value() / totalSamples;
                maxGradientNorm = Math.max(maxGradientNorm, Math.abs(avgGrad));

                if (grad.key() < featureCount) {
                    weights[grad.key()] -= learningRate * avgGrad;
                } else {
                    bias -= learningRate * avgGrad;
                }
            }

            // 每轮损失：分布式 map 算单样本损失，reduce 求和，Driver 再除以 N。
            double loss = computeLossDistributed(data, currentWeights, currentBias, totalSamples);

            if (iteration % 10 == 0 || iteration == numIterations - 1) {
                System.out.printf("  迭代 %3d/%d | 损失: %.6f | 最大梯度: %.6f%n",
                        iteration + 1, numIterations, loss, maxGradientNorm);
            }

            // 收敛检查
            if (previousWeights != null) {
                double change = 0.0;
                for (int j = 0; j < featureCount; j++) {
                    change += Math.abs(weights[j] - previousWeights.get(j));
                }
                change += Math.abs(bias - (previousWeights.get(featureCount)));
                if (change < convergenceTol) {
                    System.out.printf("  收敛！权重变化: %.6f < 阈值: %.6f%n", change, convergenceTol);
                    break;
                }
            }

            previousWeights = new ArrayList<>(featureCount + 1);
            for (double w : weights) {
                previousWeights.add(w);
            }
            previousWeights.add(bias);
        }

        System.out.println("训练完成！");
        System.out.println("  权重: " + Arrays.toString(weights));
        System.out.println("  偏置: " + bias);
    }

    /**
     * 预测样本为正类的概率 P(y=1|x)。
     */
    public double predictProbability(double[] features) {
        return sigmoid(dot(weights, features) + bias);
    }

    /**
     * 预测类别（0 或 1），阈值 0.5。
     */
    public int predict(double[] features) {
        return predictProbability(features) >= 0.5 ? 1 : 0;
    }

    public double[] getWeights() {
        return weights.clone();
    }

    public double getBias() {
        return bias;
    }

    /**
     * 分布式计算交叉熵损失。
     *
     * <p>每个样本的损失在 Executor 端算出，Driver 只收到一个总和。
     * 这样无论训练集多大，拉回 Driver 的都只有一个 double，而不是整份样本。
     */
    private double computeLossDistributed(
            RDD<LabeledPoint> data,
            double[] currentWeights,
            double currentBias,
            long totalSamples) {
        double totalLoss = data.map(point -> {
            double prediction = sigmoid(dot(currentWeights, point.features()) + currentBias);
            prediction = Math.max(1e-15, Math.min(1 - 1e-15, prediction));
            return point.label() * Math.log(prediction)
                    + (1 - point.label()) * Math.log(1 - prediction);
        }).reduce((Double a, Double b) -> a + b);
        return -totalLoss / totalSamples;
    }

    private static double sigmoid(double z) {
        return 1.0 / (1.0 + Math.exp(-z));
    }

    private static double dot(double[] a, double[] b) {
        double sum = 0.0;
        for (int i = 0; i < a.length; i++) {
            sum += a[i] * b[i];
        }
        return sum;
    }
}
