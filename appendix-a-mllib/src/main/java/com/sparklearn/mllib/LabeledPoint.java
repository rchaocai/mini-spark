package com.sparklearn.mllib;

import java.io.Serializable;
import java.util.Arrays;

/**
 * 带标签的训练样本——特征向量 + 二分类标签(0 或 1)。
 *
 * <p>实现 {@link Serializable} 是因为 network 模式下 Task 闭包和 {@code parallelize}
 * 的数据都要从 Driver 序列化到 Executor，LabeledPoint 作为样本载体必须可序列化。
 */
public record LabeledPoint(double[] features, double label) implements Serializable {

    public int numFeatures() {
        return features.length;
    }

    public double feature(int index) {
        return features[index];
    }

    @Override
    public String toString() {
        return "LabeledPoint[features=" + Arrays.toString(features) + ", label=" + label + "]";
    }
}
