package com.sparklearn.core;

import com.sparklearn.core.NarrowDependency;
import com.sparklearn.core.rdd.RDD;

import java.util.List;

/**
 * 范围窄依赖：父 RDD 的连续一段分区一对一映射到子 RDD 的连续一段分区。
 *
 * <p>对应真实 Spark 的 {@code org.apache.spark.RangeDependency}。UnionRDD 用它把
 * 每个父 RDD 的 [0, n) 分区映射到自身 [pos, pos+n) 分区。
 *
 * @param <T> 父 RDD 的元素类型
 */
public final class RangeDependency<T> extends NarrowDependency<T> {

    private final int inStart;
    private final int outStart;
    private final int length;

    public RangeDependency(RDD<T> rdd, int inStart, int outStart, int length) {
        super(rdd);
        if (length < 0) {
            throw new IllegalArgumentException("length must not be negative");
        }
        this.inStart = inStart;
        this.outStart = outStart;
        this.length = length;
    }

    @Override
    public List<Integer> getParents(int outputPartition) {
        if (outputPartition >= outStart && outputPartition < outStart + length) {
            return List.of(outputPartition - outStart + inStart);
        }
        return List.of();
    }
}
