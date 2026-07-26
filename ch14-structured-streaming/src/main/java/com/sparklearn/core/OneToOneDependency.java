package com.sparklearn.core;

import java.util.List;

/**
 * 一对一窄依赖：子分区 i 只依赖父分区 i。
 *
 * @param <T> 父 RDD 的元素类型
 */
public final class OneToOneDependency<T> extends NarrowDependency<T> {

    public OneToOneDependency(RDD<T> rdd) {
        super(rdd);
    }

    @Override
    public List<Integer> getParents(int outputPartition) {
        return List.of(outputPartition);
    }
}
