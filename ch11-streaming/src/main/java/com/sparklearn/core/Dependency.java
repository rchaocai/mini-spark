package com.sparklearn.core;

import java.io.Serializable;

/**
 * RDD 之间的依赖关系：血缘链上的一个环节。
 *
 * @param <T> 父 RDD 的元素类型
 */
public sealed interface Dependency<T> extends Serializable
        permits NarrowDependency, ShuffleDependency {

    /**
     * 这条依赖指向的父 RDD。
     */
    RDD<T> rdd();
}
