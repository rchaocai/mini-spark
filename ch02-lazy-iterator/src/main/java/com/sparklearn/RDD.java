package com.sparklearn;

import java.util.Iterator;

/**
 * RDD 的最小抽象：一个能按需生成数据迭代器的数据集。
 *
 * @param <T> 元素类型
 */
public abstract class RDD<T> {

    /**
     * 返回一个迭代器，描述如何访问这个 RDD 的数据。
     */
    public abstract Iterator<T> compute();
}
