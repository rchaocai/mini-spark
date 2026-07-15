package com.sparklearn;

import java.util.Iterator;

/**
 * RDD —— 分布式数据集的抽象（占位骨架）。
 *
 * <p>本章只暴露一个核心：每个 RDD 都能通过 {@link #compute()} 返回一个迭代器，
 * 这个迭代器描述了「怎么算」。完整的 RDD 抽象（分区、依赖、优先位置）
 * 将在第 4 章补全。
 *
 * <p>为什么是抽象类而非接口：后续章节需要添加带有默认实现的方法
 * （如 {@code iterator()} 先查缓存再 compute），用抽象类更自然。
 *
 * @param <T> 元素类型
 */
public abstract class RDD<T> {

    /**
     * 返回一个迭代器，代表「怎么算出这个 RDD 的数据」。
     *
     * <p>每次调用都可能返回一个全新的迭代器——这意味着 RDD 本身
     * <strong>不持有数据副本</strong>，只持有「怎么访问」。
     */
    public abstract Iterator<T> compute();
}
