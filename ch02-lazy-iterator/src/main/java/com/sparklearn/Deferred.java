package com.sparklearn;

import java.util.function.Supplier;

/**
 * 第 2 章垫脚石：把一段「还没执行的计算」包装起来，只在第一次 {@link #get()} 时算一次。
 *
 * <p>Deferred 的作用是让你直观感受什么叫「延迟」——构造对象时，计算还没发生；
 * 真正调用 {@code get()} 时，才第一次触发。后续再调 {@code get()} 直接返回缓存值。
 *
 * <p>这个模式贯穿全书：RDD 的 {@code compute()} 也是「延迟」的——
 * 构造 RDD 时不运行，直到有东西来消费迭代器。
 *
 * @param <T> 值的类型
 */
public class Deferred<T> {

    private final Supplier<T> supplier;
    private boolean evaluated = false;
    private T value;

    /**
     * @param supplier 一段「待执行的计算」——调用方传入时，它还没运行。
     */
    public Deferred(Supplier<T> supplier) {
        this.supplier = supplier;
    }

    /**
     * 获取计算结果。第一次调用时触发计算并缓存，后续直接返回缓存值。
     * 控制台会打印日志，让你清楚看到计算发生的时机。
     */
    public T get() {
        if (!evaluated) {
            System.out.println("  [Deferred] 第一次 get()——触发计算...");
            value = supplier.get();
            evaluated = true;
        } else {
            System.out.println("  [Deferred] 已缓存，直接返回，不再重复计算");
        }
        return value;
    }
}
