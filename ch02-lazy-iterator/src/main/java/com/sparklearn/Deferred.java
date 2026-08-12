package com.sparklearn;

import java.util.function.Supplier;

/**
 * 把一段尚未执行的计算包装起来，在第一次 {@link #get()} 时计算并缓存结果。
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
