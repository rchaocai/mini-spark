package com.sparklearn;

/**
 * 第 1 章 · 从 WordCount 开始 —— 模块入口。
 *
 * <p>真正的实现在 {@link WordCount}，这里只是转发，方便用一条命令跑起来。
 */
public final class Main {
    private Main() {
    }

    public static void main(String[] args) {
        WordCount.main(args);
    }
}
