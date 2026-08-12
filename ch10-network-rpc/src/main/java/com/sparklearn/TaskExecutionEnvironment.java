package com.sparklearn;

/**
 * Executor 工作线程持有的运行时信息。
 *
 * <p>它类似 SparkEnv 的一个极小切片：Task 本身仍是纯序列化对象，真正执行时再由
 * Executor 注入对外地址和本地磁盘目录。
 */
final class TaskExecutionEnvironment {

    private static final ThreadLocal<Environment> CURRENT = new ThreadLocal<>();

    private TaskExecutionEnvironment() {
    }

    static void set(String executorAddress, String localDir) {
        CURRENT.set(new Environment(executorAddress, localDir));
    }

    static Environment current() {
        return CURRENT.get();
    }

    static void clear() {
        CURRENT.remove();
    }

    record Environment(String executorAddress, String localDir) {
    }
}
