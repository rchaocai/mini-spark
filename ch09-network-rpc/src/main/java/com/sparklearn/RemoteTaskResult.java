package com.sparklearn;

import java.io.Serializable;

/**
 * Worker 回传给 Driver 的任务执行结果。
 */
public record RemoteTaskResult<T>(
        boolean success,
        T value,
        Throwable error) implements Serializable {

    public static <T> RemoteTaskResult<T> success(T value) {
        return new RemoteTaskResult<>(true, value, null);
    }

    public static <T> RemoteTaskResult<T> failure(Throwable error) {
        return new RemoteTaskResult<>(
                false,
                null,
                error);
    }
}
