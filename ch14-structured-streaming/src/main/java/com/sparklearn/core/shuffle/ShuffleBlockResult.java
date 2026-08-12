package com.sparklearn.core.shuffle;

import java.io.Serializable;

/** Executor 间读取 shuffle block 的响应。 */
public record ShuffleBlockResult(
        boolean success,
        byte[] bytes,
        Throwable error) implements Serializable {

    public static ShuffleBlockResult success(byte[] bytes) {
        return new ShuffleBlockResult(true, bytes, null);
    }

    public static ShuffleBlockResult failure(Throwable error) {
        return new ShuffleBlockResult(false, null, error);
    }
}
