package com.sparklearn;

import java.io.Serializable;

/** Executor 间读取 shuffle block 的请求。 */
public record ShuffleBlockRequest(
        String file) implements Serializable {
}
