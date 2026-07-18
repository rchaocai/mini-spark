package com.sparklearn;

import java.io.Serializable;

/**
 * Driver 发送给 Executor 的任务请求。
 */
public record RemoteTaskRequest<T>(
        Task<T> task,
        int attemptId) implements Serializable {
}
