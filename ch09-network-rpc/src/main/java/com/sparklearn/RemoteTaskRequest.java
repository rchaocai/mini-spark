package com.sparklearn;

import java.io.Serializable;

/**
 * Driver 发送给 Worker 的任务请求。
 */
public record RemoteTaskRequest<T>(
        Task<T> task,
        int attemptId) implements Serializable {
}

