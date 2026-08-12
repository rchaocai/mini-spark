package com.sparklearn.core.rpc;

import com.sparklearn.core.storage.BlockId;

import java.io.Serializable;
import java.util.List;

/**
 * Executor 回传给 Driver 的任务执行结果。
 *
 * <p>除了 Task 的返回值，还携带本次 Task 执行期间新缓存的 BlockId 列表，
 * 供 Driver 的 BlockManagerMaster 更新缓存位置，实现缓存感知的数据本地性。
 */
public record RemoteTaskResult<T>(
        boolean success,
        T value,
        Throwable error,
        List<BlockId> cachedBlocks) implements Serializable {

    public RemoteTaskResult {
        if (cachedBlocks == null) {
            cachedBlocks = List.of();
        }
    }

    public static <T> RemoteTaskResult<T> success(T value) {
        return new RemoteTaskResult<>(true, value, null, List.of());
    }

    public static <T> RemoteTaskResult<T> success(T value, List<BlockId> cachedBlocks) {
        return new RemoteTaskResult<>(true, value, null, cachedBlocks);
    }

    public static <T> RemoteTaskResult<T> failure(Throwable error) {
        return new RemoteTaskResult<>(false, null, error, List.of());
    }
}
