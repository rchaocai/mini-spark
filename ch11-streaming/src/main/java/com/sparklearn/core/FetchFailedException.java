package com.sparklearn.core;

import java.io.File;
import java.util.Objects;

/**
 * Reduce 端无法读取某个 Map 输出时抛出的结构化异常。
 *
 * <p>普通异常只说明当前 Task 失败；FetchFailedException 还指出丢失输出来自
 * 哪条 ShuffleDependency、哪个 Map 分区，DAGScheduler 因而能回到父 Stage 重算。
 */
public final class FetchFailedException extends RuntimeException {

    private final ShuffleDependency<?, ?> dependency;
    private final int mapId;
    private final int reduceId;
    private final File file;

    public FetchFailedException(
            ShuffleDependency<?, ?> dependency,
            int mapId,
            int reduceId,
            File file,
            Throwable cause) {
        super("读取 shuffle 输出失败: map=" + mapId
                + ", reduce=" + reduceId
                + ", file=" + file, cause);
        this.dependency = Objects.requireNonNull(dependency, "dependency");
        this.mapId = mapId;
        this.reduceId = reduceId;
        this.file = Objects.requireNonNull(file, "file");
    }

    public ShuffleDependency<?, ?> dependency() {
        return dependency;
    }

    public int mapId() {
        return mapId;
    }

    public int reduceId() {
        return reduceId;
    }

    public File file() {
        return file;
    }
}
