package com.sparklearn.core.scheduler;

import java.io.Serializable;
import java.util.List;
import java.util.Objects;

/**
 * 一个 Map 分区输出的位置和各 Reduce block 的大小。
 *
 * <p>对应 Spark 的 MapStatus：Driver 只保存元数据，shuffle 数据仍留在产出它的
 * Executor 本地磁盘上。
 */
public record MapOutputStatus(
        int mapId,
        String executorAddress,
        List<String> blockFiles,
        List<Long> blockSizes) implements Serializable {

    public MapOutputStatus {
        if (mapId < 0) {
            throw new IllegalArgumentException("mapId must not be negative");
        }
        Objects.requireNonNull(blockFiles, "blockFiles");
        Objects.requireNonNull(blockSizes, "blockSizes");
        blockFiles = List.copyOf(blockFiles);
        blockSizes = List.copyOf(blockSizes);
        if (blockFiles.size() != blockSizes.size()) {
            throw new IllegalArgumentException(
                    "blockFiles and blockSizes must have the same size");
        }
    }

    public boolean remote() {
        return executorAddress != null;
    }

    public String blockFile(int reduceId) {
        return blockFiles.get(reduceId);
    }

    public long blockSize(int reduceId) {
        return blockSizes.get(reduceId);
    }
}
