package com.sparklearn.streaming.structured;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.AbstractMap;
import java.util.Comparator;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

/**
 * 基于本地文件系统的 {@link MetadataLog} 实现。
 * <p>
 * 参考 Spark 源码：{@code org.apache.spark.sql.execution.streaming.HDFSMetadataLog}。
 * <p>
 * 存储格式：每个 batchId 对应一个文件，文件名为 batchId 数字字符串，
 * 内容为 Java 序列化的元数据对象。写入时先写临时文件（{@code {uuid}.tmp}），
 * 再原子 rename 为最终文件名——对应 Spark {@code HDFSMetadataLog.writeBatch}
 * 的 {@code fileManager.rename(tempPath, batchIdToPath(batchId))}。
 *
 * @param <T> 必须实现 {@link Serializable}
 */
public class LocalFileMetadataLog<T extends Serializable> implements MetadataLog<T> {

    private final Path metadataPath;

    /**
     * @param metadataPath 存储元数据文件的目录，不存在则自动创建
     */
    public LocalFileMetadataLog(Path metadataPath) {
        this.metadataPath = metadataPath;
        try {
            Files.createDirectories(metadataPath);
        } catch (IOException e) {
            throw new RuntimeException("无法创建元数据日志目录: " + metadataPath, e);
        }
    }

    private Path batchIdToPath(long batchId) {
        return metadataPath.resolve(Long.toString(batchId));
    }

    @Override
    public boolean add(long batchId, T metadata) {
        Path finalPath = batchIdToPath(batchId);
        if (Files.exists(finalPath)) {
            return false;
        }

        Path tempPath = metadataPath.resolve(java.util.UUID.randomUUID() + ".tmp");
        try {
            serialize(metadata, tempPath);
            try {
                Files.move(tempPath, finalPath, StandardCopyOption.ATOMIC_MOVE);
            } catch (java.nio.file.FileAlreadyExistsException e) {
                // 并发写入，另一个 writer 已成功
                Files.deleteIfExists(tempPath);
                return false;
            }
            return true;
        } catch (IOException e) {
            throw new RuntimeException("写入元数据失败: batchId=" + batchId, e);
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public Optional<T> get(long batchId) {
        Path path = batchIdToPath(batchId);
        if (!Files.exists(path)) {
            return Optional.empty();
        }
        try {
            return Optional.of((T) deserialize(path));
        } catch (Exception e) {
            throw new RuntimeException("读取元数据失败: batchId=" + batchId, e);
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public Optional<Map.Entry<Long, T>> getLatest() {
        try (Stream<Path> paths = Files.list(metadataPath)) {
            Optional<Long> latestId = paths
                    .filter(this::isBatchFile)
                    .map(p -> Long.parseLong(p.getFileName().toString()))
                    .max(Comparator.naturalOrder());
            if (latestId.isEmpty()) {
                return Optional.empty();
            }
            long id = latestId.get();
            Path path = batchIdToPath(id);
            if (!Files.exists(path)) {
                return Optional.empty();
            }
            try {
                return Optional.of(new AbstractMap.SimpleEntry<>(id, (T) deserialize(path)));
            } catch (Exception e) {
                // 对应 Spark getLatest：反序列化失败时跳过，继续查找次新
                return getLatestBefore(id);
            }
        } catch (IOException e) {
            throw new RuntimeException("列出元数据文件失败: " + metadataPath, e);
        }
    }

    @SuppressWarnings("unchecked")
    private Optional<Map.Entry<Long, T>> getLatestBefore(long exclusiveMax) {
        for (long id = exclusiveMax - 1; id >= 0; id--) {
            Path path = batchIdToPath(id);
            if (Files.exists(path)) {
                try {
                    return Optional.of(new AbstractMap.SimpleEntry<>(id, (T) deserialize(path)));
                } catch (Exception e) {
                    // 继续向前查找
                }
            }
        }
        return Optional.empty();
    }

    @Override
    public void purge(long threshold) {
        try (Stream<Path> paths = Files.list(metadataPath)) {
            paths
                    .filter(this::isBatchFile)
                    .map(p -> Long.parseLong(p.getFileName().toString()))
                    .filter(id -> id < threshold)
                    .forEach(id -> {
                        try {
                            Files.deleteIfExists(batchIdToPath(id));
                        } catch (IOException e) {
                            // 删除失败不中断
                        }
                    });
        } catch (IOException e) {
            throw new RuntimeException("清理元数据文件失败: " + metadataPath, e);
        }
    }

    private boolean isBatchFile(Path path) {
        try {
            Long.parseLong(path.getFileName().toString());
            return true;
        } catch (NumberFormatException e) {
            return false;
        }
    }

    private void serialize(T metadata, Path path) throws IOException {
        try (ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(path.toFile()))) {
            oos.writeObject(metadata);
        }
    }

    private Object deserialize(Path path) throws IOException, ClassNotFoundException {
        try (ObjectInputStream ois = new ObjectInputStream(new FileInputStream(path.toFile()))) {
            return ois.readObject();
        }
    }
}
