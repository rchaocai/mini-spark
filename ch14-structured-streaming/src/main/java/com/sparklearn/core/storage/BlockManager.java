package com.sparklearn.core.storage;


import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.*;
import java.util.function.Supplier;

/**
 * Executor 进程级的 RDD 分区缓存。
 *
 * <p>BlockManager 不进入 Task/RDD 的序列化对象图。每个 Executor JVM 持有一个实例，
 * 缓存块丢失后仍可沿 RDD 血缘重算。
 *
 * <p>本章的改进：
 * <ul>
 *   <li>支持 {@link StorageLevel}：不仅存内存，还能落磁盘（DISK_ONLY / MEMORY_AND_DISK）</li>
 *   <li>{@link MemoryStore} 带容量限制和 LRU 淘汰：内存放不下的块自动溢出到磁盘
 *       （仅当 StorageLevel 允许磁盘时）</li>
 *   <li>追踪本次 Task 执行期间新缓存的 BlockId，供 Executor 回传给 Driver 的
 *       BlockManagerMaster，实现缓存感知的数据本地性</li>
 * </ul>
 */
public final class BlockManager implements MemoryStore.EvictionListener {

    /** 默认内存缓存上限：JVM 最大堆的 1/4。 */
    private static final long DEFAULT_MAX_MEMORY_BYTES =
            Runtime.getRuntime().maxMemory() / 4;

    private final Path localDir;

    /** 内存存储：带容量限制和 LRU 淘汰。 */
    private final MemoryStore memoryStore;

    /**
     * 每个 Task 线程在执行期间新缓存的 BlockId。
     *
     * <p>BlockManager 是 Executor 内所有 Task 线程共享的，但"本次 Task 缓存了哪些块"
     * 是每条 Task 线程各自独立的状态，所以用 ThreadLocal。
     */
    private final ThreadLocal<Set<BlockId>> newlyCached = ThreadLocal.withInitial(HashSet::new);

    public BlockManager(Path localDir) {
        this(localDir, DEFAULT_MAX_MEMORY_BYTES);
    }

    public BlockManager(Path localDir, long maxMemoryBytes) {
        this.localDir = localDir;
        this.memoryStore = new MemoryStore(maxMemoryBytes, this);
    }

    /**
     * 获取或计算一个分区。
     *
     * <p>查找顺序：内存 → 磁盘 → 计算。命中哪一层就从哪一层返回；
     * 都没命中才调用 {@code producer} 计算分区，并按 {@code level} 决定写入哪些存储。
     *
     * <p>写入时，若 {@code level.useMemory()} 为真，先尝试放入内存；内存放不下时，
     * 若 {@code level.useDisk()} 也为真，则溢出到磁盘。这正是 MEMORY_AND_DISK 的行为。
     *
     * @param id       分区对应的 BlockId
     * @param level    存储级别
     * @param producer 缓存未命中时的计算逻辑
     */
    @SuppressWarnings("unchecked")
    public <T> Iterator<T> getOrCompute(
            BlockId id,
            StorageLevel level,
            Supplier<Iterator<T>> producer) {

        // 1. 尝试内存
        if (level.useMemory()) {
            List<?> memHit = memoryStore.get(id);
            if (memHit != null) {
                return new ArrayList<>((List<T>) memHit).iterator();
            }
        }

        // 2. 尝试磁盘
        if (level.useDisk() && localDir != null) {
            Path diskFile = diskFile(id);
            if (Files.isRegularFile(diskFile)) {
                try {
                    List<T> diskData = readFromDisk(diskFile);
                    // 如果同时允许内存，把磁盘读到的数据放回内存加速下次访问
                    if (level.useMemory()) {
                        memoryStore.tryPut(id, diskData, level.useDisk());
                    }
                    return new ArrayList<>(diskData).iterator();
                } catch (IOException | ClassNotFoundException e) {
                    // 磁盘块损坏，继续走计算路径
                }
            }
        }

        // 3. 计算
        List<T> computed = new ArrayList<>();
        producer.get().forEachRemaining(computed::add);
        List<T> immutable = List.copyOf(computed);

        // 4. 按 StorageLevel 写入存储
        if (level.useMemory()) {
            boolean stored = memoryStore.tryPut(id, immutable, level.useDisk());
            if (!stored && level.useDisk() && localDir != null) {
                // 内存放不下，溢出到磁盘
                try {
                    writeToDisk(id, immutable);
                } catch (IOException e) {
                    throw new UncheckedIOException("写入磁盘缓存失败: " + id, e);
                }
            }
        } else if (level.useDisk() && localDir != null) {
            try {
                writeToDisk(id, immutable);
            } catch (IOException e) {
                throw new UncheckedIOException("写入磁盘缓存失败: " + id, e);
            }
        }

        // 5. 记录本次 Task 新缓存了哪些块，供 Executor 回传给 Driver
        newlyCached.get().add(id);

        return new ArrayList<>(immutable).iterator();
    }

    /**
     * 内存淘汰回调：被 LRU 淘汰的块如果允许写磁盘，则落盘保存。
     */
    @Override
    public void onEvict(BlockId id, List<?> values) {
        if (localDir != null) {
            try {
                writeToDisk(id, values);
            } catch (IOException e) {
                throw new UncheckedIOException("淘汰块写入磁盘失败: " + id, e);
            }
        }
    }

    /**
     * 取出并清空当前线程在本次 Task 期间新缓存的 BlockId 列表。
     *
     * <p>Executor 在 Task 执行结束后调用此方法，把结果随 RemoteTaskResult 回传给 Driver。
     */
    public List<BlockId> drainNewlyCached() {
        Set<BlockId> cached = newlyCached.get();
        if (cached.isEmpty()) {
            return List.of();
        }
        List<BlockId> result = List.copyOf(cached);
        cached.clear();
        return result;
    }

    public void removeRdd(int rddId) {
        memoryStore.removeRdd(rddId);
        if (localDir != null) {
            String prefix = "rdd-" + rddId + "-partition-";
            try (var paths = Files.list(localDir)) {
                paths.filter(path -> path.getFileName().toString().startsWith(prefix))
                        .forEach(path -> {
                            try {
                                Files.deleteIfExists(path);
                            } catch (IOException ignored) {
                                // 尽力清理
                            }
                        });
            } catch (IOException ignored) {
                // 尽力清理
            }
        }
    }

    public void clear() {
        memoryStore.clear();
        if (localDir != null) {
            try (var paths = Files.list(localDir)) {
                paths.forEach(path -> {
                    try {
                        Files.deleteIfExists(path);
                    } catch (IOException ignored) {
                        // 尽力清理
                    }
                });
            } catch (IOException ignored) {
                // 尽力清理
            }
        }
    }

    // --- 供测试观察的访问方法 ---

    int memoryBlockCount() {
        return memoryStore.blockCount();
    }

    long memoryTotalBytes() {
        return memoryStore.totalBytes();
    }

    private Path diskFile(BlockId id) {
        return localDir.resolve(
                "rdd-" + id.rddId() + "-partition-" + id.partitionIndex() + ".bin");
    }

    @SuppressWarnings("unchecked")
    private <T> List<T> readFromDisk(Path file) throws IOException, ClassNotFoundException {
        try (ObjectInputStream in = new ObjectInputStream(
                new BufferedInputStream(new FileInputStream(file.toFile())))) {
            return (List<T>) in.readObject();
        }
    }

    private <T> void writeToDisk(BlockId id, List<T> data) throws IOException {
        Path file = diskFile(id);
        Path temp = file.resolveSibling(file.getFileName() + ".tmp");
        try (ObjectOutputStream out = new ObjectOutputStream(
                new BufferedOutputStream(new FileOutputStream(temp.toFile())))) {
            out.writeObject(data);
        }
        try {
            Files.move(temp, file,
                    StandardCopyOption.ATOMIC_MOVE,
                    StandardCopyOption.REPLACE_EXISTING);
        } catch (IOException e) {
            Files.deleteIfExists(temp);
            throw e;
        }
    }
}
