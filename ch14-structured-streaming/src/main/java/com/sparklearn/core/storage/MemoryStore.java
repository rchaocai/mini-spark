package com.sparklearn.core.storage;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 内存缓存存储，带容量限制和 LRU 淘汰策略。
 *
 * <p>用 {@link LinkedHashMap} 的 accessOrder 模式维护访问顺序：每次 {@link #get} 或
 * {@link #tryPut} 命中都会把对应条目移到链表尾部，淘汰时从头部取出最久未访问的块。
 *
 * <p>内存大小按字节衡量：每个缓存块通过 {@link #estimateSize(Object)} 估算字节数，
 * 累加值与 {@link #maxBytes} 比较。估算方式是将对象序列化后取字节数——
 * 虽然序列化大小不等于 JVM 堆内占用（有序列化协议的额外开销），但量级一致，
 * 足以驱动容量限制和淘汰逻辑。
 */
final class MemoryStore {

    /** 块被淘汰时的回调接口（例如把淘汰块写入磁盘）。 */
    @FunctionalInterface
    interface EvictionListener {
        void onEvict(BlockId id, List<?> values);
    }

    private final long maxBytes;
    private final EvictionListener listener;

    /** accessOrder = true：get/put 会把条目移到链表尾部，头部即为最久未访问。 */
    private final LinkedHashMap<BlockId, List<?>> entries =
            new LinkedHashMap<>(32, 0.75f, true);

    /** 每个块的估算字节数，用于在淘汰时扣减总量。 */
    private final Map<BlockId, Long> blockSizes = new LinkedHashMap<>();

    /** 每个块在淘汰时是否允许写入磁盘（取决于该块自身的 StorageLevel）。 */
    private final Map<BlockId, Boolean> blockSpillFlags = new LinkedHashMap<>();

    private long totalBytes = 0;

    MemoryStore(long maxBytes, EvictionListener listener) {
        if (maxBytes <= 0) {
            throw new IllegalArgumentException("maxBytes must be positive");
        }
        this.maxBytes = maxBytes;
        this.listener = listener;
    }

    /**
     * 通过序列化估算对象的字节大小。
     *
     * <p>将对象序列化到 {@link ByteArrayOutputStream}，取输出流的字节数。
     * 序列化大小包含类描述符等协议开销，因此略大于 JVM 堆内实际占用，
     * 但量级一致，足以驱动容量判断和淘汰逻辑。无法序列化的对象返回默认值。
     */
    static long estimateSize(Object obj) {
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
             ObjectOutputStream oos = new ObjectOutputStream(bos)) {
            oos.writeObject(obj);
            oos.flush();
            return bos.size();
        } catch (IOException e) {
            return 1024; // 无法序列化时的保守默认值
        }
    }

    /**
     * 读取一个块。命中时会更新访问顺序（标记为最近使用）。
     *
     * @return 缓存的数据列表，未命中返回 {@code null}
     */
    synchronized List<?> get(BlockId id) {
        return entries.get(id);
    }

    synchronized boolean contains(BlockId id) {
        return entries.containsKey(id);
    }

    /**
     * 尝试把一个块放入内存。
     *
     * <p>如果块已存在，直接返回 {@code true}（并更新访问顺序）。
     * 如果块不存在，先淘汰 LRU 块直到腾出足够空间，再放入。
     * 如果块自身的字节数就超过了 {@link #maxBytes}，返回 {@code false}。
     *
     * @param id           块标识
     * @param values       要缓存的数据
     * @param spillToDisk  淘汰时是否把被淘汰的块回调写入磁盘
     * @return {@code true} 表示块已在内存中；{@code false} 表示放不下
     */
    synchronized boolean tryPut(BlockId id, List<?> values, boolean spillToDisk) {
        if (entries.containsKey(id)) {
            entries.get(id); // touch，更新 LRU 顺序
            return true;
        }
        long size = estimateSize(values);
        if (size > maxBytes) {
            // 块自身比整个内存上限还大，放不进去
            return false;
        }
        // 淘汰 LRU 块，直到能容纳新块
        while (totalBytes + size > maxBytes && !entries.isEmpty()) {
            evictOne();
        }
        entries.put(id, values);
        blockSizes.put(id, size);
        blockSpillFlags.put(id, spillToDisk);
        totalBytes += size;
        return true;
    }

    /** 淘汰链表头部（最久未访问）的块。 */
    private void evictOne() {
        Iterator<Map.Entry<BlockId, List<?>>> it = entries.entrySet().iterator();
        if (!it.hasNext()) {
            return;
        }
        Map.Entry<BlockId, List<?>> entry = it.next();
        BlockId evictedId = entry.getKey();
        List<?> evictedValues = entry.getValue();
        long size = blockSizes.remove(evictedId);
        boolean shouldSpill = blockSpillFlags.remove(evictedId);
        totalBytes -= size;
        it.remove();
        if (shouldSpill && listener != null) {
            listener.onEvict(evictedId, evictedValues);
        }
    }

    synchronized boolean remove(BlockId id) {
        List<?> removed = entries.remove(id);
        if (removed == null) {
            return false;
        }
        long size = blockSizes.remove(id);
        blockSpillFlags.remove(id);
        totalBytes -= size;
        return true;
    }

    /** 移除属于指定 RDD 的所有缓存块。 */
    synchronized void removeRdd(int rddId) {
        Iterator<Map.Entry<BlockId, List<?>>> it = entries.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<BlockId, List<?>> entry = it.next();
            if (entry.getKey().rddId() == rddId) {
                long size = blockSizes.remove(entry.getKey());
                blockSpillFlags.remove(entry.getKey());
                totalBytes -= size;
                it.remove();
            }
        }
    }

    synchronized void clear() {
        entries.clear();
        blockSizes.clear();
        blockSpillFlags.clear();
        totalBytes = 0;
    }

    synchronized int blockCount() {
        return entries.size();
    }

    synchronized long totalBytes() {
        return totalBytes;
    }

    long maxBytes() {
        return maxBytes;
    }
}
