package com.sparklearn.core.storage;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertEquals;

final class MemoryStoreTest {

    /**
     * MEMORY_AND_DISK：内存放满后，最久未访问的块被淘汰到磁盘，
     * 下次读取时从磁盘读回并重新放入内存。
     */
    @Test
    void lruEvictsOldestAndSpillsToDisk(@TempDir Path dir) {
        long blockSize = MemoryStore.estimateSize(List.of(10, 20));
        // 内存上限刚好放 2 个块，第 3 个块放进去时淘汰第 1 个
        BlockManager bm = new BlockManager(dir, blockSize * 2 + 1);

        BlockId id0 = new BlockId(0, 0);
        BlockId id1 = new BlockId(0, 1);
        BlockId id2 = new BlockId(0, 2);

        Supplier<Iterator<Integer>> data = () -> List.of(10, 20).iterator();

        bm.getOrCompute(id0, StorageLevel.MEMORY_AND_DISK, data);
        bm.getOrCompute(id1, StorageLevel.MEMORY_AND_DISK, data);
        // 内存已满，放入 id2 需要淘汰 id0
        bm.getOrCompute(id2, StorageLevel.MEMORY_AND_DISK, data);

        // id0 被淘汰出内存，但仍在磁盘上；id1 和 id2 在内存中
        assertEquals(2, bm.memoryBlockCount());
        // id1、id2 从内存读取（producer 不应被调用）
        assertEquals(List.of(10, 20), toList(bm.getOrCompute(id1, StorageLevel.MEMORY_AND_DISK, failSupplier())));
        assertEquals(List.of(10, 20), toList(bm.getOrCompute(id2, StorageLevel.MEMORY_AND_DISK, failSupplier())));
        // id0 从磁盘读回（producer 不应被调用）
        assertEquals(List.of(10, 20), toList(bm.getOrCompute(id0, StorageLevel.MEMORY_AND_DISK, failSupplier())));
    }

    /**
     * LRU 访问顺序：读取一个块后它变成"最近使用"，不会被优先淘汰。
     */
    @Test
    void lruAccessOrderPreventsEviction(@TempDir Path dir) {
        long blockSize = MemoryStore.estimateSize(List.of(10, 20));
        BlockManager bm = new BlockManager(dir, blockSize * 2 + 1);

        BlockId id0 = new BlockId(0, 0);
        BlockId id1 = new BlockId(0, 1);
        BlockId id2 = new BlockId(0, 2);

        Supplier<Iterator<Integer>> data = () -> List.of(10, 20).iterator();

        bm.getOrCompute(id0, StorageLevel.MEMORY_AND_DISK, data);
        bm.getOrCompute(id1, StorageLevel.MEMORY_AND_DISK, data);
        // 访问 id0，使 id0 变成最近使用
        bm.getOrCompute(id0, StorageLevel.MEMORY_AND_DISK, failSupplier());
        // 放入 id2，需要淘汰——应该淘汰 id1 而非 id0
        bm.getOrCompute(id2, StorageLevel.MEMORY_AND_DISK, data);

        // id0 仍在内存（不需要重新计算）
        assertEquals(List.of(10, 20), toList(bm.getOrCompute(id0, StorageLevel.MEMORY_AND_DISK, failSupplier())));
        // id1 被淘汰到磁盘，仍可从磁盘读回
        assertEquals(List.of(10, 20), toList(bm.getOrCompute(id1, StorageLevel.MEMORY_AND_DISK, failSupplier())));
    }

    /**
     * MEMORY_ONLY：被淘汰的块不会写入磁盘，下次访问需要重新计算。
     */
    @Test
    void memoryOnlyDropsEvictedBlocks(@TempDir Path dir) {
        long blockSize = MemoryStore.estimateSize(List.of(10, 20));
        BlockManager bm = new BlockManager(dir, blockSize * 2 + 1);

        BlockId id0 = new BlockId(0, 0);
        BlockId id1 = new BlockId(0, 1);
        BlockId id2 = new BlockId(0, 2);

        int[] computeCount = {0};
        Supplier<Iterator<Integer>> data = () -> {
            computeCount[0]++;
            return List.of(10, 20).iterator();
        };

        bm.getOrCompute(id0, StorageLevel.MEMORY_ONLY, data);
        bm.getOrCompute(id1, StorageLevel.MEMORY_ONLY, data);
        // 放入 id2 时淘汰 id0（MEMORY_ONLY 不写磁盘）
        bm.getOrCompute(id2, StorageLevel.MEMORY_ONLY, data);

        assertEquals(3, computeCount[0]);

        // 再次访问 id0：已被淘汰且不在磁盘上，需要重新计算
        bm.getOrCompute(id0, StorageLevel.MEMORY_ONLY, data);
        assertEquals(4, computeCount[0]);
    }

    /**
     * 块自身比内存上限还大：MEMORY_AND_DISK 直接写磁盘。
     */
    @Test
    void blockLargerThanMemoryGoesStraightToDisk(@TempDir Path dir) {
        long bigBlockSize = MemoryStore.estimateSize(List.of(1, 2, 3, 4, 5));
        // 内存上限比这个块还小
        BlockManager bm = new BlockManager(dir, bigBlockSize - 1);

        BlockId id0 = new BlockId(0, 0);
        Supplier<Iterator<Integer>> data = () -> List.of(1, 2, 3, 4, 5).iterator();

        bm.getOrCompute(id0, StorageLevel.MEMORY_AND_DISK, data);

        // 内存中不应有该块
        assertEquals(0, bm.memoryBlockCount());
        // 从磁盘读回
        assertEquals(List.of(1, 2, 3, 4, 5),
                toList(bm.getOrCompute(id0, StorageLevel.MEMORY_AND_DISK, failSupplier())));
    }

    // --- 辅助方法 ---

    private static <T> Supplier<Iterator<T>> failSupplier() {
        return () -> {
            throw new AssertionError("缓存未命中且磁盘没有数据，producer 不应被调用");
        };
    }

    private static <T> List<T> toList(Iterator<T> it) {
        List<T> list = new ArrayList<>();
        it.forEachRemaining(list::add);
        return list;
    }
}
