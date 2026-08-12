package com.sparklearn.storage;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Driver 端的缓存位置注册表。
 *
 * <p>对应真实 Spark 的 {@code BlockManagerMaster}（运行在 Driver 上的 RpcEndpoint）。
 * 每个 Executor 在缓存了一个分区后，通过 Task 结果把自己的地址和 BlockId 回传给 Driver；
 * Driver 在这里维护 {@code BlockId → 哪些 Executor 有这个块} 的映射。
 *
 * <p>DAGScheduler 在创建 Task 时查询这张表：如果目标分区已经被某个 Executor 缓存，
 * 就把那个 Executor 的地址作为 Task 的 preferredLocation，让 NetworkTaskScheduler
 * 优先把 Task 派到那里——这就是"缓存感知的数据本地性"。
 *
 * <p>这个类只在 Driver 侧使用，不会被序列化发送到 Executor。
 */
public final class BlockManagerMaster {

    private final Map<BlockId, Set<String>> blockLocations = new ConcurrentHashMap<>();

    /**
     * 记录某个 Executor 上新增了一个缓存块。
     */
    void updateBlockInfo(String executorAddress, BlockId blockId) {
        blockLocations
                .computeIfAbsent(blockId, k -> ConcurrentHashMap.newKeySet())
                .add(executorAddress);
    }

    /**
     * 批量记录某个 Executor 上新增的缓存块。
     */
    public void updateBlocks(String executorAddress, List<BlockId> blockIds) {
        if (blockIds == null || blockIds.isEmpty()) {
            return;
        }
        for (BlockId blockId : blockIds) {
            updateBlockInfo(executorAddress, blockId);
        }
    }

    /**
     * 查询某个 RDD 分区的缓存位置。
     *
     * @return 持有该块的 Executor 地址集合，可能为空
     */
    public Set<String> getLocations(int rddId, int partitionIndex) {
        return blockLocations.getOrDefault(
                new BlockId(rddId, partitionIndex),
                Set.of());
    }

    /**
     * 清除某个 RDD 的全部缓存位置记录（配合 uncache / RemoveRddRequest 使用）。
     */
    public void removeRdd(int rddId) {
        blockLocations.keySet().removeIf(id -> id.rddId() == rddId);
    }

    void clear() {
        blockLocations.clear();
    }
}
