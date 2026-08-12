package com.sparklearn.storage;

import java.io.Serializable;
import java.util.Objects;

/**
 * 缓存存储级别：控制分区数据存到内存还是磁盘。
 *
 * <p>对应真实 Spark 的 {@code org.apache.spark.storage.StorageLevel}。真实版还有
 * {@code useOffHeap}、{@code deserialized}、{@code replication} 等维度；这里保留最核心的
 * 两个——内存和磁盘——足以展示"cache 不等于只在内存"这一关键设计。
 *
 * <ul>
 *   <li>{@link #MEMORY_ONLY}：只存内存（默认，等价于 {@code cache()}）</li>
 *   <li>{@link #DISK_ONLY}：只存磁盘</li>
 *   <li>{@link #MEMORY_AND_DISK}：先试内存，内存未命中再试磁盘，都没命中才计算</li>
 * </ul>
 *
 * <p>StorageLevel 是不可变值对象，可以安全地随 RDD 序列化发送到 Executor。
 */
public final class StorageLevel implements Serializable {

    private final boolean useDisk;
    private final boolean useMemory;

    public StorageLevel(boolean useDisk, boolean useMemory) {
        this.useDisk = useDisk;
        this.useMemory = useMemory;
    }

    public boolean useDisk() {
        return useDisk;
    }

    public boolean useMemory() {
        return useMemory;
    }

    /** 是否有效：至少选了一种存储介质。 */
    public boolean isValid() {
        return useMemory || useDisk;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof StorageLevel other)) {
            return false;
        }
        return useDisk == other.useDisk && useMemory == other.useMemory;
    }

    @Override
    public int hashCode() {
        return Objects.hash(useDisk, useMemory);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("StorageLevel(");
        if (useDisk) {
            sb.append("disk, ");
        }
        if (useMemory) {
            sb.append("memory, ");
        }
        if (sb.charAt(sb.length() - 1) == ' ') {
            sb.setLength(sb.length() - 2);
        }
        sb.append(")");
        return sb.toString();
    }

    /** 不缓存。 */
    public static final StorageLevel NONE = new StorageLevel(false, false);

    /** 只存内存（默认）。 */
    public static final StorageLevel MEMORY_ONLY = new StorageLevel(false, true);

    /** 只存磁盘。 */
    public static final StorageLevel DISK_ONLY = new StorageLevel(true, false);

    /** 内存和磁盘都试：读时先查内存，再查磁盘；写时两份都存。 */
    public static final StorageLevel MEMORY_AND_DISK = new StorageLevel(true, true);
}
