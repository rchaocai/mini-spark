package com.sparklearn.streaming.structured;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * 一组 Source 的偏移量快照，可序列化后持久化到 {@link MetadataLog}。
 * <p>
 * 参考 Spark branch-2.0 源码：{@code CompositeOffset}（{@code Seq[Option[Offset]]}）。
 * branch-2.0 中每个 batch 的偏移量通过 {@code CompositeOffset} 序列化到 offsetLog，
 * 与 {@code sources} 列表按位置对齐。
 * <p>
 * 这里用 {@code long[]} 存储每个 Source 的偏移量值（{@code -1} 表示该 Source 无数据），
 * 避免依赖 {@link Offset} 对象的序列化实现。
 */
public record OffsetSeq(long[] offsets) implements Serializable {

    public OffsetSeq {
        offsets = offsets.clone();
    }

    /**
     * 从 Source → Offset 映射构造，sources 决定顺序。
     * <p>
     * 对应 Spark {@code StreamProgress.toCompositeOffset(sources)}。
     */
    public static OffsetSeq fromStreamProgress(Map<Source, Offset> progress, List<Source> sources) {
        long[] offsets = new long[sources.size()];
        for (int i = 0; i < sources.size(); i++) {
            Offset offset = progress.get(sources.get(i));
            offsets[i] = offset != null ? ((LongOffset) offset).offset() : -1;
        }
        return new OffsetSeq(offsets);
    }

    /**
     * 将偏移量序列与 sources 列表重新关联，恢复为 Source → Offset 映射。
     * <p>
     * 对应 Spark {@code CompositeOffset.toStreamProgress(sources)}。
     */
    public Map<Source, Offset> toStreamProgress(List<Source> sources) {
        if (sources.size() != offsets.length) {
            throw new IllegalStateException(
                    "source count mismatch: expected " + offsets.length + ", got " + sources.size());
        }
        java.util.Map<Source, Offset> progress = new java.util.LinkedHashMap<>();
        for (int i = 0; i < sources.size(); i++) {
            if (offsets[i] >= 0) {
                progress.put(sources.get(i), new LongOffset(offsets[i]));
            }
        }
        return progress;
    }

    @Override
    public String toString() {
        return Arrays.toString(offsets);
    }
}
