package com.sparklearn;

import java.util.List;
import java.util.Optional;

/**
 * DAGScheduler 沿 Shuffle 边界切出来的执行阶段。
 *
 * <p>Stage 内部只有窄依赖，可以流水线执行；Stage 之间隔着 ShuffleDependency，
 * 父 Stage 必须先把中间文件写好，子 Stage 才能读取。
 *
 * @param id                Stage 编号
 * @param rdd               本 Stage 要计算到的 RDD
 * @param shuffleMap        true 表示输出是 shuffle 文件，false 表示输出是最终结果
 * @param parents           必须先完成的父 Stage
 * @param shuffleDependency ShuffleMapStage 对应的宽依赖，ResultStage 为空
 */
public record Stage(
        int id,
        RDD<?> rdd,
        boolean shuffleMap,
        List<Stage> parents,
        Optional<ShuffleDependency<?, ?>> shuffleDependency) {

    public Stage {
        parents = List.copyOf(parents);
        shuffleDependency = shuffleDependency == null
                ? Optional.empty()
                : shuffleDependency;
    }

    public String typeName() {
        return shuffleMap ? "ShuffleMapStage" : "ResultStage";
    }

    @Override
    public String toString() {
        return typeName() + " " + id
                + " (rdd=" + rdd.getClass().getSimpleName()
                + ", parents=" + parents.stream().map(Stage::id).toList()
                + ")";
    }
}
