package com.sparklearn;

import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

/**
 * DAGScheduler：沿 RDD 血缘回溯，遇到 ShuffleDependency 就切出新的 Stage。
 *
 * <p>TaskScheduler 只关心“一个 RDD 的所有分区怎么并行跑”。DAGScheduler
 * 先关心更上层的问题：哪些分区任务必须先跑完，哪些任务可以等中间文件就绪后再跑。
 */
public final class DAGScheduler {

    private final AtomicInteger nextStageId = new AtomicInteger(0);
    private final boolean verbose;

    public DAGScheduler() {
        this(false);
    }

    public DAGScheduler(boolean verbose) {
        this.verbose = verbose;
    }

    /**
     * 为最终 RDD 创建 ResultStage，并递归找出它依赖的 ShuffleMapStage。
     */
    public Stage createResultStage(RDD<?> finalRdd) {
        Objects.requireNonNull(finalRdd, "finalRdd");
        return newResultStage(finalRdd);
    }

    public <T, U> List<U> runJob(
            RDD<T> finalRdd,
            TaskScheduler taskScheduler,
            Function<List<T>, U> partitionFunction) {
        Objects.requireNonNull(finalRdd, "finalRdd");
        Objects.requireNonNull(taskScheduler, "taskScheduler");
        Objects.requireNonNull(partitionFunction, "partitionFunction");

        Stage finalStage = createResultStage(finalRdd);
        if (verbose) {
            System.out.println("Stage 划分结果:");
            printStage(finalStage, 0);
        }
        return runJob(finalStage, taskScheduler, partitionFunction);
    }

    /**
     * 执行已经创建好的 ResultStage。示例程序可以先打印 Stage 树，再执行同一棵树。
     */
    @SuppressWarnings("unchecked")
    public <T, U> List<U> runJob(
            Stage finalStage,
            TaskScheduler taskScheduler,
            Function<List<T>, U> partitionFunction) {
        Objects.requireNonNull(finalStage, "finalStage");
        Objects.requireNonNull(taskScheduler, "taskScheduler");
        Objects.requireNonNull(partitionFunction, "partitionFunction");
        if (finalStage.shuffleMap()) {
            throw new IllegalArgumentException("finalStage must be a ResultStage");
        }

        return submitStage(finalStage, taskScheduler, partitionFunction, new HashSet<>());
    }

    private Stage newResultStage(RDD<?> rdd) {
        List<Stage> parents = getParentStages(rdd);
        return new Stage(
                nextStageId.getAndIncrement(),
                rdd,
                false,
                parents,
                Optional.empty());
    }

    private Stage newShuffleMapStage(ShuffleDependency<?> dependency) {
        List<Stage> parents = getParentStages(dependency.rdd());
        return new Stage(
                nextStageId.getAndIncrement(),
                dependency.rdd(),
                true,
                parents,
                Optional.of(dependency));
    }

    /**
     * 从当前 RDD 往父 RDD 走。窄依赖继续走；宽依赖停下来，变成父 Stage。
     */
    private List<Stage> getParentStages(RDD<?> rdd) {
        Set<Stage> parents = new LinkedHashSet<>();
        Set<RDD<?>> visited = new HashSet<>();
        visit(rdd, parents, visited);
        return List.copyOf(parents);
    }

    private void visit(RDD<?> rdd, Set<Stage> parents, Set<RDD<?>> visited) {
        if (!visited.add(rdd)) {
            return;
        }

        for (Dependency<?> dependency : rdd.dependencies()) {
            if (dependency instanceof ShuffleDependency<?> shuffleDependency) {
                parents.add(newShuffleMapStage(shuffleDependency));
            } else {
                visit(dependency.rdd(), parents, visited);
            }
        }
    }

    @SuppressWarnings("unchecked")
    private <T, U> List<U> submitStage(
            Stage stage,
            TaskScheduler taskScheduler,
            Function<List<T>, U> partitionFunction,
            Set<Integer> finishedStages) {
        if (stage.shuffleMap()) {
            submitShuffleMapStage(stage, finishedStages);
            return List.of();
        }

        for (Stage parent : stage.parents()) {
            submitShuffleMapStage(parent, finishedStages);
        }

        if (verbose) {
            System.out.println("提交 " + stage);
        }
        return submitMissingTasks(stage, taskScheduler, partitionFunction);
    }

    private void submitShuffleMapStage(Stage stage, Set<Integer> finishedStages) {
        if (!finishedStages.add(stage.id())) {
            return;
        }

        for (Stage parent : stage.parents()) {
            submitShuffleMapStage(parent, finishedStages);
        }
        if (!stage.shuffleMap()) {
            throw new IllegalArgumentException("stage must be a ShuffleMapStage");
        }

        if (verbose) {
            System.out.println("提交 " + stage);
        }
        submitMissingTasks(stage);
        if (verbose) {
            System.out.println("  shuffle map 输出已写入磁盘");
        }
    }

    @SuppressWarnings("unchecked")
    private <T, U> List<U> submitMissingTasks(
            Stage stage,
            TaskScheduler taskScheduler,
            Function<List<T>, U> partitionFunction) {
        if (stage.shuffleMap()) {
            throw new IllegalArgumentException("result stage expected");
        }
        if (verbose) {
            System.out.println("  提交 ResultStage 的分区任务");
        }
        List<List<T>> partitions = taskScheduler.collectPartitions((RDD<T>) stage.rdd());
        List<U> result = new java.util.ArrayList<>();
        for (List<T> partition : partitions) {
            result.add(partitionFunction.apply(partition));
        }
        return result;
    }

    private void submitMissingTasks(Stage stage) {
        if (!stage.shuffleMap()) {
            throw new IllegalArgumentException("shuffle map stage expected");
        }
        stage.shuffleDependency()
                .orElseThrow(() -> new IllegalStateException("missing shuffle dependency"))
                .materialize();
    }

    private void printStage(Stage stage, int indent) {
        System.out.println("  ".repeat(indent) + stage);
        for (Stage parent : stage.parents()) {
            printStage(parent, indent + 1);
        }
    }
}
