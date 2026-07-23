package com.sparklearn.core;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * DAGScheduler：沿 RDD 血缘回溯，遇到 ShuffleDependency 就切出新的 Stage。
 *
 * <p>DAGScheduler 决定哪些 Stage 必须先完成，并为 Stage 的分区创建 Task；
 * TaskScheduler 只负责把这些 Task 提交给具体执行端。
 */
public final class DAGScheduler {

    private static final int MAX_FETCH_FAILURE_RECOVERIES = 3;

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
    Stage createResultStage(RDD<?> finalRdd) {
        Objects.requireNonNull(finalRdd, "finalRdd");
        return newResultStage(finalRdd);
    }

    public <T, U> List<U> runJob(
            RDD<T> finalRdd,
            TaskScheduler taskScheduler,
            SerializableFunction<Iterator<T>, U> partitionFunction) {
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

    @SuppressWarnings("unchecked")
    private <T, U> List<U> runJob(
            Stage finalStage,
            TaskScheduler taskScheduler,
            SerializableFunction<Iterator<T>, U> partitionFunction) {
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

    private Stage newShuffleMapStage(ShuffleDependency<?, ?> dependency) {
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
            if (dependency instanceof ShuffleDependency<?, ?> shuffleDependency) {
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
            SerializableFunction<Iterator<T>, U> partitionFunction,
            Set<Integer> finishedStages) {
        for (Stage parent : stage.parents()) {
            submitShuffleMapStage(parent, taskScheduler, finishedStages);
        }

        if (verbose) {
            System.out.println("提交 " + stage);
        }
        return submitResultStageWithRecovery(
                stage,
                taskScheduler,
                partitionFunction);
    }

    private void submitShuffleMapStage(
            Stage stage,
            TaskScheduler taskScheduler,
            Set<Integer> finishedStages) {
        if (!finishedStages.add(stage.id())) {
            return;
        }

        for (Stage parent : stage.parents()) {
            submitShuffleMapStage(parent, taskScheduler, finishedStages);
        }
        if (!stage.shuffleMap()) {
            throw new IllegalArgumentException("stage must be a ShuffleMapStage");
        }

        if (verbose) {
            System.out.println("提交 " + stage);
        }
        submitShuffleMapStageWithRecovery(stage, taskScheduler);
        if (verbose) {
            System.out.println("  shuffle map 输出已写入磁盘");
        }
    }

    private <T, U> List<U> submitResultStageWithRecovery(
            Stage stage,
            TaskScheduler taskScheduler,
            SerializableFunction<Iterator<T>, U> partitionFunction) {
        int fetchFailures = 0;
        while (true) {
            try {
                return submitMissingTasks(
                        stage,
                        taskScheduler,
                        partitionFunction);
            } catch (FetchFailedException e) {
                fetchFailures++;
                if (fetchFailures > MAX_FETCH_FAILURE_RECOVERIES) {
                    throw new IllegalStateException(
                            "fetch failure recovery exceeded limit",
                            e);
                }
                recoverMapOutput(stage, taskScheduler, e);
                if (verbose) {
                    System.out.println("  Map 输出已恢复，重新提交 " + stage);
                }
            }
        }
    }

    private void submitShuffleMapStageWithRecovery(
            Stage stage,
            TaskScheduler taskScheduler) {
        int fetchFailures = 0;
        while (true) {
            try {
                submitMissingTasks(stage, taskScheduler);
                return;
            } catch (FetchFailedException e) {
                fetchFailures++;
                if (fetchFailures > MAX_FETCH_FAILURE_RECOVERIES) {
                    throw new IllegalStateException(
                            "fetch failure recovery exceeded limit",
                            e);
                }
                recoverMapOutput(stage, taskScheduler, e);
                if (verbose) {
                    System.out.println("  Map 输出已恢复，重新提交 " + stage);
                }
            }
        }
    }

    @SuppressWarnings("unchecked")
    private <T, U> List<U> submitMissingTasks(
            Stage stage,
            TaskScheduler taskScheduler,
            SerializableFunction<Iterator<T>, U> partitionFunction) {
        if (stage.shuffleMap()) {
            throw new IllegalArgumentException("result stage expected");
        }
        if (verbose) {
            System.out.println("  提交 ResultStage 的分区任务");
        }
        RDD<T> rdd = (RDD<T>) stage.rdd();
        List<ResultTask<T, U>> tasks = new ArrayList<>();
        for (Partition partition : rdd.partitions()) {
            tasks.add(new ResultTask<>(
                    stage.id(),
                    rdd,
                    partition,
                    partitionFunction,
                    verbose));
        }
        return taskScheduler.submitTasks(tasks);
    }

    private void submitMissingTasks(Stage stage, TaskScheduler taskScheduler) {
        if (!stage.shuffleMap()) {
            throw new IllegalArgumentException("shuffle map stage expected");
        }
        ShuffleDependency<?, ?> dependency = stage.shuffleDependency()
                .orElseThrow(() -> new IllegalStateException("missing shuffle dependency"));
        submitShuffleMapTasks(stage, taskScheduler, dependency);
    }

    @SuppressWarnings("unchecked")
    private <K, V> void submitShuffleMapTasks(
            Stage stage,
            TaskScheduler taskScheduler,
            ShuffleDependency<K, V> dependency) {
        RDD<KeyValuePair<K, V>> rdd =
                (RDD<KeyValuePair<K, V>>) stage.rdd();
        List<ShuffleMapTask<K, V>> tasks = new ArrayList<>();
        for (Partition partition : rdd.partitions()) {
            tasks.add(new ShuffleMapTask<>(
                    stage.id(),
                    rdd,
                    partition,
                    dependency));
        }
        taskScheduler.submitTasks(tasks);
    }

    private void recoverMapOutput(
            Stage searchRoot,
            TaskScheduler taskScheduler,
            FetchFailedException failure) {
        Stage mapStage = findShuffleMapStage(
                searchRoot,
                failure.dependency());
        if (verbose) {
            System.out.println("  [Fetch 失败] Reduce 分区 "
                    + failure.reduceId()
                    + " 无法读取 Map 分区 "
                    + failure.mapId()
                    + " 的输出");
            System.out.println("  重新提交 "
                    + mapStage.typeName()
                    + " " + mapStage.id()
                    + " 的 Map 分区 "
                    + failure.mapId());
        }
        submitShuffleMapTask(
                mapStage,
                taskScheduler,
                failure.dependency(),
                failure.mapId());
    }

    private Stage findShuffleMapStage(
            Stage stage,
            ShuffleDependency<?, ?> dependency) {
        Stage result = findShuffleMapStageOrNull(stage, dependency);
        if (result == null) {
            throw new IllegalStateException(
                    "fetch failure does not belong to stage tree");
        }
        return result;
    }

    private Stage findShuffleMapStageOrNull(
            Stage stage,
            ShuffleDependency<?, ?> dependency) {
        if (sameShuffleDependency(
                stage.shuffleDependency().orElse(null),
                dependency)) {
            return stage;
        }
        for (Stage parent : stage.parents()) {
            Stage result = findShuffleMapStageOrNull(parent, dependency);
            if (result != null) {
                return result;
            }
        }
        return null;
    }

    private boolean sameShuffleDependency(
            ShuffleDependency<?, ?> left,
            ShuffleDependency<?, ?> right) {
        if (left == right) {
            return true;
        }
        if (left == null || right == null) {
            return false;
        }
        return left.numReducePartitions() == right.numReducePartitions()
                && left.shuffleDir().equals(right.shuffleDir());
    }

    @SuppressWarnings("unchecked")
    private <K, V> void submitShuffleMapTask(
            Stage stage,
            TaskScheduler taskScheduler,
            ShuffleDependency<?, ?> rawDependency,
            int mapId) {
        ShuffleDependency<K, V> dependency =
                (ShuffleDependency<K, V>) rawDependency;
        RDD<KeyValuePair<K, V>> rdd =
                (RDD<KeyValuePair<K, V>>) stage.rdd();
        Partition partition = rdd.partitions().stream()
                .filter(candidate -> candidate.index() == mapId)
                .findFirst()
                .orElseThrow(() -> new IllegalStateException(
                        "missing map partition: " + mapId));
        taskScheduler.submitTasks(List.of(
                new ShuffleMapTask<>(
                        stage.id(),
                        rdd,
                        partition,
                        dependency)));
    }

    private void printStage(Stage stage, int indent) {
        System.out.println("  ".repeat(indent) + stage);
        for (Stage parent : stage.parents()) {
            printStage(parent, indent + 1);
        }
    }
}
