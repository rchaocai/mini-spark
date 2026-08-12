package com.sparklearn.core.scheduler;

import com.sparklearn.core.Dependency;
import com.sparklearn.core.NarrowDependency;
import com.sparklearn.core.Partition;
import com.sparklearn.core.ShuffleDependency;
import com.sparklearn.core.rdd.RDD;
import com.sparklearn.core.shuffle.FetchFailedException;
import com.sparklearn.core.storage.BlockManagerMaster;
import com.sparklearn.core.storage.StorageLevel;
import com.sparklearn.core.util.SerializableFunction;

import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * DAGScheduler：沿 RDD 血缘回溯切分 Stage，并通过事件循环驱动 Stage 的提交、完成与容错。
 *
 * <p>本章在第 10 章事件驱动 + 网络 RPC 调度的基础上，继续沿用同一套事件循环骨架。
 * 无论 TaskScheduler 是 LocalTaskScheduler（线程池）还是 NetworkTaskScheduler
 * （Socket Executor），DAGScheduler 都通过同一套 {@link TaskCompletionHandler} 回调驱动。
 *
 * <p>所有失败处理都集中在 {@link #handleTaskFailed(int, int, Throwable)} 里，按异常类型分两条路径：
 *
 * <ul>
 *   <li><b>普通 Task 异常</b>——重试同一个 Task（最多 {@code maxTaskRetries} 次）。
 *       重试时重新调用 {@code rdd.iterator(partition)}，沿窄依赖血缘重建迭代器链。</li>
 *   <li><b>{@link FetchFailedException}</b>——Reduce 端读不到 Map 输出。
 *       DAGScheduler 定位丢失输出所属的 ShuffleMapStage，只重算对应的 Map 分区；
 *       Map 输出恢复后，只重新提交失败的那个 Reduce Task，不重跑整个 Stage。</li>
 * </ul>
 *
 * <p>两条路径都只重算最小范围：Task 级失败只重试那个 Task，Fetch 级失败只重算那个 Map 分区。
 * 所有调度状态的读写都发生在事件循环线程里，不需要加锁。
 */
public final class DAGScheduler implements TaskCompletionHandler {

    private static final int MAX_FETCH_FAILURE_RECOVERIES = 3;

    private final TaskScheduler taskScheduler;
    private final int maxTaskRetries;
    private final boolean verbose;
    private final BlockManagerMaster blockManagerMaster;

    private final AtomicInteger nextStageId = new AtomicInteger(0);
    private final AtomicInteger nextJobId = new AtomicInteger(0);

    // 事件队列：任何线程都能 put，只有事件循环线程 take
    private final BlockingQueue<DAGSchedulerEvent> eventQueue = new LinkedBlockingQueue<>();

    // Stage 状态机（用 stageId 索引）
    private final Set<Integer> waiting = new HashSet<>();
    private final Set<Integer> running = new HashSet<>();
    private final Set<Integer> failed = new HashSet<>();
    private final Set<Integer> completedStages = new HashSet<>();

    // stageId → Stage 对象
    private final Map<Integer, Stage> idToStage = new HashMap<>();
    // stageId → 已完成的 Task 数
    private final Map<Integer, Integer> stageCompletedTasks = new HashMap<>();
    // finalStageId → ActiveJob
    private final Map<Integer, ActiveJob> activeJobs = new HashMap<>();

    // Task 重试计数："stageId:partitionIndex" → 已重试次数
    private final Map<String, Integer> taskRetryCounts = new HashMap<>();

    // Fetch 恢复计数：stageId → 已恢复次数
    private final Map<Integer, Integer> fetchFailureCounts = new HashMap<>();

    // 待恢复的 Task："mapStageId:mapPartitionIndex" → 等待 Map 输出恢复的 Task 列表
    private final Map<String, List<TaskToResubmit>> pendingRecoveries = new HashMap<>();

    private Thread eventLoopThread;

    // ── 构造与生命周期 ──────────────────────────────────────────

    /**
     * 仅供 Stage 划分测试使用：不启动事件循环，不能 runJob。
     */
    public DAGScheduler() {
        this(null, 0, false, null);
    }

    public DAGScheduler(
            TaskScheduler taskScheduler,
            int maxTaskRetries,
            boolean verbose,
            BlockManagerMaster blockManagerMaster) {
        this.taskScheduler = taskScheduler;
        this.maxTaskRetries = maxTaskRetries;
        this.verbose = verbose;
        this.blockManagerMaster = blockManagerMaster;
    }

    /**
     * 启动事件循环守护线程。
     */
    public void start() {
        Objects.requireNonNull(taskScheduler, "taskScheduler");
        eventLoopThread = new Thread(this::runEventLoop, "DAGScheduler");
        eventLoopThread.setDaemon(true);
        eventLoopThread.start();
    }

    /**
     * 停止事件循环线程。
     */
    public void stop() {
        putEvent(new DAGSchedulerEvent.StopDAGScheduler());
        if (eventLoopThread != null) {
            try {
                eventLoopThread.join(5000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    // ── 对外接口 ────────────────────────────────────────────────

    /**
     * 为最终 RDD 创建 ResultStage（递归找出它依赖的 ShuffleMapStage）。
     * 仅供测试和内部使用，不经过事件循环。
     */
    public Stage createResultStage(RDD<?> finalRdd) {
        Objects.requireNonNull(finalRdd, "finalRdd");
        return newResultStage(finalRdd, 0);
    }

    /**
     * 异步提交一个作业，阻塞等待结果。
     */
    @SuppressWarnings("unchecked")
    public <T, U> List<U> runJob(
            RDD<T> rdd,
            SerializableFunction<Iterator<T>, U> partitionFunction) {
        Objects.requireNonNull(rdd, "rdd");
        Objects.requireNonNull(partitionFunction, "partitionFunction");
        Objects.requireNonNull(taskScheduler, "taskScheduler");

        int jobId = nextJobId.getAndIncrement();
        int numPartitions = rdd.partitions().size();
        JobWaiter<U> waiter = new JobWaiter<>(numPartitions);

        SerializableFunction<Iterator<?>, ?> erased =
                (SerializableFunction<Iterator<?>, ?>) (SerializableFunction<?, ?>) partitionFunction;
        putEvent(new DAGSchedulerEvent.JobSubmitted(rdd, erased, jobId, waiter));

        return waiter.awaitResult();
    }

    // ── TaskCompletionHandler：TaskScheduler 的回调 ─────────────

    @Override
    public void taskSucceeded(int stageId, int partitionIndex, Object result) {
        putEvent(new DAGSchedulerEvent.TaskCompleted(stageId, partitionIndex, result));
    }

    @Override
    public void taskFailed(int stageId, int partitionIndex, Throwable error) {
        putEvent(new DAGSchedulerEvent.TaskFailed(stageId, partitionIndex, error));
    }

    private void putEvent(DAGSchedulerEvent event) {
        try {
            eventQueue.put(event);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    // ── 事件循环 ────────────────────────────────────────────────

    private void runEventLoop() {
        while (true) {
            try {
                DAGSchedulerEvent event = eventQueue.take();
                if (verbose) {
                    System.out.println("[DAGScheduler] 处理事件: " + event.getClass().getSimpleName());
                }
                if (processEvent(event)) {
                    return; // StopDAGScheduler
                }
                submitWaitingStages();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }
    }

    private boolean processEvent(DAGSchedulerEvent event) {
        if (event instanceof DAGSchedulerEvent.JobSubmitted e) {
            handleJobSubmitted(e.rdd(), e.partitionFunction(), e.jobId(), e.waiter());
            return false;
        } else if (event instanceof DAGSchedulerEvent.TaskCompleted e) {
            handleTaskCompleted(e.stageId(), e.partitionIndex(), e.result());
            return false;
        } else if (event instanceof DAGSchedulerEvent.TaskFailed e) {
            handleTaskFailed(e.stageId(), e.partitionIndex(), e.error());
            return false;
        } else if (event instanceof DAGSchedulerEvent.StopDAGScheduler e) {
            for (ActiveJob job : activeJobs.values()) {
                job.waiter().jobFailed(new RuntimeException("SparkContext was shut down"));
            }
            activeJobs.clear();
            return true;
        } else {
            throw new IllegalStateException("Unknown event: " + event);
        }
    }

    // ── 事件处理 ────────────────────────────────────────────────

    @SuppressWarnings("unchecked")
    private void handleJobSubmitted(
            RDD<?> rdd,
            SerializableFunction<Iterator<?>, ?> partitionFunction,
            int jobId,
            JobListener waiter) {
        Stage finalStage = newResultStage(rdd, jobId);
        ActiveJob job = new ActiveJob(jobId, finalStage, partitionFunction, waiter);
        activeJobs.put(finalStage.id(), job);

        if (verbose) {
            System.out.println("[DAGScheduler] 收到 Job " + jobId
                    + "，finalStage=" + finalStage
                    + "，优先级=" + jobId);
        }
        submitStage(finalStage);
    }

    private void handleTaskCompleted(int stageId, int partitionIndex, Object result) {
        Stage stage = idToStage.get(stageId);
        if (stage == null) {
            return;
        }
        if (failed.contains(stageId)) {
            return;
        }

        // 检查是否是 Fetch 恢复的 Map Task 完成
        String recoveryKey = stageId + ":" + partitionIndex;
        List<TaskToResubmit> recoveries = pendingRecoveries.remove(recoveryKey);
        if (recoveries != null) {
            // 清除恢复 Task 产生的临时计数
            stageCompletedTasks.remove(stageId);
            // 注册恢复后的 Map 输出
            if (stage.shuffleMap() && result instanceof MapOutputStatus status) {
                registerMapOutput(stage, status);
            }
            for (TaskToResubmit pending : recoveries) {
                Stage failedStage = idToStage.get(pending.stageId());
                if (failedStage == null || failed.contains(pending.stageId())) {
                    continue;
                }
                if (verbose) {
                    System.out.println("  Map 输出已恢复，重新提交 "
                            + failedStage.typeName() + " " + pending.stageId()
                            + " 分区 " + pending.partitionIndex());
                }
                resubmitSingleTask(failedStage, pending.partitionIndex());
            }
            return;
        }

        // ShuffleMapTask 完成时注册 Map 输出
        if (stage.shuffleMap() && result instanceof MapOutputStatus status) {
            registerMapOutput(stage, status);
        }

        int completed = stageCompletedTasks.merge(stageId, 1, Integer::sum);

        // ResultTask 的结果交给 JobWaiter
        if (!stage.shuffleMap()) {
            ActiveJob job = activeJobs.get(stageId);
            if (job != null) {
                job.waiter().taskSucceeded(partitionIndex, result);
            }
        }

        int numPartitions = stage.rdd().partitions().size();
        if (completed == numPartitions) {
            completedStages.add(stageId);
            running.remove(stageId);
            stageCompletedTasks.remove(stageId);
            taskRetryCounts.keySet().removeIf(k -> k.startsWith(stageId + ":"));
            if (!stage.shuffleMap()) {
                activeJobs.remove(stageId);
            }
            if (verbose) {
                System.out.println("[DAGScheduler] " + stage + " 全部分区完成");
            }
        }
    }

    private void handleTaskFailed(int stageId, int partitionIndex, Throwable error) {
        Stage stage = idToStage.get(stageId);
        if (stage == null) {
            return;
        }

        if (error instanceof FetchFailedException fetchFailure) {
            handleFetchFailure(stage, partitionIndex, fetchFailure);
        } else {
            handleTaskFailure(stage, partitionIndex, error);
        }
    }

    // ── 容错：Task 重试 ────────────────────────────────────────

    private void handleTaskFailure(Stage stage, int partitionIndex, Throwable error) {
        int stageId = stage.id();
        String key = stageId + ":" + partitionIndex;
        int retries = taskRetryCounts.getOrDefault(key, 0);

        if (retries < maxTaskRetries) {
            taskRetryCounts.put(key, retries + 1);
            if (verbose) {
                System.out.println("  [重试] " + taskDescription(stage, partitionIndex)
                        + " 失败: " + error.getMessage()
                        + "，开始第 " + (retries + 1) + " 次重试");
            }
            resubmitSingleTask(stage, partitionIndex);
        } else {
            running.remove(stageId);
            failed.add(stageId);
            if (verbose) {
                System.out.println("  [失败] " + taskDescription(stage, partitionIndex)
                        + " 已重试 " + retries + " 次，放弃");
            }
            ActiveJob job = activeJobs.get(stageId);
            if (job != null) {
                job.waiter().jobFailed(new IllegalStateException(
                        taskDescription(stage, partitionIndex) + " failed after "
                                + (retries + 1) + " attempts", error));
                activeJobs.remove(stageId);
            }
        }
    }

    // ── 容错：Fetch 失败恢复 ───────────────────────────────────

    private void handleFetchFailure(
            Stage failedStage,
            int failedPartitionIndex,
            FetchFailedException failure) {
        int failedStageId = failedStage.id();

        int fetchFailures = fetchFailureCounts.merge(failedStageId, 1, Integer::sum);
        if (fetchFailures > MAX_FETCH_FAILURE_RECOVERIES) {
            running.remove(failedStageId);
            failed.add(failedStageId);
            ActiveJob job = activeJobs.get(failedStageId);
            if (job != null) {
                job.waiter().jobFailed(new IllegalStateException(
                        "fetch failure recovery exceeded limit", failure));
                activeJobs.remove(failedStageId);
            }
            return;
        }

        Stage mapStage = findShuffleMapStage(failedStage, failure.dependency());
        int mapPartitionIndex = failure.mapId();

        // 注销丢失的 Map 输出，避免后续 Task 再读到旧路径
        mapStage.shuffleDependency().ifPresent(
                dependency -> dependency.unregisterMapOutput(mapPartitionIndex));

        if (verbose) {
            System.out.println("  [Fetch 失败] Reduce 分区 " + failure.reduceId()
                    + " 无法读取 Map 分区 " + mapPartitionIndex + " 的输出");
            System.out.println("  重新提交 " + mapStage.typeName() + " " + mapStage.id()
                    + " 的 Map 分区 " + mapPartitionIndex);
        }

        // 重算丢失的 Map 分区
        resubmitSingleTask(mapStage, mapPartitionIndex);

        // 记录：Map 输出恢复后需要重新提交的 Task
        String recoveryKey = mapStage.id() + ":" + mapPartitionIndex;
        pendingRecoveries.computeIfAbsent(recoveryKey, k -> new ArrayList<>())
                .add(new TaskToResubmit(failedStageId, failedPartitionIndex));
    }

    // ── Stage 状态机 ────────────────────────────────────────────

    private void submitStage(Stage stage) {
        int id = stage.id();
        if (waiting.contains(id) || running.contains(id) || failed.contains(id)) {
            return;
        }

        List<Stage> missing = getMissingParentStages(stage);

        if (missing.isEmpty()) {
            SerializableFunction<Iterator<?>, ?> func = null;
            if (!stage.shuffleMap()) {
                ActiveJob job = activeJobs.get(id);
                if (job != null) {
                    func = job.partitionFunction();
                }
            }
            submitMissingTasks(stage, func);
            running.add(id);
            if (verbose) {
                System.out.println("[DAGScheduler] 提交 " + stage + " → running");
            }
        } else {
            for (Stage parent : missing) {
                submitStage(parent);
            }
            waiting.add(id);
            if (verbose) {
                System.out.println("[DAGScheduler] " + stage + " → waiting（缺少 "
                        + missing.stream().map(Stage::id).toList() + "）");
            }
        }
    }

    private void submitWaitingStages() {
        if (waiting.isEmpty()) {
            return;
        }
        List<Integer> sorted = new ArrayList<>(waiting);
        sorted.sort(Comparator.comparingInt(id -> idToStage.get(id).priority()));
        waiting.clear();
        for (Integer stageId : sorted) {
            submitStage(idToStage.get(stageId));
        }
    }

    private List<Stage> getMissingParentStages(Stage stage) {
        return stage.parents().stream()
                .filter(p -> !completedStages.contains(p.id()))
                .collect(Collectors.toList());
    }

    // ── Stage 划分 ─────────────────────────────────────────────

    private Stage newResultStage(RDD<?> rdd, int priority) {
        List<Stage> parents = getParentStages(rdd, priority);
        Stage stage = new Stage(
                nextStageId.getAndIncrement(),
                rdd,
                false,
                parents,
                Optional.empty(),
                priority);
        idToStage.put(stage.id(), stage);
        return stage;
    }

    private Stage newShuffleMapStage(ShuffleDependency<?, ?> dependency, int priority) {
        List<Stage> parents = getParentStages(dependency.rdd(), priority);
        Stage stage = new Stage(
                nextStageId.getAndIncrement(),
                dependency.rdd(),
                true,
                parents,
                Optional.of(dependency),
                priority);
        idToStage.put(stage.id(), stage);
        return stage;
    }

    private List<Stage> getParentStages(RDD<?> rdd, int priority) {
        Set<Stage> parents = new LinkedHashSet<>();
        Set<RDD<?>> visited = new HashSet<>();
        visit(rdd, parents, visited, priority);
        return List.copyOf(parents);
    }

    private void visit(RDD<?> rdd, Set<Stage> parents, Set<RDD<?>> visited, int priority) {
        if (!visited.add(rdd)) {
            return;
        }
        for (Dependency<?> dependency : rdd.dependencies()) {
            if (dependency instanceof ShuffleDependency<?, ?> shuffleDependency) {
                parents.add(newShuffleMapStage(shuffleDependency, priority));
            } else {
                visit(dependency.rdd(), parents, visited, priority);
            }
        }
    }

    // ── Task 提交 ───────────────────────────────────────────────

    @SuppressWarnings({"unchecked", "rawtypes"})
    private void submitMissingTasks(Stage stage, SerializableFunction<Iterator<?>, ?> partitionFunction) {
        if (stage.shuffleMap()) {
            ShuffleDependency dependency = stage.shuffleDependency()
                    .orElseThrow(() -> new IllegalStateException("missing shuffle dependency"));
            RDD<?> rdd = stage.rdd();
            List tasks = new ArrayList<>();
            for (Partition partition : rdd.partitions()) {
                List<String> locations = preferredLocationsFor(rdd, partition.index());
                tasks.add(new ShuffleMapTask(stage.id(), rdd, partition, dependency, locations));
            }
            taskScheduler.submitTasks(tasks, stage.id(), this);
        } else {
            if (partitionFunction == null) {
                throw new IllegalStateException("ResultStage needs partitionFunction");
            }
            RDD<?> rdd = stage.rdd();
            List tasks = new ArrayList<>();
            for (Partition partition : rdd.partitions()) {
                List<String> locations = preferredLocationsFor(rdd, partition.index());
                tasks.add(new ResultTask(
                        stage.id(), rdd, partition, partitionFunction, verbose, locations));
            }
            taskScheduler.submitTasks(tasks, stage.id(), this);
        }
    }

    /**
     * 重新提交单个分区的 Task（用于 Task 重试或 Fetch 恢复后重提失败 Task）。
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    private void resubmitSingleTask(Stage stage, int partitionIndex) {
        SerializableFunction<Iterator<?>, ?> func = null;
        if (!stage.shuffleMap()) {
            ActiveJob job = activeJobs.get(stage.id());
            if (job != null) {
                func = job.partitionFunction();
            }
        }
        RDD<?> rdd = stage.rdd();
        Partition partition = rdd.partitions().get(partitionIndex);
        List<String> locations = preferredLocationsFor(rdd, partitionIndex);
        Task<?> task;
        if (stage.shuffleMap()) {
            ShuffleDependency dependency = stage.shuffleDependency()
                    .orElseThrow(() -> new IllegalStateException("missing shuffle dependency"));
            task = new ShuffleMapTask(stage.id(), rdd, partition, dependency, locations);
        } else {
            if (func == null) {
                throw new IllegalStateException("ResultStage needs partitionFunction");
            }
            task = new ResultTask(stage.id(), rdd, partition, func, verbose, locations);
        }
        taskScheduler.submitTask(task, stage.id(), partitionIndex, this);
    }

    // ── 缓存感知的数据本地性 ───────────────────────────────────

    /**
     * 为某个 RDD 分区计算 Task 的 preferredLocations。
     *
     * <p>对应真实 Spark 的 {@code DAGScheduler.getPreferredLocsInternal}。查找顺序：
     * <ol>
     *   <li>如果该 RDD 被缓存，查 BlockManagerMaster 里记录的缓存位置</li>
     *   <li>查 RDD 自身的偏好位置（如 ListRDD 的用户指定位置、FileRDD 的文件块位置）</li>
     *   <li>沿窄依赖向上递归，找第一个有缓存位置或偏好位置的祖先分区</li>
     * </ol>
     */
    private List<String> preferredLocationsFor(RDD<?> rdd, int partitionIndex) {
        return preferredLocationsFor(rdd, partitionIndex, new HashSet<>());
    }

    @SuppressWarnings("unchecked")
    private List<String> preferredLocationsFor(
            RDD<?> rdd,
            int partitionIndex,
            Set<Integer> visited) {
        if (!visited.add(rdd.id())) {
            return List.of();
        }

        // 1. 查缓存位置
        if (blockManagerMaster != null
                && rdd.getStorageLevel() != StorageLevel.NONE) {
            Set<String> cacheLocs =
                    blockManagerMaster.getLocations(rdd.id(), partitionIndex);
            if (!cacheLocs.isEmpty()) {
                return List.copyOf(cacheLocs);
            }
        }

        // 2. 查 RDD 自身偏好位置
        Partition partition = rdd.partitions().get(partitionIndex);
        List<String> rddPrefs = rdd.preferredLocations(partition);
        if (!rddPrefs.isEmpty()) {
            return rddPrefs;
        }

        // 3. 沿窄依赖向上递归
        for (Dependency<?> dep : rdd.dependencies()) {
            if (dep instanceof NarrowDependency<?> narrowDep) {
                for (int parentIndex : narrowDep.getParents(partitionIndex)) {
                    List<String> locs =
                            preferredLocationsFor(narrowDep.rdd(), parentIndex, visited);
                    if (!locs.isEmpty()) {
                        return locs;
                    }
                }
            }
        }

        return List.of();
    }

    // ── Map 输出注册 ───────────────────────────────────────────

    @SuppressWarnings({"unchecked", "rawtypes"})
    private void registerMapOutput(Stage stage, MapOutputStatus status) {
        stage.shuffleDependency().ifPresent(
                dependency -> dependency.registerMapOutput(status));
    }

    // ── 辅助方法 ───────────────────────────────────────────────

    private Stage findShuffleMapStage(Stage stage, ShuffleDependency<?, ?> dependency) {
        Stage result = findShuffleMapStageOrNull(stage, dependency);
        if (result == null) {
            throw new IllegalStateException("fetch failure does not belong to stage tree");
        }
        return result;
    }

    private Stage findShuffleMapStageOrNull(Stage stage, ShuffleDependency<?, ?> dependency) {
        if (sameShuffleDependency(stage.shuffleDependency().orElse(null), dependency)) {
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
        return left.shuffleId().equals(right.shuffleId());
    }

    private static String taskDescription(Stage stage, int partitionIndex) {
        String taskType = stage.shuffleMap() ? "ShuffleMapTask" : "ResultTask";
        return taskType + "(partition=" + partitionIndex + ")";
    }

    /** 等待 Map 输出恢复后需要重新提交的 Task 坐标。 */
    private record TaskToResubmit(int stageId, int partitionIndex) {
    }
}
