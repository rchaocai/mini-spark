package com.sparklearn;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * DAGScheduler：沿 RDD 血缘回溯切分 Stage，并通过事件循环驱动 Stage 的提交与完成。
 *
 * <p>本章的核心变化：调度与执行拆开。runJob 不再同步阻塞地跑完所有 Task，
 * 而是把一个 JobSubmitted 事件投进 {@link #eventQueue}，然后阻塞在 JobWaiter 上。
 * 一个守护线程（事件循环）串行地处理事件——提交 Job、收 Task 结果、推进 Stage 状态机。
 *
 * <p>Stage 有三种状态：
 * <ul>
 *   <li><b>waiting</b>——父 Stage 还没跑完，先等着</li>
 *   <li><b>running</b>——Task 已经提交到 TaskScheduler，正在跑</li>
 *   <li><b>failed</b>——有 Task 失败了，直接失败作业</li>
 * </ul>
 * 每次处理完一个事件，都会调用 {@link #submitWaitingStages()} 检查是否有 waiting 的
 * Stage 可以推进——父 Stage 刚完成，子 Stage 就能进 running。
 *
 * <p>所有调度状态的读写都发生在事件循环线程里，不需要加锁。
 * TaskScheduler 的回调只是把事件投回队列，不做任何状态变更。
 */
public final class DAGScheduler implements TaskCompletionHandler {

    private final TaskScheduler taskScheduler;
    private final boolean verbose;

    private final AtomicInteger nextStageId = new AtomicInteger(0);
    private final AtomicInteger nextJobId = new AtomicInteger(0);

    // 事件队列：任何线程都能 put，只有事件循环线程 take
    private final BlockingQueue<DAGSchedulerEvent> eventQueue = new LinkedBlockingQueue<>();

    // Stage 状态机（用 stageId 索引）
    private final Set<Integer> waiting = new HashSet<>();      // 父 Stage 未完成的 Stage
    private final Set<Integer> running = new HashSet<>();      // 正在跑的 Stage
    private final Set<Integer> failed = new HashSet<>();       // 失败的 Stage
    private final Set<Integer> completedStages = new HashSet<>(); // 已全部完成的 Stage

    // stageId → Stage 对象
    private final Map<Integer, Stage> idToStage = new HashMap<>();
    // stageId → 已完成的 Task 数
    private final Map<Integer, Integer> stageCompletedTasks = new HashMap<>();
    // finalStageId → ActiveJob
    private final Map<Integer, ActiveJob> activeJobs = new HashMap<>();

    private Thread eventLoopThread;

    // ── 构造与生命周期 ──────────────────────────────────────────

    /**
     * 仅供 Stage 划分测试使用：不启动事件循环，不能 runJob。
     */
    DAGScheduler() {
        this(null, false);
    }

    public DAGScheduler(TaskScheduler taskScheduler, boolean verbose) {
        this.taskScheduler = taskScheduler;
        this.verbose = verbose;
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
    Stage createResultStage(RDD<?> finalRdd) {
        Objects.requireNonNull(finalRdd, "finalRdd");
        return newResultStage(finalRdd, 0);
    }

    /**
     * 异步提交一个作业，阻塞等待结果。
     *
     * <p>调用线程阻塞在 JobWaiter 上，但事件循环线程是自由的——
     * 它可以继续处理别的 JobSubmitted，让多个作业的 Task 并行跑。
     */
    @SuppressWarnings("unchecked")
    public <T, U> List<U> runJob(
            RDD<T> rdd,
            Function<Iterator<T>, U> partitionFunction) {
        Objects.requireNonNull(rdd, "rdd");
        Objects.requireNonNull(partitionFunction, "partitionFunction");
        Objects.requireNonNull(taskScheduler, "taskScheduler");

        int jobId = nextJobId.getAndIncrement();
        int numPartitions = rdd.partitions().size();
        JobWaiter<U> waiter = new JobWaiter<>(numPartitions);

        // 类型擦除：把 Function<Iterator<T>, U> 存进事件
        Function<Iterator<?>, ?> erased = (Function<Iterator<?>, ?>) (Function<?, ?>) partitionFunction;
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

    /**
     * 处理一个事件。返回 true 表示停止事件循环。
     */
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
            Function<Iterator<?>, ?> partitionFunction,
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

        int completed = stageCompletedTasks.merge(stageId, 1, Integer::sum);

        // ResultTask 的结果交给 JobWaiter
        if (!stage.shuffleMap()) {
            ActiveJob job = activeJobs.get(stageId);
            if (job != null) {
                job.waiter().taskSucceeded(partitionIndex, result);
            }
        }
        // ShuffleMapTask 的结果为 null（数据已写磁盘），不需要转发

        int numPartitions = stage.rdd().partitions().size();
        if (completed == numPartitions) {
            completedStages.add(stageId);
            running.remove(stageId);
            stageCompletedTasks.remove(stageId);
            // ResultStage 完成意味着作业完成，从 activeJobs 移除
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
        running.remove(stageId);
        failed.add(stageId);

        if (verbose) {
            System.out.println("[DAGScheduler] " + stage + " 分区 " + partitionIndex
                    + " 失败: " + error.getMessage());
        }

        // 直接失败作业
        ActiveJob job = activeJobs.get(stageId);
        if (job != null) {
            job.waiter().jobFailed(error);
            activeJobs.remove(stageId);
        }
    }

    // ── Stage 状态机 ────────────────────────────────────────────

    /**
     * 提交一个 Stage：如果父 Stage 都完成了，就提交 Task；否则放进 waiting。
     */
    private void submitStage(Stage stage) {
        int id = stage.id();
        if (waiting.contains(id) || running.contains(id) || failed.contains(id)) {
            return;
        }

        List<Stage> missing = getMissingParentStages(stage);

        if (missing.isEmpty()) {
            // 父 Stage 都完成了，可以直接跑
            Function<Iterator<?>, ?> func = null;
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
            // 有父 Stage 没完成，先等待
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

    /**
     * 每次事件处理完后调用：检查 waiting 的 Stage 是否可以推进。
     */
    private void submitWaitingStages() {
        if (waiting.isEmpty()) {
            return;
        }
        // 按 priority 排序（FIFO：jobId 小的先跑）
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

    // ── Stage 划分（从 ch07 继承，加 priority） ─────────────────

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
    private void submitMissingTasks(Stage stage, Function<Iterator<?>, ?> partitionFunction) {
        if (stage.shuffleMap()) {
            ShuffleDependency dependency = stage.shuffleDependency()
                    .orElseThrow(() -> new IllegalStateException("missing shuffle dependency"));
            RDD<?> rdd = stage.rdd();
            List tasks = new ArrayList<>();
            for (Partition partition : rdd.partitions()) {
                tasks.add(new ShuffleMapTask(rdd, partition, dependency));
            }
            taskScheduler.submitTasks(tasks, stage.id(), this);
        } else {
            if (partitionFunction == null) {
                throw new IllegalStateException("ResultStage needs partitionFunction");
            }
            RDD<?> rdd = stage.rdd();
            List tasks = new ArrayList<>();
            for (Partition partition : rdd.partitions()) {
                tasks.add(new ResultTask(rdd, partition, partitionFunction, verbose));
            }
            taskScheduler.submitTasks(tasks, stage.id(), this);
        }
    }
}
