---
title: "第 5 章 · 从分区到 Task"
weight: 1
date: 2026-07-16
tags: ["Task", "TaskScheduler", "ExecutorService", "并行执行", "分区"]
summary: "把分区计算封装成可调度的 Task，再用固定线程池并行执行。通过 collect、count 和 reduce，理解任务提交、结果合并、顺序保证与无共享写入。"
---

# 第 5 章 · 从分区到 Task

> 💻 本章完整代码：[GitHub 查看](https://github.com/rchaocai/mini-spark/tree/main/ch05-multithread-task)
>
> 构建运行：`mvn -pl ch05-multithread-task package && java -Dfile.encoding=UTF-8 -cp ch05-multithread-task/target/classes com.sparklearn.Main`

前四章完成了 RDD 的单机内核：RDD 能描述分区、依赖和每个分区的计算方式，`map`、`filter`、`flatMap` 组成的惰性流水线也能由 action 触发执行。

不过，第 4 章的 action 仍然按顺序遍历分区。以 `collect()` 为例：

```java
for (Partition partition : partitions()) {
    Iterator<T> iterator = iterator(partition);
    while (iterator.hasNext()) {
        result.add(iterator.next());
    }
}
```

这段代码已经按分区组织计算，却没有并行执行：当前分区处理完之后，循环才会进入下一个分区。即使机器有多个 CPU 核心，也始终只有调用 `collect()` 的线程在工作。

要让分区成为并行执行的单位，还需要在 RDD 与线程池之间补上一层：

```text
RDD 描述“怎么算”
Task 指定“算哪个分区”
TaskScheduler 决定“何时提交、如何收集结果”
```

本章就来完成这次职责拆分：把一个分区的计算封装成 Task，再由调度器把多个 Task 交给线程池执行。

先看本章最终要跑通的例子：

```java
ListRDD<Integer> rdd = new ListRDD<>(
        Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8),
        4);

System.out.println("串行 collect(): " + rdd.collect());

try (TaskScheduler scheduler = new TaskScheduler(4, true)) {
    System.out.println("并行 collect(): " + scheduler.collect(rdd));
}
```

同一个 RDD 被执行了两次。第一次调用第 4 章已有的 `rdd.collect()`，由当前线程依次计算 4 个分区；第二次调用本章新增的 `scheduler.collect(rdd)`，把 4 个分区交给线程池。

两次输出的数据相同：

```text
串行 collect(): [1, 2, 3, 4, 5, 6, 7, 8]
并行 collect(): [1, 2, 3, 4, 5, 6, 7, 8]
```

但并行版在计算过程中会打印不同的工作线程。下面只截取“开始计算”的几行，完整输出还会包含各分区的完成日志：

```text
[pool-1-thread-3] 开始计算分区 2
[pool-1-thread-1] 开始计算分区 0
[pool-1-thread-4] 开始计算分区 3
[pool-1-thread-2] 开始计算分区 1
...
```

线程名和打印顺序每次运行都可能不同。这里能观察到多个分区由不同工作线程执行，但仅凭日志还不能严格证明这些任务在时间上发生了重叠。后面的自动化测试会用一道“闸门”完成这个证明。

接下来整章只沿着这一次并行 `collect()` 往下拆：

```text
1. ListRDD 提供多个分区
2. 一个分区任务（Task）负责计算一个分区
3. TaskScheduler 先提交所有 Task
4. 工作线程并发执行这些分区任务
5. 调用 collect 的线程按分区顺序合并结果
```

理解这五步以后，`count()` 和 `reduce()` 只是改变“每个分区返回什么”以及“最后如何合并”，调度主线并没有变化。

## 5.1 先让 ListRDD 真正拥有多个分区

并行执行要有一个很朴素的前提：一份数据必须先被切成多个可以独立计算的小块。第 4 章虽然引入了 `Partition`，但 `ListRDD` 仍然只有 `Partition(0)`。只有一个分区时，调度器最多只能创建一个 Task，自然无法展示分区级并行。

本章为 `ListRDD` 增加多分区构造器：

```java
public ListRDD(List<T> data, int numberOfPartitions) {
    this.data = data;

    List<Partition> partitionList = new ArrayList<>();
    for (int index = 0; index < numberOfPartitions; index++) {
        partitionList.add(new Partition(index));
    }
    this.partitions = List.copyOf(partitionList);
}
```

每个分区对应原始 List 的一个连续区间。这里不能只看 `compute()`，还要把计算区间边界的 `startOffset()` 放在一起看：

```java
public Iterator<T> compute(Partition partition) {
    int start = startOffset(partition.index());
    int end = startOffset(partition.index() + 1);
    return data.subList(start, end).iterator();
}

private int startOffset(int partitionIndex) {
    int baseSize = data.size() / partitions.size();
    int remainder = data.size() % partitions.size();
    return partitionIndex * baseSize
            + Math.min(partitionIndex, remainder);
}
```

完整实现见 [`ListRDD.java`](https://github.com/rchaocai/mini-spark/tree/main/ch05-multithread-task/src/main/java/com/sparklearn/ListRDD.java)。

先区分两种编号：

- `partition.index()` 是**分区编号**，例如 0、1、2。
- `start` 和 `end` 是**原始 List 的数据下标**。

`startOffset(n)` 计算第 `n` 个分区从哪个数据下标开始。当前分区的结束位置，正好是下一个分区的开始位置，所以 `compute()` 分别调用：

```java
int start = startOffset(partition.index());
int end = startOffset(partition.index() + 1);
```

例如，5 个元素分成 3 个分区：

```java
ListRDD<Integer> rdd = new ListRDD<>(
        Arrays.asList(1, 2, 3, 4, 5),
        3);
```

此时基础大小是 `5 / 3 = 1`，余数是 `5 % 3 = 2`。`startOffset()` 会依次算出四个区间边界：

| 调用 | 结果 | 含义 |
|---|---:|---|
| `startOffset(0)` | 0 | 分区 0 从数据下标 0 开始 |
| `startOffset(1)` | 2 | 分区 1 从数据下标 2 开始 |
| `startOffset(2)` | 4 | 分区 2 从数据下标 4 开始 |
| `startOffset(3)` | 5 | 最后一个分区在数据下标 5 处结束 |

因此三个分区实际读取的区间是：

```java
data.subList(0, 2);  // 分区 0，读取下标 0、1
data.subList(2, 4);  // 分区 1，读取下标 2、3
data.subList(4, 5);  // 分区 2，读取下标 4
```

`subList(start, end)` 使用左闭右开区间 `[start, end)`：包含 `start`，不包含 `end`。所以最终得到：

```text
Partition(0): [1, 2]
Partition(1): [3, 4]
Partition(2): [5]
```

这里的 `startOffset(3)` 并不表示存在分区 3。它只用来计算最后一个分区的结束边界，结果正好等于 `data.size()`。

`subList(start, end)` 返回原始 List 的视图，不会复制这段数据。每次计算分区时，再为对应视图创建独立的迭代器。

如果分区数多于元素数，后面的分区会为空。空分区仍然合法：`collect()` 得到空列表，`count()` 得到 0。`reduce()` 没有元素可以归并，这个问题会在本章后面单独处理。

> [!WARNING]
> **构造 ListRDD 后不要修改原始 List**
>
> `ListRDD` 保存原始 List 的引用，分区又通过 `subList` 读取它。这样避免了复制整份数据，但也要求外部在 RDD 计算期间保持原始 List 不变。否则，并发读取可能得到不一致结果，迭代器也可能抛出 `ConcurrentModificationException`。

现在我们已经能回答一个具体问题：`new ListRDD<>(data, 4)` 会产生 4 个可独立读取的分区。下一步才是把“计算一个分区”封装成可以交给线程池的任务。

## 5.2 从 Partition 到 Task

第 4 章已经把 RDD 的计算接口改成：

```java
public abstract Iterator<T> compute(Partition partition);
```

一次调用只处理一个分区。对于本章沿用的一对一窄依赖，子分区 `i` 只读取父分区 `i`：

```text
子分区 0 -> 父分区 0 -> ... -> 源头分区 0
子分区 1 -> 父分区 1 -> ... -> 源头分区 1
```

两条分区计算链不需要交换中间数据，因此可以分别执行。这正是把分区封装成 Task 的依据。

这里要区分三个概念：

| 概念 | 本章中的职责 |
|---|---|
| `Partition` | 标识 RDD 的一个数据分片 |
| 分区任务（Task） | 描述“计算某个 RDD 的某个分区” |
| 工作线程 | 真正执行分区任务和其中的流水线 |

这里的 **Task 首先是一个调度概念**，表示“RDD + 目标分区 + 需要执行的逻辑”，并不特指某个 Java 类。本章在实现 `collect()` 时会把这个概念写成 `CollectTask` 类；`count()` 和 `reduce()` 的分区任务较短，直接用 Lambda 表示。

Task 和线程池里的线程不是固定配对的。`TaskScheduler` 会先按分区构造任务，再把这些任务提交给线程池。线程池内部有一组工作线程，它们从待执行队列中取出一个任务并执行；执行完以后，再去取下一个。

```text
分区 0 -> Task 0 --\
分区 1 -> Task 1 ----> 线程池任务队列 -> 工作线程取出并执行
分区 2 -> Task 2 --/
```

因此，一个 Task 在执行时只会由一个工作线程负责；但同一个工作线程可以先后执行多个 Task。

当前实现中，每个分区只创建一个 Task，所以**有效并行度同时受分区数和线程池大小限制**：

```text
有效并行度上限 = min(分区数, 线程池大小)
```

例如：

| 分区数 | 线程池大小 | 最多同时执行的 Task |
|---:|---:|---:|
| 1 | 4 | 1 |
| 4 | 2 | 2 |
| 4 | 8 | 4 |

线程再多，也不能把一个分区对应的 Task 自动拆成多份；分区再多，也只能由线程池中现有的工作线程分批执行。

本章的任务仍然全部运行在同一个 JVM 中。线程池拿到的是普通 Java 对象和 Lambda，不涉及网络传输。先把单机多线程这条链路跑通；跨 JVM 时为什么需要序列化，放到主实验结束后再讨论。

## 5.3 用 Callable 和 Future 表示异步结果

Java 的 `ExecutorService` 提供了线程池抽象。本章只需要其中四个动作：

```java
ExecutorService executor = Executors.newFixedThreadPool(4);
Future<String> future = executor.submit(() -> "result");
String result = future.get();
executor.shutdown();
```

逐行看：

1. `newFixedThreadPool(4)` 创建一个最多使用 4 个工作线程的固定线程池。
2. `submit(...)` 提交一个任务，并立即返回 `Future`。
3. `future.get()` 等待任务结束，然后取得返回值。
4. `shutdown()` 启动有序关闭：停止接收新任务，已提交任务可以继续执行。这个调用本身不会等待所有任务结束。

> [!INFO]
> **`Callable` 和 `Future` 分别表示什么？**
>
> `Callable<T>` 表示一个会返回 `T` 的任务，核心方法是 `call()`。
>
> `Future<T>` 表示这个任务未来产生的结果。`submit()` 返回 `Future` 时，任务可能尚未完成；调用 `get()` 时，如果结果还没准备好，当前线程会等待。
>
> 因此，`submit()` 负责启动异步计算，`Future.get()` 负责在需要结果时建立同步点。

可运行源码没有让使用者直接拿到 `ExecutorService`。线程池被包在 `TaskScheduler` 里面：

```java
public final class TaskScheduler implements AutoCloseable {

    private final ExecutorService executor;
    private final boolean verbose;

    public TaskScheduler(int numberOfThreads) {
        this(numberOfThreads, false);
    }

    public TaskScheduler(int numberOfThreads, boolean verbose) {
        if (numberOfThreads <= 0) {
            throw new IllegalArgumentException("numberOfThreads must be positive");
        }
        this.executor = Executors.newFixedThreadPool(numberOfThreads);
        this.verbose = verbose;
    }

    @Override
    public void close() {
        executor.shutdown();
    }
}
```

这段代码先看三点。

第一，创建 `TaskScheduler` 时，也就创建了线程池：

```java
new TaskScheduler(4)
```

意思是：准备一个最多使用 4 个工作线程的调度器。

第二，构造器会检查线程数必须大于 0。`verbose` 只是演示用开关，用来控制是否打印线程名，不影响调度模型。

第三，`TaskScheduler` 实现了 `AutoCloseable`，所以它可以放进 `try (...)` 这一行：

```java
try (TaskScheduler scheduler = new TaskScheduler(4)) {
    List<Integer> result = scheduler.collect(rdd);
}
```

这段代码的执行顺序可以读成：

```text
进入 try 块前：创建 TaskScheduler，也创建线程池
try 块内部：调用 collect，把 Task 提交给线程池
离开 try 块时：自动调用 scheduler.close()
close() 内部：调用 executor.shutdown()
```

也就是说，`try` 块保证离开作用域时一定会调用 `close()`。在成功路径上，`collect(rdd)` 会先等待所有 `Future` 并拿回结果，然后才离开 `try` 块；`close()` 再告诉线程池“不再接收新任务”。

这里不要把 `shutdown()` 理解成“等待所有任务结束”或“立即取消任务”。它只启动有序关闭，调用后会立即返回，已经提交的任务仍可继续执行。如果某个 Task 失败，本章实现也没有取消其他 Task；更完整的失败清理会在后续章节处理。

如果一个调度器要连续跑多个 action，也可以把它们放在同一个 `try` 块里：

```java
try (TaskScheduler scheduler = new TaskScheduler(4)) {
    List<Integer> values = scheduler.collect(rdd);
    long count = scheduler.count(rdd);
}
```

只有当整个 `try` 块结束时，线程池才会关闭。

## 5.4 先把 collect 的分区计算提取成 CollectTask

先从 `collect()` 开始。它对单个分区所做的事情很明确：

> 读取一个 RDD 的一个分区，把其中的元素收集成 `List` 并返回。

把这段逻辑写成一个具体的分区任务类 `CollectTask`：

```java
public final class CollectTask<T> implements Callable<List<T>> {
    private final RDD<T> rdd;
    private final Partition partition;

    @Override
    public List<T> call() {
        List<T> result = new ArrayList<>();
        Iterator<T> iterator = rdd.iterator(partition);
        while (iterator.hasNext()) {
            T element = iterator.next();
            result.add(element);
        }
        return result;
    }
}
```

完整实现见 [`CollectTask.java`](https://github.com/rchaocai/mini-spark/tree/main/ch05-multithread-task/src/main/java/com/sparklearn/CollectTask.java)。

正文省略了构造器、参数校验和用于演示线程名的 `verbose` 字段。核心调用链没有变化：

```text
CollectTask.call()
  -> rdd.iterator(partition)
  -> 沿窄依赖计算同号父分区
  -> 消费最外层迭代器
  -> 返回当前分区的 List
```

它与第 4 章串行 `collect()` 的分区内循环几乎相同。真正的变化是职责边界：原来一个方法连续计算所有分区，现在一个 `CollectTask` 只计算一个分区。

还要注意 `result` 的位置：

```java
List<T> result = new ArrayList<>();
```

它是 `call()` 内部的局部变量。每次调用都会创建独立的 List；不同 `CollectTask` 不会同时向同一个分区结果写数据。任务最后通过返回值把结果交给调度器，而不是直接修改一个共享结果容器。

## 5.5 TaskScheduler：先全部提交，再按序合并

`TaskScheduler.collect()` 分为两个阶段。

第一阶段，为每个分区创建一个 `CollectTask`，并把所有任务提交给线程池：

```java
List<Future<List<T>>> futures = new ArrayList<>();
for (Partition partition : rdd.partitions()) {
    futures.add(executor.submit(new CollectTask<>(rdd, partition)));
}
```

第二阶段，等待各分区结果，并把它们合并成最终 List：

```java
List<T> result = new ArrayList<>();
for (Future<List<T>> future : futures) {
    result.addAll(await(future));
}
return result;
```

完整实现见 [`TaskScheduler.java`](https://github.com/rchaocai/mini-spark/tree/main/ch05-multithread-task/src/main/java/com/sparklearn/TaskScheduler.java)。

这里采用“先提交全部任务，再读取结果”的顺序。如果在提交一个 Task 后立刻调用 `get()`，调用线程会先等待该 Task 完成，然后才有机会提交下一个 Task，计算又会退回串行。现在所有 Task 都先进入线程池，它们才有机会重叠执行。

### 执行顺序与结果顺序不是一回事

Task 的开始和完成顺序由线程调度决定，并不稳定。例如一次运行可能看到：

```text
[pool-1-thread-2] 开始计算分区 1
[pool-1-thread-4] 开始计算分区 3
[pool-1-thread-1] 开始计算分区 0
[pool-1-thread-3] 开始计算分区 2
```

另一次运行时，打印顺序可能不同。这说明任务的开始顺序由线程调度决定，也能让我们看到不同工作线程参与了计算。不过，不固定的日志顺序本身不是严格的并发证明：不同线程也可能先后执行任务而没有时间重叠。

但最终 `collect()` 仍然按分区顺序返回数据。原因是 `futures` 按 `rdd.partitions()` 的顺序保存，合并时也按同一顺序调用 `get()`：

```text
等待并合并分区 0 的结果
等待并合并分区 1 的结果
等待并合并分区 2 的结果
...
```

即使分区 2 先完成，它的结果也会等到分区 0 和分区 1 合并之后再加入最终 List。因此：

```text
任务完成顺序：不确定
collect 结果顺序：按分区顺序确定
```

这种实现简单，也延续了串行 `collect()` 的输出顺序。代价是：如果前面的分区很慢，调用线程会先等它，暂时不能处理后面已经完成的结果；不过后面的 Task 仍然在并行计算，并不会因此停止。

源码通过 `await(future)` 统一处理 `Future.get()` 的异常：线程被中断时恢复中断标记，Task 失败时保留原始异常作为 cause。本章暂不实现重试，但失败不会被静默忽略。

## 5.6 把一次并行 collect 串起来

现在把前面的代码按执行时间重新连起来。

用户调用：

```java
scheduler.collect(rdd);
```

`TaskScheduler.collect()` 先读取 RDD 的分区列表。假设有 4 个分区，它会创建并提交 4 个 `CollectTask`：

```text
Partition(0) -> CollectTask(rdd, Partition(0))
Partition(1) -> CollectTask(rdd, Partition(1))
Partition(2) -> CollectTask(rdd, Partition(2))
Partition(3) -> CollectTask(rdd, Partition(3))
```

这些任务进入线程池以后，工作线程开始调用各自拿到的 `CollectTask.call()`：

```text
工作线程
  -> CollectTask.call()
  -> rdd.iterator(partition)
  -> 沿一对一窄依赖读取同号父分区
  -> 消费这个分区的迭代器
  -> 返回当前分区的 List
```

这里没有改变 RDD 的计算方式。第 4 章怎么通过 `rdd.iterator(partition)` 计算一个分区，本章仍然怎么计算。变化只发生在外层：第 4 章由一个线程依次调用 4 次，本章由线程池安排多个工作线程调用。

| 对比项 | 第 4 章：`rdd.collect()` | 第 5 章：`scheduler.collect(rdd)` |
|---|---|---|
| 分区计算入口 | `rdd.iterator(partition)` | `rdd.iterator(partition)` |
| 分区内流水线 | 沿窄依赖逐层创建迭代器 | 完全相同 |
| 分区执行方式 | 当前线程依次执行 | 工作线程并发执行 |
| 分区局部结果 | 直接加入最终 List | 每个 `CollectTask` 返回自己的 List |
| 最终结果顺序 | 按分区顺序 | 仍按分区顺序 |

这也意味着，第 3、4 章建立的惰性流水线不需要重写。例如：

```java
RDD<Integer> pipeline = new ListRDD<>(
        Arrays.asList(1, 2, 3, 4, 5, 6),
        3)
        .map(number -> number * 10)
        .filter(number -> number > 30);

try (TaskScheduler scheduler = new TaskScheduler(3, true)) {
    System.out.println(scheduler.collect(pipeline));
}
```

线程池仍然只是为每个输出分区调用 `pipeline.iterator(partition)`。进入这个方法以后，`filter`、`map` 和 `ListRDD` 仍按前几章建立的迭代器链逐层计算。运行结果是：

```text
[40, 50, 60]
```

所以，本章没有发明第二套 RDD 计算逻辑。它只是把原来由一个线程连续执行的分区流水线，交给多个工作线程执行。

最后，调用 `scheduler.collect(rdd)` 的线程按 `Future` 的保存顺序取回结果：

```text
Task 0 返回 [1, 2]
Task 1 返回 [3, 4]
Task 2 返回 [5, 6]
Task 3 返回 [7, 8]

按分区顺序合并
  -> [1, 2, 3, 4, 5, 6, 7, 8]
```

整条链路可以收敛成一张图：

```text
RDD 的 4 个分区
  -> 创建 4 个 Task
  -> 全部提交给线程池
  -> 工作线程并发计算各分区
  -> 每个 Task 返回局部结果
  -> 调用 collect 的线程按分区顺序合并
```

这就是本章的主线。后面再看到 `count()` 和 `reduce()` 时，只需要问两个问题：

1. 每个分区返回什么？
2. 调用线程怎样合并这些返回值？

## 5.7 count 和 reduce：复用同一条调度主线

先把“任务”这个词说清楚。

前面写的 `CollectTask` 类专门服务于 `collect()`：它计算一个分区，并返回这个分区里的元素列表，所以类型是 `Callable<List<T>>`。

```java
executor.submit(new CollectTask<>(rdd, partition));
```

`count()` 和 `reduce()` 也要为每个分区提交任务，但每个分区要返回的东西不同：

| action | 每个分区要返回什么 | 提交给线程池的形式 |
|---|---|---|
| `collect()` | 当前分区的元素列表：`List<T>` | `new CollectTask<>(rdd, partition)` |
| `count()` | 当前分区的元素个数：`Long` | `() -> countPartition(rdd, partition)` |
| `reduce()` | 当前分区的局部归并结果：`PartitionResult<T>` | `() -> reducePartition(rdd, partition, operator)` |

也就是说，`count()` 和 `reduce()` 没有再定义两个新类，比如 `CountTask`、`ReduceTask`。它们直接用 Lambda 写出“这个分区怎么算”，再提交给线程池。

形式不同，主线相同：

```text
遍历所有分区
  为每个分区构造一个可提交的计算
  全部提交给线程池
  等待每个 Future 返回局部结果
  在调用线程里合并局部结果
```

### count：先分区计数，再求和

`count()` 的分区计算只做一件事：遍历当前分区，数有多少个元素。

```java
private static <T> long countPartition(RDD<T> rdd, Partition partition) {
    long count = 0;
    Iterator<T> iterator = rdd.iterator(partition);
    while (iterator.hasNext()) {
        iterator.next();
        count++;
    }
    return count;
}
```

提交时是这样：

```java
futures.add(executor.submit(() -> countPartition(rdd, partition)));
```

这个 Lambda 可以读成：“请线程池执行 `countPartition`，完成后返回这个分区的计数。”

线程池并行得到各分区计数后，调用线程把这些数字相加：

```text
Partition(0) -> 2
Partition(1) -> 2
Partition(2) -> 1

总数 -> 2 + 2 + 1 = 5
```

### reduce：先分区归并，再合并分区结果

`reduce()` 的分区计算返回的不是列表，也不是数量，而是“这个分区自己归并出来的一个结果”。

```text
Partition(0): 1 + 2 = 3
Partition(1): 3 + 4 = 7
Partition(2): 5

最终结果: 3 + 7 + 5 = 15
```

这里还有一个 `count()` 不会遇到的问题：**空分区没有局部 reduce 结果**。例如 3 个元素分成 5 个分区：

```text
Partition(0): [1] -> 局部结果 1
Partition(1): [2] -> 局部结果 2
Partition(2): [3] -> 局部结果 3
Partition(3): []  -> 没有结果
Partition(4): []  -> 没有结果
```

非空分区可以返回一个 `T`，空分区却没有任何 `T` 可以返回。直接返回 `null` 会把“没有结果”和“结果恰好为 null”混在一起，因此源码用一个很小的包装类型显式记录这两种状态：

```java
private record PartitionResult<T>(boolean hasValue, T value) {
    static <T> PartitionResult<T> empty() {
        return new PartitionResult<>(false, null);
    }

    static <T> PartitionResult<T> of(T value) {
        return new PartitionResult<>(true, value);
    }
}
```

提交时是这样：

```java
futures.add(executor.submit(
        () -> reducePartition(rdd, partition, operator)));
```

每个非空分区返回 `PartitionResult.of(value)`，空分区返回 `PartitionResult.empty()`。调用线程只合并 `hasValue()` 为真的局部结果：

```java
for (Future<PartitionResult<T>> future : futures) {
    PartitionResult<T> partitionResult = await(future);
    if (!partitionResult.hasValue()) {
        continue;
    }
    // 把非空分区的局部结果合并进最终结果
}
```

如果所有分区都为空，循环结束后仍然没有最终结果，`reduce()` 才抛出 `NoSuchElementException`。

> [!WARNING]
> **并行 reduce 的操作必须满足结合律**
>
> 串行 `reduce()` 按元素顺序逐个归并；并行版先在分区内归并，再归并分区结果，计算的分组方式发生了变化。要让两者得到相同结果，二元操作必须满足结合律：
>
> ```text
> (a op b) op c = a op (b op c)
> ```
>
> 整数加法、乘法以及字符串拼接满足结合律；减法不满足。当前实现保留了分区内元素顺序和分区合并顺序，因此不要求操作满足交换律，但不能依赖任意改变括号后的结果。

现在回头看，三个 action 的共同结构可以归纳为：

```text
为每个分区构造一个独立计算
把所有分区计算提交给线程池
每个分区通过返回值交出局部结果
调用 action 的线程合并局部结果
```

`collect()` 使用显式的 `CollectTask` 类，是为了把“分区 -> Task”这件事写成一个看得见的对象。`count()` 和 `reduce()` 的分区逻辑很短，用 Lambda 更直接。三者都是分区任务，只是 Java 表达形式不同，不是三套调度模型。

## 5.8 为什么核心计算不需要加锁

多线程代码是否需要锁，取决于多个线程是否会同时修改同一份状态。本章通过职责设计避开了共享写入。

第一，每个分区计算都有独立的局部结果。`collect()` 的 `CollectTask` 创建自己的 List，`count()` 和 `reduce()` 也只修改各自调用栈中的局部变量。

第二，RDD 只保存计算配方。多个 Task 会读取同一个 RDD 对象，但不会在 action 执行期间修改它；每个 Task 还会为自己的分区创建独立迭代器。

第三，最终结果只由调用 action 的线程合并。工作线程返回局部结果，不直接调用最终 `result.addAll(...)`。

因此，核心数据流是：

```text
工作线程：只读 RDD 配方 -> 计算一个分区 -> 返回局部结果
调用线程：等待 Future -> 合并局部结果
```

这里说“不需要锁”有明确边界，并不表示任意用户函数都天然线程安全。如果传给 `map`、`filter`、`flatMap` 或 `reduce` 的 Lambda 捕获并修改共享容器，仍然会产生并发问题。要保持本章的无锁模型，这些函数应只根据输入计算并返回结果，不修改共享可变状态。

演示模式下，多个 Task 会并发打印线程名，所以日志顺序可能交错；这些打印不参与 RDD 结果计算。

### 从同一 JVM 到远程 Executor，还差什么

到这里，单机多线程的主实验已经完整跑通。现在再看它与分布式执行之间的边界：

```text
本章：Task 对象或 Lambda -> 同一 JVM 的线程池
远程执行：Task 及其依赖 -> 字节流 -> 另一个 JVM 的 Executor
```

同一个 JVM 中的线程共享堆内存，线程池可以直接使用 `CollectTask`、RDD 和用户函数的对象引用。另一个 JVM 无法使用这些引用；要跨进程传输，必须先把任务及其依赖转换成字节流。

> [!INFO]
> **为什么跨 JVM 需要序列化？**
>
> Java 对象平时活在某个 JVM 的堆内存里。比如一个 `CollectTask` 对象，里面保存着 `rdd` 和 `partition` 两个引用。同一个 JVM 里的多个线程共享这块堆内存，所以线程池只需要拿到这个对象引用，就能调用它的 `call()`。
>
> 但另一个 JVM 看不懂这个引用。可以先把引用理解成“当前 JVM 认识的入口”：它只在这个进程里有效，离开这个进程就没有意义。
>
> 序列化做的事，就是把对象变成一串字节：
>
> ```text
> Java 对象 -> 字节流 -> 写入文件或发送到网络
> ```
>
> 另一端再把字节流还原成对象：
>
> ```text
> 字节流 -> Java 对象
> ```
>
> 所以，跨 JVM 执行 Task 时，不能只传一个引用，必须把 Task、它引用到的 RDD、用户函数等内容变成字节流发过去。这是第 9 章要处理的事。本章仍在同一个 JVM 内，只需要对象引用。

从本章代码也能直接看出这条边界：

```java
public final class CollectTask<T> implements Callable<List<T>> {
    // ...
}

executor.submit(new CollectTask<>(rdd, partition));
```

`CollectTask` 只实现了 `Callable`，没有实现 `Serializable`；`TaskScheduler` 也只是把它提交给 `ExecutorService`，没有连接远程 Executor。此时的“提交”只表示把一个对象交给同一 JVM 中的工作线程。

后面的章节会沿着这条边界继续推进：

```text
第 6 章：同一个 key 散落在不同分区时，先用 shuffle 重新分布数据
第 7 章：根据 shuffle 边界，把一项作业划分成 Stage 和 DAG
第 8 章：某个 Task 失败时，沿血缘重算对应分区
第 9 章：通过 Socket 把 Task 发给 Executor，并让 Executor 可以运行在另一个 JVM
```

## 5.9 动手验证：看日志，也跑测试

本章提供两种验证方式。

第一种是运行 `Main`，直接观察分区和工作线程：

```bash
mvn -q -pl ch05-multithread-task package
java -Dfile.encoding=UTF-8 \
  -cp ch05-multithread-task/target/classes \
  com.sparklearn.Main
```

[`Main.java`](https://github.com/rchaocai/mini-spark/tree/main/ch05-multithread-task/src/main/java/com/sparklearn/Main.java) 中有四组演示：

| 控制台部分 | 对应代码 | 验证内容 |
|---|---|---|
| `1. ListRDD：一份数据切成多个分区` | `demonstrateMultiPartitionListRDD()` | 12 个元素被切成 4 个连续分区 |
| `2. 串行 collect 与并行 collect` | `demonstrateSerialAndParallelCollect()` | 串行版和并行版结果相同，并行版打印多个工作线程 |
| `3. 流水线照旧，执行方式换成多线程` | `demonstrateParallelPipeline()` | `map/filter` 流水线不变，结果为 `[40, 50, 60]` |
| `4. 并行 count 与 reduce` | `demonstrateParallelCountAndReduce()` | `count()` 返回 5，`reduce(sum)` 返回 15 |

日志顺序不固定。例如分区 2 可能先打印，也可能分区 0 先打印。这不是测试失败，而是线程调度本来就没有固定顺序。运行 `Main` 时需要检查的是：

1. 日志里出现了多个不同的 `pool-...-thread-...`。
2. 最终 `collect()` 结果仍然按分区顺序排列。

这两点适合肉眼观察执行过程，但不能单独证明 4 个 Task 曾经同时处于计算中。为了验证真正的并发重叠，测试 `parallelCollectRunsPartitionsConcurrently` 使用 `CountDownLatch` 设置了一道闸门：

```text
Task 0 到达闸门 --\
Task 1 到达闸门 ----> 4 个 Task 全部到达后，一起继续
Task 2 到达闸门 ----> 如果只能串行执行，第一个 Task 会一直等到超时
Task 3 到达闸门 --/
```

只有 4 个分区都已经由工作线程开始计算，闸门才会打开。因此，这个测试证明的是任务在时间上确实发生了重叠，而不仅仅是日志里出现了多个线程名。

第二种是运行自动化测试：

```bash
mvn -pl ch05-multithread-task test
```

这里不要加 `-q`。`-q` 表示 quiet 模式，会隐藏 Maven 的正常构建和测试摘要；而这些测试只使用断言，不主动打印内容，所以测试全部通过时，终端看起来会是空的。

不加 `-q` 时，成功后可以看到类似摘要：

```text
Tests run: 6, Failures: 0, Errors: 0, Skipped: 0
BUILD SUCCESS
```

[`TaskSchedulerTest.java`](https://github.com/rchaocai/mini-spark/tree/main/ch05-multithread-task/src/test/java/com/sparklearn/TaskSchedulerTest.java) 覆盖了这些行为：

| 测试 | 验证内容 |
|---|---|
| `listRddSplitsDataAcrossPartitions` | 5 个元素切成 3 个分区时得到 `[1, 2]`、`[3, 4]`、`[5]` |
| `parallelCollectKeepsPartitionOrder` | 主动让分区 1、2 先算完，仍按分区 0、1、2 的顺序返回结果 |
| `parallelCollectRunsPartitionsConcurrently` | 4 个分区必须同时进入计算，并由 4 个工作线程执行 |
| `countAndReduceMergePartitionResults` | 分区局部结果能正确合并为 count 和 reduce 的最终结果 |
| `reduceOnEmptyRddFailsClearly` | 空 RDD 调用 `reduce()` 时抛出明确异常 |
| `invalidThreadCountFailsClearly` | 线程数小于 1 时拒绝创建调度器 |

`Main` 适合肉眼观察执行过程，测试则负责自动检查结果与并发约束。两者验证的是同一套实现。

## 5.10 本章小结

这一章把第 4 章的“按分区遍历”推进成了“按分区提交任务”。

核心变化有三项：

1. **多分区 `ListRDD`**：把一份内存 List 映射为多个连续分区，并允许出现空分区。
2. **分区任务**：`collect()` 用 `CollectTask` 封装一个 RDD 分区的计算，`count()` 和 `reduce()` 用 Lambda 表达同类任务。
3. **`TaskScheduler`**：把所有分区计算提交给固定线程池，再按分区顺序合并 `collect`、`count` 和 `reduce` 的结果。

RDD 与执行层之间的职责由此分开：

```text
RDD 和依赖描述计算关系
Task 确定本次计算的目标分区
TaskScheduler 负责提交、等待和合并
```

当前实现仍然局限在单个 JVM 内，也只处理不需要跨分区交换数据的窄依赖。下一章将面对按 key 聚合：同一个 key 可能散落在多个父分区中，数据必须重新分布，窄依赖流水线也会由此被切开。这就是 shuffle。
