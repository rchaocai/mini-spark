---
title: "第 5 章 · 从分区到 Task"
weight: 1
date: 2026-07-16
tags: ["Task", "TaskScheduler", "ExecutorService", "并行执行", "分区"]
summary: "把每个分区的计算包装成 Task，交给线程池并行执行。Task 自己计算、自己返回结果，不写共享容器；调度器只负责提交任务和合并结果。"
---

# 第 5 章 · 从分区到 Task

> 💻 本章完整代码：[GitHub 查看](https://github.com/rchaocai/mini-spark/tree/main/ch05-multithread-task)
>
> 构建运行：`mvn -pl ch05-multithread-task package && java -cp ch05-multithread-task/target/classes com.sparklearn.Main`

到这里，RDD 已经知道三件事：自己有哪些分区、每个分区怎么算、自己依赖哪些父 RDD。

但是 action 仍然很朴素。以 `collect()` 为例，它只是一个单线程循环：

```java
for (Partition partition : partitions()) {
    Iterator<T> iterator = iterator(partition);
    while (iterator.hasNext()) {
        result.add(iterator.next());
    }
}
```

分区 0 算完，才轮到分区 1；分区 1 算完，才轮到分区 2。你的机器可能有 8 个 CPU 核心，但这段代码只让一个线程在干活。

不是分区不能并行，而是我们还没有把“每个分区的计算”交给不同线程。

这一章要做的事很小：

> 把一个分区的计算包装成一个 Task，再把多个 Task 提交给线程池。

## 5.1 为什么分区适合变成 Task

先看一个已经有的事实：`compute(Partition partition)` 一次只计算一个分区。

这意味着分区 0 的计算和分区 1 的计算，本来就是两次不同的方法调用：

```text
compute(Partition(0))
compute(Partition(1))
```

在本章使用的一对一窄依赖里，子分区 0 只会沿着同号父分区一路往上取数据；子分区 1 也是一样。它们不会互相等待，也不会互相修改对方的数据。

因此，最自然的包装方式就是：

```text
一个 Partition
  -> 一个 Task
  -> 一个线程去执行
  -> 返回这个分区的结果
```

这里的 Task 先不要想得很复杂。它不是一个分布式系统里的远程进程，也不负责网络通信。现在它只是一个 Java 对象，里面记着两件事：

1. 要算哪个 RDD。
2. 要算这个 RDD 的哪个分区。

等线程池调用它时，它就去读这个分区的迭代器，把结果返回。

## 5.2 线程池只需要三件事

Java 里可以直接创建线程，但本章不这么做。我们用 `ExecutorService`，也就是线程池。

线程池可以先理解成一组提前准备好的工人。你把任务交给它，它找空闲线程去执行。

本章只需要三个动作：

```java
ExecutorService executor = Executors.newFixedThreadPool(4);
Future<String> future = executor.submit(() -> "result");
String result = future.get();
```

> 为了先看清“提交任务、等待结果”这条主线，这个片段省略了 `get()` 的异常处理和线程池关闭；可运行源码会完整处理这些边界。

逐行看：

1. `newFixedThreadPool(4)` 创建一个最多同时跑 4 个任务的线程池。
2. `submit(...)` 提交一个会返回结果的任务。
3. `future.get()` 等任务执行完，并拿到返回值。

> [!INFO]
> **`Callable` 和 `Future` 是什么？**
>
> `Callable<T>` 可以看成“会返回一个 `T` 的任务”。它最重要的方法是 `call()`。
>
> `Future<T>` 可以看成“将来会有一个 `T`”。任务刚提交时，结果可能还没算完；调用 `get()` 时，如果任务还在跑，当前线程会等它结束。
>
> 本章只用这两个最小动作：提交 `Callable`，再通过 `Future.get()` 拿结果。不引入锁、原子类、阻塞队列这些更复杂的并发工具。

有了这点 Java 知识，剩下的事情就很直接：把“计算一个分区”写成 `Callable<List<T>>`。

## 5.3 Task：只算一个分区

`Task` 的完整职责只有一句话：

> 读取一个 RDD 的一个分区，把这个分区里的元素收集成一个 List 返回。

代码如下：

```java
public final class Task<T> implements Callable<List<T>> {
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

完整实现见 [`Task.java`](https://github.com/rchaocai/mini-spark/tree/main/ch05-multithread-task/src/main/java/com/sparklearn/Task.java)。

这段代码和串行 `collect()` 很像。区别只在于：串行 `collect()` 在一个方法里遍历所有分区；`Task` 只负责一个分区。

> 为了突出核心调用链，正文里的 `Task` 片段省略了构造器、参数校验和演示线程名的开关；可运行源码保留了这些部分。

注意 `result` 的位置：

```java
List<T> result = new ArrayList<>();
```

它是在 `call()` 方法里创建的局部变量。每个 Task 调用 `call()` 时，都会创建自己的 `result`。线程 A 有线程 A 的 List，线程 B 有线程 B 的 List。它们不会共同往同一个 List 里写。

这就是本章最重要的约束：

> Task 不写共享可变数据，只通过返回值把结果交出去。

只要守住这个约束，就暂时不需要锁。

## 5.4 TaskScheduler：提交，再收拢

有了 `Task`，还需要一个对象负责把它们提交给线程池。这个对象叫 `TaskScheduler`。

它的 `collect()` 分两步。

第一步，给每个分区创建一个 Task，并提交给线程池：

```java
List<Future<List<T>>> futures = new ArrayList<>();
for (Partition partition : rdd.partitions()) {
    futures.add(executor.submit(new Task<>(rdd, partition)));
}
```

第二步，按提交顺序等待每个 Task 的结果，并合并：

```java
List<T> result = new ArrayList<>();
for (Future<List<T>> future : futures) {
    result.addAll(future.get());
}
return result;
```

完整实现见 [`TaskScheduler.java`](https://github.com/rchaocai/mini-spark/tree/main/ch05-multithread-task/src/main/java/com/sparklearn/TaskScheduler.java)。

这里有两个细节值得停一下。

第一，Task 是并行跑的，但合并结果是在主线程里做的。多个线程不会同时调用 `result.add(...)`，所以这里也不需要锁。

第二，合并时按 `futures` 的顺序取结果。`futures` 是按分区顺序放进去的，所以最终 `collect()` 的结果仍然保持分区顺序。哪个线程先跑完，不会改变输出顺序。

可以把执行过程看成：

```text
主线程:
  submit(Task for Partition 0)
  submit(Task for Partition 1)
  submit(Task for Partition 2)

线程池:
  同时计算多个分区

主线程:
  get 分区 0 的结果
  get 分区 1 的结果
  get 分区 2 的结果
  合并成最终 List
```

## 5.5 ListRDD 也要真的有多个分区

前面虽然已经有了 `Partition` 类型，但 `ListRDD` 还只有一个分区。为了看到并行效果，本章给它加一个多分区构造器：

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

每个分区对应原始 List 的一个窗口：

```java
public Iterator<T> compute(Partition partition) {
    int start = startOffset(partition.index());
    int end = startOffset(partition.index() + 1);
    return data.subList(start, end).iterator();
}
```

完整实现见 [`ListRDD.java`](https://github.com/rchaocai/mini-spark/tree/main/ch05-multithread-task/src/main/java/com/sparklearn/ListRDD.java)。

这里用的是 `subList(start, end)`。它不会把这段数据复制一份出来，而是返回原始 List 的一个视图。然后我们对这个视图创建迭代器。

例如：

```java
ListRDD<Integer> rdd = new ListRDD<>(
        Arrays.asList(1, 2, 3, 4, 5),
        3);
```

会得到三个分区：

```text
Partition(0): [1, 2]
Partition(1): [3, 4]
Partition(2): [5]
```

> [!INFO]
> **为什么不是每个分区一样多？**
>
> 数据条数不一定能被分区数整除。这里的规则是：先平均分，再把余下的元素从前往后每个分区多放一个。
>
> 5 个元素切成 3 个分区，基础大小是 1，还剩 2 个元素。因此前两个分区各多拿一个，得到 `[1, 2]`、`[3, 4]`、`[5]`。

有了多个分区，`TaskScheduler.collect(rdd)` 才能同时提交多个 Task。

## 5.6 count 和 reduce 也是同一个模式

`collect()` 是把每个分区的元素收集成 List。`count()` 和 `reduce()` 只是“每个分区的返回值”不同。

`count()` 的分区任务返回一个数字：

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

调度器拿到每个分区的计数后，在主线程里加起来。

`reduce()` 稍微多一步：每个分区先独立 reduce，得到一个分区结果；然后调度器再把这些分区结果 reduce 成最终结果。

```text
Partition(0): 1 + 2 = 3
Partition(1): 3 + 4 = 7
Partition(2): 5

最终结果: 3 + 7 + 5 = 15
```

> [!WARNING]
> **并行 reduce 不能随便传一个二元函数**
>
> 分区内会先算一次，分区之间还会再算一次，所以操作必须满足结合律。整数加法满足：
>
> ```text
> (1 + 2) + (3 + 4) = 1 + (2 + 3) + 4
> ```
>
> 减法不满足结合律，换一种分组方式可能得到不同结果。因此，本章的并行 `reduce` 适合加法、乘法这类满足结合律的操作，不适合直接使用减法。

所以，三个 action 的共同骨架都是：

```text
每个分区一个 Task
线程池并行执行
每个 Task 返回自己的结果
主线程合并结果
```

这个骨架非常朴素，但已经是调度器的雏形。

## 5.7 为什么我们一把锁都没用

一提到多线程，很多人第一反应是：是不是要加锁？

本章没有加锁，原因不是“并发很简单”，而是我们刻意避开了需要锁的写法。

第一，Task 不写共享容器。每个 Task 都创建自己的局部结果：

```java
List<T> result = new ArrayList<>();
```

第二，RDD 本身不在 action 中被修改。`map`、`filter` 会返回新的 RDD，不会把原来的 RDD 改掉。多个 Task 同时读同一个 RDD 对象，只是在沿着它保存的计算配方创建迭代器。

第三，合并结果发生在主线程里。`Future.get()` 拿到一个分区结果后，主线程再把它加到最终结果中。

这三点合起来，就是：

```text
计算时不共享写
RDD 不被修改
合并时回到主线程
```

所以本章的并行不是靠锁撑起来的，而是靠设计约束：**Task 没有共享可变状态，RDD 不在计算中被修改，结果用返回值交回。**

> [!WARNING]
> **传给 ListRDD 的原始 List 也要保持只读**
>
> 本章为了延续“不复制整份数据”的设计，`ListRDD` 保存的是原始 List 的引用，并通过 `subList` 创建分区视图。因此，构造 `ListRDD` 后不要再从外部修改这个 List。
>
> 本章示例里的数据在构造后都只读。这里说的“RDD 不在计算中被修改”，也包含这条使用约束。

> [!INFO]
> **没有共享可变状态的 Task 以后有什么用？**
>
> 本章的 Task 只是在本机线程池里运行。但因为它只描述“算哪个 RDD 的哪个分区”，不绑定具体线程，也不依赖共享变量，调度器将来就有空间决定把它派到哪里。
>
> 如果知道数据在哪台机器上，可以把 Task 发到数据所在的机器上执行；如果某个 Task 跑得特别慢，也可以把同一份 Task 再发给另一个执行位置。只要 RDD 的输入不变，两份 Task 算同一个分区，结果就应该一样。

## 5.8 本章小结

这一章，我们把 action 的执行方式从“一个线程遍历所有分区”，改成了“每个分区一个 Task，交给线程池并行执行”。

核心零件有三个：

1. **多分区 `ListRDD`**：一份内存 List 被切成多个分区。
2. **`Task`**：只负责计算一个 RDD 的一个分区，并通过返回值交出结果。
3. **`TaskScheduler`**：提交所有 Task，等待它们完成，再在主线程中合并结果。

更重要的是，我们没有用锁。原因是 Task 不共享可变结果，RDD 不在计算中被修改，最终合并也由主线程完成。

到这里，“把代码发给数据”的直觉第一次落到了执行层：虽然还只是在本机线程池里，但每个分区已经能由不同线程同时计算。

接下来，真正麻烦的地方会出现：有些算子不是“一个分区自己算自己”就能完成。比如按 key 聚合时，同一个 key 可能散落在多个分区里。数据必须重新分布，这就是 shuffle。
