---
title: "第 10 章 · Cache 与 Checkpoint"
weight: 2
date: 2026-07-19
tags: ["Cache", "Checkpoint", "RDD", "血缘", "容错", "重算"]
summary: "第 8 章证明了血缘可以重算，第 10 章继续追问：如果血缘很长，重算会不会太贵？本章在 RDD.iterator 这一层加入 cache 和 checkpoint，让重复计算能被内存短路，让容错重算能被磁盘检查点截断。"
---

# 第 10 章 · Cache 与 Checkpoint

> 💻 本章完整代码：[GitHub 查看](https://github.com/rchaocai/mini-spark/tree/main/ch10-cache-checkpoint)
>
> 构建运行：`mvn -pl ch10-cache-checkpoint package`
>
> 运行示例：`java -Dfile.encoding=UTF-8 -cp ch10-cache-checkpoint/target/classes com.sparklearn.Main`

先从上一章留下的判断说起：血缘能重算，但重算不一定便宜。

当一个 Task 失败时，调度器会重新提交它。Task 再沿着 RDD 血缘，从父 RDD 一层层算回来。只要源数据还在、函数还在、依赖还在，这个分区就能被重算出来。

这句话是对的，但它还不完整。它只回答了“能不能重算”，没有回答“重算要花多大代价”。

前一章已经把执行搬到另一个 JVM。到了那里，Task、RDD 血缘和用户函数都要先序列化，再穿过 Socket，再在 Executor 里反序列化。一次普通计算已经多了一层网络和序列化成本。

本章代码继续使用本地调度器路径，把重点放回 `RDD.iterator(partition)` 这个读取入口。这样可以先看清 cache 和 checkpoint 插入到哪里；Executor 侧的分布式缓存放在后面的 Info 注释里，作为对照说明。

这一章先看一个最容易暴露问题的场景：一条很长的血缘。

这条链横向展开后，会被两次 `collect` 反复访问：

```mermaid
flowchart LR
    A[ListRDD]
    B[map]
    C[filter]
    D["..."]
    E[map]
    F[collect 第 1 次]
    G[collect 第 2 次]

    A --> B --> C --> D --> E
    E --> F
    E --> G
```

第 1.5 节里说的逻辑回归和 PageRank，本质上也是同一批数据要反复读。把问题收缩成一条 RDD 链之后，可以先看清一个核心动作：只要中间某个 RDD 被缓存住，后面的 action 就不必再向前追。

如果没有任何缓存，每一次 `collect()` 都会从 `ListRDD` 开始，把这么多步重新跑一遍。

如果中间某个分区失败，也要从能找到的最早父分区重新跑一遍。

血缘是容错的保险绳。但绳子越长，重新爬一遍就越累。

本章加两个工具：

```text
cache      把算过的分区留在内存里，下次直接读
checkpoint 把分区写到磁盘，并切断它之前的血缘
```

它们都在控制重算成本，但控制的方式不一样。

cache 更像“抄近路”。路还在，只是命中缓存时不用走。

checkpoint 更像“重新设一个起点”。结果已经写到磁盘，从这里往前的父依赖可以不再追。

> [!INFO]
> **“抄近路”和“新起点”有什么区别？**
>
> 假设一条血缘是 `A -> B -> C -> D`，现在在 `C` 上做 cache。
> 第一次算 `D` 时，还是要从 `A` 一路算到 `C`，再算到 `D`。第二次算 `D` 时，如果 `C` 的缓存还在，就可以从 `C` 的缓存直接往后算到 `D`。
>
> 但这条旧路没有消失。如果缓存丢了，`A -> B -> C` 仍然可以沿血缘重算。
>
> checkpoint 不一样。如果在 `C` 上 checkpoint 成功，`C` 的分区结果已经写到文件里，调度器也会把 `C` 看成没有父依赖的 RDD。后面再算 `D`，最多追到 `C` 的 checkpoint 文件，不再继续追 `A` 和 `B`。

## 10.1 长血缘的问题藏在 iterator 里

要理解 cache 和 checkpoint，先看第 3 章留下来的这个统一入口：

```java
public final Iterator<T> iterator(Partition partition) {
    Objects.requireNonNull(partition, "partition");
    return compute(partition);
}
```

前面几章里，`iterator()` 几乎只是 `compute()` 的外壳。

它重要，是因为所有 RDD 读取分区数据时，都要经过这里。比如 `MapPartitionsRDD.compute(...)`：

```java
public Iterator<U> compute(Partition partition) {
    Iterator<T> parentIterator = parent.iterator(partition);
    return iteratorTransform.apply(parentIterator);
}
```

当前 RDD 要算一个分区，先问父 RDD 要同号分区。父 RDD 又问自己的父 RDD。这样一层层往上，就形成了血缘回溯。

所以，重算不是一个抽象概念。它在代码里就是：

```text
child.compute(partition)
  -> parent.iterator(partition)
      -> parent.compute(partition)
          -> grandParent.iterator(partition)
              -> grandParent.compute(partition)
```

只要没有人拦住这条调用链，它就会一直追到源头。

第 10 章的关键点不是“多了两个新接口”，而是入口没有变：所有分区读取仍然经过 `iterator()`。

在本章代码里，cache 只要插在这里，就能让后续读取自动命中同一份缓存。

checkpoint 也会先借这个入口改掉“从哪里读数据”。同时，它还要让调度器看到“这个 RDD 已经没有父依赖了”。依赖边界放到 10.6 再拆。

cache 不需要新开一条读取路径。`cache()` 只做一件事：把这个 RDD 标记成「以后算出来的分区要留下来」。真正决定命中还是重算，仍然发生在 `iterator(partition)` 这一层——命中就返回内存里的列表，没命中就继续调用 `compute(partition)`。

复杂实现可以藏在这层判断后面，比如用专门的缓存管理器去维护块、统计命中率、做淘汰策略；但插入点不变，所有读取都先过 `iterator()`。本章把缓存管理简化成一个内存 Map，职责不变。

## 10.2 cache：第一次算，第二次读

cache 的基本流程是：

```text
第一次有人要这个分区：
  compute(partition)
  把结果存进内存
  返回结果

第二次有人要这个分区：
  从内存里取
  不再调用 compute(partition)
```

先只看 cache 需要的状态。这些字段都挂在父类 `RDD` 上，和 checkpoint 的字段声明在同一块里：

```java
public abstract class RDD<T> implements Serializable {

    private final transient SparkContext sparkContext;
    private boolean shouldCache;
    private boolean checkpointRequested;
    private boolean checkpointed;
    private File checkpointDir;
    private final Map<Integer, List<T>> cache = new ConcurrentHashMap<>();
    private final Set<Integer> checkpointedPartitions = ConcurrentHashMap.newKeySet();
    private final AtomicInteger computeCount = new AtomicInteger();

    // 后面还有 partitions()、compute()、dependencies() 等方法
}
```

这一节只看 cache 的三个：`shouldCache`、`cache`、`computeCount`。

`shouldCache` 表示这个 RDD 想缓存。

`cache` 用分区编号做 key，保存这个分区已经算出来的元素列表。

`computeCount` 不是框架的生产功能，只是本章 demo 用来观察“真正调用了几次 compute”。它统计的是 `compute()` 被调用的次数，不是元素处理次数，也不是 action 次数。

中间夹着的 `checkpointRequested`、`checkpointed`、`checkpointDir`、`checkpointedPartitions` 是 checkpoint 用的，这一节先不展开，10.5 再讲。

> [!INFO]
> **cache 保存在哪里？会不会丢？**
>
> 在本章代码里，cache 是 `RDD` 对象里的一个内存 `Map`。它只保存已经算出来的分区结果，不会把数据写到 checkpoint 文件，也不会切断血缘。
>
> 生产级实现里，同样的职责由 Executor 侧的 `BlockManager` 承担。`persist()` 默认先把分区结果放进内存块；如果缓存块丢失，下一次 action 仍会沿血缘重算，再把结果重新放回缓存。
>
> 这意味着缓存块属于具体 Executor。如果某个 Executor 被销毁，或者内存压力导致缓存块被清掉，那些分区的缓存就没了。下一次再需要这些分区时，系统不会报错，而是沿着 `A -> B -> C` 这样的血缘重新计算，再把新结果放回缓存。
>
> 所以 cache 是“加速用的捷径”，不是新的数据源。需要更强的保留策略时，系统还可以指定缓存怎么存——存内存还是磁盘、要不要多存一份副本（比如 `MEMORY_AND_DISK`、`MEMORY_ONLY_2` 这类 storage level）。它们调整的是缓存的保存方式，不改变 cache 不切断血缘这一点。

`cache()` 只记录缓存意图，不负责计算分区：

```java
public final RDD<T> cache() {
    shouldCache = true;
    return this;
}
```

调用 `rdd.cache()` 时，框架只是记下“以后如果真的算到这个 RDD，就把结果留下来”。这一步不会读取父 RDD，也不会生成任何分区结果。

真正填充缓存的时机，是 action 触发 `iterator(partition)` 之后。外层 `iterator()` 先处理 checkpoint，再把普通读取交给 `iteratorWithoutCheckpoint(...)`：

```java
public final Iterator<T> iterator(Partition partition) {
    Objects.requireNonNull(partition, "partition");
    if (checkpointed) {
        return readCheckpointFile(partition);
    }
    if (checkpointRequested) {
        return checkpointPartition(partition);
    }
    return iteratorWithoutCheckpoint(partition);
}
```

cache 分支在 `iteratorWithoutCheckpoint(...)` 里：

```java
private Iterator<T> iteratorWithoutCheckpoint(Partition partition) {
    if (!shouldCache) {
        return computeTracked(partition);
    }

    List<T> cached = cache.get(partition.index());
    if (cached != null) {
        return new ArrayList<>(cached).iterator();
    }

    List<T> computed = materialize(computeTracked(partition));
    cache.put(partition.index(), computed);
    return new ArrayList<>(computed).iterator();
}
```

这个顺序决定了读取优先级。一个 RDD 如果已经 checkpoint，就说明它的数据已经物化到文件里，读取时直接读文件。如果只是请求了 checkpoint，当前分区会先完成 checkpoint 物化。只有没有 checkpoint 介入时，才进入 cache 分支。

这一节先看 cache 分支。没有开 cache 时，和前几章一样，回到原来的 `compute(partition)` 路径。

代码里多包了一层 `computeTracked(...)`，用于给本章示例统计 compute 次数：

```java
private Iterator<T> computeTracked(Partition partition) {
    computeCount.incrementAndGet();
    return compute(partition);
}
```

开了 cache 时，先用分区编号查 `cache`。命中了，就从内存里的列表返回迭代器。

没有命中，才回到 `compute(partition)` 路径。算完以后把迭代器里的元素收集成 `List`，放进缓存，再返回。

这里的 `materialize(...)` 就是做这件事的：

```java
private static <T> List<T> materialize(Iterator<T> iterator) {
    List<T> values = new ArrayList<>();
    iterator.forEachRemaining(values::add);
    return values;
}
```

它把一次性的迭代器完整消费掉，留下一个可以反复读取的 `List`。缓存不能保存“只能走一遍”的迭代器，所以必须先把结果物化出来，再放进 `cache`。

## 10.3 缓存命中时，短路的是整条上游链

示例里先构造一条长血缘。`buildLongLineage(source)` 不做计算，只连续挂上几层 transformation：

```java
private static RDD<Integer> buildLongLineage(RDD<Integer> source) {
    return source
            .map(value -> value * 2)
            .filter(value -> value > 5)
            .map(value -> value + 10)
            .filter(value -> value < 30)
            .map(value -> value * 3)
            .filter(value -> value > 30)
            .map(value -> value - 5)
            .map(value -> value + 1);
}
```

接着选一个中间点作为缓存点。`traceUp(chain, 3)` 从末端 `chain` 沿着父依赖往上走 3 层，拿到链路中间的那个 RDD。

```java
private static RDD<Integer> traceUp(RDD<?> rdd, int steps) {
    RDD<?> current = rdd;
    for (int index = 0; index < steps; index++) {
        current = current.dependencies().get(0).rdd();
    }
    @SuppressWarnings("unchecked")
    RDD<Integer> result = (RDD<Integer>) current;
    return result;
}
```

于是，示例里会观察三个位置：

```java
RDD<Integer> source = sc.parallelize(input, 3);
RDD<Integer> chain = buildLongLineage(source);
RDD<Integer> cachedPoint = traceUp(chain, 3);
```

`source` 是源头 RDD，`chain` 是整条长血缘的末端，`cachedPoint` 是中间缓存点。输入被切成 3 个分区，一次完整计算会把这 3 个分区各算一遍，每个分区触发一次 `compute`，所以一个 RDD 被完整计算一次，`compute` 次数通常就是 3。

先看不加 cache 的版本。两次 `collect()` 都要从 `source` 往后跑完整条链：

```java
List<Integer> first = chain.collect();
List<Integer> second = chain.collect();
```

| 场景 | 第一次 collect 时 | 第二次 collect 时 |
| --- | --- | --- |
| 不加 cache，源头 `source` | 3 | 3 |
| 不加 cache，中间点 `cachedPoint` | 3 | 3 |

两次都是 3，说明每一次 `collect()` 都会重新访问 3 个分区。没有 cache 时，第二次 action 不会复用第一次 action 的分区结果。

加上 cache 后，唯一的代码变化是在中间点 `cachedPoint` 上调用 `cache()`。

```java
cachedPoint.cache();

List<Integer> first = chain.collect();
List<Integer> second = chain.collect();
```

第一次 `collect()` 仍然要从源头算，因为缓存还没有内容；但这次算到 `cachedPoint` 时，会把 3 个分区结果放进缓存。第二次 `collect()` 再经过 `cachedPoint`，就会直接命中缓存：

| 场景 | 第一次 collect 后 | 第二次 collect 时 |
| --- | --- | --- |
| cache 中间点，源头 `source` | 3 | 0 |
| cache 中间点，中间点 `cachedPoint` | 3 | 0 |
| cache 中间点，末端 `chain` | 3 | 3 |

末端 `chain` 仍然要算 3 次，因为缓存点之后的下游链路还要继续执行。真正被短路的是缓存点之前的上游链路。

第二次 `collect()` 经过缓存点时，调用链在这里断开：

```mermaid
flowchart LR
    A[ListRDD source]
    B[map/filter/...]
    C[(cachedPoint cache)]
    D[map/filter/...]
    E[chain collect]

    A -. 不再访问 .-> B
    B -. 不再访问 .-> C
    C -->|命中缓存，返回 List.iterator| D --> E
```

cache 命中时，`cachedPoint.iterator(partition)` 直接返回缓存里的 `List.iterator()`。它不会调用 `cachedPoint.compute(partition)`，也就不会继续调用父 RDD 的 `iterator(partition)`。

cache 在长血缘里省下的成本，不是某一个 `map` 或 `filter`，而是缓存点之前的整段血缘。

## 10.4 cache 放在哪里

cache 不是越早越好，也不是越晚越好。

先看一个更准确的说法：cache 应该放在会被复用的位置上，通常是几条下游链路的共同上游。

```text
如果一个 RDD 只会被一个下游 action 用一次：
  cache 它通常不值

如果一个 RDD 会被多个下游 action 复用：
  cache 它能让这些 action 共享同一份结果

如果同一条长链会被反复 action 访问：
  cache 它能让后续 action 直接复用结果
```

更准确地说，要看三个因素：

```text
这个 RDD 被复用几次
算到这个 RDD 要花多少钱
这个 RDD 的分区结果占多少内存
```

如果一个 RDD 只会被用一次，cache 没意义。保存它还会额外占用内存。

如果一个 RDD 会被反复使用，而且算它要读文件、走网络、跑很长血缘，cache 的收益就很明显。

迭代式机器学习就是这种复用模式的典型场景。

第 1.5 节用逻辑回归举过例子：它一轮一轮地调参，每一轮都要把整份训练数据从头过一遍。这里把那个训练循环收到最简单的一维形式——一个特征 `x`、一个权重 `w`、标签 `y` 只有 0 或 1，用 `map` 算每条样本的梯度，用 `reduce` 汇总：

```mermaid
flowchart LR
    S[(训练数据 RDD)]
    M[map: 算每条样本的梯度]
    R[reduce: 汇总本轮梯度]
    U[更新参数 w]

    S --> M --> R --> U --> S
```

`map` 这一步就是逻辑回归的梯度公式（`sigmoid` 把 `w·x` 压成 0~1 的预测概率，再和标签 `y` 算差）：

```java
double gradient = source
        .map(sample -> (sigmoid(currentWeight * sample.x()) - sample.y()) * sample.x())
        .reduce(Double::sum);
```

这里要关注的不是公式本身，而是它读的数据：每一轮都从同一个 `source`（训练数据 RDD）里重新算梯度。这正好和 10.3 互补——10.3 是一条长血缘被多次 action 访问，cache 用来短路缓存点之前的上游；这里是同一批训练数据被多轮迭代反复读，cache 用来让第一轮之后各轮直接复用。

本章 `Main` 里就有这个小例子，跑三轮训练：

```text
不缓存训练数据:
  第 1 轮: w=0.0708, gradient=-4.2500, source.compute=3
  第 2 轮: w=0.1255, gradient=-3.2788, source.compute=3
  第 3 轮: w=0.1681, gradient=-2.5558, source.compute=3
cache 训练数据:
  第 1 轮: w=0.0708, gradient=-4.2500, source.compute=3
  第 2 轮: w=0.1255, gradient=-3.2788, source.compute=0
  第 3 轮: w=0.1681, gradient=-2.5558, source.compute=0
```

先看两边相同的地方：三轮的 `w` 和 `gradient` 一模一样，`w` 从 0 一路涨到 `0.1681`（样本里 `x` 越大越是正类，所以权重朝正方向走）。这说明逻辑回归确实在学，而且不管缓不缓存训练数据，参数更新过程完全一致。

差别只在最后那一列 `source.compute`：不缓存时每轮都是 3（每轮都把训练数据的 3 个分区重算一遍）；缓存以后，只有第一轮是 3，后两轮变成 0——训练数据算过一次就留在内存里，后面直接复用。

这正是 cache 在迭代式算法里的价值：训练往往要跑几十上百轮，没有 cache 就等于把同一批数据读几十上百遍；如果数据在 HDFS 上，每轮都是一次磁盘和网络 I/O。把训练数据 RDD 缓存住，第一轮读进内存，后面的轮次都从内存里拿。

## 10.5 checkpoint：把血缘切断

前面 cache 控制的是“反复读太贵”，但它不切断血缘——缓存丢了，还能沿血缘重算回来。checkpoint 要处理的是血缘太长的情况：重算的代价会一路累积，最后大到难以接受。拿 PageRank（一种给网页算权重的迭代算法）来说，`links` 是网页之间的链接图，固定不变，适合反复缓存；`ranks` 是每个网页的当前权重，每一轮都由上一轮的 `ranks` 重新算出来。如果第 20 轮某个分区丢了，还要从第 1 轮一路重算到第 20 轮，成本就太高了。

所以 checkpoint 要做两件事：

1. 把当前 RDD 的每个分区写到一个可重新读取的位置
2. 让这个 RDD 不再向前暴露父依赖

当前例子仍然沿用前面的长血缘。`checkpointPoint` 是 `Main.runWithCheckpoint(...)` 里的一个局部变量：它表示从长血缘中间取出的那个 RDD。

```java
private static void runWithCheckpoint(SparkContext sc, List<Integer> input) {
    System.out.println();
    System.out.println("Part C: checkpoint 中间 RDD，切断它的父依赖");

    RDD<Integer> source = sc.parallelize(input, 3);
    RDD<Integer> chain = buildLongLineage(source);
    RDD<Integer> checkpointPoint = traceUp(chain, 3);

    System.out.println("checkpoint 前依赖数: " + checkpointPoint.dependencies().size());
    checkpointPoint.checkpoint();
    System.out.println("checkpoint 请求后依赖数: " + checkpointPoint.dependencies().size());
    System.out.println("isCheckpointed: " + checkpointPoint.isCheckpointed());

    System.out.println("触发 checkpoint 的 collect: " + checkpointPoint.collect());
    System.out.println("checkpoint 物化后依赖数: " + checkpointPoint.dependencies().size());
    System.out.println("isCheckpointed: " + checkpointPoint.isCheckpointed());

    source.resetComputeCount();
    checkpointPoint.resetComputeCount();

    RDD<Integer> downstream = checkpointPoint
            .map(value -> value * 10)
            .filter(value -> value > 200);
    System.out.println("checkpoint 后继续往下计算: " + downstream.collect());
    System.out.println("源头 compute 次数: " + source.getComputeCount());
    System.out.println("checkpoint 点 compute 次数: " + checkpointPoint.getComputeCount());
}
```

把这段代码跑起来，前半段输出是这样的：

```text
Part C: checkpoint 中间 RDD，切断它的父依赖
checkpoint 前依赖数: 1
checkpoint 请求后依赖数: 1
isCheckpointed: false
触发 checkpoint 的 collect: [48, 54, 60, 66, 72, 78, 84]
checkpoint 物化后依赖数: 0
isCheckpointed: true
```

输出的关键，是中间那次跳变。调用 `checkpoint()` 之后，依赖数仍然是 1，`isCheckpointed` 仍然是 false；等到 `collect()` 跑完，依赖数才掉到 0，`isCheckpointed` 才变成 true。这说明 `checkpoint()` 这一步什么也没真正做，只是登记了一个请求；真正把分区写进文件、把血缘切断的，是后面那次 `collect()`。

代码后半段还会从 checkpoint 点继续往下算一条 `downstream` 链，它用来验证血缘是不是真的被切断了，留到本节后面再看。下面先把这两步拆开。

`checkpoint()` 只记录意图：

```java
public final void checkpoint() {
    checkpointRequested = true;
}
```

checkpoint 相关状态集中在 `RDD` 父类里：

```java
private boolean checkpointRequested;
private boolean checkpointed;
private File checkpointDir;
private final Set<Integer> checkpointedPartitions =
        ConcurrentHashMap.newKeySet();
```

`checkpointRequested` 表示已经登记 checkpoint 请求。`checkpointedPartitions` 记录哪些分区已经写入文件。只有所有分区都写完，`checkpointed` 才会变成 true。

此时 `checkpointPoint` 还没有数据文件，`dependencies()` 也仍然能看到父 RDD。下一次 action 经过这个 RDD 时，走的还是 10.2 贴过的那个 `iterator()`——它的前两个分支就是为 checkpoint 留的：`checkpointed` 为真就直接读 checkpoint 文件，`checkpointRequested` 为真就进入物化。只有两者都不满足，才回到 cache 那条 `iteratorWithoutCheckpoint` 路。

物化具体怎么做？进入 `checkpointPartition(partition)`：

```java
private Iterator<T> checkpointPartition(Partition partition) {
    File dir = ensureCheckpointDir();
    File file = checkpointFile(dir, partition);
    if (file.exists()) {
        return readCheckpointFile(partition);
    }
    List<T> values = materialize(iteratorWithoutCheckpoint(partition));
    writeCheckpointFile(dir, partition, values);
    markCheckpointPartitionComplete(partition);
    return new ArrayList<>(values).iterator();
}
```

这一段就是 checkpoint 的物化过程：当前分区先按原来的读取路径拿到数据，再写入 checkpoint 文件。这个读取路径可能沿血缘计算，也可能命中已有 cache。`collect()` 会访问所有分区，所以它结束后，所有分区都已经写好。

等最后一个分区写完，`markCheckpointPartitionComplete(partition)` 会把这个 RDD 标记为 checkpointed。此后它发生两个变化：

1. `iterator(partition)` 不再沿父血缘往上算，而是直接读 checkpoint 文件。
2. `dependencies()` 返回空列表，调度器再往上找时，会把这里当成血缘终点。

这两个变化合起来，等于把这个 RDD 原地变成了一个 source RDD。

source RDD 之所以是「源头」，靠的就是这两条：没有父依赖、自己能产出分区数据。`ListRDD` 就是这样——`dependencies()` 返回空，`compute()` 直接读内存列表里的数据。

物化后的 RDD 这两条都满足：`dependencies()` 被改成返回空，读取时直接读 checkpoint 文件。所以对下游 RDD 和调度器而言，它和一个 `ListRDD` 没有本质区别，都是血缘的终点。

差别只在数据从哪儿来：source RDD 读的是外部输入（用户传进来的列表、磁盘上的原始文件），checkpoint RDD 读的是它自己之前算过、再写下来的结果。但这不影响「无父依赖、自产数据」这个身份。

这一点可以用示例后半段的 `downstream` 验证。前面 `runWithCheckpoint` 的代码里，`checkpointPoint` 物化之后还从它往下建了一条 `downstream` 链再 `collect`，输出是：

```text
checkpoint 后继续往下计算: [480, 540, 600, 660, 720, 780, 840]
源头 compute 次数: 0
checkpoint 点 compute 次数: 0
```

源头是 0，说明 `downstream` 算的时候没有回溯到 `ListRDD`；checkpoint 点也是 0，说明它连自己的 `compute()` 都没调，直接从 checkpoint 文件读。依赖数变 0 是调度器看到的景象，`compute` 次数为 0 是实际执行的结果——两点合起来，血缘是真的被切断了，下游不再回头。

把请求登记和物化分开，是这个机制的关键。`checkpoint()` 通常写在构建 RDD 的代码里，那一刻还没有任何 action，分区结果还没算出来。若要求它在调用那一刻就把数据写进文件，框架只能自己在背后触发一次 action，把分区算出来再写，凭空多一轮计算。所以请求只负责登记，物化等下一个 action 第一次访问这个 RDD 时再发生——它顺着用户已经要做的计算走，不另起一趟。

物化的目标位置必须能被重新读取。本章用本地临时目录扮演这个角色：写文件、读文件、再切断血缘，与落到分布式存储的流程一致，只是位置换成了本机磁盘。

## 10.6 dependencies 为什么变成 final

上一节验证了 checkpoint 真的切断了血缘：依赖数变 0、下游的 `compute` 次数也变 0。这两件事是怎么在代码上保证的？

checkpoint 其实动了两个入口，职责要分开看：

```text
iterator()      决定分区数据从哪儿读
dependencies()  决定调度器还能不能继续向上找
```

第 7 章的 `DAGScheduler` 会沿着 `rdd.dependencies()` 回溯，遇到窄依赖继续走，遇到 shuffle 依赖切 Stage。

如果 checkpoint 物化后 `dependencies()` 仍然返回父 RDD，调度器还是会继续向上追。那就没有真正切断血缘。

这条规则不能交给每个子类自己处理：否则每个 `MapPartitionsRDD`、`ListRDD` 都得在自己的依赖方法开头判断一遍「我是不是已经 checkpoint 了？是的话就不返回父 RDD」，每个子类都要抄一遍这段判断，只要有一个漏掉，它的 checkpoint 就形同虚设——调度器照样能从它追到父 RDD，血缘根本没断。

所以本章把这条判断收到父类 `RDD` 的 `dependencies()` 里，并设成 `final`，子类不能再重写。父类长这样：

```java
public abstract class RDD<T> {
    // ...

    public final List<Dependency<?>> dependencies() {
        if (checkpointed) {
            return List.of();
        }
        return getDependenciesInternal();
    }

    // 子类只实现这个：返回自己原本的父依赖
    protected abstract List<Dependency<?>> getDependenciesInternal();
}
```

`dependencies()` 是 `final`，里面只做一件事——checkpoint 物化了就返回空，否则委托给 `getDependenciesInternal()`。子类碰不到这段判断，只能实现 `getDependenciesInternal()`，老老实实交代自己的父依赖：

```java
// MapPartitionsRDD：由 map 产生，有一个父 RDD
protected List<Dependency<?>> getDependenciesInternal() {
    return dependencies;
}

// ListRDD：parallelize 产生，本身就是源头，没有父 RDD
protected List<Dependency<?>> getDependenciesInternal() {
    return List.of();
}
```

不管子类原本有没有父依赖，只要 checkpoint 物化，父类的 `dependencies()` 都会拦在前面返回空。前面看到依赖数从 1 变成 0，就是这个 `final` 方法生效的结果。

同一个思路也适用于 cache。前面 10.2 讲过，cache 的命中和填充都发生在 `iterator()` 里，而 `iterator()` 本来就是 `final`——子类不能重写它，只能重写 `compute(partition)`，告诉父类「这个分区原本该怎么算」。至于「命中缓存就直接返回」「没命中就算完存起来」，由父类的 `iterator()` 统一把关。

所以 cache 和 checkpoint 是同一个设计：把短路逻辑收到父类的 `final` 方法里，子类只负责「不短路时原本该怎么做」。区别只在它们改的是哪个入口——cache 改的是数据从哪儿读（`iterator()`），checkpoint 改的是依赖到哪儿停（`dependencies()`）。

## 10.7 本章小结

第 8 章说：血缘让失败的分区可以重算。

本章补上后半句：血缘太长时，重算成本会变高。

cache 和 checkpoint 都是在控制这个成本。

cache 把分区结果留在内存里。它快，适合反复使用的数据，比如逻辑回归每轮都要读的训练数据。但它不切断血缘。缓存丢了，还能从父 RDD 重算。

checkpoint 把分区结果写到磁盘，并让 `dependencies()` 返回空列表。它慢一些，但给容错重算设了上限。PageRank 里不断变长的 `ranks` 就是典型对象。

本章的关键改动集中在一个方法上：

```java
RDD.iterator(partition)
```

前几章里，所有父子 RDD 都通过它读取分区。到了第 10 章，这个统一入口同时承担 cache 和 checkpoint 两种短路：

```text
checkpoint 命中 -> 读磁盘文件
cache 命中      -> 读内存列表
都没命中        -> compute(partition)
```

到这里，`iterator()` 仍然是分区读取入口。cache 和 checkpoint 只是让这个入口在合适的时候提前返回：命中缓存时从内存返回，checkpoint 完成后从文件返回，否则继续沿血缘调用 `compute(partition)`。

下一章不再加新的计算功能。我们会把这 10 章写过的类，和真实 Apache Spark 源码并排放在一起，看看工业级系统到底多了什么、又有哪些地方和你写的一模一样。

