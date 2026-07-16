---
title: "第 6 章 · 亲手写一个 Shuffle"
weight: 2
date: 2026-07-16
tags: ["Shuffle", "reduceByKey", "ShuffledRDD", "本地文件", "Partitioner"]
summary: "在第 5 章 TaskScheduler 的基础上，亲手实现一个硬编码 reduceByKey：Map 端按 key 哈希写本地文件，Reduce 端读文件合并，从而看清 Shuffle 为什么是一道真实的物理边界。"
---

# 第 6 章 · 亲手写一个 Shuffle

> 💻 本章完整代码：[GitHub 查看](https://github.com/rchaocai/mini-spark/tree/main/ch06-shuffle)
>
> 构建运行：`mvn -pl ch06-shuffle package && java -Dfile.encoding=UTF-8 -cp ch06-shuffle/target/classes com.sparklearn.Main`

第 5 章完成了一个重要跃迁：RDD 的每个分区都可以变成一个 Task，交给 `TaskScheduler` 并行执行。

不过，到目前为止，所有流水线都有一个共同特点：

```text
分区 0 只读分区 0
分区 1 只读分区 1
分区 2 只读分区 2
```

`map`、`filter`、`flatMap` 都是这样。一个分区从头算到尾，最多沿着窄依赖去读同号父分区，不需要知道其他分区发生了什么。

本章要处理第一个真正麻烦的算子：`reduceByKey`。

先看一组键值对：

```text
分区 0:  (hello,1), (world,1), (hello,1)
分区 1:  (spark,1), (world,1), (hello,1)
分区 2:  (java,1),  (spark,1), (hello,1)
```

我们想得到词频：

```text
hello -> 4
world -> 2
spark -> 2
java  -> 1
```

问题来了：`hello` 分散在 3 个分区里。第 5 章的 Task 只会计算自己负责的分区，分区 0 的 Task 看不到分区 1 和分区 2。只靠原来的“同号父分区一路算下去”，已经不够了。

这就是 Shuffle 要解决的问题：

> 把原来按输入分区摆放的数据，重新按 key 摆放，让相同 key 的值最终来到同一个 Reduce 分区。

代码上，本章不是重写第 5 章的调度器，而是在第 5 章快照上只新增两样东西：

| 新增类 | 职责 |
|---|---|
| `KeyValuePair<K, V>` | 表示 `(key, value)` |
| `ShuffledRDD<K, V>` | 实现硬编码版 `reduceByKey` |

程序入口保持在 RDD 这一层：先用 `SparkContext` 创建 RDD，在键值对 RDD 上调用 `rdd.reduceByKey(...)`，最后用 `shuffled.collect()` 触发 action。`collect()` 会把作业交给上下文运行；再往下，分区任务才会被提交到线程池。

## 6.1 单个分区算不出全局答案

如果只看分区 0：

```text
(hello,1), (world,1), (hello,1)
```

它能算出：

```text
hello -> 2
world -> 1
```

这个结果不是“算错了”。在分区 0 自己的世界里，它完全正确。

但全局答案里，`hello` 应该是 4，`world` 应该是 2。差出来的部分，在别的分区里。

也就是说，`reduceByKey` 和 `map` 不一样。`map` 的每条输入记录只影响一条输出记录，分区之间互不打扰；`reduceByKey` 要把相同 key 的值收拢到一起，天然会跨分区。

最朴素的想法是：那就让几个 Task 共享一个 `HashMap`，大家一起往里面加。

单机 Java 当然做得到。加个锁，或者用 `ConcurrentHashMap`，技术上都能跑。

但这条路和第 5 章刚建立的 Task 边界冲突：第 5 章里，每个 Task 只计算自己的分区，通过返回值交出结果，不写共享容器。这个约束让 Task 天然容易并行，也为后面跨机器执行留下空间。

更重要的是，一旦 Task 不在同一个 JVM 里，共享内存这条路就不存在了。线程之间还能共享对象，机器之间不能共享内存地址。

所以本章坚持一个规则：

```text
Task 之间不直接通信。
```

要跨分区交换数据，就必须把上游产物放到一个下游能够重新读取的位置。我们先用最朴素、最容易观察的办法：写本地文件。

## 6.2 先把键值对落成代码

`reduceByKey` 处理的是 `(key, value)`。本章先加一个很小的 record：

```java
public record KeyValuePair<K, V>(K key, V value) implements Serializable {
}
```

完整实现见 [`KeyValuePair.java`](https://github.com/rchaocai/mini-spark/tree/main/ch06-shuffle/src/main/java/com/sparklearn/KeyValuePair.java)。

它只负责保存一对值。比如一条词频输入就是：

```java
new KeyValuePair<>("hello", 1)
```

接着，用 `SparkContext.parallelize(...)` 把这些键值对切成 3 个 Map 分区：

```java
RDD<KeyValuePair<String, Integer>> rdd = sc.parallelize(words, 3);
```

注意变量类型写的是 `RDD`，不是 `ListRDD`。用户只需要拿到一个抽象 RDD；至于它底下是不是来自本地 List，是实现细节。这里的实际对象仍然是 `ListRDD`，它按连续区间切分 List。也就是说，本章只是增加了 Shuffle 逻辑，不是换了一套数据源。

## 6.3 Map 端：按 key 哈希写文件

现在开始写 `reduceByKey`。

先看调用方式：

```java
ShuffledRDD<String, Integer> shuffled = rdd.reduceByKey(
        Integer::sum, 2);
```

完整实现见 [`ShuffledRDD.java`](https://github.com/rchaocai/mini-spark/tree/main/ch06-shuffle/src/main/java/com/sparklearn/ShuffledRDD.java)。

这里的 2 表示有 2 个 Reduce 分区。构造 `ShuffledRDD` 时不会立刻计算，也不会写文件。它和前几章的 `map`、`filter` 一样，只是记录一份“将来怎么算”的配方。

先把 `ShuffledRDD.compute()` 当成一条完整路线看：

```java
public Iterator<KeyValuePair<K, V>> compute(Partition partition) {
    ensureMapPhase();
    return toKeyValuePairs(readAndMergeReducePartition(partition.index()));
}
```

这段代码先做两件事：保证 Map 阶段已经写完文件，再读取当前 Reduce 分区对应的文件并合并。整体就这么简单。后面再把这两步拆开看。

> [!INFO]
> **这里为什么由 `ShuffledRDD` 同时写和读？**
>
> 用户写的是 `rdd.reduceByKey(...)`，底层返回的是 `ShuffledRDD`。这是当前 API 的一个小妥协：方法暂时挂在所有 `RDD` 上，Java 类型系统还不能阻止你在非键值对 RDD 上误用它。专门的键值对 RDD 可以解决这个类型边界；这里先保持调用方向简单。
>
> 另外，本章还没有调度器来拆分“先跑 Map 任务、再跑 Reduce 任务”，所以“通知上游 Map 分区先落盘”这件事，暂时放在 `ShuffledRDD.compute()` 里面做。
>
> 当前代码的执行顺序是：`collect(shuffled)` 先提交 2 个 Reduce 分区任务；第一个进入 `compute()` 的线程拿到锁，顺序遍历父 RDD 的 3 个分区，写出 `3 × 2` 个文件；随后 2 个 Reduce 分区任务分别读自己的文件。
>
> 当执行层继续展开以后，这个物理过程会被拆开：Map 任务先在持有输入分区的位置写出 shuffle 文件，Reduce 任务再读取这些文件并合并。逻辑 API 上仍然是 `rdd.reduceByKey(...)` 返回一个下游 `ShuffledRDD`，不是两个逻辑 RDD；只是执行层会分成两批任务。

### 先保证 Map 阶段只跑一次

```java
private void ensureMapPhase() {
    if (!mapPhaseDone) {
        synchronized (this) {
            if (!mapPhaseDone) {
                runMapPhase();
                mapPhaseDone = true;
            }
        }
    }
}
```

为什么这里要加一个小小的 `synchronized`？因为第 5 章的 `TaskScheduler` 会并行计算多个 Reduce 分区。两个工作线程可能同时进入 `compute()`，Map 阶段只应该写一次文件，所以这里用一个很小的临界区守住“只跑一次”。

### 再写出 Map 端文件

进入 `runMapPhase()` 后，逻辑分两层：

1. 遍历父 RDD 的每个 Map 分区。
2. 对每个 Map 分区，准备 N 个桶，N 等于 Reduce 分区数。

每读到一条键值对，就用 key 的哈希值决定它进入哪个桶：

```java
static int partition(Object key, int numPartitions) {
    return (key.hashCode() & Integer.MAX_VALUE) % numPartitions;
}
```

这行代码很普通，却是 Shuffle 的核心：它把“原来在哪个输入分区”这件事抹掉，重新按 key 决定“应该去哪个 Reduce 分区”。

然后把值放进对应桶里：

```java
int bucketId = partition(kv.key(), numReducePartitions);
buckets.get(bucketId).merge(kv.key(), kv.value(), reduceFunc);
```

`merge` 做了两件事。

如果这个 key 第一次进入桶，就把它放进去。如果桶里已经有这个 key，就用 `reduceFunc` 把旧值和新值合并。

这叫 Map 端本地 combine。它的好处很直接：同一个 Map 分区里重复出现的 key，不用一条一条写到磁盘，而是先合并成一条再写。

例如分区 0 里有两个 `hello`：

```text
(hello,1), (world,1), (hello,1)
```

写盘前就可以先变成：

```text
hello -> 2
world -> 1
```

最后，每个桶写成一个文件。文件名同时记录来源和去向：

```text
map_0_reduce_0
map_0_reduce_1
map_1_reduce_0
map_1_reduce_1
map_2_reduce_0
map_2_reduce_1
```

3 个 Map 分区，2 个 Reduce 分区，所以一共 6 个文件。空桶也会写一个表示 `size=0` 的文件。这样 Reduce 端可以清楚地区分“这个桶为空”和“这个文件丢了”。

文件格式不是本章重点。代码里直接用了 Java 自带的 `ObjectOutputStream`，把 key 和 value 按对象写进去。因为用了对象序列化，本章示例默认 key 和 value 都能被序列化；`String` 和 `Integer` 没问题，如果换成自定义对象，就要自己实现可序列化。现在先关心 Shuffle 的物理过程：谁写、写到哪、谁再读。

## 6.4 Reduce 端：读属于自己的那批文件

Reduce 端的整体逻辑也先看一眼：

```java
private Map<K, V> readAndMergeReducePartition(int reduceId) {
    Map<K, V> merged = new HashMap<>();
    for (int mapId = 0; mapId < numMapPartitions; mapId++) {
        Map<K, V> mapOutput = readMapOutput(mapId, reduceId);
        for (var entry : mapOutput.entrySet()) {
            merged.merge(entry.getKey(), entry.getValue(), reduceFunc);
        }
    }
    return merged;
}
```

它做的事很直白：给定一个 Reduce 分区编号，把所有对应的 Map 文件都读出来，顺手合并成一个结果 `Map`。真正的细节，还是要拆开看。

Map 端写完以后，Reduce 分区开始计算。

Reduce 分区 0 只读所有 `*_reduce_0` 文件：

```text
map_0_reduce_0
map_1_reduce_0
map_2_reduce_0
```

Reduce 分区 1 只读所有 `*_reduce_1` 文件：

```text
map_0_reduce_1
map_1_reduce_1
map_2_reduce_1
```

对应代码也很直接：

```java
Map<K, V> merged = readAndMergeReducePartition(reduceId);
```

注意，Reduce 端又用了一次同样的 `merge`。

Map 端的 `merge` 是把同一个 Map 分区内部的重复 key 先合并。Reduce 端的 `merge` 是把来自多个 Map 分区的局部结果合并成全局结果。

也就是说，`reduceByKey` 在这个 mini 实现里就是两段合并：

```text
Map 端：同一输入分区内先合并
Reduce 端：不同输入分区的局部结果再合并
```

同一个 `reduceFunc` 被用了两次。对于整数求和，这完全自然：

```java
Integer::sum
```

这也解释了为什么这类 reduce 函数要满足结合律。先局部合并，再全局合并，结果应该和从头到尾一次性合并一样。本章用整数加法，因为它既满足结合律，也满足交换律；像减法这类和合并顺序强相关的函数，就不适合直接拿来做 `reduceByKey`。

## 6.5 跑一次，看见中间文件

先看 demo 入口，运行的就是这段流程。完整实现见 [`Main.java`](https://github.com/rchaocai/mini-spark/tree/main/ch06-shuffle/src/main/java/com/sparklearn/Main.java)。

```java
public static void main(String[] args) {
    demonstrateShuffle();
}

private static void demonstrateShuffle() {
    List<KeyValuePair<String, Integer>> words = Arrays.asList(
            new KeyValuePair<>("hello", 1),
            new KeyValuePair<>("world", 1),
            new KeyValuePair<>("hello", 1),
            new KeyValuePair<>("spark", 1),
            new KeyValuePair<>("world", 1),
            new KeyValuePair<>("hello", 1),
            new KeyValuePair<>("java", 1),
            new KeyValuePair<>("spark", 1),
            new KeyValuePair<>("hello", 1));

    try (SparkContext sc = new SparkContext(2, true)) {
        RDD<KeyValuePair<String, Integer>> rdd = sc.parallelize(words, 3);
        ShuffledRDD<String, Integer> shuffled = rdd.reduceByKey(
                Integer::sum, 2);

        System.out.println("reduceByKey 结果: " + shuffled.collect());
    }
}
```

前两步只是准备数据和构造 `ShuffledRDD`。真正触发 Shuffle 的，是最后这一句 `shuffled.collect()`。

运行本章 demo：

```bash
mvn -pl ch06-shuffle package
java -Dfile.encoding=UTF-8 -cp ch06-shuffle/target/classes com.sparklearn.Main
```

前半段会打印输入数据和 Map 分区：

```text
分区 0: [KeyValuePair[key=hello, value=1], KeyValuePair[key=world, value=1], KeyValuePair[key=hello, value=1]]
分区 1: [KeyValuePair[key=spark, value=1], KeyValuePair[key=world, value=1], KeyValuePair[key=hello, value=1]]
分区 2: [KeyValuePair[key=java, value=1], KeyValuePair[key=spark, value=1], KeyValuePair[key=hello, value=1]]
```

然后构造 `ShuffledRDD`：

```text
这里只是构造结果 RDD，还没有真正写文件
```

这句话很重要。`reduceByKey` 仍然是 transformation，不是 action。真正触发计算的是后面的：

```java
shuffled.collect()
```

`shuffled.collect()` 会先进入 `SparkContext.runJob(...)`，再由底层 `TaskScheduler` 为 `ShuffledRDD` 的 2 个 Reduce 分区提交 2 个 Task。第一次进入 `ShuffledRDD.compute()` 时，当前线程会先顺序推动 3 个父分区写出中间文件。随后两个 Reduce 分区任务再读这些文件，合并出最终结果。

结果里 key 的顺序不重要。当前实现最后从 `HashMap` 取结果，输出顺序可能和前面列出的期望顺序不同；只要每个 key 的计数对上，就是正确结果。

demo 还会打印 Shuffle 目录：

```text
Shuffle 目录: /var/folders/.../spark-shuffle-...
  map_0_reduce_0  (...)
  map_0_reduce_1  (...)
  map_1_reduce_0  (...)
  map_1_reduce_1  (...)
  map_2_reduce_0  (...)
  map_2_reduce_1  (...)
```

你不需要关心临时目录的具体路径。关键是这 6 个文件真的出现在磁盘上了。

到这里，`reduceByKey` 的路线已经完整：

```text
父 RDD 的 3 个 Map 分区
  -> 按 key 哈希写成 3 × 2 个文件
  -> 2 个 Reduce 分区分别读属于自己的文件
  -> 合并相同 key
  -> collect 收回结果
```

## 6.6 删除文件，Reduce 端立刻失败

demo 最后做了一件有点“粗暴”的事：先成功 `collect` 一次，又对同一个 `ShuffledRDD` 再 `collect` 一次，确认它会复用已经写好的中间文件；随后删除刚才那 6 个中间文件，再对同一个 `ShuffledRDD` 第三次 `collect`。

第三次会失败。

这不是 bug。恰恰相反，它说明我们真的写出了 Shuffle。

在本章 mini 实现里，`ShuffledRDD` 的 Map 阶段已经完成过一次，后面的 Reduce 分区不再重新走父 RDD 的迭代器，而是读磁盘上的 Map 输出文件。文件删掉以后，下游就没有输入了。

任何 shuffle 系统都必须先有 Map 输出，Reduce 端才能读取。成熟的分布式执行器可以重新调度丢失的 Map 任务，把缺失的 shuffle 输出再写出来。本章还没有容错和重试，所以会直接失败。

换句话说，中间文件不是日志，不是调试产物，也不是“顺手写一下”。它就是 Map 端和 Reduce 端之间传递数据的物理介质。

这也是本章最重要的一点：

```text
Shuffle 不是一个抽象名词。
它首先是一批被写下来的中间数据。
```

现在先不要急着给这条边界起更多名字。先把手感建立起来：只要同 key 数据散在多个分区里，就必须先重新分布；重新分布，就一定要有一个中间落点。等调度器开始关心执行顺序时，这个中间落点就会变成一条清晰的执行边界。

## 6.7 从哈希分桶到 Partitioner

最后回头看这行代码：

```java
(key.hashCode() & Integer.MAX_VALUE) % numPartitions
```

它决定每个 key 进入哪个 Reduce 分区。这个“决定 key 去哪”的规则，有一个正式名字：**Partitioner**。

本章没有把它抽成一个独立类。现在如果立刻引入 `Partitioner`、`ShuffleManager`、`PairRDD`，代码会变得比问题本身还大。我们先把硬编码版跑通，让读者看见最小的物理过程。

但这个名字值得先记住。

如果两个 RDD 使用同一个 Partitioner，并且 Reduce 分区数也相同，那么相同 key 会天然落到同一个分区里。将来做 join 时，它们可能就不需要再次 Shuffle。

这叫 co-partitioning。它不是本章要实现的功能，只是一颗伏笔。先记住这句话就够了：

> Shuffle 的核心是按 key 重新分布；如果数据已经按同一套规则分好了，就可以少搬一次。

## 6.8 本章小结

本章在第 5 章的 TaskScheduler 基础上，只增加了一个最小版 `reduceByKey`。

它做的事情可以压缩成三步：

1. Map 端遍历父 RDD 分区，按 key 哈希进入 Reduce 桶。
2. 每个 Map 分区把每个 Reduce 桶写成一个本地文件。
3. Reduce 分区读取属于自己的那批文件，再次合并相同 key。

这套实现很小，但它已经抓住了 Shuffle 最核心的事实：跨分区的同 key 数据不会自己聚到一起，必须被重新分布；重新分布需要中间产物。在本章 mini 实现里，中间文件一旦丢失，下游计算就无法继续；后面的容错章节会再补上“如何重算回来”。

下一章，我们会把这道物理边界交给调度器。到那时，再来回答一个更大的问题：一条 RDD 血缘里，哪些计算可以放在一起跑，哪些地方必须切开？
