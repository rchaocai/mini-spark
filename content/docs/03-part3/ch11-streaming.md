---
title: "第 11 章 · Spark Streaming 与 DStream"
weight: 3
date: 2026-07-22
tags: ["Streaming", "DStream", "微批", "window", "StreamingContext"]
summary: "在第 10 章完整 RDD 内核上实现教学版 Spark Streaming：把连续输入切成 batch，每个 batch 还是一批普通 RDD，DStream 只是把这些 batch 串起来。"
---

# 第 11 章 · Spark Streaming 与 DStream

> 💻 本章完整代码：[GitHub 查看](https://github.com/rchaocai/mini-spark/tree/main/ch11-streaming)
>
> 构建运行：`mvn -pl ch11-streaming package`
>
> 运行示例：`java -Dfile.encoding=UTF-8 -cp ch11-streaming/target/classes com.sparklearn.streaming.Main`

前 10 章，你已经写出了一台能跑的 mini-Spark。

```text
RDD / Dependency / Stage / DAGScheduler
Task / Shuffle / cache / checkpoint
```

这一章不再加一台新引擎。

这一章只做一件事：给这台引擎装上“时间”。

Spark Streaming 的思路很朴素：

```text
连续输入
  → 按时间切成 batch
  → 每个 batch 变成一个 RDD
  → 对这个 RDD 跑 map / reduceByKey / window
  → 下一批再来一次
```

这就是 micro-batch。它不是“每来一条就立刻处理”的事件循环，而是“每隔一小段时间，统一提交一批普通 Spark job”。

真实 Spark 0.7 里，Streaming 还是一个独立模块，但它的骨架已经很清楚了：

```text
StreamingContext
  DStream
  DStreamGraph
  QueueInputDStream / WindowedDStream / ForEachDStream
```

本章就照着这个骨架，做一个能看懂、能跑、还能对照源码的最小版本。

## 11.1 先看最小闭环

先别管类图。

先看最小演示。

`Main` 里，我们先准备 3 个输入 batch，再把它们塞进队列：

```text
batch1: "hello spark" / "hello stream"
batch2: "spark streaming" / "hello spark"
batch3: "mini spark streaming"
```

然后把这条流写成：

```text
queueStream
  → flatMap 拆词
  → map 成 (word, 1)
  → reduceByKey
  → foreachRDD 打印
```

再加一条 window 支线：

```text
最近 2 秒内的词频
```

最后手动推进 4 个 batch：

```text
ssc.start()
ssc.advance(4)
```

这 4 次推进里，前 3 次有输入，第 4 次已经没有新数据，但 batch 仍然会走完。这个细节很重要：流系统不能因为“这一拍没新输入”就把时钟停掉，否则 window、checkpoint、定时输出都会断。

## 11.2 先把时间说清楚

`Duration` 和 `Time` 是这一章最底层的两个对象。

```text
Duration = 多长时间
Time     = 某个逻辑时刻
```

在本章代码里：

```text
batchDuration = 1s
zeroTime      = Time(0)
Time(1000)    = 第 1 个 batch
Time(2000)    = 第 2 个 batch
Time(3000)    = 第 3 个 batch
```

你可以把它理解成一把很简单的尺子：每次 `advance()`，时间就往前走一格。

这比真实墙钟更适合教学，因为它把两个问题拆开了：

```text
1. 什么时候生成 batch
2. batch 里到底怎么算
```

我们先只看第 2 个问题。

## 11.3 DStream 不是“另一种数据结构”

`DStream` 这个名字容易吓人，其实它很朴素。

在本书里，它就是：

```text
按时间排列的一串 RDD
```

每个 DStream 都只回答三件事：

```text
slideDuration()   多久生成一次 RDD
dependencies()    它依赖哪些父流
compute(time)     某个时间点上，这个 RDD 怎么来
```

这和 RDD 的心智模型很像：

```text
RDD 关心“怎么从父 RDD 算出来”
DStream 关心“怎么从父 DStream 算出某个时间点的 RDD”
```

所以你会看到 `map`、`filter`、`flatMap`、`reduceByKey`、`window` 这些操作都还在。只是它们返回的不再是一个立刻可用的结果，而是一个新的 DStream。

### getOrCompute 为什么重要

`getOrCompute(time)` 做的事也不复杂：

```text
如果这个时间点已经算过，就直接拿缓存
如果没算过，就先 compute(time)
```

这一步很像第 2 章的迭代器流水线，只不过这里缓存的是“某个时刻的 RDD”，不是单个元素。

这也是 window 能工作的基础。因为 window 不是“只看当前 batch”，而是会回头用到前几个 batch 的结果。

## 11.4 先看代码怎么走：StreamingContext

`StreamingContext` 是这一章的入口。

它站在 `SparkContext` 上面，职责只有一个：

```text
按 batch 间隔，不断把 DStream 图变成一批批 job
```

你可以把它理解成一个很薄的外壳：

```text
SparkContext      负责跑 RDD job
StreamingContext  负责按时间重复跑这些 job
```

本章的 `Main` 也是这么用它的：

```java
try (SparkContext sc = new SparkContext(2, false);
     StreamingContext ssc = new StreamingContext(sc, Duration.seconds(1))) {
    ...
    ssc.start();
    ssc.advance(4);
}
```

这里没有 socket，没有 Kafka，没有 receiver。我们故意先用 `queueStream`，因为它最容易看清楚“流”到底是怎么一批一批走的。

## 11.5 输入流：queueStream

`queueStream` 是这一章最适合初学者的输入源。

它做的事非常直接：

```text
把一队 RDD 当成连续输入
每个 batch 从队列里取一个，或者取当前队列快照
```

所以它有两个常见模式：

```text
oneAtATime = true   每个 batch 取一个 RDD
oneAtATime = false  每个 batch 看当前队列里的所有 RDD，但不把它们删掉
```

源码里还有一个很关键的细节：当队列空了，仍然可以返回一个默认的空 RDD。这样 batch 的节奏不会停，时间还会往前走，window 和输出操作也还能继续跑。

本章代码保留了这个行为。

这也是为什么你在演示里会看到第 4 个 batch 仍然存在，只不过它已经没有新词了。

### 为什么这里不用 socket

不是不能写 socket。

而是如果一上来就把输入换成网络，你会被序列化、线程、连接、异常处理这些噪声拖走，看不清 Streaming 的本体。

先用 queue，把“时间”单独拎出来，最划算。

## 11.6 一条流怎么变成一批 job

现在看最关键的一段。

`Main` 里这条流：

```text
queueStream
  → flatMap
  → map
  → reduceByKey
  → foreachRDD
```

其实只是把一串 DStream 对象接起来。真正计算发生在 `advance()` 的时候。

当时间走到 `Time(1000)`，大概会发生这样一串事：

```text
ForEachDStream.generateJob(1000)
  → parent.getOrCompute(1000)
      → ReducedDStream.compute(1000)
          → parent.getOrCompute(1000)
              → MappedDStream.compute(1000)
                  → parent.getOrCompute(1000)
                      → FlatMappedDStream.compute(1000)
                          → parent.getOrCompute(1000)
                              → QueueInputDStream.compute(1000)
  → 拿到这一批的 RDD
  → 在这批 RDD 上执行输出动作
```

这就是本章最重要的心智模型：

```text
DStream 只是“推迟执行的计划”
真正干活的还是 RDD
```

所以你会发现，Streaming 没有改变第 1 到第 10 章的核心机制。它只是把这些机制放进了一个时间循环里。

> [!INFO]
> **对照真实 Spark 0.7**
>
> 你本地的 `/Users/cairuchao/project/spark/streaming` 里，核心文件已经很接近：
>
> ```text
> StreamingContext.scala
> DStream.scala
> DStreamGraph.scala
> dstream/QueueInputDStream.scala
> dstream/WindowedDStream.scala
> dstream/ForEachDStream.scala
> ```
>
> 真实实现多了 receiver、网络输入、checkpoint 恢复和更多输入源；但“DStream 按时间生成 RDD”这个核心形状是一致的。

## 11.7 window 为什么能跨 batch

window 是流计算里最像“真的在看一段时间”的操作。

它回答的问题是：

```text
不是只看当前 batch
而是看最近 N 秒
```

在本章代码里，`window(windowDuration, slideDuration)` 的实现很直白：

```text
把最近若干个 batch 的 RDD 取出来
用 UnionRDD 拼成一个大 RDD
再继续 map / reduceByKey
```

比如：

```text
windowDuration = 2s
slideDuration  = 1s
```

那么：

```text
Time(1000) -> 只有 batch1
Time(2000) -> batch1 + batch2
Time(3000) -> batch2 + batch3
Time(4000) -> batch3 + 一个空 batch
```

这一段非常适合初学者先这样理解：

```text
window = “把最近几批数据先并起来，再按普通 RDD 算一遍”
```

后面如果你再去看 Spark 的增量 window 算法，就会知道它为什么值得优化。

## 11.8 输出流：foreachRDD 才是扳机

如果说 `map`、`filter`、`window` 都是在搭计划，那 `foreachRDD` 才是把计划变成现实的扳机。

本章里，输出流不是返回一个新 DStream，而是注册一个每个 batch 都会执行的动作：

```text
拿到当前 batch 的 RDD
打印
或者写到外部系统
```

这和 RDD 的 action 很像：

```text
RDD:    transform 惰性，action 触发
DStream: transform 惰性，output operation 触发
```

所以这里最该记住的，不是 `foreachRDD` 这个名字，而是它的角色：

```text
没有输出动作，流图只是图
有了输出动作，图才会在每个 batch 上真正跑起来
```

## 11.9 这一章刻意没做什么

这章最容易写散的地方，就是一口气把所有 Streaming 特性都塞进来。

我们没有这么做。

本章刻意没实现的东西包括：

```text
socket / Kafka / Flume receiver
backpressure
并发 job 调度
Driver 故障恢复
reduceByKeyAndWindow 的增量优化
Structured Streaming
```

原因很简单：

```text
这些都属于工业级外衣
本章先把微批本体看清
```

如果你现在已经明白：

```text
连续输入
→ batch
→ RDD
→ DStream
→ job
```

那这一章就算达成任务了。

## 11.10 本章小结

这一章其实只是在第 10 章的基础上，多加了一层时间。

```text
core       = 会算一个 RDD
streaming  = 会按时间反复算一串 RDD
```

你已经能把这条链路讲清楚：

```text
输入先变成 batch
batch 再变成 RDD
DStream 只负责把这些 RDD 串起来
window 负责回看最近几批
foreachRDD 负责把每个 batch 真正执行掉
```

这就是 Spark Streaming 最朴素、也最重要的骨架。
