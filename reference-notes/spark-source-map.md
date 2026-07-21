# 真实 Spark 源码对照地图

> 支撑第 11 章（运行行为对照），也供各章 spec「参考对照」一节引用。
> 参考工程：[apache/spark](https://github.com/apache/spark) 的 [`branch-0.5`](https://github.com/apache/spark/tree/branch-0.5) 分支（约 `v0.5.2`，早期 Apache Spark，**Scala / SBT，无 Spark SQL**）。
> 下表「真实 Spark 文件」均相对仓库根目录（如 `core/src/main/scala/spark/RDD.scala`）。

## Scala 速查（给 Java 读者）
- `case class Foo(x: Int)` ≈ Java `record Foo(int x)`。
- `trait` ≈ Java `interface`（可带默认方法）。
- `sealed trait` ≈ Java `sealed interface`。
- `object Bar` ≈ 单例 / 静态工具类。
- `def f(x: Int): Int = ...` ≈ Java 方法。
- `class C(...) { ... }` 的主构造器参数 ≈ Java 字段 + 构造器。

## 概念 → 真实 Spark 文件

| 本书概念 | 章 | 真实 Spark 文件 | 说明 |
|---|:--:|---|---|
| RDD 抽象 | 4 | `core/src/main/scala/spark/RDD.scala` | `abstract class RDD`：`splits` / `compute` / `dependencies` / `preferredLocations` |
| Partition | 2–4 | `core/src/main/scala/spark/Split.scala` | `trait Split { val index: Int }`（本书用 `record Partition(int index)`） |
| iterator() / compute() | 2–3 | `core/src/main/scala/spark/RDD.scala` | `final def iterator(split)`：先查缓存再 `compute(split)` |
| MapPartitionsRDD / MappedRDD | 3 | `core/src/main/scala/spark/RDD.scala` | `compute(split) = f(prev.iterator(split))`——包装父迭代器 |
| 依赖 / 窄依赖 | 4 | `core/src/main/scala/spark/Dependency.scala` | `abstract class Dependency[T](val rdd, val isShuffle: Boolean)` / `OneToOneDependency` / `NarrowDependency` |
| 多线程 / 本地调度 | 5 | `core/src/main/scala/spark/Executor.scala`、`LocalScheduler.scala` | 本书 `LocalTaskScheduler`：线程池并行跑 Task |
| reduceByKey | 6 | `core/src/main/scala/spark/PairRDDFunctions.scala` | `reduceByKey` 经 `combineByKey` + `ShuffledRDD` |
| Shuffle（Map 写） | 6 | `core/src/main/scala/spark/ShuffleMapTask.scala` | Map 端写本地文件 |
| Shuffle（Reduce 读） | 6 | `core/src/main/scala/spark/SimpleShuffleFetcher.scala` | HTTP 拉取 shuffle 数据 |
| Shuffle 管理 | 6 | `core/src/main/scala/spark/ShuffleManager.scala` | 注册 / 获取 shuffle |
| ShuffledRDD | 6 | `core/src/main/scala/spark/ShuffledRDD.scala` | 表示 shuffle 后的 RDD |
| 宽依赖 | 7 | `core/src/main/scala/spark/Dependency.scala` | `ShuffleDependency` |
| Stage | 7 | `core/src/main/scala/spark/Stage.scala` | `isShuffleMap = shuffleDep != None` |
| DAG / DAGScheduler | 7 | `core/src/main/scala/spark/DAGScheduler.scala` | `newStage` / `getParentStages` / 按 Stage 提交 |
| 容错重试 | 8 | `core/src/main/scala/spark/LocalScheduler.scala` | `failCount` / `maxFailures` 重试 |
| fetch failure | 8 | `core/src/main/scala/spark/DAGScheduler.scala` | shuffle 拉取失败处理 |
| Cache / persist | 10 | `core/src/main/scala/spark/RDD.scala`（`cache()`）、`CacheTracker.scala`、`BoundedMemoryCache.scala` | 内存缓存 + 缓存跟踪 |
| Checkpoint（切断血缘） | 10 | （**0.5 无**；v0.7.0 起）`rdd/CheckpointRDD.scala`、`RDD.scala` | `checkpoint()`；checkpoint 后 `final def dependencies` 改指向 `CheckpointRDD`，切断血缘 |
| RPC / 分布式通信 | 9 | `core/src/main/scala/spark/Executor.scala`、`HttpServer` | 本书 `NetworkTaskScheduler`/`Executor` 用 Java Socket；参考工程用 **Scala Actors + HTTP** |
| DataFrame / Catalyst | 12 | （参考工程无） | 指向**现代 Spark** 的 `sql/catalyst` 模块 |

## 备注
- 参考工程为 **RDD 时代 Spark**（无 Spark SQL）。第 9 章 RPC、第 12 章 DataFrame/Catalyst 在参考工程中**无直接对应**，需自实现或对照现代 Spark。
- 第 11 章建议按上表逐行打开真实文件与本书实现**并排阅读**，体会「核心一模一样」。
