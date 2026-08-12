# mini-spark

用 **Java 17 从零手写一个 mini-spark**，在亲手实现中理解 Spark 内核：RDD、惰性求值、Shuffle、Stage/DAG、容错、Cache，直至 Streaming 与 DataFrame。不需要预先懂 Scala 或分布式——会基础 Java 就够了。

## 模块速览

共 15 个编号章节模块与 2 个附录模块。其中第 13 章分为上、中、下三个模块，共 17 个代码模块。

| 部分 | 模块 | 主题 |
|------|------|------|
| 第一部分 · RDD 核心 | Ch1-4 | 分区、惰性迭代器、流水线、血缘与依赖 |
| 第二部分 · 执行引擎 | Ch5-9 | Task、Shuffle、Stage/DAG、多作业调度与容错 |
| 第三部分 · 分布式计算与流处理 | Ch10-12 | Executor、网络通信、Cache、Checkpoint 与 DStream |
| 第四部分 · 结构化计算与工业实践 | Ch13（上中下）、Ch14-15 | DataFrame、Catalyst、代码生成、Structured Streaming 与工业级 Spark |
| 附录 | A-B | mini-spark 机器学习实战、ANTLR4 与 Janino 入门 |

## 构建

```bash
mvn -q compile                          # 编译全部模块
mvn -q -pl ch01-wordcount compile       # 只编译第 1 章
mvn -q -pl ch01-wordcount exec:java -Dexec.mainClass=com.sparklearn.WordCount   # 跑第 1 章示例
```

> 需要 **JDK 17** 与 **Maven**。

## 代码组织

每个模块是一个独立的 Maven 子模块——打开任一 `chNN-*` 目录，就是该模块的完整可运行代码，模块之间没有横向依赖，可以独立编译和运行。

各模块按功能分包：

- `rdd` — RDD 基类与具体实现（ListRDD、MapPartitionsRDD、ShuffledRDD 等）
- `scheduler` — DAGScheduler、TaskScheduler、Stage 与 Task
- `executor` — Executor 与 Task 执行
- `storage` — BlockManager 与 Cache
- `shuffle` — Shuffle 写入与读取
- `util` — 工具类
- `rpc` — 网络通信（第 10 章起）
- `streaming` — DStream 与 Structured Streaming（第 12、14 章）
- `sql` — DataFrame、Catalyst 优化器与 SQL 解析（第 13 章）

## 许可

代码以 [CC BY-NC-SA 4.0](https://creativecommons.org/licenses/by-nc-sa/4.0/) 许可发布。
