# 手写 mini-spark（写给大数据初学者的 Spark 内核课）

用 **Java 17 从零手写一个 mini-spark**，在亲手实现中理解 Spark 内核：RDD、惰性求值、Shuffle、Stage/DAG、容错、Cache，直至 DataFrame。不需要预先懂 Scala 或分布式——会基础 Java 就够了。

> 在线阅读：[rchaocai.github.io/mini-spark](https://rchaocai.github.io/mini-spark/)

## 目录速览

| 章 | 标题 | module |
|----|------|--------|
| 1 | 从 WordCount 到 50GB 日志 | `ch01-wordcount` |
| 2 | 数据流与延迟迭代 | `ch02-lazy-iterator` |
| 3 | MapPartitionsRDD 与惰性流水线 | `ch03-mappartitions-pipeline` |
| 4 | 血缘与窄依赖 | `ch04-dependencies` |
| 5 | 多线程执行：从分区到 Task | `ch05-multithread-task` |
| 6 | 亲手写一个 Shuffle | `ch06-shuffle` |
| 7 | 划分 Stage 与 DAG | `ch07-stage-dag` |
| 8 | 容错：FaultyIterator 与重算 | `ch08-fault-tolerance` |
| 9 | 从线程池到真正的网络 | `ch09-network-rpc` |
| 10 | Cache 与 Checkpoint | `ch10-cache-checkpoint` |
| 11 | 致敬工业级 Spark | `ch11-real-spark` |
| 12 | 从 RDD 到 DataFrame | `ch12-dataframe-future` |

## 构建

```bash
mvn -q compile                          # 编译全部 12 章
mvn -q -pl ch01-wordcount compile       # 只编译第 1 章
mvn -q -pl ch01-wordcount exec:java -Dexec.mainClass=com.sparklearn.WordCount   # 跑第 1 章示例
```

> 需要 **JDK 17** 与 **Maven**。

## 这本书怎么读

每章是一个独立的 Maven 模块——打开任一 `chNN-*` 目录，就是该章的完整可运行代码。不需要在章节间跳来跳去找依赖。

书稿分为三个部分，建议按顺序阅读：

- **第一部分（Ch1-4）**：RDD 核心——分区、惰性迭代器、流水线、血缘
- **第二部分（Ch5-8）**：Shuffle 与调度——多线程、落盘、DAG、容错
- **第三部分（Ch9-12）**：云端与未来——网络、缓存、真实 Spark 对照、DataFrame

## 许可

本书文字和代码均以 [CC BY-NC-SA 4.0](https://creativecommons.org/licenses/by-nc-sa/4.0/) 许可发布。
