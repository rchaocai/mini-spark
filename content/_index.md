---
title: 手写 mini-spark
bookToc: false
---

# 手写 mini-spark

## 写给大数据初学者的 Spark 内核课

用 **Java 17 从零手写一个 mini-spark**，在亲手实现中理解 Spark 内核：RDD、惰性求值、Shuffle、Stage/DAG、容错、Cache，直至 Streaming 与 DataFrame。不需要预先懂 Scala 或分布式——会基础 Java 就够了。

[开始阅读 →]({{< relref "docs/00-preface/_index.md" >}})

---

## 全书三部分

### 第一部分 · RDD 核心

| 章 | 标题 |
|---|------|
| 1 | 从 WordCount 开始 |
| 2 | 数据流与延迟迭代 |
| 3 | RDD 的惰性流水线 |
| 4 | 血缘与依赖 |

### 第二部分 · 调度与 Shuffle

| 章 | 标题 |
|---|------|
| 5 | 从分区到 Task |
| 6 | 亲手写一个 Shuffle |
| 7 | 划分 Stage 与 DAG |
| 8 | 容错：FaultyIterator 与重算 |

### 第三部分 · 云端与未来

| 章 | 标题 |
|---|------|
| 9 | 从单机到分布式执行 |
| 10 | Cache 与 Checkpoint |
| 11 | Spark Streaming 与 DStream |
| 12 | 从 RDD 到 DataFrame |
| 13 | 致敬工业级 Spark |

---

## 构建运行

```bash
git clone https://github.com/rchaocai/mini-spark.git
cd mini-spark
mvn -pl ch01-wordcount compile exec:java -Dexec.mainClass=com.sparklearn.WordCount
```

> 需要 **JDK 17** 与 **Maven**。

---

[GitHub 源码](https://github.com/rchaocai/mini-spark) · [许可协议](https://creativecommons.org/licenses/by-nc-sa/4.0/)
