# 第 1 章 · 从 WordCount 开始

本章是 mini-spark 的起点：用最朴素的 Java，在单机内存里统计词频。它是一个基准——当数据量涨到 50GB 时，这段代码会撞上「内存装不下、单核算得慢」两堵墙，从而引出分区、并行，以及后面 RDD 要解决的数据复用问题。

核心内容：

- `WordCount`：朴素单机版词频统计，所有数据放在一个 `List` 里，单线程循环处理。
- `Main`：模块入口，转发到 `WordCount`，便于统一运行命令。
- 第 1 章正文：从这段小程序出发，推导出「分块」和「并行」两件事为什么必要。

运行：

```bash
mvn -pl ch01-wordcount compile exec:java -Dexec.mainClass=com.sparklearn.Main
```

运行测试：

```bash
mvn -pl ch01-wordcount test
```

看输出时重点不是词频本身，而是这段程序的限制：

```text
List<String> lines       假设所有输入都能放进单机内存
for (...)                假设只有一个线程顺序处理全部数据
HashMap<String, Integer> 假设最终计数表也能放进单机内存
```

下一章会把「一批数据」抽成 `ListRDD`，开始给分区、访问方式和延迟计算找一个代码里的落点。
