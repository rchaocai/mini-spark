# 第 1 章 · 从 WordCount 开始

本章是 mini-spark 的起点：用最朴素的 Java，在单机内存里统计词频。它是一个基准——当数据量涨到 50GB 时，这段代码会撞上「内存装不下、单核算得慢」两堵墙，从而引出分区与「把代码发给数据」。

- 本章正文：[`content/docs/01-part1/ch01-wordcount.md`](../content/docs/01-part1/ch01-wordcount.md)
- 构建运行：`mvn -pl ch01-wordcount compile exec:java -Dexec.mainClass=com.sparklearn.WordCount`
