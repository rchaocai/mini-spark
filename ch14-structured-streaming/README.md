# 第 14 章 · Structured Streaming

本章在第 12 章的 DataFrame / SQL 引擎上，加一个最小版 Structured Streaming。

核心思路：

```text
StreamingRelation
  -> 每个微批替换成 Scan
  -> 复用 Catalyst 优化
  -> 复用物理计划
  -> 落回 RDD / Stage / Task
  -> 写入 Sink
```

## 本章实现了什么

- `Offset` / `LongOffset`：记录流式数据处理到哪里
- `Batch`：一个微批的数据和结束 offset
- `Source` / `MemoryStream`：用内存模拟持续到来的数据
- `Sink` / `MemorySink`：保存每个微批的输出结果
- `StreamingRelation`：逻辑计划里的流式叶子节点
- `StreamExecution`：把流式节点替换成当前批数据，再走 DataFrame 执行链路
- SQL 查询入口：`SELECT word, count(*) FROM words GROUP BY word`

## 教学边界

本章实现的是无状态、按微批独立执行的教学版。`groupBy().count()` 会对当前新批次做聚合，然后把结果追加到 `MemorySink`。

真实 Spark Structured Streaming 的跨批聚合需要状态存储、checkpoint、watermark 和输出模式语义。本章只保留主线：流式数据怎样接进 DataFrame 逻辑计划。

## 推荐阅读顺序

1. **Main.java** - 先看 DataFrame API 和 SQL 两种流式 WordCount 写法
2. **Offset.java + LongOffset.java + Batch.java** - 理解微批进度模型
3. **Source.java + Sink.java** - 理解数据从哪里来、结果写到哪里
4. **MemoryStream.java + MemorySink.java** - 看内存版 Source/Sink 怎么工作
5. **StreamingRelation.java** - 看流式数据源如何进入逻辑计划树
6. **StreamExecution.java** - 看每个微批如何替换流式节点并复用第 12 章执行链路
7. **sql/** - 回看 DataFrame、Catalyst 和物理计划如何被复用

## 运行

```bash
mvn -pl ch14-structured-streaming package
java -Dfile.encoding=UTF-8 -cp ch14-structured-streaming/target/classes com.sparklearn.streaming.structured.Main
```

## 运行测试

```bash
mvn -pl ch14-structured-streaming test
```
