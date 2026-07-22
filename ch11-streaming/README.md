# 第 11 章 · Spark Streaming 与 DStream

本章在第 10 章的执行内核之上，实现一个 DStream 微批调度层。

包结构按职责分成两组：

```text
com.sparklearn.core.*                 # RDD / Stage / Shuffle / Task 执行内核
com.sparklearn.streaming.*            # StreamingContext / DStream / 时间与调度
com.sparklearn.streaming.dstream.*    # 具体 DStream 实现（map/window/queue...）
```

核心内容：

- `DStream` = 连续时间上的一串 RDD
- `StreamingContext` 按 batch 间隔推进逻辑时间
- 每个 batch 把输出操作变成普通 Spark job
- `queueStream` 输入 + `map/filter/flatMap/reduceByKey`
- `window`：把最近若干 batch 的 RDD 并起来再算

运行：

```bash
mvn -pl ch11-streaming package
java -Dfile.encoding=UTF-8 -cp ch11-streaming/target/classes com.sparklearn.streaming.Main
```

运行测试：

```bash
mvn -pl ch11-streaming test
```
