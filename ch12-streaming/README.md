# 第 12 章 · Spark Streaming 与 DStream

本章在第 11 章的执行内核之上，实现一个 DStream 微批调度层。

包结构按职责分成两组：

```text
com.sparklearn.core.*                 # RDD / Stage / Shuffle / Task 执行内核
com.sparklearn.streaming.*            # StreamingContext / DStream / 时间与调度
com.sparklearn.streaming.dstream.*    # 具体 DStream 实现（map/window/queue...）
```

核心内容：

- `DStream`：连续时间上的一串 RDD，一个 `Time` 对应一个 batch RDD。
- `StreamingContext`：按 batch 间隔推进逻辑时间，并为每个输出流生成 `StreamingJob`。
- `DStreamGraph`：登记输入流和输出流，启动 receiver，并在每个 batch 生成 job。
- `queueStream`：用预先准备好的 RDD 队列模拟微批输入。
- `socketTextStream`：用 receiver 后台收集 socket 行，再在 batch 到点时排空成 RDD。
- `window`：把最近若干 batch 的 RDD 并成 `UnionRDD` 后继续计算。

运行：

```bash
mvn -pl ch11-streaming package
java -Dfile.encoding=UTF-8 -cp ch11-streaming/target/classes com.sparklearn.streaming.Main
```

运行测试：

```bash
mvn -pl ch11-streaming test
```

`Main` 会展示三条线：

```text
queueStream WordCount  -> 每个 batch 取一个 RDD，输出操作提交普通 RDD job
window 2s              -> 复用最近两个 batch 的 RDD，Union 后再 reduceByKey
socketTextStream       -> receiver 后台收集行；空 batch 不生成 job
```

看输出时重点盯住：

```text
=== batch @Time(...) jobs=N ===  逻辑时间推进到哪个 batch，本批生成了几个 job
Time: Time(...)                  output operation 对哪个 batch 的 RDD 执行
[window 2s] @Time(...)           窗口流把哪些历史 batch 合并后再算
```
