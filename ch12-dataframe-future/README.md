# 第 12 章 · 从 RDD 到 DataFrame

本章在第 11 章拆开的 RDD 内核之上，加一个教学版 Spark SQL / DataFrame 层。

目录刻意拆开：

```text
com.sparklearn.core.*                   # RDD / Stage / Shuffle / Task 执行内核
com.sparklearn.sql.*                    # SQLContext / DataFrame / Row / Schema
com.sparklearn.sql.catalyst.*           # 表达式、逻辑计划、规则优化器
com.sparklearn.sql.execution.*          # 物理计划，把逻辑计划落回 RDD
```

核心内容：

- `DataFrame` = 带 schema 的惰性逻辑计划
- Catalyst = 不可变树 + 规则批次 + fixed point
- 谓词下推：`Filter` 进入数据源扫描
- 列裁剪：只读后续真正用到的列
- `groupBy().count()` 最终落成 `RDD.reduceByKey()`，仍然由 DAGScheduler 切 Stage

运行：

```bash
mvn -pl ch12-dataframe-future package
java -Dfile.encoding=UTF-8 -cp ch12-dataframe-future/target/classes com.sparklearn.sql.Main
```

运行测试：

```bash
mvn -pl ch12-dataframe-future test
```
