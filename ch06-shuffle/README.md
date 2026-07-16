# 第 6 章 · 亲手写一个 Shuffle

本章是逐章递进的 **mini-Spark 快照**模块，可独立编译运行。

## 运行

在仓库根目录执行：

```bash
mvn -q -pl ch06-shuffle package
java -Dfile.encoding=UTF-8 \
  -cp ch06-shuffle/target/classes \
  com.sparklearn.Main
```

## 对应正文

- `KeyValuePair.java`：键值对 record，作为 reduceByKey 的基本数据单元。
- `ShuffledRDD.java`：Map 端写本地文件，Reduce 端读文件合并。
- `Main.java`：演示 reduceByKey 的懒执行、中间文件落盘和删除文件后的失败。
