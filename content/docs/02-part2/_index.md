---
title: 第二部分 · 调度与 Shuffle
url: /docs/part2/
weight: 3
bookCollapseSection: true
---

# 第二部分 · 调度与 Shuffle

这一部分仍然运行在单个 JVM 中，但执行方式会从串行遍历逐步发展为按 Task 调度分区。遇到跨分区计算时，我们再让中间数据通过 Shuffle 重新分布，并由此引出 Stage、DAG 和基于血缘的容错。

这一部分会把前面搭好的 RDD 从“描述计算”推到“能够调度和恢复计算”。第三部分再把同一套 Task 边界扩展到 Worker 与网络通信。
