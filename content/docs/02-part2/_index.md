---
title: 第二部分 · 调度与 Shuffle
url: /docs/part2/
weight: 3
bookCollapseSection: true
---

# 第二部分 · 调度与 Shuffle

从单线程走向分布式，先要把分区交给 Task 并行执行；遇到跨分区计算时，再让中间数据通过 Shuffle 重新分布。沿着这条路，我们会继续得到 Stage、DAG 和基于血缘的容错。

这一部分会把前面搭好的 RDD 从“描述计算”推到“真正调度计算”。
