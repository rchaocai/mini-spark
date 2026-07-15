---
title: 第二部分 · Shuffle 与调度
url: /docs/part2/
weight: 3
bookCollapseSection: true
---

# 第二部分 · Shuffle 与调度

从单机到分布式，核心跨越是 **Shuffle 和调度**。这部分先让你亲手把中间结果落盘（Shuffle），再由"必须落盘"反推出 Stage 边界和 DAG，最后用不可变 + 血缘实现廉价容错。

这是全书最深的一部分——你将亲手触摸到 Spark 设计中最精妙的那层"为什么"。
