---
title: 第三部分 · 云端与未来
url: /docs/part3/
weight: 4
bookCollapseSection: true
---

# 第三部分 · 云端与未来

最后一部分把 mini-spark 从单机线程池推向真正的网络通信，再加上 Cache 与 Checkpoint 切断长血缘；然后在同一套 RDD 内核上长出 Streaming，再往上盖一层 DataFrame 和 Catalyst，最后把真实 Spark 源码和这套实现并排对照。

到这里，你会发现：这套实现和工业级 Spark 几乎一一对应。
