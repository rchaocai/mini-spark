---
title: "第 9 章 · 从单机到分布式执行"
weight: 1
date: 2026-07-17
tags: ["Socket", "RPC", "序列化", "Executor", "Task", "preferredLocations", "数据本地性"]
summary: "把第 8 章的本地线程池调度升级成 Socket Executor：Task、RDD 血缘和用户闭包都必须可序列化，Driver 才能把任务发到另一个 JVM 执行，并由此看见网络和序列化的真实开销。"
---

# 第 9 章 · 从单机到分布式执行

> 💻 本章完整代码：[GitHub 查看](https://github.com/rchaocai/mini-spark/tree/main/ch09-network-rpc)
>
> 构建运行：`mvn -pl ch09-network-rpc package`
>
> 先启动 Executor：`java -Dfile.encoding=UTF-8 -cp ch09-network-rpc/target/classes com.sparklearn.Executor 9091`
>
> 再启动 Driver：`java -Dfile.encoding=UTF-8 -cp ch09-network-rpc/target/classes com.sparklearn.Main network localhost:9091`

第 8 章已经能在 Task 失败后重试，也能在 shuffle 文件丢失后重算对应的 Map 分区。

但它始终有一个前提：Driver、TaskScheduler、Task、RDD、shuffle 文件，都在同一个 JVM 里。

第 5 章我们说“把 Task 交给线程池”。这句话在代码里就是：

```java
executor.submit(task);
```

这行代码看起来很像分布式调度。Task 被放进队列，由另一个工作线程执行，结果通过 `Future` 回来。

但线程池不是分布式。

线程之间共享同一块堆内存。`submit(task)` 交出去的不是一份 Task，而是一个对象引用。工作线程拿到这个引用，就能继续访问 Task 里的 `rdd`、`partition`、用户函数、依赖链。所有东西本来就在同一个 JVM 里。

现在把执行端换成另一个 JVM。引用就没用了。一个内存地址在 Driver 进程里有意义，到了 Executor 进程里只是一个数字。

所以第 9 章只做一件事：

```mermaid
sequenceDiagram
    participant D as Driver / DAGScheduler
    participant S as NetworkTaskScheduler
    participant W as Executor JVM

    D->>S: submitTasks(List<Task>)
    S->>W: writeObject(RemoteTaskRequest)
    W->>W: readObject() 重建 Task
    W->>W: task.run(attemptId)
    W-->>S: writeObject(RemoteTaskResult)
    S-->>D: 返回分区结果
```

到了这里，Task 就不能再只是内存里的一个对象了。Driver 必须先把它写成字节，Executor 才能收到；Executor 再把这些字节还原成 Task，继续执行。第 9 章后面的所有变化，都从这里开始。

## 9.1 DAGScheduler 需要知道网络吗？

先回忆第 8 章的分工。

`DAGScheduler` 沿着 RDD 血缘划分 Stage，创建 `ShuffleMapTask` 和 `ResultTask`。Task 创建好以后，再交给 `TaskScheduler` 放进线程池。

现在只是把“在线程池里执行”换成“发给 Executor 执行”。那么，应该修改谁？

最直接的做法，是在 `DAGScheduler` 里连接 Socket、发送 Task、等待结果。但仔细想一想，`DAGScheduler` 的工作其实没有变：

```text
划分 Stage
  -> 创建一批 Task
  -> 提交 Task
  -> 等待结果
```

至于 Task 是进入本地线程池，还是穿过 Socket 到另一个 JVM，并不是 `DAGScheduler` 需要关心的事。

因此，本章让 `DAGScheduler` 只依赖 `TaskScheduler` 这一个入口：

```java
public interface TaskScheduler extends AutoCloseable {

    <T> List<T> submitTasks(List<? extends Task<T>> tasks);

    @Override
    void close();
}
```

这个接口只表达一件事：给我一批 Task，我负责执行，再把结果交回来。

第 8 章的线程池实现改名为 `LocalTaskScheduler`：

```java
public final class LocalTaskScheduler implements TaskScheduler {
    ...
}
```

第 9 章再增加 `NetworkTaskScheduler`：

```java
public final class NetworkTaskScheduler implements TaskScheduler {
    ...
}
```

```mermaid
flowchart LR
    D["DAGScheduler<br/>创建 Task"]
    S["TaskScheduler<br/>提交 Task"]
    L["LocalTaskScheduler<br/>本地线程池"]
    N["NetworkTaskScheduler<br/>Socket Executor"]

    D --> S
    S --> L
    S --> N
```

这样一来，`DAGScheduler` 的代码几乎不用变化。变化被留在了 Task 的执行方式里。

这一节只做了两个整理：把“要执行的东西”统一叫 `Task`，把“怎么执行 Task”交给 `TaskScheduler`。前者让 `ResultTask` 和 `ShuffleMapTask` 有同一个入口，后者让本地线程池和 Socket Executor 可以替换。

## 9.2 Task 不是引用了，是字节流

上一节把入口统一成了 `TaskScheduler.submitTasks(...)`。现在看同一个入口在两个实现里分别做了什么。

在线程池版 `LocalTaskScheduler` 里，提交 Task 的关键代码是：

```java
private <T> Future<T> submitTask(Task<T> task, int attemptId) {
    return executor.submit(() -> task.run(attemptId));
}
```

`executor.submit(...)` 做的是同 JVM 内的对象传递。`task` 这个变量里保存的是一个引用，线程池把这个引用交给工作线程。工作线程拿到引用后，直接调用 `task.run(attemptId)`。

到了网络版，`submitTasks(...)` 会走到 `NetworkTaskScheduler.sendTask(...)`。这里不再有共享堆内存，Executor 拿不到 Driver 里的对象引用，所以发送端必须把 Task 写进 Socket：

```java
try (Socket socket = new Socket(host, port);
     ObjectOutputStream out = new ObjectOutputStream(
             new BufferedOutputStream(socket.getOutputStream()))) {
    out.flush();
    ObjectInputStream in = new ObjectInputStream(
            new BufferedInputStream(socket.getInputStream()));

    out.writeObject(new RemoteTaskRequest<>(task, attemptId));
    out.flush();

    RemoteTaskResult<T> response =
            (RemoteTaskResult<T>) in.readObject();
    return response.value();
}
```

这段代码在做两件事。

第一，发送请求：

```java
out.writeObject(new RemoteTaskRequest<>(task, attemptId));
out.flush();
```

`RemoteTaskRequest` 里装着 Task 和本次尝试编号。`writeObject(...)` 会从这个请求对象开始，把它引用到的对象一起序列化。

第二，等待结果：

```java
RemoteTaskResult<T> response =
        (RemoteTaskResult<T>) in.readObject();
return response.value();
```

Executor 执行完 Task 后，把结果也序列化回来。Driver 再用 `readObject()` 把结果还原。

所以，任务入口没有变，传递方式变了：

```text
线程池版：把 task 引用放进 BlockingQueue
网络版：  把 task 序列化后写进 Socket
```

差别就在“引用”和“字节流”之间。

引用只在同一个 JVM 里有意义。字节流可以穿过 Socket，到另一个 JVM 里重新变成对象。

> [!INFO]
> **序列化发送的是对象状态，不是 class 文件**
>
> `writeObject(...)` 会把对象字段里的状态写出去，比如 `ResultTask` 里的 `rdd`、`partition`、用户函数。
>
> 它不会把 `ResultTask.class`、`ListRDD.class` 这些类定义一起发过去。Executor JVM 必须已经在 classpath 里加载得到同一套代码；否则 `readObject()` 找不到类，Task 还没开始运行就会失败。

所以本章先抽出一个可序列化的 `Task`，再让 `ResultTask` 和 `ShuffleMapTask` 继承它。但真正被发送的不是这个外壳本身，而是具体的子类对象。以 `ResultTask` 为例：

```java
public abstract class Task<T> implements Serializable {
    public final T run(int attemptId) {
        return runTask(new TaskContext(stageId, partition, attemptId));
    }
    protected abstract T runTask(TaskContext context);
}

public final class ResultTask<T, U> extends Task<U> {
    private final RDD<T> rdd;
    private final Partition partition;
    private final SerializableFunction<Iterator<T>, U> partitionFunction;

    @Override
    protected U runTask(TaskContext context) {
        return partitionFunction.apply(rdd.iterator(partition));
    }
}
```

`TaskContext` 不参与 `WordCount` 的计算结果。它记录的是这次运行的身份，供日志和错误处理使用。

比如同一个结果分区第一次运行失败，调度器会再发起一次运行。第 8 章只是再次调用同一个 `call()`，Task 内部看不出“这是重试”。现在 `run(attemptId)` 会把尝试编号带进去，Task 打日志或上报错误时，就能说清楚自己是哪一次运行：

```text
ResultTask(stage=1, partition=0, attempt=0) 失败
ResultTask(stage=1, partition=0, attempt=1) 重试成功
```

现在再回到 `writeObject(new RemoteTaskRequest<>(task, attemptId))`。这里的 `task` 如果是一个 `ResultTask`，Java 序列化不会只写 `ResultTask` 自己。它会沿着 `ResultTask` 的字段引用继续展开。

先看 `ResultTask` 自己的三个关键字段：

```java
private final RDD<T> rdd;
private final Partition partition;
private final SerializableFunction<Iterator<T>, U> partitionFunction;
```

所以，发送一个 `ResultTask` 时，至少要一起发送三类东西：

1. `rdd`：这个分区从哪条 RDD 血缘算出来。
2. `partition`：这次要算哪个分区。
3. `partitionFunction`：算完这个分区后，action 要怎样把迭代器变成结果。

难点在 `rdd`。它通常不是一个孤立对象，而是一串 RDD 血缘。比如：

```java
sc.parallelize(words, 3)
        .map(...)
        .filter(...)
        .collect();
```

这个 `collect()` 生成的 `ResultTask`，会指向最后一个 RDD；最后一个 RDD 又会指向它的父 RDD，父 RDD 再指向更早的父 RDD。每个变换 RDD 还会保存自己的用户函数。

```mermaid
flowchart TB
    T["ResultTask"]
    R2["MapPartitionsRDD<br/>保存 filter 函数"]
    R1["MapPartitionsRDD<br/>保存 map 函数"]
    R0["ListRDD<br/>保存源数据或分区描述"]
    P["Partition<br/>分区编号"]
    A["partitionFunction<br/>collect 的分区函数"]
    D2["Dependency<br/>R2 依赖 R1"]
    D1["Dependency<br/>R1 依赖 R0"]

    T -->|"字段 rdd"| R2
    T -->|"字段 partition"| P
    T -->|"字段 partitionFunction"| A
    R2 -->|"父 RDD"| R1
    R2 -->|"依赖信息"| D2
    R1 -->|"父 RDD"| R0
    R1 -->|"依赖信息"| D1
```

这张图里，从 `ResultTask` 出发，沿字段引用能到达的所有对象，合起来就叫**对象图**。它不一定是严格的树形结构；只要某个对象能被这些引用一路找到，序列化就会把它纳入检查范围。

Java 序列化会从 `task` 这个根对象出发，沿对象图把可达对象都检查一遍。中间任何一个对象不能序列化，整个 Task 就发不出去。

这里就出现了一个很具体的问题：用户传进来的函数怎么办？

第 8 章还在同一个 JVM 里运行。`map` 只需要一个普通 `Function`：

```java
Function<T, U> elementFunction
```

这句话的意思很简单：给我一个 `T`，我能算出一个 `U`。至于这个函数能不能写进 Socket，第 8 章不关心。

到了第 9 章，函数会被放进 `MapPartitionsRDD`，再跟着 `ResultTask` 一起发到 Executor。于是接口只说“我能计算”就不够了，还要多说一句“我能序列化”。

普通 `Function` 的承诺只有一个：

```text
Function
  -> 能 apply(...)
```

本章需要的是两个承诺叠在一起：

```text
SerializableFunction
  -> 能 apply(...)
  -> 能 Serializable
```

所以我们新增了 `SerializableFunction`：

```java
@FunctionalInterface
public interface SerializableFunction<T, R>
        extends Function<T, R>, Serializable {
}
```

它没有发明新的计算能力，只是把“这个函数可以被写成字节”放进类型里。这样 `map` 的签名就从第 8 章的：

```java
public <U> MapPartitionsRDD<T, U> map(
        Function<T, U> elementFunction) {
    ...
}
```

变成第 9 章的：

```java
public <U> MapPartitionsRDD<T, U> map(
        SerializableFunction<T, U> elementFunction) {
    ...
}
```

这不是 Java 语法上的小改动，而是模型契约的升级：

```text
第 8 章：这个函数能在另一个线程里调用
第 9 章：这个函数能被复制到另一个 JVM 里调用
```

`filter` 和 `reduceByKey` 也是同一个道理。它们在本地线程池里只需要“能判断”“能合并”；跨 JVM 后，还要能跟着 Task 过网络。所以本章同样补了 `SerializablePredicate` 和 `SerializableBinaryOperator`。

## 9.3 一个 lambda，能不能过网络

先看一个容易踩坑的例子。

下面这段代码在第 8 章没问题：

```java
rdd.map(Function.identity())
```

到第 9 章会编译失败。

原因不是 `identity()` 做不了恒等映射，而是它返回的是普通 `Function`。普通 `Function` 没有承诺自己可序列化。

所以第 9 章的代码改成：

```java
rdd.map(value -> value)
```

此时 `map` 方法的参数类型是 `SerializableFunction`，编译器就会把这个 lambda 编译成一个可序列化的函数对象。

`reduceByKey` 也一样。本章不用 `Integer::sum`，而写成：

```java
reduceByKey((left, right) -> left + right, 2)
```

看起来啰嗦一点，但目标类型很清楚：这是一个 `SerializableBinaryOperator<Integer>`。

这里有一个 Java 初学者很容易跳过的点：lambda 能不能序列化，不只看 lambda 里面写了什么，还看它被赋给了什么类型。同样的 `(x -> x)`，赋给 `Function` 就只是普通函数，赋给 `SerializableFunction` 才是可序列化函数。

分布式计算里很多“奇怪的 API 约束”，背后都是这个原因。你以为只是写一个函数，系统看到的是：这个函数要不要被打包，能不能过网络，到了远端 JVM 还能不能重新加载。

还要再补一刀：lambda 自己可序列化，不代表它捕获的东西也可序列化。

```java
Object notSerializable = new Object();
rdd.map(value -> value + notSerializable.toString());
```

这段代码的目标类型仍然是 `SerializableFunction`，所以 lambda 本身愿意被序列化。但它里面用到了外面的 `notSerializable`，这个对象也会被放进刚才那张对象图。

问题在这里：`new Object()` 只是一个普通 Java 对象，它没有实现 `Serializable`。Java 序列化走到它时，就不知道该怎样把它写成字节，于是抛 `NotSerializableException`。

如果确实要把某个小配置带到 Executor，就让这个配置对象自己可序列化：

```java
record Prefix(String value) implements Serializable {
}

Prefix prefix = new Prefix("word=");
rdd.map(value -> prefix.value() + value);
```

这种对象只有普通字段，写成字节、传到 Executor、再还原回来，都说得通。

但有些东西不应该放进闭包里，比如 `FileInputStream`、数据库连接、线程池。它们代表的是 Driver 进程里的某个运行时资源，不是一份稳定的数据。即使强行让类型实现了 `Serializable`，传到另一个 JVM 后通常也没有原来的含义。更稳妥的做法，是在 Executor 侧按需创建这些资源，或者只把连接参数、文件路径这类小配置传过去。

> [!INFO]
> **真实 Spark 怎么处理闭包**
>
> 真实 Spark 也要处理同一个问题。用户写的 `map`、`filter`、`foreach` 这类函数，会和 Task 一起形成闭包。提交 Task 前，Spark 会分析这个闭包，把 Executor 运行时真正需要的变量整理出来，再交给后面的序列化层发送到 Executor。
>
> Spark 里有 `ClosureCleaner` / `SparkClosureCleaner` 这类组件做这件事。比如一个 lambda 只用到了外部对象里的一个普通字符串，但编译后的闭包可能顺手带上了整个外部对象。Cleaner 会尽量剪掉这些没用到的引用，避免一个本来只需要带字符串的 Task，因为多带了外部对象而序列化失败。
>
> 但它不是魔法。如果你的函数明确引用了一个不能序列化的对象，比如打开的文件流、数据库连接，Spark 仍然可能报 `Task not serializable`。所以真实 Spark 的规则和本章一样：闭包里真正要带到 Executor 的东西，必须能被序列化；运行时资源不要直接捕获。

跨 JVM 后还有一个更隐蔽的变化：闭包状态会变成**副本**。第 8 章的 `FaultyRDD` 用 `AtomicInteger` 共享剩余失败次数，在线程池里没问题，因为所有线程看到的是同一个对象；网络版发送 Task 时，Executor 改的是反序列化后的副本，Driver 侧原对象不会跟着变。

```mermaid
sequenceDiagram
    participant D as Driver
    participant W as Executor JVM

    Note over D: remainingFailures = 1
    D->>W: 序列化 Task 和 FaultyRDD
    Note over W: 副本 remainingFailures = 1
    W->>W: 减到 0 后抛出异常
    W-->>D: 返回失败
    Note over D: Driver 里的 remainingFailures 仍然是 1
    D->>W: 重试时再次发送 remainingFailures = 1
```

也就是说，本章保留第 8 章容错代码作为基线，但不要用 `FaultyRDD` 去演示网络重试。跨 JVM 的故障计数要放进 `attemptId`、Driver 事件或外部状态里，而不是放进会被复制的闭包对象里。

## 9.4 Executor：另一个 JVM 里的 run()

Executor 这边要做的事，可以先看成一个完整的请求处理函数：

```java
private void handle(Socket client) throws IOException {
    try (Socket socket = client;
         ObjectOutputStream out = new ObjectOutputStream(
                 new BufferedOutputStream(socket.getOutputStream()))) {
        out.flush();
        ObjectInputStream in = new ObjectInputStream(
                new BufferedInputStream(socket.getInputStream()));

        RemoteTaskRequest<?> request =
                (RemoteTaskRequest<?>) in.readObject();

        try {
            Object value = request.task().run(request.attemptId());
            out.writeObject(RemoteTaskResult.success(value));
        } catch (Throwable e) {
            out.writeObject(RemoteTaskResult.failure(e));
        }
        out.flush();
    } catch (ClassNotFoundException e) {
        throw new IOException("Task 反序列化失败", e);
    }
}
```

外层的服务器循环只负责接连接：

```java
try (ServerSocket socket = new ServerSocket(port)) {
    while (running) {
        handle(socket.accept());
    }
}
```

`accept()` 会阻塞等待 Driver 连接。连接来了，就交给 `handle(...)`。这段 `handle(...)` 只做四步：

```text
读 RemoteTaskRequest
执行 request.task().run(...)
成功时写 RemoteTaskResult.success(...)
失败时写 RemoteTaskResult.failure(...)
```

这里的重点不是 `ObjectInputStream` 这几个类名，而是职责边界：Executor 不是在“理解 RDD”。它只是在反序列化一个请求，执行请求里的 Task，再把响应写回去。

最关键的是第二步：

```java
Object value = request.task().run(request.attemptId());
```

这里没有反射，也没有“根据字符串方法名去找代码”。`readObject()` 之后，`request.task()` 已经是一个正常的 Java 对象了。它的真实类型可能是 `ResultTask`，也可能是 `ShuffleMapTask`。

`Task.run(...)` 本身是父类里的固定入口：

```java
public final T run(int attemptId) {
    return runTask(new TaskContext(stageId, partition, attemptId));
}
```

真正不同的地方在 `runTask(...)`。如果反序列化出来的是 `ResultTask`，Java 的普通虚方法分派会调用 `ResultTask.runTask(...)`；如果是 `ShuffleMapTask`，就调用 `ShuffleMapTask.runTask(...)`。

```mermaid
flowchart TB
    A["Executor<br/>request.task().run(attemptId)"]
    B["Task.run(...)<br/>创建 TaskContext"]
    C{"真实 Task 类型"}
    D["ResultTask.runTask(...)<br/>rdd.iterator(partition)<br/>partitionFunction.apply(...)"]
    E["ShuffleMapTask.runTask(...)<br/>rdd.iterator(partition)<br/>写 shuffle 文件"]

    A --> B --> C
    C --> D
    C --> E
```

所以，远程执行并不是 Executor 临时“拼出一段代码”再运行。代码的 class 必须已经在 Executor 的 classpath 里；网络上传过来的是对象状态。对象还原以后，Executor 像本地线程池一样调用 `run()`，只是这个对象来自 Socket。

> [!INFO]
> **为什么这里不需要反射**
>
> 反射解决的是另一类问题：运行时才知道类名、方法名，然后用字符串去找类、找方法、再调用。比如一个通用 RPC 框架收到 `"method": "add"`，它可能需要用反射找到 `add(...)` 方法。
>
> 本章不需要这样做，因为协议里传来的不是“类名 + 方法名”，而是已经反序列化好的 `Task` 对象。Executor 只调用固定入口 `task.run(...)`，至于它实际走 `ResultTask.runTask(...)` 还是 `ShuffleMapTask.runTask(...)`，交给 Java 普通多态完成。
>
> 简单说：反射是“拿名字找代码”，多态是“对象自己知道该执行哪段代码”。本章用的是后者。

如果 `run()` 抛出异常，Executor 不会让连接直接断掉，而是把失败也包装成 `RemoteTaskResult.failure(e)`。Driver 读到响应后再判断：

```java
if (!response.success()) {
    throw new IllegalStateException(..., response.error());
}
```

这样，远端 Task 失败会重新变成 Driver 侧能处理的异常。第 8 章的 `FetchFailedException` 也因此可以从 Executor 回到 Driver，再触发对应的 shuffle map 重算。

这里同样有序列化边界：`RemoteTaskResult.failure(e)` 会把异常对象写回 Driver。异常类和异常里携带的字段也要能被 Driver 反序列化。完整系统通常会把远端失败整理成更稳定的错误信息，而不是直接传任意 `Throwable`。

注意这一点：Executor 的协议入口没有认识 `RDD`，也没有认识 `DAGScheduler`。它只认识 `Task`。但 Task 内部仍然携带 RDD 血缘，真正执行 `run()` 时，还是会调用 `rdd.iterator(partition)`。

因为 Stage 切分、Task 创建已经在 Driver 里完成了。Executor 只负责执行收到的请求。

这就是本章想让你亲手摸到的“RPC”本质。RPC 框架可以更复杂，可以复用连接，可以异步，可以压缩，可以做心跳和失败探测。但在这些能力出现之前，先要有这条请求和响应路径。

> [!INFO]
> **真实 Spark 的序列化和 RPC**
>
> 这里用 `ObjectOutputStream` 把对象写进 `Socket`，是最小的一条链路。
>
> 真实 Spark 把这件事拆成两层。第一层是序列化：`JavaSerializer` 使用 Java 自带对象序列化，`KryoSerializer` 使用开源库 Kryo。Kryo 通常比 Java 序列化更快、结果也更紧凑，但为了更好性能，常常需要提前注册自定义类。
>
> 第二层是网络通信：RPC 由 `NettyRpcEnv` 这类组件承接，底层基于开源网络框架 Netty。
>
> 换句话说，Kryo 负责把对象变成更紧凑的字节，Netty 负责把这些字节高效地送过网络。本章把这两层都压缩成了 `ObjectOutputStream + Socket`，是为了先看清最小闭环。
>
> 名字变复杂了，本质没有变：Driver 先把“要做什么”变成字节，发给远端；Executor 反序列化、执行，再把结果或失败写回 Driver。
>
> 但本章只实现“Task 怎么过网络”。`reduceByKey` 的 shuffle 文件仍沿用第 8 章的本地文件模型。真正跨机器时，Reduce 端不能靠同一个 `File` 路径读取 Map 端输出，必须引入 `MapOutputTracker` / BlockManager 这类组件。这个边界会在 9.8 展开。

## 9.5 为什么 RDD 里的 SparkContext 要 transient

本章的 `RDD` 是这样声明的：

```java
public abstract class RDD<T> implements Serializable {

    private final transient SparkContext sparkContext;
    ...
}
```

`transient` 的意思是：序列化时跳过这个字段。

为什么要跳过 `SparkContext`？

因为 `SparkContext` 是 Driver 侧入口，里面有调度器。把 RDD 发给 Executor，是为了让 Executor 计算这个分区，不是为了把 Driver 的调度器、线程池、网络调度器也复制一份过去。

本章沿用这个边界：RDD 本身可序列化，但 `SparkContext` 是 transient。

这又是一个“分布式边界”的例子。单机时对象之间互相引用没什么问题；跨进程后，你必须非常清楚哪些东西是计算描述的一部分，哪些东西只属于 Driver。

RDD 血缘、分区、依赖、用户闭包，属于计算描述，要发。

SparkContext、调度器、线程池，属于 Driver 控制面，不发。

## 9.6 preferredLocations：把 Task 发到数据那里

现在 Task 已经能发到另一个 JVM 了。接下来会出现一个很自然的问题：

```text
如果有多个 Executor，这个 Task 应该发给谁？
```

最朴素的答案是轮询：第一个 Task 发给 Executor-1，第二个发给 Executor-2，依次循环。这样可以跑，但不够好。

因为分布式计算里，数据常常已经在某台机器上。比如分区 0 的文件块在 Executor-1 附近，分区 1 的文件块在 Executor-2 附近。此时把分区 0 的 Task 发给 Executor-1，就只需要移动一小段计算描述；如果发给 Executor-2，可能就要跨网络搬一大块数据。

所以第 4 章先按下不表的 `preferredLocations`，到这里终于有了用途：它回答的是“这个分区更适合在哪些 Executor 上计算”。

```mermaid
flowchart LR
    L["ListRDD<br/>分区 0 偏好 Executor-1"]
    M["MapPartitionsRDD<br/>沿用父分区位置"]
    T["ResultTask<br/>携带最终 RDD + 分区号"]
    S["NetworkTaskScheduler<br/>选择 Executor"]
    E1["Executor-1"]
    E2["Executor-2"]

    L --> M --> T --> S
    S -->|"命中 preferredLocations"| E1
    S -.没有命中时轮询.-> E2
```

先看源头。`RDD` 增加一个默认方法：

```java
public List<String> preferredLocations(Partition partition) {
    return List.of();
}
```

默认返回空列表，意思是“我没有位置偏好”。源头 RDD 如果知道数据在哪，就可以覆盖它。本章的 `ListRDD` 是内存演示 RDD，但为了把接口形状做出来，也允许每个分区带一组位置偏好：

```java
public ListRDD(
        SparkContext sparkContext,
        List<T> data,
        int numberOfPartitions,
        List<List<String>> preferredLocations) {
    ...
}
```

比如 3 个分区可以这样描述：

```text
partition 0 -> localhost:9091
partition 1 -> localhost:9092
partition 2 -> 没有偏好
```

真实系统里的位置通常来自 HDFS block、本地文件分片、缓存 block 或 shuffle block；本章先用字符串地址模拟这个信息。

再看变换 RDD。`map`、`filter` 这类窄依赖不会改变分区对应关系：子分区 0 仍然读取父分区 0，子分区 1 仍然读取父分区 1。所以 `MapPartitionsRDD` 不需要自己发明位置，只要把父分区的位置传下来：

```java
@Override
public List<String> preferredLocations(Partition partition) {
    return parent.preferredLocations(partition);
}
```

然后看 Task。`ResultTask` 持有最终 RDD 和当前分区，所以它可以把这个问题转交给 RDD：

```java
@Override
public List<String> preferredLocations() {
    return rdd.preferredLocations(partition);
}
```

这样，位置偏好就从源头 RDD，一路经过窄依赖 RDD，传到了 Task。

最后才轮到调度器选择 Executor。这里如果只看选择 Executor 的几行代码，会有两个问题：`task` 是从哪里来的？`taskIndex` 又是谁传进来的？

先从外层开始看。`NetworkTaskScheduler` 收到的不是一个 Task，而是一批 Task。`submitTasks(...)` 用第一层循环依次取出它们：

```java
public <T> List<T> submitTasks(List<? extends Task<T>> tasks) {
    List<T> result = new ArrayList<>();

    for (int index = 0; index < tasks.size(); index++) {
        Task<T> task = tasks.get(index);
        result.add(sendWithRetry(task, index));
    }

    return result;
}
```

这里的 `index` 就是后面看到的 `taskIndex`。它表示当前 Task 在这批任务中的位置：第一个是 0，第二个是 1，第三个是 2。

`sendWithRetry(task, index)` 准备发送 Task 时，会调用：

```java
String executorAddress = executorFor(task, taskIndex);
```

现在再进入 `executorFor(...)`，看第二层循环：

```java
private String executorFor(Task<?> task, int taskIndex) {
    for (String location : task.preferredLocations()) {
        if (executorAddresses.contains(location)) {
            return location;
        }
    }

    return executorAddresses.get(
            taskIndex % executorAddresses.size());
}
```

两层循环遍历的对象不同：

```text
第一层循环：遍历这一批 Task
  -> 取出当前 task
  -> 把 task 和它的 taskIndex 交给 sendWithRetry(...)

第二层循环：遍历当前 Task 的位置偏好
  -> 如果某个偏好地址也在可用 Executor 列表中，立即选中它
  -> 如果所有偏好地址都不可用，或者 Task 根本没有位置偏好，就退回轮询
```

轮询发生在最后一行。假设有两个 Executor，Task 的位置偏好又没有命中：

```text
taskIndex = 0 -> executorAddresses.get(0 % 2) -> Executor-0
taskIndex = 1 -> executorAddresses.get(1 % 2) -> Executor-1
taskIndex = 2 -> executorAddresses.get(2 % 2) -> Executor-0
```

因此，这段调度逻辑的顺序是：**先尝试把 Task 送到数据附近；做不到时，再用 `taskIndex` 轮询 Executor。**

如果数据在 Executor-1，把 Task 发给 Executor-1，网络上传的是 Task：闭包、RDD 血缘、分区号。通常是几 KB 到几 MB。

如果把 Task 发给 Executor-2，而数据在 Executor-1，就要先把数据从 Executor-1 搬到 Executor-2。一个分区可能是几十 MB、几百 MB，甚至更大。

所以分布式计算的经验法则不是“找一台空闲机器就跑”，而是：

```text
能移动计算，就不要移动数据。
```

Task 是说明书，数据是货物。能寄说明书，就不要搬货物。

> [!INFO]
> **数据为什么会在某个 Executor 附近？**
>
> 经典 Spark 集群常常和 HDFS 放在一起使用。HDFS 会把大文件切成 block，分散存到多台机器的 DataNode 上；Spark 的 Executor 也运行在这些机器上。于是某个输入分区可能正好对应机器 A 上的一个 HDFS block，把 Task 派到机器 A 的 Executor，就能尽量从本地磁盘读取，少走网络。
>
> 数据位置不只来自 HDFS。缓存后的 RDD 分区会留在某个 Executor 的内存或磁盘里；shuffle map 输出也会先落在某个 Executor 本地。调度器如果知道这些位置，就可以优先把后续 Task 派到数据附近。
>
> 本章的 `ListRDD` 还只是内存演示 RDD。它把完整 `List` 保存在对象字段里，所以网络发送 `Task` 时，`ResultTask -> RDD -> ListRDD -> data` 这条引用链会被一起序列化。这里的 `preferredLocations` 先用字符串地址模拟真实系统里的数据位置；真正的大数据系统里，Task 通常带的是“这个分区怎么读”的描述，而不是把几百 MB 数据本体塞进 Task。
>
> 为什么有时要问父 RDD？因为最后提交的 Task 常常对应的是变换后的 RDD。比如 `textFile(...).map(...).filter(...)`，最后的 `filter` RDD 自己没有文件块位置；它要问父 RDD，父 RDD 如果也只是一次 `map`，还要继续往前问。一直问到源头 RDD，才可能知道“分区 0 对应的 HDFS block 在机器 A 附近”。
>
> 本章的 `MapPartitionsRDD` 也是这个思路：它自己不保存数据位置，只把 `preferredLocations(partition)` 转交给父 RDD。这样一层层传下去，最后 Task 就能拿到源头分区的位置偏好。

所以本章先兑现调度接口和位置偏好的形状；真正“不搬数据，只发计算”的前提，是源头 RDD 不能把大数据本体直接放进要序列化的对象图里。

## 9.7 网络到底贵在哪里

线程池版提交一个 Task 时，Task 没有离开当前 JVM。`LocalTaskScheduler` 只是把这个 Task 对象的引用交给线程池：

```text
把引用放进队列
工作线程取出引用
调用 run()
```

这几步是执行路径，不全是额外成本。尤其是 `run()`，它是 Task 本来就要做的计算；不管用线程池、Socket Executor，还是直接在当前线程调用，最终都要执行它。

线程池真正额外增加的，是为了把 Task 交给另一个线程而付出的协调开销：

```text
提交线程和工作线程通过并发队列交接 Task
工作线程可能发生的唤醒与操作系统调度
通过 Future 保存结果、通知等待线程
```

这些操作需要原子指令、锁或线程调度，但 Task 对象仍在同一个 JVM 中。传递的是引用，不需要把整个 Task 序列化，也不需要经过 Socket。

网络版提交 Task 时，边界变了。Executor 在另一个 JVM 里，引用不能跨进程使用。Driver 必须把 Task 变成字节，Executor 再把字节还原成对象：

```mermaid
sequenceDiagram
    participant D as Driver
    participant W as Executor

    Note over D: 序列化 Task 对象图
    D->>W: 建立 Socket + 发送字节
    Note over W: 反序列化 Task
    W->>W: run()
    Note over W: 序列化结果
    W-->>D: 发送结果字节
    Note over D: 反序列化结果
```

其中 `run()` 仍然是 Task 本身的计算。网络执行额外增加的是 Task 和结果的序列化、字节复制、Socket 系统调用、网络协议处理、反序列化以及等待响应的时间。

因此，线程池和网络版都会执行同一个 `run()`。差别不在“算什么”，而在“为了让另一个执行单元开始算，需要怎样交付 Task，又要付出多少交付成本”。

本章的 `NetworkTaskScheduler` 还是同步 RPC。它会发送一个 Task，等结果回来，再发送下一个 Task。第 8 章的 `LocalTaskScheduler` 会先把一批 Task 都提交到线程池，因此保留了分区并行；第 9 章的网络版先只实现跨 JVM 执行这条主线。

```mermaid
sequenceDiagram
    participant D as Driver
    participant L as LocalTaskScheduler
    participant N as NetworkTaskScheduler
    participant W as Executor

    D->>L: submitTasks(task0, task1, task2)
    par 本地并行
        L->>L: task0.run()
        L->>L: task1.run()
        L->>L: task2.run()
    end

    D->>N: submitTasks(task0, task1, task2)
    N->>W: task0
    W-->>N: result0
    N->>W: task1
    W-->>N: result1
    N->>W: task2
    W-->>N: result2
```

生产系统不会这样串行跑远端分区。它会把一批 Task 分发给多个 Executor，并异步接收完成事件。要做到这一点，调度器还需要异步事件循环、连接复用、多个 Executor 的并发提交、任务完成事件队列和失败回调。本章暂时不实现这些机制，只保留最小网络路径：Driver 把 Task 序列化后发给 Executor，Executor 执行后把结果发回 Driver。

即使 Executor 跑在 `localhost`，这笔账也不会消失。数据仍然要经过 Socket、TCP 协议栈、内核缓冲区和用户态 / 内核态切换。`localhost` 只是没有经过物理网卡，不等于免费。

所以本章的 `Main` 保留了两个入口。

直接运行：

```bash
java -Dfile.encoding=UTF-8 \
  -cp ch09-network-rpc/target/classes \
  com.sparklearn.Main
```

走本地线程池。

先启动 Executor：

```bash
java -Dfile.encoding=UTF-8 \
  -cp ch09-network-rpc/target/classes \
  com.sparklearn.Executor 9091
```

再启动 Driver：

```bash
java -Dfile.encoding=UTF-8 \
  -cp ch09-network-rpc/target/classes \
  com.sparklearn.Main network localhost:9091
```

走 Socket Executor。

这次 `Main` 里的 WordCount 用 3 个输入分区、2 个 reduce 分区。网络版运行时，Driver 会按 Stage 依次发出这些 Task：

| 次序 | Task 类型 | 分区 | Executor 做什么 | 文件动作 |
| --- | --- | --- | --- | --- |
| 1 | `ShuffleMapTask` | map 0 | 读取输入分区 0，按 key 分到 2 个桶 | 写 `map_0_reduce_0`、`map_0_reduce_1` |
| 2 | `ShuffleMapTask` | map 1 | 读取输入分区 1，按 key 分到 2 个桶 | 写 `map_1_reduce_0`、`map_1_reduce_1` |
| 3 | `ShuffleMapTask` | map 2 | 读取输入分区 2，按 key 分到 2 个桶 | 写 `map_2_reduce_0`、`map_2_reduce_1` |
| 4 | `ResultTask` | reduce 0 | 读取所有 map 输出里属于 reduce 0 的文件，合并结果 | 读 `map_*_reduce_0` |
| 5 | `ResultTask` | reduce 1 | 读取所有 map 输出里属于 reduce 1 的文件，合并结果 | 读 `map_*_reduce_1` |

这张表也解释了为什么本章还能用第 8 章的 shuffle 文件模型跑通：Map 和 Reduce 都在同一台机器的 Executor JVM 上执行，它们看到的是同一个本地 shuffle 目录。

你会看到两边结果一致，但网络版会多出发送、接收、序列化和反序列化的开销。数据量越小，这些固定开销越显眼；数据量越大，位置选择和数据搬运成本越关键。

这也是为什么 Cache 会在下一章出现。网络这么贵，如果一个中间结果下次还要用，能不能别再跨网络重算？能不能把它留在某台 Executor 上，下次把 Task 派过去？

这就是 Cache 和数据本地性接上的地方。

## 9.8 shuffle 文件的边界

本章代码保留了第 8 章的 Stage 和 shuffle 文件模型，因此 `reduceByKey` 在网络版 demo 中能在本机 Executor 上跑通。

但这还不是完整的集群 shuffle。第 8 章的 shuffle 输出仍然是本地文件。Map Task 写出：

```text
map_0_reduce_0
map_0_reduce_1
...
```

Reduce Task 再按路径读取这些文件。

如果 Driver 和 Executor 只是同一台机器上的两个 JVM，它们可以看到同一个临时目录，demo 就能闭环。

如果真的换成两台机器，这个路径就不成立了。Executor-1 写出的本地文件，Executor-2 不能靠同一个 `File` 路径读到。完整系统需要 `MapOutputTracker` 记录每个 Map 输出在哪台 Executor 上，再让 Reduce 端通过网络去拉对应的数据。

```mermaid
flowchart TB
    subgraph T["当前实现：同机两个 JVM"]
        D1["Driver JVM"]
        W1["Executor JVM"]
        F1["同一个本地 shuffle 目录"]
        W1 -->|ShuffleMapTask 写文件| F1
        W1 -->|ResultTask 按 File 路径读| F1
        D1 -.提交 Task.-> W1
    end

    subgraph R["完整集群：多台机器"]
        E1["Executor-1"]
        E2["Executor-2"]
        M["MapOutputTracker"]
        S1["Executor-1<br/>BlockManager / Shuffle service"]
        B1["本地 shuffle block"]
        E1 -->|Map 输出位置注册| M
        E1 -->|写本地 block| S1
        S1 --> B1
        E2 -->|查询 block 位置| M
        E2 -->|通过网络拉取| S1
    end
```

`MapOutputTracker` 解决的正是这个问题：Map 端注册输出位置，Reduce 端查询这些位置。

当前代码不包含 `MapOutputTracker`。它属于 shuffle block 跨节点拉取层，而不是 Task 跨进程发送层。

当前网络实现覆盖范围如下：

```text
已实现：
Task / RDD 血缘 / 用户闭包跨 JVM 序列化
Socket Executor 执行 Task，包括 ShuffleMapTask 和 ResultTask
调度器按 preferredLocations 选择 Executor

实现边界：
shuffle 文件仍沿用第 8 章的本地文件模型
完整跨机器 shuffle 需要 MapOutputTracker / BlockManager
```

## 9.9 本章小结

第 9 章没有推翻前面的实现。它把第 8 章的“提交 Task”这一步接到了另一个出口。

但这一换，整个模型的物理含义变了：

```text
线程池：Task 是同 JVM 里的对象引用
网络：  Task 是跨 Socket 传输的字节流
```

为了让这件事成立，Task、RDD、Dependency、Partition、用户闭包、action 分区函数，都必须能序列化。

你也第一次真正兑现了“把代码发给数据”。这句话不再是比喻。用户函数、RDD 血缘和分区任务真的被打包成字节，发到 Executor JVM，在那里重新变成对象，再调用同一个 `run()`。

网络让分布式变成事实，也让代价变成事实。序列化、连接、传输、反序列化，全都要花时间。于是数据本地性不再是锦上添花，而是调度器必须考虑的基本问题。

接着可以思考一个问题：既然跨网络这么贵，算过一次的分区结果，能不能留在内存里，下一次直接复用？
