package com.sparklearn.streaming.dstream;

import java.io.InputStream;
import java.util.function.Consumer;

/**
 * 把字节流解释成一连串对象的方式：从 in 读，每得到一个对象就调 emit 发出去。
 * 给 SocketReceiver 用，决定 socket 的字节怎么变成一条条数据。
 */
@FunctionalInterface
public interface Deserializer<T> {
    void read(InputStream in, Consumer<T> emit) throws Exception;
}
