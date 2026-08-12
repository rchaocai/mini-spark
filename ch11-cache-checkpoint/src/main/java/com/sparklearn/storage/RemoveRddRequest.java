package com.sparklearn.storage;

import java.io.Serializable;

/** Driver 通知 Executor 删除某个 RDD 的全部缓存块。 */
public record RemoveRddRequest(int rddId) implements Serializable {
}
