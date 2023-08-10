package org.apache.iotdb.db.storageengine.dataregion.compaction.tool.reader;

import java.util.concurrent.*;

public class AsyncThreadExecutor {
  private ExecutorService executor;

  public AsyncThreadExecutor(int threadCount) {
    executor = Executors.newFixedThreadPool(threadCount);
  }

  public Future<TaskSummary> submit(Callable<TaskSummary> task) {
    return executor.submit(task);
  }

  public void shutdown() {
    executor.shutdown();
  }
}
