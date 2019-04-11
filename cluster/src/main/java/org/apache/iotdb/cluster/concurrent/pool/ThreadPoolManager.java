
package org.apache.iotdb.cluster.concurrent.pool;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.iotdb.db.exception.ProcessorException;

public abstract class ThreadPoolManager {

  ExecutorService pool;
  int threadCnt;

  private void checkInit() {
    if (pool == null) {
      init();
    }
  }

  /**
   * Init pool manager
   */
  public abstract void init();

  /**
   * Block new submits and exit when all RUNNING THREADS AND TASKS IN THE QUEUE end.
   *
   * @param block if set to true, this method will wait for timeOut milliseconds. false, return
   * directly. False, return directly.
   * @param timeOut block time out in milliseconds.
   * @throws ProcessorException if timeOut is reached or being interrupted while waiting to exit.
   */
  public void close(boolean block, long timeOut) throws ProcessorException {
    if (pool != null) {
      try {
        pool.shutdown();
        if (block) {
          try {
            if (!pool.awaitTermination(timeOut, TimeUnit.MILLISECONDS)) {
              throw new ProcessorException(
                  String
                      .format("%s thread pool doesn't exit after %d ms", getManagerName(),
                          timeOut));
            }
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new ProcessorException(
                String
                    .format("Interrupted while waiting %s thread pool to exit.", getManagerName()),
                e);
          }
        }
      } finally {
        pool = null;
      }
    }
  }

  /**
   * Name of Pool Manager
   */
  public abstract String getManagerName();

  public void execute(Runnable task) {
    checkInit();
    pool.execute(task);
  }

  public Future<?> submit(Runnable task) {
    checkInit();
    return pool.submit(task);
  }

  public int getActiveCnt() {
    return ((ThreadPoolExecutor) pool).getActiveCount();
  }

  public int getThreadCnt() {
    return threadCnt;
  }
}
