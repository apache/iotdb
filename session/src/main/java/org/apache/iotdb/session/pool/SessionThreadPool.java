package org.apache.iotdb.session.pool;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.iotdb.session.Config;

public class SessionThreadPool {

  private ExecutorService pool;
  private BlockingQueue<Runnable> threadQueue;
  private static final int WAIT_TIMEOUT = 10000;

  public SessionThreadPool() {
    threadQueue = new LinkedBlockingQueue<Runnable>(Config.DEFAULT_BLOCKING_QUEUE_SIZE);
    pool = new ThreadPoolExecutor(Config.DEFAULT_THREAD_POOL_SIZE, Config.DEFAULT_THREAD_POOL_SIZE,
        0L, TimeUnit.MILLISECONDS, threadQueue, new CustomPolicy());
  }

  public SessionThreadPool(int poolSize, int blockingQueueSize) {
    threadQueue = new LinkedBlockingQueue<Runnable>(blockingQueueSize);
    pool = new ThreadPoolExecutor(poolSize, poolSize,
        0L, TimeUnit.MILLISECONDS, threadQueue, new CustomPolicy());
  }

  public synchronized Future<?> submit(Runnable task) {
    return pool.submit(task);
  }

  public synchronized <T> Future<T> submit(Callable<T> task) {
    return pool.submit(task);
  }

  private static class CustomPolicy implements RejectedExecutionHandler {
    public CustomPolicy() {}

    @Override
    public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
      try {
        synchronized (r) {
          r.wait(WAIT_TIMEOUT);
        }
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
  }
}
