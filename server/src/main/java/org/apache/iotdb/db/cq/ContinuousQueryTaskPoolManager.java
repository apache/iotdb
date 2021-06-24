package org.apache.iotdb.db.cq;

import org.apache.iotdb.db.concurrent.IoTThreadFactory;
import org.apache.iotdb.db.concurrent.ThreadName;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.flush.pool.AbstractPoolManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class ContinuousQueryTaskPoolManager extends AbstractPoolManager {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(ContinuousQueryTaskPoolManager.class);

  private static final int nThreads =
      IoTDBDescriptor.getInstance().getConfig().getContinuousQueryThreadNum();

  private ContinuousQueryTaskPoolManager() {

    LOGGER.info("ContinuousQueryTaskPoolManager is initializing, thread number: {}", nThreads);

    pool =
        new ThreadPoolExecutor(
            nThreads,
            nThreads,
            0L,
            TimeUnit.MILLISECONDS,
            new LinkedBlockingQueue<>(
                IoTDBDescriptor.getInstance().getConfig().getMaxPendingContinuousQueryTasks()),
            new IoTThreadFactory(ThreadName.CONTINUOUS_QUERY_SERVICE.getName()));
  }

  public void submit(ContinuousQueryTask task) {
    try {
      super.submit(task);
    } catch (RejectedExecutionException e) {
      task.onRejection();
    }
  }

  @Override
  public Logger getLogger() {
    return LOGGER;
  }

  @Override
  public String getName() {
    return "continuous query task";
  }

  @Override
  public void start() {
    if (pool != null) {
      return;
    }

    pool =
        new ThreadPoolExecutor(
            nThreads,
            nThreads,
            0L,
            TimeUnit.MILLISECONDS,
            new LinkedBlockingQueue<>(
                IoTDBDescriptor.getInstance().getConfig().getMaxPendingContinuousQueryTasks()),
            new IoTThreadFactory(ThreadName.CONTINUOUS_QUERY_SERVICE.getName()));
  }

  public static ContinuousQueryTaskPoolManager getInstance() {
    return ContinuousQueryTaskPoolManager.InstanceHolder.INSTANCE;
  }

  private static class InstanceHolder {

    private InstanceHolder() {
      // nothing to do
    }

    private static final ContinuousQueryTaskPoolManager INSTANCE =
        new ContinuousQueryTaskPoolManager();
  }
}
