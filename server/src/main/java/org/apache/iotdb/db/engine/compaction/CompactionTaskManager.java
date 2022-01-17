/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.engine.compaction;

import org.apache.iotdb.db.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.db.concurrent.ThreadName;
import org.apache.iotdb.db.concurrent.threadpool.WrappedScheduledExecutorService;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.compaction.cross.AbstractCrossSpaceCompactionTask;
import org.apache.iotdb.db.engine.compaction.inner.AbstractInnerSpaceCompactionTask;
import org.apache.iotdb.db.engine.compaction.task.AbstractCompactionTask;
import org.apache.iotdb.db.service.IService;
import org.apache.iotdb.db.service.ServiceType;
import org.apache.iotdb.db.service.metrics.Metric;
import org.apache.iotdb.db.service.metrics.MetricsService;
import org.apache.iotdb.db.service.metrics.Tag;
import org.apache.iotdb.db.utils.TestOnly;
import org.apache.iotdb.db.utils.datastructure.PriorityBlockingQueueWithMaxSize;
import org.apache.iotdb.metrics.config.MetricConfigDescriptor;
import org.apache.iotdb.metrics.type.Gauge;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/** CompactionMergeTaskPoolManager provides a ThreadPool tPro queue and run all compaction tasks. */
public class CompactionTaskManager implements IService {
  private static final Logger logger = LoggerFactory.getLogger("COMPACTION");
  private static final CompactionTaskManager INSTANCE = new CompactionTaskManager();

  // The thread pool that executes the compaction task. The default number of threads for this pool
  // is 10.
  private WrappedScheduledExecutorService taskExecutionPool;
  public static volatile AtomicInteger currentTaskNum = new AtomicInteger(0);
  private PriorityBlockingQueueWithMaxSize<AbstractCompactionTask> candidateCompactionTaskQueue =
      new PriorityBlockingQueueWithMaxSize<>(1000, new CompactionTaskComparator());
  // <logicalStorageGroupName,futureSet>, it is used to terminate all compaction tasks under the
  // logicalStorageGroup
  private Map<String, Set<Future<Void>>> storageGroupTasks = new ConcurrentHashMap<>();
  private Map<String, Map<Long, Set<Future<Void>>>> compactionTaskFutures =
      new ConcurrentHashMap<>();
  private List<AbstractCompactionTask> runningCompactionTaskList = new ArrayList<>();

  public static Semaphore semaphore = null;

  public static CompactionTaskManager getInstance() {
    return INSTANCE;
  }

  @Override
  public void start() {
    if (taskExecutionPool == null
        && IoTDBDescriptor.getInstance().getConfig().getConcurrentCompactionThread() > 0) {
      this.taskExecutionPool =
          (WrappedScheduledExecutorService)
              IoTDBThreadPoolFactory.newScheduledThreadPool(
                  IoTDBDescriptor.getInstance().getConfig().getConcurrentCompactionThread(),
                  ThreadName.COMPACTION_SERVICE.getName());
      currentTaskNum = new AtomicInteger(0);
      semaphore =
          new Semaphore(IoTDBDescriptor.getInstance().getConfig().getConcurrentCompactionThread());
    }
    logger.info("Compaction task manager started.");
  }

  @Override
  public void stop() {
    if (taskExecutionPool != null) {
      taskExecutionPool.shutdownNow();
      logger.info("Waiting for task taskExecutionPool to shut down");
      waitTermination();
      storageGroupTasks.clear();
    }
  }

  @Override
  public void waitAndStop(long milliseconds) {
    if (taskExecutionPool != null) {
      awaitTermination(taskExecutionPool, milliseconds);
      logger.info("Waiting for task taskExecutionPool to shut down");
      waitTermination();
      storageGroupTasks.clear();
    }
  }

  @TestOnly
  public void waitAllCompactionFinish() {
    long sleepingStartTime = 0;
    long MAX_WAITING_TIME = 120_000L;
    if (taskExecutionPool != null) {
      while (taskExecutionPool.getActiveCount() > 0 || taskExecutionPool.getQueue().size() > 0) {
        // wait
        try {
          Thread.sleep(200);
          sleepingStartTime += 200;
          if (sleepingStartTime % 10000 == 0) {
            logger.warn(
                "Has waiting {} seconds for all compaction task finish", sleepingStartTime / 1000);
          }
          if (sleepingStartTime >= MAX_WAITING_TIME) {
            return;
          }
        } catch (InterruptedException e) {
          logger.error("thread interrupted while waiting for compaction to end", e);
          return;
        }
      }
      storageGroupTasks.clear();
      logger.info("All compaction task finish");
    }
  }

  private void waitTermination() {
    long startTime = System.currentTimeMillis();
    while (!taskExecutionPool.isTerminated()) {
      int timeMillis = 0;
      try {
        Thread.sleep(200);
      } catch (InterruptedException e) {
        logger.error(
            "CompactionMergeTaskPoolManager {} shutdown",
            ThreadName.COMPACTION_SERVICE.getName(),
            e);
        Thread.currentThread().interrupt();
      }
      timeMillis += 200;
      long time = System.currentTimeMillis() - startTime;
      if (timeMillis % 60_000 == 0) {
        logger.info("CompactionManager has wait for {} seconds to stop", time / 1000);
      }
    }
    taskExecutionPool = null;
    storageGroupTasks.clear();
    logger.info("CompactionManager stopped");
  }

  private void awaitTermination(ExecutorService service, long milliseconds) {
    try {
      service.shutdown();
      service.awaitTermination(milliseconds, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      logger.warn("CompactionThreadPool can not be closed in {} ms", milliseconds);
      Thread.currentThread().interrupt();
    }
    service.shutdownNow();
  }

  @Override
  public ServiceType getID() {
    return ServiceType.COMPACTION_SERVICE;
  }

  /**
   * This method submit the compaction task to the PriorityQueue in CompactionTaskManager. Notice!
   * The task will not be submitted immediately. If the queue size is larger than max size, the task
   * with last priority will be removed from the task.
   */
  public synchronized boolean addTaskToWaitingQueue(AbstractCompactionTask compactionTask)
      throws InterruptedException {
    if (!candidateCompactionTaskQueue.contains(compactionTask)
        && !runningCompactionTaskList.contains(compactionTask)) {
      candidateCompactionTaskQueue.put(compactionTask);

      // add metrics
      if (MetricConfigDescriptor.getInstance().getMetricConfig().getEnableMetric()) {
        addMetrics(compactionTask, true, false);
      }

      return true;
    }
    return false;
  }

  /**
   * This method will submit task cached in queue with most priority to execution thread pool if
   * there is available thread.
   */
  public void submitTaskFromTaskQueue() {
    try {
      while (true) {
        semaphore.acquire();
        AbstractCompactionTask compactionTask;
        do {
          compactionTask = candidateCompactionTaskQueue.take();
        } while (!compactionTask.checkValidAndSetMerging());

        if (MetricConfigDescriptor.getInstance().getMetricConfig().getEnableMetric()) {
          addMetrics(compactionTask, false, false);
        }

        submitTask(
            compactionTask.getFullStorageGroupName(),
            compactionTask.getTimePartition(),
            compactionTask);
        runningCompactionTaskList.add(compactionTask);

        // add metrics
        if (MetricConfigDescriptor.getInstance().getMetricConfig().getEnableMetric()) {
          addMetrics(compactionTask, true, true);
        }
      }
    } catch (InterruptedException e) {
      logger.error("Exception occurs while submitting compaction task", e);
    }
  }

  private void addMetrics(AbstractCompactionTask task, boolean isAdd, boolean isRunning) {
    String taskType = "unknown";
    if (task instanceof AbstractInnerSpaceCompactionTask) {
      taskType = "inner";
    } else if (task instanceof AbstractCrossSpaceCompactionTask) {
      taskType = "cross";
    }
    Gauge gauge =
        MetricsService.getInstance()
            .getMetricManager()
            .getOrCreateGauge(
                Metric.QUEUE.toString(),
                Tag.NAME.toString(),
                "compaction_" + taskType,
                Tag.STATUS.toString(),
                isRunning ? "running" : "waiting");
    if (isAdd) {
      gauge.incr(1L);
    } else {
      gauge.decr(1L);
    }
  }

  public synchronized void removeRunningTaskFromList(AbstractCompactionTask task) {
    runningCompactionTaskList.remove(task);
    // add metrics
    if (MetricConfigDescriptor.getInstance().getMetricConfig().getEnableMetric()) {
      addMetrics(task, false, true);
    }
  }

  /**
   * This method will directly submit a task to thread pool if there is available thread.
   *
   * @throws RejectedExecutionException
   */
  public void submitTask(
      String fullStorageGroupName, long timePartition, Callable<Void> compactionMergeTask)
      throws RejectedExecutionException {
    if (taskExecutionPool != null && !taskExecutionPool.isTerminated()) {
      Future<Void> future = taskExecutionPool.submit(compactionMergeTask);
      CompactionScheduler.addPartitionCompaction(fullStorageGroupName, timePartition);
      compactionTaskFutures
          .computeIfAbsent(fullStorageGroupName, k -> new ConcurrentHashMap<>())
          .computeIfAbsent(timePartition, k -> new HashSet<>())
          .add(future);
      return;
    }
    logger.warn(
        "A CompactionTask failed to be submitted to CompactionTaskManager because {}",
        taskExecutionPool == null
            ? "taskExecutionPool is null"
            : "taskExecutionPool is terminated");
  }

  /**
   * Abort all compactions of a storage group. The caller must acquire the write lock of the
   * corresponding storage group.
   */
  public void abortCompaction(String fullStorageGroupName) {
    Set<Future<Void>> subTasks =
        storageGroupTasks.getOrDefault(fullStorageGroupName, Collections.emptySet());
    Iterator<Future<Void>> subIterator = subTasks.iterator();
    while (subIterator.hasNext()) {
      Future<Void> next = subIterator.next();
      if (!next.isDone() && !next.isCancelled()) {
        next.cancel(true);
      }
      subIterator.remove();
    }
  }

  public int getExecutingTaskCount() {
    return taskExecutionPool.getActiveCount() + taskExecutionPool.getQueue().size();
  }

  public int getTotalTaskCount() {
    return getExecutingTaskCount() + candidateCompactionTaskQueue.size();
  }

  public synchronized List<AbstractCompactionTask> getRunningCompactionTaskList() {
    return new ArrayList<>(runningCompactionTaskList);
  }

  public long getFinishTaskNum() {
    return taskExecutionPool.getCompletedTaskCount();
  }

  @TestOnly
  public void restart() {
    if (IoTDBDescriptor.getInstance().getConfig().getConcurrentCompactionThread() > 0) {
      this.taskExecutionPool =
          (WrappedScheduledExecutorService)
              IoTDBThreadPoolFactory.newScheduledThreadPool(
                  IoTDBDescriptor.getInstance().getConfig().getConcurrentCompactionThread(),
                  ThreadName.COMPACTION_SERVICE.getName());
    }
    currentTaskNum = new AtomicInteger(0);
    logger.info("Compaction task manager started.");
  }
}
