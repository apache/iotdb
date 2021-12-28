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
import org.apache.iotdb.metrics.config.MetricConfigDescriptor;
import org.apache.iotdb.metrics.type.Gauge;

import com.google.common.collect.MinMaxPriorityQueue;
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
import java.util.concurrent.ScheduledExecutorService;
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
  private MinMaxPriorityQueue<AbstractCompactionTask> candidateCompactionTaskQueue =
      MinMaxPriorityQueue.orderedBy(new CompactionTaskComparator()).maximumSize(1000).create();
  // <logicalStorageGroupName,futureSet>, it is used to terminate all compaction tasks under the
  // logicalStorageGroup
  private Map<String, Set<Future<Void>>> storageGroupTasks = new ConcurrentHashMap<>();
  private Map<String, Map<Long, Set<Future<Void>>>> compactionTaskFutures =
      new ConcurrentHashMap<>();
  private List<AbstractCompactionTask> runningCompactionTaskList = new ArrayList<>();

  // The thread pool that periodically fetches and executes the compaction task from
  // candidateCompactionTaskQueue to taskExecutionPool. The default number of threads for this pool
  // is 1.
  private ScheduledExecutorService compactionTaskSubmissionThreadPool;

  private final long TASK_SUBMIT_INTERVAL =
      IoTDBDescriptor.getInstance().getConfig().getCompactionSubmissionInterval();

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
      compactionTaskSubmissionThreadPool =
          IoTDBThreadPoolFactory.newScheduledThreadPool(1, ThreadName.COMPACTION_SERVICE.getName());

      // Periodically do the following: fetch the highest priority thread from the
      // candidateCompactionTaskQueue, check that all tsfiles in the compaction task are valid, and
      // if there is thread space available in the taskExecutionPool, put the compaction task thread
      // into the taskExecutionPool and perform the compaction.
      compactionTaskSubmissionThreadPool.scheduleWithFixedDelay(
          this::submitTaskFromTaskQueue,
          TASK_SUBMIT_INTERVAL,
          TASK_SUBMIT_INTERVAL,
          TimeUnit.MILLISECONDS);
    }
    logger.info("Compaction task manager started.");
  }

  @Override
  public void stop() {
    if (taskExecutionPool != null) {
      taskExecutionPool.shutdownNow();
      compactionTaskSubmissionThreadPool.shutdownNow();
      logger.info("Waiting for task taskExecutionPool to shut down");
      waitTermination();
      storageGroupTasks.clear();
    }
  }

  @Override
  public void waitAndStop(long milliseconds) {
    if (taskExecutionPool != null) {
      awaitTermination(taskExecutionPool, milliseconds);
      awaitTermination(compactionTaskSubmissionThreadPool, milliseconds);
      logger.info("Waiting for task taskExecutionPool to shut down");
      waitTermination();
      storageGroupTasks.clear();
    }
  }

  @TestOnly
  public void waitAllCompactionFinish() {
    if (taskExecutionPool != null) {
      while (taskExecutionPool.getActiveCount() > 0 || taskExecutionPool.getQueue().size() > 0) {
        // wait
        try {
          Thread.sleep(200);
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
  public synchronized boolean addTaskToWaitingQueue(AbstractCompactionTask compactionTask) {
    if (!candidateCompactionTaskQueue.contains(compactionTask)
        && !runningCompactionTaskList.contains(compactionTask)) {
      candidateCompactionTaskQueue.add(compactionTask);

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
  public synchronized void submitTaskFromTaskQueue() {
    while (currentTaskNum.get()
            < IoTDBDescriptor.getInstance().getConfig().getConcurrentCompactionThread()
        && candidateCompactionTaskQueue.size() > 0) {
      AbstractCompactionTask task = candidateCompactionTaskQueue.poll();

      // add metrics
      if (MetricConfigDescriptor.getInstance().getMetricConfig().getEnableMetric()) {
        addMetrics(task, false, false);
      }

      if (task != null && task.checkValidAndSetMerging()) {
        submitTask(task.getFullStorageGroupName(), task.getTimePartition(), task);
        runningCompactionTaskList.add(task);

        // add metrics
        if (MetricConfigDescriptor.getInstance().getMetricConfig().getEnableMetric()) {
          addMetrics(task, true, true);
        }
      }
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
  public synchronized void submitTask(
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
      this.compactionTaskSubmissionThreadPool =
          IoTDBThreadPoolFactory.newScheduledThreadPool(1, ThreadName.COMPACTION_SERVICE.getName());
    }
    currentTaskNum = new AtomicInteger(0);
    logger.info("Compaction task manager started.");
  }
}
