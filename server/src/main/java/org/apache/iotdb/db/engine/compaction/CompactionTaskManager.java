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

import org.apache.iotdb.commons.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.commons.concurrent.ThreadName;
import org.apache.iotdb.commons.concurrent.threadpool.WrappedScheduledExecutorService;
import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.service.IService;
import org.apache.iotdb.commons.service.ServiceType;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.compaction.constant.CompactionTaskStatus;
import org.apache.iotdb.db.engine.compaction.task.AbstractCompactionTask;
import org.apache.iotdb.db.utils.datastructure.FixedPriorityBlockingQueue;

import com.google.common.util.concurrent.RateLimiter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/** CompactionMergeTaskPoolManager provides a ThreadPool tPro queue and run all compaction tasks. */
public class CompactionTaskManager implements IService {
  private static final Logger logger =
      LoggerFactory.getLogger(IoTDBConstant.COMPACTION_LOGGER_NAME);
  private static final CompactionTaskManager INSTANCE = new CompactionTaskManager();

  // The thread pool that executes the compaction task. The default number of threads for this pool
  // is 10.
  private WrappedScheduledExecutorService taskExecutionPool;

  // The thread pool that executes the sub compaction task.
  private ScheduledExecutorService subCompactionTaskExecutionPool;

  public static volatile AtomicInteger currentTaskNum = new AtomicInteger(0);
  private FixedPriorityBlockingQueue<AbstractCompactionTask> candidateCompactionTaskQueue =
      new FixedPriorityBlockingQueue<>(1024, new CompactionTaskComparator());
  // <fullStorageGroupName,futureSet>, it is used to store all compaction tasks under each
  // virtualStorageGroup
  private Map<String, Map<AbstractCompactionTask, Future<Void>>> storageGroupTasks =
      new HashMap<>();

  // The thread pool that periodically fetches and executes the compaction task from
  // candidateCompactionTaskQueue to taskExecutionPool. The default number of threads for this pool
  // is 1.
  private ScheduledExecutorService compactionTaskSubmissionThreadPool;

  private final long TASK_SUBMIT_INTERVAL =
      IoTDBDescriptor.getInstance().getConfig().getCompactionSubmissionIntervalInMs();

  private final RateLimiter mergeWriteRateLimiter = RateLimiter.create(Double.MAX_VALUE);

  public static CompactionTaskManager getInstance() {
    return INSTANCE;
  }

  @Override
  public synchronized void start() {
    if (taskExecutionPool == null
        && IoTDBDescriptor.getInstance().getConfig().getConcurrentCompactionThread() > 0) {
      this.taskExecutionPool =
          (WrappedScheduledExecutorService)
              IoTDBThreadPoolFactory.newScheduledThreadPool(
                  IoTDBDescriptor.getInstance().getConfig().getConcurrentCompactionThread(),
                  ThreadName.COMPACTION_SERVICE.getName());
      this.subCompactionTaskExecutionPool =
          IoTDBThreadPoolFactory.newScheduledThreadPool(
              IoTDBDescriptor.getInstance().getConfig().getConcurrentCompactionThread()
                  * IoTDBDescriptor.getInstance().getConfig().getSubCompactionTaskNum(),
              ThreadName.COMPACTION_SUB_SERVICE.getName());
      currentTaskNum = new AtomicInteger(0);
      compactionTaskSubmissionThreadPool =
          IoTDBThreadPoolFactory.newScheduledThreadPool(1, ThreadName.COMPACTION_SERVICE.getName());
      candidateCompactionTaskQueue.regsitPollLastHook(
          AbstractCompactionTask::resetCompactionCandidateStatusForAllSourceFiles);
      candidateCompactionTaskQueue.regsitPollLastHook(
          x ->
              CompactionMetricsManager.recordTaskInfo(
                  x, CompactionTaskStatus.POLL_FROM_QUEUE, candidateCompactionTaskQueue.size()));

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
  public synchronized void stop() {
    if (taskExecutionPool != null) {
      taskExecutionPool.shutdownNow();
      compactionTaskSubmissionThreadPool.shutdownNow();
      logger.info("Waiting for task taskExecutionPool to shut down");
      waitTermination();
      storageGroupTasks.clear();
      candidateCompactionTaskQueue.clear();
    }
  }

  @Override
  public synchronized void waitAndStop(long milliseconds) {
    if (taskExecutionPool != null) {
      awaitTermination(taskExecutionPool, milliseconds);
      awaitTermination(compactionTaskSubmissionThreadPool, milliseconds);
      logger.info("Waiting for task taskExecutionPool to shut down");
      waitTermination();
      storageGroupTasks.clear();
    }
  }

  @TestOnly
  public synchronized void waitAllCompactionFinish() {
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

  private synchronized void waitTermination() {
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

  private synchronized void awaitTermination(ExecutorService service, long milliseconds) {
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
    if (!candidateCompactionTaskQueue.contains(compactionTask) && !isTaskRunning(compactionTask)) {
      compactionTask.setSourceFilesToCompactionCandidate();
      candidateCompactionTaskQueue.put(compactionTask);

      // add metrics
      CompactionMetricsManager.recordTaskInfo(
          compactionTask, CompactionTaskStatus.ADD_TO_QUEUE, candidateCompactionTaskQueue.size());

      return true;
    }
    return false;
  }

  private boolean isTaskRunning(AbstractCompactionTask task) {
    String storageGroupName = task.getFullStorageGroupName();
    return storageGroupTasks
        .computeIfAbsent(storageGroupName, x -> new HashMap<>())
        .containsKey(task);
  }

  /**
   * This method will submit task cached in queue with most priority to execution thread pool if
   * there is available thread.
   */
  public synchronized void submitTaskFromTaskQueue() {
    try {
      while (currentTaskNum.get()
              < IoTDBDescriptor.getInstance().getConfig().getConcurrentCompactionThread()
          && !candidateCompactionTaskQueue.isEmpty()) {
        AbstractCompactionTask task = candidateCompactionTaskQueue.take();

        // add metrics
        CompactionMetricsManager.recordTaskInfo(
            task, CompactionTaskStatus.POLL_FROM_QUEUE, candidateCompactionTaskQueue.size());

        if (task != null && task.checkValidAndSetMerging()) {
          submitTask(task);
          CompactionMetricsManager.recordTaskInfo(
              task, CompactionTaskStatus.READY_TO_EXECUTE, currentTaskNum.get());
        }
      }
    } catch (InterruptedException e) {
      logger.error("Exception occurs while submitting compaction task", e);
    }
  }

  public RateLimiter getMergeWriteRateLimiter() {
    setWriteMergeRate(
        IoTDBDescriptor.getInstance().getConfig().getCompactionWriteThroughputMbPerSec());
    return mergeWriteRateLimiter;
  }

  private void setWriteMergeRate(final double throughoutMbPerSec) {
    double throughout = throughoutMbPerSec * 1024.0 * 1024.0;
    // if throughout = 0, disable rate limiting
    if (throughout == 0) {
      throughout = Double.MAX_VALUE;
    }
    if (mergeWriteRateLimiter.getRate() != throughout) {
      mergeWriteRateLimiter.setRate(throughout);
    }
  }
  /** wait by throughoutMbPerSec limit to avoid continuous Write Or Read */
  public static void mergeRateLimiterAcquire(RateLimiter limiter, long bytesLength) {
    while (bytesLength >= Integer.MAX_VALUE) {
      limiter.acquire(Integer.MAX_VALUE);
      bytesLength -= Integer.MAX_VALUE;
    }
    if (bytesLength > 0) {
      limiter.acquire((int) bytesLength);
    }
  }

  public synchronized void removeRunningTaskFromList(AbstractCompactionTask task) {
    String storageGroupName = task.getFullStorageGroupName();
    if (storageGroupTasks.containsKey(storageGroupName)) {
      storageGroupTasks.get(storageGroupName).remove(task);
    }
    // add metrics
    CompactionMetricsManager.recordTaskInfo(
        task, CompactionTaskStatus.FINISHED, currentTaskNum.get());
  }

  /**
   * This method will directly submit a task to thread pool if there is available thread.
   *
   * @throws RejectedExecutionException
   */
  public synchronized void submitTask(AbstractCompactionTask compactionTask)
      throws RejectedExecutionException {
    if (taskExecutionPool != null && !taskExecutionPool.isTerminated()) {
      Future<Void> future = taskExecutionPool.submit(compactionTask);
      storageGroupTasks
          .computeIfAbsent(compactionTask.getFullStorageGroupName(), x -> new HashMap<>())
          .put(compactionTask, future);
      return;
    }
    logger.warn(
        "A CompactionTask failed to be submitted to CompactionTaskManager because {}",
        taskExecutionPool == null
            ? "taskExecutionPool is null"
            : "taskExecutionPool is terminated");
  }

  public synchronized Future<Void> submitSubTask(Callable<Void> subCompactionTask) {
    if (subCompactionTaskExecutionPool != null && !subCompactionTaskExecutionPool.isTerminated()) {
      Future<Void> future = subCompactionTaskExecutionPool.submit(subCompactionTask);
      return future;
    }
    return null;
  }

  /**
   * Abort all compactions of a storage group. The running compaction tasks will be returned as a
   * list, the compaction threads for the storage group are not terminated util all the tasks in the
   * list is finish. The outer caller can use function isAnyTaskInListStillRunning to determine
   * this.
   */
  public synchronized List<AbstractCompactionTask> abortCompaction(String storageGroupName) {
    List<AbstractCompactionTask> compactionTaskOfCurSG = new ArrayList<>();
    if (storageGroupTasks.containsKey(storageGroupName)) {
      for (Map.Entry<AbstractCompactionTask, Future<Void>> taskFutureEntry :
          storageGroupTasks.get(storageGroupName).entrySet()) {
        taskFutureEntry.getValue().cancel(true);
        compactionTaskOfCurSG.add(taskFutureEntry.getKey());
      }
    }

    storageGroupTasks.remove(storageGroupName);

    candidateCompactionTaskQueue.clear();
    return compactionTaskOfCurSG;
  }

  public boolean isAnyTaskInListStillRunning(List<AbstractCompactionTask> compactionTasks) {
    boolean anyTaskRunning = false;
    for (AbstractCompactionTask task : compactionTasks) {
      anyTaskRunning = anyTaskRunning || (task.isTaskRan() && !task.isTaskFinished());
    }
    return anyTaskRunning;
  }

  public int getExecutingTaskCount() {
    return taskExecutionPool.getActiveCount() + taskExecutionPool.getQueue().size();
  }

  public int getTotalTaskCount() {
    return getExecutingTaskCount() + candidateCompactionTaskQueue.size();
  }

  public synchronized List<AbstractCompactionTask> getRunningCompactionTaskList() {
    List<AbstractCompactionTask> tasks = new ArrayList<>();
    for (Map<AbstractCompactionTask, Future<Void>> taskFutureMap : storageGroupTasks.values()) {
      tasks.addAll(taskFutureMap.keySet());
    }
    return tasks;
  }

  public long getFinishTaskNum() {
    return taskExecutionPool.getCompletedTaskCount();
  }

  @TestOnly
  public void restart() throws InterruptedException {
    if (IoTDBDescriptor.getInstance().getConfig().getConcurrentCompactionThread() > 0) {
      if (taskExecutionPool != null) {
        this.taskExecutionPool.shutdownNow();
        this.taskExecutionPool.awaitTermination(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
      }
      this.taskExecutionPool =
          (WrappedScheduledExecutorService)
              IoTDBThreadPoolFactory.newScheduledThreadPool(
                  IoTDBDescriptor.getInstance().getConfig().getConcurrentCompactionThread(),
                  ThreadName.COMPACTION_SERVICE.getName());
      this.compactionTaskSubmissionThreadPool =
          IoTDBThreadPoolFactory.newScheduledThreadPool(1, ThreadName.COMPACTION_SERVICE.getName());
      candidateCompactionTaskQueue.regsitPollLastHook(
          AbstractCompactionTask::resetCompactionCandidateStatusForAllSourceFiles);
      candidateCompactionTaskQueue.clear();
    }
    currentTaskNum = new AtomicInteger(0);
    logger.info("Compaction task manager started.");
  }

  @TestOnly
  public void clearCandidateQueue() {
    candidateCompactionTaskQueue.clear();
  }
}
