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
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.compaction.constant.CompactionTaskStatus;
import org.apache.iotdb.db.engine.compaction.task.AbstractCompactionTask;
import org.apache.iotdb.db.engine.compaction.task.CompactionTaskSummary;
import org.apache.iotdb.db.service.IService;
import org.apache.iotdb.db.service.IoTDB;
import org.apache.iotdb.db.service.ServiceType;
import org.apache.iotdb.db.utils.TestOnly;
import org.apache.iotdb.db.utils.datastructure.FixedPriorityBlockingQueue;

import com.google.common.util.concurrent.RateLimiter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
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
  // <logicalStorageGroupName,futureSet>, it is used to terminate all compaction tasks under the
  // logicalStorageGroup
  private Map<String, Set<Future<CompactionTaskSummary>>> storageGroupTasks =
      new ConcurrentHashMap<>();
  private List<AbstractCompactionTask> runningCompactionTaskList = new ArrayList<>();

  // The thread pool that periodically fetches and executes the compaction task from
  // candidateCompactionTaskQueue to taskExecutionPool. The default number of threads for this pool
  // is 1.
  private ScheduledExecutorService compactionTaskSubmissionThreadPool;

  private final long TASK_SUBMIT_INTERVAL =
      IoTDBDescriptor.getInstance().getConfig().getCompactionSubmissionIntervalInMs();

  private final RateLimiter compactionIORateLimiter = RateLimiter.create(Double.MAX_VALUE);

  public static CompactionTaskManager getInstance() {
    return INSTANCE;
  }

  private final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();

  @Override
  public void start() {
    if (taskExecutionPool == null
        && IoTDBDescriptor.getInstance().getConfig().getConcurrentCompactionThread() > 0
        && (config.isEnableSeqSpaceCompaction()
            || config.isEnableCrossSpaceCompaction()
            || config.isEnableUnseqSpaceCompaction())) {
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
  public void stop() {
    if (taskExecutionPool != null) {
      subCompactionTaskExecutionPool.shutdownNow();
      taskExecutionPool.shutdownNow();
      compactionTaskSubmissionThreadPool.shutdownNow();
      logger.info("Waiting for task taskExecutionPool to shut down");
      waitTermination();
      storageGroupTasks.clear();
      candidateCompactionTaskQueue.clear();
    }
  }

  @Override
  public void waitAndStop(long milliseconds) {
    if (taskExecutionPool != null) {
      awaitTermination(subCompactionTaskExecutionPool, milliseconds);
      awaitTermination(taskExecutionPool, milliseconds);
      awaitTermination(compactionTaskSubmissionThreadPool, milliseconds);
      logger.info("Waiting for task taskExecutionPool to shut down in {} ms", milliseconds);
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
      candidateCompactionTaskQueue.clear();
      logger.info("All compaction task finish");
    }
  }

  private void waitTermination() {
    long startTime = System.currentTimeMillis();
    while (!subCompactionTaskExecutionPool.isTerminated() || !taskExecutionPool.isTerminated()) {
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
    subCompactionTaskExecutionPool = null;
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
      compactionTask.setSourceFilesToCompactionCandidate();
      candidateCompactionTaskQueue.put(compactionTask);

      // add metrics
      CompactionMetricsManager.recordTaskInfo(
          compactionTask, CompactionTaskStatus.ADD_TO_QUEUE, candidateCompactionTaskQueue.size());

      return true;
    }
    return false;
  }

  /**
   * This method will submit task cached in queue with most priority to execution thread pool if
   * there is available thread.
   */
  public synchronized void submitTaskFromTaskQueue() {
    try {
      while (IoTDB.activated
          && currentTaskNum.get()
              < IoTDBDescriptor.getInstance().getConfig().getConcurrentCompactionThread()
          && !candidateCompactionTaskQueue.isEmpty()) {
        AbstractCompactionTask task = candidateCompactionTaskQueue.take();

        // add metrics
        CompactionMetricsManager.recordTaskInfo(
            task, CompactionTaskStatus.POLL_FROM_QUEUE, candidateCompactionTaskQueue.size());

        if (task != null && task.checkValidAndSetMerging()) {
          submitTask(task);
          runningCompactionTaskList.add(task);
          CompactionMetricsManager.recordTaskInfo(
              task, CompactionTaskStatus.READY_TO_EXECUTE, runningCompactionTaskList.size());
        }
      }
    } catch (InterruptedException e) {
      logger.error("Exception occurs while submitting compaction task", e);
    }
  }

  public RateLimiter getCompactionIORateLimiter() {
    setWriteMergeRate(IoTDBDescriptor.getInstance().getConfig().getCompactionIORatePerSec());
    return compactionIORateLimiter;
  }

  private void setWriteMergeRate(final double throughoutMbPerSec) {
    double throughout = throughoutMbPerSec * 1024.0 * 1024.0;
    // if throughout = 0, disable rate limiting
    if (throughout == 0) {
      throughout = Double.MAX_VALUE;
    }
    if (compactionIORateLimiter.getRate() != throughout) {
      compactionIORateLimiter.setRate(throughout);
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
    runningCompactionTaskList.remove(task);
    // add metrics
    CompactionMetricsManager.recordTaskInfo(
        task, CompactionTaskStatus.FINISHED, runningCompactionTaskList.size());
  }

  /**
   * This method will directly submit a task to thread pool if there is available thread.
   *
   * @return the future of the task.
   */
  public synchronized Future<CompactionTaskSummary> submitTask(
      Callable<CompactionTaskSummary> compactionMergeTask) throws RejectedExecutionException {
    if (taskExecutionPool != null && !taskExecutionPool.isShutdown()) {
      Future<CompactionTaskSummary> future = taskExecutionPool.submit(compactionMergeTask);
      return future;
    }
    logger.warn(
        "A CompactionTask failed to be submitted to CompactionTaskManager because {}",
        taskExecutionPool == null
            ? "taskExecutionPool is null"
            : "taskExecutionPool is terminated");
    return null;
  }

  public synchronized Future<Void> submitSubTask(Callable<Void> subCompactionTask) {
    if (subCompactionTaskExecutionPool != null && !subCompactionTaskExecutionPool.isShutdown()) {
      return subCompactionTaskExecutionPool.submit(subCompactionTask);
    }
    return null;
  }

  /**
   * Abort all compactions of a storage group. The caller must acquire the write lock of the
   * corresponding storage group.
   */
  public void abortCompaction(String fullStorageGroupName) {
    Set<Future<CompactionTaskSummary>> subTasks =
        storageGroupTasks.getOrDefault(fullStorageGroupName, Collections.emptySet());
    candidateCompactionTaskQueue.clear();
    Iterator<Future<CompactionTaskSummary>> subIterator = subTasks.iterator();
    while (subIterator.hasNext()) {
      Future<CompactionTaskSummary> next = subIterator.next();
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
  public void restart() throws InterruptedException {
    if (IoTDBDescriptor.getInstance().getConfig().getConcurrentCompactionThread() > 0) {
      if (taskExecutionPool != null) {
        subCompactionTaskExecutionPool.shutdownNow();
        subCompactionTaskExecutionPool.awaitTermination(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
        this.taskExecutionPool.shutdownNow();
        this.taskExecutionPool.awaitTermination(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
      }
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
