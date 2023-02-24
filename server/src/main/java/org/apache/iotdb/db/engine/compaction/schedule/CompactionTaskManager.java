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

package org.apache.iotdb.db.engine.compaction.schedule;

import org.apache.iotdb.commons.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.commons.concurrent.ThreadName;
import org.apache.iotdb.commons.concurrent.threadpool.WrappedThreadPoolExecutor;
import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.service.IService;
import org.apache.iotdb.commons.service.ServiceType;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.compaction.execute.task.AbstractCompactionTask;
import org.apache.iotdb.db.engine.compaction.execute.task.CompactionTaskSummary;
import org.apache.iotdb.db.engine.compaction.schedule.comparator.DefaultCompactionTaskComparatorImpl;
import org.apache.iotdb.db.service.metrics.recorder.CompactionMetricsManager;
import org.apache.iotdb.db.utils.datastructure.FixedPriorityBlockingQueue;

import com.google.common.util.concurrent.RateLimiter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

/** CompactionMergeTaskPoolManager provides a ThreadPool tPro queue and run all compaction tasks. */
public class CompactionTaskManager implements IService {
  private static final Logger logger =
      LoggerFactory.getLogger(IoTDBConstant.COMPACTION_LOGGER_NAME);

  private static final long MAX_WAITING_TIME = 120_000L;

  private static final CompactionTaskManager INSTANCE = new CompactionTaskManager();

  private final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();

  // The thread pool that executes the compaction task. The default number of threads for this pool
  // is 10.
  private WrappedThreadPoolExecutor taskExecutionPool;

  // The thread pool that executes the sub compaction task.
  private WrappedThreadPoolExecutor subCompactionTaskExecutionPool;

  public static volatile AtomicInteger currentTaskNum = new AtomicInteger(0);
  private final FixedPriorityBlockingQueue<AbstractCompactionTask> candidateCompactionTaskQueue =
      new FixedPriorityBlockingQueue<>(
          config.getCandidateCompactionTaskQueueSize(), new DefaultCompactionTaskComparatorImpl());
  // <StorageGroup-DataRegionId,futureSet>, it is used to store all compaction tasks under each
  // virtualStorageGroup
  private final Map<String, Map<AbstractCompactionTask, Future<CompactionTaskSummary>>>
      storageGroupTasks = new ConcurrentHashMap<>();
  private final AtomicInteger finishedTaskNum = new AtomicInteger(0);

  private final RateLimiter mergeWriteRateLimiter = RateLimiter.create(Double.MAX_VALUE);

  private volatile boolean init = false;

  public static CompactionTaskManager getInstance() {
    return INSTANCE;
  }

  @Override
  public synchronized void start() {
    if (taskExecutionPool == null
        && IoTDBDescriptor.getInstance().getConfig().getCompactionThreadCount() > 0
        && (config.isEnableSeqSpaceCompaction()
            || config.isEnableUnseqSpaceCompaction()
            || config.isEnableCrossSpaceCompaction())) {
      initThreadPool();
      currentTaskNum = new AtomicInteger(0);
      candidateCompactionTaskQueue.regsitPollLastHook(
          AbstractCompactionTask::resetCompactionCandidateStatusForAllSourceFiles);
      candidateCompactionTaskQueue.regsitPollLastHook(
          x ->
              CompactionMetricsManager.getInstance()
                  .reportPollTaskFromWaitingQueue(x.isCrossTask(), x.isInnerSeqTask()));
      init = true;
    }
    logger.info("Compaction task manager started.");
  }

  private void initThreadPool() {
    int compactionThreadNum = IoTDBDescriptor.getInstance().getConfig().getCompactionThreadCount();
    this.taskExecutionPool =
        (WrappedThreadPoolExecutor)
            IoTDBThreadPoolFactory.newFixedThreadPool(
                compactionThreadNum, ThreadName.COMPACTION_SERVICE.getName());
    this.subCompactionTaskExecutionPool =
        (WrappedThreadPoolExecutor)
            IoTDBThreadPoolFactory.newFixedThreadPool(
                compactionThreadNum
                    * IoTDBDescriptor.getInstance().getConfig().getSubCompactionTaskNum(),
                ThreadName.COMPACTION_SUB_SERVICE.getName());
    for (int i = 0; i < compactionThreadNum; ++i) {
      taskExecutionPool.submit(new CompactionWorker(i, candidateCompactionTaskQueue));
    }
  }

  @Override
  public void stop() {
    if (taskExecutionPool != null) {
      subCompactionTaskExecutionPool.shutdownNow();
      taskExecutionPool.shutdownNow();
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
      logger.info("Waiting for task taskExecutionPool to shut down in {} ms", milliseconds);
      waitTermination();
      storageGroupTasks.clear();
    }
  }

  @TestOnly
  public void waitAllCompactionFinish() {
    long sleepingStartTime = 0;
    if (taskExecutionPool != null) {
      WrappedThreadPoolExecutor tmpThreadPool = taskExecutionPool;
      taskExecutionPool = null;
      candidateCompactionTaskQueue.clear();
      while (true) {
        int totalSize = 0;
        for (Map<AbstractCompactionTask, Future<CompactionTaskSummary>> taskMap :
            storageGroupTasks.values()) {
          totalSize += taskMap.size();
        }
        if (totalSize > 0) {
          try {
            Thread.sleep(100);
          } catch (InterruptedException e) {
            logger.error("Interrupted when waiting all task finish", e);
            break;
          }
        } else {
          break;
        }
      }
      storageGroupTasks.clear();
      taskExecutionPool = tmpThreadPool;
      logger.info("All compaction task finish");
    }
  }

  private void waitTermination() {
    long startTime = System.currentTimeMillis();
    int timeMillis = 0;
    while (!subCompactionTaskExecutionPool.isTerminated() || !taskExecutionPool.isTerminated()) {
      try {
        Thread.sleep(200);
      } catch (InterruptedException e) {
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
      service.shutdownNow();
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
    if (init
        && !candidateCompactionTaskQueue.contains(compactionTask)
        && !isTaskRunning(compactionTask)
        && candidateCompactionTaskQueue.size() < config.getCandidateCompactionTaskQueueSize()) {
      compactionTask.setSourceFilesToCompactionCandidate();
      candidateCompactionTaskQueue.put(compactionTask);

      // add metrics
      CompactionMetricsManager.getInstance()
          .reportAddTaskToWaitingQueue(
              compactionTask.isCrossTask(), compactionTask.isInnerSeqTask());

      return true;
    }
    return false;
  }

  private boolean isTaskRunning(AbstractCompactionTask task) {
    String regionWithSG = getSGWithRegionId(task.getStorageGroupName(), task.getDataRegionId());
    return storageGroupTasks
        .computeIfAbsent(regionWithSG, x -> new ConcurrentHashMap<>())
        .containsKey(task);
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

  public synchronized void removeRunningTaskFuture(AbstractCompactionTask task) {
    String regionWithSG = getSGWithRegionId(task.getStorageGroupName(), task.getDataRegionId());
    if (storageGroupTasks.containsKey(regionWithSG)) {
      storageGroupTasks.get(regionWithSG).remove(task);
    }
    finishedTaskNum.incrementAndGet();
  }

  public synchronized Future<Void> submitSubTask(Callable<Void> subCompactionTask) {
    if (subCompactionTaskExecutionPool != null && !subCompactionTaskExecutionPool.isShutdown()) {
      return subCompactionTaskExecutionPool.submit(subCompactionTask);
    }
    return null;
  }

  /**
   * Abort all compactions of a database. The running compaction tasks will be returned as a list,
   * the compaction threads for the database are not terminated util all the tasks in the list is
   * finish. The outer caller can use function isAnyTaskInListStillRunning to determine this.
   */
  public synchronized List<AbstractCompactionTask> abortCompaction(String storageGroupName) {
    List<AbstractCompactionTask> compactionTaskOfCurSG = new ArrayList<>();
    if (storageGroupTasks.containsKey(storageGroupName)) {
      for (Map.Entry<AbstractCompactionTask, Future<CompactionTaskSummary>> compactionTaskEntry :
          storageGroupTasks.get(storageGroupName).entrySet()) {
        compactionTaskEntry.getValue().cancel(true);
        compactionTaskOfCurSG.add(compactionTaskEntry.getKey());
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
    int runningTaskCnt = 0;
    for (Map<AbstractCompactionTask, Future<CompactionTaskSummary>> runningTaskMap :
        storageGroupTasks.values()) {
      runningTaskCnt += runningTaskMap.size();
    }
    return runningTaskCnt;
  }

  public int getTotalTaskCount() {
    return getExecutingTaskCount() + candidateCompactionTaskQueue.size();
  }

  public int getCompactionCandidateTaskCount() {
    return candidateCompactionTaskQueue.size();
  }

  public synchronized List<AbstractCompactionTask> getRunningCompactionTaskList() {
    List<AbstractCompactionTask> tasks = new ArrayList<>();
    for (Map<AbstractCompactionTask, Future<CompactionTaskSummary>> runningTaskMap :
        storageGroupTasks.values()) {
      tasks.addAll(runningTaskMap.keySet());
    }
    return tasks;
  }

  public long getFinishedTaskNum() {
    return finishedTaskNum.get();
  }

  public void recordTask(AbstractCompactionTask task, Future<CompactionTaskSummary> summary) {
    storageGroupTasks
        .computeIfAbsent(
            getSGWithRegionId(task.getStorageGroupName(), task.getDataRegionId()),
            x -> new ConcurrentHashMap<>())
        .put(task, summary);
  }

  public static String getSGWithRegionId(String storageGroupName, String dataRegionId) {
    return storageGroupName + "-" + dataRegionId;
  }

  @TestOnly
  public void restart() throws InterruptedException {
    if (IoTDBDescriptor.getInstance().getConfig().getCompactionThreadCount() > 0) {
      if (subCompactionTaskExecutionPool != null) {
        this.subCompactionTaskExecutionPool.shutdownNow();
        if (!this.subCompactionTaskExecutionPool.awaitTermination(
            MAX_WAITING_TIME, TimeUnit.MILLISECONDS)) {
          throw new InterruptedException(
              "Has been waiting over "
                  + MAX_WAITING_TIME / 1000
                  + " seconds for all sub compaction tasks to finish.");
        }
      }
      if (taskExecutionPool != null) {
        this.taskExecutionPool.shutdownNow();
        if (!this.taskExecutionPool.awaitTermination(MAX_WAITING_TIME, TimeUnit.MILLISECONDS)) {
          throw new InterruptedException(
              "Has been waiting over "
                  + MAX_WAITING_TIME / 1000
                  + " seconds for all compaction tasks to finish.");
        }
      }
      initThreadPool();
      finishedTaskNum.set(0);
      candidateCompactionTaskQueue.clear();
      init = true;
    }
    currentTaskNum = new AtomicInteger(0);
    init = true;
    logger.info("Compaction task manager started.");
  }

  @TestOnly
  public void clearCandidateQueue() {
    candidateCompactionTaskQueue.clear();
  }

  @TestOnly
  public Future<CompactionTaskSummary> getCompactionTaskFutureMayBlock(AbstractCompactionTask task)
      throws InterruptedException, TimeoutException {
    String regionWithSG = getSGWithRegionId(task.getStorageGroupName(), task.getDataRegionId());
    long startTime = System.currentTimeMillis();
    while (!storageGroupTasks.containsKey(regionWithSG)
        || !storageGroupTasks.get(regionWithSG).containsKey(task)) {
      Thread.sleep(10);
      if (System.currentTimeMillis() - startTime > 20_000) {
        throw new TimeoutException("Timeout when waiting for task future");
      }
    }
    return storageGroupTasks.get(regionWithSG).get(task);
  }
}
