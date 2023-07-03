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

package org.apache.iotdb.db.storageengine.dataregion.compaction.schedule;

import org.apache.iotdb.commons.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.commons.concurrent.ThreadName;
import org.apache.iotdb.commons.concurrent.threadpool.ScheduledExecutorUtil;
import org.apache.iotdb.commons.concurrent.threadpool.WrappedThreadPoolExecutor;
import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.service.IService;
import org.apache.iotdb.commons.service.ServiceType;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.storageengine.dataregion.compaction.constant.CompactionTaskStatus;
import org.apache.iotdb.db.storageengine.dataregion.compaction.constant.CompactionTaskType;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.task.AbstractCompactionTask;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.task.CompactionTaskSummary;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.task.InnerSpaceCompactionTask;
import org.apache.iotdb.db.storageengine.dataregion.compaction.schedule.comparator.DefaultCompactionTaskComparatorImpl;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileManager;
import org.apache.iotdb.db.utils.datastructure.FixedPriorityBlockingQueue;

import com.google.common.util.concurrent.RateLimiter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

/** CompactionMergeTaskPoolManager provides a ThreadPool tPro queue and run all compaction tasks. */
@SuppressWarnings("squid:S6548")
public class CompactionTaskManager implements IService {
  private static final Logger logger =
      LoggerFactory.getLogger(IoTDBConstant.COMPACTION_LOGGER_NAME);

  private static final long MAX_WAITING_TIME = 120_000L;

  private static final CompactionTaskManager INSTANCE = new CompactionTaskManager();

  private final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();

  private final int compactionScheduledThreadNum = config.getCompactionScheduledThreadCount();

  @SuppressWarnings("squid:S1444")
  public static long compactionTaskSubmitDelay = 20L * 1000L;

  private AtomicInteger dataRegionNum = new AtomicInteger(0);

  // scheduledCompactionThreadID -> tsFileManager list
  private final Map<Integer, List<TsFileManager>> dataRegionMap = new ConcurrentHashMap<>();

  private ScheduledExecutorService compactionScheduledPool;

  // The thread pool that executes the compaction task. The default number of threads for this pool
  // is 10.
  private WrappedThreadPoolExecutor taskExecutionPool;

  // The thread pool that executes the sub compaction task.
  private WrappedThreadPoolExecutor subCompactionTaskExecutionPool;

  public static final AtomicInteger currentTaskNum = new AtomicInteger(0);

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
      currentTaskNum.set(0);
      candidateCompactionTaskQueue.regsitPollLastHook(
          AbstractCompactionTask::resetCompactionCandidateStatusForAllSourceFiles);
      init = true;
    }
    logger.info("Compaction task manager started.");
  }

  private void initThreadPool() {
    this.compactionScheduledPool =
        IoTDBThreadPoolFactory.newScheduledThreadPool(
            compactionScheduledThreadNum, ThreadName.COMPACTION_SCHEDULE.getName());
    for (int i = 0; i < compactionScheduledThreadNum; i++) {
      ScheduledExecutorUtil.safelyScheduleWithFixedDelay(
          compactionScheduledPool,
          new CompactionScheduledWorker(i, dataRegionMap),
          compactionTaskSubmitDelay,
          IoTDBDescriptor.getInstance().getConfig().getCompactionScheduleIntervalInMs(),
          TimeUnit.MILLISECONDS);
    }
    int compactionThreadNum = IoTDBDescriptor.getInstance().getConfig().getCompactionThreadCount();
    this.taskExecutionPool =
        (WrappedThreadPoolExecutor)
            IoTDBThreadPoolFactory.newFixedThreadPool(
                compactionThreadNum, ThreadName.COMPACTION_WORKER.getName());
    this.subCompactionTaskExecutionPool =
        (WrappedThreadPoolExecutor)
            IoTDBThreadPoolFactory.newFixedThreadPool(
                compactionThreadNum
                    * IoTDBDescriptor.getInstance().getConfig().getSubCompactionTaskNum(),
                ThreadName.COMPACTION_SUB_TASK.getName());
    for (int i = 0; i < compactionThreadNum; ++i) {
      taskExecutionPool.submit(new CompactionWorker(i, candidateCompactionTaskQueue));
    }
  }

  @Override
  public void stop() {
    if (taskExecutionPool != null) {
      subCompactionTaskExecutionPool.shutdownNow();
      taskExecutionPool.shutdownNow();
      compactionScheduledPool.shutdownNow();
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
      awaitTermination(compactionScheduledPool, milliseconds);
      logger.info("Waiting for task taskExecutionPool to shut down in {} ms", milliseconds);
      waitTermination();
      storageGroupTasks.clear();
    }
  }

  @SuppressWarnings({"squid:S2142", "squid:S135", "VariableDeclarationUsageDistanceCheck"})
  @TestOnly
  public void waitAllCompactionFinish() {
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

  public synchronized void register(TsFileManager tsFileManager) {
    int idx = dataRegionNum.getAndAdd(1) % compactionScheduledThreadNum;
    dataRegionMap.computeIfAbsent(idx, k -> new ArrayList<>()).add(tsFileManager);
  }

  public synchronized void unRegister(TsFileManager tsFileManager) {
    for (List<TsFileManager> tsFileManagerList : dataRegionMap.values()) {
      if (tsFileManagerList.contains(tsFileManager)) {
        tsFileManagerList.remove(tsFileManager);

        // balance the number of data regions of each thread
        List<TsFileManager> tsFileManagers =
            dataRegionMap.get(dataRegionNum.addAndGet(-1) % compactionScheduledThreadNum);
        if (tsFileManagers != null && !tsFileManagers.isEmpty()) {
          tsFileManagerList.add(tsFileManagers.remove(tsFileManagers.size() - 1));
        }
        break;
      }
    }
  }

  private void waitTermination() {
    long startTime = System.currentTimeMillis();
    int timeMillis = 0;
    while (!subCompactionTaskExecutionPool.isTerminated()
        || !taskExecutionPool.isTerminated()
        || !compactionScheduledPool.isTerminated()) {
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
    compactionScheduledPool = null;
    storageGroupTasks.clear();
    dataRegionMap.clear();
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
   *
   * @throws InterruptedException if there is an issue when put compaction task
   */
  public synchronized boolean addTaskToWaitingQueue(AbstractCompactionTask compactionTask)
      throws InterruptedException {
    if (init
        && !candidateCompactionTaskQueue.contains(compactionTask)
        && !isTaskRunning(compactionTask)
        && compactionTask.setSourceFilesToCompactionCandidate()) {
      candidateCompactionTaskQueue.put(compactionTask);
      return true;
    }
    return false;
  }

  private boolean isTaskRunning(AbstractCompactionTask task) {
    String regionWithSG = getSgWithRegionId(task.getStorageGroupName(), task.getDataRegionId());
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

  public synchronized void removeRunningTaskFuture(AbstractCompactionTask task) {
    String regionWithSG = getSgWithRegionId(task.getStorageGroupName(), task.getDataRegionId());
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
            getSgWithRegionId(task.getStorageGroupName(), task.getDataRegionId()),
            x -> new ConcurrentHashMap<>())
        .put(task, summary);
  }

  private void getWaitingTaskStatus(
      Map<CompactionTaskType, Map<CompactionTaskStatus, Integer>> statistic) {
    List<AbstractCompactionTask> waitingTaskList =
        this.candidateCompactionTaskQueue.getAllElementAsList();
    for (AbstractCompactionTask task : waitingTaskList) {
      if (task instanceof InnerSpaceCompactionTask) {
        statistic
            .computeIfAbsent(
                task.isInnerSeqTask()
                    ? CompactionTaskType.INNER_SEQ
                    : CompactionTaskType.INNER_UNSEQ,
                x -> new EnumMap<>(CompactionTaskStatus.class))
            .compute(CompactionTaskStatus.WAITING, (k, v) -> v == null ? 1 : v + 1);
      } else {
        statistic
            .computeIfAbsent(
                CompactionTaskType.CROSS, x -> new EnumMap<>(CompactionTaskStatus.class))
            .compute(CompactionTaskStatus.WAITING, (k, v) -> v == null ? 1 : v + 1);
      }
    }
  }

  private void getRunningTaskStatus(
      Map<CompactionTaskType, Map<CompactionTaskStatus, Integer>> statistic) {
    List<AbstractCompactionTask> runningTaskList = this.getRunningCompactionTaskList();
    for (AbstractCompactionTask task : runningTaskList) {
      if (task instanceof InnerSpaceCompactionTask) {
        statistic
            .computeIfAbsent(
                task.isInnerSeqTask()
                    ? CompactionTaskType.INNER_SEQ
                    : CompactionTaskType.INNER_UNSEQ,
                x -> new EnumMap<>(CompactionTaskStatus.class))
            .compute(CompactionTaskStatus.RUNNING, (k, v) -> v == null ? 1 : v + 1);
      } else {
        statistic
            .computeIfAbsent(
                CompactionTaskType.CROSS, x -> new EnumMap<>(CompactionTaskStatus.class))
            .compute(CompactionTaskStatus.RUNNING, (k, v) -> v == null ? 1 : v + 1);
      }
    }
  }

  public Map<CompactionTaskType, Map<CompactionTaskStatus, Integer>> getCompactionTaskStatistic() {
    Map<CompactionTaskType, Map<CompactionTaskStatus, Integer>> statistic =
        new EnumMap<>(CompactionTaskType.class);

    // update statistic of waiting tasks
    getWaitingTaskStatus(statistic);

    // update statistic of running tasks
    getRunningTaskStatus(statistic);

    return statistic;
  }

  public static String getSgWithRegionId(String storageGroupName, String dataRegionId) {
    return storageGroupName + "-" + dataRegionId;
  }

  @TestOnly
  @SuppressWarnings({"squid:S3776", "squid:S1192"})
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
      if (compactionScheduledPool != null) {
        this.compactionScheduledPool.shutdownNow();
        if (!this.compactionScheduledPool.awaitTermination(
            MAX_WAITING_TIME, TimeUnit.MILLISECONDS)) {
          throw new InterruptedException(
              "Has been waiting over "
                  + MAX_WAITING_TIME / 1000
                  + " seconds for all scheduled compaction threads to finish.");
        }
      }
      initThreadPool();
      finishedTaskNum.set(0);
      candidateCompactionTaskQueue.clear();
      init = true;
    }
    currentTaskNum.set(0);
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
    String regionWithSG = getSgWithRegionId(task.getStorageGroupName(), task.getDataRegionId());
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

  @TestOnly
  public Map<Integer, List<TsFileManager>> getDataRegionMap() {
    return dataRegionMap;
  }

  @TestOnly
  public boolean isAllThreadPoolTerminated() {
    if (taskExecutionPool == null) {
      return subCompactionTaskExecutionPool == null && compactionScheduledPool == null;
    }
    return compactionScheduledPool.isTerminated()
        && taskExecutionPool.isTerminated()
        && subCompactionTaskExecutionPool.isTerminated();
  }

  @TestOnly
  public boolean isDataRegionStillInScheduled(TsFileManager tsFileManager) {
    for (Map.Entry<Integer, List<TsFileManager>> entry : dataRegionMap.entrySet()) {
      if (entry.getValue().contains(tsFileManager)) {
        return true;
      }
    }
    return false;
  }
}
