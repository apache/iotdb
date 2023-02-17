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
package org.apache.iotdb.db.mpp.execution.schedule;

import org.apache.iotdb.commons.exception.StartupException;
import org.apache.iotdb.commons.service.IService;
import org.apache.iotdb.commons.service.ServiceType;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.mpp.common.FragmentInstanceId;
import org.apache.iotdb.db.mpp.common.QueryId;
import org.apache.iotdb.db.mpp.execution.driver.IDriver;
import org.apache.iotdb.db.mpp.execution.exchange.IMPPDataExchangeManager;
import org.apache.iotdb.db.mpp.execution.exchange.MPPDataExchangeService;
import org.apache.iotdb.db.mpp.execution.schedule.queue.IndexedBlockingQueue;
import org.apache.iotdb.db.mpp.execution.schedule.queue.IndexedBlockingReserveQueue;
import org.apache.iotdb.db.mpp.execution.schedule.queue.L1PriorityQueue;
import org.apache.iotdb.db.mpp.execution.schedule.queue.multilevelqueue.DriverTaskHandle;
import org.apache.iotdb.db.mpp.execution.schedule.queue.multilevelqueue.MultilevelPriorityQueue;
import org.apache.iotdb.db.mpp.execution.schedule.task.DriverTask;
import org.apache.iotdb.db.mpp.execution.schedule.task.DriverTaskStatus;
import org.apache.iotdb.db.mpp.metric.QueryMetricsManager;
import org.apache.iotdb.db.utils.SetThreadName;
import org.apache.iotdb.mpp.rpc.thrift.TFragmentInstanceId;

import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.SettableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.OptionalInt;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.iotdb.db.mpp.metric.DriverSchedulerMetricSet.BLOCK_QUEUED_TIME;
import static org.apache.iotdb.db.mpp.metric.DriverSchedulerMetricSet.READY_QUEUED_TIME;

/** the manager of fragment instances scheduling */
public class DriverScheduler implements IDriverScheduler, IService {

  private static final Logger logger = LoggerFactory.getLogger(DriverScheduler.class);
  private static final QueryMetricsManager QUERY_METRICS = QueryMetricsManager.getInstance();
  private static final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();

  private static final double LEVEL_TIME_MULTIPLIER = 2;

  public static DriverScheduler getInstance() {
    return InstanceHolder.instance;
  }

  private final IndexedBlockingReserveQueue<DriverTask> readyQueue;
  private final IndexedBlockingQueue<DriverTask> timeoutQueue;
  private final Set<DriverTask> blockedTasks;
  private final Map<QueryId, Map<FragmentInstanceId, Set<DriverTask>>> queryMap;
  private final ITaskScheduler scheduler;

  private final AtomicInteger nextDriverTaskHandleId = new AtomicInteger(0);
  private IMPPDataExchangeManager blockManager;

  private static final int QUERY_MAX_CAPACITY = config.getMaxAllowedConcurrentQueries();
  private static final int WORKER_THREAD_NUM = config.getQueryThreadCount();
  private static final int TASK_MAX_CAPACITY = QUERY_MAX_CAPACITY * config.getDegreeOfParallelism();
  private static final long QUERY_TIMEOUT_MS = config.getQueryTimeoutThreshold();
  private final ThreadGroup workerGroups;
  private final List<AbstractDriverThread> threads;

  private DriverScheduler() {
    this.readyQueue =
        new MultilevelPriorityQueue(LEVEL_TIME_MULTIPLIER, TASK_MAX_CAPACITY, new DriverTask());
    this.timeoutQueue =
        new L1PriorityQueue<>(
            QUERY_MAX_CAPACITY, new DriverTask.TimeoutComparator(), new DriverTask());
    this.queryMap = new ConcurrentHashMap<>();
    this.blockedTasks = Collections.synchronizedSet(new HashSet<>());
    this.scheduler = new Scheduler();
    this.workerGroups = new ThreadGroup("ScheduleThreads");
    this.threads = new ArrayList<>();
    this.blockManager = MPPDataExchangeService.getInstance().getMPPDataExchangeManager();
  }

  @Override
  public void start() throws StartupException {
    for (int i = 0; i < WORKER_THREAD_NUM; i++) {
      int index = i;
      String threadName = "Query-Worker-Thread-" + i;
      ThreadProducer producer =
          new ThreadProducer() {
            @Override
            public void produce(
                String threadName,
                ThreadGroup workerGroups,
                IndexedBlockingQueue<DriverTask> queue,
                ThreadProducer producer) {
              DriverTaskThread newThread =
                  new DriverTaskThread(threadName, workerGroups, readyQueue, scheduler, this);
              threads.set(index, newThread);
              newThread.start();
            }
          };
      AbstractDriverThread t =
          new DriverTaskThread(threadName, workerGroups, readyQueue, scheduler, producer);
      threads.add(t);
      t.start();
    }

    String threadName = "Query-Sentinel-Thread";
    ThreadProducer producer =
        new ThreadProducer() {
          @Override
          public void produce(
              String threadName,
              ThreadGroup workerGroups,
              IndexedBlockingQueue<DriverTask> queue,
              ThreadProducer producer) {
            DriverTaskTimeoutSentinelThread newThread =
                new DriverTaskTimeoutSentinelThread(
                    threadName, workerGroups, timeoutQueue, scheduler, this);
            threads.set(WORKER_THREAD_NUM, newThread);
            newThread.start();
          }
        };

    AbstractDriverThread t =
        new DriverTaskTimeoutSentinelThread(
            threadName, workerGroups, timeoutQueue, scheduler, producer);
    threads.add(t);
    t.start();
  }

  @Override
  public void stop() {
    this.threads.forEach(
        t -> {
          try {
            t.close();
          } catch (IOException e) {
            // Only a field is set, there's no chance to throw an IOException
          }
        });
  }

  @Override
  public ServiceType getID() {
    return ServiceType.FRAGMENT_INSTANCE_MANAGER_SERVICE;
  }

  @Override
  public void submitDrivers(QueryId queryId, List<IDriver> drivers, long timeOut) {
    DriverTaskHandle driverTaskHandle =
        new DriverTaskHandle(
            getNextDriverTaskHandleId(),
            (MultilevelPriorityQueue) readyQueue,
            OptionalInt.of(Integer.MAX_VALUE));
    List<DriverTask> tasks = new ArrayList<>();
    drivers.forEach(
        driver ->
            tasks.add(
                new DriverTask(
                    driver,
                    timeOut > 0 ? timeOut : QUERY_TIMEOUT_MS,
                    DriverTaskStatus.READY,
                    driverTaskHandle)));

    List<DriverTask> submittedTasks = new ArrayList<>();
    for (DriverTask task : tasks) {
      IDriver driver = task.getDriver();
      if (driver.getDependencyDriverIndex() != -1) {
        SettableFuture<?> blockedDependencyFuture =
            tasks.get(driver.getDependencyDriverIndex()).getBlockedDependencyDriver();
        blockedDependencyFuture.addListener(
            () -> {
              // Only if query is alive, we can submit this task
              queryMap.computeIfPresent(
                  queryId,
                  (k1, queryRelatedTasks) -> {
                    queryRelatedTasks.computeIfPresent(
                        task.getDriverTaskId().getFragmentInstanceId(),
                        (k2, instanceRelatedTasks) -> {
                          instanceRelatedTasks.add(task);
                          submitTaskToReadyQueue(task);
                          return instanceRelatedTasks;
                        });
                    return queryRelatedTasks;
                  });
            },
            MoreExecutors.directExecutor());
      } else {
        submittedTasks.add(task);
      }
    }

    for (DriverTask task : submittedTasks) {
      registerTaskToQueryMap(queryId, task);
    }
    for (DriverTask task : submittedTasks) {
      submitTaskToReadyQueue(task);
    }
  }

  public void registerTaskToQueryMap(QueryId queryId, DriverTask driverTask) {
    // If query has not been registered by other fragment instances,
    // add the first task as timeout checking task to timeoutQueue.
    queryMap
        .computeIfAbsent(
            queryId,
            v -> {
              timeoutQueue.push(driverTask);
              return new ConcurrentHashMap<>();
            })
        .computeIfAbsent(
            driverTask.getDriverTaskId().getFragmentInstanceId(),
            v -> Collections.synchronizedSet(new HashSet<>()))
        .add(driverTask);
  }

  public void submitTaskToReadyQueue(DriverTask task) {
    task.lock();
    try {
      if (task.getStatus() != DriverTaskStatus.READY) {
        return;
      }
      readyQueue.push(task);
      task.setLastEnterReadyQueueTime(System.nanoTime());
    } finally {
      task.unlock();
    }
  }

  @Override
  public void abortQuery(QueryId queryId) {
    Map<FragmentInstanceId, Set<DriverTask>> queryRelatedTasks = queryMap.remove(queryId);
    if (queryRelatedTasks != null) {
      for (Set<DriverTask> fragmentRelatedTasks : queryRelatedTasks.values()) {
        if (fragmentRelatedTasks != null) {
          for (DriverTask task : fragmentRelatedTasks) {
            task.lock();
            try {
              task.setAbortCause(DriverTaskAbortedException.BY_QUERY_CASCADING_ABORTED);
              clearDriverTask(task);
            } finally {
              task.unlock();
            }
          }
        }
      }
    }
  }

  @Override
  public void abortFragmentInstance(FragmentInstanceId instanceId) {
    Set<DriverTask> instanceRelatedTasks = queryMap.get(instanceId.getQueryId()).remove(instanceId);
    if (instanceRelatedTasks != null) {
      for (DriverTask task : instanceRelatedTasks) {
        if (task == null) {
          return;
        }
        task.lock();
        try {
          task.setAbortCause(DriverTaskAbortedException.BY_FRAGMENT_ABORT_CALLED);
          clearDriverTask(task);
        } finally {
          task.unlock();
        }
      }
    }
  }

  private void clearDriverTask(DriverTask task) {
    try (SetThreadName driverTaskName =
        new SetThreadName(task.getDriver().getDriverTaskId().getFullId())) {
      DriverTaskStatus status = task.getStatus();
      switch (status) {
          // If it has been aborted, return directly
        case ABORTED:
          return;
        case READY:
          task.setStatus(DriverTaskStatus.ABORTED);
          readyQueue.remove(task.getDriverTaskId());
          break;
        case BLOCKED:
          task.setStatus(DriverTaskStatus.ABORTED);
          blockedTasks.remove(task);
          readyQueue.decreaseReservedSize();
          break;
        case RUNNING:
          task.setStatus(DriverTaskStatus.ABORTED);
          readyQueue.decreaseReservedSize();
          break;
        case FINISHED:
          readyQueue.decreaseReservedSize();
          break;
      }

      timeoutQueue.remove(task.getDriverTaskId());
      Map<FragmentInstanceId, Set<DriverTask>> queryRelatedTasks =
          queryMap.get(task.getDriverTaskId().getQueryId());
      if (queryRelatedTasks != null) {
        Set<DriverTask> instanceRelatedTasks =
            queryRelatedTasks.get(task.getDriverTaskId().getFragmentInstanceId());
        if (instanceRelatedTasks != null) {
          instanceRelatedTasks.remove(task);
          if (instanceRelatedTasks.isEmpty()) {
            queryRelatedTasks.remove(task.getDriverTaskId().getFragmentInstanceId());
          }
        }
        if (queryRelatedTasks.isEmpty()) {
          queryMap.remove(task.getDriverTaskId().getQueryId());
        }
      }
      if (task.getAbortCause() != null) {
        try {
          task.getDriver()
              .failed(
                  new DriverTaskAbortedException(
                      task.getDriver().getDriverTaskId().getFullId(), task.getAbortCause()));
        } catch (Exception e) {
          logger.error("Clear DriverTask failed", e);
        }
      }
      if (task.getStatus() == DriverTaskStatus.ABORTED) {
        try {
          blockManager.forceDeregisterFragmentInstance(
              new TFragmentInstanceId(
                  task.getDriverTaskId().getQueryId().getId(),
                  task.getDriverTaskId().getFragmentId().getId(),
                  task.getDriverTaskId().getFragmentInstanceId().getInstanceId()));
        } catch (Exception e) {
          logger.error("Clear DriverTask failed", e);
        }
      }
    }
  }

  private int getNextDriverTaskHandleId() {
    return nextDriverTaskHandleId.getAndIncrement();
  }

  ITaskScheduler getScheduler() {
    return scheduler;
  }

  public long getReadyQueueTaskCount() {
    return readyQueue.size();
  }

  public long getBlockQueueTaskCount() {
    return blockedTasks.size();
  }

  @TestOnly
  IndexedBlockingQueue<DriverTask> getReadyQueue() {
    return readyQueue;
  }

  @TestOnly
  IndexedBlockingQueue<DriverTask> getTimeoutQueue() {
    return timeoutQueue;
  }

  @TestOnly
  Set<DriverTask> getBlockedTasks() {
    return blockedTasks;
  }

  @TestOnly
  Map<QueryId, Map<FragmentInstanceId, Set<DriverTask>>> getQueryMap() {
    return queryMap;
  }

  @TestOnly
  void setBlockManager(IMPPDataExchangeManager blockManager) {
    this.blockManager = blockManager;
  }

  private static class InstanceHolder {

    private InstanceHolder() {}

    private static final DriverScheduler instance = new DriverScheduler();
  }
  /** the default scheduler implementation */
  private class Scheduler implements ITaskScheduler {
    @Override
    public void blockedToReady(DriverTask task) {
      task.lock();
      try {
        if (task.getStatus() != DriverTaskStatus.BLOCKED) {
          return;
        }

        task.setStatus(DriverTaskStatus.READY);
        QUERY_METRICS.recordTaskQueueTime(
            BLOCK_QUEUED_TIME, System.nanoTime() - task.getLastEnterBlockQueueTime());
        task.setLastEnterReadyQueueTime(System.nanoTime());
        task.resetLevelScheduledTime();
        readyQueue.repush(task);
        blockedTasks.remove(task);
      } finally {
        task.unlock();
      }
    }

    @Override
    public boolean readyToRunning(DriverTask task) {
      task.lock();
      try {
        if (task.getStatus() != DriverTaskStatus.READY) {
          return false;
        }

        task.setStatus(DriverTaskStatus.RUNNING);
        QUERY_METRICS.recordTaskQueueTime(
            READY_QUEUED_TIME, System.nanoTime() - task.getLastEnterReadyQueueTime());
      } finally {
        task.unlock();
      }
      return true;
    }

    @Override
    public void runningToReady(DriverTask task, ExecutionContext context) {
      task.lock();
      try {
        if (task.getStatus() != DriverTaskStatus.RUNNING) {
          return;
        }
        task.updateSchedulePriority(context);
        task.setStatus(DriverTaskStatus.READY);
        task.setLastEnterReadyQueueTime(System.nanoTime());
        readyQueue.repush(task);
      } finally {
        task.unlock();
      }
    }

    @Override
    public void runningToBlocked(DriverTask task, ExecutionContext context) {
      task.lock();
      try {
        if (task.getStatus() != DriverTaskStatus.RUNNING) {
          return;
        }
        task.updateSchedulePriority(context);
        task.setStatus(DriverTaskStatus.BLOCKED);
        task.setLastEnterBlockQueueTime(System.nanoTime());
        blockedTasks.add(task);
      } finally {
        task.unlock();
      }
    }

    @Override
    public void runningToFinished(DriverTask task, ExecutionContext context) {
      task.lock();
      try {
        if (task.getStatus() != DriverTaskStatus.RUNNING) {
          return;
        }
        task.updateSchedulePriority(context);
        task.setStatus(DriverTaskStatus.FINISHED);
      } finally {
        task.unlock();
      }
      // Dependency driver must be submitted before this task is cleared
      task.submitDependencyDriver();
      task.lock();
      try {
        clearDriverTask(task);
      } finally {
        task.unlock();
      }
    }

    @Override
    public void toAborted(DriverTask task) {
      try (SetThreadName driverTaskName =
          new SetThreadName(task.getDriver().getDriverTaskId().getFullId())) {
        task.lock();
        try {
          // If a task is already in an end state, it indicates that the task is finalized in other
          // threads.
          if (task.isEndState()) {
            return;
          }
          logger.warn(
              "The task {} is aborted. All other tasks in the same query will be cancelled",
              task.getDriverTaskId());
          clearDriverTask(task);
        } finally {
          task.unlock();
        }
        QueryId queryId = task.getDriverTaskId().getQueryId();
        Map<FragmentInstanceId, Set<DriverTask>> queryRelatedTasks = queryMap.remove(queryId);
        if (queryRelatedTasks != null) {
          for (Set<DriverTask> fragmentRelatedTasks : queryRelatedTasks.values()) {
            if (fragmentRelatedTasks != null) {
              synchronized (fragmentRelatedTasks) {
                for (DriverTask otherTask : fragmentRelatedTasks) {
                  if (task.equals(otherTask)) {
                    continue;
                  }
                  otherTask.lock();
                  try {
                    otherTask.setAbortCause(DriverTaskAbortedException.BY_QUERY_CASCADING_ABORTED);
                    clearDriverTask(otherTask);
                  } finally {
                    otherTask.unlock();
                  }
                }
              }
            }
          }
        }
      }
    }
  }
}
