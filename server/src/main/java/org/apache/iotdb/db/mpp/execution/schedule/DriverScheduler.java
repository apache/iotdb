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
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.mpp.common.FragmentInstanceId;
import org.apache.iotdb.db.mpp.common.QueryId;
import org.apache.iotdb.db.mpp.execution.driver.IDriver;
import org.apache.iotdb.db.mpp.execution.exchange.IMPPDataExchangeManager;
import org.apache.iotdb.db.mpp.execution.exchange.MPPDataExchangeService;
import org.apache.iotdb.db.mpp.execution.schedule.queue.IndexedBlockingQueue;
import org.apache.iotdb.db.mpp.execution.schedule.queue.L1PriorityQueue;
import org.apache.iotdb.db.mpp.execution.schedule.queue.L2PriorityQueue;
import org.apache.iotdb.db.mpp.execution.schedule.task.DriverTask;
import org.apache.iotdb.db.mpp.execution.schedule.task.DriverTaskId;
import org.apache.iotdb.db.mpp.execution.schedule.task.DriverTaskStatus;
import org.apache.iotdb.db.utils.SetThreadName;
import org.apache.iotdb.mpp.rpc.thrift.TFragmentInstanceId;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/** the manager of fragment instances scheduling */
public class DriverScheduler implements IDriverScheduler, IService {

  private static final Logger logger = LoggerFactory.getLogger(DriverScheduler.class);

  public static DriverScheduler getInstance() {
    return InstanceHolder.instance;
  }

  private final IndexedBlockingQueue<DriverTask> readyQueue;
  private final IndexedBlockingQueue<DriverTask> timeoutQueue;
  private final Set<DriverTask> blockedTasks;
  private final Map<QueryId, Map<FragmentInstanceId, Set<DriverTask>>> queryMap;
  private final ITaskScheduler scheduler;
  private IMPPDataExchangeManager blockManager;

  private static final int MAX_CAPACITY =
      IoTDBDescriptor.getInstance().getConfig().getMaxAllowedConcurrentQueries();
  private static final int WORKER_THREAD_NUM =
      IoTDBDescriptor.getInstance().getConfig().getQueryThreadCount();
  private static final long QUERY_TIMEOUT_MS =
      IoTDBDescriptor.getInstance().getConfig().getQueryTimeoutThreshold();
  private final ThreadGroup workerGroups;
  private final List<AbstractDriverThread> threads;

  private DriverScheduler() {
    this.readyQueue =
        new L2PriorityQueue<>(
            MAX_CAPACITY, new DriverTask.SchedulePriorityComparator(), new DriverTask());
    this.timeoutQueue =
        new L1PriorityQueue<>(MAX_CAPACITY, new DriverTask.TimeoutComparator(), new DriverTask());
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
    List<DriverTask> tasks =
        drivers.stream()
            .map(
                v ->
                    new DriverTask(
                        v, timeOut > 0 ? timeOut : QUERY_TIMEOUT_MS, DriverTaskStatus.READY))
            .collect(Collectors.toList());
    for (DriverTask driverTask : tasks) {
      queryMap
          .computeIfAbsent(queryId, v -> new ConcurrentHashMap<>())
          .computeIfAbsent(
              driverTask.getDriverTaskId().getFragmentInstanceId(),
              v -> Collections.synchronizedSet(new HashSet<>()))
          .add(driverTask);
    }
    for (DriverTask task : tasks) {
      task.lock();
      try {
        if (task.getStatus() != DriverTaskStatus.READY) {
          continue;
        }
        timeoutQueue.push(task);
        readyQueue.push(task);
      } finally {
        task.unlock();
      }
    }
  }

  @Override
  public void abortQuery(QueryId queryId) {
    Map<FragmentInstanceId, Set<DriverTask>> queryRelatedTasks = queryMap.remove(queryId);
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
    // help for gc
    queryRelatedTasks = null;
  }

  @Override
  public void abortFragmentInstance(FragmentInstanceId instanceId) {
    Set<DriverTask> instanceRelatedTasks = queryMap.get(instanceId.getQueryId()).remove(instanceId);
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

  @Override
  public double getSchedulePriority(DriverTaskId driverTaskID) {
    DriverTask task = timeoutQueue.get(driverTaskID);
    if (task == null) {
      throw new IllegalStateException(
          "the fragmentInstance " + driverTaskID.getFullId() + " has been cleared");
    }
    return task.getSchedulePriority();
  }

  private void clearDriverTask(DriverTask task) {
    try (SetThreadName driverTaskName =
        new SetThreadName(task.getDriver().getDriverTaskId().getFullId())) {
      if (task.getStatus() != DriverTaskStatus.FINISHED) {
        task.setStatus(DriverTaskStatus.ABORTED);
      }
      readyQueue.remove(task.getDriverTaskId());
      timeoutQueue.remove(task.getDriverTaskId());
      blockedTasks.remove(task);
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

  ITaskScheduler getScheduler() {
    return scheduler;
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
        readyQueue.push(task);
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
        readyQueue.push(task);
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
              task.getDriverTaskId().toString());
          clearDriverTask(task);
        } finally {
          task.unlock();
        }
        QueryId queryId = task.getDriverTaskId().getQueryId();
        Collection<Set<DriverTask>> queryRelatedTasks = queryMap.get(queryId).values();
        for (Set<DriverTask> fragmentRelatedTasks : queryRelatedTasks) {
          if (fragmentRelatedTasks != null) {
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
