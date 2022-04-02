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
package org.apache.iotdb.db.mpp.schedule;

import org.apache.iotdb.commons.exception.StartupException;
import org.apache.iotdb.commons.service.IService;
import org.apache.iotdb.commons.service.ServiceType;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.db.mpp.buffer.IDataBlockManager;
import org.apache.iotdb.db.mpp.common.FragmentInstanceId;
import org.apache.iotdb.db.mpp.common.QueryId;
import org.apache.iotdb.db.mpp.execution.ExecFragmentInstance;
import org.apache.iotdb.db.mpp.schedule.queue.IndexedBlockingQueue;
import org.apache.iotdb.db.mpp.schedule.queue.L1PriorityQueue;
import org.apache.iotdb.db.mpp.schedule.queue.L2PriorityQueue;
import org.apache.iotdb.db.mpp.schedule.task.FragmentInstanceTask;
import org.apache.iotdb.db.mpp.schedule.task.FragmentInstanceTaskID;
import org.apache.iotdb.db.mpp.schedule.task.FragmentInstanceTaskStatus;
import org.apache.iotdb.mpp.rpc.thrift.InternalService;
import org.apache.iotdb.mpp.rpc.thrift.TCancelQueryReq;
import org.apache.iotdb.mpp.rpc.thrift.TFragmentInstanceId;

import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/** the manager of fragment instances scheduling */
public class FragmentInstanceScheduler implements IFragmentInstanceScheduler, IService {

  private static final Logger logger = LoggerFactory.getLogger(FragmentInstanceScheduler.class);

  public static FragmentInstanceScheduler getInstance() {
    return InstanceHolder.instance;
  }

  private final IndexedBlockingQueue<FragmentInstanceTask> readyQueue;
  private final IndexedBlockingQueue<FragmentInstanceTask> timeoutQueue;
  private final Set<FragmentInstanceTask> blockedTasks;
  private final Map<QueryId, Set<FragmentInstanceTask>> queryMap;
  private final ITaskScheduler scheduler;
  private IDataBlockManager blockManager; // TODO: init with real IDataBlockManager

  private static final int MAX_CAPACITY = 1000; // TODO: load from config files
  private static final int WORKER_THREAD_NUM = 4; // TODO: load from config files
  private static final int QUERY_TIMEOUT_MS = 10000; // TODO: load from config files or requests
  private final ThreadGroup workerGroups;
  private InternalService.Client mppServiceClient; // TODO: use from client pool
  private final List<AbstractExecutor> threads;

  private FragmentInstanceScheduler() {
    this.readyQueue =
        new L2PriorityQueue<>(
            MAX_CAPACITY,
            new FragmentInstanceTask.SchedulePriorityComparator(),
            new FragmentInstanceTask());
    this.timeoutQueue =
        new L1PriorityQueue<>(
            MAX_CAPACITY,
            new FragmentInstanceTask.SchedulePriorityComparator(),
            new FragmentInstanceTask());
    this.queryMap = new ConcurrentHashMap<>();
    this.blockedTasks = Collections.synchronizedSet(new HashSet<>());
    this.scheduler = new Scheduler();
    this.workerGroups = new ThreadGroup("ScheduleThreads");
    this.threads = new ArrayList<>();
  }

  @Override
  public void start() throws StartupException {
    for (int i = 0; i < WORKER_THREAD_NUM; i++) {
      AbstractExecutor t =
          new FragmentInstanceTaskExecutor(
              "Worker-Thread-" + i, workerGroups, readyQueue, scheduler);
      threads.add(t);
      t.start();
    }
    AbstractExecutor t =
        new FragmentInstanceTimeoutSentinel(
            "Sentinel-Thread", workerGroups, timeoutQueue, scheduler);
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
  public void submitFragmentInstances(QueryId queryId, List<ExecFragmentInstance> instances) {
    List<FragmentInstanceTask> tasks =
        instances.stream()
            .map(
                v ->
                    new FragmentInstanceTask(v, QUERY_TIMEOUT_MS, FragmentInstanceTaskStatus.READY))
            .collect(Collectors.toList());
    queryMap
        .computeIfAbsent(queryId, v -> Collections.synchronizedSet(new HashSet<>()))
        .addAll(tasks);
    for (FragmentInstanceTask task : tasks) {
      task.lock();
      try {
        if (task.getStatus() != FragmentInstanceTaskStatus.READY) {
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
    Set<FragmentInstanceTask> queryRelatedTasks = queryMap.remove(queryId);
    if (queryRelatedTasks != null) {
      for (FragmentInstanceTask task : queryRelatedTasks) {
        task.lock();
        try {
          clearFragmentInstanceTask(task);
        } finally {
          task.unlock();
        }
      }
    }
  }

  @Override
  public void abortFragmentInstance(FragmentInstanceId instanceId) {
    // TODO(EricPai)
  }

  @Override
  public void fetchFragmentInstance(ExecFragmentInstance instance) {}

  @Override
  public double getSchedulePriority(FragmentInstanceId instanceId) {
    FragmentInstanceTask task = timeoutQueue.get(new FragmentInstanceTaskID(instanceId));
    if (task == null) {
      throw new IllegalStateException(
          "the fragmentInstance " + instanceId.getFullId() + " has been cleared");
    }
    return task.getSchedulePriority();
  }

  private void clearFragmentInstanceTask(FragmentInstanceTask task) {
    if (task.getStatus() != FragmentInstanceTaskStatus.FINISHED) {
      task.setStatus(FragmentInstanceTaskStatus.ABORTED);
    }
    if (task.getStatus() == FragmentInstanceTaskStatus.ABORTED) {
      blockManager.forceDeregisterFragmentInstance(
          new TFragmentInstanceId(
              task.getId().getQueryId().getId(),
              String.valueOf(task.getId().getFragmentId().getId()),
              task.getId().getInstanceId()));
    }
    readyQueue.remove(task.getId());
    timeoutQueue.remove(task.getId());
    blockedTasks.remove(task);
    Set<FragmentInstanceTask> tasks = queryMap.get(task.getId().getQueryId());
    if (tasks != null) {
      tasks.remove(task);
      if (tasks.isEmpty()) {
        queryMap.remove(task.getId().getQueryId());
      }
    }
  }

  ITaskScheduler getScheduler() {
    return scheduler;
  }

  @TestOnly
  IndexedBlockingQueue<FragmentInstanceTask> getReadyQueue() {
    return readyQueue;
  }

  @TestOnly
  IndexedBlockingQueue<FragmentInstanceTask> getTimeoutQueue() {
    return timeoutQueue;
  }

  @TestOnly
  Set<FragmentInstanceTask> getBlockedTasks() {
    return blockedTasks;
  }

  @TestOnly
  Map<QueryId, Set<FragmentInstanceTask>> getQueryMap() {
    return queryMap;
  }

  @TestOnly
  void setBlockManager(IDataBlockManager blockManager) {
    this.blockManager = blockManager;
  }

  @TestOnly
  void setMppServiceClient(InternalService.Client client) {
    this.mppServiceClient = client;
  }

  private static class InstanceHolder {

    private InstanceHolder() {}

    private static final FragmentInstanceScheduler instance = new FragmentInstanceScheduler();
  }
  /** the default scheduler implementation */
  private class Scheduler implements ITaskScheduler {
    @Override
    public void blockedToReady(FragmentInstanceTask task) {
      task.lock();
      try {
        if (task.getStatus() != FragmentInstanceTaskStatus.BLOCKED) {
          return;
        }
        task.setStatus(FragmentInstanceTaskStatus.READY);
        readyQueue.push(task);
        blockedTasks.remove(task);
      } finally {
        task.unlock();
      }
    }

    @Override
    public boolean readyToRunning(FragmentInstanceTask task) {
      task.lock();
      try {
        if (task.getStatus() != FragmentInstanceTaskStatus.READY) {
          return false;
        }
        task.setStatus(FragmentInstanceTaskStatus.RUNNING);
      } finally {
        task.unlock();
      }
      return true;
    }

    @Override
    public void runningToReady(FragmentInstanceTask task, ExecutionContext context) {
      task.lock();
      try {
        if (task.getStatus() != FragmentInstanceTaskStatus.RUNNING) {
          return;
        }
        task.updateSchedulePriority(context);
        task.setStatus(FragmentInstanceTaskStatus.READY);
        readyQueue.push(task);
      } finally {
        task.unlock();
      }
    }

    @Override
    public void runningToBlocked(FragmentInstanceTask task, ExecutionContext context) {
      task.lock();
      try {
        if (task.getStatus() != FragmentInstanceTaskStatus.RUNNING) {
          return;
        }
        task.updateSchedulePriority(context);
        task.setStatus(FragmentInstanceTaskStatus.BLOCKED);
        blockedTasks.add(task);
      } finally {
        task.unlock();
      }
    }

    @Override
    public void runningToFinished(FragmentInstanceTask task, ExecutionContext context) {
      task.lock();
      try {
        if (task.getStatus() != FragmentInstanceTaskStatus.RUNNING) {
          return;
        }
        task.updateSchedulePriority(context);
        task.setStatus(FragmentInstanceTaskStatus.FINISHED);
        clearFragmentInstanceTask(task);
      } finally {
        task.unlock();
      }
    }

    @Override
    public void toAborted(FragmentInstanceTask task) {
      task.lock();
      try {
        // If a task is already in an end state, it indicates that the task is finalized in other
        // threads.
        if (task.isEndState()) {
          return;
        }
        logger.warn(
            "The task {} is aborted. All other tasks in the same query will be cancelled",
            task.getId().toString());
        clearFragmentInstanceTask(task);
      } finally {
        task.unlock();
      }
      QueryId queryId = task.getId().getQueryId();
      Set<FragmentInstanceTask> queryRelatedTasks = queryMap.remove(queryId);
      if (queryRelatedTasks != null) {
        try {
          mppServiceClient.cancelQuery(new TCancelQueryReq(queryId.getId()));
        } catch (TException e) {
          // If coordinator cancel query failed, we should continue clean other tasks.
          logger.error("cancel query " + queryId.getId() + " failed", e);
        }
        for (FragmentInstanceTask otherTask : queryRelatedTasks) {
          if (task.equals(otherTask)) {
            continue;
          }
          otherTask.lock();
          try {
            clearFragmentInstanceTask(otherTask);
          } finally {
            otherTask.unlock();
          }
        }
      }
    }
  }
}
