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
import org.apache.iotdb.db.mpp.common.QueryId;
import org.apache.iotdb.db.mpp.execution.ExecFragmentInstance;
import org.apache.iotdb.db.mpp.schedule.queue.IndexedBlockingQueue;
import org.apache.iotdb.db.mpp.schedule.queue.L1PriorityQueue;
import org.apache.iotdb.db.mpp.schedule.queue.L2PriorityQueue;
import org.apache.iotdb.db.mpp.schedule.task.FragmentInstanceTask;
import org.apache.iotdb.db.mpp.schedule.task.FragmentInstanceTaskStatus;

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
public class FragmentInstanceManager implements IFragmentInstanceManager, IService {

  private static final Logger logger = LoggerFactory.getLogger(FragmentInstanceManager.class);

  public static IFragmentInstanceManager getInstance() {
    return InstanceHolder.instance;
  }

  private final IndexedBlockingQueue<FragmentInstanceTask> readyQueue;
  private final IndexedBlockingQueue<FragmentInstanceTask> timeoutQueue;
  private final Set<FragmentInstanceTask> blockedTasks;
  private final Map<QueryId, Set<FragmentInstanceTask>> queryMap;
  private final ITaskScheduler scheduler;

  private static final int MAX_CAPACITY = 1000; // TODO: load from config files
  private static final int WORKER_THREAD_NUM = 4; // TODO: load from config files
  private static final int QUERY_TIMEOUT_MS = 10000; // TODO: load from config files or requests
  private final ThreadGroup workerGroups;
  private final List<AbstractExecutor> threads;

  public FragmentInstanceManager() {
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
    Set<FragmentInstanceTask> tasks =
        instances.stream()
            .map(
                v ->
                    new FragmentInstanceTask(v, QUERY_TIMEOUT_MS, FragmentInstanceTaskStatus.READY))
            .collect(Collectors.toSet());
    queryMap.put(queryId, Collections.synchronizedSet(tasks));
    for (FragmentInstanceTask task : tasks) {
      task.lock();
      try {
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
  public void fetchFragmentInstance(ExecFragmentInstance instance) {}

  private void clearFragmentInstanceTask(FragmentInstanceTask task) {
    if (task.getStatus() != FragmentInstanceTaskStatus.FINISHED) {
      task.setStatus(FragmentInstanceTaskStatus.ABORTED);
    }
    if (task.getStatus() == FragmentInstanceTaskStatus.ABORTED) {
      // TODO: remember to call the implementation
      // IDataBlockManager.forceDeregisterFragmentInstance(task);
    }
    readyQueue.remove(task.getId());
    timeoutQueue.remove(task.getId());
    blockedTasks.remove(task);
    Set<FragmentInstanceTask> tasks = queryMap.get(task.getId().getQueryId());
    tasks.remove(task);
    if (tasks.isEmpty()) {
      queryMap.remove(task.getId().getQueryId());
    }
  }

  private static class InstanceHolder {

    private InstanceHolder() {}

    private static final IFragmentInstanceManager instance = new FragmentInstanceManager();
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
      Set<FragmentInstanceTask> queryRelatedTasks = queryMap.get(task.getId().getQueryId());
      if (queryRelatedTasks != null) {
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
