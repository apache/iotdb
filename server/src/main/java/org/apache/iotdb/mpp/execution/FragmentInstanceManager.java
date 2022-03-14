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
package org.apache.iotdb.mpp.execution;

import org.apache.iotdb.db.exception.StartupException;
import org.apache.iotdb.db.service.IService;
import org.apache.iotdb.db.service.ServiceType;
import org.apache.iotdb.mpp.execution.queue.IndexedBlockingQueue;
import org.apache.iotdb.mpp.execution.queue.L1PriorityQueue;
import org.apache.iotdb.mpp.execution.queue.L2PriorityQueue;
import org.apache.iotdb.mpp.execution.task.FragmentInstanceID;
import org.apache.iotdb.mpp.execution.task.FragmentInstanceTask;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/** the manager of fragment instances scheduling */
public class FragmentInstanceManager implements IFragmentInstanceManager, IService {

  public static IFragmentInstanceManager getInstance() {
    return InstanceHolder.instance;
  }

  private final IndexedBlockingQueue<FragmentInstanceTask> readyQueue;
  private final IndexedBlockingQueue<FragmentInstanceTask> timeoutQueue;
  private final Map<String, List<FragmentInstanceID>> queryMap;

  private static final int MAX_CAPACITY = 1000; // TODO: load from config files
  private static final int WORKER_THREAD_NUM = 4; // TODO: load from config files
  private final ThreadGroup workerGroups = new ThreadGroup("ScheduleThreads");

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
  }

  @Override
  public void start() throws StartupException {
    for (int i = 0; i < WORKER_THREAD_NUM; i++) {
      new FragmentInstanceTaskExecutor("Worker-Thread-" + i, workerGroups, readyQueue).start();
    }
    new FragmentInstanceTimeoutSentinel("Sentinel-Thread", workerGroups, timeoutQueue).start();
  }

  @Override
  public void stop() {
    workerGroups.interrupt();
  }

  @Override
  public ServiceType getID() {
    return ServiceType.FRAGMENT_INSTANCE_MANAGER_SERVICE;
  }

  @Override
  public void submitFragmentInstance() {}

  @Override
  public void inputBlockAvailable(
      FragmentInstanceID instanceID, FragmentInstanceID upstreamInstanceId) {}

  @Override
  public void outputBlockAvailable(
      FragmentInstanceID instanceID, FragmentInstanceID downstreamInstanceId) {}

  @Override
  public void abortQuery(String queryId) {}

  private static class InstanceHolder {

    private InstanceHolder() {}

    private static final IFragmentInstanceManager instance = new FragmentInstanceManager();
  }
}
