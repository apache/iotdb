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

package org.apache.iotdb.db.pipe.core.connector.manager;

import org.apache.iotdb.db.pipe.execution.executor.PipeConnectorSubtaskExecutor;
import org.apache.iotdb.db.pipe.task.queue.ListenableBlockingPendingQueue;
import org.apache.iotdb.db.pipe.task.subtask.PipeConnectorSubtask;
import org.apache.iotdb.pipe.api.event.Event;

public class PipeConnectorSubtaskLifeCycle implements AutoCloseable {

  private final PipeConnectorSubtaskExecutor executor;
  private final PipeConnectorSubtask subtask;
  private final ListenableBlockingPendingQueue<Event> pendingQueue;

  private int runningTaskCount;
  private int aliveTaskCount;

  public PipeConnectorSubtaskLifeCycle(
      PipeConnectorSubtaskExecutor executor,
      PipeConnectorSubtask subtask,
      ListenableBlockingPendingQueue<Event> pendingQueue) {
    this.executor = executor;
    this.subtask = subtask;
    this.pendingQueue = pendingQueue;

    pendingQueue.registerEmptyToNotEmptyListener(
        subtask.getTaskID(),
        () -> {
          if (hasRunningTasks()) {
            executor.start(subtask.getTaskID());
          }
        });
    this.pendingQueue.registerNotEmptyToEmptyListener(
        subtask.getTaskID(), () -> executor.stop(subtask.getTaskID()));

    runningTaskCount = 0;
    aliveTaskCount = 0;
  }

  public PipeConnectorSubtask getSubtask() {
    return subtask;
  }

  public ListenableBlockingPendingQueue<Event> getPendingQueue() {
    return pendingQueue;
  }

  public synchronized void register() {
    if (aliveTaskCount < 0) {
      throw new IllegalStateException("aliveTaskCount < 0");
    }
    if (aliveTaskCount == 0) {
      executor.register(subtask);
      runningTaskCount = 0;
    }
    aliveTaskCount++;
  }

  /**
   * @return true if the subtask is out of life cycle, indicating that the subtask should never be
   *     used again
   */
  public synchronized boolean deregister() {
    if (aliveTaskCount <= 0) {
      throw new IllegalStateException("aliveTaskCount <= 0");
    }
    if (aliveTaskCount == 1) {
      close();
      // this subtask is out of life cycle, should never be used again
      return true;
    }
    aliveTaskCount--;
    return false;
  }

  public synchronized void start() {
    if (runningTaskCount < 0) {
      throw new IllegalStateException("runningTaskCount < 0");
    }
    if (runningTaskCount == 0) {
      executor.start(subtask.getTaskID());
    }
    runningTaskCount++;
  }

  public synchronized void stop() {
    if (runningTaskCount <= 0) {
      throw new IllegalStateException("runningTaskCount <= 0");
    }
    if (runningTaskCount == 1) {
      executor.stop(subtask.getTaskID());
    }
    runningTaskCount--;
  }

  @Override
  public synchronized void close() {
    pendingQueue.removeEmptyToNotEmptyListener(subtask.getTaskID());
    pendingQueue.removeNotEmptyToEmptyListener(subtask.getTaskID());

    executor.deregister(subtask.getTaskID());
  }

  private synchronized boolean hasRunningTasks() {
    return runningTaskCount > 0;
  }
}
