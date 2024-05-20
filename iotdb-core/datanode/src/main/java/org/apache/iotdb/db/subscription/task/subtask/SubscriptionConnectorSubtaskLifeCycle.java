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

package org.apache.iotdb.db.subscription.task.subtask;

import org.apache.iotdb.commons.pipe.task.connection.UnboundedBlockingPendingQueue;
import org.apache.iotdb.db.pipe.execution.PipeConnectorSubtaskExecutor;
import org.apache.iotdb.db.pipe.task.subtask.connector.PipeConnectorSubtask;
import org.apache.iotdb.db.pipe.task.subtask.connector.PipeConnectorSubtaskLifeCycle;
import org.apache.iotdb.db.subscription.agent.SubscriptionAgent;
import org.apache.iotdb.pipe.api.event.Event;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SubscriptionConnectorSubtaskLifeCycle extends PipeConnectorSubtaskLifeCycle {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(SubscriptionConnectorSubtaskLifeCycle.class);

  private int runningTaskCount;
  private int registeredTaskCount;

  public SubscriptionConnectorSubtaskLifeCycle(
      PipeConnectorSubtaskExecutor executor, // SubscriptionSubtaskExecutor
      PipeConnectorSubtask subtask, // SubscriptionConnectorSubtask
      UnboundedBlockingPendingQueue<Event> pendingQueue) {
    super(executor, subtask, pendingQueue);

    runningTaskCount = 0;
    registeredTaskCount = 0;
  }

  @Override
  public synchronized void register() {
    if (registeredTaskCount < 0) {
      throw new IllegalStateException("registeredTaskCount < 0");
    }

    if (registeredTaskCount == 0) {
      SubscriptionAgent.broker().bindPrefetchingQueue((SubscriptionConnectorSubtask) subtask);
      executor.register(subtask);
      runningTaskCount = 0;
    }

    registeredTaskCount++;
    LOGGER.info(
        "Register subtask {}. runningTaskCount: {}, registeredTaskCount: {}",
        subtask,
        runningTaskCount,
        registeredTaskCount);
  }

  @Override
  public synchronized boolean deregister(String ignored) {
    if (registeredTaskCount <= 0) {
      throw new IllegalStateException("registeredTaskCount <= 0");
    }

    try {
      if (registeredTaskCount > 1) {
        return false;
      }

      close();
      // This subtask is out of life cycle, should never be used again
      return true;
    } finally {
      registeredTaskCount--;
      LOGGER.info(
          "Deregister subtask {}. runningTaskCount: {}, registeredTaskCount: {}",
          subtask,
          runningTaskCount,
          registeredTaskCount);
    }
  }

  @Override
  public synchronized void start() {
    if (runningTaskCount < 0) {
      throw new IllegalStateException("runningTaskCount < 0");
    }

    if (runningTaskCount == 0) {
      executor.start(subtask.getTaskID());
    }

    runningTaskCount++;
    LOGGER.info(
        "Start subtask {}. runningTaskCount: {}, registeredTaskCount: {}",
        subtask,
        runningTaskCount,
        registeredTaskCount);
  }

  @Override
  public synchronized void stop() {
    if (runningTaskCount <= 0) {
      throw new IllegalStateException("runningTaskCount <= 0");
    }

    if (runningTaskCount == 1) {
      executor.stop(subtask.getTaskID());
    }

    runningTaskCount--;
    LOGGER.info(
        "Stop subtask {}. runningTaskCount: {}, registeredTaskCount: {}",
        subtask,
        runningTaskCount,
        registeredTaskCount);
  }

  @Override
  public synchronized void close() {
    executor.deregister(subtask.getTaskID());
    SubscriptionAgent.broker().unbindPrefetchingQueue((SubscriptionConnectorSubtask) subtask);
  }
}
