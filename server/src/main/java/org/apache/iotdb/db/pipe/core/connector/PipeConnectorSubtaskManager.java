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

package org.apache.iotdb.db.pipe.core.connector;

import org.apache.iotdb.db.pipe.agent.PipeAgent;
import org.apache.iotdb.db.pipe.execution.executor.PipeConnectorSubtaskExecutor;
import org.apache.iotdb.db.pipe.task.binder.PendingQueue;
import org.apache.iotdb.db.pipe.task.subtask.PipeConnectorSubtask;
import org.apache.iotdb.pipe.api.PipeConnector;
import org.apache.iotdb.pipe.api.customizer.PipeParameters;
import org.apache.iotdb.pipe.api.exception.PipeException;

import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

public class PipeConnectorSubtaskManager {

  private final Map<String, PipeConnectorSubtaskLifeCycle>
      attributeSortedString2SubtaskLifeCycleMap = new HashMap<>();

  public synchronized String register(
      PipeConnectorSubtaskExecutor executor, PipeParameters connectorAttributes) {
    final String attributeSortedString =
        new TreeMap<>(connectorAttributes.getAttribute()).toString();

    if (!attributeSortedString2SubtaskLifeCycleMap.containsKey(attributeSortedString)) {
      final PipeConnector pipeConnector = PipeAgent.plugin().reflectConnector(connectorAttributes);
      // TODO: make pendingQueue size configurable
      final PendingQueue pendingQueue = new PendingQueue(1024 * 1024);
      final PipeConnectorSubtask pipeConnectorSubtask =
          new PipeConnectorSubtask(attributeSortedString, pendingQueue, pipeConnector);
      final PipeConnectorSubtaskLifeCycle pipeConnectorSubtaskLifeCycle =
          new PipeConnectorSubtaskLifeCycle(executor, pipeConnectorSubtask, pendingQueue);
      attributeSortedString2SubtaskLifeCycleMap.put(
          attributeSortedString, pipeConnectorSubtaskLifeCycle);
    }

    attributeSortedString2SubtaskLifeCycleMap.get(attributeSortedString).register();

    return attributeSortedString;
  }

  public synchronized void deregister(String attributeSortedString) {
    if (!attributeSortedString2SubtaskLifeCycleMap.containsKey(attributeSortedString)) {
      throw new PipeException(
          "Failed to deregister PipeConnectorSubtask. No such subtask: " + attributeSortedString);
    }

    if (attributeSortedString2SubtaskLifeCycleMap.get(attributeSortedString).deregister()) {
      attributeSortedString2SubtaskLifeCycleMap.remove(attributeSortedString);
    }
  }

  public synchronized void start(String attributeSortedString) {
    if (!attributeSortedString2SubtaskLifeCycleMap.containsKey(attributeSortedString)) {
      throw new PipeException(
          "Failed to deregister PipeConnectorSubtask. No such subtask: " + attributeSortedString);
    }

    attributeSortedString2SubtaskLifeCycleMap.get(attributeSortedString).start();
  }

  public synchronized void stop(String attributeSortedString) {
    if (!attributeSortedString2SubtaskLifeCycleMap.containsKey(attributeSortedString)) {
      throw new PipeException(
          "Failed to deregister PipeConnectorSubtask. No such subtask: " + attributeSortedString);
    }

    attributeSortedString2SubtaskLifeCycleMap.get(attributeSortedString).stop();
  }

  public PipeConnectorSubtask getPipeConnectorSubtask(String attributeSortedString) {
    if (!attributeSortedString2SubtaskLifeCycleMap.containsKey(attributeSortedString)) {
      throw new PipeException(
          "Failed to get PipeConnectorSubtask. No such subtask: " + attributeSortedString);
    }

    return attributeSortedString2SubtaskLifeCycleMap.get(attributeSortedString).getSubtask();
  }

  public PendingQueue getPipeConnectorPendingQueue(String attributeSortedString) {
    if (!attributeSortedString2SubtaskLifeCycleMap.containsKey(attributeSortedString)) {
      throw new PipeException(
          "Failed to get PendingQueue. No such subtask: " + attributeSortedString);
    }

    return attributeSortedString2SubtaskLifeCycleMap.get(attributeSortedString).getPendingQueue();
  }

  /////////////////////////  Singleton Instance Holder  /////////////////////////

  private PipeConnectorSubtaskManager() {}

  private static class PipeSubtaskManagerHolder {
    private static final PipeConnectorSubtaskManager INSTANCE = new PipeConnectorSubtaskManager();
  }

  public static PipeConnectorSubtaskManager instance() {
    return PipeSubtaskManagerHolder.INSTANCE;
  }
}
