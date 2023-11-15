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

package org.apache.iotdb.db.pipe.task.subtask.connector;

import org.apache.iotdb.commons.pipe.config.PipeConfig;
import org.apache.iotdb.db.pipe.agent.PipeAgent;
import org.apache.iotdb.db.pipe.config.constant.PipeConnectorConstant;
import org.apache.iotdb.db.pipe.config.plugin.configuraion.PipeTaskRuntimeConfiguration;
import org.apache.iotdb.db.pipe.execution.executor.PipeConnectorSubtaskExecutor;
import org.apache.iotdb.db.pipe.task.connection.BoundedBlockingPendingQueue;
import org.apache.iotdb.pipe.api.PipeConnector;
import org.apache.iotdb.pipe.api.customizer.configuration.PipeRuntimeEnvironment;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameters;
import org.apache.iotdb.pipe.api.event.Event;
import org.apache.iotdb.pipe.api.exception.PipeException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class PipeConnectorSubtaskManager {

  private static final String FAILED_TO_DEREGISTER_EXCEPTION_MESSAGE =
      "Failed to deregister PipeConnectorSubtask. No such subtask: ";

  private final Map<String, List<PipeConnectorSubtaskLifeCycle>>
      attributeSortedString2SubtaskLifeCycleMap = new HashMap<>();

  public synchronized String register(
      PipeConnectorSubtaskExecutor executor,
      PipeParameters pipeConnectorParameters,
      PipeRuntimeEnvironment pipeRuntimeEnvironment) {
    final String attributeSortedString =
        new TreeMap<>(pipeConnectorParameters.getAttribute()).toString();

    if (!attributeSortedString2SubtaskLifeCycleMap.containsKey(attributeSortedString)) {
      final int connectorNum =
          pipeConnectorParameters.getIntOrDefault(
              Arrays.asList(
                  PipeConnectorConstant.CONNECTOR_IOTDB_PARALLEL_TASKS_KEY,
                  PipeConnectorConstant.SINK_IOTDB_PARALLEL_TASKS_KEY),
              PipeConnectorConstant.CONNECTOR_IOTDB_PARALLEL_TASKS_DEFAULT_VALUE);
      final List<PipeConnectorSubtaskLifeCycle> pipeConnectorSubtaskLifeCycleList =
          new ArrayList<>(connectorNum);

      // Shared pending queue for all subtasks
      final BoundedBlockingPendingQueue<Event> pendingQueue =
          new BoundedBlockingPendingQueue<>(
              PipeConfig.getInstance().getPipeConnectorPendingQueueSize());

      for (int connectorIndex = 0; connectorIndex < connectorNum; connectorIndex++) {
        final PipeConnector pipeConnector =
            PipeAgent.plugin().reflectConnector(pipeConnectorParameters);

        // 1. Construct and customize PipeConnector, and then handshake (create
        // connection) with the target
        try {
          // Skip validation because it has be done in the DataNode scale
          pipeConnector.customize(
              pipeConnectorParameters, new PipeTaskRuntimeConfiguration(pipeRuntimeEnvironment));
          pipeConnector.handshake();
        } catch (Exception e) {
          throw new PipeException(
              "Failed to construct PipeConnector, because of " + e.getMessage(), e);
        }

        // 2. Construct PipeConnectorSubtaskLifeCycle to manage PipeConnectorSubtask's life cycle
        final PipeConnectorSubtask pipeConnectorSubtask =
            new PipeConnectorSubtask(
                String.format(
                    "%s_%s_%s",
                    attributeSortedString,
                    pipeRuntimeEnvironment.getCreationTime(),
                    connectorIndex),
                pipeRuntimeEnvironment.getCreationTime(),
                attributeSortedString,
                connectorIndex,
                pendingQueue,
                pipeConnector);
        final PipeConnectorSubtaskLifeCycle pipeConnectorSubtaskLifeCycle =
            new PipeConnectorSubtaskLifeCycle(executor, pipeConnectorSubtask, pendingQueue);
        pipeConnectorSubtaskLifeCycleList.add(pipeConnectorSubtaskLifeCycle);
      }

      attributeSortedString2SubtaskLifeCycleMap.put(
          attributeSortedString, pipeConnectorSubtaskLifeCycleList);
    }

    for (final PipeConnectorSubtaskLifeCycle lifeCycle :
        attributeSortedString2SubtaskLifeCycleMap.get(attributeSortedString)) {
      lifeCycle.register();
    }

    return attributeSortedString;
  }

  public synchronized void deregister(String attributeSortedString) {
    if (!attributeSortedString2SubtaskLifeCycleMap.containsKey(attributeSortedString)) {
      throw new PipeException(FAILED_TO_DEREGISTER_EXCEPTION_MESSAGE + attributeSortedString);
    }

    final List<PipeConnectorSubtaskLifeCycle> lifeCycles =
        attributeSortedString2SubtaskLifeCycleMap.get(attributeSortedString);
    lifeCycles.removeIf(PipeConnectorSubtaskLifeCycle::deregister);

    if (lifeCycles.isEmpty()) {
      attributeSortedString2SubtaskLifeCycleMap.remove(attributeSortedString);
    }
  }

  public synchronized void start(String attributeSortedString) {
    if (!attributeSortedString2SubtaskLifeCycleMap.containsKey(attributeSortedString)) {
      throw new PipeException(FAILED_TO_DEREGISTER_EXCEPTION_MESSAGE + attributeSortedString);
    }

    for (final PipeConnectorSubtaskLifeCycle lifeCycle :
        attributeSortedString2SubtaskLifeCycleMap.get(attributeSortedString)) {
      lifeCycle.start();
    }
  }

  public synchronized void stop(String attributeSortedString) {
    if (!attributeSortedString2SubtaskLifeCycleMap.containsKey(attributeSortedString)) {
      throw new PipeException(FAILED_TO_DEREGISTER_EXCEPTION_MESSAGE + attributeSortedString);
    }

    for (final PipeConnectorSubtaskLifeCycle lifeCycle :
        attributeSortedString2SubtaskLifeCycleMap.get(attributeSortedString)) {
      lifeCycle.stop();
    }
  }

  public BoundedBlockingPendingQueue<Event> getPipeConnectorPendingQueue(
      String attributeSortedString) {
    if (!attributeSortedString2SubtaskLifeCycleMap.containsKey(attributeSortedString)) {
      throw new PipeException(
          "Failed to get PendingQueue. No such subtask: " + attributeSortedString);
    }

    // All subtasks share the same pending queue
    return attributeSortedString2SubtaskLifeCycleMap
        .get(attributeSortedString)
        .get(0)
        .getPendingQueue();
  }

  /////////////////////////  Singleton Instance Holder  /////////////////////////

  private PipeConnectorSubtaskManager() {
    // Do nothing
  }

  private static class PipeSubtaskManagerHolder {
    private static final PipeConnectorSubtaskManager INSTANCE = new PipeConnectorSubtaskManager();
  }

  public static PipeConnectorSubtaskManager instance() {
    return PipeSubtaskManagerHolder.INSTANCE;
  }
}
