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

import org.apache.iotdb.commons.pipe.config.PipeConfig;
import org.apache.iotdb.commons.pipe.plugin.builtin.BuiltinPipePlugin;
import org.apache.iotdb.db.pipe.agent.PipeAgent;
import org.apache.iotdb.db.pipe.config.PipeConnectorConstant;
import org.apache.iotdb.db.pipe.core.connector.impl.iotdb.v1.IoTDBThriftConnectorV1;
import org.apache.iotdb.db.pipe.execution.executor.PipeConnectorSubtaskExecutor;
import org.apache.iotdb.db.pipe.task.queue.ListenableBoundedBlockingPendingQueue;
import org.apache.iotdb.db.pipe.task.subtask.PipeConnectorSubtask;
import org.apache.iotdb.pipe.api.PipeConnector;
import org.apache.iotdb.pipe.api.customizer.PipeParameterValidator;
import org.apache.iotdb.pipe.api.customizer.PipeParameters;
import org.apache.iotdb.pipe.api.customizer.connector.PipeConnectorRuntimeConfiguration;
import org.apache.iotdb.pipe.api.event.Event;
import org.apache.iotdb.pipe.api.exception.PipeException;
import org.apache.iotdb.pipe.api.exception.PipeManagementException;

import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

public class PipeConnectorSubtaskManager {

  private final Map<String, PipeConnectorSubtaskLifeCycle>
      attributeSortedString2SubtaskLifeCycleMap = new HashMap<>();

  public synchronized String register(
      PipeConnectorSubtaskExecutor executor, PipeParameters pipeConnectorParameters) {
    final String attributeSortedString =
        new TreeMap<>(pipeConnectorParameters.getAttribute()).toString();

    if (!attributeSortedString2SubtaskLifeCycleMap.containsKey(attributeSortedString)) {
      // TODO: construct all PipeConnector with the same reflection method, avoid using if-else
      // 1. construct, validate and customize PipeConnector, and then handshake (create connection)
      // with the target
      final PipeConnector pipeConnector =
          pipeConnectorParameters
                  .getStringOrDefault(
                      PipeConnectorConstant.CONNECTOR_KEY,
                      BuiltinPipePlugin.IOTDB_THRIFT_CONNECTOR.getPipePluginName())
                  .equals(BuiltinPipePlugin.IOTDB_THRIFT_CONNECTOR.getPipePluginName())
              ? new IoTDBThriftConnectorV1()
              : PipeAgent.plugin().reflectConnector(pipeConnectorParameters);
      try {
        pipeConnector.validate(new PipeParameterValidator(pipeConnectorParameters));
        final PipeConnectorRuntimeConfiguration runtimeConfiguration =
            new PipeConnectorRuntimeConfiguration();
        pipeConnector.customize(pipeConnectorParameters, runtimeConfiguration);
        // TODO: use runtimeConfiguration to configure PipeConnector
        pipeConnector.handshake();
      } catch (Exception e) {
        throw new PipeManagementException(
            "Failed to construct PipeConnector, because of " + e.getMessage(), e);
      }

      // 2. construct PipeConnectorSubtaskLifeCycle to manage PipeConnectorSubtask's life cycle
      final ListenableBoundedBlockingPendingQueue<Event> pendingQueue =
          new ListenableBoundedBlockingPendingQueue<>(
              PipeConfig.getInstance().getPipeConnectorPendingQueueSize());
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

  public ListenableBoundedBlockingPendingQueue<Event> getPipeConnectorPendingQueue(
      String attributeSortedString) {
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
