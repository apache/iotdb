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
import org.apache.iotdb.commons.pipe.plugin.builtin.BuiltinPipePlugin;
import org.apache.iotdb.db.pipe.agent.PipeAgent;
import org.apache.iotdb.db.pipe.config.constant.PipeConnectorConstant;
import org.apache.iotdb.db.pipe.config.plugin.configuraion.PipeTaskRuntimeConfiguration;
import org.apache.iotdb.db.pipe.connector.protocol.airgap.IoTDBAirGapConnector;
import org.apache.iotdb.db.pipe.connector.protocol.legacy.IoTDBLegacyPipeConnector;
import org.apache.iotdb.db.pipe.connector.protocol.opcua.OpcUaConnector;
import org.apache.iotdb.db.pipe.connector.protocol.thrift.async.IoTDBThriftAsyncConnector;
import org.apache.iotdb.db.pipe.connector.protocol.thrift.sync.IoTDBThriftSyncConnector;
import org.apache.iotdb.db.pipe.connector.protocol.websocket.WebSocketConnector;
import org.apache.iotdb.db.pipe.execution.executor.PipeConnectorSubtaskExecutor;
import org.apache.iotdb.db.pipe.task.connection.BoundedBlockingPendingQueue;
import org.apache.iotdb.pipe.api.PipeConnector;
import org.apache.iotdb.pipe.api.customizer.configuration.PipeRuntimeEnvironment;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameterValidator;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameters;
import org.apache.iotdb.pipe.api.event.Event;
import org.apache.iotdb.pipe.api.exception.PipeException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

public class PipeConnectorSubtaskManager {

  private static final String FAILED_TO_DEREGISTER_EXCEPTION_MESSAGE =
      "Failed to deregister PipeConnectorSubtask. No such subtask: ";

  private final Map<String, ArrayList<PipeConnectorSubtaskLifeCycle>>
      attributeSortedString2SubtaskLifeCycleMap = new HashMap<>();

  public synchronized String register(
      PipeConnectorSubtaskExecutor executor,
      PipeParameters pipeConnectorParameters,
      PipeRuntimeEnvironment pipeRuntimeEnvironment) {
    final String attributeSortedString =
        new TreeMap<>(pipeConnectorParameters.getAttribute()).toString();

    if (!attributeSortedString2SubtaskLifeCycleMap.containsKey(attributeSortedString)) {
      // 1. Construct, validate and customize PipeConnector, and then handshake (create connection)
      // with the target
      final String connectorKey =
          pipeConnectorParameters.getStringOrDefault(
              PipeConnectorConstant.CONNECTOR_KEY,
              BuiltinPipePlugin.IOTDB_THRIFT_CONNECTOR.getPipePluginName());

      final int connectorNum =
          pipeConnectorParameters.getIntOrDefault(
              PipeConnectorConstant.CONNECTOR_IOTDB_NUMBER_KEY,
              PipeConnectorConstant.CONNECTOR_IOTDB_NUMBER_DEFAULT_VALUE);

      ArrayList<PipeConnector> pipeConnectorList = new ArrayList<>(connectorNum);

      for (int i = 0; i < connectorNum; i++) {
        PipeConnector pipeConnector;
        if (connectorKey.equals(BuiltinPipePlugin.IOTDB_THRIFT_CONNECTOR.getPipePluginName())
            || connectorKey.equals(
                BuiltinPipePlugin.IOTDB_THRIFT_SYNC_CONNECTOR.getPipePluginName())) {
          pipeConnector = new IoTDBThriftSyncConnector();
        } else if (connectorKey.equals(
            BuiltinPipePlugin.IOTDB_THRIFT_ASYNC_CONNECTOR.getPipePluginName())) {
          pipeConnector = new IoTDBThriftAsyncConnector();
        } else if (connectorKey.equals(BuiltinPipePlugin.OPC_UA_CONNECTOR.getPipePluginName())) {
          pipeConnector = new OpcUaConnector();
        } else if (connectorKey.equals(
            BuiltinPipePlugin.IOTDB_LEGACY_PIPE_CONNECTOR.getPipePluginName())) {
          pipeConnector = new IoTDBLegacyPipeConnector();
        } else if (connectorKey.equals(
            BuiltinPipePlugin.IOTDB_AIR_GAP_CONNECTOR.getPipePluginName())) {
          pipeConnector = new IoTDBAirGapConnector();
        } else if (connectorKey.equals(BuiltinPipePlugin.WEBSOCKET_CONNECTOR.getPipePluginName())) {
          pipeConnector = new WebsocketConnector();
        } else {
          pipeConnector = PipeAgent.plugin().reflectConnector(pipeConnectorParameters);
        }

        try {
          pipeConnector.validate(new PipeParameterValidator(pipeConnectorParameters));
          pipeConnector.customize(
              pipeConnectorParameters, new PipeTaskRuntimeConfiguration(pipeRuntimeEnvironment));
          pipeConnector.handshake();
        } catch (Exception e) {
          throw new PipeException(
              "Failed to construct PipeConnector, because of " + e.getMessage(), e);
        }

        pipeConnectorList.add(pipeConnector);
      }

      // 2. Construct PipeConnectorSubtaskLifeCycle to manage PipeConnectorSubtask's life cycle
      final BoundedBlockingPendingQueue<Event> pendingQueue =
          new BoundedBlockingPendingQueue<>(
              PipeConfig.getInstance().getPipeConnectorPendingQueueSize());
      ArrayList<PipeConnectorSubtaskLifeCycle> pipeConnectorSubtaskLifeCycleList =
          new ArrayList<>(connectorNum);

      for (int i = 0; i < connectorNum; i++) {
        final PipeConnectorSubtask pipeConnectorSubtask =
            new PipeConnectorSubtask(
                attributeSortedString + i, pendingQueue, pipeConnectorList.get(i));
        final PipeConnectorSubtaskLifeCycle pipeConnectorSubtaskLifeCycle =
            new PipeConnectorSubtaskLifeCycle(executor, pipeConnectorSubtask, pendingQueue);
        pipeConnectorSubtaskLifeCycleList.add(pipeConnectorSubtaskLifeCycle);
      }

      attributeSortedString2SubtaskLifeCycleMap.put(
          attributeSortedString, pipeConnectorSubtaskLifeCycleList);
    }

    ArrayList<PipeConnectorSubtaskLifeCycle> list =
        attributeSortedString2SubtaskLifeCycleMap.get(attributeSortedString);

    for (PipeConnectorSubtaskLifeCycle o : list) {
      o.register();
    }

    return attributeSortedString;
  }

  public synchronized void deregister(String attributeSortedString) {
    if (!attributeSortedString2SubtaskLifeCycleMap.containsKey(attributeSortedString)) {
      throw new PipeException(FAILED_TO_DEREGISTER_EXCEPTION_MESSAGE + attributeSortedString);
    }

    for (PipeConnectorSubtaskLifeCycle o :
        attributeSortedString2SubtaskLifeCycleMap.get(attributeSortedString)) {
      if (o.deregister()) {
        attributeSortedString2SubtaskLifeCycleMap.remove(attributeSortedString);
      }
    }
  }

  public synchronized void start(String attributeSortedString) {
    if (!attributeSortedString2SubtaskLifeCycleMap.containsKey(attributeSortedString)) {
      throw new PipeException(FAILED_TO_DEREGISTER_EXCEPTION_MESSAGE + attributeSortedString);
    }

    for (PipeConnectorSubtaskLifeCycle o :
        attributeSortedString2SubtaskLifeCycleMap.get(attributeSortedString)) {
      o.start();
    }
  }

  public synchronized void stop(String attributeSortedString) {
    if (!attributeSortedString2SubtaskLifeCycleMap.containsKey(attributeSortedString)) {
      throw new PipeException(FAILED_TO_DEREGISTER_EXCEPTION_MESSAGE + attributeSortedString);
    }

    for (PipeConnectorSubtaskLifeCycle o :
        attributeSortedString2SubtaskLifeCycleMap.get(attributeSortedString)) {
      o.stop();
    }
  }

  public BoundedBlockingPendingQueue<Event> getPipeConnectorPendingQueue(
      String attributeSortedString) {
    if (!attributeSortedString2SubtaskLifeCycleMap.containsKey(attributeSortedString)) {
      throw new PipeException(
          "Failed to get PendingQueue. No such subtask: " + attributeSortedString);
    }

    return attributeSortedString2SubtaskLifeCycleMap
        .get(attributeSortedString)
        .get(0)
        .getPendingQueue();
  }

  /////////////////////////  Singleton Instance Holder  /////////////////////////

  private PipeConnectorSubtaskManager() {
    // Empty constructor
  }

  private static class PipeSubtaskManagerHolder {
    private static final PipeConnectorSubtaskManager INSTANCE = new PipeConnectorSubtaskManager();
  }

  public static PipeConnectorSubtaskManager instance() {
    return PipeSubtaskManagerHolder.INSTANCE;
  }
}
