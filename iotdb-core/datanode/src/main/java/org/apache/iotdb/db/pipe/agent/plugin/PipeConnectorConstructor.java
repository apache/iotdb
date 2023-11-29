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

package org.apache.iotdb.db.pipe.agent.plugin;

import org.apache.iotdb.commons.pipe.config.constant.PipeConnectorConstant;
import org.apache.iotdb.commons.pipe.plugin.builtin.BuiltinPipePlugin;
import org.apache.iotdb.commons.pipe.plugin.builtin.connector.DoNothingConnector;
import org.apache.iotdb.commons.pipe.plugin.meta.DataNodePipePluginMetaKeeper;
import org.apache.iotdb.db.pipe.connector.protocol.airgap.IoTDBAirGapConnector;
import org.apache.iotdb.db.pipe.connector.protocol.legacy.IoTDBLegacyPipeConnector;
import org.apache.iotdb.db.pipe.connector.protocol.opcua.OpcUaConnector;
import org.apache.iotdb.db.pipe.connector.protocol.thrift.async.IoTDBThriftAsyncConnector;
import org.apache.iotdb.db.pipe.connector.protocol.thrift.sync.IoTDBThriftSyncConnector;
import org.apache.iotdb.db.pipe.connector.protocol.websocket.WebSocketConnector;
import org.apache.iotdb.db.pipe.connector.protocol.writeback.WriteBackConnector;
import org.apache.iotdb.pipe.api.PipeConnector;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameters;
import org.apache.iotdb.pipe.api.exception.PipeException;

import java.util.Arrays;

public class PipeConnectorConstructor extends PipePluginConstructor {

  PipeConnectorConstructor(DataNodePipePluginMetaKeeper pipePluginMetaKeeper) {
    super(pipePluginMetaKeeper);
  }

  @Override
  protected void initConstructors() {
    PLUGIN_CONSTRUCTORS.put(
        BuiltinPipePlugin.IOTDB_THRIFT_CONNECTOR.getPipePluginName(),
        IoTDBThriftAsyncConnector::new);
    PLUGIN_CONSTRUCTORS.put(
        BuiltinPipePlugin.IOTDB_THRIFT_SYNC_CONNECTOR.getPipePluginName(),
        IoTDBThriftSyncConnector::new);
    PLUGIN_CONSTRUCTORS.put(
        BuiltinPipePlugin.IOTDB_THRIFT_ASYNC_CONNECTOR.getPipePluginName(),
        IoTDBThriftAsyncConnector::new);
    PLUGIN_CONSTRUCTORS.put(
        BuiltinPipePlugin.IOTDB_LEGACY_PIPE_CONNECTOR.getPipePluginName(),
        IoTDBLegacyPipeConnector::new);
    PLUGIN_CONSTRUCTORS.put(
        BuiltinPipePlugin.IOTDB_AIR_GAP_CONNECTOR.getPipePluginName(), IoTDBAirGapConnector::new);
    PLUGIN_CONSTRUCTORS.put(
        BuiltinPipePlugin.WEBSOCKET_CONNECTOR.getPipePluginName(), WebSocketConnector::new);
    PLUGIN_CONSTRUCTORS.put(
        BuiltinPipePlugin.OPC_UA_CONNECTOR.getPipePluginName(), OpcUaConnector::new);
    PLUGIN_CONSTRUCTORS.put(
        BuiltinPipePlugin.DO_NOTHING_CONNECTOR.getPipePluginName(), DoNothingConnector::new);
    PLUGIN_CONSTRUCTORS.put(
        BuiltinPipePlugin.WRITE_BACK_CONNECTOR.getPipePluginName(), WriteBackConnector::new);

    PLUGIN_CONSTRUCTORS.put(
        BuiltinPipePlugin.IOTDB_THRIFT_SINK.getPipePluginName(), IoTDBThriftAsyncConnector::new);
    PLUGIN_CONSTRUCTORS.put(
        BuiltinPipePlugin.IOTDB_THRIFT_SYNC_SINK.getPipePluginName(),
        IoTDBThriftSyncConnector::new);
    PLUGIN_CONSTRUCTORS.put(
        BuiltinPipePlugin.IOTDB_THRIFT_ASYNC_SINK.getPipePluginName(),
        IoTDBThriftAsyncConnector::new);
    PLUGIN_CONSTRUCTORS.put(
        BuiltinPipePlugin.IOTDB_LEGACY_PIPE_SINK.getPipePluginName(),
        IoTDBLegacyPipeConnector::new);
    PLUGIN_CONSTRUCTORS.put(
        BuiltinPipePlugin.IOTDB_AIR_GAP_SINK.getPipePluginName(), IoTDBAirGapConnector::new);
    PLUGIN_CONSTRUCTORS.put(
        BuiltinPipePlugin.WEBSOCKET_SINK.getPipePluginName(), WebSocketConnector::new);
    PLUGIN_CONSTRUCTORS.put(BuiltinPipePlugin.OPC_UA_SINK.getPipePluginName(), OpcUaConnector::new);
    PLUGIN_CONSTRUCTORS.put(
        BuiltinPipePlugin.DO_NOTHING_SINK.getPipePluginName(), DoNothingConnector::new);
    PLUGIN_CONSTRUCTORS.put(
        BuiltinPipePlugin.WRITE_BACK_SINK.getPipePluginName(), WriteBackConnector::new);
  }

  @Override
  PipeConnector reflectPlugin(PipeParameters connectorParameters) {
    if (!connectorParameters.hasAnyAttributes(
        PipeConnectorConstant.CONNECTOR_KEY, PipeConnectorConstant.SINK_KEY)) {
      throw new PipeException(
          "Failed to reflect PipeConnector instance because "
              + "'connector' is not specified in the parameters.");
    }

    return (PipeConnector)
        reflectPluginByKey(
            connectorParameters
                .getStringOrDefault(
                    Arrays.asList(
                        PipeConnectorConstant.CONNECTOR_KEY, PipeConnectorConstant.SINK_KEY),
                    BuiltinPipePlugin.IOTDB_THRIFT_CONNECTOR.getPipePluginName())
                // Convert the value of `CONNECTOR_KEY` or `SINK_KEY` to lowercase for matching in
                // `PLUGIN_CONSTRUCTORS`
                .toLowerCase());
  }
}
