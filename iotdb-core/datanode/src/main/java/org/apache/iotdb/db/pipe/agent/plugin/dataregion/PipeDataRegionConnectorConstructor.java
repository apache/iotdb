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

package org.apache.iotdb.db.pipe.agent.plugin.dataregion;

import org.apache.iotdb.commons.pipe.agent.plugin.builtin.BuiltinPipePlugin;
import org.apache.iotdb.commons.pipe.agent.plugin.builtin.connector.donothing.DoNothingConnector;
import org.apache.iotdb.commons.pipe.agent.plugin.constructor.PipeConnectorConstructor;
import org.apache.iotdb.commons.pipe.agent.plugin.meta.DataNodePipePluginMetaKeeper;
import org.apache.iotdb.db.pipe.connector.protocol.airgap.IoTDBDataRegionAirGapConnector;
import org.apache.iotdb.db.pipe.connector.protocol.legacy.IoTDBLegacyPipeConnector;
import org.apache.iotdb.db.pipe.connector.protocol.opcua.OpcUaConnector;
import org.apache.iotdb.db.pipe.connector.protocol.pipeconsensus.PipeConsensusAsyncConnector;
import org.apache.iotdb.db.pipe.connector.protocol.thrift.async.IoTDBDataRegionAsyncConnector;
import org.apache.iotdb.db.pipe.connector.protocol.thrift.sync.IoTDBDataRegionSyncConnector;
import org.apache.iotdb.db.pipe.connector.protocol.websocket.WebSocketConnector;
import org.apache.iotdb.db.pipe.connector.protocol.writeback.WriteBackConnector;

class PipeDataRegionConnectorConstructor extends PipeConnectorConstructor {

  PipeDataRegionConnectorConstructor(DataNodePipePluginMetaKeeper pipePluginMetaKeeper) {
    super(pipePluginMetaKeeper);
  }

  @Override
  protected void initConstructors() {
    pluginConstructors.put(
        BuiltinPipePlugin.IOTDB_THRIFT_CONNECTOR.getPipePluginName(),
        IoTDBDataRegionAsyncConnector::new);
    pluginConstructors.put(
        BuiltinPipePlugin.IOTDB_THRIFT_SSL_CONNECTOR.getPipePluginName(),
        IoTDBDataRegionSyncConnector::new);
    pluginConstructors.put(
        BuiltinPipePlugin.IOTDB_THRIFT_SYNC_CONNECTOR.getPipePluginName(),
        IoTDBDataRegionSyncConnector::new);
    pluginConstructors.put(
        BuiltinPipePlugin.IOTDB_THRIFT_ASYNC_CONNECTOR.getPipePluginName(),
        IoTDBDataRegionAsyncConnector::new);
    pluginConstructors.put(
        BuiltinPipePlugin.PIPE_CONSENSUS_ASYNC_CONNECTOR.getPipePluginName(),
        PipeConsensusAsyncConnector::new);
    pluginConstructors.put(
        BuiltinPipePlugin.IOTDB_LEGACY_PIPE_CONNECTOR.getPipePluginName(),
        IoTDBLegacyPipeConnector::new);
    pluginConstructors.put(
        BuiltinPipePlugin.IOTDB_AIR_GAP_CONNECTOR.getPipePluginName(),
        IoTDBDataRegionAirGapConnector::new);
    pluginConstructors.put(
        BuiltinPipePlugin.WEBSOCKET_CONNECTOR.getPipePluginName(), WebSocketConnector::new);
    pluginConstructors.put(
        BuiltinPipePlugin.OPC_UA_CONNECTOR.getPipePluginName(), OpcUaConnector::new);
    pluginConstructors.put(
        BuiltinPipePlugin.DO_NOTHING_CONNECTOR.getPipePluginName(), DoNothingConnector::new);
    pluginConstructors.put(
        BuiltinPipePlugin.WRITE_BACK_CONNECTOR.getPipePluginName(), WriteBackConnector::new);

    pluginConstructors.put(
        BuiltinPipePlugin.IOTDB_THRIFT_SINK.getPipePluginName(),
        IoTDBDataRegionAsyncConnector::new);
    pluginConstructors.put(
        BuiltinPipePlugin.IOTDB_THRIFT_SSL_SINK.getPipePluginName(),
        IoTDBDataRegionSyncConnector::new);
    pluginConstructors.put(
        BuiltinPipePlugin.IOTDB_THRIFT_SYNC_SINK.getPipePluginName(),
        IoTDBDataRegionSyncConnector::new);
    pluginConstructors.put(
        BuiltinPipePlugin.IOTDB_THRIFT_ASYNC_SINK.getPipePluginName(),
        IoTDBDataRegionAsyncConnector::new);
    pluginConstructors.put(
        BuiltinPipePlugin.IOTDB_LEGACY_PIPE_SINK.getPipePluginName(),
        IoTDBLegacyPipeConnector::new);
    pluginConstructors.put(
        BuiltinPipePlugin.IOTDB_AIR_GAP_SINK.getPipePluginName(),
        IoTDBDataRegionAirGapConnector::new);
    pluginConstructors.put(
        BuiltinPipePlugin.WEBSOCKET_SINK.getPipePluginName(), WebSocketConnector::new);
    pluginConstructors.put(BuiltinPipePlugin.OPC_UA_SINK.getPipePluginName(), OpcUaConnector::new);
    pluginConstructors.put(
        BuiltinPipePlugin.DO_NOTHING_SINK.getPipePluginName(), DoNothingConnector::new);
    pluginConstructors.put(
        BuiltinPipePlugin.WRITE_BACK_SINK.getPipePluginName(), WriteBackConnector::new);
    pluginConstructors.put(
        BuiltinPipePlugin.SUBSCRIPTION_SINK.getPipePluginName(), DoNothingConnector::new);
    pluginConstructors.put(
        BuiltinPipePlugin.PIPE_CONSENSUS_ASYNC_SINK.getPipePluginName(),
        PipeConsensusAsyncConnector::new);
  }
}
