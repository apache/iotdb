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
import org.apache.iotdb.commons.pipe.agent.plugin.builtin.sink.donothing.DoNothingSink;
import org.apache.iotdb.commons.pipe.agent.plugin.constructor.PipeSinkConstructor;
import org.apache.iotdb.commons.pipe.agent.plugin.meta.DataNodePipePluginMetaKeeper;
import org.apache.iotdb.db.pipe.sink.protocol.airgap.IoTDBDataRegionAirGapSink;
import org.apache.iotdb.db.pipe.sink.protocol.legacy.IoTDBLegacyPipeSink;
import org.apache.iotdb.db.pipe.sink.protocol.opcda.OpcDaSink;
import org.apache.iotdb.db.pipe.sink.protocol.opcua.OpcUaSink;
import org.apache.iotdb.db.pipe.sink.protocol.pipeconsensus.PipeConsensusAsyncSink;
import org.apache.iotdb.db.pipe.sink.protocol.thrift.async.IoTDBDataRegionAsyncSink;
import org.apache.iotdb.db.pipe.sink.protocol.thrift.sync.IoTDBDataRegionSyncSink;
import org.apache.iotdb.db.pipe.sink.protocol.websocket.WebSocketSink;
import org.apache.iotdb.db.pipe.sink.protocol.writeback.WriteBackSink;

class PipeDataRegionSinkConstructor extends PipeSinkConstructor {

  PipeDataRegionSinkConstructor(DataNodePipePluginMetaKeeper pipePluginMetaKeeper) {
    super(pipePluginMetaKeeper);
  }

  @Override
  protected void initConstructors() {
    pluginConstructors.put(
        BuiltinPipePlugin.IOTDB_THRIFT_CONNECTOR.getPipePluginName(),
        IoTDBDataRegionAsyncSink::new);
    pluginConstructors.put(
        BuiltinPipePlugin.IOTDB_THRIFT_SSL_CONNECTOR.getPipePluginName(),
        IoTDBDataRegionSyncSink::new);
    pluginConstructors.put(
        BuiltinPipePlugin.IOTDB_THRIFT_SYNC_CONNECTOR.getPipePluginName(),
        IoTDBDataRegionSyncSink::new);
    pluginConstructors.put(
        BuiltinPipePlugin.IOTDB_THRIFT_ASYNC_CONNECTOR.getPipePluginName(),
        IoTDBDataRegionAsyncSink::new);
    pluginConstructors.put(
        BuiltinPipePlugin.PIPE_CONSENSUS_ASYNC_CONNECTOR.getPipePluginName(),
        PipeConsensusAsyncSink::new);
    pluginConstructors.put(
        BuiltinPipePlugin.IOTDB_LEGACY_PIPE_CONNECTOR.getPipePluginName(),
        IoTDBLegacyPipeSink::new);
    pluginConstructors.put(
        BuiltinPipePlugin.IOTDB_AIR_GAP_CONNECTOR.getPipePluginName(),
        IoTDBDataRegionAirGapSink::new);
    pluginConstructors.put(
        BuiltinPipePlugin.WEBSOCKET_CONNECTOR.getPipePluginName(), WebSocketSink::new);
    pluginConstructors.put(BuiltinPipePlugin.OPC_UA_CONNECTOR.getPipePluginName(), OpcUaSink::new);
    pluginConstructors.put(BuiltinPipePlugin.OPC_DA_CONNECTOR.getPipePluginName(), OpcDaSink::new);
    pluginConstructors.put(
        BuiltinPipePlugin.DO_NOTHING_CONNECTOR.getPipePluginName(), DoNothingSink::new);
    pluginConstructors.put(
        BuiltinPipePlugin.WRITE_BACK_CONNECTOR.getPipePluginName(), WriteBackSink::new);

    pluginConstructors.put(
        BuiltinPipePlugin.IOTDB_THRIFT_SINK.getPipePluginName(), IoTDBDataRegionAsyncSink::new);
    pluginConstructors.put(
        BuiltinPipePlugin.IOTDB_THRIFT_SSL_SINK.getPipePluginName(), IoTDBDataRegionSyncSink::new);
    pluginConstructors.put(
        BuiltinPipePlugin.IOTDB_THRIFT_SYNC_SINK.getPipePluginName(), IoTDBDataRegionSyncSink::new);
    pluginConstructors.put(
        BuiltinPipePlugin.IOTDB_THRIFT_ASYNC_SINK.getPipePluginName(),
        IoTDBDataRegionAsyncSink::new);
    pluginConstructors.put(
        BuiltinPipePlugin.IOTDB_LEGACY_PIPE_SINK.getPipePluginName(), IoTDBLegacyPipeSink::new);
    pluginConstructors.put(
        BuiltinPipePlugin.IOTDB_AIR_GAP_SINK.getPipePluginName(), IoTDBDataRegionAirGapSink::new);
    pluginConstructors.put(
        BuiltinPipePlugin.WEBSOCKET_SINK.getPipePluginName(), WebSocketSink::new);
    pluginConstructors.put(BuiltinPipePlugin.OPC_UA_SINK.getPipePluginName(), OpcUaSink::new);
    pluginConstructors.put(BuiltinPipePlugin.OPC_DA_SINK.getPipePluginName(), OpcDaSink::new);
    pluginConstructors.put(
        BuiltinPipePlugin.DO_NOTHING_SINK.getPipePluginName(), DoNothingSink::new);
    pluginConstructors.put(
        BuiltinPipePlugin.WRITE_BACK_SINK.getPipePluginName(), WriteBackSink::new);
    pluginConstructors.put(
        BuiltinPipePlugin.SUBSCRIPTION_SINK.getPipePluginName(), DoNothingSink::new);
    pluginConstructors.put(
        BuiltinPipePlugin.PIPE_CONSENSUS_ASYNC_SINK.getPipePluginName(),
        PipeConsensusAsyncSink::new);
  }
}
