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

package org.apache.iotdb.db.pipe.agent.plugin.schemaregion;

import org.apache.iotdb.commons.pipe.agent.plugin.builtin.BuiltinPipePlugin;
import org.apache.iotdb.commons.pipe.agent.plugin.builtin.sink.donothing.DoNothingSink;
import org.apache.iotdb.commons.pipe.agent.plugin.constructor.PipeSinkConstructor;
import org.apache.iotdb.db.pipe.sink.protocol.airgap.IoTDBSchemaRegionAirGapSink;
import org.apache.iotdb.db.pipe.sink.protocol.thrift.sync.IoTDBSchemaRegionSink;
import org.apache.iotdb.pipe.api.PipeConnector;

class PipeSchemaRegionSinkConstructor extends PipeSinkConstructor {

  @Override
  protected void initConstructors() {
    pluginConstructors.put(
        BuiltinPipePlugin.IOTDB_THRIFT_CONNECTOR.getPipePluginName(), IoTDBSchemaRegionSink::new);
    pluginConstructors.put(
        BuiltinPipePlugin.IOTDB_THRIFT_SSL_CONNECTOR.getPipePluginName(),
        IoTDBSchemaRegionSink::new);
    pluginConstructors.put(
        BuiltinPipePlugin.IOTDB_THRIFT_SYNC_CONNECTOR.getPipePluginName(),
        IoTDBSchemaRegionSink::new);
    pluginConstructors.put(
        BuiltinPipePlugin.IOTDB_THRIFT_ASYNC_CONNECTOR.getPipePluginName(),
        IoTDBSchemaRegionSink::new);
    pluginConstructors.put(
        BuiltinPipePlugin.IOTDB_AIR_GAP_CONNECTOR.getPipePluginName(),
        IoTDBSchemaRegionAirGapSink::new);
    pluginConstructors.put(
        BuiltinPipePlugin.DO_NOTHING_CONNECTOR.getPipePluginName(), DoNothingSink::new);

    pluginConstructors.put(
        BuiltinPipePlugin.IOTDB_THRIFT_SINK.getPipePluginName(), IoTDBSchemaRegionSink::new);
    pluginConstructors.put(
        BuiltinPipePlugin.IOTDB_THRIFT_SSL_SINK.getPipePluginName(), IoTDBSchemaRegionSink::new);
    pluginConstructors.put(
        BuiltinPipePlugin.IOTDB_THRIFT_SYNC_SINK.getPipePluginName(), IoTDBSchemaRegionSink::new);
    pluginConstructors.put(
        BuiltinPipePlugin.IOTDB_THRIFT_ASYNC_SINK.getPipePluginName(), IoTDBSchemaRegionSink::new);
    pluginConstructors.put(
        BuiltinPipePlugin.IOTDB_AIR_GAP_SINK.getPipePluginName(), IoTDBSchemaRegionAirGapSink::new);
    pluginConstructors.put(
        BuiltinPipePlugin.DO_NOTHING_SINK.getPipePluginName(), DoNothingSink::new);
  }

  @Override
  public PipeConnector reflectPluginByKey(String pluginKey) {
    // TODO: support constructing plugin by reflection
    return (PipeConnector) pluginConstructors.getOrDefault(pluginKey, DoNothingSink::new).get();
  }
}
