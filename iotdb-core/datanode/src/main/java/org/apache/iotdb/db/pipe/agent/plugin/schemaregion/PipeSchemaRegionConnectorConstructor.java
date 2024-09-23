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
import org.apache.iotdb.commons.pipe.agent.plugin.builtin.connector.donothing.DoNothingConnector;
import org.apache.iotdb.commons.pipe.agent.plugin.constructor.PipeConnectorConstructor;
import org.apache.iotdb.db.pipe.connector.protocol.airgap.IoTDBSchemaRegionAirGapConnector;
import org.apache.iotdb.db.pipe.connector.protocol.thrift.sync.IoTDBSchemaRegionConnector;
import org.apache.iotdb.pipe.api.PipeConnector;

class PipeSchemaRegionConnectorConstructor extends PipeConnectorConstructor {

  @Override
  protected void initConstructors() {
    pluginConstructors.put(
        BuiltinPipePlugin.IOTDB_THRIFT_CONNECTOR.getPipePluginName(),
        IoTDBSchemaRegionConnector::new);
    pluginConstructors.put(
        BuiltinPipePlugin.IOTDB_THRIFT_SSL_CONNECTOR.getPipePluginName(),
        IoTDBSchemaRegionConnector::new);
    pluginConstructors.put(
        BuiltinPipePlugin.IOTDB_THRIFT_SYNC_CONNECTOR.getPipePluginName(),
        IoTDBSchemaRegionConnector::new);
    pluginConstructors.put(
        BuiltinPipePlugin.IOTDB_THRIFT_ASYNC_CONNECTOR.getPipePluginName(),
        IoTDBSchemaRegionConnector::new);
    pluginConstructors.put(
        BuiltinPipePlugin.IOTDB_AIR_GAP_CONNECTOR.getPipePluginName(),
        IoTDBSchemaRegionAirGapConnector::new);
    pluginConstructors.put(
        BuiltinPipePlugin.DO_NOTHING_CONNECTOR.getPipePluginName(), DoNothingConnector::new);

    pluginConstructors.put(
        BuiltinPipePlugin.IOTDB_THRIFT_SINK.getPipePluginName(), IoTDBSchemaRegionConnector::new);
    pluginConstructors.put(
        BuiltinPipePlugin.IOTDB_THRIFT_SSL_SINK.getPipePluginName(),
        IoTDBSchemaRegionConnector::new);
    pluginConstructors.put(
        BuiltinPipePlugin.IOTDB_THRIFT_SYNC_SINK.getPipePluginName(),
        IoTDBSchemaRegionConnector::new);
    pluginConstructors.put(
        BuiltinPipePlugin.IOTDB_THRIFT_ASYNC_SINK.getPipePluginName(),
        IoTDBSchemaRegionConnector::new);
    pluginConstructors.put(
        BuiltinPipePlugin.IOTDB_AIR_GAP_SINK.getPipePluginName(),
        IoTDBSchemaRegionAirGapConnector::new);
    pluginConstructors.put(
        BuiltinPipePlugin.DO_NOTHING_SINK.getPipePluginName(), DoNothingConnector::new);
  }

  @Override
  public PipeConnector reflectPluginByKey(String pluginKey) {
    // TODO: support constructing plugin by reflection
    return (PipeConnector)
        pluginConstructors.getOrDefault(pluginKey, DoNothingConnector::new).get();
  }
}
