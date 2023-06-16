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

package org.apache.iotdb.commons.pipe.plugin.builtin;

import org.apache.iotdb.commons.pipe.plugin.builtin.collector.IoTDBCollector;
import org.apache.iotdb.commons.pipe.plugin.builtin.connector.DoNothingConnector;
import org.apache.iotdb.commons.pipe.plugin.builtin.connector.IoTDBSyncConnectorV1_1;
import org.apache.iotdb.commons.pipe.plugin.builtin.connector.IoTDBThriftConnector;
import org.apache.iotdb.commons.pipe.plugin.builtin.connector.IoTDBThriftConnectorV1;
import org.apache.iotdb.commons.pipe.plugin.builtin.connector.IoTDBThriftConnectorV2;
import org.apache.iotdb.commons.pipe.plugin.builtin.processor.DoNothingProcessor;

public enum BuiltinPipePlugin {

  // collectors
  IOTDB_COLLECTOR("iotdb_collector", IoTDBCollector.class),

  // processors
  DO_NOTHING_PROCESSOR("do_nothing_processor", DoNothingProcessor.class),

  // connectors
  DO_NOTHING_CONNECTOR("do_nothing_connector", DoNothingConnector.class),
  IOTDB_THRIFT_CONNECTOR("iotdb_thrift_connector", IoTDBThriftConnector.class),
  IOTDB_THRIFT_CONNECTOR_V1("iotdb_thrift_connector_v1", IoTDBThriftConnectorV1.class),
  IOTDB_THRIFT_CONNECTOR_V2("iotdb_thrift_connector_v2", IoTDBThriftConnectorV2.class),
  IOTDB_SYNC_CONNECTOR_V_1_1("iotdb_sync_connector_v1.1", IoTDBSyncConnectorV1_1.class),
  ;

  private final String pipePluginName;
  private final Class<?> pipePluginClass;
  private final String className;

  BuiltinPipePlugin(String pipePluginName, Class<?> pipePluginClass) {
    this.pipePluginName = pipePluginName;
    this.pipePluginClass = pipePluginClass;
    this.className = pipePluginClass.getName();
  }

  public String getPipePluginName() {
    return pipePluginName;
  }

  public Class<?> getPipePluginClass() {
    return pipePluginClass;
  }

  public String getClassName() {
    return className;
  }
}
