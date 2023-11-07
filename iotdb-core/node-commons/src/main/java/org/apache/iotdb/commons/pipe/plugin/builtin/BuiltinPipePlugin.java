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

import org.apache.iotdb.commons.pipe.plugin.builtin.connector.DoNothingConnector;
import org.apache.iotdb.commons.pipe.plugin.builtin.connector.IoTDBAirGapConnector;
import org.apache.iotdb.commons.pipe.plugin.builtin.connector.IoTDBLegacyPipeConnector;
import org.apache.iotdb.commons.pipe.plugin.builtin.connector.IoTDBThriftAsyncConnector;
import org.apache.iotdb.commons.pipe.plugin.builtin.connector.IoTDBThriftConnector;
import org.apache.iotdb.commons.pipe.plugin.builtin.connector.IoTDBThriftSyncConnector;
import org.apache.iotdb.commons.pipe.plugin.builtin.connector.OpcUaConnector;
import org.apache.iotdb.commons.pipe.plugin.builtin.connector.WebSocketConnector;
import org.apache.iotdb.commons.pipe.plugin.builtin.connector.WriteBackConnector;
import org.apache.iotdb.commons.pipe.plugin.builtin.extractor.IoTDBExtractor;
import org.apache.iotdb.commons.pipe.plugin.builtin.processor.DoNothingProcessor;

import java.util.HashSet;
import java.util.Set;

public enum BuiltinPipePlugin {

  // extractors
  IOTDB_EXTRACTOR("iotdb-extractor", IoTDBExtractor.class),

  IOTDB_SOURCE("iotdb-source", IoTDBExtractor.class),

  // processors
  DO_NOTHING_PROCESSOR("do-nothing-processor", DoNothingProcessor.class),

  // connectors
  DO_NOTHING_CONNECTOR("do-nothing-connector", DoNothingConnector.class),
  IOTDB_THRIFT_CONNECTOR("iotdb-thrift-connector", IoTDBThriftConnector.class),
  IOTDB_THRIFT_SYNC_CONNECTOR("iotdb-thrift-sync-connector", IoTDBThriftSyncConnector.class),
  IOTDB_THRIFT_ASYNC_CONNECTOR("iotdb-thrift-async-connector", IoTDBThriftAsyncConnector.class),
  IOTDB_LEGACY_PIPE_CONNECTOR("iotdb-legacy-pipe-connector", IoTDBLegacyPipeConnector.class),
  IOTDB_AIR_GAP_CONNECTOR("iotdb-air-gap-connector", IoTDBAirGapConnector.class),
  WEBSOCKET_CONNECTOR("websocket-connector", WebSocketConnector.class),
  OPC_UA_CONNECTOR("opc-ua-connector", OpcUaConnector.class),
  WRITE_BACK_CONNECTOR("write-back-connector", WriteBackConnector.class),

  DO_NOTHING_SINK("do-nothing-sink", DoNothingConnector.class),
  IOTDB_THRIFT_SINK("iotdb-thrift-sink", IoTDBThriftConnector.class),
  IOTDB_THRIFT_SYNC_SINK("iotdb-thrift-sync-sink", IoTDBThriftSyncConnector.class),
  IOTDB_THRIFT_ASYNC_SINK("iotdb-thrift-async-sink", IoTDBThriftAsyncConnector.class),
  IOTDB_LEGACY_PIPE_SINK("iotdb-legacy-pipe-sink", IoTDBLegacyPipeConnector.class),
  IOTDB_AIR_GAP_SINK("iotdb-air-gap-sink", IoTDBAirGapConnector.class),
  WEBSOCKET_SINK("websocket-sink", WebSocketConnector.class),
  OPC_UA_SINK("opc-ua-sink", OpcUaConnector.class),
  WRITE_BACK_SINK("write-back-sink", WriteBackConnector.class),
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

  public static final Set<String> SHOW_PIPE_PLUGINS_BLACKLIST = new HashSet<>();

  static {
    SHOW_PIPE_PLUGINS_BLACKLIST.add(IOTDB_EXTRACTOR.getPipePluginName());

    SHOW_PIPE_PLUGINS_BLACKLIST.add(DO_NOTHING_CONNECTOR.getPipePluginName());
    SHOW_PIPE_PLUGINS_BLACKLIST.add(IOTDB_THRIFT_CONNECTOR.getPipePluginName());
    SHOW_PIPE_PLUGINS_BLACKLIST.add(IOTDB_THRIFT_SYNC_CONNECTOR.getPipePluginName());
    SHOW_PIPE_PLUGINS_BLACKLIST.add(IOTDB_THRIFT_ASYNC_CONNECTOR.getPipePluginName());
    SHOW_PIPE_PLUGINS_BLACKLIST.add(IOTDB_LEGACY_PIPE_CONNECTOR.getPipePluginName());
    SHOW_PIPE_PLUGINS_BLACKLIST.add(IOTDB_AIR_GAP_CONNECTOR.getPipePluginName());
    SHOW_PIPE_PLUGINS_BLACKLIST.add(WEBSOCKET_CONNECTOR.getPipePluginName());
    SHOW_PIPE_PLUGINS_BLACKLIST.add(OPC_UA_CONNECTOR.getPipePluginName());

    SHOW_PIPE_PLUGINS_BLACKLIST.add(IOTDB_THRIFT_SYNC_SINK.getPipePluginName());
    SHOW_PIPE_PLUGINS_BLACKLIST.add(IOTDB_THRIFT_ASYNC_SINK.getPipePluginName());
    SHOW_PIPE_PLUGINS_BLACKLIST.add(IOTDB_LEGACY_PIPE_SINK.getPipePluginName());
    SHOW_PIPE_PLUGINS_BLACKLIST.add(WEBSOCKET_SINK.getPipePluginName());
    SHOW_PIPE_PLUGINS_BLACKLIST.add(OPC_UA_SINK.getPipePluginName());
  }
}
