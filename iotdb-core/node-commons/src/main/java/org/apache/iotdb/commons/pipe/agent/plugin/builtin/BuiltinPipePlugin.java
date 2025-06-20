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

package org.apache.iotdb.commons.pipe.agent.plugin.builtin;

import org.apache.iotdb.commons.pipe.agent.plugin.builtin.connector.donothing.DoNothingConnector;
import org.apache.iotdb.commons.pipe.agent.plugin.builtin.connector.iotdb.consensus.PipeConsensusAsyncConnector;
import org.apache.iotdb.commons.pipe.agent.plugin.builtin.connector.iotdb.thrift.IoTDBLegacyPipeConnector;
import org.apache.iotdb.commons.pipe.agent.plugin.builtin.connector.iotdb.thrift.IoTDBThriftAsyncConnector;
import org.apache.iotdb.commons.pipe.agent.plugin.builtin.connector.iotdb.thrift.IoTDBThriftConnector;
import org.apache.iotdb.commons.pipe.agent.plugin.builtin.connector.iotdb.thrift.IoTDBThriftSslConnector;
import org.apache.iotdb.commons.pipe.agent.plugin.builtin.connector.iotdb.thrift.IoTDBThriftSyncConnector;
import org.apache.iotdb.commons.pipe.agent.plugin.builtin.connector.websocket.WebSocketConnector;
import org.apache.iotdb.commons.pipe.agent.plugin.builtin.connector.writeback.WriteBackConnector;
import org.apache.iotdb.commons.pipe.agent.plugin.builtin.extractor.donothing.DoNothingExtractor;
import org.apache.iotdb.commons.pipe.agent.plugin.builtin.extractor.iotdb.IoTDBExtractor;
import org.apache.iotdb.commons.pipe.agent.plugin.builtin.processor.donothing.DoNothingProcessor;
import org.apache.iotdb.commons.pipe.agent.plugin.builtin.processor.pipeconsensus.PipeConsensusProcessor;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

public enum BuiltinPipePlugin {

  // extractors
  DO_NOTHING_EXTRACTOR("do-nothing-extractor", DoNothingExtractor.class),
  IOTDB_EXTRACTOR("iotdb-extractor", IoTDBExtractor.class),

  DO_NOTHING_SOURCE("do-nothing-source", DoNothingExtractor.class),
  IOTDB_SOURCE("iotdb-source", IoTDBExtractor.class),

  // processors
  DO_NOTHING_PROCESSOR("do-nothing-processor", DoNothingProcessor.class),

  // Hidden-processors, which are plugins of the processors
  PIPE_CONSENSUS_PROCESSOR("pipe-consensus-processor", PipeConsensusProcessor.class),

  // connectors
  DO_NOTHING_CONNECTOR("do-nothing-connector", DoNothingConnector.class),
  IOTDB_THRIFT_CONNECTOR("iotdb-thrift-connector", IoTDBThriftConnector.class),
  IOTDB_THRIFT_SSL_CONNECTOR("iotdb-thrift-ssl-connector", IoTDBThriftSslConnector.class),
  IOTDB_THRIFT_SYNC_CONNECTOR("iotdb-thrift-sync-connector", IoTDBThriftSyncConnector.class),
  IOTDB_THRIFT_ASYNC_CONNECTOR("iotdb-thrift-async-connector", IoTDBThriftAsyncConnector.class),
  IOTDB_LEGACY_PIPE_CONNECTOR("iotdb-legacy-pipe-connector", IoTDBLegacyPipeConnector.class),
  PIPE_CONSENSUS_ASYNC_CONNECTOR(
      "pipe-consensus-async-connector", PipeConsensusAsyncConnector.class),

  WEBSOCKET_CONNECTOR("websocket-connector", WebSocketConnector.class),
  WRITE_BACK_CONNECTOR("write-back-connector", WriteBackConnector.class),

  DO_NOTHING_SINK("do-nothing-sink", DoNothingConnector.class),
  IOTDB_THRIFT_SINK("iotdb-thrift-sink", IoTDBThriftConnector.class),
  IOTDB_THRIFT_SSL_SINK("iotdb-thrift-ssl-sink", IoTDBThriftSslConnector.class),
  IOTDB_THRIFT_SYNC_SINK("iotdb-thrift-sync-sink", IoTDBThriftSyncConnector.class),
  IOTDB_THRIFT_ASYNC_SINK("iotdb-thrift-async-sink", IoTDBThriftAsyncConnector.class),
  IOTDB_LEGACY_PIPE_SINK("iotdb-legacy-pipe-sink", IoTDBLegacyPipeConnector.class),
  WEBSOCKET_SINK("websocket-sink", WebSocketConnector.class),
  WRITE_BACK_SINK("write-back-sink", WriteBackConnector.class),
  SUBSCRIPTION_SINK("subscription-sink", DoNothingConnector.class),
  PIPE_CONSENSUS_ASYNC_SINK("pipe-consensus-async-sink", PipeConsensusAsyncConnector.class),
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

  public static final Set<String> SHOW_PIPE_PLUGINS_BLACKLIST =
      Collections.unmodifiableSet(
          new HashSet<>(
              Arrays.asList(
                  // Extractors
                  DO_NOTHING_EXTRACTOR.getPipePluginName().toUpperCase(),
                  IOTDB_EXTRACTOR.getPipePluginName().toUpperCase(),
                  // Sources
                  DO_NOTHING_SOURCE.getPipePluginName().toUpperCase(),
                  // Processors
                  PIPE_CONSENSUS_PROCESSOR.getPipePluginName().toUpperCase(),
                  // Connectors
                  DO_NOTHING_CONNECTOR.getPipePluginName().toUpperCase(),
                  IOTDB_THRIFT_CONNECTOR.getPipePluginName().toUpperCase(),
                  IOTDB_THRIFT_SSL_CONNECTOR.getPipePluginName().toUpperCase(),
                  IOTDB_THRIFT_SYNC_CONNECTOR.getPipePluginName().toUpperCase(),
                  IOTDB_THRIFT_ASYNC_CONNECTOR.getPipePluginName().toUpperCase(),
                  IOTDB_LEGACY_PIPE_CONNECTOR.getPipePluginName().toUpperCase(),
                  WEBSOCKET_CONNECTOR.getPipePluginName().toUpperCase(),
                  WRITE_BACK_CONNECTOR.getPipePluginName().toUpperCase(),
                  PIPE_CONSENSUS_ASYNC_CONNECTOR.getPipePluginName().toUpperCase(),
                  // Sinks
                  IOTDB_THRIFT_SYNC_SINK.getPipePluginName().toUpperCase(),
                  IOTDB_THRIFT_ASYNC_SINK.getPipePluginName().toUpperCase(),
                  IOTDB_LEGACY_PIPE_SINK.getPipePluginName().toUpperCase(),
                  WEBSOCKET_SINK.getPipePluginName().toUpperCase(),
                  SUBSCRIPTION_SINK.getPipePluginName().toUpperCase(),
                  PIPE_CONSENSUS_ASYNC_SINK.getPipePluginName().toUpperCase())));
}
