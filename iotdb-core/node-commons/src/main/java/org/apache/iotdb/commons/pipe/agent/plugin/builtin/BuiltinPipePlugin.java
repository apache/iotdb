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

import org.apache.iotdb.commons.pipe.agent.plugin.builtin.processor.donothing.DoNothingProcessor;
import org.apache.iotdb.commons.pipe.agent.plugin.builtin.processor.pipeconsensus.PipeConsensusProcessor;
import org.apache.iotdb.commons.pipe.agent.plugin.builtin.processor.schemachange.RenameDatabaseProcessor;
import org.apache.iotdb.commons.pipe.agent.plugin.builtin.sink.donothing.DoNothingSink;
import org.apache.iotdb.commons.pipe.agent.plugin.builtin.sink.iotdb.consensus.PipeConsensusAsyncSink;
import org.apache.iotdb.commons.pipe.agent.plugin.builtin.sink.iotdb.thrift.IoTDBLegacyPipeSink;
import org.apache.iotdb.commons.pipe.agent.plugin.builtin.sink.iotdb.thrift.IoTDBThriftAsyncSink;
import org.apache.iotdb.commons.pipe.agent.plugin.builtin.sink.iotdb.thrift.IoTDBThriftSink;
import org.apache.iotdb.commons.pipe.agent.plugin.builtin.sink.iotdb.thrift.IoTDBThriftSslSink;
import org.apache.iotdb.commons.pipe.agent.plugin.builtin.sink.iotdb.thrift.IoTDBThriftSyncSink;
import org.apache.iotdb.commons.pipe.agent.plugin.builtin.sink.websocket.WebSocketSink;
import org.apache.iotdb.commons.pipe.agent.plugin.builtin.sink.writeback.WriteBackSink;
import org.apache.iotdb.commons.pipe.agent.plugin.builtin.source.donothing.DoNothingSource;
import org.apache.iotdb.commons.pipe.agent.plugin.builtin.source.iotdb.IoTDBSource;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

public enum BuiltinPipePlugin {

  // extractors
  DO_NOTHING_EXTRACTOR("do-nothing-extractor", DoNothingSource.class),
  IOTDB_EXTRACTOR("iotdb-extractor", IoTDBSource.class),

  DO_NOTHING_SOURCE("do-nothing-source", DoNothingSource.class),
  IOTDB_SOURCE("iotdb-source", IoTDBSource.class),

  // processors
  DO_NOTHING_PROCESSOR("do-nothing-processor", DoNothingProcessor.class),

  // Hidden-processors, which are plugins of the processors
  PIPE_CONSENSUS_PROCESSOR("pipe-consensus-processor", PipeConsensusProcessor.class),
  RENAME_DATABASE_PROCESSOR("rename-database-processor", RenameDatabaseProcessor.class),

  // connectors
  DO_NOTHING_CONNECTOR("do-nothing-connector", DoNothingSink.class),
  IOTDB_THRIFT_CONNECTOR("iotdb-thrift-connector", IoTDBThriftSink.class),
  IOTDB_THRIFT_SSL_CONNECTOR("iotdb-thrift-ssl-connector", IoTDBThriftSslSink.class),
  IOTDB_THRIFT_SYNC_CONNECTOR("iotdb-thrift-sync-connector", IoTDBThriftSyncSink.class),
  IOTDB_THRIFT_ASYNC_CONNECTOR("iotdb-thrift-async-connector", IoTDBThriftAsyncSink.class),
  IOTDB_LEGACY_PIPE_CONNECTOR("iotdb-legacy-pipe-connector", IoTDBLegacyPipeSink.class),
  PIPE_CONSENSUS_ASYNC_CONNECTOR("pipe-consensus-async-connector", PipeConsensusAsyncSink.class),

  WEBSOCKET_CONNECTOR("websocket-connector", WebSocketSink.class),
  WRITE_BACK_CONNECTOR("write-back-connector", WriteBackSink.class),

  DO_NOTHING_SINK("do-nothing-sink", DoNothingSink.class),
  IOTDB_THRIFT_SINK("iotdb-thrift-sink", IoTDBThriftSink.class),
  IOTDB_THRIFT_SSL_SINK("iotdb-thrift-ssl-sink", IoTDBThriftSslSink.class),
  IOTDB_THRIFT_SYNC_SINK("iotdb-thrift-sync-sink", IoTDBThriftSyncSink.class),
  IOTDB_THRIFT_ASYNC_SINK("iotdb-thrift-async-sink", IoTDBThriftAsyncSink.class),
  IOTDB_LEGACY_PIPE_SINK("iotdb-legacy-pipe-sink", IoTDBLegacyPipeSink.class),
  WEBSOCKET_SINK("websocket-sink", WebSocketSink.class),
  WRITE_BACK_SINK("write-back-sink", WriteBackSink.class),
  SUBSCRIPTION_SINK("subscription-sink", DoNothingSink.class),
  PIPE_CONSENSUS_ASYNC_SINK("pipe-consensus-async-sink", PipeConsensusAsyncSink.class),
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

  // used to distinguish between builtin and external sources
  public static final Set<String> BUILTIN_SOURCES =
      Collections.unmodifiableSet(
          new HashSet<>(
              Arrays.asList(
                  DO_NOTHING_EXTRACTOR.getPipePluginName().toLowerCase(),
                  IOTDB_EXTRACTOR.getPipePluginName().toLowerCase(),
                  DO_NOTHING_SOURCE.getPipePluginName().toLowerCase(),
                  IOTDB_SOURCE.getPipePluginName().toLowerCase())));

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
                  RENAME_DATABASE_PROCESSOR.getPipePluginName().toUpperCase(),
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
