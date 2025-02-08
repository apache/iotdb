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
import org.apache.iotdb.commons.pipe.agent.plugin.builtin.connector.iotdb.airgap.IoTDBAirGapConnector;
import org.apache.iotdb.commons.pipe.agent.plugin.builtin.connector.iotdb.consensus.PipeConsensusAsyncConnector;
import org.apache.iotdb.commons.pipe.agent.plugin.builtin.connector.iotdb.thrift.IoTDBLegacyPipeConnector;
import org.apache.iotdb.commons.pipe.agent.plugin.builtin.connector.iotdb.thrift.IoTDBThriftAsyncConnector;
import org.apache.iotdb.commons.pipe.agent.plugin.builtin.connector.iotdb.thrift.IoTDBThriftConnector;
import org.apache.iotdb.commons.pipe.agent.plugin.builtin.connector.iotdb.thrift.IoTDBThriftSslConnector;
import org.apache.iotdb.commons.pipe.agent.plugin.builtin.connector.iotdb.thrift.IoTDBThriftSyncConnector;
import org.apache.iotdb.commons.pipe.agent.plugin.builtin.connector.opcua.OpcUaConnector;
import org.apache.iotdb.commons.pipe.agent.plugin.builtin.connector.websocket.WebSocketConnector;
import org.apache.iotdb.commons.pipe.agent.plugin.builtin.connector.writeback.WriteBackConnector;
import org.apache.iotdb.commons.pipe.agent.plugin.builtin.extractor.donothing.DoNothingExtractor;
import org.apache.iotdb.commons.pipe.agent.plugin.builtin.extractor.iotdb.IoTDBExtractor;
import org.apache.iotdb.commons.pipe.agent.plugin.builtin.processor.aggregate.AggregateProcessor;
import org.apache.iotdb.commons.pipe.agent.plugin.builtin.processor.aggregate.StandardStatisticsProcessor;
import org.apache.iotdb.commons.pipe.agent.plugin.builtin.processor.aggregate.TumblingWindowingProcessor;
import org.apache.iotdb.commons.pipe.agent.plugin.builtin.processor.donothing.DoNothingProcessor;
import org.apache.iotdb.commons.pipe.agent.plugin.builtin.processor.downsampling.ChangingValueSamplingProcessor;
import org.apache.iotdb.commons.pipe.agent.plugin.builtin.processor.downsampling.SwingingDoorTrendingSamplingProcessor;
import org.apache.iotdb.commons.pipe.agent.plugin.builtin.processor.downsampling.TumblingTimeSamplingProcessor;
import org.apache.iotdb.commons.pipe.agent.plugin.builtin.processor.pipeconsensus.PipeConsensusProcessor;
import org.apache.iotdb.commons.pipe.agent.plugin.builtin.processor.schemachange.RenameDatabaseProcessor;
import org.apache.iotdb.commons.pipe.agent.plugin.builtin.processor.throwing.ThrowingExceptionProcessor;
import org.apache.iotdb.commons.pipe.agent.plugin.builtin.processor.twostage.TwoStageCountProcessor;

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
  TUMBLING_TIME_SAMPLING_PROCESSOR(
      "tumbling-time-sampling-processor", TumblingTimeSamplingProcessor.class),
  SDT_SAMPLING_PROCESSOR("sdt-sampling-processor", SwingingDoorTrendingSamplingProcessor.class),
  CHANGING_VALUE_SAMPLING_PROCESSOR(
      "changing-value-sampling-processor", ChangingValueSamplingProcessor.class),
  THROWING_EXCEPTION_PROCESSOR("throwing-exception-processor", ThrowingExceptionProcessor.class),
  AGGREGATE_PROCESSOR("aggregate-processor", AggregateProcessor.class),
  COUNT_POINT_PROCESSOR("count-point-processor", TwoStageCountProcessor.class),

  // Hidden-processors, which are plugins of the processors
  STANDARD_STATISTICS_PROCESSOR("standard-statistics-processor", StandardStatisticsProcessor.class),
  TUMBLING_WINDOWING_PROCESSOR("tumbling-windowing-processor", TumblingWindowingProcessor.class),
  PIPE_CONSENSUS_PROCESSOR("pipe-consensus-processor", PipeConsensusProcessor.class),
  RENAME_DATABASE_PROCESSOR("rename-database-processor", RenameDatabaseProcessor.class),

  // connectors
  DO_NOTHING_CONNECTOR("do-nothing-connector", DoNothingConnector.class),
  IOTDB_THRIFT_CONNECTOR("iotdb-thrift-connector", IoTDBThriftConnector.class),
  IOTDB_THRIFT_SSL_CONNECTOR("iotdb-thrift-ssl-connector", IoTDBThriftSslConnector.class),
  IOTDB_THRIFT_SYNC_CONNECTOR("iotdb-thrift-sync-connector", IoTDBThriftSyncConnector.class),
  IOTDB_THRIFT_ASYNC_CONNECTOR("iotdb-thrift-async-connector", IoTDBThriftAsyncConnector.class),
  IOTDB_LEGACY_PIPE_CONNECTOR("iotdb-legacy-pipe-connector", IoTDBLegacyPipeConnector.class),
  IOTDB_AIR_GAP_CONNECTOR("iotdb-air-gap-connector", IoTDBAirGapConnector.class),
  PIPE_CONSENSUS_ASYNC_CONNECTOR(
      "pipe-consensus-async-connector", PipeConsensusAsyncConnector.class),

  WEBSOCKET_CONNECTOR("websocket-connector", WebSocketConnector.class),
  OPC_UA_CONNECTOR("opc-ua-connector", OpcUaConnector.class),
  WRITE_BACK_CONNECTOR("write-back-connector", WriteBackConnector.class),

  DO_NOTHING_SINK("do-nothing-sink", DoNothingConnector.class),
  IOTDB_THRIFT_SINK("iotdb-thrift-sink", IoTDBThriftConnector.class),
  IOTDB_THRIFT_SSL_SINK("iotdb-thrift-ssl-sink", IoTDBThriftSslConnector.class),
  IOTDB_THRIFT_SYNC_SINK("iotdb-thrift-sync-sink", IoTDBThriftSyncConnector.class),
  IOTDB_THRIFT_ASYNC_SINK("iotdb-thrift-async-sink", IoTDBThriftAsyncConnector.class),
  IOTDB_LEGACY_PIPE_SINK("iotdb-legacy-pipe-sink", IoTDBLegacyPipeConnector.class),
  IOTDB_AIR_GAP_SINK("iotdb-air-gap-sink", IoTDBAirGapConnector.class),
  WEBSOCKET_SINK("websocket-sink", WebSocketConnector.class),
  OPC_UA_SINK("opc-ua-sink", OpcUaConnector.class),
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
                  TUMBLING_TIME_SAMPLING_PROCESSOR.getPipePluginName().toUpperCase(),
                  SDT_SAMPLING_PROCESSOR.getPipePluginName().toUpperCase(),
                  CHANGING_VALUE_SAMPLING_PROCESSOR.getPipePluginName().toUpperCase(),
                  THROWING_EXCEPTION_PROCESSOR.getPipePluginName().toUpperCase(),
                  AGGREGATE_PROCESSOR.getPipePluginName().toUpperCase(),
                  COUNT_POINT_PROCESSOR.getPipePluginName().toUpperCase(),
                  STANDARD_STATISTICS_PROCESSOR.getPipePluginName().toUpperCase(),
                  TUMBLING_WINDOWING_PROCESSOR.getPipePluginName().toUpperCase(),
                  PIPE_CONSENSUS_PROCESSOR.getPipePluginName().toUpperCase(),
                  RENAME_DATABASE_PROCESSOR.getPipePluginName().toUpperCase(),
                  // Connectors
                  DO_NOTHING_CONNECTOR.getPipePluginName().toUpperCase(),
                  IOTDB_THRIFT_CONNECTOR.getPipePluginName().toUpperCase(),
                  IOTDB_THRIFT_SSL_CONNECTOR.getPipePluginName().toUpperCase(),
                  IOTDB_THRIFT_SYNC_CONNECTOR.getPipePluginName().toUpperCase(),
                  IOTDB_THRIFT_ASYNC_CONNECTOR.getPipePluginName().toUpperCase(),
                  IOTDB_LEGACY_PIPE_CONNECTOR.getPipePluginName().toUpperCase(),
                  IOTDB_AIR_GAP_CONNECTOR.getPipePluginName().toUpperCase(),
                  WEBSOCKET_CONNECTOR.getPipePluginName().toUpperCase(),
                  OPC_UA_CONNECTOR.getPipePluginName().toUpperCase(),
                  WRITE_BACK_CONNECTOR.getPipePluginName().toUpperCase(),
                  PIPE_CONSENSUS_ASYNC_CONNECTOR.getPipePluginName().toUpperCase(),
                  // Sinks
                  IOTDB_THRIFT_SYNC_SINK.getPipePluginName().toUpperCase(),
                  IOTDB_THRIFT_ASYNC_SINK.getPipePluginName().toUpperCase(),
                  IOTDB_LEGACY_PIPE_SINK.getPipePluginName().toUpperCase(),
                  WEBSOCKET_SINK.getPipePluginName().toUpperCase(),
                  OPC_UA_SINK.getPipePluginName().toUpperCase(),
                  WRITE_BACK_SINK.getPipePluginName().toUpperCase(),
                  SUBSCRIPTION_SINK.getPipePluginName().toUpperCase(),
                  PIPE_CONSENSUS_ASYNC_SINK.getPipePluginName().toUpperCase())));
}
