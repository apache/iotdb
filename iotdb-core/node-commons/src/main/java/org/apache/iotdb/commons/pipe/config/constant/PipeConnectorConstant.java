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

package org.apache.iotdb.commons.pipe.config.constant;

import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.pipe.config.PipeConfig;

import com.github.luben.zstd.Zstd;

import java.io.File;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static org.apache.iotdb.commons.conf.IoTDBConstant.MB;

public class PipeConnectorConstant {

  public static final String CONNECTOR_KEY = "connector";
  public static final String SINK_KEY = "sink";

  public static final String CONNECTOR_IOTDB_IP_KEY = "connector.ip";
  public static final String SINK_IOTDB_IP_KEY = "sink.ip";
  public static final String CONNECTOR_IOTDB_HOST_KEY = "connector.host";
  public static final String SINK_IOTDB_HOST_KEY = "sink.host";
  public static final String CONNECTOR_IOTDB_PORT_KEY = "connector.port";
  public static final String SINK_IOTDB_PORT_KEY = "sink.port";
  public static final String CONNECTOR_IOTDB_NODE_URLS_KEY = "connector.node-urls";
  public static final String SINK_IOTDB_NODE_URLS_KEY = "sink.node-urls";

  public static final String SINK_IOTDB_SSL_ENABLE_KEY = "sink.ssl.enable";
  public static final String SINK_IOTDB_SSL_TRUST_STORE_PATH_KEY = "sink.ssl.trust-store-path";
  public static final String SINK_IOTDB_SSL_TRUST_STORE_PWD_KEY = "sink.ssl.trust-store-pwd";

  public static final String CONNECTOR_IOTDB_PARALLEL_TASKS_KEY = "connector.parallel.tasks";
  public static final String SINK_IOTDB_PARALLEL_TASKS_KEY = "sink.parallel.tasks";
  public static final int CONNECTOR_IOTDB_PARALLEL_TASKS_DEFAULT_VALUE =
      PipeConfig.getInstance().getPipeSubtaskExecutorMaxThreadNum();

  public static final String CONNECTOR_REALTIME_FIRST_KEY = "connector.realtime-first";
  public static final String SINK_REALTIME_FIRST_KEY = "sink.realtime-first";
  public static final boolean CONNECTOR_REALTIME_FIRST_DEFAULT_VALUE = true;

  public static final String CONNECTOR_IOTDB_BATCH_MODE_ENABLE_KEY = "connector.batch.enable";
  public static final String SINK_IOTDB_BATCH_MODE_ENABLE_KEY = "sink.batch.enable";
  public static final boolean CONNECTOR_IOTDB_BATCH_MODE_ENABLE_DEFAULT_VALUE = true;

  public static final String CONNECTOR_IOTDB_BATCH_DELAY_KEY = "connector.batch.max-delay-seconds";
  public static final String SINK_IOTDB_BATCH_DELAY_KEY = "sink.batch.max-delay-seconds";
  public static final int CONNECTOR_IOTDB_PLAIN_BATCH_DELAY_DEFAULT_VALUE = 1;
  public static final int CONNECTOR_IOTDB_TS_FILE_BATCH_DELAY_DEFAULT_VALUE = 5;

  public static final String CONNECTOR_IOTDB_BATCH_SIZE_KEY = "connector.batch.size-bytes";
  public static final String SINK_IOTDB_BATCH_SIZE_KEY = "sink.batch.size-bytes";
  public static final long CONNECTOR_IOTDB_PLAIN_BATCH_SIZE_DEFAULT_VALUE = 16 * MB;
  public static final long CONNECTOR_IOTDB_TS_FILE_BATCH_SIZE_DEFAULT_VALUE = 80 * MB;

  public static final String CONNECTOR_IOTDB_USER_KEY = "connector.user";
  public static final String SINK_IOTDB_USER_KEY = "sink.user";
  public static final String CONNECTOR_IOTDB_USER_DEFAULT_VALUE = "root";

  public static final String CONNECTOR_IOTDB_PASSWORD_KEY = "connector.password";
  public static final String SINK_IOTDB_PASSWORD_KEY = "sink.password";
  public static final String CONNECTOR_IOTDB_PASSWORD_DEFAULT_VALUE = "root";

  public static final String CONNECTOR_EXCEPTION_DATA_CONVERT_ON_TYPE_MISMATCH_KEY =
      "connector.exception.data.convert-on-type-mismatch";
  public static final String SINK_EXCEPTION_DATA_CONVERT_ON_TYPE_MISMATCH_KEY =
      "sink.exception.data.convert-on-type-mismatch";
  public static final boolean CONNECTOR_EXCEPTION_DATA_CONVERT_ON_TYPE_MISMATCH_DEFAULT_VALUE =
      true;

  public static final String CONNECTOR_EXCEPTION_CONFLICT_RESOLVE_STRATEGY_KEY =
      "connector.exception.conflict.resolve-strategy";
  public static final String SINK_EXCEPTION_CONFLICT_RESOLVE_STRATEGY_KEY =
      "sink.exception.conflict.resolve-strategy";
  public static final String CONNECTOR_EXCEPTION_CONFLICT_RESOLVE_STRATEGY_DEFAULT_VALUE = "retry";

  public static final String CONNECTOR_EXCEPTION_CONFLICT_RETRY_MAX_TIME_SECONDS_KEY =
      "connector.exception.conflict.retry-max-time-seconds";
  public static final String SINK_EXCEPTION_CONFLICT_RETRY_MAX_TIME_SECONDS_KEY =
      "sink.exception.conflict.retry-max-time-seconds";
  public static final long CONNECTOR_EXCEPTION_CONFLICT_RETRY_MAX_TIME_SECONDS_DEFAULT_VALUE = 60;

  public static final String CONNECTOR_EXCEPTION_CONFLICT_RECORD_IGNORED_DATA_KEY =
      "connector.exception.conflict.record-ignored-data";
  public static final String SINK_EXCEPTION_CONFLICT_RECORD_IGNORED_DATA_KEY =
      "sink.exception.conflict.record-ignored-data";
  public static final boolean CONNECTOR_EXCEPTION_CONFLICT_RECORD_IGNORED_DATA_DEFAULT_VALUE = true;

  public static final String CONNECTOR_EXCEPTION_OTHERS_RETRY_MAX_TIME_SECONDS_KEY =
      "connector.exception.others.retry-max-time-seconds";
  public static final String SINK_EXCEPTION_OTHERS_RETRY_MAX_TIME_SECONDS_KEY =
      "sink.exception.others.retry-max-time-seconds";
  public static final long CONNECTOR_EXCEPTION_OTHERS_RETRY_MAX_TIME_SECONDS_DEFAULT_VALUE = -1;

  public static final String CONNECTOR_EXCEPTION_OTHERS_RECORD_IGNORED_DATA_KEY =
      "connector.exception.others.record-ignored-data";
  public static final String SINK_EXCEPTION_OTHERS_RECORD_IGNORED_DATA_KEY =
      "sink.exception.others.record-ignored-data";
  public static final boolean CONNECTOR_EXCEPTION_OTHERS_RECORD_IGNORED_DATA_DEFAULT_VALUE = true;

  public static final String CONNECTOR_AIR_GAP_E_LANGUAGE_ENABLE_KEY =
      "connector.air-gap.e-language.enable";
  public static final String SINK_AIR_GAP_E_LANGUAGE_ENABLE_KEY = "sink.air-gap.e-language.enable";
  public static final boolean CONNECTOR_AIR_GAP_E_LANGUAGE_ENABLE_DEFAULT_VALUE = false;

  public static final String CONNECTOR_AIR_GAP_HANDSHAKE_TIMEOUT_MS_KEY =
      "connector.air-gap.handshake-timeout-ms";
  public static final String SINK_AIR_GAP_HANDSHAKE_TIMEOUT_MS_KEY =
      "sink.air-gap.handshake-timeout-ms";
  public static final int CONNECTOR_AIR_GAP_HANDSHAKE_TIMEOUT_MS_DEFAULT_VALUE = 5000;

  public static final String CONNECTOR_IOTDB_SYNC_CONNECTOR_VERSION_KEY = "connector.version";
  public static final String SINK_IOTDB_SYNC_CONNECTOR_VERSION_KEY = "sink.version";
  public static final String CONNECTOR_IOTDB_SYNC_CONNECTOR_VERSION_DEFAULT_VALUE = "1.1";

  public static final String CONNECTOR_WEBSOCKET_PORT_KEY = "connector.websocket.port";
  public static final String SINK_WEBSOCKET_PORT_KEY = "sink.websocket.port";
  public static final int CONNECTOR_WEBSOCKET_PORT_DEFAULT_VALUE = 8080;

  public static final String CONNECTOR_OPC_UA_MODEL_KEY = "connector.opcua.model";
  public static final String SINK_OPC_UA_MODEL_KEY = "sink.opcua.model";
  public static final String CONNECTOR_OPC_UA_MODEL_CLIENT_SERVER_VALUE = "client-server";
  public static final String CONNECTOR_OPC_UA_MODEL_PUB_SUB_VALUE = "pub-sub";
  public static final String CONNECTOR_OPC_UA_MODEL_DEFAULT_VALUE =
      CONNECTOR_OPC_UA_MODEL_PUB_SUB_VALUE;

  public static final String CONNECTOR_OPC_UA_TCP_BIND_PORT_KEY = "connector.opcua.tcp.port";
  public static final String SINK_OPC_UA_TCP_BIND_PORT_KEY = "sink.opcua.tcp.port";
  public static final int CONNECTOR_OPC_UA_TCP_BIND_PORT_DEFAULT_VALUE = 12686;

  public static final String CONNECTOR_OPC_UA_HTTPS_BIND_PORT_KEY = "connector.opcua.https.port";
  public static final String SINK_OPC_UA_HTTPS_BIND_PORT_KEY = "sink.opcua.https.port";
  public static final int CONNECTOR_OPC_UA_HTTPS_BIND_PORT_DEFAULT_VALUE = 8443;

  public static final String CONNECTOR_OPC_UA_SECURITY_DIR_KEY = "connector.opcua.security.dir";
  public static final String SINK_OPC_UA_SECURITY_DIR_KEY = "sink.opcua.security.dir";
  public static final String CONNECTOR_OPC_UA_SECURITY_DIR_DEFAULT_VALUE =
      CommonDescriptor.getInstance().getConfDir() != null
          ? CommonDescriptor.getInstance().getConfDir() + File.separatorChar + "opc_security"
          : System.getProperty("user.home") + File.separatorChar + "iotdb_opc_security";

  public static final String CONNECTOR_OPC_UA_ENABLE_ANONYMOUS_ACCESS_KEY =
      "connector.opcua.enable-anonymous-access";
  public static final String SINK_OPC_UA_ENABLE_ANONYMOUS_ACCESS_KEY =
      "sink.opcua.enable-anonymous-access";
  public static final boolean CONNECTOR_OPC_UA_ENABLE_ANONYMOUS_ACCESS_DEFAULT_VALUE = true;

  public static final String CONNECTOR_LEADER_CACHE_ENABLE_KEY = "connector.leader-cache.enable";
  public static final String SINK_LEADER_CACHE_ENABLE_KEY = "sink.leader-cache.enable";
  public static final boolean CONNECTOR_LEADER_CACHE_ENABLE_DEFAULT_VALUE = true;

  public static final String CONNECTOR_LOAD_BALANCE_STRATEGY_KEY =
      "connector.load-balance-strategy";
  public static final String SINK_LOAD_BALANCE_STRATEGY_KEY = "sink.load-balance-strategy";
  public static final String CONNECTOR_LOAD_BALANCE_ROUND_ROBIN_STRATEGY = "round-robin";
  public static final String CONNECTOR_LOAD_BALANCE_RANDOM_STRATEGY = "random";
  public static final String CONNECTOR_LOAD_BALANCE_PRIORITY_STRATEGY = "priority";
  public static final Set<String> CONNECTOR_LOAD_BALANCE_STRATEGY_SET =
      Collections.unmodifiableSet(
          new HashSet<>(
              Arrays.asList(
                  CONNECTOR_LOAD_BALANCE_ROUND_ROBIN_STRATEGY,
                  CONNECTOR_LOAD_BALANCE_RANDOM_STRATEGY,
                  CONNECTOR_LOAD_BALANCE_PRIORITY_STRATEGY)));

  public static final String CONNECTOR_COMPRESSOR_KEY = "connector.compressor";
  public static final String SINK_COMPRESSOR_KEY = "sink.compressor";
  public static final String CONNECTOR_COMPRESSOR_DEFAULT_VALUE = "";
  public static final String CONNECTOR_COMPRESSOR_SNAPPY = "snappy";
  public static final String CONNECTOR_COMPRESSOR_GZIP = "gzip";
  public static final String CONNECTOR_COMPRESSOR_LZ4 = "lz4";
  public static final String CONNECTOR_COMPRESSOR_ZSTD = "zstd";
  public static final String CONNECTOR_COMPRESSOR_LZMA2 = "lzma2";
  public static final Set<String> CONNECTOR_COMPRESSOR_SET =
      Collections.unmodifiableSet(
          new HashSet<>(
              Arrays.asList(
                  CONNECTOR_COMPRESSOR_SNAPPY,
                  CONNECTOR_COMPRESSOR_GZIP,
                  CONNECTOR_COMPRESSOR_LZ4,
                  CONNECTOR_COMPRESSOR_ZSTD,
                  CONNECTOR_COMPRESSOR_LZMA2)));

  public static final String CONNECTOR_COMPRESSOR_ZSTD_LEVEL_KEY =
      "connector.compressor.zstd.level";
  public static final String SINK_COMPRESSOR_ZSTD_LEVEL_KEY = "sink.compressor.zstd.level";
  public static final int CONNECTOR_COMPRESSOR_ZSTD_LEVEL_DEFAULT_VALUE =
      Zstd.defaultCompressionLevel();
  public static final int CONNECTOR_COMPRESSOR_ZSTD_LEVEL_MIN_VALUE = Zstd.minCompressionLevel();
  public static final int CONNECTOR_COMPRESSOR_ZSTD_LEVEL_MAX_VALUE = Zstd.maxCompressionLevel();

  public static final String CONNECTOR_RATE_LIMIT_KEY = "connector.rate-limit-bytes-per-second";
  public static final String SINK_RATE_LIMIT_KEY = "sink.rate-limit-bytes-per-second";
  public static final double CONNECTOR_RATE_LIMIT_DEFAULT_VALUE = -1;

  public static final String CONNECTOR_FORMAT_KEY = "connector.format";
  public static final String SINK_FORMAT_KEY = "sink.format";
  public static final String CONNECTOR_FORMAT_TABLET_VALUE = "tablet";
  public static final String CONNECTOR_FORMAT_TS_FILE_VALUE = "tsfile";
  public static final String CONNECTOR_FORMAT_HYBRID_VALUE = "hybrid";

  public static final String SINK_TOPIC_KEY = "sink.topic";
  public static final String SINK_CONSUMER_GROUP_KEY = "sink.consumer-group";

  public static final String CONNECTOR_CONSENSUS_GROUP_ID_KEY = "connector.consensus.group-id";
  public static final String CONNECTOR_CONSENSUS_PIPE_NAME = "connector.consensus.pipe-name";

  public static final String CONNECTOR_LOAD_TSFILE_STRATEGY_KEY = "connector.load-tsfile-strategy";
  public static final String SINK_LOAD_TSFILE_STRATEGY_KEY = "sink.load-tsfile-strategy";
  public static final String CONNECTOR_LOAD_TSFILE_STRATEGY_ASYNC_VALUE = "async";
  public static final String CONNECTOR_LOAD_TSFILE_STRATEGY_SYNC_VALUE = "sync";
  public static final Set<String> CONNECTOR_LOAD_TSFILE_STRATEGY_SET =
      Collections.unmodifiableSet(
          new HashSet<>(
              Arrays.asList(
                  CONNECTOR_LOAD_TSFILE_STRATEGY_ASYNC_VALUE,
                  CONNECTOR_LOAD_TSFILE_STRATEGY_SYNC_VALUE)));

  private PipeConnectorConstant() {
    throw new IllegalStateException("Utility class");
  }
}
