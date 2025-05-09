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

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

public class PipeExtractorConstant {

  public static final String EXTRACTOR_KEY = "extractor";
  public static final String SOURCE_KEY = "source";

  public static final String EXTRACTOR_CAPTURE_TREE_KEY = "extractor.capture.tree";
  public static final String SOURCE_CAPTURE_TREE_KEY = "source.capture.tree";
  public static final String EXTRACTOR_CAPTURE_TABLE_KEY = "extractor.capture.table";
  public static final String SOURCE_CAPTURE_TABLE_KEY = "source.capture.table";

  public static final String EXTRACTOR_INCLUSION_KEY = "extractor.inclusion";
  public static final String SOURCE_INCLUSION_KEY = "source.inclusion";
  public static final String EXTRACTOR_INCLUSION_DEFAULT_VALUE = "data.insert";

  public static final String EXTRACTOR_EXCLUSION_KEY = "extractor.inclusion.exclusion";
  public static final String SOURCE_EXCLUSION_KEY = "source.inclusion.exclusion";
  public static final String EXTRACTOR_EXCLUSION_DEFAULT_VALUE = "";

  public static final String EXTRACTOR_MODE_KEY = "extractor.mode";
  public static final String SOURCE_MODE_KEY = "source.mode";
  public static final String EXTRACTOR_MODE_QUERY_VALUE = "query";
  public static final String EXTRACTOR_MODE_SNAPSHOT_VALUE = "snapshot";
  public static final String EXTRACTOR_MODE_SUBSCRIBE_VALUE = "subscribe";
  public static final String EXTRACTOR_MODE_LIVE_VALUE = "live";
  public static final String EXTRACTOR_MODE_DEFAULT_VALUE = EXTRACTOR_MODE_LIVE_VALUE;

  public static final String EXTRACTOR_PATTERN_KEY = "extractor.pattern";
  public static final String SOURCE_PATTERN_KEY = "source.pattern";
  public static final String EXTRACTOR_PATH_KEY = "extractor.path";
  public static final String SOURCE_PATH_KEY = "source.path";
  public static final String EXTRACTOR_PATTERN_FORMAT_KEY = "extractor.pattern.format";
  public static final String SOURCE_PATTERN_FORMAT_KEY = "source.pattern.format";
  public static final String EXTRACTOR_PATTERN_FORMAT_PREFIX_VALUE = "prefix";
  public static final String EXTRACTOR_PATTERN_FORMAT_IOTDB_VALUE = "iotdb";
  public static final String EXTRACTOR_PATTERN_PREFIX_DEFAULT_VALUE = "root";
  public static final String EXTRACTOR_PATTERN_IOTDB_DEFAULT_VALUE = "root.**";
  public static final String EXTRACTOR_DATABASE_NAME_KEY = "extractor.database-name";
  public static final String SOURCE_DATABASE_NAME_KEY = "source.database-name";
  public static final String EXTRACTOR_TABLE_NAME_KEY = "extractor.table-name";
  public static final String SOURCE_TABLE_NAME_KEY = "source.table-name";
  public static final String EXTRACTOR_DATABASE_NAME_DEFAULT_VALUE = ".*";
  public static final String EXTRACTOR_TABLE_NAME_DEFAULT_VALUE = ".*";
  public static final String EXTRACTOR_DATABASE_KEY = "extractor.database";
  public static final String SOURCE_DATABASE_KEY = "source.database";
  public static final String EXTRACTOR_TABLE_KEY = "extractor.table";
  public static final String SOURCE_TABLE_KEY = "source.table";

  public static final String EXTRACTOR_FORWARDING_PIPE_REQUESTS_KEY =
      "extractor.forwarding-pipe-requests";
  public static final String SOURCE_FORWARDING_PIPE_REQUESTS_KEY =
      "source.forwarding-pipe-requests";
  public static final boolean EXTRACTOR_FORWARDING_PIPE_REQUESTS_DEFAULT_VALUE = true;

  public static final String EXTRACTOR_HISTORY_ENABLE_KEY = "extractor.history.enable";
  public static final String SOURCE_HISTORY_ENABLE_KEY = "source.history.enable";
  public static final boolean EXTRACTOR_HISTORY_ENABLE_DEFAULT_VALUE = true;
  public static final String EXTRACTOR_HISTORY_START_TIME_KEY = "extractor.history.start-time";
  public static final String SOURCE_HISTORY_START_TIME_KEY = "source.history.start-time";
  public static final String EXTRACTOR_HISTORY_END_TIME_KEY = "extractor.history.end-time";
  public static final String SOURCE_HISTORY_END_TIME_KEY = "source.history.end-time";
  public static final String EXTRACTOR_HISTORY_LOOSE_RANGE_KEY = "extractor.history.loose-range";
  public static final String SOURCE_HISTORY_LOOSE_RANGE_KEY = "source.history.loose-range";
  public static final String EXTRACTOR_HISTORY_LOOSE_RANGE_TIME_VALUE = "time";
  public static final String EXTRACTOR_HISTORY_LOOSE_RANGE_PATH_VALUE = "path";
  public static final String EXTRACTOR_HISTORY_LOOSE_RANGE_ALL_VALUE = "all";
  public static final String EXTRACTOR_HISTORY_LOOSE_RANGE_DEFAULT_VALUE = "";
  public static final String EXTRACTOR_MODS_ENABLE_KEY = "extractor.mods.enable";
  public static final String SOURCE_MODS_ENABLE_KEY = "source.mods.enable";
  public static final boolean EXTRACTOR_MODS_ENABLE_DEFAULT_VALUE = false;
  public static final String EXTRACTOR_MODS_KEY = "extractor.mods";
  public static final String SOURCE_MODS_KEY = "source.mods";
  public static final boolean EXTRACTOR_MODS_DEFAULT_VALUE = EXTRACTOR_MODS_ENABLE_DEFAULT_VALUE;

  public static final String EXTRACTOR_REALTIME_ENABLE_KEY = "extractor.realtime.enable";
  public static final String SOURCE_REALTIME_ENABLE_KEY = "source.realtime.enable";
  public static final boolean EXTRACTOR_REALTIME_ENABLE_DEFAULT_VALUE = true;
  public static final String EXTRACTOR_REALTIME_MODE_KEY = "extractor.realtime.mode";
  public static final String SOURCE_REALTIME_MODE_KEY = "source.realtime.mode";
  public static final String EXTRACTOR_REALTIME_MODE_HYBRID_VALUE = "hybrid";
  public static final String EXTRACTOR_REALTIME_MODE_FILE_VALUE = "file";
  public static final String EXTRACTOR_REALTIME_MODE_LOG_VALUE = "log";
  public static final String EXTRACTOR_REALTIME_MODE_FORCED_LOG_VALUE = "forced-log";
  public static final String EXTRACTOR_REALTIME_MODE_STREAM_MODE_VALUE = "stream";
  public static final String EXTRACTOR_REALTIME_MODE_BATCH_MODE_VALUE = "batch";
  public static final String EXTRACTOR_REALTIME_LOOSE_RANGE_KEY = "extractor.realtime.loose-range";
  public static final String SOURCE_REALTIME_LOOSE_RANGE_KEY = "source.realtime.loose-range";
  public static final String EXTRACTOR_REALTIME_LOOSE_RANGE_TIME_VALUE = "time";
  public static final String EXTRACTOR_REALTIME_LOOSE_RANGE_PATH_VALUE = "path";
  public static final String EXTRACTOR_REALTIME_LOOSE_RANGE_ALL_VALUE = "all";
  public static final String EXTRACTOR_REALTIME_LOOSE_RANGE_DEFAULT_VALUE = "";

  public static final String EXTRACTOR_MODE_STREAMING_KEY = "extractor.mode.streaming";
  public static final String SOURCE_MODE_STREAMING_KEY = "source.mode.streaming";
  public static final boolean EXTRACTOR_MODE_STREAMING_DEFAULT_VALUE = true;
  public static final String EXTRACTOR_MODE_STRICT_KEY = "extractor.mode.strict";
  public static final String SOURCE_MODE_STRICT_KEY = "source.mode.strict";
  public static final boolean EXTRACTOR_MODE_STRICT_DEFAULT_VALUE = true;
  public static final String EXTRACTOR_MODE_SNAPSHOT_KEY = "extractor.mode.snapshot";
  public static final String SOURCE_MODE_SNAPSHOT_KEY = "source.mode.snapshot";
  public static final boolean EXTRACTOR_MODE_SNAPSHOT_DEFAULT_VALUE = false;
  public static final String EXTRACTOR_MODE_DOUBLE_LIVING_KEY = "extractor.mode.double-living";
  public static final String SOURCE_MODE_DOUBLE_LIVING_KEY = "source.mode.double-living";
  public static final boolean EXTRACTOR_MODE_DOUBLE_LIVING_DEFAULT_VALUE = false;

  public static final String EXTRACTOR_START_TIME_KEY = "extractor.start-time";
  public static final String SOURCE_START_TIME_KEY = "source.start-time";
  public static final String EXTRACTOR_END_TIME_KEY = "extractor.end-time";
  public static final String SOURCE_END_TIME_KEY = "source.end-time";
  public static final String NOW_TIME_VALUE = "now";

  public static final String _EXTRACTOR_WATERMARK_INTERVAL_KEY = "extractor.watermark-interval-ms";
  public static final String _SOURCE_WATERMARK_INTERVAL_KEY = "source.watermark-interval-ms";
  public static final long EXTRACTOR_WATERMARK_INTERVAL_DEFAULT_VALUE = -1; // -1 means no watermark
  public static final String EXTRACTOR_WATERMARK_INTERVAL_KEY = "extractor.watermark.interval-ms";
  public static final String SOURCE_WATERMARK_INTERVAL_KEY = "source.watermark.interval-ms";

  public static final String EXTRACTOR_IOTDB_USER_KEY = "extractor.user";
  public static final String SOURCE_IOTDB_USER_KEY = "source.user";
  public static final String EXTRACTOR_IOTDB_USERNAME_KEY = "extractor.username";
  public static final String SOURCE_IOTDB_USERNAME_KEY = "source.username";

  public static final String EXTRACTOR_IOTDB_PASSWORD_KEY = "extractor.password";
  public static final String SOURCE_IOTDB_PASSWORD_KEY = "source.password";

  public static final String EXTRACTOR_SKIP_IF_KEY = "extractor.skipif";
  public static final String SOURCE_SKIP_IF_KEY = "source.skipif";
  public static final String EXTRACTOR_IOTDB_SKIP_IF_NO_PRIVILEGES = "no-privileges";

  ////////////////// external sources ////////////////
  public static final String EXTERNAL_EXTRACTOR_BALANCE_STRATEGY_KEY = "extractor.balance-strategy";
  public static final String EXTERNAL_SOURCE_BALANCE_STRATEGY_KEY = "source.balance-strategy";
  public static final String EXTERNAL_EXTRACTOR_BALANCE_PROPORTION_STRATEGY = "proportion";
  public static final Set<String> EXTERNAL_EXTRACTOR_BALANCE_STRATEGY_SET =
      Collections.unmodifiableSet(
          new HashSet<>(Arrays.asList(EXTERNAL_EXTRACTOR_BALANCE_PROPORTION_STRATEGY)));
  public static final String EXTERNAL_EXTRACTOR_PARALLELISM_KEY = "extractor.parallelism";
  public static final String EXTERNAL_SOURCE_PARALLELISM_KEY = "source.parallelism";
  public static final int EXTERNAL_EXTRACTOR_PARALLELISM_DEFAULT_VALUE = 1;
  public static final String EXTERNAL_EXTRACTOR_SINGLE_INSTANCE_PER_NODE_KEY =
      "extractor.single-mode";
  public static final String EXTERNAL_SOURCE_SINGLE_INSTANCE_PER_NODE_KEY = "source.single-mode";
  public static final boolean EXTERNAL_EXTRACTOR_SINGLE_INSTANCE_PER_NODE_DEFAULT_VALUE = true;

  public static final String MQTT_BROKER_HOST_KEY = "mqtt.host";
  public static final String MQTT_BROKER_HOST_DEFAULT_VALUE = "127.0.0.1";
  public static final String MQTT_BROKER_PORT_KEY = "mqtt.port";
  public static final String MQTT_BROKER_PORT_DEFAULT_VALUE = "1883";
  public static final String MQTT_BROKER_INTERCEPTOR_THREAD_POOL_SIZE_KEY = "mqtt.pool-size";
  public static final int MQTT_BROKER_INTERCEPTOR_THREAD_POOL_SIZE_DEFAULT_VALUE = 1;
  public static final String MQTT_DATA_PATH_PROPERTY_NAME_KEY = "mqtt.data-path";
  public static final String MQTT_DATA_PATH_PROPERTY_NAME_DEFAULT_VALUE = "data/";
  public static final String MQTT_IMMEDIATE_BUFFER_FLUSH_PROPERTY_NAME_KEY = "mqtt.immediate-flush";
  public static final boolean MQTT_IMMEDIATE_BUFFER_FLUSH_PROPERTY_NAME_DEFAULT_VALUE = true;
  public static final String MQTT_ALLOW_ANONYMOUS_PROPERTY_NAME_KEY = "mqtt.allow-anonymous";
  public static final boolean MQTT_ALLOW_ANONYMOUS_PROPERTY_NAME_DEFAULT_VALUE = false;
  public static final String MQTT_ALLOW_ZERO_BYTE_CLIENT_ID_PROPERTY_NAME_KEY =
      "mqtt.allow-zero-byte-client-id";
  public static final boolean MQTT_ALLOW_ZERO_BYTE_CLIENT_ID_PROPERTY_NAME_DEFAULT_VALUE = true;
  public static final String MQTT_NETTY_MAX_BYTES_PROPERTY_NAME_KEY = "mqtt.max-message-size";
  public static final long MQTT_NETTY_MAX_BYTES_PROPERTY_NAME_DEFAULT_VALUE = 1048576;
  public static final String MQTT_PAYLOAD_FORMATTER_KEY = "mqtt.payload-formatter";
  public static final String MQTT_PAYLOAD_FORMATTER_DEFAULT_VALUE = "json";

  ///////////////////// pipe consensus /////////////////////

  public static final String EXTRACTOR_CONSENSUS_GROUP_ID_KEY = "extractor.consensus.group-id";
  public static final String EXTRACTOR_CONSENSUS_SENDER_DATANODE_ID_KEY =
      "extractor.consensus.sender-dn-id";
  public static final String EXTRACTOR_CONSENSUS_RECEIVER_DATANODE_ID_KEY =
      "extractor.consensus.receiver-dn-id";

  private PipeExtractorConstant() {
    throw new IllegalStateException("Utility class");
  }
}
