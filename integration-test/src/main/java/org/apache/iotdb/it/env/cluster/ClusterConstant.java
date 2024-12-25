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

package org.apache.iotdb.it.env.cluster;

import org.apache.commons.lang3.SystemUtils;

import java.io.File;

public class ClusterConstant {

  // System arguments in command line
  // ConfigNode
  public static final String DEFAULT_CONFIG_NODE_NUM = "DefaultConfigNodeNum";
  public static final String CONFIG_NODE_INIT_HEAP_SIZE = "ConfigNodeInitHeapSize";
  public static final String CONFIG_NODE_MAX_HEAP_SIZE = "ConfigNodeMaxHeapSize";
  public static final String CONFIG_NODE_MAX_DIRECT_MEMORY_SIZE = "ConfigNodeMaxDirectMemorySize";
  public static final String DEFAULT_CONFIG_NODE_PROPERTIES = "DefaultConfigNodeProperties";
  public static final String DEFAULT_CONFIG_NODE_COMMON_PROPERTIES =
      "DefaultConfigNodeCommonProperties";

  // DataNode
  public static final String DEFAULT_DATA_NODE_NUM = "DefaultDataNodeNum";
  public static final String DATANODE_INIT_HEAP_SIZE = "DataNodeInitHeapSize";
  public static final String DATANODE_MAX_HEAP_SIZE = "DataNodeMaxHeapSize";
  public static final String DATANODE_MAX_DIRECT_MEMORY_SIZE = "DataNodeMaxDirectMemorySize";
  public static final String DEFAULT_DATA_NODE_PROPERTIES = "DefaultDataNodeProperties";
  public static final String DEFAULT_DATA_NODE_COMMON_PROPERTIES =
      "DefaultDataNodeCommonProperties";

  // Cluster Configurations
  public static final String CLUSTER_CONFIGURATIONS = "ClusterConfigurations";

  // Possible values of Cluster Configurations
  public static final String LIGHT_WEIGHT_STANDALONE_MODE = "LightWeightStandaloneMode";
  public static final String SCALABLE_SINGLE_NODE_MODE = "ScalableSingleNodeMode";
  public static final String HIGH_PERFORMANCE_MODE = "HighPerformanceMode";
  public static final String STRONG_CONSISTENCY_CLUSTER_MODE = "StrongConsistencyClusterMode";
  public static final String PIPE_CONSENSUS_BATCH_MODE = "PipeConsensusBatchMode";
  public static final String PIPE_CONSENSUS_STREAM_MODE = "PipeConsensusStreamMode";

  // System arguments in pom.xml
  public static final String LIGHT_WEIGHT_STANDALONE_MODE_CONFIG_NODE_NUM =
      "lightWeightStandaloneMode.configNodeNumber";
  public static final String LIGHT_WEIGHT_STANDALONE_MODE_DATA_NODE_NUM =
      "lightWeightStandaloneMode.dataNodeNumber";
  public static final String LIGHT_WEIGHT_STANDALONE_MODE_CONFIG_NODE_CONSENSUS =
      "lightWeightStandaloneMode.configNodeConsensus";
  public static final String LIGHT_WEIGHT_STANDALONE_MODE_SCHEMA_REGION_CONSENSUS =
      "lightWeightStandaloneMode.schemaRegionConsensus";
  public static final String LIGHT_WEIGHT_STANDALONE_MODE_DATA_REGION_CONSENSUS =
      "lightWeightStandaloneMode.dataRegionConsensus";
  public static final String LIGHT_WEIGHT_STANDALONE_MODE_SCHEMA_REGION_REPLICA_NUM =
      "lightWeightStandaloneMode.schemaRegionReplicaNumber";
  public static final String LIGHT_WEIGHT_STANDALONE_MODE_DATA_REGION_REPLICA_NUM =
      "lightWeightStandaloneMode.dataRegionReplicaNumber";

  public static final String SCALABLE_SINGLE_NODE_MODE_CONFIG_NODE_NUM =
      "scalableSingleNodeMode.configNodeNumber";
  public static final String SCALABLE_SINGLE_NODE_MODE_DATA_NODE_NUM =
      "scalableSingleNodeMode.dataNodeNumber";
  public static final String SCALABLE_SINGLE_NODE_MODE_CONFIG_NODE_CONSENSUS =
      "scalableSingleNodeMode.configNodeConsensus";
  public static final String SCALABLE_SINGLE_NODE_MODE_SCHEMA_REGION_CONSENSUS =
      "scalableSingleNodeMode.schemaRegionConsensus";
  public static final String SCALABLE_SINGLE_NODE_MODE_DATA_REGION_CONSENSUS =
      "scalableSingleNodeMode.dataRegionConsensus";
  public static final String SCALABLE_SINGLE_NODE_MODE_SCHEMA_REGION_REPLICA_NUM =
      "scalableSingleNodeMode.schemaRegionReplicaNumber";
  public static final String SCALABLE_SINGLE_NODE_MODE_DATA_REGION_REPLICA_NUM =
      "scalableSingleNodeMode.dataRegionReplicaNumber";

  public static final String HIGH_PERFORMANCE_MODE_CONFIG_NODE_NUM =
      "highPerformanceMode.configNodeNumber";
  public static final String HIGH_PERFORMANCE_MODE_DATA_NODE_NUM =
      "highPerformanceMode.dataNodeNumber";
  public static final String HIGH_PERFORMANCE_MODE_CONFIG_NODE_CONSENSUS =
      "highPerformanceMode.configNodeConsensus";
  public static final String HIGH_PERFORMANCE_MODE_SCHEMA_REGION_CONSENSUS =
      "highPerformanceMode.schemaRegionConsensus";
  public static final String HIGH_PERFORMANCE_MODE_DATA_REGION_CONSENSUS =
      "highPerformanceMode.dataRegionConsensus";
  public static final String HIGH_PERFORMANCE_MODE_SCHEMA_REGION_REPLICA_NUM =
      "highPerformanceMode.schemaRegionReplicaNumber";
  public static final String HIGH_PERFORMANCE_MODE_DATA_REGION_REPLICA_NUM =
      "highPerformanceMode.dataRegionReplicaNumber";

  public static final String STRONG_CONSISTENCY_CLUSTER_MODE_CONFIG_NODE_NUM =
      "strongConsistencyClusterMode.configNodeNumber";
  public static final String STRONG_CONSISTENCY_CLUSTER_MODE_DATA_NODE_NUM =
      "strongConsistencyClusterMode.dataNodeNumber";
  public static final String STRONG_CONSISTENCY_CLUSTER_MODE_CONFIG_NODE_CONSENSUS =
      "strongConsistencyClusterMode.configNodeConsensus";
  public static final String STRONG_CONSISTENCY_CLUSTER_MODE_SCHEMA_REGION_CONSENSUS =
      "strongConsistencyClusterMode.schemaRegionConsensus";
  public static final String STRONG_CONSISTENCY_CLUSTER_MODE_DATA_REGION_CONSENSUS =
      "strongConsistencyClusterMode.dataRegionConsensus";
  public static final String STRONG_CONSISTENCY_CLUSTER_MODE_SCHEMA_REGION_REPLICA_NUM =
      "strongConsistencyClusterMode.schemaRegionReplicaNumber";
  public static final String STRONG_CONSISTENCY_CLUSTER_MODE_DATA_REGION_REPLICA_NUM =
      "strongConsistencyClusterMode.dataRegionReplicaNumber";

  public static final String PIPE_CONSENSUS_BATCH_MODE_CONFIG_NODE_NUM =
      "pipeConsensusBatchMode.configNodeNumber";
  public static final String PIPE_CONSENSUS_BATCH_MODE_DATA_NODE_NUM =
      "pipeConsensusBatchMode.dataNodeNumber";
  public static final String PIPE_CONSENSUS_BATCH_MODE_CONFIG_NODE_CONSENSUS =
      "pipeConsensusBatchMode.configNodeConsensus";
  public static final String PIPE_CONSENSUS_BATCH_MODE_SCHEMA_REGION_CONSENSUS =
      "pipeConsensusBatchMode.schemaRegionConsensus";
  public static final String PIPE_CONSENSUS_BATCH_MODE_DATA_REGION_CONSENSUS =
      "pipeConsensusBatchMode.dataRegionConsensus";
  public static final String PIPE_CONSENSUS_BATCH_MODE_SCHEMA_REGION_REPLICA_NUM =
      "pipeConsensusBatchMode.schemaRegionReplicaNumber";
  public static final String PIPE_CONSENSUS_BATCH_MODE_DATA_REGION_REPLICA_NUM =
      "pipeConsensusBatchMode.dataRegionReplicaNumber";

  public static final String PIPE_CONSENSUS_STREAM_MODE_CONFIG_NODE_NUM =
      "pipeConsensusStreamMode.configNodeNumber";
  public static final String PIPE_CONSENSUS_STREAM_MODE_DATA_NODE_NUM =
      "pipeConsensusStreamMode.dataNodeNumber";
  public static final String PIPE_CONSENSUS_STREAM_MODE_CONFIG_NODE_CONSENSUS =
      "pipeConsensusStreamMode.configNodeConsensus";
  public static final String PIPE_CONSENSUS_STREAM_MODE_SCHEMA_REGION_CONSENSUS =
      "pipeConsensusStreamMode.schemaRegionConsensus";
  public static final String PIPE_CONSENSUS_STREAM_MODE_DATA_REGION_CONSENSUS =
      "pipeConsensusStreamMode.dataRegionConsensus";
  public static final String PIPE_CONSENSUS_STREAM_MODE_SCHEMA_REGION_REPLICA_NUM =
      "pipeConsensusStreamMode.schemaRegionReplicaNumber";
  public static final String PIPE_CONSENSUS_STREAM_MODE_DATA_REGION_REPLICA_NUM =
      "pipeConsensusStreamMode.dataRegionReplicaNumber";

  // Property file names
  public static final String IOTDB_SYSTEM_PROPERTIES_FILE = "iotdb-system.properties";

  // Properties' keys
  // Common
  public static final String CONFIG_NODE_CONSENSUS_PROTOCOL_CLASS =
      "config_node_consensus_protocol_class";
  public static final String SCHEMA_REGION_CONSENSUS_PROTOCOL_CLASS =
      "schema_region_consensus_protocol_class";
  public static final String DATA_REGION_CONSENSUS_PROTOCOL_CLASS =
      "data_region_consensus_protocol_class";
  public static final String IOT_CONSENSUS_V2_MODE = "iot_consensus_v2_mode";
  public static final String SCHEMA_REPLICATION_FACTOR = "schema_replication_factor";
  public static final String DATA_REPLICATION_FACTOR = "data_replication_factor";

  public static final String MQTT_HOST = "mqtt_host";
  public static final String MQTT_PORT = "mqtt_port";
  public static final String UDF_LIB_DIR = "udf_lib_dir";
  public static final String TRIGGER_LIB_DIR = "trigger_lib_dir";
  public static final String PIPE_LIB_DIR = "pipe_lib_dir";
  public static final String REST_SERVICE_PORT = "rest_service_port";
  public static final String INFLUXDB_RPC_PORT = "influxdb_rpc_port";

  // ConfigNode
  public static final String CN_SYSTEM_DIR = "cn_system_dir";
  public static final String CN_CONSENSUS_DIR = "cn_consensus_dir";
  public static final String CN_METRIC_IOTDB_REPORTER_HOST = "cn_metric_iotdb_reporter_host";

  public static final String CN_CONNECTION_TIMEOUT_MS = "cn_connection_timeout_ms";

  // DataNode
  public static final String DN_SYSTEM_DIR = "dn_system_dir";
  public static final String DN_DATA_DIRS = "dn_data_dirs";
  public static final String DN_CONSENSUS_DIR = "dn_consensus_dir";
  public static final String DN_WAL_DIRS = "dn_wal_dirs";
  public static final String DN_TRACING_DIR = "dn_tracing_dir";
  public static final String DN_SYNC_DIR = "dn_sync_dir";
  public static final String DN_METRIC_IOTDB_REPORTER_HOST = "dn_metric_iotdb_reporter_host";

  public static final String DN_MPP_DATA_EXCHANGE_PORT = "dn_mpp_data_exchange_port";
  public static final String DN_DATA_REGION_CONSENSUS_PORT = "dn_data_region_consensus_port";
  public static final String DN_SCHEMA_REGION_CONSENSUS_PORT = "dn_schema_region_consensus_port";
  public static final String PIPE_AIR_GAP_RECEIVER_PORT = "pipe_air_gap_receiver_port";
  public static final String MAX_TSBLOCK_SIZE_IN_BYTES = "max_tsblock_size_in_bytes";
  public static final String PAGE_SIZE_IN_BYTE = "page_size_in_byte";
  public static final String DN_JOIN_CLUSTER_RETRY_INTERVAL_MS =
      "dn_join_cluster_retry_interval_ms";
  public static final String DN_CONNECTION_TIMEOUT_MS = "dn_connection_timeout_ms";
  public static final String DN_METRIC_INTERNAL_REPORTER_TYPE = "dn_metric_internal_reporter_type";
  public static final String CONFIG_NODE_RATIS_LOG_APPENDER_BUFFER_SIZE_MAX =
      "config_node_ratis_log_appender_buffer_size_max";
  public static final String WAL_BUFFER_SIZE_IN_BYTE = "wal_buffer_size_in_byte";
  public static final String SCHEMA_REGION_RATIS_LOG_APPENDER_BUFFER_SIZE_MAX =
      "schema_region_ratis_log_appender_buffer_size_max";
  public static final String DATA_REGION_RATIS_LOG_APPENDER_BUFFER_SIZE_MAX =
      "data_region_ratis_log_appender_buffer_size_max";

  // Paths
  public static final String USER_DIR = "user.dir";
  public static final String TARGET = "target";
  public static final String PYTHON_PATH = "venv/bin/python3";

  public static final String DATA_NODE_NAME = "DataNode";

  public static final String AI_NODE_NAME = "AINode";

  public static final String LOCK_FILE_PATH =
      System.getProperty(USER_DIR) + File.separator + TARGET + File.separator + "lock-";
  public static final String TEMPLATE_NODE_PATH =
      System.getProperty(USER_DIR) + File.separator + TARGET + File.separator + "template-node";
  public static final String TEMPLATE_NODE_LIB_PATH =
      System.getProperty(USER_DIR)
          + File.separator
          + TARGET
          + File.separator
          + "template-node-share"
          + File.separator
          + "lib"
          + File.separator
          + "*";

  // Env Constant
  public static final int NODE_START_TIMEOUT = 100;
  public static final int NODE_NETWORK_TIMEOUT_MS = 0;
  public static final String ZERO_TIME_ZONE = "GMT+0";

  public static final String DELIMITER = ",";
  public static final String TAB = "  ";
  public static final String DIR_TIME_REPLACEMENT = ".";
  public static final String HYPHEN = "-";

  public static final String SIMPLE_CONSENSUS_STR = "Simple";
  public static final String RATIS_CONSENSUS_STR = "Ratis";
  public static final String IOT_CONSENSUS_STR = "IoT";
  public static final String IOT_CONSENSUS_V2_STR = "IoTV2";

  public static final String JAVA_CMD =
      System.getProperty("java.home")
          + File.separator
          + "bin"
          + File.separator
          + (SystemUtils.IS_OS_WINDOWS ? "java.exe" : "java");
  public static final String MAIN_CLASS_NAME = "org.apache.iotdb.db.service.DataNode";

  private ClusterConstant() {
    throw new IllegalStateException("Utility class");
  }
}
