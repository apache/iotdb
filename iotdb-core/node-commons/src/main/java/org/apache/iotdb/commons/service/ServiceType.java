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

package org.apache.iotdb.commons.service;

public enum ServiceType {
  STORAGE_ENGINE_SERVICE("Storage Engine ServerService", "StorageEngine"),
  JMX_SERVICE("JMX ServerService", "JMX ServerService"),
  METRIC_SERVICE("Metrics ServerService", "MetricService"),
  RPC_SERVICE("RPC ServerService", "RPCService"),
  INFLUX_SERVICE("InfluxDB Protocol Service", "InfluxDB Protocol"),
  MQTT_SERVICE("MQTTService", "MqttService"),
  AIR_GAP_SERVICE("AirGapService", "AirGapService"),
  MONITOR_SERVICE("Monitor ServerService", "Monitor"),
  STAT_MONITOR_SERVICE("Statistics ServerService", "StatMonitorService"),
  WAL_SERVICE("WAL ServerService", "WalService"),
  CLOSE_MERGE_SERVICE("Close&Merge ServerService", "CloseMergeService"),
  JVM_MEM_CONTROL_SERVICE("Memory Controller", "JvmMemControlService"),
  AUTHORIZATION_SERVICE("Authorization ServerService", "AuthService"),
  FILE_READER_MANAGER_SERVICE("File reader manager ServerService", "FileReaderManagerService"),
  SETTLE_SERVICE("SETTLE DataService", "SettleService"),
  SYNC_RPC_SERVICE("Sync RPC ServerService", ""),
  SYNC_SERVICE("Sync Service", "SyncService"),
  MERGE_SERVICE("Merge Manager", "Merge Manager"),
  COMPACTION_SERVICE("Compaction Manager", "Compaction Manager"),
  COMPACTION_SCHEDULE_SERVICE("Compaction Schedule Manager", "Compaction Schedule Manger"),
  REPAIR_DATA_SERVICE("Repair Manager", "Repair Manager"),
  PERFORMANCE_STATISTIC_SERVICE("PERFORMANCE_STATISTIC_SERVICE", "PERFORMANCE_STATISTIC_SERVICE"),
  TVLIST_ALLOCATOR_SERVICE("TVList Allocator", ""),
  UDF_CLASSLOADER_MANAGER_SERVICE("UDF Classloader Manager Service", "UdfClassLoader"),
  TEMPORARY_QUERY_DATA_FILE_SERVICE("Temporary Query Data File Service", "TempQueryDataFile"),
  TRIGGER_REGISTRATION_SERVICE_OLD(
      "Old Standalone Trigger Registration Service", "TriggerRegistration"),
  CACHE_HIT_RATIO_DISPLAY_SERVICE(
      "CACHE_HIT_RATIO_DISPLAY_SERVICE",
      generateJmxName("org.apache.iotdb.service", "Cache Hit Ratio")),
  QUERY_TIME_MANAGER("Query time manager", "Query time"),

  FLUSH_SERVICE(
      "Flush ServerService",
      generateJmxName("org.apache.iotdb.db.storageengine.pool", "Flush Manager")),
  CLUSTER_MONITOR_SERVICE("Cluster Monitor ServerService", "Cluster Monitor"),
  SYSTEMINFO_SERVICE("MemTable Monitor Service", "MemTable, Monitor"),
  CONTINUOUS_QUERY_SERVICE("Continuous Query Service", "Continuous Query Service"),
  CLUSTER_INFO_SERVICE("Cluster Monitor Service (thrift-based)", "Cluster Monitor-Thrift"),
  CLUSTER_RPC_SERVICE("Cluster RPC Service", "ClusterRPCService"),
  CLUSTER_META_RPC_SERVICE("Cluster Meta RPC Service", "ClusterMetaRPCService"),
  CLUSTER_META_HEART_BEAT_RPC_SERVICE(
      "Cluster Meta Heartbeat RPC Service", "ClusterMetaHeartbeatRPCService"),
  CLUSTER_DATA_RPC_SERVICE("Cluster Data RPC Service", "ClusterDataRPCService"),
  CLUSTER_DATA_HEART_BEAT_RPC_SERVICE(
      "Cluster Data Heartbeat RPC Service", "ClusterDataHeartbeatRPCService"),
  CLUSTER_META_ENGINE("Cluster Meta Engine", "ClusterMetaEngine"),
  CLUSTER_DATA_ENGINE("Cluster Data Engine", "ClusterDataEngine"),
  REST_SERVICE("REST Service", "REST Service"),
  CONFIG_NODE_SERVICE("Config Node service", "ConfigNodeRPCServer"),
  DATA_NODE_REGION_MIGRATE_SERVICE("Data Node Region Migrate service", ""),
  DATA_NODE_MANAGEMENT_SERVICE("Data Node management service", "DataNodeManagementServer"),
  FRAGMENT_INSTANCE_MANAGER_SERVICE("Fragment instance manager", "FragmentInstanceManager"),
  MPP_DATA_EXCHANGE_SERVICE("MPP Data exchange manager", "MPPDataExchangeManager"),
  INTERNAL_SERVICE("Internal Service", "InternalService"),
  IOT_CONSENSUS_SERVICE("IoTConsensus Service", "IoTConsensusRPCService"),
  PIPE_CONSENSUS_SERVICE("PipeConsensus Service", "PipeConsensusRPCService"),
  PIPE_PLUGIN_CLASSLOADER_MANAGER_SERVICE(
      "Pipe Plugin Classloader Manager Service", "PipePluginClassLoader"),
  AINode_RPC_SERVICE("Rpc Service for AINode", "AINodeRPCService"),
  PIPE_RUNTIME_DATA_NODE_AGENT("Pipe Runtime Data Node Agent", "PipeRuntimeDataNodeAgent"),
  PIPE_RUNTIME_CONFIG_NODE_AGENT("Pipe Runtime Config Node Agent", "PipeRuntimeConfigNodeAgent"),
  SUBSCRIPTION_RUNTIME_AGENT("Subscription Runtime Agent", "SubscriptionRuntimeAgent"),
  GENERAL_REGION_ATTRIBUTE_SECURITY_SERVICE(
      "General Region Attribute Security Service", "GeneralRegionAttributeSecurityService"),

  SESSION_MANAGER("Session Manager", "RpcSession"),
  CONFIG_NODE("Config Node", "ConfigNode"),
  DATA_NODE("Data Node", "DataNode");
  private final String name;
  private final String jmxName;

  ServiceType(String name, String jmxName) {
    this.name = name;
    this.jmxName = jmxName;
  }

  public String getName() {
    return name;
  }

  public String getJmxName() {
    return jmxName;
  }

  private static String generateJmxName(String packageName, String jmxName) {
    return String.format("%s:type=%s", packageName, jmxName);
  }
}
