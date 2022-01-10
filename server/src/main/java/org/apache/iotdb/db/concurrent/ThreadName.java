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
package org.apache.iotdb.db.concurrent;

public enum ThreadName {
  METRICS_SERVICE("Metrics-ServerServiceImpl"),
  RPC_SERVICE("RPC"),
  RPC_CLIENT("RPC-Client"),
  INFLUXDB_SERVICE("Influxdb-Service"),
  INFLUXDB_CLIENT("Influxdb-Client"),
  MERGE_SERVICE("Merge"),
  CLOSE_MERGE_SERVICE("Close-Merge"),
  CLOSE_MERGE_DAEMON("Close-Merge-Daemon"),
  CLOSE_DAEMON("Close-Daemon"),
  MERGE_DAEMON("Merge-Daemon"),
  MEMORY_MONITOR("MemMonitor"),
  MEMORY_STATISTICS("MemStatistic"),
  FLUSH_PARTIAL_POLICY("FlushPartialPolicy"),
  FORCE_FLUSH_ALL_POLICY("ForceFlushAllPolicy"),
  STAT_MONITOR("StatMonitor"),
  FLUSH_SERVICE("Flush"),
  FLUSH_SUB_TASK_SERVICE("Flush-SubTask"),
  COMPACTION_SERVICE("Compaction"),
  WAL_DAEMON("WAL-Sync"),
  WAL_FORCE_DAEMON("WAL-Force"),
  INDEX_SERVICE("Index"),
  SYNC_CLIENT("Sync-Client"),
  SYNC_SERVER("Sync"),
  SYNC_MONITOR("Sync-Monitor"),
  LOAD_TSFILE("Load-TsFile"),
  TIME_COST_STATISTIC("TIME_COST_STATISTIC"),
  QUERY_SERVICE("Query"),
  SUB_RAW_QUERY_SERVICE("Sub_RawQuery"),
  INSERTION_SERVICE("MultithreadingInsertionPool"),
  WINDOW_EVALUATION_SERVICE("WindowEvaluationTaskPoolManager"),
  CONTINUOUS_QUERY_SERVICE("ContinuousQueryTaskPoolManager"),
  CLUSTER_INFO_SERVICE("ClusterInfoClient"),
  CLUSTER_RPC_SERVICE("ClusterRPC"),
  CLUSTER_RPC_CLIENT("Cluster-RPC-Client"),
  CLUSTER_META_RPC_SERVICE("ClusterMetaRPC"),
  CLUSTER_META_RPC_CLIENT("ClusterMetaRPC-Client"),
  CLUSTER_META_HEARTBEAT_RPC_SERVICE("ClusterMetaHeartbeatRPC"),
  CLUSTER_META_HEARTBEAT_RPC_CLIENT("ClusterMetaHeartbeatRPC-Client"),
  CLUSTER_DATA_RPC_SERVICE("ClusterDataRPC"),
  CLUSTER_DATA_RPC_CLIENT("ClusterDataRPC-Client"),
  CLUSTER_DATA_HEARTBEAT_RPC_SERVICE("ClusterDataHeartbeatRPC"),
  CLUSTER_DATA_HEARTBEAT_RPC_CLIENT("ClusterDataHeartbeatRPC-Client"),
  ;

  private final String name;

  ThreadName(String name) {
    this.name = name;
  }

  public String getName() {
    return name;
  }
}
