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
package org.apache.iotdb.commons.concurrent;

public enum ThreadName {
  CLIENT_RPC_SERVICE("ClientRPC-Service"),
  CLIENT_RPC_PROCESSOR("ClientRPC-Processor"),
  CONFIGNODE_RPC_SERVICE("ConfigNodeRPC-Service"),
  CONFIGNODE_RPC_PROCESSOR("ConfigNodeRPC-Processor"),
  IOT_CONSENSUS_RPC_SERVICE("IoTConsensusRPC-Service"),
  IOT_CONSENSUS_RPC_PROCESSOR("IoTConsensusRPC-Processor"),
  MPP_DATA_EXCHANGE_RPC_SERVICE("MPPDataExchangeRPC-Service"),
  MPP_DATA_EXCHANGE_RPC_PROCESSOR("MPPDataExchangeRPC-Processor"),
  DATANODE_INTERNAL_RPC_SERVICE("DataNodeInternalRPC-Service"),
  DATANODE_INTERNAL_RPC_PROCESSOR("DataNodeInternalRPC-Processor"),
  INFLUXDB_RPC_SERVICE("InfluxdbRPC-Service"),
  INFLUXDB_RPC_PROCESSOR("InfluxdbRPC-Processor"),
  STORAGE_ENGINE_CACHED_SERVICE("StorageEngine"),
  FLUSH_SERVICE("Flush"),
  FLUSH_SUB_TASK_SERVICE("Flush-SubTask"),
  FLUSH_TASK_SUBMIT("FlushTask-Submit-Pool"),
  COMPACTION_SERVICE("Compaction"),
  COMPACTION_SUB_SERVICE("Sub-Compaction"),
  COMPACTION_SCHEDULE("Compaction_Schedule"),
  WAL_SERIALIZE("WAL-Serialize"),
  WAL_SYNC("WAL-Sync"),
  WAL_DELETE("WAL-Delete"),
  WAL_RECOVER("WAL-Recover"),
  SYNC_CLIENT("Sync-Client"),
  SYNC_SERVER("Sync"),
  QUERY_SERVICE("Query"),
  INSERTION_SERVICE("MultithreadingInsertionPool"),
  WINDOW_EVALUATION_SERVICE("WindowEvaluationTaskPoolManager"),
  TTL_CHECK_SERVICE("TTL-CHECK"),
  TIMED_FLUSH_SEQ_MEMTABLE("Timed-Flush-Seq-Memtable"),
  TIMED_FLUSH_UNSEQ_MEMTABLE("Timed-Flush-Unseq-Memtable"),
  SETTLE_SERVICE("Settle"),
  SYNC_SENDER_PIPE("Sync-Pipe"),
  SYNC_SENDER_HEARTBEAT("Sync-Heartbeat"),
  SYNC_RECEIVER_COLLECTOR("Sync-Collector"),
  CONTINUOUS_QUERY_SERVICE("ContinuousQueryTaskPoolManager"),
  EXT_PIPE_PLUGIN_WORKER("ExtPipePlugin-Worker"),
  ASYNC_DATANODE_CLIENT_POOL("AsyncDataNodeInternalServiceClientPool"),
  ASYNC_CONFIGNODE_HEARTBEAT_CLIENT_POOL("AsyncConfigNodeHeartbeatServiceClientPool"),
  ASYNC_DATANODE_HEARTBEAT_CLIENT_POOL("AsyncDataNodeHeartbeatServiceClientPool"),
  ASYNC_CONFIGNODE_CLIENT_POOL("AsyncConfigNodeIServiceClientPool"),
  ASYNC_DATANODE_MPP_DATA_EXCHANGE_CLIENT_POOL("AsyncDataNodeMPPDataExchangeServiceClientPool"),
  ASYNC_DATANODE_IOT_CONSENSUS_CLIENT_POOL("AsyncDataNodeMPPDataExchangeServiceClientPool"),
  SCHEMA_REGION_RELEASE_PROCESSOR("SchemaRegion-Release-Task-Processor"),
  SCHEMA_RELEASE_MONITOR("Schema-Release-Task-Monitor"),
  SCHEMA_REGION_FLUSH_PROCESSOR("SchemaRegion-Flush-Task-Processor"),
  SCHEMA_FLUSH_MONITOR("Schema-Flush-Task-Monitor");

  private final String name;

  ThreadName(String name) {
    this.name = name;
  }

  public String getName() {
    return name;
  }
}
