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
  // -------------------------- QueryThread --------------------------
  QUERY_WORKER_THREAD_NAME("Query-Worker-Thread"),
  QUERY_SENTINEL_THREAD_NAME("Query-Sentinel-Thread"),
  TIMED_QUERY_SQL_COUNT_THREAD_POOL_NAME("Timed-Query-SQL-Count"),
  MPP_DATA_EXCHANGE_TASK_EXECUTOR_POOL("MPP-Data-Exchange-Task-Executors"),
  FRAGMENT_INSTANCE_MANAGEMENT_THREAD("Fragment-Instance-Management"),
  FRAGMENT_INSTANCE_NOTIFICATION_THREAD("Fragment-Instance-Notification"),
  DATANODE_INTERNAL_RPC_SERVICE("DataNodeInternalRPC-Service"),
  DATANODE_INTERNAL_RPC_PROCESSOR("DataNodeInternalRPC-Processor"),
  MPP_DATA_EXCHANGE_RPC_SERVICE("MPPDataExchangeRPC-Service"),
  MPP_DATA_EXCHANGE_RPC_PROCESSOR("MPPDataExchangeRPC-Processor"),
  MPP_COORDINATOR_EXECUTOR_POOL_NAME("MPP-Coordinator-Executor"),
  DRIVER_TASK_SCHEDULER_NOTIFICATION("Driver-Task-Scheduler-Notification"),
  // -------------------------- MPPSchedule --------------------------
  MPP_COORDINATOR_SCHEDULED_EXECUTOR_POOL_NAME("MPP-Coordinator-Scheduled-Executor"),
  // -------------------------- Compaction --------------------------
  COMPACTION_WORKER("Compaction-Worker"),
  COMPACTION_SUB_TASK("Compaction-Sub-Task"),
  COMPACTION_SCHEDULE("Compaction-Schedule"),
  // -------------------------- Wal --------------------------
  WAL_SERIALIZE("WAL-Serialize"),
  WAL_SYNC("WAL-Sync"),
  WAL_DELETE("WAL-Delete"),
  WAL_RECOVER("WAL-Recover"),
  // -------------------------- Write --------------------------
  MPP_COORDINATOR_WRITE_EXECUTOR_POOL_NAME("MPP-Coordinator-Write-Executor"),
  // -------------------------- Flush --------------------------
  FLUSH_SERVICE("Flush"),
  FLUSH_SUB_TASK_SERVICE("Flush-SubTask"),
  FLUSH_TASK_SUBMIT("FlushTask-Submit-Pool"),
  TIMED_FLUSH_SEQ_MEMTABLE("Timed-Flush-Seq-Memtable"),
  TIMED_FLUSH_UNSEQ_MEMTABLE("Timed-Flush-Unseq-Memtable"),
  // -------------------------- SchemaEngine --------------------------
  SCHEMA_REGION_RELEASE_PROCESSOR("SchemaRegion-Release-Task-Processor"),
  SCHEMA_RELEASE_MONITOR("Schema-Release-Task-Monitor"),
  SCHEMA_REGION_FLUSH_PROCESSOR("SchemaRegion-Flush-Task-Processor"),
  SCHEMA_FLUSH_MONITOR("Schema-Flush-Task-Monitor"),
  // -------------------------- ClientService --------------------------
  CLIENT_RPC_SERVICE("ClientRPC-Service"),
  CLIENT_RPC_PROCESSOR("ClientRPC-Processor"),
  // -------------------------- ConfigNode-RPC --------------------------
  CONFIGNODE_RPC_SERVICE("ConfigNodeRPC-Service"),
  CONFIGNODE_RPC_PROCESSOR("ConfigNodeRPC-Processor"),
  ASYNC_CONFIGNODE_HEARTBEAT_CLIENT_POOL("AsyncConfigNodeHeartbeatServiceClientPool"),
  ASYNC_DATANODE_HEARTBEAT_CLIENT_POOL("AsyncDataNodeHeartbeatServiceClientPool"),
  ASYNC_CONFIGNODE_CLIENT_POOL("AsyncConfigNodeIServiceClientPool"),
  // -------------------------- ConfigNode-Query --------------------------
  CQ_MANAGER("CQ-Scheduler"),
  // -------------------------- IoTConsensus --------------------------
  IOT_CONSENSUS_RPC_SERVICE("IoTConsensusRPC-Service"),
  IOT_CONSENSUS_RPC_PROCESSOR("IoTConsensusRPC-Processor"),
  ASYNC_DATANODE_CLIENT_POOL("AsyncDataNodeInternalServiceClientPool"),
  ASYNC_DATANODE_MPP_DATA_EXCHANGE_CLIENT_POOL("AsyncDataNodeMPPDataExchangeServiceClientPool"),
  ASYNC_DATANODE_IOT_CONSENSUS_CLIENT_POOL("AsyncDataNodeIoTConsensusServiceClientPool"),
  LOG_DISPATCHER("LogDispatcher"),
  // -------------------------- Ratis(In regex form) --------------------------
  // NOTICE: The thread name of ratis cannot be edited here!
  // We list the thread name here just for distinguishing what module the thread belongs to.
  RAFT_SERVER_PROXY_EXECUTOR_THREAD_NAME_PATTERN("\\d+-impl"),
  RAFT_SERVER_EXECUTOR_THREAD_NAME_PATTERN("\\d+-server"),
  RAFT_SERVER_CLIENT_EXECUTOR_THREAD_NAME_PATTERN("\\d+-client"),
  SEGMENT_RAFT_WORKER_THREAD_NAME_PATTERN("SegmentedRaftLogWorker"),
  STATE_MACHINE_UPDATER_THREAD_NAME_PATTERN("StateMachineUpdater"),
  FOLLOWER_STATE_THREAD_NAME_PATTERN("FollowerState"),
  LEADER_STATE_IMPL_PROCESSOR_THREAD_NAME("LeaderStateImpl"),
  LEADER_ELECTION_THREAD_NAME("LeaderElection"),
  LOG_APPENDER_THREAD_NAME("GrpcLogAppender"),
  EVENT_PROCESSOR_THREAD_NAME("EventProcessor"),
  RATIS_BG_DISK_GUARDIAN_THREAD_NAME("ratis-bg-disk-guardian"),
  GRPC_DEFAULT_BOSS_ELG("grpc-default-boss-ELG"),
  GRPC_DEFAULT_EXECUTOR("grpc-default-executor"),
  GROUP_MANAGMENT("group-management"),

  // -------------------------- Compute --------------------------
  PIPE_ASSIGNER_EXECUTOR_POOL("Pipe-Assigner-Executor-Pool"),
  PIPE_PROCESSOR_EXECUTOR_POOL("Pipe-Processor-Executor-Pool"),
  PIPE_CONNECTOR_EXECUTOR_POOL("Pipe-Connector-Executor-Pool"),
  PIPE_SUBTASK_CALLBACK_EXECUTOR_POOL("Pipe-SubTask-Callback-Executor-Pool"),
  EXT_PIPE_PLUGIN_WORKER("ExtPipePlugin-Worker"),
  WINDOW_EVALUATION_SERVICE("WindowEvaluationTaskPoolManager"),
  // -------------------------- Sync --------------------------
  SYNC_SENDER_PIPE("Sync-Pipe"),
  SYNC_SENDER_HEARTBEAT("Sync-Heartbeat"),
  // -------------------------- JVM --------------------------
  // NOTICE: The thread name of jvm cannot be edited here!
  // We list the thread name here just for distinguishing what module the thread belongs to.
  JVM_PAUSE_MONITOR_THREAD_NAME("JvmPauseMonitor"),
  GC_THREAD_NAME("GC task thread"),
  COMPILE_THREAD_NAME("CompilerThread"),
  VM_PERIODIC_TASK("VM Periodic Task Thread"),
  VM_THREAD("VM Thread"),
  REFERENCE_HANDLER("Reference Handler"),
  FINALIZER("Finalizer"),
  SIGNAL_DISPATCHER("Signal Dispatcher"),
  DESTROY_JVM("DestroyJavaVM"),
  // -------------------------- LogThread --------------------------
  LOG_BACK("logback"),
  // -------------------------- Other --------------------------
  TTL_CHECK_SERVICE("TTL-CHECK"),
  SETTLE_SERVICE("Settle"),
  INFLUXDB_RPC_SERVICE("InfluxdbRPC-Service"),
  INFLUXDB_RPC_PROCESSOR("InfluxdbRPC-Processor"),
  STORAGE_ENGINE_CACHED_SERVICE("StorageEngine"),
  MLNODE_RPC_SERVICE("MLNodeRpc-Service"),
  ;

  private final String name;

  ThreadName(String name) {
    this.name = name;
  }

  public String getName() {
    return name;
  }
}
