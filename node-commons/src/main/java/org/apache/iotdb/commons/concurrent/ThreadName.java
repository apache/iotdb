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

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
  ASYNC_DATANODE_MPP_DATA_EXCHANGE_CLIENT_POOL("AsyncDataNodeMPPDataExchangeServiceClientPool"),

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
  ASYNC_DATANODE_IOT_CONSENSUS_CLIENT_POOL("AsyncDataNodeIoTConsensusServiceClientPool"),
  LOG_DISPATCHER("LogDispatcher"),
  // -------------------------- Ratis --------------------------
  // NOTICE: The thread name of ratis cannot be edited here!
  // We list the thread name here just for distinguishing what module the thread belongs to.
  RAFT_SERVER_PROXY_EXECUTOR_THREAD_NAME_PATTERN("\\d+-impl-thread"),
  RAFT_SERVER_EXECUTOR_THREAD_NAME_PATTERN("\\d+-server-thread"),
  RAFT_SERVER_CLIENT_EXECUTOR_THREAD_NAME_PATTERN("\\d+-client-thread"),
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
  GPRC_DEFAULT_WORKER_ELG("grpc-default-worker-ELG"),
  GROUP_MANAGEMENT("groupManagement"),

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
  COMMON_CLEANER("Common-Cleaner"),
  // -------------------------- LogThread --------------------------
  LOG_BACK("logback"),
  // -------------------------- Metrics --------------------------
  SYSTEM_SCHEDULE_METRICS("SystemScheduleMetrics"),
  RESOURCE_CONTROL_DISK_STATISTIC("ResourceControl-DataRegionDiskStatistics"),
  PROMETHEUS_REACTOR_HTTP_NIO("reactor-http-nio"),
  PROMETHEUS_BOUNDED_ELASTIC("boundedElastic-evictor"),
  // -------------------------- Other --------------------------
  TTL_CHECK_SERVICE("TTL-CHECK"),
  SETTLE_SERVICE("Settle"),
  INFLUXDB_RPC_SERVICE("InfluxdbRPC-Service"),
  INFLUXDB_RPC_PROCESSOR("InfluxdbRPC-Processor"),
  STORAGE_ENGINE_CACHED_SERVICE("StorageEngine"),
  MLNODE_RPC_SERVICE("MLNodeRpc-Service"),
  IOTDB_SHUTDOWN_HOOK("IoTDB-Shutdown-Hook"),
  STORAGE_ENGINE_RECOVER_TRIGGER("StorageEngine-RecoverTrigger"),
  ;

  private final String name;
  private static final Logger log = LoggerFactory.getLogger(ThreadName.class);
  private static Set<ThreadName> queryThreadNames =
      new HashSet<>(
          Arrays.asList(
              QUERY_WORKER_THREAD_NAME,
              QUERY_SENTINEL_THREAD_NAME,
              TIMED_QUERY_SQL_COUNT_THREAD_POOL_NAME,
              MPP_DATA_EXCHANGE_TASK_EXECUTOR_POOL,
              FRAGMENT_INSTANCE_MANAGEMENT_THREAD,
              FRAGMENT_INSTANCE_NOTIFICATION_THREAD,
              DATANODE_INTERNAL_RPC_SERVICE,
              DATANODE_INTERNAL_RPC_PROCESSOR,
              MPP_DATA_EXCHANGE_RPC_SERVICE,
              MPP_DATA_EXCHANGE_RPC_PROCESSOR,
              MPP_COORDINATOR_EXECUTOR_POOL_NAME,
              DRIVER_TASK_SCHEDULER_NOTIFICATION,
              ASYNC_DATANODE_MPP_DATA_EXCHANGE_CLIENT_POOL));
  private static Set<ThreadName> compactionThreadNames =
      new HashSet<>(Arrays.asList(COMPACTION_WORKER, COMPACTION_SUB_TASK, COMPACTION_SCHEDULE));

  private static Set<ThreadName> walThreadNames =
      new HashSet<>(Arrays.asList(WAL_DELETE, WAL_SERIALIZE, WAL_SYNC, WAL_DELETE, WAL_RECOVER));

  private static Set<ThreadName> flushThreadNames =
      new HashSet<>(
          Arrays.asList(
              FLUSH_SERVICE,
              FLUSH_SUB_TASK_SERVICE,
              FLUSH_TASK_SUBMIT,
              TIMED_FLUSH_SEQ_MEMTABLE,
              TIMED_FLUSH_UNSEQ_MEMTABLE));

  private static Set<ThreadName> schemaEngineThreadNames =
      new HashSet<>(
          Arrays.asList(
              SCHEMA_REGION_FLUSH_PROCESSOR,
              SCHEMA_RELEASE_MONITOR,
              SCHEMA_REGION_RELEASE_PROCESSOR,
              SCHEMA_FLUSH_MONITOR));

  private static Set<ThreadName> clientServiceThreadNames =
      new HashSet<>(Arrays.asList(CLIENT_RPC_SERVICE, CLIENT_RPC_PROCESSOR));

  private static Set<ThreadName> iotConsensusThrreadNames =
      new HashSet<>(
          Arrays.asList(
              IOT_CONSENSUS_RPC_SERVICE,
              IOT_CONSENSUS_RPC_PROCESSOR,
              ASYNC_DATANODE_CLIENT_POOL,
              ASYNC_DATANODE_IOT_CONSENSUS_CLIENT_POOL,
              LOG_DISPATCHER));

  private static Set<ThreadName> ratisThreadNames =
      new HashSet<>(
          Arrays.asList(
              RAFT_SERVER_PROXY_EXECUTOR_THREAD_NAME_PATTERN,
              RAFT_SERVER_EXECUTOR_THREAD_NAME_PATTERN,
              RAFT_SERVER_CLIENT_EXECUTOR_THREAD_NAME_PATTERN,
              SEGMENT_RAFT_WORKER_THREAD_NAME_PATTERN,
              STATE_MACHINE_UPDATER_THREAD_NAME_PATTERN,
              FOLLOWER_STATE_THREAD_NAME_PATTERN,
              LEADER_STATE_IMPL_PROCESSOR_THREAD_NAME,
              LEADER_ELECTION_THREAD_NAME,
              LOG_APPENDER_THREAD_NAME,
              EVENT_PROCESSOR_THREAD_NAME,
              RATIS_BG_DISK_GUARDIAN_THREAD_NAME,
              GRPC_DEFAULT_BOSS_ELG,
              GPRC_DEFAULT_WORKER_ELG,
              GRPC_DEFAULT_EXECUTOR,
              GROUP_MANAGEMENT));
  private static Set<ThreadName> computeThreadNames =
      new HashSet<>(
          Arrays.asList(
              PIPE_ASSIGNER_EXECUTOR_POOL,
              PIPE_PROCESSOR_EXECUTOR_POOL,
              PIPE_CONNECTOR_EXECUTOR_POOL,
              PIPE_SUBTASK_CALLBACK_EXECUTOR_POOL,
              EXT_PIPE_PLUGIN_WORKER,
              WINDOW_EVALUATION_SERVICE));
  private static Set<ThreadName> syncThreadNames =
      new HashSet<>(Arrays.asList(SYNC_SENDER_PIPE, SYNC_SENDER_HEARTBEAT));
  private static Set<ThreadName> jvmThreadNames =
      new HashSet<>(
          Arrays.asList(
              JVM_PAUSE_MONITOR_THREAD_NAME,
              GC_THREAD_NAME,
              COMPILE_THREAD_NAME,
              VM_PERIODIC_TASK,
              VM_THREAD,
              REFERENCE_HANDLER,
              FINALIZER,
              SIGNAL_DISPATCHER,
              DESTROY_JVM,
              COMMON_CLEANER));

  private static Set<ThreadName> metricsThreadNames =
      new HashSet<>(
          Arrays.asList(
              SYSTEM_SCHEDULE_METRICS,
              RESOURCE_CONTROL_DISK_STATISTIC,
              PROMETHEUS_REACTOR_HTTP_NIO,
              PROMETHEUS_BOUNDED_ELASTIC));
  private static Set<ThreadName> otherThreadNames =
      new HashSet<>(
          Arrays.asList(
              TTL_CHECK_SERVICE,
              SETTLE_SERVICE,
              INFLUXDB_RPC_SERVICE,
              INFLUXDB_RPC_PROCESSOR,
              STORAGE_ENGINE_CACHED_SERVICE,
              MLNODE_RPC_SERVICE));

  ThreadName(String name) {
    this.name = name;
  }

  public String getName() {
    return name;
  }

  public static DataNodeThreadModule getModuleTheThreadBelongs(String givenThreadName) {
    for (ThreadName threadName : queryThreadNames) {
      if (givenThreadName.contains(threadName.getName())) {
        return DataNodeThreadModule.QUERY;
      }
    }
    if (givenThreadName.contains(MPP_COORDINATOR_SCHEDULED_EXECUTOR_POOL_NAME.getName())) {
      return DataNodeThreadModule.MPP_SCHEDULE;
    }
    for (ThreadName threadName : compactionThreadNames) {
      if (givenThreadName.contains(threadName.getName())) {
        return DataNodeThreadModule.COMPACTION;
      }
    }
    for (ThreadName threadName : walThreadNames) {
      if (givenThreadName.contains(threadName.getName())) {
        return DataNodeThreadModule.WAL;
      }
    }
    if (givenThreadName.contains(MPP_COORDINATOR_WRITE_EXECUTOR_POOL_NAME.getName())) {
      return DataNodeThreadModule.MPP_WRITE;
    }
    for (ThreadName threadName : flushThreadNames) {
      if (givenThreadName.contains(threadName.getName())) {
        return DataNodeThreadModule.FLUSH;
      }
    }
    for (ThreadName threadName : schemaEngineThreadNames) {
      if (givenThreadName.contains(threadName.getName())) {
        return DataNodeThreadModule.SCHEMA_ENGINE;
      }
    }
    for (ThreadName threadName : clientServiceThreadNames) {
      if (givenThreadName.contains(threadName.getName())) {
        return DataNodeThreadModule.CLIENT_SERVICE;
      }
    }
    for (ThreadName threadName : iotConsensusThrreadNames) {
      if (givenThreadName.contains(threadName.getName())) {
        return DataNodeThreadModule.IOT_CONSENSUS;
      }
    }
    for (ThreadName threadName : ratisThreadNames) {
      if (threadName.getName().contains("\\d")) {
        if (Pattern.compile(threadName.getName()).matcher(givenThreadName).find()) {
          return DataNodeThreadModule.RATIS_CONSENSUS;
        }
      } else if (givenThreadName.contains(threadName.getName())) {
        return DataNodeThreadModule.RATIS_CONSENSUS;
      }
    }
    for (ThreadName threadName : computeThreadNames) {
      if (givenThreadName.contains(threadName.getName())) {
        return DataNodeThreadModule.COMPUTE;
      }
    }
    for (ThreadName threadName : syncThreadNames) {
      if (givenThreadName.contains(threadName.getName())) {
        return DataNodeThreadModule.SYNC;
      }
    }
    for (ThreadName threadName : jvmThreadNames) {
      if (givenThreadName.contains(threadName.getName())) {
        return DataNodeThreadModule.JVM;
      }
    }
    if (givenThreadName.contains(LOG_BACK.getName())) {
      return DataNodeThreadModule.LOG_BACK;
    }
    for (ThreadName threadName : metricsThreadNames) {
      if (givenThreadName.contains(threadName.getName())) {
        return DataNodeThreadModule.METRICS;
      }
    }
    for (ThreadName threadName : otherThreadNames) {
      if (givenThreadName.contains(threadName.getName())) {
        return DataNodeThreadModule.OTHER;
      }
    }
    log.error("Unknown thread name {}", givenThreadName);
    return DataNodeThreadModule.UNKNOWN;
  }

  public static ThreadName getThreadPoolTheThreadBelongs(String givenThreadName) {
    ThreadName[] threadNames = ThreadName.values();
    for (ThreadName threadName : threadNames) {
      String name = threadName.getName();
      if (name.contains("\\d")) {
        // regex pattern
        if (Pattern.compile(name).matcher(givenThreadName).find()) {
          return threadName;
        }
      } else {
        if (givenThreadName.contains(name)) {
          return threadName;
        }
      }
    }
    return null;
  }
}
