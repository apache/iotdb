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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Pattern;

public enum ThreadName {
  // -------------------------- QueryThread --------------------------
  QUERY_WORKER("Query-Worker-Thread"),
  QUERY_SENTINEL("Query-Sentinel-Thread"),
  TIMED_QUERY_SQL_COUNT("Timed-Query-SQL-Count"),
  MPP_DATA_EXCHANGE_TASK_EXECUTOR("MPP-Data-Exchange-Task-Executors"),
  FRAGMENT_INSTANCE_MANAGEMENT("Fragment-Instance-Management"),
  FRAGMENT_INSTANCE_NOTIFICATION("Fragment-Instance-Notification"),
  DATANODE_INTERNAL_RPC_SERVICE("DataNodeInternalRPC-Service"),
  DATANODE_INTERNAL_RPC_PROCESSOR("DataNodeInternalRPC-Processor"),
  DRIVER_TASK_SCHEDULER_NOTIFICATION("Driver-Task-Scheduler-Notification"),
  // -------------------------- MPP --------------------------
  MPP_COORDINATOR_SCHEDULED_EXECUTOR("MPP-Coordinator-Scheduled-Executor"),

  ASYNC_DATANODE_CLIENT_POOL("AsyncDataNodeInternalServiceClientPool"),
  MPP_DATA_EXCHANGE_RPC_SERVICE("MPPDataExchangeRPC-Service"),
  MPP_DATA_EXCHANGE_RPC_PROCESSOR("MPPDataExchangeRPC-Processor"),
  MPP_COORDINATOR_EXECUTOR_POOL("MPP-Coordinator-Executor"),
  ASYNC_DATANODE_MPP_DATA_EXCHANGE_CLIENT_POOL("AsyncDataNodeMPPDataExchangeServiceClientPool"),

  // -------------------------- Compaction --------------------------
  COMPACTION_WORKER("Compaction-Worker"),
  COMPACTION_SUB_TASK("Compaction-Sub-Task"),
  COMPACTION_SCHEDULE("Compaction-Schedule"),
  // -------------------------- Wal --------------------------
  WAL_SERIALIZE("WAL-Serialize"),
  WAL_SYNC("WAL-Sync"),
  WAL_DELETE("WAL-Delete"),
  WAL_RECOVER("WAL-Recover"),
  TSFILE_RECOVER("TsFile-Recover"),
  // -------------------------- Write --------------------------
  MPP_COORDINATOR_WRITE_EXECUTOR("MPP-Coordinator-Write-Executor"),
  // -------------------------- Flush --------------------------
  FLUSH("Flush"),
  FLUSH_SUB_TASK("Flush-SubTask"),
  FLUSH_TASK_SUBMIT("FlushTask-Submit-Pool"),
  TIMED_FLUSH_SEQ_MEMTABLE("Timed-Flush-Seq-Memtable"),
  TIMED_FLUSH_UNSEQ_MEMTABLE("Timed-Flush-Unseq-Memtable"),
  // -------------------------- SchemaEngine --------------------------
  SCHEMA_REGION_RELEASE_PROCESSOR("SchemaRegion-Release-Task-Processor"),
  SCHEMA_REGION_RECOVER_TASK("SchemaRegion-recover-task"),
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
  TRIGGER_STATISTIC_UPDATER("Stateful-Trigger-Statistics-Updater"),
  CONFIG_NODE_RETRY_FAILED_TASK("Cluster-RetryFailedTasks-Service"),
  // -------------------------- ConfigNode-Heartbeat --------------------------
  CONFIG_NODE_HEART_BEAT_SERVICE("Cluster-Heartbeat-Service"),
  // -------------------------- ConfigNode-Compute --------------------------
  CONFIG_NODE_LOAD_STATISTIC("Cluster-LoadStatistics-Service"),
  // -------------------------- ConfigNode-RegionManagement --------------------------
  CONFIG_NODE_REGION_MAINTAINER("IoTDB-Region-Maintainer"),
  // -------------------------- ConfigNode-Recover --------------------------
  CONFIG_NODE_RECOVER("ConfigNode-Manager-Recovery"),

  // -------------------------- IoTConsensus --------------------------
  IOT_CONSENSUS_RPC_SERVICE("IoTConsensusRPC-Service"),
  IOT_CONSENSUS_RPC_PROCESSOR("IoTConsensusRPC-Processor"),
  ASYNC_DATANODE_IOT_CONSENSUS_CLIENT_POOL("AsyncDataNodeIoTConsensusServiceClientPool"),
  LOG_DISPATCHER("LogDispatcher"),
  // -------------------------- Ratis --------------------------
  // NOTICE: The thread name of ratis cannot be edited here!
  // We list the thread name here just for distinguishing what module the thread belongs to.
  RAFT_SERVER_PROXY_EXECUTOR("\\d+-impl-thread"),
  RAFT_SERVER_EXECUTOR("\\d+-server-thread"),
  RAFT_SERVER_CLIENT_EXECUTOR("\\d+-client-thread"),
  RATIS_ADD("Ratis-Add"),
  SEGMENT_RAFT_WORKER("SegmentedRaftLogWorker"),
  STATE_MACHINE_UPDATER("StateMachineUpdater"),
  FOLLOWER_STATE("FollowerState"),
  LEADER_STATE_IMPL_PROCESSOR("LeaderStateImpl"),
  LEADER_ELECTION("LeaderElection"),
  LOG_APPENDER("GrpcLogAppender"),
  EVENT_PROCESSOR("EventProcessor"),
  RATIS_BG_DISK_GUARDIAN("RatisBgDiskGuardian"),
  GRPC_DEFAULT_BOSS_ELG("grpc-default-boss-ELG"),
  GRPC_DEFAULT_EXECUTOR("grpc-default-executor"),
  GPRC_DEFAULT_WORKER_ELG("grpc-default-worker-ELG"),
  GROUP_MANAGEMENT("groupManagement"),
  // -------------------------- Compute --------------------------
  PIPE_ASSIGNER_EXECUTOR_POOL("Pipe-Assigner-Executor-Pool"),
  PIPE_PROCESSOR_EXECUTOR_POOL("Pipe-Processor-Executor-Pool"),
  PIPE_CONNECTOR_EXECUTOR_POOL("Pipe-Connector-Executor-Pool"),
  PIPE_SUBTASK_CALLBACK_EXECUTOR_POOL("Pipe-SubTask-Callback-Executor-Pool"),
  PIPE_RUNTIME_META_SYNCER("Pipe-Runtime-Meta-Syncer"),
  PIPE_RUNTIME_HEARTBEAT("Pipe-Runtime-Heartbeat"),
  PIPE_RUNTIME_PROCEDURE_SUBMITTER("Pipe-Runtime-Procedure-Submitter"),
  PIPE_WAL_RESOURCE_TTL_CHECKER("Pipe-WAL-Resource-TTL-Checker"),
  WINDOW_EVALUATION_SERVICE("WindowEvaluationTaskPoolManager"),
  // -------------------------- JVM --------------------------
  // NOTICE: The thread name of jvm cannot be edited here!
  // We list the thread name here just for distinguishing what module the thread belongs to.
  JVM_PAUSE_MONITOR("JvmPauseMonitor"),
  PARALLEL_GC("GC task thread"),
  G1_GC("GC Thread"),
  G1_MAIN_MARKER("G1 Main Marker"),
  G1_CONC("G1 Conc"),
  G1_REFINE("G1 Refine"),
  G1_YOUNG_REMSET_SAMPLING("G1 Young RemSet Sampling"),
  COMPILE("CompilerThread"),
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
  PROMETHEUS_REACTOR_HTTP_EPOLL("reactor-http-epoll"),
  PROMETHEUS_BOUNDED_ELASTIC("boundedElastic-evictor"),
  // -------------------------- Other --------------------------
  TTL_CHECK("TTL-CHECK"),
  SETTLE("Settle"),
  INFLUXDB_RPC_SERVICE("InfluxdbRPC-Service"),
  INFLUXDB_RPC_PROCESSOR("InfluxdbRPC-Processor"),
  STORAGE_ENGINE_CACHED_POOL("StorageEngine"),
  MLNODE_RPC_SERVICE("MLNodeRpc-Service"),
  IOTDB_SHUTDOWN_HOOK("IoTDB-Shutdown-Hook"),
  UPGRADE_TASK("UpgradeThread"),
  REGION_MIGRATE("Region-Migrate-Pool"),
  STORAGE_ENGINE_RECOVER_TRIGGER("StorageEngine-RecoverTrigger"),
  // the unknown thread name is used for metrics
  UNKOWN("UNKNOWN");

  private final String name;
  private static final Logger log = LoggerFactory.getLogger(ThreadName.class);
  private static Set<ThreadName> queryThreadNames =
      new HashSet<>(
          Arrays.asList(
              QUERY_WORKER,
              QUERY_SENTINEL,
              TIMED_QUERY_SQL_COUNT,
              MPP_DATA_EXCHANGE_TASK_EXECUTOR,
              FRAGMENT_INSTANCE_MANAGEMENT,
              FRAGMENT_INSTANCE_NOTIFICATION,
              DATANODE_INTERNAL_RPC_SERVICE,
              DATANODE_INTERNAL_RPC_PROCESSOR,
              DRIVER_TASK_SCHEDULER_NOTIFICATION));
  private static Set<ThreadName> mppThreadNames =
      new HashSet<>(
          Arrays.asList(
              MPP_COORDINATOR_SCHEDULED_EXECUTOR,
              ASYNC_DATANODE_CLIENT_POOL,
              MPP_COORDINATOR_WRITE_EXECUTOR,
              MPP_DATA_EXCHANGE_RPC_SERVICE,
              MPP_DATA_EXCHANGE_RPC_PROCESSOR,
              MPP_COORDINATOR_EXECUTOR_POOL,
              ASYNC_DATANODE_MPP_DATA_EXCHANGE_CLIENT_POOL));
  private static Set<ThreadName> compactionThreadNames =
      new HashSet<>(Arrays.asList(COMPACTION_WORKER, COMPACTION_SUB_TASK, COMPACTION_SCHEDULE));

  private static Set<ThreadName> walThreadNames =
      new HashSet<>(
          Arrays.asList(
              WAL_DELETE, WAL_SERIALIZE, WAL_SYNC, WAL_DELETE, WAL_RECOVER, TSFILE_RECOVER));

  private static Set<ThreadName> flushThreadNames =
      new HashSet<>(
          Arrays.asList(
              FLUSH,
              FLUSH_SUB_TASK,
              FLUSH_TASK_SUBMIT,
              TIMED_FLUSH_SEQ_MEMTABLE,
              TIMED_FLUSH_UNSEQ_MEMTABLE));

  private static Set<ThreadName> schemaEngineThreadNames =
      new HashSet<>(
          Arrays.asList(
              SCHEMA_REGION_FLUSH_PROCESSOR,
              SCHEMA_RELEASE_MONITOR,
              SCHEMA_REGION_RELEASE_PROCESSOR,
              SCHEMA_FLUSH_MONITOR,
              SCHEMA_REGION_RECOVER_TASK));

  private static Set<ThreadName> clientServiceThreadNames =
      new HashSet<>(Arrays.asList(CLIENT_RPC_SERVICE, CLIENT_RPC_PROCESSOR));

  private static Set<ThreadName> iotConsensusThrreadNames =
      new HashSet<>(
          Arrays.asList(
              IOT_CONSENSUS_RPC_SERVICE,
              IOT_CONSENSUS_RPC_PROCESSOR,
              ASYNC_DATANODE_IOT_CONSENSUS_CLIENT_POOL,
              LOG_DISPATCHER));

  private static Set<ThreadName> ratisThreadNames =
      new HashSet<>(
          Arrays.asList(
              RAFT_SERVER_PROXY_EXECUTOR,
              RAFT_SERVER_EXECUTOR,
              RAFT_SERVER_CLIENT_EXECUTOR,
              SEGMENT_RAFT_WORKER,
              STATE_MACHINE_UPDATER,
              FOLLOWER_STATE,
              LEADER_STATE_IMPL_PROCESSOR,
              LEADER_ELECTION,
              LOG_APPENDER,
              EVENT_PROCESSOR,
              RATIS_BG_DISK_GUARDIAN,
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
              PIPE_RUNTIME_META_SYNCER,
              PIPE_RUNTIME_PROCEDURE_SUBMITTER,
              PIPE_WAL_RESOURCE_TTL_CHECKER,
              WINDOW_EVALUATION_SERVICE));

  private static Set<ThreadName> jvmThreadNames =
      new HashSet<>(
          Arrays.asList(
              JVM_PAUSE_MONITOR,
              PARALLEL_GC,
              G1_GC,
              G1_MAIN_MARKER,
              G1_REFINE,
              G1_CONC,
              G1_YOUNG_REMSET_SAMPLING,
              COMPILE,
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
              PROMETHEUS_REACTOR_HTTP_EPOLL,
              PROMETHEUS_BOUNDED_ELASTIC));
  private static Set<ThreadName> otherThreadNames =
      new HashSet<>(
          Arrays.asList(
              TTL_CHECK,
              SETTLE,
              INFLUXDB_RPC_SERVICE,
              INFLUXDB_RPC_PROCESSOR,
              STORAGE_ENGINE_CACHED_POOL,
              MLNODE_RPC_SERVICE));

  private static Set<ThreadName>[] threadNameSetList =
      new Set[] {
        queryThreadNames,
        mppThreadNames,
        compactionThreadNames,
        walThreadNames,
        flushThreadNames,
        schemaEngineThreadNames,
        clientServiceThreadNames,
        iotConsensusThrreadNames,
        ratisThreadNames,
        computeThreadNames,
        jvmThreadNames,
        metricsThreadNames,
        otherThreadNames
      };

  private static DataNodeThreadModule[] modules =
      new DataNodeThreadModule[] {
        DataNodeThreadModule.QUERY,
        DataNodeThreadModule.MPP,
        DataNodeThreadModule.COMPACTION,
        DataNodeThreadModule.WAL,
        DataNodeThreadModule.FLUSH,
        DataNodeThreadModule.SCHEMA_ENGINE,
        DataNodeThreadModule.CLIENT_SERVICE,
        DataNodeThreadModule.IOT_CONSENSUS,
        DataNodeThreadModule.RATIS_CONSENSUS,
        DataNodeThreadModule.COMPUTE,
        DataNodeThreadModule.SYNC,
        DataNodeThreadModule.JVM,
        DataNodeThreadModule.METRICS,
        DataNodeThreadModule.OTHER
      };

  ThreadName(String name) {
    this.name = name;
  }

  public String getName() {
    return name;
  }

  public static DataNodeThreadModule getDataNodeModuleTheThreadBelongs(String givenThreadName) {
    for (int i = 0, length = modules.length; i < length; ++i) {
      if (matchModuleWithThreadNames(threadNameSetList[i], modules[i], givenThreadName) != null) {
        return modules[i];
      }
    }
    if (givenThreadName.contains(LOG_BACK.getName())) {
      return DataNodeThreadModule.LOG_BACK;
    }

    return DataNodeThreadModule.UNKNOWN;
  }

  private static DataNodeThreadModule matchModuleWithThreadNames(
      Set<ThreadName> threadNames, DataNodeThreadModule module, String givenThreadName) {
    for (ThreadName threadName : threadNames) {
      if (threadName.getName().contains("\\d")) {
        if (Pattern.compile(threadName.getName()).matcher(givenThreadName).find()) {
          return module;
        }
      } else if (givenThreadName.contains(threadName.getName())) {
        return module;
      }
    }
    return null;
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
    log.debug("Unknown thread name: {}", givenThreadName);
    return ThreadName.UNKOWN;
  }
}
