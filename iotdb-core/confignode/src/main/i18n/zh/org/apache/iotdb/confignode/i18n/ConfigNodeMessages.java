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

package org.apache.iotdb.confignode.i18n;

public final class ConfigNodeMessages {

  public static final String
      MESSAGE_SUBSCRIPTION_IS_DISABLED_WHEN_DATA_REGION_CONSENSUS_PROTOCOL_IS_RATIS_194CE431 =
          "当数据区域共识协议为 Ratis 时，Subscription 功能不可用。";

  public static final String
      MESSAGE_PIPE_DATA_INSERTION_IS_DISABLED_WHEN_DATA_REGION_CONSENSUS_PROTOCOL_IS_RATIS_7D669ECC =
          "当数据区域共识协议为 Ratis 时，包含 data.insert 的 Pipe 同步不可用。";

  public static final String MESSAGE_FAILED_TO_ALTER_PIPE_ARG_FAILURES_ARG_DOES_NOT_EXIST_169BDB85 =
      "无法修改 Pipe %s。原因：%s 不存在。";

  public static final String MESSAGE_FAILED_TO_START_PIPE_ARG_FAILURES_ARG_DOES_NOT_EXIST_F13A63EE =
      "无法启动 Pipe %s。原因：%s 不存在。";

  public static final String
      EXCEPTION_CONFIGNODE_SIMPLECONSENSUS_WRITES_ARE_DISABLED_AFTER_A_WAL_CLEANUP_FAILURE_RESTART_IS_REQUIRED_D52F0107 =
          "ConfigNode SimpleConsensus 因 WAL 清理失败已禁用写入，需要重启。";
  public static final String
      EXCEPTION_FAILED_TO_APPLY_CONFIGNODE_ARG_THROUGH_CONSENSUS_LAYER_ARG_B4A985D7 =
          "通过共识层应用 ConfigNode %s 失败：%s";
  public static final String
      EXCEPTION_FAILED_TO_CLOSE_THE_CONFIGNODE_SIMPLECONSENSUS_LOG_WRITER_BC0F417C =
          "关闭 ConfigNode SimpleConsensus 日志写入器失败。";
  public static final String
      EXCEPTION_FAILED_TO_PERSIST_VERSION_INFO_FOR_CONFIGNODE_ARG_ARG_REGISTRATION_REMAINS_PENDING_AND_MUST_BE_RETRIED_F662C350 =
          "持久化 ConfigNode %d 的版本信息失败：%s。注册尚未完成，需要重试。";
  public static final String EXECUTE_NON_QUERY_PLAN_FAILED = "[ConfigRegion Update] 执行非查询计划失败";
  public static final String FAILED_TO_ROLLBACK_PERSISTED_CONFIGNODE_SIMPLECONSENSUS_LOG =
      "回滚已持久化的 ConfigNode SimpleConsensus 日志失败。该 plan 可能会在重启后被重放。重启前将禁止后续 SimpleConsensus 写入。";
  public static final String
      LOG_CONFIGREGION_UPDATE_FAILED_TO_CLOSE_THE_CONFIGNODE_SIMPLECONSENSUS_LOG_FILEPATH_ARG_RETRY_ARG_188A702E =
          "[ConfigRegion Update] 关闭 ConfigNode SimpleConsensus 日志失败，filePath: {}，重试: {}。";
  public static final String
      LOG_FAILED_TO_ROLL_BACK_AINODE_REGISTRATION_ARG_AFTER_VERSION_INFO_PERSISTENCE_FAILURE_VERSIONUPDATESTATUS_ARG_ROLLBACKSTATUS_ARG_BFF24BD4 =
          "版本信息持久化失败后，回滚 AINode {} 的注册失败。versionUpdateStatus: {}，rollbackStatus: {}";
  public static final String
      LOG_FAILED_TO_ROLL_BACK_DATANODE_REGISTRATION_ARG_AFTER_VERSION_INFO_PERSISTENCE_FAILURE_VERSIONUPDATESTATUS_ARG_ROLLBACKSTATUS_ARG_437E6F09 =
          "版本信息持久化失败后，回滚 DataNode {} 的注册失败。versionUpdateStatus: {}，rollbackStatus: {}";
  public static final String
      MESSAGE_FAILED_TO_PERSIST_VERSION_INFO_FOR_AINODE_ARG_ARG_THE_REGISTRATION_HAS_BEEN_ROLLED_BACK_PLEASE_RETRY_THE_REGISTRATION_7571C626 =
          "持久化 AINode %d 的版本信息失败：%s。注册已回滚，请重试注册。";
  public static final String
      MESSAGE_FAILED_TO_PERSIST_VERSION_INFO_FOR_AINODE_ARG_ARG_THE_REGISTRATION_ROLLBACK_ALSO_FAILED_ARG_MANUAL_CLEANUP_MAY_BE_REQUIRED_BEFORE_RETRYING_THE_REGISTRATION_B9B1DCCD =
          "持久化 AINode %d 的版本信息失败：%s。注册回滚也失败：%s。重试注册前可能需要手动清理。";
  public static final String
      MESSAGE_FAILED_TO_PERSIST_VERSION_INFO_FOR_DATANODE_ARG_ARG_THE_REGISTRATION_HAS_BEEN_ROLLED_BACK_PLEASE_RETRY_THE_REGISTRATION_5DDB311F =
          "持久化 DataNode %d 的版本信息失败：%s。注册已回滚，请重试注册。";
  public static final String
      MESSAGE_FAILED_TO_PERSIST_VERSION_INFO_FOR_DATANODE_ARG_ARG_THE_REGISTRATION_ROLLBACK_ALSO_FAILED_ARG_MANUAL_CLEANUP_MAY_BE_REQUIRED_BEFORE_RETRYING_THE_REGISTRATION_5463B5FF =
          "持久化 DataNode %d 的版本信息失败：%s。注册回滚也失败：%s。重试注册前可能需要手动清理。";
  public static final String MESSAGE_STATUS_CODE_ARG_4B3BCD8B = "状态码 %d";
  public static final String MESSAGE_UNKNOWN_ERROR_E9EEAFF8 = "未知错误";
  public static final String PERSIST_CONFIGNODE_SIMPLECONSENSUS_LOG_FAILED =
      "持久化 ConfigNode SimpleConsensus 日志失败：%s";
  public static final String
      PERSIST_CURRENT_CONFIGPHYSICALPLAN_FOR_CONFIGNODE_SIMPLECONSENSUS_MODE_FAILED =
          "为 ConfigNode SimpleConsensus 模式持久化当前 ConfigPhysicalPlan 失败";
  public static final String
      ROLLBACK_FAILED_CONFIGPHYSICALPLAN_FOR_CONFIGNODE_SIMPLECONSENSUS_MODE_FAILED =
          "回滚 ConfigNode SimpleConsensus 模式下失败的 ConfigPhysicalPlan 失败，planType: {}, logFile: {}, truncateOffset: {}, endIndexBeforeWrite: {}。重启前将禁止后续 SimpleConsensus 写入。";
  public static final String SEAL_RECOVERED_CONFIGNODE_SIMPLECONSENSUS_LOG_FAILED =
      "封存恢复出的 ConfigNode SimpleConsensus 日志失败：{}";
  public static final String SIMPLECONSENSUS_LOG_WRITER_IS_NOT_INITIALIZED =
      "SimpleConsensus log writer 未初始化。";
  public static final String UNEXPECTED_INTERRUPTION_DURING_THE_CLOSE_METHOD_OF_LOGWRITER =
      "[ConfigRegion Update] 关闭 logWriter 过程中发生意外中断";

  public static final String
      MESSAGE_CURRENT_CONFIGNODE_NOT_LEADER_PLEASE_REDIRECT_NEW_CONFIGNODE_F9AF262D =
          "当前 ConfigNode 不是 Leader，请重定向到新的 ConfigNode。";
  public static final String
      MESSAGE_CURRENT_CONFIGNODE_LEADER_BUT_CONSENSUS_NOT_READY_YET_F1FB7C9A =
          "当前 ConfigNode 是 Leader，但共识层尚未就绪。";
  public static final String
      MESSAGE_CURRENT_CONFIGNODE_LEADER_BUT_LEADER_SERVICES_NOT_READY_YET_ECC25E1A =
          "当前 ConfigNode 是 Leader，但 Leader 服务尚未就绪。";
  public static final String MESSAGE_CONFIGNODE_LEADER_LOAD_SERVICES_NOT_STARTED_1C75DB82 =
      "ConfigNode Leader 的负载服务尚未启动。";
  public static final String
      MESSAGE_CONFIGNODE_LEADER_WAITING_FOR_CLUSTER_HEARTBEAT_SAMPLING_EDB0331F =
          "ConfigNode Leader 正在等待集群心跳采样。";
  public static final String MESSAGE_CONFIGNODE_LEADER_LOAD_SERVICES_STOPPED_5884BB0F =
      "ConfigNode Leader 的负载服务已停止。";
  public static final String MESSAGE_CONFIGNODE_LEADER_LOAD_SERVICES_READY_CC9C90E7 =
      "ConfigNode Leader 的负载服务已就绪。";
  public static final String
      MESSAGE_CONFIGNODE_LEADER_WAITING_FOR_FIRST_HEARTBEAT_FROM_REGISTERED_CONFIGNODES_DATANODES_ARG_2FD3F6FA =
          "ConfigNode Leader 正在等待已注册 ConfigNode/DataNode 的首次心跳：%s";
  public static final String
      MESSAGE_CONFIGNODE_LEADER_LOAD_SERVICES_READY_AFTER_WAITING_ARG_MS_FOR_FIRST_HEARTBEAT_MISSING_HEARTBEATS_ARG_A3AEA8B4 =
          "ConfigNode Leader 等待首次心跳 %d 毫秒后负载服务已就绪。缺少心跳的节点：%s";
  public static final String
      LOG_UNEXPECTED_INTERRUPTION_WHILE_WAITING_FOR_CONFIGNODE_LEADER_LOAD_WARM_UP_BB5AA4F7 =
          "等待 ConfigNode Leader 负载预热时发生意外中断。";
  public static final String
      LOG_CURRENT_CONFIGNODE_NODEID_ARG_IP_ARG_FINISHED_STARTING_LEADER_SERVICES_WHILE_LOAD_WARM_UP_IS_STILL_IN_PROGRESS_ARG_17C09A31 =
          "当前 ConfigNode(nodeId: {}, ip: {}) 已完成 Leader 服务启动，但负载预热仍在进行：{}";
  public static final String
      LOG_CONFIGNODE_LEADER_WARMING_UP_BEFORE_SERVING_REGISTERING_CONFIGNODE_STATUS_ARG_RETRY_ARG_3C924873 =
          "ConfigNode Leader 正在预热，暂不能处理注册中的 ConfigNode，将等待并重试。状态：{}，重试次数：{}";
  public static final String
      LOG_CONFIGNODE_LEADER_ARG_WARMING_UP_BEFORE_SERVING_DATANODE_ARG_WILL_WAIT_AND_RETRY_REASON_ARG_3A2A4163 =
          "ConfigNode Leader {} 正在预热，暂不能处理 DataNode {}，将等待并重试。原因：{}";
  public static final String
      LOG_SKIP_STARTING_CONFIGNODE_LEADER_SERVICES_BECAUSE_THE_LEADER_EPOCH_IS_STALE_9E4467F6 =
          "Leader 任期已过期，跳过启动 ConfigNode Leader 服务。";
  public static final String LOG_CONFIGNODE_ARG_AT_ARG_IS_NO_LONGER_LEADER_NEW_LEADER_ARG_5C41D7D7 =
      "ConfigNode %d（%s）不再是 Leader；新 Leader 为节点 %d。";
  public static final String
      LOG_CONFIGNODE_ARG_AT_ARG_IS_NO_LONGER_LEADER_CLEANING_SERVICES_37CA9A1A =
          "ConfigNode %d（%s）不再是 Leader，正在清理 Leader 服务。";
  public static final String LOG_CONFIGNODE_ARG_AT_ARG_BECOMES_CONFIG_REGION_LEADER_2EB9B9E2 =
      "ConfigNode %d（%s）成为 ConfigRegion Leader。";
  public static final String LOG_CONFIGNODE_ARG_AT_ARG_AS_CONFIG_REGION_LEADER_IS_READY_74DFCB26 =
      "ConfigNode %d（%s）已作为 ConfigRegion Leader 就绪。";
  public static final String LOG_CONFIGNODE_ARG_AT_ARG_LEADER_SERVICES_ARE_UNAVAILABLE_0223CF16 =
      "ConfigNode %d（%s）不再是 Leader，所有 Leader 服务均不可用。";
  public static final String
      LOG_FAILED_TO_START_CONFIGNODE_LEADER_SERVICE_ON_NODE_ARG_AT_ARG_ARG_THE_NODE_WILL_FINISH_WARMING_UP_BUT_THIS_SERVICE_REMAINS_UNAVAILABLE_UNTIL_THE_NEXT_LEADERSHIP_TRANSITION_9E0D12F9 =
          "启动节点 %s（地址 %s）的 ConfigNode Leader 服务 %s 失败；节点仍将完成预热，但该服务在下一次 Leader 切换前不可用。";

  public static final String MESSAGE_UNREADY_NODES_ARG_144B31E1 = "未就绪节点：%s";
  public static final String MESSAGE_MORE_UNREADY_NODES_ARG_C7CDA960 = "...（另有 %d 个）";

  public static final String
      LOG_FAILED_TO_START_ONE_OR_MORE_CONFIGNODE_LEADER_SERVICES_ON_NODE_ARG_AT_ARG_THE_NODE_WILL_CONTINUE_STARTUP_BUT_FAILED_SERVICES_MAY_REMAIN_UNAVAILABLE_UNTIL_THE_NEXT_LEADERSHIP_TRANSITION_42BDDB32 =
          "ConfigNode 节点 %d（%s）启动一个或多个 Leader 服务失败；节点将继续启动，但失败的服务可能会一直不可用，直到下一次 Leader 切换。";

  private ConfigNodeMessages() {}
}
