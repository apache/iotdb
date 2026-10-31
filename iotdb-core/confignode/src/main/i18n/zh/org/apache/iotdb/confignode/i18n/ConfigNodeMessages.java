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

  private ConfigNodeMessages() {}
}
