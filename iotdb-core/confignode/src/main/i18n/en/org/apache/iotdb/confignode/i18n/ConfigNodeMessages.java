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
          "ConfigNode SimpleConsensus writes are disabled after a WAL cleanup failure; restart is required.";
  public static final String
      EXCEPTION_FAILED_TO_APPLY_CONFIGNODE_ARG_THROUGH_CONSENSUS_LAYER_ARG_B4A985D7 =
          "Failed to apply ConfigNode %s through consensus layer: %s";
  public static final String
      EXCEPTION_FAILED_TO_CLOSE_THE_CONFIGNODE_SIMPLECONSENSUS_LOG_WRITER_BC0F417C =
          "Failed to close the ConfigNode SimpleConsensus log writer.";
  public static final String
      EXCEPTION_FAILED_TO_PERSIST_VERSION_INFO_FOR_CONFIGNODE_ARG_ARG_REGISTRATION_REMAINS_PENDING_AND_MUST_BE_RETRIED_F662C350 =
          "Failed to persist version info for ConfigNode %d: %s. Registration remains pending and must be retried.";
  public static final String EXECUTE_NON_QUERY_PLAN_FAILED =
      "[ConfigRegion Update] Execute non-query plan failed";
  public static final String FAILED_TO_ROLLBACK_PERSISTED_CONFIGNODE_SIMPLECONSENSUS_LOG =
      "Failed to rollback persisted ConfigNode SimpleConsensus log. The persisted plan may be replayed after restart. Further SimpleConsensus writes are disabled until restart.";
  public static final String
      LOG_CONFIGREGION_UPDATE_FAILED_TO_CLOSE_THE_CONFIGNODE_SIMPLECONSENSUS_LOG_FILEPATH_ARG_RETRY_ARG_188A702E =
          "[ConfigRegion Update] Failed to close the ConfigNode SimpleConsensus log, filePath: {}, retry: {}.";
  public static final String
      LOG_FAILED_TO_ROLL_BACK_AINODE_REGISTRATION_ARG_AFTER_VERSION_INFO_PERSISTENCE_FAILURE_VERSIONUPDATESTATUS_ARG_ROLLBACKSTATUS_ARG_BFF24BD4 =
          "Failed to roll back AINode registration {} after version info persistence failure. versionUpdateStatus: {}, rollbackStatus: {}";
  public static final String
      LOG_FAILED_TO_ROLL_BACK_DATANODE_REGISTRATION_ARG_AFTER_VERSION_INFO_PERSISTENCE_FAILURE_VERSIONUPDATESTATUS_ARG_ROLLBACKSTATUS_ARG_437E6F09 =
          "Failed to roll back DataNode registration {} after version info persistence failure. versionUpdateStatus: {}, rollbackStatus: {}";
  public static final String
      MESSAGE_FAILED_TO_PERSIST_VERSION_INFO_FOR_AINODE_ARG_ARG_THE_REGISTRATION_HAS_BEEN_ROLLED_BACK_PLEASE_RETRY_THE_REGISTRATION_7571C626 =
          "Failed to persist version info for AINode %d: %s. The registration has been rolled back. Please retry the registration.";
  public static final String
      MESSAGE_FAILED_TO_PERSIST_VERSION_INFO_FOR_AINODE_ARG_ARG_THE_REGISTRATION_ROLLBACK_ALSO_FAILED_ARG_MANUAL_CLEANUP_MAY_BE_REQUIRED_BEFORE_RETRYING_THE_REGISTRATION_B9B1DCCD =
          "Failed to persist version info for AINode %d: %s. The registration rollback also failed: %s. Manual cleanup may be required before retrying the registration.";
  public static final String
      MESSAGE_FAILED_TO_PERSIST_VERSION_INFO_FOR_DATANODE_ARG_ARG_THE_REGISTRATION_HAS_BEEN_ROLLED_BACK_PLEASE_RETRY_THE_REGISTRATION_5DDB311F =
          "Failed to persist version info for DataNode %d: %s. The registration has been rolled back. Please retry the registration.";
  public static final String
      MESSAGE_FAILED_TO_PERSIST_VERSION_INFO_FOR_DATANODE_ARG_ARG_THE_REGISTRATION_ROLLBACK_ALSO_FAILED_ARG_MANUAL_CLEANUP_MAY_BE_REQUIRED_BEFORE_RETRYING_THE_REGISTRATION_5463B5FF =
          "Failed to persist version info for DataNode %d: %s. The registration rollback also failed: %s. Manual cleanup may be required before retrying the registration.";
  public static final String MESSAGE_STATUS_CODE_ARG_4B3BCD8B = "status code %d";
  public static final String MESSAGE_UNKNOWN_ERROR_E9EEAFF8 = "unknown error";
  public static final String PERSIST_CONFIGNODE_SIMPLECONSENSUS_LOG_FAILED =
      "Persist ConfigNode SimpleConsensus log failed: %s";
  public static final String
      PERSIST_CURRENT_CONFIGPHYSICALPLAN_FOR_CONFIGNODE_SIMPLECONSENSUS_MODE_FAILED =
          "Persist current ConfigPhysicalPlan for ConfigNode SimpleConsensus mode failed";
  public static final String
      ROLLBACK_FAILED_CONFIGPHYSICALPLAN_FOR_CONFIGNODE_SIMPLECONSENSUS_MODE_FAILED =
          "Rollback failed ConfigPhysicalPlan for ConfigNode SimpleConsensus mode failed, planType: {}, logFile: {}, truncateOffset: {}, endIndexBeforeWrite: {}. Further SimpleConsensus writes are disabled until restart.";
  public static final String SEAL_RECOVERED_CONFIGNODE_SIMPLECONSENSUS_LOG_FAILED =
      "Seal recovered ConfigNode SimpleConsensus log failed: {}";
  public static final String SIMPLECONSENSUS_LOG_WRITER_IS_NOT_INITIALIZED =
      "SimpleConsensus log writer is not initialized.";
  public static final String UNEXPECTED_INTERRUPTION_DURING_THE_CLOSE_METHOD_OF_LOGWRITER =
      "[ConfigRegion Update] Unexpected interruption during the close method of logWriter";

  private ConfigNodeMessages() {}
}
