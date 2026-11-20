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
          "Subscription is disabled when data region consensus protocol is Ratis.";

  public static final String
      MESSAGE_PIPE_DATA_INSERTION_IS_DISABLED_WHEN_DATA_REGION_CONSENSUS_PROTOCOL_IS_RATIS_7D669ECC =
          "Pipe synchronization with data insertion is disabled when data region consensus protocol is Ratis.";

  public static final String MESSAGE_FAILED_TO_ALTER_PIPE_ARG_FAILURES_ARG_DOES_NOT_EXIST_169BDB85 =
      "Failed to alter pipe %s. Failures: %s does not exist.";

  public static final String MESSAGE_FAILED_TO_START_PIPE_ARG_FAILURES_ARG_DOES_NOT_EXIST_F13A63EE =
      "Failed to start pipe %s. Failures: %s does not exist.";

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

  public static final String
      MESSAGE_CURRENT_CONFIGNODE_NOT_LEADER_PLEASE_REDIRECT_NEW_CONFIGNODE_F9AF262D =
          "The current ConfigNode is not leader, please redirect to a new ConfigNode.";
  public static final String
      MESSAGE_CURRENT_CONFIGNODE_LEADER_BUT_CONSENSUS_NOT_READY_YET_F1FB7C9A =
          "The current ConfigNode is leader but consensus is not ready yet.";
  public static final String
      MESSAGE_CURRENT_CONFIGNODE_LEADER_BUT_LEADER_SERVICES_NOT_READY_YET_ECC25E1A =
          "The current ConfigNode is leader but leader services are not ready yet.";
  public static final String MESSAGE_CONFIGNODE_LEADER_LOAD_SERVICES_NOT_STARTED_1C75DB82 =
      "ConfigNode leader load services are not started.";
  public static final String
      MESSAGE_CONFIGNODE_LEADER_WAITING_FOR_CLUSTER_HEARTBEAT_SAMPLING_EDB0331F =
          "ConfigNode leader is waiting for cluster heartbeat sampling.";
  public static final String MESSAGE_CONFIGNODE_LEADER_LOAD_SERVICES_STOPPED_5884BB0F =
      "ConfigNode leader load services are stopped.";
  public static final String MESSAGE_CONFIGNODE_LEADER_LOAD_SERVICES_READY_CC9C90E7 =
      "ConfigNode leader load services are ready.";
  public static final String
      MESSAGE_CONFIGNODE_LEADER_WAITING_FOR_FIRST_HEARTBEAT_FROM_REGISTERED_CONFIGNODES_DATANODES_ARG_2FD3F6FA =
          "ConfigNode leader is waiting for first heartbeat from registered ConfigNodes/DataNodes: %s";
  public static final String
      MESSAGE_CONFIGNODE_LEADER_LOAD_SERVICES_READY_AFTER_WAITING_ARG_MS_FOR_FIRST_HEARTBEAT_MISSING_HEARTBEATS_ARG_A3AEA8B4 =
          "ConfigNode leader load services are ready after waiting %dms for first heartbeat. Missing heartbeats: %s";
  public static final String
      LOG_UNEXPECTED_INTERRUPTION_WHILE_WAITING_FOR_CONFIGNODE_LEADER_LOAD_WARM_UP_BB5AA4F7 =
          "Unexpected interruption while waiting for ConfigNode leader load warm-up.";
  public static final String
      LOG_CURRENT_CONFIGNODE_NODEID_ARG_IP_ARG_FINISHED_STARTING_LEADER_SERVICES_WHILE_LOAD_WARM_UP_IS_STILL_IN_PROGRESS_ARG_17C09A31 =
          "Current ConfigNode(nodeId: {}, ip: {}) finished starting leader services while load warm-up is still in progress: {}";
  public static final String
      LOG_CONFIGNODE_LEADER_WARMING_UP_BEFORE_SERVING_REGISTERING_CONFIGNODE_STATUS_ARG_RETRY_ARG_3C924873 =
          "ConfigNode leader is warming up before serving the registering ConfigNode, will wait and retry. Status: {}, retry: {}";
  public static final String
      LOG_CONFIGNODE_LEADER_ARG_WARMING_UP_BEFORE_SERVING_DATANODE_ARG_WILL_WAIT_AND_RETRY_REASON_ARG_3A2A4163 =
          "ConfigNode leader {} is warming up before serving DataNode {}, will wait and retry. Reason: {}";
  public static final String
      LOG_SKIP_STARTING_CONFIGNODE_LEADER_SERVICES_BECAUSE_THE_LEADER_EPOCH_IS_STALE_9E4467F6 =
          "Skip starting ConfigNode leader services because the leader epoch is stale.";
  public static final String LOG_CONFIGNODE_ARG_AT_ARG_IS_NO_LONGER_LEADER_NEW_LEADER_ARG_5C41D7D7 =
      "ConfigNode %d at %s is no longer the leader; new leader is node %d.";
  public static final String
      LOG_CONFIGNODE_ARG_AT_ARG_IS_NO_LONGER_LEADER_CLEANING_SERVICES_37CA9A1A =
          "ConfigNode %d at %s is no longer the leader; cleaning up leader services.";
  public static final String LOG_CONFIGNODE_ARG_AT_ARG_BECOMES_CONFIG_REGION_LEADER_2EB9B9E2 =
      "ConfigNode %d at %s becomes ConfigRegion leader.";
  public static final String LOG_CONFIGNODE_ARG_AT_ARG_AS_CONFIG_REGION_LEADER_IS_READY_74DFCB26 =
      "ConfigNode %d at %s as ConfigRegion leader is ready to work.";
  public static final String LOG_CONFIGNODE_ARG_AT_ARG_LEADER_SERVICES_ARE_UNAVAILABLE_0223CF16 =
      "ConfigNode %d at %s is no longer leader; all leader services are unavailable.";
  public static final String
      LOG_FAILED_TO_START_CONFIGNODE_LEADER_SERVICE_ON_NODE_ARG_AT_ARG_ARG_THE_NODE_WILL_FINISH_WARMING_UP_BUT_THIS_SERVICE_REMAINS_UNAVAILABLE_UNTIL_THE_NEXT_LEADERSHIP_TRANSITION_9E0D12F9 =
          "Failed to start ConfigNode leader service on node %s at %s: %s; the node will finish warming up, but this service remains unavailable until the next leadership transition.";

  public static final String MESSAGE_UNREADY_NODES_ARG_144B31E1 = "nodes=%s";
  public static final String MESSAGE_MORE_UNREADY_NODES_ARG_C7CDA960 = "...(%d more)";

  private ConfigNodeMessages() {}
}
