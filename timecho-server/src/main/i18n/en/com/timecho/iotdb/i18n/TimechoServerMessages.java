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

package com.timecho.iotdb.i18n;

/** Compile-time i18n constants for TimechoDB server subsystems (English). */
public final class TimechoServerMessages {

  private TimechoServerMessages() {}

  // DataNode startup
  public static final String IOTDB_DATANODE_ENVIRONMENT_VARIABLES =
      "IoTDB-DataNode environment variables: {}";
  public static final String IOTDB_DATANODE_DEFAULT_CHARSET = "IoTDB-DataNode default charset is: {}";
  public static final String HARDWARE_GENERATION_FAILED = "hardware generation failed.";
  public static final String
      EXCEPTION_DATANODE_CANNOT_START_ON_WINDOWS_WHEN_ENABLE_SECURE_ERASE_IS_TRUE_SET_ENABLE_SECURE_ERASE_TO_FALSE_AND_RESTART_DATANODE_18580227 =
          "DataNode cannot start on Windows when enable_secure_erase is true. Set enable_secure_erase to false and restart DataNode.";

  // Auth (separation of admin powers)
  public static final String UNSUPPORTED_AUTHOR_TYPE = "Unsupported authorType: ";
  public static final String ONLY_BUILTIN_ADMIN_CAN_GRANT_REVOKE_ADMIN =
      "Only the builtin admin can grant/revoke admin permissions";

  // Session
  public static final String CANNOT_DISCONNECT_EXPIRED_SESSION = "Cannot disconnect expired session {}";
  public static final String MESSAGE_FAILED_TO_FETCH_DEVICE_LEADER_ARG_E11B34D5 =
      "Failed to fetch device leader: %s";
  public static final String MESSAGE_INVALID_TABLE_DEVICE_LEADER_REQUEST_F3FD7229 =
      "Invalid table device leader request.";

  // Shared storage compaction
  public static final String FAILED_TO_SELECT_SHARED_STORAGE_COMPACTION_TASK =
      "Failed to select shared storage compaction task.";
  public static final String CANNOT_GET_REMOTE_STORAGE_BLOCK_OF_TSFILE =
      "Cannot get the remote storage block of tsfile {}.";
  public static final String FAIL_TO_DELETE_REMOTE_TMP_FILES_IN_DIR =
      "Fail to delete remote tmp files in the dir {}";
  public static final String TSFILE_RESOURCE_CANNOT_BE_DELETED = "TsFileResource {} cannot be deleted:";
  public static final String STOP_COMPACTION_BECAUSE_OF_EXCEPTION_DURING_RECOVERING =
      "stop compaction because of exception during recovering";
  public static final String FAIL_TO_DELETE_OLD_LOG_FILE = "Fail to delete old log file {}";
  public static final String FAIL_TO_PULL_REMOTE_REPLICA_FROM_ENDPOINT =
      "Fail to pull remote replica from endpoint {}";
  public static final String FAIL_TO_PERSIST_REMOTE_REPLICA_OF_ENDPOINT =
      "Fail to persist remote replica of endpoint {}";
  public static final String
      LOG_FAIL_TO_DELETE_SHARED_OBJECT_FILES_FOR_DATA_REGION_ARG_TIME_PARTITION_ARG_6A59C405 =
          "Fail to delete shared object files for data region {} time partition {}";
  public static final String
      LOG_SKIP_DELETE_SHARED_OBJECT_FILES_FOR_DATA_REGION_ARG_TIME_PARTITION_UNMIGRATED_OBJECTS_REMAIN_94B6DD34 =
          "Skip deleting shared object files for data region {} time partition {} because unmigrated objects remain";

  // Object table size index
  public static final String FAILED_TO_EXECUTE_COMPACTION_FOR_OBJECT_TABLE_SIZE_INDEX_FILE =
      "Failed to execute compaction for object table size index file";
  public static final String FAILED_TO_SYNC_OBJECT_TABLE_SIZE_INDEX_FILE =
      "Failed to sync object table size index file {}";

  // Migration tasks
  public static final String FAIL_TO_COPY_TSFILE_FROM_LOCAL_TO_LOCAL =
      "Fail to copy TsFile from local {} to local {}";
  public static final String FAIL_TO_MIGRATE_OBJECT_FILE =
      "Fail to migrate object file from {} to {}";
  public static final String SUCCESSFULLY_MIGRATE_OBJECT_FILE =
      "Successfully migrate object file {} to {}, caused by {}, costs {}ns";
  public static final String SKIP_OBJECT_FILE_BECAUSE_TEMP_SIBLING_EXISTS =
      "Skip object file {} because temp sibling exists";
  public static final String ERROR_WHEN_CHECK_AND_TRY_TO_MIGRATE_OBJECT_FILE =
      "An error occurred when check and try to migrate object file {}";
  public static final String EXCEPTION_OBJECT_FILE_ARG_IS_NOT_UNDER_OBJECT_ROOT_ARG_1E9ADBF8 =
      "Object file %s is not under object root %s";
  public static final String EXCEPTION_OBJECT_DESTINATION_MISSING_AFTER_COPY_ARG_57B61C6C =
      "Object destination missing after copy: %s";
  public static final String FAIL_TO_SERIALIZE_REMOTE_STORAGE_INFO_INTO_FILE =
      "Fail to serialize remote storage info into file {}";
  public static final String FAIL_TO_MIGRATE_RESOURCE_FROM_LOCAL_TO_REMOTE =
      "Fail to migrate resource from local {} to remote {}";
  public static final String FAIL_TO_DELETE_LOCAL_TSFILE = "Fail to delete local TsFile {}";
  public static final String LOG_FAILED_TO_DELETE_MIGRATION_FILE_ARG_24B85A35 =
      "Failed to delete migration file {}";
  public static final String SUCCESSFULLY_DELETE_TSFILE_BY_SPACE_TL =
      "Successfully delete TsFile {} by the SpaceTL.";
  public static final String MIGRATE_TASK_ERROR = "migrate task error";
  public static final String
      LOG_AN_ERROR_OCCURRED_WHEN_CHECKING_AND_TRYING_TO_MIGRATE_TSFILE_ARG_A4343079 =
          "An error occurred when checking and trying to migrate TsFileResource {}";

  // RPC / IPFilter
  public static final String CANNOT_INSTANTIATE_THIS_CLASS = "Cannot instantiate this class";
  public static final String INITIALIZING_WHITE_BLACK_LIST_UPDATE_CALLBACK =
      "Initializing white/black list update call back";
  public static final String
      LOG_THE_IP_FORMAT_CONFIGURATION_FOR_ARG_LIST_IS_INCORRECT_THE_DETAILED_INFORMATION_OF_THE_INCORRECT_IPS_IS_ARG_8649B43F =
          "The IP format configuration for {}list is incorrect. The detailed information of the incorrect IPs is: {}";
}
