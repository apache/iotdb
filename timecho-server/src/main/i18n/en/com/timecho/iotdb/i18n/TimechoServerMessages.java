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

  // Auth (separation of admin powers)
  public static final String UNSUPPORTED_AUTHOR_TYPE = "Unsupported authorType: ";
  public static final String ONLY_BUILTIN_ADMIN_CAN_GRANT_REVOKE_ADMIN =
      "Only the builtin admin can grant/revoke admin permissions";

  // Session
  public static final String CANNOT_DISCONNECT_EXPIRED_SESSION = "Cannot disconnect expired session {}";

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

  // Object table size index
  public static final String FAILED_TO_EXECUTE_COMPACTION_FOR_OBJECT_TABLE_SIZE_INDEX_FILE =
      "Failed to execute compaction for object table size index file";
  public static final String FAILED_TO_SYNC_OBJECT_TABLE_SIZE_INDEX_FILE =
      "Failed to sync object table size index file {}";

  // Migration tasks
  public static final String FAIL_TO_COPY_TSFILE_FROM_LOCAL_TO_LOCAL =
      "Fail to copy TsFile from local {} to local {}";
  public static final String FAIL_TO_SERIALIZE_REMOTE_STORAGE_INFO_INTO_FILE =
      "Fail to serialize remote storage info into file {}";
  public static final String FAIL_TO_MIGRATE_RESOURCE_FROM_LOCAL_TO_REMOTE =
      "Fail to migrate resource from local {} to remote {}";
  public static final String FAIL_TO_DELETE_LOCAL_TSFILE = "Fail to delete local TsFile {}";
  public static final String SUCCESSFULLY_DELETE_TSFILE_BY_SPACE_TL =
      "Successfully delete TsFile {} by the SpaceTL.";
  public static final String MIGRATE_TASK_ERROR = "migrate task error";

  // RPC / IPFilter
  public static final String CANNOT_INSTANTIATE_THIS_CLASS = "Cannot instantiate this class";
  public static final String INITIALIZING_WHITE_BLACK_LIST_UPDATE_CALLBACK =
      "Initializing white/black list update call back";
}
