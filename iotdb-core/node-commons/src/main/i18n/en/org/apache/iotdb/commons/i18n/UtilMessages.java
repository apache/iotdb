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

package org.apache.iotdb.commons.i18n;

public final class UtilMessages {

  // ======================== AuthUtils ========================

  public static final String FAILED_TO_SERIALIZE_PARTIAL_PATH_LIST =
      "Failed to serialize PartialPath list";
  public static final String UNSUPPORTED_POSITION = "Not support position";
  public static final String UNSUPPORTED_PRIVILEGE_TYPE = "Not support PrivilegeType ";

  // ======================== BasicStructureSerDeUtil ========================

  public static final String STRING_LIST_MUST_NOT_BE_NULL = "stringList must not be null!";

  // ======================== BlobUtils ========================

  public static final String BLOB_MUST_BE_HEX_STRING =
      "Binary literal must be in the form X'hexstring'";
  public static final String BLOB_ONLY_HEX_DIGITS =
      "Binary literal can only contain hexadecimal digits";
  public static final String BLOB_EVEN_NUMBER_OF_DIGITS =
      "Binary literal must contain an even number of digits";

  // ======================== CommonDateTimeUtils ========================

  public static final String INTEGER_OVERFLOW_CONVERTING_TIME =
      "Integer overflow when converting {}ms to {}{}.";

  // ======================== FileUtils ========================

  public static final String DELETE_FOLDER_FAILED = "Delete folder failed: {}";
  public static final String COPY_FOLDER_SOURCE_NOT_EXIST =
      "Failed to copy folder, because source folder [{}] doesn't exist.";
  public static final String COPY_FOLDER_CREATE_TARGET_FAILED =
      "Failed to copy folder, because failed to create target folder[{}].";
  public static final String COPY_FOLDER_TARGET_ALREADY_EXISTS =
      "Failed to copy folder, because target folder [{}] already exist.";
  public static final String IO_EXCEPTION_ON_FILE = "get ioexception on file {}";
  public static final String MOVE_FILE_TARGET_ALREADY_EXISTS =
      "won't move file again because target file already exists: {}";
  public static final String MOVE_FILE_DELETE_SOURCE_HINT =
      "you may manually delete source file if necessary: {}";
  public static final String MOVE_FILE_START = "start to move file, {}";
  public static final String DELETE_UNFINISHED_TARGET_FAILED =
      "delete unfinished target file failed: {}";
  public static final String UNFINISHED_TARGET_DELETED =
      "unfinished target file which was created last time has been deleted: {}";
  public static final String FILE_COPY_FAIL = "file copy fail";
  public static final String FILE_RENAME_FAIL = "file rename fail";
  public static final String DELETE_SOURCE_FILE_FAIL = "delete source file fail: {}";
  public static final String MOVE_FILE_SUCCESS = "move file success, {}";
  public static final String HARDLINK_ALREADY_EXISTS =
      "Hardlink {} already exists, will not create it again. Source file: {}";
  public static final String HARDLINK_MISMATCH_RETRY =
      "Hardlink {} already exists but does not match source file {}, will try create it again.";
  public static final String FAILED_TO_CREATE_HARDLINK =
      "Failed to create hardlink {} for file {}: {}";
  public static final String FAILED_TO_CREATE_HARDLINK_PARENT_DIR =
      "failed to create hardlink %s for file %s: failed to create parent dir %s";
  public static final String FAILED_TO_COPY_FILE_PARENT_DIR =
      "failed to copy file %s to %s: failed to create parent dir %s";
  public static final String DELETED_DUPLICATE_FILE =
      "Deleted the file {} because it already exists in the target directory: {}";
  public static final String FAILED_TO_CREATE_TARGET_DIRECTORY =
      "failed to create target directory: {}";
  public static final String RENAMED_FILE_ALREADY_EXISTS =
      "Renamed file {} to {} because it already exists in the target directory: {}";
  public static final String COPIED_FILE_ALREADY_EXISTS =
      "Copy file {} to {} because it already exists in the target directory: {}";
  public static final String ILLEGAL_EMPTY_PATH = "The path cannot be empty. ";
  public static final String ILLEGAL_PATH_DOTS_OR_SEPARATORS =
      "The path cannot be '.', '..', './' or '.\\'. ";

  // ======================== IOUtils ========================

  public static final String CANNOT_DELETE_OLD_USER_FILE = "Cannot delete old user file : %s";
  public static final String CANNOT_REPLACE_OLD_USER_FILE =
      "Cannot replace old user file with new one : %s";

  // ======================== JVMCommonUtils ========================

  public static final String UNEXPECTED_ERROR_CHECKING_DISK_SPACE_FOR_DIR =
      "Unexpected error checking disk space for directory: {}";
  public static final String CANNOT_GET_FREE_SPACE =
      "Cannot get free space for {} after retries, please check the disk status";
  public static final String DISK_ABOVE_WARNING_THRESHOLD =
      "{} is above the warning threshold, free space {}, total space {}";
  public static final String UNEXPECTED_ERROR_CHECKING_DISK_SPACE =
      "Unexpected error checking disk space for {}";

  // ======================== FolderManager ========================

  public static final String ALL_FOLDERS_FULL_CHANGE_TO_READ_ONLY =
      "All folders are full, change system mode to read-only.";
  public static final String FAILED_TO_PROCESS_FOLDER = "Failed to process folder {}";
  public static final String FAILED_TO_READ_FILE_STORE_PATH =
      "Failed to read file store path '{}'";
  public static final String DISK_SPACE_INSUFFICIENT_READ_ONLY =
      "Disk space is insufficient, change system mode to read-only.";
  public static final String CANNOT_CALCULATE_OCCUPIED_SPACE =
      "Cannot calculate occupied space of folder {}";
  public static final String UNRECOGNIZED_MULTI_DIR_STRATEGY =
      "Unrecognized multi-dir strategy '{}', falling back to {}.";

  // ======================== NodeUrlUtils ========================

  public static final String BAD_CONFIG_NODE_URL = "Bad ConfigNode url: {}";
  public static final String BAD_NODE_URL = "Bad node url: %s";
  public static final String ENDPOINT_URLS_IS_NULL = "endPointUrls is null";

  // ======================== RetryUtils ========================

  public static final String OPERATION_SUCCEEDED_AFTER_RETRIES =
      "Operation '{}' succeeded after {} attempts";
  public static final String OPERATION_FAILED_RETRYING =
      "Operation '{}' failed (attempt {}). Retrying in {}ms...";
  public static final String RETRY_WAIT_INTERRUPTED =
      "Retry wait for operation '{}' was interrupted, stopping retries.";

  // ======================== KillPoint ========================

  public static final String KILL_POINT_SET = "Kill point set: {}";

  // ======================== ThriftCommonsSerDeUtils ========================

  public static final String WRITE_T_ENDPOINT_FAILED = "Write TEndPoint failed: ";
  public static final String READ_T_ENDPOINT_FAILED = "Read TEndPoint failed: ";
  public static final String WRITE_T_DATA_NODE_CONFIGURATION_FAILED =
      "Write TDataNodeConfiguration failed: ";
  public static final String READ_T_DATA_NODE_CONFIGURATION_FAILED =
      "Read TDataNodeConfiguration failed: ";
  public static final String WRITE_T_DATA_NODE_LOCATION_FAILED =
      "Write TDataNodeLocation failed: ";
  public static final String READ_T_DATA_NODE_LOCATION_FAILED =
      "Read TDataNodeLocation failed: ";
  public static final String WRITE_T_CREATE_CQ_REQ_FAILED = "Write TCreateCQReq failed: ";
  public static final String READ_T_CREATE_CQ_REQ_FAILED = "Read TCreateCQReq failed: ";
  public static final String WRITE_T_DATA_NODE_INFO_FAILED = "Write TDataNodeInfo failed: ";
  public static final String READ_T_DATA_NODE_INFO_FAILED = "Read TDataNodeInfo failed: ";
  public static final String WRITE_T_SERIES_PARTITION_SLOT_FAILED =
      "Write TSeriesPartitionSlot failed: ";
  public static final String READ_T_SERIES_PARTITION_SLOT_FAILED =
      "Read TSeriesPartitionSlot failed: ";
  public static final String WRITE_T_TIME_SLOT_LIST_FAILED = "Write TTimeSlotList failed: ";
  public static final String READ_T_TIME_SLOT_LIST_FAILED = "Read TTimeSlotList failed: ";
  public static final String WRITE_T_TIME_PARTITION_SLOT_FAILED =
      "Write TTimePartitionSlot failed: ";
  public static final String READ_T_TIME_PARTITION_SLOT_FAILED =
      "Read TTimePartitionSlot failed: ";
  public static final String WRITE_T_CONSENSUS_GROUP_ID_FAILED =
      "Write TConsensusGroupId failed: ";
  public static final String READ_T_CONSENSUS_GROUP_ID_FAILED =
      "Read TConsensusGroupId failed: ";
  public static final String WRITE_T_REGION_REPLICA_SET_FAILED =
      "Write TRegionReplicaSet failed: ";
  public static final String READ_T_REGION_REPLICA_SET_FAILED =
      "Read TRegionReplicaSet failed: ";
  public static final String WRITE_T_SCHEMA_NODE_FAILED = "Write TSchemaNode failed: ";
  public static final String READ_T_SCHEMA_NODE_FAILED = "Read TSchemaNode failed: ";
  public static final String WRITE_T_AI_NODE_INFO_FAILED = "Write TAINodeInfo failed: ";
  public static final String READ_T_AI_NODE_INFO_FAILED = "Read TAINodeInfo failed: ";
  public static final String WRITE_T_AI_NODE_LOCATION_FAILED = "Write TAINodeLocation failed: ";
  public static final String READ_T_AI_NODE_LOCATION_FAILED = "Read TDataNodeLocation failed: ";
  public static final String READ_T_AI_NODE_CONFIGURATION_FAILED =
      "Read TAINodeConfiguration failed: ";

  // ======================== ThriftConfigNodeSerDeUtils ========================

  public static final String WRITE_T_STORAGE_GROUP_SCHEMA_FAILED =
      "Write TStorageGroupSchema failed: ";
  public static final String READ_T_STORAGE_GROUP_SCHEMA_FAILED =
      "Read TStorageGroupSchema failed: ";
  public static final String WRITE_T_CONFIG_NODE_LOCATION_FAILED =
      "Write TConfigNodeLocation failed: ";
  public static final String READ_T_CONFIG_NODE_LOCATION_FAILED =
      "Read TConfigNodeLocation failed: ";
  public static final String WRITE_T_PIPE_SINK_INFO_FAILED = "Write TPipeSinkInfo failed: ";
  public static final String READ_T_PIPE_SINK_INFO_FAILED = "Read TPipeSinkInfo failed: ";

  private UtilMessages() {}
}
