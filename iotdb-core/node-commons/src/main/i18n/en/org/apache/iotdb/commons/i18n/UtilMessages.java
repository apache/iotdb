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
  // ---------------------------------------------------------------------------
  // Additional auto-collected messages
  // ---------------------------------------------------------------------------
  public static final String EXCEPTION_INTERNAL_USER_NAMES_EXCEPT_DEFAULT_SUPERUSER_SEPARATION_DUTIES_ADMINS_MUST_3F045FC1 = "Internal user names (except the default superuser and separation-of-duties admins) must ";
  public static final String EXCEPTION_START_01CCD2EF = "start with \"";
  public static final String EXCEPTION_USER_NAMES_STARTING_8FDC637E = "User names starting with \"";
  public static final String EXCEPTION_RESERVED_SYSTEM_USE_CANNOT_USED_NEW_USERS_AS_RENAME_CEB73835 = "\" are reserved for system use and cannot be used for new users or as a rename ";
  public static final String EXCEPTION_TARGET_42AEFBAE = "target";
  public static final String EXCEPTION_LENGTH_NAME_MUST_GREATER_THAN_EQUAL_0387C3A5 = "The length of name must be greater than or equal to ";
  public static final String EXCEPTION_LENGTH_NAME_MUST_LESS_THAN_EQUAL_B7B31C94 = "The length of name must be less than or equal to ";
  public static final String EXCEPTION_NAME_CAN_ONLY_CONTAIN_LETTERS_NUMBERS_A313856E = "The name can only contain letters, numbers or !@#$%^*()_+-=";
  public static final String EXCEPTION_LENGTH_PASSWORD_MUST_GREATER_THAN_EQUAL_F95F3E8F = "The length of password must be greater than or equal to ";
  public static final String EXCEPTION_LENGTH_PASSWORD_MUST_LESS_THAN_EQUAL_C822ECBE = "The length of password must be less than or equal to ";
  public static final String EXCEPTION_PASSWORD_CAN_ONLY_CONTAIN_LETTERS_NUMBERS_D84EE152 = "The password can only contain letters, numbers or !@#$%^*()_+-=";
  public static final String EXCEPTION_ILLEGAL_SERIESPATH_ARG_SERIESPATH_SHOULD_START_ARG_FB9E3C07 = "Illegal seriesPath %s, seriesPath should start with \"%s\"";
  public static final String EXCEPTION_ILLEGAL_PATTERN_PATH_ARG_ONLY_PATTERN_PATH_END_SUPPORTED_16CBB77D = "Illegal pattern path: %s, only pattern path that end with ** are supported.";
  public static final String EXCEPTION_ILLEGAL_PATTERN_PATH_ARG_ONLY_PATTERN_PATH_END_WILDCARDS_SUPPORTED_7183A896 = "Illegal pattern path: %s, only pattern path that end with wildcards are supported.";
  public static final String EXCEPTION_NO_SUCH_PRIVILEGE_62644205 = "No such privilege ";
  public static final String MESSAGE_EXECUTED_SUCCESSFULLY_1EAF1169 = "Executed successfully.";
  public static final String MESSAGE_REQUEST_TIMED_OUT_FD587FC4 = "Request timed out.";
  public static final String MESSAGE_INCOMPATIBLE_VERSION_0C5CB2AF = "Incompatible version.";
  public static final String MESSAGE_FAILED_REMOVING_DATANODE_B4B7F050 = "Failed while removing DataNode.";
  public static final String MESSAGE_ALIAS_ALREADY_EXISTS_C05A2E5A = "Alias already exists.";
  public static final String MESSAGE_PATH_ALREADY_EXIST_56F8BFF9 = "Path already exist.";
  public static final String MESSAGE_PATH_DOES_NOT_EXIST_93310498 = "Path does not exist.";
  public static final String MESSAGE_MEET_ERROR_DEALING_METADATA_1C4A38B9 = "Meet error when dealing with metadata.";
  public static final String MESSAGE_INSERTION_TIME_LESS_THAN_TTL_TIME_BOUND_0F1BB861 = "Insertion time is less than TTL time bound.";
  public static final String MESSAGE_MEET_ERROR_MERGING_28424A77 = "Meet error while merging.";
  public static final String MESSAGE_MEET_ERROR_DISPATCHING_73E4FD5E = "Meet error while dispatching.";
  public static final String MESSAGE_DATABASE_PROCESSOR_RELATED_ERROR_C58690B1 = "Database processor related error.";
  public static final String MESSAGE_STORAGE_ENGINE_RELATED_ERROR_94DEBBCF = "Storage engine related error.";
  public static final String MESSAGE_TSFILE_PROCESSOR_RELATED_ERROR_B6C57C3E = "TsFile processor related error.";
  public static final String MESSAGE_ILLEGAL_PATH_020B26BC = "Illegal path.";
  public static final String MESSAGE_MEET_ERROR_LOADING_FILE_A90EBC21 = "Meet error while loading file.";
  public static final String MESSAGE_EXECUTE_STATEMENT_ERROR_54E4A395 = "Execute statement error.";
  public static final String MESSAGE_MEET_ERROR_PARSING_SQL_3C5A3B80 = "Meet error while parsing SQL.";
  public static final String MESSAGE_MEET_ERROR_GENERATING_TIME_ZONE_94E03CA3 = "Meet error while generating time zone.";
  public static final String MESSAGE_MEET_ERROR_SETTING_TIME_ZONE_CBE88DCF = "Meet error while setting time zone.";
  public static final String MESSAGE_QUERY_STATEMENTS_NOT_ALLOWED_ERROR_58D30C99 = "Query statements are not allowed error.";
  public static final String MESSAGE_LOGICAL_OPERATOR_RELATED_ERROR_D94D5972 = "Logical operator related error.";
  public static final String MESSAGE_LOGICAL_OPTIMIZE_RELATED_ERROR_36D63CB3 = "Logical optimize related error.";
  public static final String MESSAGE_UNSUPPORTED_FILL_TYPE_RELATED_ERROR_67BE2CF2 = "Unsupported fill type related error.";
  public static final String MESSAGE_QUERY_PROCESS_RELATED_ERROR_93FA1016 = "Query process related error.";
  public static final String MESSAGE_WRITING_DATA_RELATED_ERROR_0CA06C4D = "Writing data related error.";
  public static final String MESSAGE_INTERNAL_SERVER_ERROR_12F61DF7 = "Internal server error.";
  public static final String MESSAGE_MEET_ERROR_CLOSE_OPERATION_1C7D0589 = "Meet error in close operation.";
  public static final String MESSAGE_FAIL_DO_NON_QUERY_OPERATIONS_BECAUSE_SYSTEM_READ_ONLY_10CA1ED2 = "Fail to do non-query operations because system is read-only.";
  public static final String MESSAGE_DISK_SPACE_INSUFFICIENT_DF6205B0 = "Disk space is insufficient.";
  public static final String MESSAGE_MEET_ERROR_STARTING_UP_22A4CBFE = "Meet error while starting up.";
  public static final String MESSAGE_USERNAME_PASSWORD_WRONG_C44C4AF0 = "Username or password is wrong.";
  public static final String MESSAGE_HAS_NOT_LOGGED_A2BA0267 = "Has not logged in.";
  public static final String MESSAGE_NO_PERMISSIONS_OPERATION_PLEASE_ADD_PRIVILEGE_64047D1E = "No permissions for this operation, please add privilege.";
  public static final String MESSAGE_FAILED_INIT_AUTHORIZER_1E2B017E = "Failed to init authorizer.";
  public static final String MESSAGE_UNSUPPORTED_OPERATION_295CDB21 = "Unsupported operation.";
  public static final String MESSAGE_NODE_CANNOT_REACHED_D3FD04A8 = "Node cannot be reached.";
  public static final String LOG_ARG_ABOVE_WARNING_THRESHOLD_NOT_ACCESSIBLE_FREE_SPACE_ARG_TOTAL_87DAD16A = "{} is above the warning threshold, or not accessible, free space {}, total space {}";
  public static final String EXCEPTION_VALUE_IS_NULL_192F6BFF = "value is null";
  public static final String LOG_ARG_COLON_ARG_DCE519A1 = "{}: {}";
  public static final String EMPTY_MESSAGE = "";

}
