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

package org.apache.iotdb.cli.i18n;

public final class CliMessages {

  // CliContext
  public static final String EXITING_WITH_CODE = "Exiting with code %d";

  // Cli
  public static final String SUCCESSFULLY_LOGIN_AT = "Successfully login at %s";

  // IoTDBDataBackTool
  public static final String TARGET_DIR_EMPTY =
      " -targetdir cannot be empty， The backup folder must be specified";
  public static final String TARGET_DIR_USE_ABSOLUTE_PATH =
      "-targetdir parameter exception, please use absolute path";
  public static final String TARGET_DATA_DIR_USE_ABSOLUTE_PATH =
      "-targetdatadir parameter exception, please use absolute path";
  public static final String TARGET_WAL_DIR_USE_ABSOLUTE_PATH =
      "-targetwaldir parameter exception, please use absolute path";
  public static final String BACKUP_FOLDER_EXISTS = "The backup folder already exists:{}";
  public static final String ALL_OPERATIONS_COMPLETE = "all operations are complete";
  public static final String COPY_FILE_ERROR = "copy file error";
  public static final String COPY_FILE_ERROR_WITH_PATH = "copy file error {}";
  public static final String START_READ_CONFIG = "Start to read config file {}";
  public static final String READ_CONFIG_ERROR = "Read config file {} error";
  public static final String DIRECTORY_CREATED = "Directory created successfully:{}";
  public static final String FAILED_TO_CREATE_DIRECTORY = "Failed to create directory:{}";
  public static final String LINK_FILE_ERROR = "link file error {}";
  public static final String PROPERTIES_FILE_UPDATE_ERROR = "properties file update error.";
  public static final String FAILED_TO_READ_DATA = "Failed to read data from file: {}";
  public static final String FAILED_TO_WRITE_DATA = "Failed to write data to file: {}";
  public static final String FAILED_TO_CREATE_FILE = "Failed to create file: {}";

  // AbstractDataTool
  public static final String USE_HELP_FOR_MORE = "Use -help for more information";

  // ImportTsFileRemotely
  public static final String SYNC_CLIENT_INIT_ERROR = "Sync client init error because %s";

  // UnsupportedOperationException
  public static final String NOT_SUPPORTED_YET = "Not supported yet.";

  // ImportData
  public static final String UNKNOWN_TYPE_INFER_KEY = "Unknown type infer key: %s";
  public static final String UNKNOWN_TYPE_INFER_VALUE = "Unknown type infer value: %s";
  public static final String NAN_CANNOT_CONVERT = "NaN can not convert to %s";
  public static final String BOOLEAN_CANNOT_CONVERT = "Boolean can not convert to %s";
  public static final String DATE_CANNOT_CONVERT = "Date can not convert to %s";
  public static final String TIMESTAMP_CANNOT_CONVERT = "Timestamp can not convert to %s";
  public static final String BLOB_CANNOT_CONVERT = "Blob can not convert to %s";
  public static final String CANNOT_CONVERT = "%s can not convert to %s";
  public static final String
      MESSAGE_INVALID_ARGS_REQUIRED_VALUES_FOR_OPTION_TABLE_NOT_PROVIDED_4BC3FCFA =
          "Invalid args: Required values for option table not provided.";

  private CliMessages() {}
  // ---------------------------------------------------------------------------
  // Additional auto-collected messages
  // ---------------------------------------------------------------------------
  public static final String EXCEPTION_HANDSHAKE_ERROR_TARGET_SERVER_IP_ARG_PORT_ARG_BECAUSE_ARG_9D522E62 = "Handshake error with target server ip: %s, port: %s, because: %s.";
  public static final String EXCEPTION_NETWORK_ERROR_SEAL_FILE_ARG_BECAUSE_ARG_62E92EE8 = "Network error when seal file %s, because %s.";
  public static final String EXCEPTION_SEAL_FILE_ARG_ERROR_RESULT_STATUS_ARG_FE3B82AC = "Seal file %s error, result status %s.";
  public static final String EXCEPTION_NETWORK_ERROR_TRANSFER_FILE_ARG_BECAUSE_ARG_BC25323C = "Network error when transfer file %s, because %s.";
  public static final String EXCEPTION_TRANSFER_FILE_ARG_ERROR_RESULT_STATUS_ARG_E565D9FD = "Transfer file %s error, result status %s.";
  public static final String LOG_TARGETDATADIR_PARAMETER_EXCEPTION_NUMBER_ORIGINAL_PATHS_DOES_NOT_MATCH_NUMBER_8B31BF59 =
      "-targetdatadir parameter exception, the number of original paths does not match the number of"
      + " specified paths";
  public static final String LOG_TARGETWALDIR_PARAMETER_EXCEPTION_NUMBER_ORIGINAL_PATHS_DOES_NOT_MATCH_NUMBER_94AFE885 =
      "-targetwaldir parameter exception, the number of original paths does not match the number of"
      + " specified paths";
  public static final String LOG_DIRECTORY_BACKED_UP_CANNOT_SOURCE_DIRECTORY_PLEASE_CHECK_ARG_ARG_371383B7 = "The directory to be backed up cannot be in the source directory, please check:{},{},{}";
  public static final String LOG_DIRECTORY_BACKED_UP_CANNOT_SOURCE_DIRECTORY_PLEASE_CHECK_ARG_ARG_6DA7D5DA = "The directory to be backed up cannot be in the source directory, please check:{},{}";
  public static final String LOG_DIRECTORY_BACKED_UP_CANNOT_SOURCE_DIRECTORY_PLEASE_CHECK_ARG_CFA67674 = "The directory to be backed up cannot be in the source directory, please check:{}";
  public static final String LOG_TOTAL_FILE_NUMBER_A1554ADC = "total file number:";
  public static final String LOG_VERIFY_NUMBER_FILES_E171592C = ",verify the number of files:";
  public static final String LOG_BACKUP_FILE_NUMBER_72FC1312 = ",backup file number:";
  public static final String LOG_INPUT_TIME_FORMAT_ARG_NOT_SUPPORTED_00172A7B = "Input time format {} is not supported, ";
  public static final String LOG_PLEASE_INPUT_LIKE_YYYY_MM_DD_HH_MM_SS_SSS_9318BFC7 = "please input like yyyy-MM-dd\\ HH:mm:ss.SSS or yyyy-MM-dd'T'HH:mm:ss.SSS%n";

  // Pipe logical backup tool
  public static final String EXCEPTION_LOGICAL_BACKUP_COMMAND_FAILED_ARG_9973B0C0 =
      "Logical backup command failed: %s";
  public static final String EXCEPTION_UNKNOWN_LOGICAL_BACKUP_COMMAND_ARG_79275619 =
      "Unknown logical backup command: %s";
  public static final String EXCEPTION_OUTPUT_IS_REQUIRED_FOR_LOGICAL_BACKUP_EXPORT_603340B1 =
      "--output is required for logical backup export";
  public static final String LOG_STREAM_ARG_ARG_RECORDS_ARG_COMMITTED_EVENT_GROUPS_DE6BBAD0 =
      "Stream %s: %d records, %d committed event groups";
  public static final String
      LOG_LOGICAL_BACKUP_VERIFIED_ARG_STREAMS_ARG_RECORDS_ARG_COMMITTED_EVENT_GROUPS_5210360B =
          "Logical backup verified: %d streams, %d records, %d committed event groups";
  public static final String
      EXCEPTION_LOGICAL_BACKUP_EXPORT_TARGET_MUST_NOT_BE_INSIDE_SOURCE_A499F994 =
          "Logical backup export target must not be inside source";
  public static final String LOG_LOGICAL_BACKUP_EXPORTED_FROM_ARG_TO_ARG_B3E8D280 =
      "Logical backup exported from %s to %s";
  public static final String LOG_DRY_RUN_COMPLETED_NO_DATA_WAS_WRITTEN_38AE244B =
      "Dry run completed; no data was written";
  public static final String
      LOG_LOGICAL_BACKUP_IMPORT_COMPLETED_ARG_EVENT_GROUPS_CHECKPOINT_ARG_16F6A72D =
          "Logical backup import completed: %d event groups, checkpoint %s";
  public static final String EXCEPTION_LOGICAL_BACKUP_OPTION_ARG_IS_REQUIRED_1E7449AA =
      "Logical backup option --%s is required";
  public static final String
      EXCEPTION_LOGICAL_BACKUP_PASSWORD_ENVIRONMENT_VARIABLE_IS_NOT_SET_616738A2 =
          "Logical backup password environment variable is not set";
  public static final String EXCEPTION_LOGICAL_BACKUP_HANDSHAKE_FAILED_ARG_7CDD4697 =
      "Logical backup handshake failed: %s";
  public static final String EXCEPTION_LOGICAL_BACKUP_REQUEST_TYPE_ARG_FAILED_ARG_75EE2D11 =
      "Logical backup request type %d failed: %s";
  public static final String
      LOG_USE_INPUT_TO_SPECIFY_THE_INPUT_EXPORT_ALSO_REQUIRES_OUTPUT_IMPORT_REQUIRES_HOST_AND_PORT_USE_PASSWORD_STDIN_OR_PASSWORD_ENV_TO_AVOID_COMMAND_LINE_PASSWORDS_4677380E =
          "Use --input to specify the input. Export also requires --output; import requires --host and --port. Use --password-stdin or --password-env to avoid command-line passwords.";
  public static final String
      LOG_PIPE_LOGICAL_BACKUP_INSPECT_VERIFY_EXPORT_IMPORT_RESTORE_STATS_BFF9FDC2 =
          "pipe-logical-backup <inspect|verify|export|import|restore|stats>";
  public static final String EXCEPTION_LOGICAL_BACKUP_ARCHIVE_ENTRY_IS_UNSAFE_ARG_3E548152 =
      "Logical backup archive entry is unsafe: %s";
  public static final String EXCEPTION_LOGICAL_BACKUP_ARCHIVE_EXCEEDS_SAFETY_LIMIT_FFC54432 =
      "Logical backup archive exceeds safety limit";
  public static final String
      EXCEPTION_LOGICAL_BACKUP_EXPORT_SOURCE_MUST_BE_A_DIRECTORY_OR_MANIFEST_ARG_0BBDE9F0 =
          "Logical backup export source must be a directory or manifest: %s";
  public static final String
      EXCEPTION_SPECIFY_EXACTLY_ONE_OF_PASSWORD_STDIN_AND_PASSWORD_ENV_FOR_LOGICAL_BACKUP_IMPORT_A96813D9 =
          "Specify exactly one of --password-stdin and --password-env for logical backup import";
  public static final String EXCEPTION_NO_PASSWORD_WAS_READ_FROM_STANDARD_INPUT_6294AB8E =
      "No password was read from standard input";
  public static final String
      EXCEPTION_LOGICAL_BACKUP_CHECKPOINT_DOES_NOT_MATCH_THE_SOURCE_OR_TARGET_ARG_B978184D =
          "Logical backup checkpoint does not match the source or target: %s";
  public static final String EXCEPTION_LOGICAL_BACKUP_CHECKPOINT_IS_INVALID_ARG_71E82F4C =
      "Logical backup checkpoint is invalid: %s";
  public static final String EXCEPTION_UNSUPPORTED_LOGICAL_BACKUP_EXPORT_FORMAT_ARG_A6D7DEB1 =
      "Unsupported logical backup export format: %s";

}
