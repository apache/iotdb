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

  // AbstractCli
  public static final String EXCEPTION_UNSUPPORTED_DATA_TYPE_ARG_B411C29E =
      "Unsupported data type: %s";

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

  // Data import/export
  public static final String EXCEPTION_FAILED_TO_INSERT_CSV_BATCH_ARG_159E0269 =
      "Failed to insert CSV batch: %s";
  public static final String MESSAGE_ARG_UNEXPECTED_ERROR_OCCURRED_ARG_E7255EE6 =
      "[%s] - Unexpected error occurred: %s";
  public static final String
      EXCEPTION_SQL_TSFILE_EXPORT_REQUIRES_A_QUERY_FROM_A_SINGLE_TABLE_IN_DATABASE_ARG_C9EB3768 =
          "SQL/TsFile export requires a query from a single table in database %s.";
  public static final String
      MESSAGE_QUERY_TO_EXPORT_OPTIONAL_TABLE_SQL_TSFILE_STORED_COLUMNS_OF_ONE_TABLE_6D79406B =
          "Query to export (optional). Table SQL/TsFile: stored columns of one table.";
  public static final String
      MESSAGE_MULTIPLE_CSV_HEADERS_RESOLVE_TO_THE_SAME_TARGET_COLUMN_99189563 =
          "Multiple CSV headers resolve to the same target column.";

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

  // Export data
  public static final String MESSAGE_EXPORT_LOG_FILE_DEFAULT_TARGET_LOG_EXPORT_DATA_YYYYMMDDHHMMSS_LOG_B29FC35F =
      "Export log file (default: <target>/log_export_data_yyyyMMddHHmmss.log).";
  public static final String MESSAGE_INCLUSIVE_START_TIME_FOR_MEASUREMENT_FILE_OR_AUTOMATIC_TABLE_EXPORT_69FE75CF =
      "Inclusive start time for measurement-file or automatic Table export.";
  public static final String MESSAGE_INCLUSIVE_END_TIME_FOR_MEASUREMENT_FILE_OR_AUTOMATIC_TABLE_EXPORT_D9C24D95 =
      "Inclusive end time for measurement-file or automatic Table export.";
  public static final String MESSAGE_UTF_8_IOTDB_PATH_PATTERN_FILE_FOR_TREE_EXPORT_MUTUALLY_EXCLUSIVE_WITH_Q_DD1F2CDA =
      "UTF-8 IoTDB path-pattern file for Tree export; mutually exclusive with -q.";
  public static final String MESSAGE_TABLE_TO_EXPORT_OPTIONAL_IGNORED_WHEN_Q_IS_GIVEN_4F37C0B5 =
      "Table to export (optional). Ignored when -q is given.";
  public static final String
      MESSAGE_DATABASE_TO_EXPORT_REQUIRED_WITHOUT_TABLE_OR_Q_EXPORTS_ALL_ITS_TABLES_704F6B55 =
          "Database to export (required). Without -table or -q, exports all its tables.";
  public static final String MESSAGE_MAXIMUM_DATA_ROWS_PER_CSV_OR_SQL_FILE_DEFAULT_10000_000CEFB9 =
      "Maximum data rows per CSV or SQL file (default 10000).";
  public static final String EXCEPTION_FILE_PREFIX_MUST_NOT_CONTAIN_PATH_SEPARATORS_ARG_C89823EE =
      "File prefix must not contain path separators: %s";
  public static final String EXCEPTION_INVALID_OUTPUT_TIME_FORMAT_ARG_9C690675 =
      "Invalid output time format: %s";
  public static final String EXCEPTION_EXPORTING_INFORMATION_SCHEMA_IS_NOT_SUPPORTED_6FD0FB85 =
      "Exporting information_schema is not supported.";
  public static final String EXCEPTION_THE_EXPORT_LOG_MUST_DIFFER_FROM_THE_MEASUREMENT_INPUT_FILE_088A542C =
      "The export log must differ from the measurement input file.";
  public static final String LOG_PLEASE_INPUT_YOUR_PASSWORD_AEE49DC5 =
      "Please input your password:";
  public static final String LOG_PLEASE_INPUT_YOUR_TRUST_STORE_4C50D824 =
      "Please input your trust_store:";
  public static final String LOG_PLEASE_INPUT_YOUR_TRUST_STORE_PASSWORD_86ED5CB6 =
      "Please input your trust_store_password:";
  public static final String LOG_PLEASE_INPUT_YOUR_KEY_STORE_AC5DB91F =
      "Please input your key_store:";
  public static final String LOG_PLEASE_INPUT_YOUR_KEY_STORE_PASSWORD_19ED16D4 =
      "Please input your key_store_password:";
  public static final String EXCEPTION_UNSUPPORTED_SQL_DIALECT_ARG_C4D0D97C =
      "Unsupported sql_dialect: %s";
  public static final String EXCEPTION_SPECIFY_AN_EXPORT_FORMAT_CSV_SQL_OR_TSFILE_RECEIVED_ARG_5B5211B5 =
      "Specify an export format: csv, sql or tsfile (received: %s).";
  public static final String EXCEPTION_MEASUREMENT_FILE_IS_SUPPORTED_ONLY_IN_THE_TREE_MODEL_1ED604B5 =
      "-measurement_file is supported only in the Tree model.";
  public static final String EXCEPTION_MEASUREMENT_FILE_AND_Q_ARE_MUTUALLY_EXCLUSIVE_7A7457D6 =
      "-measurement_file and -q are mutually exclusive.";
  public static final String EXCEPTION_UNEXPECTED_POSITIONAL_ARGUMENTS_ARG_027F44CB =
      "Unexpected positional arguments: %s";
  public static final String EXCEPTION_INVALID_POSITIVE_INTEGER_FOR_OPTION_ARG_ARG_30FB488D =
      "Invalid positive integer for option %s: %s";
  public static final String EXCEPTION_OPTION_ARG_REQUIRES_TRUE_OR_FALSE_ARG_0DC9BE3D =
      "Option %s requires true or false: %s";
  public static final String EXCEPTION_START_TIME_MUST_BE_LESS_THAN_OR_EQUAL_TO_END_TIME_3551199C =
      "start_time must be less than or equal to end_time.";
  public static final String EXCEPTION_CANNOT_PARSE_TIME_ARG_ARG_40257DF7 =
      "Cannot parse time %s: %s";
  public static final String EXCEPTION_MEASUREMENT_INPUT_MUST_BE_A_READABLE_REGULAR_FILE_ARG_43D4A272 =
      "Measurement input must be a readable regular file: %s";
  public static final String EXCEPTION_A_COMPLETE_IOTDB_PATH_PATTERN_STARTING_WITH_ROOT_IS_REQUIRED_7D000A79 =
      "A complete IoTDB path pattern starting with root is required.";
  public static final String EXCEPTION_INVALID_MEASUREMENT_INPUT_ARG_AT_LINE_ARG_CONTENT_ARG_ARG_27A0F948 =
      "Invalid measurement input %s at line %d, content=%s: %s";
  public static final String EXCEPTION_CANNOT_READ_UTF_8_MEASUREMENT_FILE_ARG_AT_LINE_ARG_ARG_A5739D3F =
      "Cannot read UTF-8 measurement file %s at line %d: %s";
  public static final String EXCEPTION_MEASUREMENT_INPUT_CONTAINS_NO_PATH_PATTERNS_ARG_96760070 =
      "Measurement input contains no path patterns: %s";
  public static final String EXCEPTION_MEASUREMENT_OUTPUT_FILENAME_EXCEEDS_THE_SUPPORTED_LENGTH_ARG_FF01275D =
      "Measurement output filename exceeds the supported length: %s";
  public static final String EXCEPTION_MEASUREMENTS_ARG_AND_ARG_MAP_TO_THE_SAME_OUTPUT_FILENAME_ARG_58E1FD4D =
      "Measurements %s and %s map to the same output filename: %s";
  public static final String EXCEPTION_EXPORT_OUTPUT_CONFLICTS_WITH_THE_LOG_FILE_ARG_F01BE79D =
      "Export output conflicts with the log file: %s";
  public static final String LOG_EXPORT_STARTED_45465087 =
      "Export started.";
  public static final String LOG_EXPORT_COMPLETELY_34200D19 =
      "Export completely!";
  public static final String EXCEPTION_EXPORT_CANCELLED_85D44F54 =
      "Export cancelled.";
  public static final String LOG_EXPORT_FAILED_77EE72CD =
      "Export failed.";
  public static final String LOG_CANNOT_WRITE_OR_CLOSE_THE_EXPORT_LOG_32579AC6 =
      "Cannot write or close the export log.";
  public static final String LOG_EXPORT_STARTED_ARG_ARG_TARGET_ARG_LOG_ARG_BD58AECB =
      "Export started (%s, %s): target %s, log %s";
  public static final String LOG_MEASUREMENTS_TO_EXPORT_ARG_PATH_PATTERNS_ARG_A0AC5F31 =
      "Measurements to export: %d (path patterns: %d).";
  public static final String LOG_EXPORTING_ARG_ARG_ARG_ARG_ROWS_ARG_ETA_ARG_C41CA790 =
      "Exporting %s  %d/%d  %s rows  %s  ETA %s";
  public static final String LOG_ARG_ARG_ARG_MEASUREMENTS_ARG_ROWS_ELAPSED_ARG_ETA_ARG_CURRENT_ARG_B6DEA0BD =
      "[%s] %d/%d measurements, %s rows, elapsed %s, ETA %s, current %s";
  public static final String LOG_EXPORTING_ARG_ROWS_ARG_ARG_ROWS_S_56BD441C =
      "Exporting %s rows  %s  %s rows/s";
  public static final String LOG_ARG_ROWS_ELAPSED_ARG_ARG_ROWS_S_AE3CF660 =
      "%s rows, elapsed %s, %s rows/s";
  public static final String LOG_WARNING_ARG_05BE32E9 =
      "WARNING: %s";
  public static final String LOG_EXPORT_COMPLETELY_MEASUREMENTS_ARG_ARG_ROWS_ARG_FILES_ARG_ELAPSED_ARG_36D9F8E5 =
      "Export completely! Measurements: %d/%d, rows: %s, files: %s, elapsed: %s.";
  public static final String LOG_EXPORT_COMPLETELY_ROWS_ARG_FILES_ARG_ELAPSED_ARG_A94BBB8B =
      "Export completely! Rows: %s, files: %s, elapsed: %s.";
  public static final String LOG_EXPORT_FAILED_AFTER_ARG_ARG_C123F0EF =
      "Export failed after %s: %s";
  public static final String LOG_EXPORT_FAILED_AFTER_ARG_DA6D56F9 =
      "Export failed after %s.";
  public static final String LOG_EXPORT_CANCELLED_AFTER_ARG_ROWS_EXPORTED_ARG_0999F99F =
      "Export cancelled after %s. Rows exported: %s.";
  public static final String LOG_LOG_ARG_FBF61F63 =
      "Log: %s";
  public static final String LOG_ARG_82AC62F7 =
      "%.2f%%";
  public static final String EXCEPTION_AGGREGATE_QUERIES_CANNOT_BE_EXPORTED_AS_SQL_OR_TSFILE_48BC1932 =
      "Aggregate queries cannot be exported as SQL or TsFile.";
  public static final String LOG_NO_MEASUREMENTS_MATCHED_ARG_MEASUREMENT_FILE_LINE_ARG_94C4EF10 =
      "No measurements matched %s (measurement file line %d).";
  public static final String LOG_START_TIME_AND_END_TIME_DO_NOT_APPLY_TO_QUERIES_AND_ARE_IGNORED_02DC37CC =
      "-start_time and -end_time do not apply to queries and are ignored.";
  public static final String LOG_TABLE_DOES_NOT_APPLY_TO_QUERIES_AND_IS_IGNORED_DBA31209 =
      "-table does not apply to queries and is ignored.";
  public static final String EXCEPTION_NO_MEASUREMENTS_REMAIN_AFTER_PATTERN_EXPANSION_AND_DEDUPLICATION_40D930B7 =
      "No measurements remain after pattern expansion and deduplication.";
  public static final String EXCEPTION_THE_TARGET_DATABASE_ARG_DOES_NOT_EXIST_80E096EB =
      "The target database %s does not exist.";
  public static final String EXCEPTION_THERE_ARE_NO_TABLES_OR_THE_TARGET_TABLE_ARG_DOES_NOT_EXIST_E5F88A98 =
      "There are no tables or the target table %s does not exist.";
  public static final String LOG_PLEASE_INPUT_QUERY_EEAD5433 =
      "Please input query: ";
  public static final String EXCEPTION_NO_EXPORT_QUERY_WAS_PROVIDED_09E24A92 =
      "No export query was provided.";
  public static final String LOG_EXPORT_FAILED_ARG_57F9420F =
      "Export failed: %s";
  public static final String EXCEPTION_CANNOT_OBTAIN_SCHEMA_FOR_MEASUREMENT_ARG_D3F1DAF3 =
      "Cannot obtain schema for measurement %s.";
  public static final String
      EXCEPTION_SQL_TSFILE_OUTPUT_REQUIRES_THE_TIME_COLUMN_AND_COLUMNS_OF_TABLE_ARG_14B7FB67 =
          "SQL/TsFile output requires the time column and columns of table %s.";
  public static final String
      EXCEPTION_SQL_TSFILE_EXPORT_CAN_ONLY_SELECT_OR_COLUMN_NAMES_WITHOUT_FILL_EC83126D =
          "SQL/TsFile export can only select * or column names, without FILL.";
  public static final String
      EXCEPTION_SQL_TSFILE_OUTPUT_REQUIRES_A_FIELD_COLUMN_FROM_TABLE_ARG_1C08B044 =
          "SQL/TsFile output requires a FIELD column from table %s.";
  public static final String
      EXCEPTION_TSFILE_OUTPUT_REQUIRES_EVERY_TAG_COLUMN_FROM_TABLE_ARG_901F420F =
          "TsFile output requires every TAG column from table %s.";
  public static final String EXCEPTION_SQL_OUTPUT_REQUIRES_ORIGINAL_MEASUREMENT_PATHS_OR_AN_ALIGN_BY_DEVICE_QUERY_ADA72EA8 =
      "SQL output requires original measurement paths or an ALIGN BY DEVICE query.";




  public static final String EXCEPTION_EXPORT_OUTPUT_CONFLICTS_WITH_THE_MEASUREMENT_INPUT_FILE_ARG_1CBEBB6C =
      "Export output conflicts with the measurement input file: %s";
  public static final String EXCEPTION_TREE_TSFILE_OUTPUT_REQUIRES_A_TIME_COLUMN_AND_ORIGINAL_MEASUREMENT_PATHS_1DCEE8EA =
      "Tree TsFile output requires a time column and original measurement paths.";

}
