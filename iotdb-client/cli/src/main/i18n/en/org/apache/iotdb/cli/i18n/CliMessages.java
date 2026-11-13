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

  public static final String LOG_UNKNOWN_696B0310 = "UNKNOWN";

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

  public static final String EXCEPTION_ARGS_ARGS_ARG_C39A089A = "Args args: %s";
  public static final String EXCEPTION_ARGS_ERROR_ARG_E49DCA27 = "Args error: %s";
  public static final String EXCEPTION_ARG_CSV_FILE_READ_EXCEPTION_BECAUSE_ARG_4F31CBC1 =
      "%s : CSV file read exception because: %s";
  public static final String EXCEPTION_CANNOT_DUMP_FAIL_RESULT_BECAUSE_ARG_5C6E82E0 =
      "Cannot dump fail result because: %s";
  public static final String EXCEPTION_CANNOT_DUMP_RESULT_BECAUSE_ARG_F78D917B =
      "Cannot dump result because: %s";
  public static final String EXCEPTION_ENCOUNTER_AN_ERROR_BECAUSE_ARG_484F4B80 =
      "Encounter an error, because %s";
  public static final String EXCEPTION_ENCOUNTER_AN_ERROR_BECAUSE_ARG_49284363 =
      "Encounter an error, because: %s";
  public static final String EXCEPTION_IMPORTDATA_THREAD_JOIN_INTERRUPTED_ARG_B94FC04B =
      "ImportData thread join interrupted: %s";
  public static final String EXCEPTION_PARSE_ERROR_ARG_DA103556 = "Parse error: %s";
  public static final String EXCEPTION_SQL_FILE_READ_EXCEPTION_BECAUSE_ARG_1626B867 =
      "SQL file read exception because: %s";
  public static final String EXCEPTION_UNEXPECTED_ERROR_OCCURRED_ARG_402FA331 =
      "Unexpected error occurred: %s";
  public static final String MESSAGE_ARG_EMPTY_FILE_4366EB0C = "%s : Empty file!";
  public static final String MESSAGE_ARG_IMPORT_COMPLETELY_FAIL_9B6F42FD =
      "%s : Import completely fail!";
  public static final String MESSAGE_ARG_IMPORT_COMPLETELY_SUCCESSFUL_BB17250E =
      "%s : Import completely successful!";
  public static final String MESSAGE_ARG_NO_RECORDS_5672039C = "%s : No records!";
  public static final String MESSAGE_ARG_PLEASE_INPUT_PATH_PATTERN_D784AEFD =
      "%s> please input path pattern: ";
  public static final String
      MESSAGE_ARG_THE_FILE_NAME_MUST_END_WITH_CSV_WHEN_SQL_DIALECT_TREE_OR_SQL_WHEN_SQL_DIALECT_TABLE_7A2FD49A =
          "%s : The file name must end with \"csv\" when sql_dialect tree or \"sql\" when"
              + " sql_dialect table!";
  public static final String MESSAGE_DOES_NOT_SUPPORT_EXPORTING_SYSTEM_DATABASES_ARG_D86F9100 =
      "Does not support exporting system databases %s";
  public static final String MESSAGE_EXPORT_SCHEMA_FAIL_ARG_75FE5946 = "Export schema fail: %s";
  public static final String MESSAGE_IMPORT_SCHEMA_FAIL_ARG_609A6D30 = "Import schema fail: %s";
  public static final String MESSAGE_LINE_ARG_COLUMN_ARG_ARG_UNKNOWN_COMPRESSIONTYPE_N_8E111BB4 =
      "Line '%s', column '%s': '%s' unknown compressionType %n";
  public static final String MESSAGE_LINE_ARG_COLUMN_ARG_ARG_UNKNOWN_DATATYPE_N_81E56571 =
      "Line '%s', column '%s': '%s' unknown dataType %n";
  public static final String MESSAGE_LINE_ARG_COLUMN_ARG_ARG_UNKNOWN_ENCODINGTYPE_N_066E2D0D =
      "Line '%s', column '%s': '%s' unknown encodingType %n";
  public static final String MESSAGE_LINE_ARG_COLUMN_ARG_ILLEGAL_PATH_ARG_D5C1EBD6 =
      "Line '%s', column '%s': illegal path %s";
  public static final String MESSAGE_PLEASE_INPUT_YOUR_KEY_STORE_F2FBBF40 =
      "please input your key_store:";
  public static final String MESSAGE_PLEASE_INPUT_YOUR_KEY_STORE_PWD_D18797A4 =
      "please input your key_store_pwd:";
  public static final String MESSAGE_PLEASE_INPUT_YOUR_PASSWORD_72B23434 =
      "please input your password:";
  public static final String MESSAGE_PLEASE_INPUT_YOUR_TRUST_STORE_387CF139 =
      "please input your trust_store:";
  public static final String MESSAGE_PLEASE_INPUT_YOUR_TRUST_STORE_PWD_F402D8D9 =
      "please input your trust_store_pwd:";
  public static final String MESSAGE_REQUIRED_VALUES_FOR_OPTION_ARG_NOT_PROVIDED_86467CA2 =
      "Required values for option '%s' not provided";
  public static final String MESSAGE_SOURCE_FILE_OR_DIRECTORY_ARG_DOES_NOT_EXIST_0299DF95 =
      "Source file or directory %s does not exist";
  public static final String
      MESSAGE_THE_DATABASE_PARAM_IS_REQUIRED_WHEN_SQL_DIALECT_IS_TABLE_1EFE6DAF =
          "The database param is required when sql_dialect is table ";
  public static final String
      MESSAGE_THE_HEADER_OF_THE_CSV_FILE_TO_BE_IMPORTED_IS_ILLEGAL_THE_CORRECT_FORMAT_IS_TIMESERIES_ALIAS_DATATYPE_ENCODING_COMPRESSION_FB573E37 =
          "The header of the CSV file to be imported is illegal. The correct format is"
              + " \"Timeseries, Alias, DataType, Encoding, Compression\"!";

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
      LOG_USE_INPUT_TO_SPECIFY_THE_INPUT_EXPORT_ALSO_REQUIRES_OUTPUT_EXPORT_CAN_USE_BACKUP_DIR_TO_CREATE_MONITOR_AND_CLEAN_UP_A_SNAPSHOT_PIPE_AUTOMATICALLY_IMPORT_REQUIRES_HOST_AND_PORT_USE_PASSWORD_STDIN_OR_PASSWORD_ENV_TO_AVOID_COMMAND_LINE_PASSWORDS_F5B223CB =
          "Use --input to specify the input. Export also requires --output; export can use --backup-dir to create, monitor, and clean up a snapshot Pipe automatically. Import requires --host and --port. Use --password-stdin or --password-env to avoid command-line passwords.";
  public static final String LOG_PIPE_LOGICAL_BACKUP_INSPECT_VERIFY_EXPORT_IMPORT_6D62F9CE =
      "logical-backup <inspect|verify|export|import>";
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
  public static final String
      EXCEPTION_LOGICAL_BACKUP_EXPORT_TIMEOUT_AND_POLL_INTERVAL_MUST_BE_POSITIVE_38622769 =
      "Logical backup export timeout and poll interval must be positive";
  public static final String
      LOG_LOGICAL_BACKUP_EXPORT_ARG_STATE_ARG_REMAINING_ARG_ESTIMATED_SECONDS_ARG_6256B85B =
      "Logical backup export %s: state=%s, remaining=%s, estimated_seconds=%s";
  public static final String EXCEPTION_LOGICAL_BACKUP_EXPORT_PIPE_ARG_FAILED_ARG_F52EB190 =
      "Logical backup export pipe %s failed: %s";
  public static final String
      EXCEPTION_LOGICAL_BACKUP_EXPORT_PIPE_ARG_TIMED_OUT_AFTER_ARG_SECONDS_76CF4B4B =
      "Logical backup export pipe %s timed out after %d seconds";
  public static final String
      EXCEPTION_LOGICAL_BACKUP_OPTION_ARG_MUST_BE_AN_INTEGER_C9B397BE =
      "Logical backup option --%s must be an integer";
  public static final String
      EXCEPTION_LOGICAL_BACKUP_OPTION_ARG_HAS_INVALID_IDENTIFIER_ARG_BB015204 =
      "Logical backup option --%s has invalid identifier: %s";

  public static final String LOG_EXPORT_IS_DEPRECATED_USE_DUMP_TO_GENERATE_A_BACKUP_AND_PACK_TO_CREATE_A_ZIP_47709D2D =
      "export is deprecated; use dump to generate a backup and pack to create a ZIP.";
  public static final String LOG_IMPORT_IS_DEPRECATED_USE_RESTORE_2F1AE2CE =
      "import is deprecated; use restore.";
  public static final String LOG_LOGICAL_BACKUP_ARG_1E5C23CE =
      "Logical backup: %s";
  public static final String LOG_LOGICAL_BACKUP_PACKED_FROM_ARG_TO_ARG_CB9D5304 =
      "Logical backup packed from %s to %s";
public static final String EXCEPTION_STOP_SEQUENCE_REQUIRES_A_STABLE_SOURCE_SEQUENCE_CAPABILITY_USE_UNTIL_WITH_THIS_SERVER_VERSION_8323B09E =
      "--stop-sequence requires a stable source sequence capability; use --until with this server version.";
  public static final String EXCEPTION_OBJECT_FILTERS_REQUIRE_MODEL_TREE_OR_MODEL_TABLE_SPLIT_MIXED_MODEL_FILTERED_DUMPS_INTO_SEPARATE_BACKUPS_0488699E =
      "Object filters require --model tree or --model table; split mixed-model filtered dumps into separate backups.";
  public static final String EXCEPTION_TABLE_FILTERS_USE_DATABASE_REGEX_TABLE_REGEX_EXCLUSIONS_MUST_USE_FOR_EITHER_DATABASE_OR_TABLE_42F0FA23 =
      "Table filters use database-regex/table-regex; exclusions must use .* for either database or table.";
  public static final String EXCEPTION_INVALID_LOGICAL_BACKUP_DUMP_OPTION_ARG_894E11E0 =
      "Invalid logical backup dump option: --%s";
  public static final String EXCEPTION_INVALID_OR_CONFLICTING_LOGICAL_BACKUP_ARGUMENTS_76233F81 =
      "Invalid or conflicting logical backup arguments.";
  public static final String EXCEPTION_ONLY_CONFLICT_FAIL_IS_SUPPORTED_RESTORE_NEVER_IMPLICITLY_OVERWRITES_OR_DELETES_TARGET_OBJECTS_70C9523E =
      "Only --conflict fail is supported; restore never implicitly overwrites or deletes target objects.";
  public static final String EXCEPTION_THE_EXISTING_BACKUP_TASK_DOES_NOT_MATCH_THIS_BACKUP_IDENTITY_OR_CAPTURE_OPTIONS_50E6388F =
      "The existing backup task does not match this backup identity or capture options.";
  public static final String EXCEPTION_ANOTHER_DUMP_PROCESS_IS_MANAGING_THIS_BACKUP_FE8EE828 =
      "Another dump process is managing this backup.";
  public static final String EXCEPTION_BACKUP_ARG_TASK_ARG_DIRECTORY_ARG_ARG_THE_TASK_IS_RETAINED_QUERY_SHOW_PIPE_OR_REPEAT_DUMP_WITH_THE_SAME_OPTIONS_AND_RESUME_APPEND_6F8C1E87 =
      "Backup %s, task %s, directory %s: %s. The task is retained; query SHOW PIPE or repeat dump with the same options and --resume append.";
  public static final String EXCEPTION_THE_BACKUP_TASK_HAS_NOT_PRODUCED_SEALED_STREAMS_YET_RESUME_THE_SAME_TASK_TO_FINISH_SEALING_87C9B599 =
      "The backup task has not produced sealed streams yet; resume the same task to finish sealing.";
  public static final String EXCEPTION_AN_EXISTING_CHECKPOINT_REQUIRES_RESUME_RESUME_REQUIRES_AN_EXISTING_CHECKPOINT_FB6D0B7D =
      "An existing checkpoint requires --resume; --resume requires an existing checkpoint.";
  public static final String EXCEPTION_THE_TARGET_TIMESTAMP_PRECISION_DOES_NOT_MATCH_THE_BACKUP_70B225CB =
      "The target timestamp precision does not match the backup.";
  public static final String EXCEPTION_THE_TARGET_VERSION_OR_DATA_MODEL_IS_INCOMPATIBLE_WITH_THE_BACKUP_220EB40C =
      "The target version or data model is incompatible with the backup.";
  public static final String EXCEPTION_THE_CHECKPOINT_MUST_BE_OUTSIDE_THE_INPUT_BACKUP_15B70F59 =
      "The checkpoint must be outside the input backup.";
  public static final String EXCEPTION_RESTORING_AUTHORIZATION_OBJECTS_REQUIRES_INCLUDE_AUTH_TRUE_INSPECT_THE_BACKUP_OR_USE_DRY_RUN_FIRST_83DDCD79 =
      "Restoring authorization objects requires --include-auth true; inspect the backup or use --dry-run first.";
  public static final String LOG_LOGICAL_BACKUP_DUMP_VERIFY_INSPECT_PACK_RESTORE_OPTIONS_DUMP_CONNECT_TO_A_SOURCE_AND_SEAL_A_BACKUP_DIRECTORY_DOES_NOT_CREATE_A_ZIP_INSPECT_SHOW_BACKUP_IDENTITY_SCOPE_STREAMS_SEGMENTS_COUNTS_AND_STATUS_VERIFY_CHECK_STRUCTURE_CHECKSUMS_AND_SEMANTICS_WITHOUT_CHANGING_THE_INPUT_PACK_VERIFY_AND_PACKAGE_AN_EXISTING_DIRECTORY_AS_ZIP_ACCEPTS_NO_CONNECTION_OPTIONS_RESTORE_PREFLIGHT_AND_REPLAY_A_DIRECTORY_ROOT_MANIFEST_JSON_OR_ZIP_INSPECT_VERIFY_INPUT_PATH_JSON_ALLOW_INCOMPLETE_PACK_INPUT_DIR_OUTPUT_ZIP_JSON_ALLOW_INCOMPLETE_DUMP_HOST_HOST_PORT_PORT_USER_ROOT_PASSWORD_STDIN_PASSWORD_ENV_NAME_BACKUP_DIR_DIR_BACKUP_ID_ID_SCOPE_CONFIG_SCHEMA_DATA_MODEL_TREE_TABLE_INCLUDE_PATTERN_EXCLUDE_PATTERN_START_TIME_TIME_END_TIME_TIME_INCLUDE_AUTH_TRUE_FALSE_MODE_SNAPSHOT_CONTINUOUS_UNTIL_ISO_8601_RESUME_FAIL_IF_EXISTS_APPEND_NEW_PIPE_NAME_NAME_SEGMENT_SIZE_BYTES_268435456_MAX_RECORD_BYTES_67108864_FSYNC_POLICY_ALWAYS_BATCH_PERIODIC_NONE_FSYNC_BATCH_OPERATIONS_N_FSYNC_PERIOD_MS_N_ON_UNSUPPORTED_FAIL_SKIP_INCLUDE_HEARTBEAT_TRUE_FALSE_TIMEOUT_SECONDS_3600_POLL_INTERVAL_SECONDS_2_JSON_ALLOW_INCOMPLETE_CONTINUOUS_REQUIRES_UNTIL_STOP_SEQUENCE_IS_RESERVED_AND_CURRENTLY_REJECTED_OBJECT_FILTERS_REQUIRE_ONE_MODEL_TREE_USES_PATH_LISTS_TABLE_USES_DATABASE_REGEX_TABLE_REGEX_TREE_CONFIG_SCHEMA_CAPTURE_ACCEPTS_A_SINGLE_PREFIX_FULL_PATH_WITHOUT_EXCLUSIONS_TABLE_EXCLUSIONS_MUST_SELECT_WHOLE_DATABASES_OR_WHOLE_TABLE_NAME_PATTERNS_RESTORE_INPUT_PATH_HOST_HOST_PORT_PORT_USER_USER_PASSWORD_STDIN_PASSWORD_ENV_NAME_DRY_RUN_CHECKPOINT_FILE_RESUME_CONFLICT_FAIL_INCLUDE_AUTH_TRUE_FALSE_JSON_ALLOW_INCOMPLETE_RESTORING_AUTHORIZATION_OBJECTS_REQUIRES_INCLUDE_AUTH_TRUE_LEGACY_CONFIG_IS_CONSERVATIVE_A_TARGET_CONNECTION_IS_REQUIRED_FOR_REPLAY_LOCAL_DRY_RUN_NEEDS_NO_CONNECTION_TARGET_PREFLIGHT_CHECKS_VERSION_PRECISION_AUTHENTICATION_AND_HANDSHAKE_SUPPORT_OBJECT_CONFLICTS_AND_PER_OBJECT_PRIVILEGES_ARE_CHECKED_DURING_REPLAY_DEPRECATED_ALIASES_EXPORT_INPUT_DIR_OUTPUT_ZIP_PACK_EXPORT_WITH_SOURCE_CONNECTION_OPTIONS_DUMP_THEN_PACK_IMPORT_RESTORE_EXIT_CODE_0_SUCCESS_1_FAILURE_ERRORS_AND_DEPRECATION_NOTICES_GO_TO_STDERR_B1983BE8 =
      "logical-backup <dump|verify|inspect|pack|restore> [options]\n  dump     Connect to a source and seal a backup directory; does not create a ZIP.\n  inspect  Show backup identity, scope, streams, segments, counts and status.\n  verify   Check structure, checksums and semantics without changing the input.\n  pack     Verify and package an existing directory as ZIP; accepts no connection options.\n  restore  Preflight and replay a directory, root manifest.json or ZIP.\n\ninspect/verify: --input PATH [--json] [--allow-incomplete]\npack: --input DIR --output ZIP [--json] [--allow-incomplete]\ndump: --host HOST --port PORT [--user root] (--password-stdin|--password-env NAME)\n  --backup-dir DIR --backup-id ID --scope config,schema,data [--model tree,table]\n  [--include PATTERN] [--exclude PATTERN] [--start-time TIME] [--end-time TIME]\n  [--include-auth true|false] [--mode snapshot|continuous] [--until ISO-8601]\n  [--resume fail-if-exists|append|new] [--pipe-name NAME]\n  [--segment-size-bytes 268435456] [--max-record-bytes 67108864]\n  [--fsync-policy always|batch|periodic|none] [--fsync-batch-operations N|--fsync-period-ms N]\n  [--on-unsupported fail|skip] [--include-heartbeat true|false]\n  [--timeout-seconds 3600] [--poll-interval-seconds 2] [--json] [--allow-incomplete]\n  continuous requires --until; --stop-sequence is reserved and currently rejected.\n  Object filters require one model: Tree uses path lists; Table uses database-regex/table-regex.\n  Tree config/schema capture accepts a single prefix/full path without exclusions.\n  Table exclusions must select whole databases or whole table-name patterns.\nrestore: --input PATH [--host HOST --port PORT --user USER\n  (--password-stdin|--password-env NAME)] [--dry-run] [--checkpoint FILE] [--resume]\n  [--conflict fail] [--include-auth true|false] [--json] [--allow-incomplete]\n  Restoring authorization objects requires --include-auth true (legacy config is conservative).\n  A target connection is required for replay; local dry-run needs no connection.\n  Target preflight checks version, precision, authentication and handshake support;\n  object conflicts and per-object privileges are checked during replay.\n\nDeprecated aliases: export --input DIR --output ZIP = pack;\n  export with source connection options = dump then pack; import = restore.\nExit code: 0 success, 1 failure. Errors and deprecation notices go to stderr.";
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

  public static final String LOG_CONNECT_IS_ABORT_TRY_TO_RECONNECT_MAX_RETRY_COUNT_ARG_EBF7D348 =
      "Connect is abort, try to reconnect, max retry count: %s";

  public static final String LOG_THE_ARG_TH_RETRY_WILL_AFTER_ARG_SECONDS_F4AECD33 =
      "The %sth retry will after %s seconds.";

  public static final String LOG_RECONNECT_SUCCESSFUL_B741C2DE = "Reconnect successful.";

  public static final String LOG_THE_ARG_TH_RECONNECT_FAILED_F5DEEF49 = "The %sth reconnect failed";

  public static final String
      LOG_HANDSHAKE_ERROR_WITH_TARGET_SERVER_IP_ARG_PORT_ARG_BECAUSE_ARG_RETRY_TO_HANDSHAKE_BY_PIPETRANSFERHANDSHAKEV1REQ_42529EF7 =
          "Handshake error with target server ip: %s, port: %s, because: %s. Retry to handshake by"
              + " PipeTransferHandshakeV1Req.";

  public static final String LOG_HANDSHAKE_SUCCESS_TARGET_SERVER_IP_ARG_PORT_ARG_8FA76B58 =
      "Handshake success. Target server ip: %s, port: %s";

  public static final String LOG_SUCCESSFULLY_TRANSFERRED_FILE_ARG_303718B9 =
      "Successfully transferred file %s";

  public static final String LOG_REDIRECT_FILE_POSITION_TO_ARG_2690B376 =
      "Redirect file position to %s.";

  public static final String LOG_FAILED_TO_CLOSE_CLIENT_BECAUSE_ARG_8EBA5673 =
      "Failed to close client because %s";

  public static final String
      LOG_REJECTING_FILE_ARG_DUE_TO_MEMORY_CONSTRAINTS_WILL_RETRY_LATER_AE83A15F =
          "Rejecting file [ %s ] due to memory constraints, will retry later.";

  public static final String LOG_FAILED_TO_IMPORT_ARG_FILE_ARG_F085D907 =
      "Failed to import [ %s ] file: %s";

  public static final String LOG_PROCESSED_FAIL_FILE_ARG_SUCCESSFULLY_B430F7F0 =
      "Processed fail file [ %s ] successfully!";

  public static final String LOG_FAILED_TO_PROCESS_FAIL_FILE_ARG_ARG_D01A0F75 =
      "Failed to process fail file [ %s ]: %s";

  public static final String LOG_IMPORTED_ARG_FILE_SUCCESSFULLY_B323C7DC =
      "Imported [ %s ] file successfully!";

  public static final String LOG_PROCESSED_SUCCESS_FILE_ARG_SUCCESSFULLY_B2CCF2E5 =
      "Processed success file [ %s ] successfully!";

  public static final String LOG_FAILED_TO_PROCESS_SUCCESS_FILE_ARG_ARG_8E0E4B9C =
      "Failed to process success file [ %s ]: %s";

  public static final String
      LOG_SUCCESSFULLY_LOAD_ARG_TSFILE_S_ON_SUCCESS_OPERATION_S_ARG_SUCCEED_ARG_FAILED_A04B748F =
          "Successfully load %s tsfile(s) (--on_success operation(s): %s succeed, %s failed)";

  public static final String
      LOG_FAILED_TO_LOAD_ARG_FILE_S_ON_FAIL_OPERATION_S_ARG_SUCCEED_ARG_FAILED_F5B202E2 =
          "Failed to load %s file(s) (--on_fail operation(s): %s succeed, %s failed)";

  public static final String LOG_UNPROCESSED_ARG_FILE_S_DUE_TO_UNEXPECTED_EXCEPTIONS_0950B97D =
      "Unprocessed %s file(s), due to unexpected exceptions";

  public static final String LOG_FOR_MORE_DETAILS_PLEASE_CHECK_THE_LOG_D0597775 =
      "For more details, please check the log.";

  public static final String LOG_TOTAL_OPERATION_TIME_ARG_MS_D8632871 =
      "Total operation time: %s ms.";

  public static final String LOG_WORK_HAS_BEEN_COMPLETED_B4F48618 = "Work has been completed!";

  public static final String LOG_TOO_FEW_ARGUMENTS_PLEASE_CHECK_THE_FOLLOWING_HINT_7EB254F2 =
      "Too few arguments, please check the following hint.";

  public static final String LOG_FAILED_TO_PARSE_THE_PROVIDED_OPTIONS_ARG_5E552330 =
      "Failed to parse the provided options: %s";

  public static final String LOG_ENCOUNTER_AN_ERROR_WHEN_PARSING_THE_PROVIDED_OPTIONS_ARG_D74C6F72 =
      "Encounter an error when parsing the provided options: %s";

  public static final String LOG_LOAD_REMOTELY_87E711F6 = "Load remotely.";

  public static final String LOG_LOAD_LOCALLY_F1E20946 = "Load locally.";

  public static final String
      LOG_ENCOUNTER_AN_ERROR_WHEN_GET_IOTDB_SERVER_TIMESTAMPPRECISION_ARG_DECCC828 =
          "Encounter an error when get IoTDB server timestampPrecision : %s";

  public static final String
      LOG_ENCOUNTER_AN_ERROR_THE_TIME_ACCURACY_OF_THE_IOTDB_SERVER_IS_ARG_BUT_THE_CLIENT_TIME_ACCURACY_IS_ARG_8A881838 =
          "Encounter an error The time accuracy of the iotdb server is \"%s\", but the client time"
              + " accuracy is %s";

  public static final String LOG_ARGS_ERROR_OS_OF_MUST_BE_ONE_OF_NONE_MV_CP_DELETE_6E478D75 =
      "Args error: os/of must be one of none, mv, cp, delete";

  public static final String LOG_UNKNOWN_HOST_ARG_EXCEPTION_ARG_WILL_USE_REMOTE_LOAD_7E2F7E65 =
      "Unknown host: %s. Exception: %s. Will use remote load.";

  public static final String LOG_IOEXCEPTION_WHEN_CHECKING_FILE_STORE_ARG_B5302D40 =
      "IOException when checking file store: %s";

  public static final String LOG_FAILED_TO_CREATE_ARG_ARG_3A4A1CFA = "Failed to create %s %s";

  public static final String LOG_IMPORT_TSFILE_FAIL_ARG_6A83990E = "Import tsfile fail: %s";

  public static final String LOG_IMPORTTSFILE_THREAD_JOIN_INTERRUPTED_ARG_E4141B99 =
      "ImportTsFile thread join interrupted: %s";

  public static final String LOG_EXPORT_TSFILE_COUNT_ARG_C216A69B = "Export TsFile Count: %s";

  public static final String LOG_EMPTY_FILE_83778141 = "Empty file!";

  public static final String LOG_THE_FIRST_FIELD_OF_HEADER_MUST_BE_TIME_F48E3999 =
      "The first field of header must be `Time`!";

  public static final String LOG_CSV_FILE_READ_EXCEPTION_BECAUSE_ARG_5EE7AFBF =
      "CSV file read exception because: %s";

  public static final String LOG_THE_FILE_NAME_MUST_END_WITH_CSV_OR_TXT_0C020AB3 =
      "The file name must end with \"csv\" or \"txt\"!";

  public static final String LOG_LINE_ARG_COLUMN_ARG_ARG_UNKNOWN_TYPE_ARG_93D3B31D =
      "Line '%s', column '%s': '%s' unknown type%n";

  public static final String LOG_LINE_ARG_COLUMN_ARG_ARG_CAN_T_CONVERT_TO_ARG_ARG_F85541A2 =
      "Line '%s', column '%s': '%s' can't convert to '%s'%n";

  public static final String LOG_MEET_ERROR_WHEN_QUERY_THE_TYPE_OF_TIMESERIES_BECAUSE_ARG_21949119 =
      "Meet error when query the type of timeseries because %s";

  public static final String
      LOG_THE_EXTRACTED_DATABASE_ARG_IN_SQL_STATEMENT_DOES_NOT_MATCH_THE_TARGET_DATABASE_ARG_2A442AEB =
          "The extracted database '%s' in SQL statement does not match the target database '%s'";

  public static final String LOG_THE_FIRST_FIELD_OF_HEADER_MUST_BE_TIME_2D6FAABE =
      "The first field of header must be `time`!";

  public static final String
      LOG_THESE_COLUMNS_DO_NOT_MATCH_THE_COLUMN_TYPES_IN_THE_TARGET_TABLE_ARG_FC76A705 =
          "These columns do not match the column types in the target table：%s";

  public static final String LOG_SQL_DIALECT_ARG_IS_NOT_SUPPORT_5301233A =
      "sql_dialect %s is not support";

  public static final String LOG_FILE_TYPE_ARG_IS_NOT_SUPPORT_D210E109 =
      "File type %s is not support";

  public static final String
      LOG_ERROR_INVALID_THREAD_NUMBER_ARG_PLEASE_SET_A_POSITIVE_INTEGER_AFD679CF =
          "error: Invalid thread number '%s'. Please set a positive integer.";

  public static final String
      LOG_HOST_ARG_IS_REMOTE_LOAD_ONLY_LOCAL_LOAD_IS_SUPPORTED_IN_TABLE_MODEL_AB4AEC31 =
          "host: %s is remote load,only local load is supported in table model";

  public static final String LOG_UNKNOWN_HOST_ARG_EXCEPTION_ARG_WILL_USE_LOCAL_LOAD_28E4A226 =
      "Unknown host: %s. Exception: %s. Will use local load.";

  public static final String LOG_FILE_NOT_FOUND_D5ED74DC = "File not found!";

  public static final String LOG_ENCOUNTER_AN_ERROR_WHEN_CONNECTING_TO_SERVER_BECAUSE_ARG_68D0B497 =
      "Encounter an error when connecting to server, because %s";

  public static final String LOG_ARG_IMPORT_COMPLETELY_790B9524 = "%s Import completely!";

  public static final String LOG_IMPORT_COMPLETELY_B7451B2C = "Import completely!";

  public static final String LOG_NO_RECORDS_05AB97C4 = "No records!";

  public static final String EXCEPTION_EMPTY_SET_A4A8C90D = "Empty set.";

  public static final String EXCEPTION_TOTAL_LINE_NUMBER_ARG_43CC83F4 = "Total line number = %s";

  public static final String
      LOG_REQUIRE_MORE_PARAMS_INPUT_EG_START_CLI_SH_START_CLI_BAT_IF_WINDOWS_H_XXX_XXX_XXX_XXX_P_XXXX_U_XXX_AE58F13C =
          "Require more params input, eg. ./start-cli.sh(start-cli.bat if Windows) -h"
              + " xxx.xxx.xxx.xxx -p xxxx -u xxx.";

  public static final String LOG_FOR_MORE_INFORMATION_PLEASE_CHECK_THE_FOLLOWING_HINT_85DAC74B =
      "For more information, please check the following hint.";

  public static final String LOG_ARG_INPUT_PARAMS_ERROR_BECAUSE_ARG_4CA51653 =
      "%s: Input params error because %s";

  public static final String LOG_ARG_EXIT_CLI_WITH_ERROR_ARG_4BAF4EB2 =
      "%s: Exit cli with error: %s";

  public static final String
      LOG_ARG_ERROR_FORMAT_OF_MAX_PRINT_ROW_COUNT_IT_SHOULD_BE_AN_INTEGER_NUMBER_F7E9B25D =
          "%s: error format of max print row count, it should be an integer number";

  public static final String LOG_ARG_CAN_T_EXECUTE_SQL_BECAUSE_ARG_9C05D8B0 =
      "%s: Can't execute sql because %s";

  public static final String EXCEPTION_ARG_REQUIRED_VALUES_FOR_OPTION_ARG_NOT_PROVIDED_1593074C =
      "%s: Required values for option '%s' not provided";

  public static final String EXCEPTION_ARG_REQUIRED_VALUES_FOR_OPTION_ARG_IS_NULL_E4215AA7 =
      "%s: Required values for option '%s' is null.";

  public static final String LOG_FETCH_SIZE_FORMAT_ERROR_PLEASE_INPUT_LIKE_ARG_10000_F43DF137 =
      "Fetch size format error, please input like %s=10000";

  public static final String LOG_FETCH_SIZE_FORMAT_ERROR_ARG_C2A64235 =
      "Fetch size format error, %s";

  public static final String LOG_FETCH_SIZE_HAS_SET_TO_ARG_1EC2519D = "Fetch size has set to %s";

  public static final String LOG_ARG_VERSION_ARG_BUILD_ARG_73F1169F =
      "%sversion %s (Build: %s)\n                                           \n";

  public static final String LOG_STARTING_IOTDB_CLI_D08E81B1 = "Starting IoTDB Cli";

  public static final String LOG_CURRENT_TIME_FORMAT_ARG_4AF56328 = "Current time format: %s";

  public static final String LOG_CURRENT_FETCH_SIZE_ARG_D0149220 = "Current fetch size: %s";

  public static final String LOG_YOUR_SQL_EXECUTE_YOUR_SQL_STATMENT_BFC89C6C =
      "    <your-sql>\t\t\t execute your sql statment";

  public static final String LOG_ARG_SHOW_HOW_MANY_TIMESERIES_ARE_IN_IOTDB_359C5CCB =
      "    %s\t\t show how many timeseries are in iotdb";

  public static final String LOG_ARG_XXX_EG_LONG_DEFAULT_ISO8601_YYYY_MM_DD_HH_MM_SS_AB86BCB9 =
      "    %s=xxx\t eg. long, default, ISO8601, yyyy-MM-dd HH:mm:ss.";

  public static final String LOG_ARG_SHOW_TIME_DISPLAY_TYPE_F4FDBA29 =
      "    %s\t show time display type";

  public static final String LOG_ARG_XXX_EG_08_00_ASIA_SHANGHAI_C03D8EBF =
      "    %s=xxx\t\t eg. +08:00, Asia/Shanghai.";

  public static final String LOG_ARG_SHOW_CLI_TIME_ZONE_9F84BADC = "    %s\t\t show cli time zone";

  public static final String LOG_ARG_XXX_SET_FETCH_SIZE_WHEN_QUERYING_DATA_FROM_SERVER_4B58C233 =
      "    %s=xxx\t\t set fetch size when querying data from server.";

  public static final String LOG_ARG_SHOW_FETCH_SIZE_58D138A1 = "    %s\t\t show fetch size";

  public static final String LOG_TIME_DISPLAY_FORMAT_ERROR_PLEASE_INPUT_LIKE_ARG_ISO8601_A0F723FB =
      "Time display format error, please input like %s=ISO8601";

  public static final String LOG_TIME_DISPLAY_FORMAT_ERROR_ARG_2BE8FA7D =
      "time display format error, %s";

  public static final String LOG_TIME_DISPLAY_TYPE_HAS_SET_TO_ARG_1E0B2163 =
      "Time display type has set to %s";

  public static final String LOG_TIME_ZONE_FORMAT_ERROR_PLEASE_INPUT_LIKE_ARG_08_00_26C955E6 =
      "Time zone format error, please input like %s=+08:00";

  public static final String LOG_TIME_ZONE_FORMAT_ERROR_ARG_F356979E = "Time zone format error: %s";

  public static final String LOG_TIME_ZONE_HAS_SET_TO_ARG_4D07E82F = "Time zone has set to %s";

  public static final String LOG_CURRENT_TIME_ZONE_ARG_A0221687 = "Current time zone: %s";

  public static final String LOG_CANNOT_GET_TIME_ZONE_FROM_SERVER_SIDE_BECAUSE_ARG_E3FE596C =
      "Cannot get time zone from server side because: %s";

  public static final String
      LOG_PLEASE_INPUT_LIKE_IMPORT_USER_MYFILE_NOTED_THAT_YOUR_FILE_PATH_CANNOT_CONTAIN_ANY_SPACE_CHARACTER_F0E1F5B8 =
          "Please input like: import /User/myfile. Noted that your file path cannot contain any"
              + " space character)";

  public static final String LOG_IT_COSTS_ARG_S_B7D08F3A = "It costs %.3fs";

  public static final String
      LOG_THIS_DISPLAY_1000_ROWS_PRESS_ENTER_TO_SHOW_MORE_INPUT_Q_TO_QUIT_F674ED97 =
          "This display 1000 rows. Press ENTER to show more, input 'q' to quit.";

  public static final String LOG_MSG_ARG_240992E0 = "Msg: %s";

  public static final String LOG_TRACING_ACTIVITIES_CE1F6C12 = "Tracing Activities:";

  public static final String EXCEPTION_TIMESTAMP_PRECISION_MS_US_NS_5DC07992 =
      "timestamp precision (ms/us/ns)";

  public static final String
      EXCEPTION_THE_SOURCE_FILE_OR_DIRECTORY_PATH_WHICH_CAN_BE_A_TSFILE_OR_A_DIRECTORY_CONTAINING_TSFILES_REQUIRED_5C5AC0CC =
          "The source file or directory path, which can be a tsfile or a directory containing"
              + " tsfiles. (required)";

  public static final String
      EXCEPTION_WHEN_LOADING_TSFILE_SUCCESSFULLY_DO_OPERATION_ON_TSFILE_AND_ITS_RESOURCE_AND_MODS_FILES_OPTIONAL_PARAMETERS_ARE_NONE_MV_CP_DELETE_REQUIRED_601E75DC =
          "When loading tsfile successfully, do operation on tsfile (and its .resource and .mods"
              + " files), optional parameters are none, mv, cp, delete. (required)";

  public static final String
      EXCEPTION_WHEN_LOADING_TSFILE_FAIL_DO_OPERATION_ON_TSFILE_AND_ITS_RESOURCE_AND_MODS_FILES_OPTIONAL_PARAMETERS_ARE_NONE_MV_CP_DELETE_REQUIRED_7BA55A84 =
          "When loading tsfile fail, do operation on tsfile (and its .resource and .mods files),"
              + " optional parameters are none, mv, cp, delete. (required)";

  public static final String EXCEPTION_THE_TARGET_FOLDER_WHEN_OS_IS_MV_OR_CP_C2F33AE4 =
      "The target folder when 'os' is 'mv' or 'cp'.";

  public static final String EXCEPTION_THE_TARGET_FOLDER_WHEN_OF_IS_MV_OR_CP_6F43F81D =
      "The target folder when 'of' is 'mv' or 'cp'.";

  public static final String
      EXCEPTION_THE_NUMBER_OF_THREADS_USED_TO_IMPORT_TSFILE_DEFAULT_IS_8_4623E27F =
          "The number of threads used to import tsfile, default is 8.";

  public static final String EXCEPTION_TIMESTAMP_PRECISION_MS_US_NS_OPTIONAL_E312B661 =
      "Timestamp precision (ms/us/ns) (optional)";

  public static final String EXCEPTION_DISPLAY_HELP_INFORMATION_3BBD4899 =
      "Display help information";

  public static final String EXCEPTION_HOST_NAME_OPTIONAL_FD001BC0 = "Host Name (optional)";

  public static final String EXCEPTION_USE_SSL_STATEMENT_OPTIONAL_92E1AA92 =
      "Use SSL statement. (optional)";

  public static final String EXCEPTION_TRUST_STORE_OPTIONAL_EA106F8E = "Trust store. (optional)";

  public static final String EXCEPTION_TRUST_STORE_PASSWORD_OPTIONAL_B017719D =
      "Trust store password. (optional)";

  public static final String EXCEPTION_KEY_STORE_FOR_MUTUAL_SSL_OPTIONAL_A617339F =
      "Key store for mutual SSL. (optional)";

  public static final String EXCEPTION_KEY_STORE_PASSWORD_FOR_MUTUAL_SSL_OPTIONAL_CA3006DD =
      "Key store password for mutual SSL. (optional)";

  public static final String EXCEPTION_SSL_PROTOCOL_OPTIONAL_F7C84E29 = "SSL protocol. (optional)";

  public static final String
      EXCEPTION_FILE_TYPE_YOU_CAN_CHOOSE_TSFILE_CSV_OR_SQL_REQUIRED_046E0304 =
          "File type?You can choose tsfile)、csv) or sql).(required)";

  public static final String
      EXCEPTION_EXPORT_FILE_TYPE_YOU_CAN_CHOOSE_TSFILE_CSV_OR_SQL_REQUIRED_04770009 =
          "Export file type ?You can choose tsfile)、csv) or sql).(required)";

  public static final String EXCEPTION_TYPES_OF_IMPORTED_FILES_CSV_SQL_TSFILE_REQUIRED_E2678FF9 =
      "Types of imported files: csv, sql, tsfile.(required)";

  public static final String
      EXCEPTION_OUTPUT_TIME_FORMAT_IN_CSV_FILE_YOU_CAN_CHOOSE_1_TIMESTAMP_NUMBER_LONG_2_ISO8601_DEFAULT_3_USER_DEFINED_PATTERN_LIKE_YYYY_MM_DD_HH_MM_SS_DEFAULT_ISO8601_OUTPUT_TIMESTAMP_IN_SQL_FILE_NO_MATTER_WHAT_TIME_FORMAT_IS_SET_OPTIONAL_D766AEFB =
          "Output time Format in csv file. You can choose 1) timestamp, number, long 2) ISO8601,"
              + " default 3) user-defined pattern like yyyy-MM-dd HH:mm:ss, default ISO8601.\n"
              + " OutPut timestamp in sql file, No matter what time format is set(optional)";

  public static final String EXCEPTION_TIME_ZONE_EG_08_00_OR_01_00_OPTIONAL_7BEA75F1 =
      "Time Zone eg. +08:00 or -01:00 .(optional)";

  public static final String EXCEPTION_TIMEOUT_FOR_SESSION_QUERY_OPTIONAL_69034017 =
      "Timeout for session query.(optional)";

  public static final String EXCEPTION_EXPORT_ALIGNED_INSERT_SQL_EA4543D3 =
      "export aligned insert sql";

  public static final String EXCEPTION_USE_THE_ALIGNED_INTERFACE_F915187B =
      "use the aligned interface";

  public static final String EXCEPTION_WHETHER_EXPORT_TO_SQL_OF_ALIGNED_OPTIONAL_CF4C6EA0 =
      "Whether export to sql of aligned.(optional)";

  public static final String EXCEPTION_WHETHER_TO_USE_THE_INTERFACE_OF_ALIGNED_OPTIONAL_440B01C4 =
      "Whether to use the interface of aligned.(optional)";

  public static final String
      EXCEPTION_CURRENTLY_SUPPORTS_TREE_AND_TABLE_MODEL_DEFAULT_TREE_OPTIONAL_FB4F4486 =
          "Currently supports tree and table model, default tree. (optional)";

  public static final String
      EXCEPTION_THE_DATABASE_TO_BE_EXPORTED_ONLY_TAKES_EFFECT_WHEN_SQL_DIALECT_IS_TABLE_AND_REQUIRED_WHEN_FILE_TYPE_IS_CSV_AND_TSFILE_OPTIONAL_DCB4863E =
          "The database to be exported,only takes effect when sql_dialect is table and required"
              + " when file_type is csv and tsfile.(optional)";

  public static final String
      EXCEPTION_THE_TABLE_TO_BE_EXPORTED_ONLY_TAKES_EFFECT_WHEN_SQL_DIALECT_IS_TABLE_OPTIONAL_718386C9 =
          "The table to be exported,only takes effect when sql_dialect is table.(optional)";

  public static final String
      EXCEPTION_ARG_IF_THE_Q_PARAMETER_IS_SPECIFIED_THIS_PARAMETER_DOES_NOT_TAKE_EFFECT_IF_THE_EXPORT_TYPE_IS_TSFILE_OR_SQL_THIS_PARAMETER_IS_REQUIRED_OPTIONAL_8F65937E =
          "%s.If the '-q' parameter is specified, this parameter does not take effect. If the"
              + " export type is tsfile or sql, this parameter is required. (optional)";

  public static final String EXCEPTION_ARG_AND_FILE_TYPE_IS_CSV_OPTIONAL_695FD8E8 =
      "%s and file_type is csv. (optional)";

  public static final String EXCEPTION_EXPORT_DATA_2F1FB3D9 = "Export Data";

  public static final String
      EXCEPTION_PLEASE_OBTAIN_HELP_INFORMATION_FOR_THE_CORRESPONDING_DATA_TYPE_BASED_ON_DIFFERENT_PARAMETERS_FOR_EXAMPLE_EXPORT_DATA_SH_HELP_TSFILE_EXPORT_DATA_SH_HELP_SQL_EXPORT_DATA_SH_HELP_CSV_0D410E8A =
          "Please obtain help information for the corresponding data type based on different"
              + " parameters, for example:\n"
              + "./export_data.sh -help tsfile\n"
              + "./export_data.sh -help sql\n"
              + "./export_data.sh -help csv";

  public static final String
      EXCEPTION_TOO_FEW_PARAMS_INPUT_PLEASE_CHECK_THE_FOLLOWING_HINT_E6D41E98 =
          "Too few params input, please check the following hint.";

  public static final String EXCEPTION_THE_START_TIME_TO_BE_EXPORTED_OPTIONAL_8133559A =
      "The start time to be exported (optional)";

  public static final String EXCEPTION_THE_END_TIME_TO_BE_EXPORTED_OPTIONAL_355E448D =
      "The end time to be exported. (optional)";

  public static final String EXCEPTION_TARGET_FILE_DIRECTORY_REQUIRED_9A407F06 =
      "Target file directory (required)";

  public static final String EXCEPTION_TARGET_FILE_DIRECTORY_DEFAULT_TARGET_OPTIONAL_52390574 =
      "Target file directory.default ./target (optional)";

  public static final String EXCEPTION_EXPORT_PATH_PATTERN_OPTIONAL_8E9C0DE8 =
      "Export Path Pattern (optional)";

  public static final String EXCEPTION_EXPORT_FILE_NAME_OPTIONAL_BBFDC3AE =
      "Export file name .(optional)";

  public static final String
      EXCEPTION_THE_MAX_FRAME_SIZE_OF_RPC_DEFAULT_IS_536870912_BYTES_OPTIONAL_DDC009D1 =
          "The max frame size of RPC, default is 536870912 bytes.(optional)";

  public static final String
      EXCEPTION_WILL_THE_DATA_TYPE_OF_TIMESERIES_BE_PRINTED_IN_THE_HEAD_LINE_OF_THE_CSV_FILE_ARG_YOU_CAN_CHOOSE_TRUE_OR_FALSE_OPTIONAL_D1409A80 =
          "Will the data type of timeseries be printed in the head line of the CSV file?%sYou can"
              + " choose true) or false) . (optional)";

  public static final String
      EXCEPTION_LINES_PER_DUMP_FILE_ONLY_EFFECTIVE_IN_TREE_MODEL_OPTIONAL_8138FC83 =
          "Lines per dump file,only effective in tree model.(optional)";

  public static final String
      EXCEPTION_EITHER_Q_OR_TABLE_IS_REQUIRED_WHEN_SQL_DIALECT_IS_TABLE_378EE68E =
          "Either '-q' or '-table' is required when 'sql-dialect' is' table '";

  public static final String EXCEPTION_MEET_ERROR_WHEN_INSERT_CSV_BECAUSE_F4B95972 =
      "Meet error when insert csv because ";

  public static final String EXCEPTION_MEET_ERROR_WHEN_INSERT_SQL_BECAUSE_54810B8F =
      "Meet error when insert sql because ";

  public static final String
      EXCEPTION_MEET_ERROR_WHEN_GET_TABLE_COLUMNS_INFORMATION_BECAUSE_3E210150 =
          "Meet error when get table columns information because ";

  public static final String EXCEPTION_THE_TARGET_DATABASE_ARG_DOES_NOT_EXIST_342912D0 =
      "The target database %s does not exist";

  public static final String
      EXCEPTION_THERE_ARE_NO_TABLES_OR_THE_TARGET_TABLE_ARG_DOES_NOT_EXIST_23BCB7AF =
          "There are no tables or the target table %s does not exist";

  public static final String
      EXCEPTION_THERE_ARE_NO_TABLES_TO_EXPORT_PLEASE_CHECK_IF_THE_TABLES_IN_THE_TARGET_DATABASE_EXIST_AND_IF_YOU_HAVE_PERMISSION_TO_ACCESS_THEM_1592463E =
          "There are no tables to export. Please check if the tables in the target database exist"
              + " and if you have permission to access them.";

  public static final String
      EXCEPTION_THE_PATH_TO_BE_EXPORTED_ONLY_TAKES_EFFECT_WHEN_SQL_DIALECT_IS_TREE_OPTIONAL_99A39D5E =
          "The path to be exported,only takes effect when sql_dialect is tree.(optional)";

  public static final String EXCEPTION_EXPORT_TSFILE_201F2F41 = "Export TsFile";

  public static final String EXCEPTION_IMPORT_DATA_446C4D18 = "Import Data";

  public static final String
      EXCEPTION_PLEASE_OBTAIN_HELP_INFORMATION_FOR_THE_CORRESPONDING_DATA_TYPE_BASED_ON_DIFFERENT_PARAMETERS_FOR_EXAMPLE_IMPORT_DATA_SH_HELP_TSFILE_IMPORT_DATA_SH_HELP_SQL_IMPORT_DATA_SH_HELP_CSV_F9320898 =
          "Please obtain help information for the corresponding data type based on different"
              + " parameters, for example:\n"
              + "./import_data.sh -help tsfile\n"
              + "./import_data.sh -help sql\n"
              + "./import_data.sh -help csv";

  public static final String
      EXCEPTION_THE_LOCAL_DIRECTORY_PATH_OF_THE_SCRIPT_FILE_FOLDER_TO_BE_LOADED_REQUIRED_AC5278C5 =
          "The local directory path of the script file (folder) to be loaded. (required)";

  public static final String
      EXCEPTION_SPECIFYING_A_DIRECTORY_TO_SAVE_FAILED_FILE_DEFAULT_YOUR_CSV_FILE_PATH_OPTIONAL_DADE5B55 =
          "Specifying a directory to save failed file, default YOUR_CSV_FILE_PATH (optional)";

  public static final String EXCEPTION_THE_TARGET_FOLDER_WHEN_OS_IS_MV_OR_CP_OPTIONAL_8AFE2264 =
      "The target folder when 'os' is 'mv' or 'cp'.(optional)";

  public static final String
      EXCEPTION_SPECIFYING_A_DIRECTORY_TO_SAVE_FAILED_FILE_DEFAULT_YOUR_CSV_FILE_PATH_OPTIONAL_EA6E7DF7 =
          "Specifying a directory to save failed file, default YOUR_CSV_FILE_PATH.(optional)";

  public static final String
      EXCEPTION_SPECIFYING_A_DIRECTORY_TO_SAVE_FAILED_FILE_DEFAULT_YOUR_SQL_FILE_PATH_OPTIONAL_8CF7BBC4 =
          "Specifying a directory to save failed file, default YOUR_SQL_FILE_PATH.(optional)";

  public static final String EXCEPTION_THE_TARGET_FOLDER_WHEN_OF_IS_MV_OR_CP_OPTIONAL_E72A0E75 =
      "The target folder when 'of' is 'mv' or 'cp'.(optional)";

  public static final String
      EXCEPTION_THE_NUMBER_OF_THREADS_USED_TO_IMPORT_TSFILE_DEFAULT_IS_8_OPTIONAL_EAAFD75C =
          "The number of threads used to import tsfile, default is 8.(optional)";

  public static final String
      EXCEPTION_10000_ONLY_NOT_ALIGNED_AND_SQL_DIALECT_TREE_OPTIONAL_75B83039 =
          "10000 (only not aligned and sql_dialect tree optional)";

  public static final String EXCEPTION_TIMESTAMP_PRECISION_MS_US_NS_OPTIONAL_10C6809A =
      "Timestamp precision (ms/us/ns).(optional)";

  public static final String
      EXCEPTION_DEFINE_TYPE_INFO_BY_OPTION_BOOLEAN_TEXT_INT_LONG_OPTIONAL_CF9F51B3 =
          "Define type info by option:\"boolean=text,int=long, ... (optional)";

  public static final String
      EXCEPTION_LINES_PER_FAILED_FILE_ONLY_TAKES_EFFECT_AND_REQUIRED_WHEN_SQL_DIALECT_IS_TABLE_OPTION_55D8B932 =
          "Lines per failed file,only takes effect and required when sql_dialect is table"
              + " .(option)";

  public static final String EXCEPTION_MEET_ERROR_WHEN_INIT_IMPORT_BECAUSE_A38F65C6 =
      "Meet error when init import because ";

  public static final String
      EXCEPTION_INVALID_ARGS_REQUIRED_VALUES_FOR_OPTION_ARG_NOT_PROVIDED_601B17E3 =
          "Invalid args: Required values for option '%s' not provided";

  public static final String
      EXCEPTION_PRESS_CTRL_C_AGAIN_TO_EXIT_OR_PRESS_ENTER_TO_CONTINUE_8CA49A92 =
          "Press CTRL+C again to exit, or press ENTER to continue";

  public static final String EXCEPTION_THE_STATEMENT_IS_EXECUTED_SUCCESSFULLY_B07F8CCB =
      "The statement is executed successfully.";

  public static final String EXCEPTION_DISPLAY_HELP_INFORMATION_OPTIONAL_BF7EAC3D =
      "Display help information. (optional)";

  public static final String EXCEPTION_DISPLAY_TIMESTAMP_IN_NUMERIC_FORMAT_OPTIONAL_74353C35 =
      "Display timestamp in numeric format. (optional)";

  public static final String EXCEPTION_HOST_NAME_DEFAULT_IS_127_0_0_1_OPTIONAL_487641A0 =
      "Host Name. Default is 127.0.0.1. (optional)";

  public static final String EXCEPTION_PORT_DEFAULT_IS_6667_OPTIONAL_97563E4C =
      "Port. Default is 6667. (optional)";

  public static final String EXCEPTION_USER_NAME_REQUIRED_45948411 = "User name. (required)";

  public static final String EXCEPTION_PASSWORD_DEFAULT_IS_ROOT_OPTIONAL_E6E2197E =
      "Password. Default is root. (optional)";

  public static final String EXCEPTION_ENABLE_OR_DISABLE_RPC_COMPRESSION_OPTIONAL_9F1F97AB =
      "Enable or disable Rpc Compression. (optional)";

  public static final String
      EXCEPTION_THE_TIMEOUT_IN_SECONDS_USES_THE_SERVER_CONFIGURATION_IF_NOT_SET_OPTIONAL_49A917F7 =
          "The timeout in seconds. Uses the server configuration if not set. (optional)";

  public static final String
      EXCEPTION_CURRENTLY_SUPPORTS_TREE_AND_TABLE_USES_TREE_IF_NOT_SET_OPTIONAL_9D030956 =
          "Currently supports tree and table; uses tree if not set. (optional)";

  public static final String EXCEPTION_ELAPSED_TIME_7083D10C = "Elapsed Time";
}
