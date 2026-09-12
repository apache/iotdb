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

  public static final String FS_DIRECTORY_NOT_EMPTY = "Directory not empty: %s";
  public static final String FS_TREE_SUMMARY = "%d directories, %d files";
  public static final String FS_STAT_FILE = "File: %s";
  public static final String FS_STAT_TYPE = "Type: %s";
  public static final String FS_STAT_SIZE = "Size: %d bytes";
  public static final String FS_FILE_CSV = "CSV text";
  public static final String FS_FILE_METADATA = "CSV metadata text";
  public static final String FS_FILE_DIRECTORY = "directory";
  public static final String FS_FILE_UNKNOWN = "unknown";
  public static final String FS_CSV_PARSE_FAILED = "Failed to parse CSV input";
  public static final String MESSAGE_FS_INVALID_TIME_RANGE = "--start must not exceed --end";
  public static final String MESSAGE_FS_OFFSET_WITH_ZERO_LIMIT = "--offset requires a nonzero limit";
  public static final String FS_INVALID_WRITE_OPERATION =
      "Invalid filesystem write operation for this path";
  public static final String FS_WRITE_UNSUPPORTED = "Filesystem write operation is not supported";
  public static final String FS_SAME_FILE = "Source and destination are the same file: %s";
  public static final String FS_SCOPE_MODEL = "Option %s is not valid for the %s model";
  public static final String FS_SCOPE_PATH = "Object %s does not match path %s";
  public static final String FS_UNKNOWN_FIELD = "Unknown FIELD column: %s";
  public static final String FS_UNKNOWN_TAG = "Unknown TAG column: %s";
  public static final String FS_INVALID_REGEX = "Invalid regular expression: %s";
  public static final String MESSAGE_FS_INVALID_PATTERN = "Invalid pattern: %s";
  public static final String MESSAGE_FS_JOIN_UNSORTED = "join: input %s is not sorted";
  public static final String FS_CONFIRM_REPLACE = "%s: replace %s? [y/N] ";
  public static final String FS_CONFIRM_REMOVE = "rm: remove %s? [y/N] ";
  public static final String FS_EXPORT_TARGET =
      "export requires -o for one object or --output-dir for multiple objects";
  public static final String FS_PIPE_SYNTAX = "Invalid pipeline or redirection";
  public static final String FS_FOLLOW_COMPOUND = "tail -f cannot be used in pipelines or redirections";
  public static final String FS_SQL_READONLY = "This SQL statement requires --fs_write_mode enabled";
  public static final String FS_INTERRUPTED = "Command interrupted";
  public static final String FS_FILE_EXISTS = "File exists: %s";
  public static final String FS_VIRTUAL_MODE =
      "Virtual database objects do not support Unix permission modes";

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


  // Filesystem command execution
  public static final String MESSAGE_ARG_ARG_NO_SUCH_FILE_OR_DIRECTORY_ABDC5A9C = "%s: %s: No such file or directory";
  public static final String MESSAGE_ARG_ARG_NOT_A_DIRECTORY_CF18DCA5 = "%s: %s: Not a directory";
  public static final String MESSAGE_ARG_ARG_READ_ONLY_FILE_SYSTEM_A86EB99C = "%s: %s: Read-only file system";
  public static final String MESSAGE_UNSUPPORTED_FILESYSTEM_COMMAND_ARG_428768D0 = "Unsupported filesystem command: %s";
  public static final String MESSAGE_FAILED_TO_WRITE_STANDARD_OUTPUT_C1A5CCF7 = "Failed to write standard output";
  public static final String MESSAGE_FAILED_TO_READ_STANDARD_INPUT_3CB0AD1E = "Failed to read standard input";
  public static final String MESSAGE_CANNOT_EXECUTE_FILESYSTEM_COMMAND_ARG_C61FAE4B = "Cannot execute filesystem command: %s";
  public static final String MESSAGE_TEE_USE_WQ_TO_WRITE_OR_Q_TO_QUIT_WITHOUT_WRITING_C46EFD2C = "tee: use :wq to write or :q! to quit without writing";

  // Filesystem command validation
  public static final String MESSAGE_EMPTY_COMMAND_943E8DA9 = "Empty command";
  public static final String MESSAGE_UNCLOSED_QUOTE_OR_ESCAPE_IN_FILESYSTEM_COMMAND_42C74084 = "Unclosed quote or escape in filesystem command";
  public static final String MESSAGE_UNKNOWN_COMMAND_ARG_00157142 = "Unknown command: %s";
  public static final String MESSAGE_USE_HELP_COMMAND_OR_COMMAND_HELP_WITHOUT_OTHER_ARGUMENTS_3EED45E5 = "Use help [command] or <command> --help without other arguments";
  public static final String MESSAGE_SQL_STATEMENT_IS_EMPTY_676FCD59 = "SQL statement is empty";
  public static final String EXCEPTION_ARG_UNSUPPORTED_OPTION_ARG_33DE669D = "%s: unsupported option: %s";
  public static final String EXCEPTION_ARG_OPTION_SPECIFIED_MORE_THAN_ONCE_ARG_CEB275DB = "%s: option specified more than once: %s";
  public static final String EXCEPTION_ARG_MISSING_VALUE_FOR_ARG_695999CB = "%s: missing value for %s";
  public static final String EXCEPTION_ARG_UNEXPECTED_ARGUMENT_ARG_3EF9EC3F = "%s: unexpected argument: %s";
  public static final String EXCEPTION_ARG_EXPECTED_AT_LEAST_ARG_PATH_ARGUMENT_S_2040D496 = "%s: expected at least %d path argument(s)";
  public static final String EXCEPTION_ARG_PATH_MUST_NOT_BE_EMPTY_FEC583BE = "%s: path must not be empty";
  public static final String EXCEPTION_ARG_INVALID_UNSIGNED_INTEGER_FOR_ARG_ARG_D3792B04 = "%s: invalid unsigned integer for %s: %s";
  public static final String EXCEPTION_INVALID_TREE_DEPTH_ARG_EF544DD4 = "Invalid tree depth: %s";
  public static final String EXCEPTION_ARG_DELIMITER_MUST_BE_A_SINGLE_CHARACTER_23C3CA5E = "%s: delimiter must be a single character";
  public static final String EXCEPTION_INVALID_CUT_FIELDS_ARG_USE_POSITIVE_FIELD_NUMBERS_OR_ASCENDING_RANGES_95F4C873 = "Invalid cut fields: %s; use positive field numbers or ascending ranges";

  // Filesystem command help
  public static final String MESSAGE_USAGE_ARG_RESULT_ARG_DEFAULT_ARG_EXAMPLES_ARG_05BEA07B = "Usage: %s\nResult: %s\nDefault: %s\nExamples:\n  %s";
  public static final String MESSAGE_FILESYSTEM_COMMANDS_USE_HELP_COMMAND_FOR_DETAILS_38FE89C6 = "Filesystem commands (use help <command> for details):";
  public static final String MESSAGE_META_PATH_USAGE = "meta [path]";
  public static final String MESSAGE_META_PATH_RESULT = "Print object metadata rows for a table or timeseries path.";
  public static final String MESSAGE_META_PATH_DEFAULT = "Current directory; table paths may use /database/table or /database/table.csv.";
  public static final String MESSAGE_META_PATH_EXAMPLE = "meta /db1/table1.csv";
  public static final String MESSAGE_SCHEMA_PATH_USAGE = "schema [path]";
  public static final String MESSAGE_SCHEMA_PATH_RESULT = "Print schema rows for a table or timeseries path.";
  public static final String MESSAGE_SCHEMA_PATH_DEFAULT = "Current directory; table paths may use /database/table or /database/table.csv.";
  public static final String MESSAGE_SCHEMA_PATH_EXAMPLE = "schema /db1/table1.csv";
  public static final String MESSAGE_QUOTE_PATHS_AND_PATTERNS_CONTAINING_SPACES_USE_BEFORE_OPERANDS_BEGINNING_WITH_COUNTS_USE_UNSIGNED_DECIMAL_INTEGERS_WITHOUT_LEADING_ZEROS_FIELDS_START_AT_1_OPTIONS_MAY_PRECEDE_OR_FOLLOW_PATHS_SINGLETON_OPTIONS_MUST_NOT_REPEAT_WRITES_REQUIRE_FS_WRITE_MODE_ENABLED_BATCH_OUTPUT_GOES_TO_STDOUT_ERRORS_GO_TO_STDERR_EXIT_STATUS_0_SUCCESS_1_USAGE_ERROR_2_INPUT_ERROR_3_RUNTIME_ERROR_832F0BFC = "Quote paths and patterns containing spaces. Use -- before operands beginning with -.\nCounts use unsigned decimal integers without leading zeros; fields start at 1.\nOptions may precede or follow paths; singleton options must not repeat.\nWrites require --fs_write_mode enabled. Batch output goes to stdout; errors go to stderr.\nExit status: 0 success, 1 usage error, 2 input error, 3 runtime error.";
  public static final String MESSAGE_PWD_9003D1DF = "pwd";
  public static final String MESSAGE_ABSOLUTE_VIRTUAL_WORKING_DIRECTORY_A179DC18 = "Absolute virtual working directory.";
  public static final String MESSAGE_NO_OPTIONS_2420248A = "No options.";
  public static final String MESSAGE_LS_LAR_PATH_B103CAFF = "ls [-laR] [path]";
  public static final String MESSAGE_ENTRY_NAMES_L_ADDS_MODE_LINK_COUNT_OWNER_GROUP_AND_PLACEHOLDER_SIZE_A_INCLUDES_THE_CURRENT_AND_PARENT_DIRECTORIES_83802D00 = "Entry names; -l adds mode, link count, owner, group and placeholder size. -a includes the current and parent directories.";
  public static final String MESSAGE_CURRENT_DIRECTORY_R_PRINTS_THE_RECURSIVE_TREE_F35CB2DB = "Current directory; -R prints the recursive tree.";
  public static final String MESSAGE_LS_LS_LA_DB1_0A5F54F4 = "ls /\n  ls -la /db1";
  public static final String MESSAGE_LL_LAR_PATH_60CAF30F = "ll [-laR] [path]";
  public static final String MESSAGE_LONG_LISTING_AS_WITH_LS_L_1817C31A = "Long listing, as with ls -l.";
  public static final String MESSAGE_CURRENT_DIRECTORY_4B3788F6 = "Current directory (.).";
  public static final String MESSAGE_LL_A_DB1_13B82162 = "ll -a /db1";
  public static final String MESSAGE_CD_PATH_3F25118B = "cd [path]";
  public static final String MESSAGE_CHANGES_THE_VIRTUAL_WORKING_DIRECTORY_NO_OUTPUT_ON_SUCCESS_2F56A52C = "Changes the virtual working directory; no output on success.";
  public static final String MESSAGE_CD_DB1_CD_9B32B3E0 = "cd /db1\n  cd ..";
  public static final String MESSAGE_STAT_PATH_09D48F35 = "stat [path]";
  public static final String MESSAGE_PATH_VIRTUAL_FILE_TYPE_AND_AVAILABLE_METADATA_7EC74779 = "Path, virtual file type and available metadata.";
  public static final String MESSAGE_STAT_DB1_TABLE1_CSV_411B4588 = "stat /db1/table1.csv";
  public static final String MESSAGE_CAT_PATH_C889DFB2 = "cat [path ...]";
  public static final String MESSAGE_SIDECAR_TEXT_IS_PRINTED_UNCHANGED_DATA_ROWS_ARE_TAB_SEPARATED_WITHOUT_AN_ADDED_HEADER_2E8E51C3 = "Sidecar text is printed unchanged; data rows are tab-separated without an added header.";
  public static final String MESSAGE_CURRENT_DIRECTORY_READS_AT_MOST_20_ROWS_FROM_EACH_PATH_IN_ORDER_9BD68A9A = "Current directory; reads at most 20 rows from each path in order.";
  public static final String MESSAGE_CAT_DB1_TABLE1_CSV_CAT_REPORT_CSV_F9359B28 = "cat /db1/table1.csv\n  cat -- -report.csv";
  public static final String MESSAGE_HEAD_N_COUNT_COUNT_PATH_2A61E54C = "head [-n count | -count] [path]";
  public static final String MESSAGE_FIRST_COUNT_TEXT_LINES_OR_DATA_ROWS_8ABCCAD5 = "First count text lines or data rows.";
  public static final String MESSAGE_CURRENT_DIRECTORY_COUNT_IS_10_INCLUDING_ANY_SIDECAR_HEADER_6C7FD37F = "Current directory; count is 10, including any sidecar header.";
  public static final String MESSAGE_HEAD_N_5_DB1_TABLE1_CSV_4DE75834 = "head -n 5 /db1/table1.csv";
  public static final String MESSAGE_TAIL_N_COUNT_COUNT_PATH_07ADC736 = "tail [-n count | -count] [path]";
  public static final String MESSAGE_LAST_COUNT_TEXT_LINES_OR_DATA_ROWS_EF2CC0FC = "Last count text lines or data rows.";
  public static final String MESSAGE_TAIL_N_5_DB1_TABLE1_CSV_E75ED46B = "tail -n 5 /db1/table1.csv";
  public static final String MESSAGE_GREP_PATTERN_PATH_3EF6BB72 = "grep <pattern> <path>";
  public static final String MESSAGE_LINES_CONTAINING_THE_LITERAL_PATTERN_REGULAR_EXPRESSIONS_ARE_NOT_USED_47F2D493 = "Lines containing the literal pattern; regular expressions are not used.";
  public static final String MESSAGE_BOTH_PATTERN_AND_PATH_ARE_REQUIRED_SEARCHES_AT_MOST_20_ROWS_0B801CE1 = "Both pattern and path are required; searches at most 20 rows.";
  public static final String MESSAGE_GREP_DEVICE_1_DB1_TABLE1_CSV_6B7DEB82 = "grep \"device 1\" /db1/table1.csv";
  public static final String MESSAGE_FIND_PATH_NAME_PATTERN_D67E4643 = "find [path] [-name pattern]";
  public static final String MESSAGE_MATCHING_ABSOLUTE_PATHS_VISITED_RECURSIVELY_NAME_MATCHES_THE_EXACT_ENTRY_NAME_5B1560AA = "Matching absolute paths, visited recursively; -name matches the exact entry name.";
  public static final String MESSAGE_CURRENT_DIRECTORY_INCLUDES_ALL_NAMES_IF_NAME_IS_OMITTED_B5541942 = "Current directory; includes all names if -name is omitted.";
  public static final String MESSAGE_FIND_DB1_NAME_TABLE1_CSV_9B02F992 = "find /db1 -name table1.csv";
  public static final String MESSAGE_LESS_PATH_8196C183 = "less [path]";
  public static final String MESSAGE_TEXT_LINES_OR_DATA_ROWS_PRINTED_WITHOUT_INTERACTIVE_PAGING_6C652AEF = "Text lines or data rows printed without interactive paging.";
  public static final String MESSAGE_CURRENT_DIRECTORY_READS_AT_MOST_20_ROWS_29770E37 = "Current directory; reads at most 20 rows.";
  public static final String MESSAGE_LESS_DB1_TABLE1_CSV_1C486298 = "less /db1/table1.csv";
  public static final String MESSAGE_MORE_PATH_75C477B2 = "more [path]";
  public static final String MESSAGE_MORE_DB1_TABLE1_CSV_63580724 = "more /db1/table1.csv";
  public static final String MESSAGE_FILE_PATH_4928CBD2 = "file [path]";
  public static final String MESSAGE_ABSOLUTE_PATH_AND_VIRTUAL_FILE_TYPE_C3A88F3C = "Absolute path and virtual file type.";
  public static final String MESSAGE_FILE_DB1_TABLE1_CSV_A4914994 = "file /db1/table1.csv";
  public static final String MESSAGE_MKDIR_PATH_76FAFA85 = "mkdir [path]";
  public static final String MESSAGE_CREATES_A_TABLE_MODEL_DATABASE_NO_OUTPUT_ON_SUCCESS_DAE4AAC6 = "Creates a table-model database; no output on success.";
  public static final String MESSAGE_CURRENT_DIRECTORY_REQUIRES_FS_WRITE_MODE_ENABLED_5A485B47 = "Current directory; requires --fs_write_mode enabled.";
  public static final String MESSAGE_MKDIR_DB1_FD8E7AF9 = "mkdir /db1";
  public static final String MESSAGE_RMDIR_PATH_A23525AE = "rmdir [path]";
  public static final String MESSAGE_DROPS_A_TABLE_MODEL_DATABASE_AND_ITS_TABLES_NO_OUTPUT_ON_SUCCESS_1F281CFB = "Drops a table-model database and its tables; no output on success.";
  public static final String MESSAGE_RMDIR_DB1_40BDEEB7 = "rmdir /db1";
  public static final String MESSAGE_RM_R_PATH_B96CAAA7 = "rm [-r] <path>";
  public static final String MESSAGE_DROPS_THE_SELECTED_CSV_TABLE_R_DROPS_A_DATABASE_AND_ITS_TABLES_C499AA6A = "Drops the selected .csv table; -r drops a database and its tables.";
  public static final String MESSAGE_PATH_IS_REQUIRED_REQUIRES_FS_WRITE_MODE_ENABLED_4429FD1C = "Path is required; requires --fs_write_mode enabled.";
  public static final String MESSAGE_RM_DB1_TABLE1_CSV_RM_R_DB1_5F30D7BC = "rm /db1/table1.csv\n  rm -r /db1";
  public static final String MESSAGE_MV_SOURCE_TARGET_A3FDF16A = "mv <source> <target>";
  public static final String MESSAGE_RENAMES_A_CSV_TABLE_WITHIN_THE_SAME_DATABASE_NO_OUTPUT_ON_SUCCESS_214800C4 = "Renames a .csv table within the same database; no output on success.";
  public static final String MESSAGE_BOTH_PATHS_ARE_REQUIRED_REQUIRES_FS_WRITE_MODE_ENABLED_0CF793C2 = "Both paths are required; requires --fs_write_mode enabled.";
  public static final String MESSAGE_MV_DB1_TABLE1_CSV_DB1_TABLE2_CSV_E8D0CD22 = "mv /db1/table1.csv /db1/table2.csv";
  public static final String MESSAGE_CP_SOURCE_TARGET_AF9799A3 = "cp <source> <target>";
  public static final String MESSAGE_COPIES_A_SCHEMA_TABLE_DEFINITION_USING_CREATE_TABLE_LIKE_NO_DATA_IS_COPIED_4B219F3F = "Copies a .schema table definition using CREATE TABLE LIKE; no data is copied.";
  public static final String MESSAGE_CP_DB1_TABLE1_SCHEMA_DB1_TABLE2_SCHEMA_DD111A6B = "cp /db1/table1.schema /db1/table2.schema";
  public static final String MESSAGE_CUT_D_DELIMITER_F_FIELDS_PATH_9FE10722 = "cut [-d delimiter] -f fields <path>";
  public static final String MESSAGE_SELECTED_FIELDS_IN_SOURCE_ORDER_FIELDS_ACCEPT_COMMA_SEPARATED_POSITIVE_NUMBERS_AND_CLOSED_ASCENDING_RANGES_E5FF82DC = "Selected fields in source order; fields accept comma-separated positive numbers and closed ascending ranges.";
  public static final String MESSAGE_TAB_DELIMITER_FIELDS_AND_PATH_ARE_REQUIRED_READS_AT_MOST_20_ROWS_46932C89 = "Tab delimiter; fields and path are required; reads at most 20 rows.";
  public static final String MESSAGE_CUT_D_F1_3_5_DB1_TABLE1_CSV_D096FEBD = "cut -d, -f1-3,5 /db1/table1.csv";
  public static final String MESSAGE_PASTE_PATH_PATH_2EBAB7CB = "paste <path> [path ...]";
  public static final String MESSAGE_CORRESPONDING_LINES_JOINED_WITH_TABS_SHORTER_INPUTS_CONTRIBUTE_EMPTY_COLUMNS_A4A4DA9C = "Corresponding lines joined with tabs; shorter inputs contribute empty columns.";
  public static final String MESSAGE_AT_LEAST_ONE_PATH_IS_REQUIRED_READS_AT_MOST_20_ROWS_PER_PATH_2BF93D65 = "At least one path is required; reads at most 20 rows per path.";
  public static final String MESSAGE_PASTE_DB1_TABLE1_CSV_DB1_TABLE2_CSV_35614E37 = "paste /db1/table1.csv /db1/table2.csv";
  public static final String MESSAGE_JOIN_T_DELIMITER_1_FIELD_2_FIELD_PATH1_PATH2_2425772D = "join [-t delimiter] [-1 field] [-2 field] <path1> <path2>";
  public static final String MESSAGE_MATCHING_ROWS_JOINED_BY_KEY_FOLLOWED_BY_NON_KEY_FIELDS_FROM_EACH_INPUT_46F574EA = "Matching rows joined by key, followed by non-key fields from each input.";
  public static final String MESSAGE_WHITESPACE_DELIMITER_FIELD_1_IS_THE_KEY_READS_AT_MOST_20_ROWS_PER_INPUT_7C11E3C7 = "Whitespace delimiter; field 1 is the key; reads at most 20 rows per input.";
  public static final String MESSAGE_JOIN_T_1_2_2_1_DB1_TABLE1_CSV_DB1_TABLE2_CSV_7E3608F9 = "join -t, -1 2 -2 1 /db1/table1.csv /db1/table2.csv";
  public static final String MESSAGE_TEE_A_PATH_9071FE69 = "tee -a <path>";
  public static final String MESSAGE_APPENDS_STDIN_LINES_IN_BATCH_MODE_INTERACTIVELY_USE_WQ_TO_WRITE_OR_Q_TO_DISCARD_DD0C826A = "Appends stdin lines in batch mode; interactively use :wq to write or :q! to discard.";
  public static final String MESSAGE_A_AND_PATH_ARE_REQUIRED_REQUIRES_FS_WRITE_MODE_ENABLED_CA1D5E95 = "-a and path are required; requires --fs_write_mode enabled.";
  public static final String MESSAGE_TEE_A_DB1_TABLE1_CSV_DEFFDD8A = "tee -a /db1/table1.csv";
  public static final String MESSAGE_TREE_L_DEPTH_PATH_80213473 = "tree [-L depth] [path]";
  public static final String MESSAGE_ENTRY_NAMES_WITH_INDENTATION_FOR_EACH_DIRECTORY_LEVEL_0A2D420E = "Entry names with indentation for each directory level.";
  public static final String MESSAGE_CURRENT_DIRECTORY_UNLIMITED_DEPTH_DEPTH_0_PRINTS_NO_DESCENDANTS_7B9643C9 = "Current directory; unlimited depth. Depth 0 prints no descendants.";
  public static final String MESSAGE_TREE_L_2_DB1_4857586B = "tree -L 2 /db1";
  public static final String MESSAGE_SQL_STATEMENT_635619E5 = "sql <statement>";
  public static final String MESSAGE_SQL_PASSTHROUGH_IS_NOT_SUPPORTED_IN_FILESYSTEM_MODE_USE_THE_DEFAULT_SQL_ACCESS_MODE_6B83ED75 = "SQL passthrough is not supported in filesystem mode; use the default SQL access mode.";
  public static final String MESSAGE_THE_STATEMENT_IS_REQUIRED_AND_KEEPS_ITS_ORIGINAL_QUOTING_D8A652DF = "The statement is required and keeps its original quoting.";
  public static final String MESSAGE_SQL_SELECT_FROM_ROOT_SG_D1_74E0542D = "sql SELECT * FROM root.sg.d1";
  public static final String MESSAGE_HELP_COMMAND_D620EA8F = "help [command]";
  public static final String MESSAGE_GENERAL_HELP_OR_HELP_FOR_ONE_KNOWN_COMMAND_COMMAND_HELP_IS_EQUIVALENT_E6ADD9ED = "General help or help for one known command; <command> --help is equivalent.";
  public static final String MESSAGE_GENERAL_HELP_HELP_MUST_BE_USED_WITHOUT_OTHER_ARGUMENTS_67857584 = "General help; --help must be used without other arguments.";
  public static final String MESSAGE_HELP_HELP_HEAD_HEAD_HELP_77DB2FED = "help\n  help head\n  head --help";
  public static final String MESSAGE_EXIT_F24F62EE = "exit";
  public static final String MESSAGE_LEAVES_FILESYSTEM_MODE_QUIT_IS_AN_ALIAS_FOR_EXIT_B456121F = "Leaves filesystem mode; quit is an alias for exit.";
  public static final String MESSAGE_QUIT_DBD73C2B = "quit";
  public static final String EXCEPTION_WRITE_COMMAND_IS_REQUIRED_6DD7F72C = "write command is required";
  public static final String EXCEPTION_MISSING_VALUE_FOR_ARG_0AF4A1C7 = "Missing value for %s";
  public static final String EXCEPTION_ARG_SPECIFIED_MORE_THAN_ONCE_255E2870 = "%s specified more than once";
  public static final String EXCEPTION_CHOOSE_EXACTLY_ONE_OF_INPUT_OR_STDIN_966F4870 = "choose exactly one of --input or --stdin";
  public static final String EXCEPTION_UNKNOWN_WRITE_OPTION_ARG_EAF8B4F9 = "Unknown write option: %s";
  public static final String EXCEPTION_WRITE_REQUIRES_T_TABLE_4B9990EB = "write requires -t/--table";
  public static final String EXCEPTION_WRITE_REQUIRES_AT_LEAST_ONE_FIELD_COLUMN_E714D04C = "write requires at least one --field column";
  public static final String EXCEPTION_WRITE_REQUIRES_O_OUTPUT_25A5E793 = "write requires -o/--output";
  public static final String EXCEPTION_INVALID_NAME_ARG_NAMES_MUST_BE_NONEMPTY_UTF_8_WITHOUT_BOM_OR_CONTROL_CHARACTERS_C6B33704 = "invalid name '%s': names must be nonempty UTF-8 without BOM or control characters";
  public static final String EXCEPTION_NAME_ARG_IS_RESERVED_59CAD66D = "name '%s' is reserved";
  public static final String EXCEPTION_DUPLICATE_COLUMN_NAME_ARG_AB717F15 = "duplicate column name '%s'";
  public static final String EXCEPTION_UNKNOWN_TYPE_ARG_0FCF53E3 = "unknown type '%s'";
  public static final String EXCEPTION_TAG_COLUMN_ARG_MUST_USE_STRING_93F86185 = "TAG column '%s' must use STRING";
  public static final String EXCEPTION_PHYSICAL_OVERRIDE_TYPE_ARG_MUST_BE_A_USED_CANONICAL_DATA_TYPE_C36A91EC = "physical override type '%s' must be a used canonical data type";
  public static final String EXCEPTION_PHYSICAL_OVERRIDE_TYPE_ARG_IS_NOT_USED_BY_ANY_DECLARED_TAG_OR_FIELD_E0A808E7 = "physical override type %s is not used by any declared TAG or FIELD";
  public static final String EXCEPTION_ARG_FOR_DATA_TYPE_ARG_SPECIFIED_MORE_THAN_ONCE_6D9687F3 = "%s for data type %s specified more than once";
  public static final String EXCEPTION_ENCODING_ARG_IS_NOT_SUPPORTED_FOR_DATA_TYPE_ARG_218D4FB0 = "encoding %s is not supported for data type %s";
  public static final String EXCEPTION_COMPRESSION_ARG_IS_NOT_SUPPORTED_18307F13 = "compression %s is not supported";
  public static final String MESSAGE_WRITE_TABLE_NAME_TAG_NAME_STRING_FIELD_NAME_TYPE_ENCODING_TYPE_ENCODING_COMPRESSION_TYPE_COMPRESSION_I_INPUT_INPUT_CSV_STDIN_O_OUTPUT_OUT_TSFILE_V_VERBOSE_AE31E0C1 = "write --table <name> (--tag <name> STRING)* (--field <name> <type>)+ [--encoding <type> <encoding>] [--compression <type> <compression>] (-i/--input <input.csv> | --stdin) -o/--output <out.tsfile> [-v/--verbose]";
  public static final String MESSAGE_CREATE_A_NEW_LOCAL_TABLE_MODEL_TSFILE_FROM_STRICT_CSV_SUCCESS_IS_SILENT_V_PRINTS_DETAILS_TO_STDERR_2ADF8D40 = "Create a new local table-model TsFile from strict CSV. Success is silent; -v prints details to stderr.";
  public static final String MESSAGE_REQUIRES_FS_WRITE_MODE_ENABLED_THE_TARGET_MUST_NOT_EXIST_CSV_REQUIRES_TIME_AND_EXACTLY_THE_DECLARED_COLUMNS_UNQUOTED_N_IS_NULL_TIME_MUST_STRICTLY_INCREASE_PER_TAG_DEVICE_LOCAL_PATHS_ARE_RELATIVE_TO_THE_PROCESS_WORKING_DIRECTORY_43E64C5C = "Requires --fs_write_mode enabled. The target must not exist. CSV requires time and exactly the declared columns; unquoted \\N is null. Time must strictly increase per TAG device. Local paths are relative to the process working directory.";
  public static final String MESSAGE_WRITE_TABLE_SENSORS_TAG_SITE_STRING_FIELD_TEMPERATURE_DOUBLE_I_INPUT_CSV_O_OUTPUT_TSFILE_D9241DEC = "write --table sensors --tag site STRING --field temperature DOUBLE -i input.csv -o output.tsfile";
  public static final String EXCEPTION_CSV_HEADER_IS_MISSING_REQUIRED_COLUMN_ARG_27DD7D74 = "CSV header is missing required column '%s'";
  public static final String EXCEPTION_INVALID_CSV_HEADER_NAME_ARG_FE536B69 = "Invalid CSV header name '%s'";
  public static final String EXCEPTION_CSV_HEADER_NAME_ARG_CONFLICTS_CASE_INSENSITIVELY_CB40F241 = "CSV header name '%s' conflicts case-insensitively";
  public static final String EXCEPTION_CSV_CONTAINS_UNDECLARED_COLUMN_ARG_2E8E0B09 = "CSV contains undeclared column '%s'";
  public static final String EXCEPTION_EXPECTED_ARG_FIELDS_GOT_ARG_LINE_ARG_848602BC = "Expected %d fields, got %d (line %d)";
  public static final String EXCEPTION_BAD_TIMESTAMP_ARG_LINE_ARG_EA69CAC9 = "Bad timestamp '%s' (line %d)";
  public static final String EXCEPTION_TIMESTAMPS_MUST_BE_STRICTLY_INCREASING_PER_DEVICE_LINE_ARG_ARG_PREVIOUS_ARG_ECC72725 = "Timestamps must be strictly increasing per device (line %d: %d <= previous %d)";
  public static final String EXCEPTION_UNTERMINATED_QUOTED_CSV_FIELD_LINE_ARG_24E8F1B9 = "Unterminated quoted CSV field (line %d)";
  public static final String EXCEPTION_INVALID_OR_OUT_OF_RANGE_ARG_VALUE_ARG_LINE_ARG_341A3B62 = "Invalid or out-of-range %s value '%s' (line %d)";
  public static final String EXCEPTION_INVALID_UTF_8_OR_MISPLACED_BOM_LINE_ARG_16664A56 = "Invalid UTF-8 or misplaced BOM (line %d)";
  public static final String EXCEPTION_OUTPUT_TARGET_ALREADY_EXISTS_ARG_238A4D83 = "Output target already exists: %s";
  public static final String MESSAGE_CANNOT_WRITE_OUTPUT_ARG_ARG_AB420A33 = "Cannot write output '%s': %s";
  public static final String MESSAGE_CANNOT_REMOVE_TEMPORARY_OUTPUT_ARG_ARG_187E734C = "Cannot remove temporary output '%s': %s";
  public static final String EXCEPTION_INPUT_MUST_BE_A_REGULAR_CSV_FILE_ARG_AEB02C75 = "Input must be a regular CSV file: %s";
  public static final String EXCEPTION_COMPRESSION_ARG_IS_NOT_SUPPORTED_BY_THE_BUNDLED_TSFILE_WRITER_03C0806D = "Compression %s is not supported by the bundled TsFile writer";
  public static final String MESSAGE_CREATED_MODEL_TABLE_OBJECT_ARG_ROWS_ARG_OUTPUT_ARG_70FEA8AE = "created model=table object=%s rows=%d output=%s";
  public static final String MESSAGE_COLUMN_ARG_CATEGORY_ARG_DATA_TYPE_ARG_ENCODING_ARG_SOURCE_ARG_COMPRESSION_ARG_SOURCE_ARG_4AA9D567 = "column=%s category=%s data_type=%s encoding=%s source=%s compression=%s source=%s";
  public static final String MESSAGE_ERROR_ARG_10E10A81 = "Error: %s";
  public static final String EXCEPTION_INVALID_CSV_SYNTAX_LINE_ARG_46CAD7A0 = "Invalid CSV syntax (line %d)";
  public static final String EXCEPTION_NAME_ARG_CANNOT_BE_PRESERVED_BY_THE_BUNDLED_TSFILE_WRITER_RESOLVED_AS_ARG_697B06A9 = "Name '%s' cannot be preserved by the bundled TsFile writer (resolved as '%s')";
}
