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

package org.apache.iotdb.tool.common;

import org.apache.iotdb.cli.i18n.CliMessages;

import org.apache.tsfile.enums.TSDataType;

import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class Constants {

  // common
  public static final int CODE_OK = 0;
  public static final int CODE_ERROR = 1;

  public static final String HOST_ARGS = "h";
  public static final String HOST_NAME = "host";
  public static final String HOST_DESC = CliMessages.EXCEPTION_HOST_NAME_OPTIONAL_FD001BC0;
  public static final String HOST_DEFAULT_VALUE = "127.0.0.1";

  public static final String HELP_ARGS = "help";
  public static final String HELP_DESC = CliMessages.EXCEPTION_DISPLAY_HELP_INFORMATION_3BBD4899;

  public static final String PORT_ARGS = "p";
  public static final String PORT_NAME = "port";
  public static final String PORT_DESC = "Port (optional)";
  public static final String PORT_DEFAULT_VALUE = "6667";

  public static final String PW_ARGS = "pw";
  public static final String PW_NAME = "password";
  public static final String PW_DESC = "Password (optional)";
  public static final String PW_DEFAULT_VALUE = "root";

  public static final String USERNAME_ARGS = "u";
  public static final String USERNAME_NAME = "username";
  public static final String USERNAME_DESC = "Username (optional)";
  public static final String USERNAME_DEFAULT_VALUE = "root";

  public static final String USE_SSL_ARGS = "usessl";
  public static final String USE_SSL_NAME = "use_ssl";
  public static final String USE_SSL_DESC =
      CliMessages.EXCEPTION_USE_SSL_STATEMENT_OPTIONAL_92E1AA92;

  public static final String TRUST_STORE_ARGS = "ts";
  public static final String TRUST_STORE_NAME = "trust_store";
  public static final String TRUST_STORE_DESC = CliMessages.EXCEPTION_TRUST_STORE_OPTIONAL_EA106F8E;

  public static final String TRUST_STORE_PWD_ARGS = "tpw";
  public static final String TRUST_STORE_PWD_NAME = "trust_store_password";
  public static final String TRUST_STORE_PWD_DESC =
      CliMessages.EXCEPTION_TRUST_STORE_PASSWORD_OPTIONAL_B017719D;

  public static final String KEY_STORE_ARGS = "ks";
  public static final String KEY_STORE_NAME = "key_store";
  public static final String KEY_STORE_DESC =
      CliMessages.EXCEPTION_KEY_STORE_FOR_MUTUAL_SSL_OPTIONAL_A617339F;

  public static final String KEY_STORE_PWD_ARGS = "kpw";
  public static final String KEY_STORE_PWD_NAME = "key_store_password";
  public static final String KEY_STORE_PWD_DESC =
      CliMessages.EXCEPTION_KEY_STORE_PASSWORD_FOR_MUTUAL_SSL_OPTIONAL_CA3006DD;

  public static final String SSL_PROTOCOL_ARGS = "ssl_protocol";
  public static final String SSL_PROTOCOL_NAME = "ssl_protocol";
  public static final String SSL_PROTOCOL_DESC =
      CliMessages.EXCEPTION_SSL_PROTOCOL_OPTIONAL_F7C84E29;

  public static final String FILE_TYPE_ARGS = "ft";
  public static final String FILE_TYPE_NAME = "file_type";
  public static final String FILE_TYPE_ARGS_NAME = "format";
  public static final String FILE_TYPE_DESC =
      CliMessages.EXCEPTION_FILE_TYPE_YOU_CAN_CHOOSE_TSFILE_CSV_OR_SQL_REQUIRED_046E0304;
  public static final String FILE_TYPE_DESC_EXPORT =
      CliMessages.EXCEPTION_EXPORT_FILE_TYPE_YOU_CAN_CHOOSE_TSFILE_CSV_OR_SQL_REQUIRED_04770009;
  public static final String FILE_TYPE_DESC_IMPORT =
      CliMessages.EXCEPTION_TYPES_OF_IMPORTED_FILES_CSV_SQL_TSFILE_REQUIRED_E2678FF9;

  public static final String TIME_FORMAT_ARGS = "tf";
  public static final String TIME_FORMAT_NAME = "time_format";
  public static final String TIME_FORMAT_DESC =
      CliMessages
          .EXCEPTION_OUTPUT_TIME_FORMAT_IN_CSV_FILE_YOU_CAN_CHOOSE_1_TIMESTAMP_NUMBER_LONG_2_ISO8601_DEFAULT_3_USER_DEFINED_PATTERN_LIKE_YYYY_MM_DD_HH_MM_SS_DEFAULT_ISO8601_OUTPUT_TIMESTAMP_IN_SQL_FILE_NO_MATTER_WHAT_TIME_FORMAT_IS_SET_OPTIONAL_D766AEFB;

  public static final String TIME_ZONE_ARGS = "tz";
  public static final String TIME_ZONE_NAME = "timezone";
  public static final String TIME_ZONE_DESC =
      CliMessages.EXCEPTION_TIME_ZONE_EG_08_00_OR_01_00_OPTIONAL_7BEA75F1;

  public static final String TIMEOUT_ARGS = "timeout";
  public static final String TIMEOUT_NAME = "query_timeout";
  public static final String TIMEOUT_DESC =
      CliMessages.EXCEPTION_TIMEOUT_FOR_SESSION_QUERY_OPTIONAL_69034017;

  public static final String ALIGNED_ARGS = "aligned";
  public static final String ALIGNED_NAME = "use_aligned";
  public static final String ALIGNED_ARGS_NAME_EXPORT =
      CliMessages.EXCEPTION_EXPORT_ALIGNED_INSERT_SQL_EA4543D3;
  public static final String ALIGNED_ARGS_NAME_IMPORT =
      CliMessages.EXCEPTION_USE_THE_ALIGNED_INTERFACE_F915187B;
  public static final String ALIGNED_EXPORT_DESC =
      CliMessages.EXCEPTION_WHETHER_EXPORT_TO_SQL_OF_ALIGNED_OPTIONAL_CF4C6EA0;
  public static final String ALIGNED_IMPORT_DESC =
      CliMessages.EXCEPTION_WHETHER_TO_USE_THE_INTERFACE_OF_ALIGNED_OPTIONAL_440B01C4;

  public static final String SQL_DIALECT_ARGS = "sql_dialect";
  public static final String SQL_DIALECT_DESC =
      CliMessages.EXCEPTION_CURRENTLY_SUPPORTS_TREE_AND_TABLE_MODEL_DEFAULT_TREE_OPTIONAL_FB4F4486;
  public static final String SQL_DIALECT_VALUE_TREE = "tree";
  public static final String SQL_DIALECT_VALUE_TABLE = "table";

  public static final String DB_ARGS = "db";
  public static final String DB_NAME = "database";
  public static final String DB_DESC =
      CliMessages
          .EXCEPTION_THE_DATABASE_TO_BE_EXPORTED_ONLY_TAKES_EFFECT_WHEN_SQL_DIALECT_IS_TABLE_AND_REQUIRED_WHEN_FILE_TYPE_IS_CSV_AND_TSFILE_OPTIONAL_DCB4863E;

  public static final String TABLE_ARGS = "table";
  public static final String TABLE_DESC =
      CliMessages
          .EXCEPTION_THE_TABLE_TO_BE_EXPORTED_ONLY_TAKES_EFFECT_WHEN_SQL_DIALECT_IS_TABLE_OPTIONAL_718386C9;
  public static final String TABLE_DESC_EXPORT =
      String.format(
          CliMessages
              .EXCEPTION_ARG_IF_THE_Q_PARAMETER_IS_SPECIFIED_THIS_PARAMETER_DOES_NOT_TAKE_EFFECT_IF_THE_EXPORT_TYPE_IS_TSFILE_OR_SQL_THIS_PARAMETER_IS_REQUIRED_OPTIONAL_8F65937E,
          Objects.toString(TABLE_DESC));
  public static final String TABLE_DESC_IMPORT =
      String.format(
          CliMessages.EXCEPTION_ARG_AND_FILE_TYPE_IS_CSV_OPTIONAL_695FD8E8,
          Objects.toString(TABLE_DESC));

  public static final String DATATYPE_BOOLEAN = "boolean";
  public static final String DATATYPE_INT = "int";
  public static final String DATATYPE_LONG = "long";
  public static final String DATATYPE_FLOAT = "float";
  public static final String DATATYPE_DOUBLE = "double";
  public static final String DATATYPE_TIMESTAMP = "timestamp";
  public static final String DATATYPE_DATE = "date";
  public static final String DATATYPE_BLOB = "blob";
  public static final String DATATYPE_NAN = "NaN";
  public static final String DATATYPE_TEXT = "text";
  public static final String DATATYPE_STRING = "string";
  public static final String DATATYPE_NULL = "null";
  public static final Map<String, TSDataType> TYPE_INFER_KEY_DICT = new HashMap<>();

  static {
    TYPE_INFER_KEY_DICT.put(DATATYPE_BOOLEAN, TSDataType.BOOLEAN);
    TYPE_INFER_KEY_DICT.put(DATATYPE_INT, TSDataType.FLOAT);
    TYPE_INFER_KEY_DICT.put(DATATYPE_LONG, TSDataType.DOUBLE);
    TYPE_INFER_KEY_DICT.put(DATATYPE_FLOAT, TSDataType.FLOAT);
    TYPE_INFER_KEY_DICT.put(DATATYPE_DOUBLE, TSDataType.DOUBLE);
    TYPE_INFER_KEY_DICT.put(DATATYPE_TIMESTAMP, TSDataType.TIMESTAMP);
    TYPE_INFER_KEY_DICT.put(DATATYPE_DATE, TSDataType.TIMESTAMP);
    TYPE_INFER_KEY_DICT.put(DATATYPE_BLOB, TSDataType.BLOB);
    TYPE_INFER_KEY_DICT.put(DATATYPE_NAN, TSDataType.DOUBLE);
    TYPE_INFER_KEY_DICT.put(DATATYPE_STRING, TSDataType.STRING);
  }

  public static final Map<String, TSDataType> TYPE_INFER_VALUE_DICT = new HashMap<>();

  static {
    TYPE_INFER_VALUE_DICT.put(DATATYPE_BOOLEAN, TSDataType.BOOLEAN);
    TYPE_INFER_VALUE_DICT.put(DATATYPE_INT, TSDataType.INT32);
    TYPE_INFER_VALUE_DICT.put(DATATYPE_LONG, TSDataType.INT64);
    TYPE_INFER_VALUE_DICT.put(DATATYPE_FLOAT, TSDataType.FLOAT);
    TYPE_INFER_VALUE_DICT.put(DATATYPE_DOUBLE, TSDataType.DOUBLE);
    TYPE_INFER_VALUE_DICT.put(DATATYPE_TIMESTAMP, TSDataType.TIMESTAMP);
    TYPE_INFER_VALUE_DICT.put(DATATYPE_DATE, TSDataType.DATE);
    TYPE_INFER_VALUE_DICT.put(DATATYPE_BLOB, TSDataType.BLOB);
    TYPE_INFER_VALUE_DICT.put(DATATYPE_TEXT, TSDataType.TEXT);
    TYPE_INFER_VALUE_DICT.put(DATATYPE_STRING, TSDataType.STRING);
  }

  public static final int MAX_HELP_CONSOLE_WIDTH = 92;

  public static final String CSV_SUFFIXS = "csv";
  public static final String TXT_SUFFIXS = "txt";
  public static final String SQL_SUFFIXS = "sql";
  public static final String TSFILE_SUFFIXS = "tsfile";

  public static final String TSFILEDB_CLI_DIVIDE = "-------------------";
  public static final String COLON = ": ";
  public static final String MINUS = "-";

  public static final List<String> HEAD_COLUMNS =
      Arrays.asList("Timeseries", "Alias", "DataType", "Encoding", "Compression");

  // export constants
  public static final String EXPORT_CLI_PREFIX = CliMessages.EXCEPTION_EXPORT_DATA_2F1FB3D9;
  public static final String EXPORT_SCHEMA_CLI_PREFIX = "ExportSchema";

  public static final String EXPORT_CLI_HEAD =
      CliMessages
          .EXCEPTION_PLEASE_OBTAIN_HELP_INFORMATION_FOR_THE_CORRESPONDING_DATA_TYPE_BASED_ON_DIFFERENT_PARAMETERS_FOR_EXAMPLE_EXPORT_DATA_SH_HELP_TSFILE_EXPORT_DATA_SH_HELP_SQL_EXPORT_DATA_SH_HELP_CSV_0D410E8A;

  public static final String SCHEMA_CLI_CHECK_IN_HEAD =
      CliMessages.EXCEPTION_TOO_FEW_PARAMS_INPUT_PLEASE_CHECK_THE_FOLLOWING_HINT_E6D41E98;
  public static final String START_TIME_ARGS = "start_time";
  public static final String START_TIME_DESC =
      CliMessages.EXCEPTION_THE_START_TIME_TO_BE_EXPORTED_OPTIONAL_8133559A;

  public static final String END_TIME_ARGS = "end_time";
  public static final String END_TIME_DESC =
      CliMessages.EXCEPTION_THE_END_TIME_TO_BE_EXPORTED_OPTIONAL_355E448D;

  public static final String TARGET_DIR_ARGS = "t";
  public static final String TARGET_DIR_NAME = "target";
  public static final String TARGET_DIR_ARGS_NAME = "target_directory";
  public static final String TARGET_DIR_DESC =
      CliMessages.EXCEPTION_TARGET_FILE_DIRECTORY_REQUIRED_9A407F06;
  public static final String TARGET_DIR_SUBSCRIPTION_DESC =
      CliMessages.EXCEPTION_TARGET_FILE_DIRECTORY_DEFAULT_TARGET_OPTIONAL_52390574;

  public static final String TARGET_PATH_ARGS = "path";
  public static final String TARGET_PATH_ARGS_NAME = "path_pattern";
  public static final String TARGET_PATH_NAME = "exportPathPattern";
  public static final String TARGET_PATH_DESC =
      CliMessages.EXCEPTION_EXPORT_PATH_PATTERN_OPTIONAL_8E9C0DE8;

  public static final String QUERY_COMMAND_ARGS = "q";
  public static final String QUERY_COMMAND_NAME = "query";
  public static final String QUERY_COMMAND_ARGS_NAME = "query_command";
  public static final String QUERY_COMMAND_DESC =
      CliMessages
          .MESSAGE_QUERY_TO_EXPORT_OPTIONAL_TABLE_SQL_TSFILE_STORED_COLUMNS_OF_ONE_TABLE_6D79406B;

  public static final String TARGET_FILE_ARGS = "pfn";
  public static final String TARGET_FILE_NAME = "prefix_file_name";
  public static final String TARGET_FILE_DESC =
      CliMessages.EXCEPTION_EXPORT_FILE_NAME_OPTIONAL_BBFDC3AE;

  public static final String MEASUREMENT_FILE_ARGS = "mf";
  public static final String MEASUREMENT_FILE_NAME = "measurement_file";
  public static final String MEASUREMENT_FILE_DESC =
      CliMessages
          .MESSAGE_UTF_8_IOTDB_PATH_PATTERN_FILE_FOR_TREE_EXPORT_MUTUALLY_EXCLUSIVE_WITH_Q_DD1F2CDA;
  public static final String EXPORT_DATA_LOG_ARGS = "l";
  public static final String EXPORT_DATA_LOG_NAME = "export_data_log";
  public static final String EXPORT_DATA_LOG_DESC =
      CliMessages
          .MESSAGE_EXPORT_LOG_FILE_DEFAULT_TARGET_LOG_EXPORT_DATA_YYYYMMDDHHMMSS_LOG_B29FC35F;
  public static final String EXPORT_START_TIME_DESC =
      CliMessages
          .MESSAGE_INCLUSIVE_START_TIME_FOR_MEASUREMENT_FILE_OR_AUTOMATIC_TABLE_EXPORT_69FE75CF;
  public static final String EXPORT_END_TIME_DESC =
      CliMessages
          .MESSAGE_INCLUSIVE_END_TIME_FOR_MEASUREMENT_FILE_OR_AUTOMATIC_TABLE_EXPORT_D9C24D95;
  public static final String EXPORT_DB_DESC =
      CliMessages
          .MESSAGE_DATABASE_TO_EXPORT_REQUIRED_WITHOUT_TABLE_OR_Q_EXPORTS_ALL_ITS_TABLES_704F6B55;
  public static final String EXPORT_TABLE_DESC =
      CliMessages.MESSAGE_TABLE_TO_EXPORT_OPTIONAL_IGNORED_WHEN_Q_IS_GIVEN_4F37C0B5;
  public static final String EXPORT_LINES_PER_FILE_DESC =
      CliMessages.MESSAGE_MAXIMUM_DATA_ROWS_PER_CSV_OR_SQL_FILE_DEFAULT_10000_000CEFB9;

  public static final String RPC_MAX_FRAME_SIZE_ARGS = "mfs";
  public static final String RPC_MAX_FRAME_SIZE_NAME = "rpc_max_frame_size";
  public static final String RPC_MAX_FRAME_SIZE_DESC =
      CliMessages.EXCEPTION_THE_MAX_FRAME_SIZE_OF_RPC_DEFAULT_IS_536870912_BYTES_OPTIONAL_DDC009D1;

  public static final String DATA_TYPE_ARGS = "dt";
  public static final String DATA_TYPE_NAME = "datatype";
  public static final String DATA_TYPE_DESC =
      String.format(
          CliMessages
              .EXCEPTION_WILL_THE_DATA_TYPE_OF_TIMESERIES_BE_PRINTED_IN_THE_HEAD_LINE_OF_THE_CSV_FILE_ARG_YOU_CAN_CHOOSE_TRUE_OR_FALSE_OPTIONAL_D1409A80,
          Objects.toString('\n'));

  public static final String LINES_PER_FILE_ARGS = "lpf";
  public static final String LINES_PER_FILE_NAME = "lines_per_file";
  public static final String LINES_PER_FILE_DESC =
      CliMessages.EXCEPTION_LINES_PER_DUMP_FILE_ONLY_EFFECTIVE_IN_TREE_MODEL_OPTIONAL_8138FC83;

  public static final String DUMP_FILE_NAME_DEFAULT = "dump";

  public static final String queryTableParamRequired =
      CliMessages.EXCEPTION_EITHER_Q_OR_TABLE_IS_REQUIRED_WHEN_SQL_DIALECT_IS_TABLE_378EE68E;
  public static final String INSERT_CSV_MEET_ERROR_MSG =
      CliMessages.EXCEPTION_MEET_ERROR_WHEN_INSERT_CSV_BECAUSE_F4B95972;
  public static final String INSERT_SQL_MEET_ERROR_MSG =
      CliMessages.EXCEPTION_MEET_ERROR_WHEN_INSERT_SQL_BECAUSE_54810B8F;
  public static final String COLUMN_SQL_MEET_ERROR_MSG =
      CliMessages.EXCEPTION_MEET_ERROR_WHEN_GET_TABLE_COLUMNS_INFORMATION_BECAUSE_3E210150;
  public static final String TARGET_DATABASE_NOT_EXIST_MSG =
      CliMessages.EXCEPTION_THE_TARGET_DATABASE_ARG_DOES_NOT_EXIST_342912D0;
  public static final String TARGET_TABLE_NOT_EXIST_MSG =
      CliMessages.EXCEPTION_THERE_ARE_NO_TABLES_OR_THE_TARGET_TABLE_ARG_DOES_NOT_EXIST_23BCB7AF;
  public static final String TARGET_TABLE_EMPTY_MSG =
      CliMessages
          .EXCEPTION_THERE_ARE_NO_TABLES_TO_EXPORT_PLEASE_CHECK_IF_THE_TABLES_IN_THE_TARGET_DATABASE_EXIST_AND_IF_YOU_HAVE_PERMISSION_TO_ACCESS_THEM_1592463E;

  public static final String[] TIME_FORMAT =
      new String[] {"default", "long", "number", "timestamp"};

  public static final String PATH_ARGS = "path";
  public static final String PATH_DESC =
      CliMessages
          .EXCEPTION_THE_PATH_TO_BE_EXPORTED_ONLY_TAKES_EFFECT_WHEN_SQL_DIALECT_IS_TREE_OPTIONAL_99A39D5E;

  public static final long memoryThreshold = 10 * 1024 * 1024;

  public static final String[] STRING_TIME_FORMAT =
      new String[] {
        "yyyy-MM-dd HH:mm:ss.SSSX",
        "yyyy/MM/dd HH:mm:ss.SSSX",
        "yyyy.MM.dd HH:mm:ss.SSSX",
        "yyyy-MM-dd HH:mm:ssX",
        "yyyy/MM/dd HH:mm:ssX",
        "yyyy.MM.dd HH:mm:ssX",
        "yyyy-MM-dd HH:mm:ss.SSSz",
        "yyyy/MM/dd HH:mm:ss.SSSz",
        "yyyy.MM.dd HH:mm:ss.SSSz",
        "yyyy-MM-dd HH:mm:ssz",
        "yyyy/MM/dd HH:mm:ssz",
        "yyyy.MM.dd HH:mm:ssz",
        "yyyy-MM-dd HH:mm:ss.SSS",
        "yyyy/MM/dd HH:mm:ss.SSS",
        "yyyy.MM.dd HH:mm:ss.SSS",
        "yyyy-MM-dd HH:mm:ss",
        "yyyy/MM/dd HH:mm:ss",
        "yyyy.MM.dd HH:mm:ss",
        "yyyy-MM-dd'T'HH:mm:ss.SSSX",
        "yyyy/MM/dd'T'HH:mm:ss.SSSX",
        "yyyy.MM.dd'T'HH:mm:ss.SSSX",
        "yyyy-MM-dd'T'HH:mm:ssX",
        "yyyy/MM/dd'T'HH:mm:ssX",
        "yyyy.MM.dd'T'HH:mm:ssX",
        "yyyy-MM-dd'T'HH:mm:ss.SSSz",
        "yyyy/MM/dd'T'HH:mm:ss.SSSz",
        "yyyy.MM.dd'T'HH:mm:ss.SSSz",
        "yyyy-MM-dd'T'HH:mm:ssz",
        "yyyy/MM/dd'T'HH:mm:ssz",
        "yyyy.MM.dd'T'HH:mm:ssz",
        "yyyy-MM-dd'T'HH:mm:ss.SSS",
        "yyyy/MM/dd'T'HH:mm:ss.SSS",
        "yyyy.MM.dd'T'HH:mm:ss.SSS",
        "yyyy-MM-dd'T'HH:mm:ss",
        "yyyy/MM/dd'T'HH:mm:ss",
        "yyyy.MM.dd'T'HH:mm:ss"
      };

  public static final String SUBSCRIPTION_CLI_PREFIX = CliMessages.EXCEPTION_EXPORT_TSFILE_201F2F41;
  public static final int MAX_RETRY_TIMES = 2;
  public static final String LOOSE_RANGE = "";
  public static final boolean STRICT = false;
  public static final String MODE = "snapshot";
  public static final boolean AUTO_COMMIT = false;
  public static final String TABLE_MODEL = "table";
  public static final long POLL_MESSAGE_TIMEOUT = 10000;
  public static final String TOPIC_NAME_PREFIX = "topic_";
  public static final String GROUP_NAME_PREFIX = "group_";
  public static final String HANDLER = "TsFileHandler";
  public static final String CONSUMER_NAME_PREFIX = "consumer_";
  public static final SimpleDateFormat DATE_FORMAT_VIEW = new SimpleDateFormat("yyyyMMddHHmmssSSS");
  public static final String BASE_VIEW_TYPE = "BASE";
  public static final String HEADER_VIEW_TYPE = "ViewType";
  public static final String HEADER_TIMESERIES = "Timeseries";
  public static final String EXPORT_COMPLETELY = CliMessages.LOG_EXPORT_COMPLETELY_34200D19;
  public static final String EXPORT_SCHEMA_TABLES_SELECT =
      "select * from information_schema.tables where database = '%s'";
  public static final String EXPORT_SCHEMA_TABLES_SHOW = "show tables details from %s";
  public static final String EXPORT_SCHEMA_TABLES_SHOW_DATABASES = "show databases";
  public static final String EXPORT_SCHEMA_COLUMNS_SELECT =
      "select * from information_schema.columns where database like '%s' and table_name like '%s'";
  public static final String EXPORT_SCHEMA_COLUMNS_DESC = "desc %s.%s details";
  public static final String SHOW_CREATE_TABLE = "SHOW CREATE TABLE %s.%s";
  public static final String DROP_TABLE_IF_EXIST = "DROP TABLE IF EXISTS %s";
  public static final String PROCESSED_PROGRESS = "\rProcessed %d rows";

  // import constants
  public static final String IMPORT_SCHEMA_CLI_PREFIX = "ImportSchema";
  public static final String IMPORT_CLI_PREFIX = CliMessages.EXCEPTION_IMPORT_DATA_446C4D18;

  public static final String IMPORT_CLI_HEAD =
      CliMessages
          .EXCEPTION_PLEASE_OBTAIN_HELP_INFORMATION_FOR_THE_CORRESPONDING_DATA_TYPE_BASED_ON_DIFFERENT_PARAMETERS_FOR_EXAMPLE_IMPORT_DATA_SH_HELP_TSFILE_IMPORT_DATA_SH_HELP_SQL_IMPORT_DATA_SH_HELP_CSV_F9320898;

  public static final String FILE_ARGS = "s";
  public static final String FILE_NAME = "source";
  public static final String FILE_DESC =
      CliMessages
          .EXCEPTION_THE_LOCAL_DIRECTORY_PATH_OF_THE_SCRIPT_FILE_FOLDER_TO_BE_LOADED_REQUIRED_AC5278C5;

  public static final String FAILED_FILE_ARGS = "fd";
  public static final String FAILED_FILE_NAME = "fail_dir";
  public static final String FAILED_FILE_ARGS_NAME = "failDir";
  public static final String FAILED_FILE_DESC =
      CliMessages
          .EXCEPTION_SPECIFYING_A_DIRECTORY_TO_SAVE_FAILED_FILE_DEFAULT_YOUR_CSV_FILE_PATH_OPTIONAL_DADE5B55;

  public static final String ON_SUCCESS_ARGS = "os";
  public static final String ON_SUCCESS_NAME = "on_success";
  public static final String ON_SUCCESS_DESC =
      CliMessages
          .EXCEPTION_WHEN_LOADING_TSFILE_SUCCESSFULLY_DO_OPERATION_ON_TSFILE_AND_ITS_RESOURCE_AND_MODS_FILES_OPTIONAL_PARAMETERS_ARE_NONE_MV_CP_DELETE_REQUIRED_601E75DC;

  public static final String SUCCESS_DIR_ARGS = "sd";
  public static final String SUCCESS_DIR_NAME = "success_dir";
  public static final String SUCCESS_DIR_DESC =
      CliMessages.EXCEPTION_THE_TARGET_FOLDER_WHEN_OS_IS_MV_OR_CP_OPTIONAL_8AFE2264;

  public static final String FAIL_DIR_ARGS = "fd";
  public static final String FAIL_DIR_NAME = "fail_dir";
  public static final String FAIL_DIR_CSV_DESC =
      CliMessages
          .EXCEPTION_SPECIFYING_A_DIRECTORY_TO_SAVE_FAILED_FILE_DEFAULT_YOUR_CSV_FILE_PATH_OPTIONAL_EA6E7DF7;
  public static final String FAIL_DIR_SQL_DESC =
      CliMessages
          .EXCEPTION_SPECIFYING_A_DIRECTORY_TO_SAVE_FAILED_FILE_DEFAULT_YOUR_SQL_FILE_PATH_OPTIONAL_8CF7BBC4;
  public static final String FAIL_DIR_TSFILE_DESC =
      CliMessages.EXCEPTION_THE_TARGET_FOLDER_WHEN_OF_IS_MV_OR_CP_OPTIONAL_E72A0E75;

  public static final String ON_FAIL_ARGS = "of";
  public static final String ON_FAIL_NAME = "on_fail";
  public static final String ON_FAIL_DESC =
      CliMessages
          .EXCEPTION_WHEN_LOADING_TSFILE_FAIL_DO_OPERATION_ON_TSFILE_AND_ITS_RESOURCE_AND_MODS_FILES_OPTIONAL_PARAMETERS_ARE_NONE_MV_CP_DELETE_REQUIRED_7BA55A84;

  public static final String THREAD_NUM_ARGS = "tn";
  public static final String THREAD_NUM_NAME = "thread_num";
  public static final String THREAD_NUM_DESC =
      CliMessages
          .EXCEPTION_THE_NUMBER_OF_THREADS_USED_TO_IMPORT_TSFILE_DEFAULT_IS_8_OPTIONAL_EAAFD75C;

  public static final String BATCH_POINT_SIZE_ARGS = "batch";
  public static final String BATCH_POINT_SIZE_NAME = "batch_size";
  public static final String BATCH_POINT_SIZE_ARGS_NAME = "batch_size";
  public static final String BATCH_POINT_SIZE_DESC = "100000 (optional)";
  public static final String BATCH_POINT_SIZE_LIMIT_DESC =
      CliMessages.EXCEPTION_10000_ONLY_NOT_ALIGNED_AND_SQL_DIALECT_TREE_OPTIONAL_75B83039;

  public static final String TIMESTAMP_PRECISION_ARGS = "tp";
  public static final String TIMESTAMP_PRECISION_NAME = "timestamp_precision";
  public static final String TIMESTAMP_PRECISION_ARGS_NAME =
      CliMessages.EXCEPTION_TIMESTAMP_PRECISION_MS_US_NS_5DC07992;
  public static final String TIMESTAMP_PRECISION_DESC =
      CliMessages.EXCEPTION_TIMESTAMP_PRECISION_MS_US_NS_OPTIONAL_10C6809A;

  public static final String TYPE_INFER_ARGS = "ti";
  public static final String TYPE_INFER_NAME = "type_infer";
  public static final String TYPE_INFER_DESC =
      CliMessages.EXCEPTION_DEFINE_TYPE_INFO_BY_OPTION_BOOLEAN_TEXT_INT_LONG_OPTIONAL_CF9F51B3;

  public static final String LINES_PER_FAILED_FILE_ARGS = "lpf";
  public static final String LINES_PER_FAILED_FILE_ARGS_NAME = "lines_per_failed_file";
  public static final String LINES_PER_FAILED_FILE_DESC =
      CliMessages
          .EXCEPTION_LINES_PER_FAILED_FILE_ONLY_TAKES_EFFECT_AND_REQUIRED_WHEN_SQL_DIALECT_IS_TABLE_OPTION_55D8B932;
  public static final String IMPORT_COMPLETELY = CliMessages.LOG_IMPORT_COMPLETELY_B7451B2C;
  public static final int BATCH_POINT_SIZE = 10000;

  public static final String IMPORT_INIT_MEET_ERROR_MSG =
      CliMessages.EXCEPTION_MEET_ERROR_WHEN_INIT_IMPORT_BECAUSE_A38F65C6;
  public static final String REQUIRED_ARGS_ERROR_MSG =
      CliMessages.EXCEPTION_INVALID_ARGS_REQUIRED_VALUES_FOR_OPTION_ARG_NOT_PROVIDED_601B17E3;
}
