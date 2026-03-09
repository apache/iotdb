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

import org.apache.tsfile.enums.TSDataType;

import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Constants {

  // common
  public static final int CODE_OK = 0;
  public static final int CODE_ERROR = 1;

  public static final String HOST_ARGS = "h";
  public static final String HOST_NAME = "host";
  public static final String HOST_DESC = "Host Name (optional)";
  public static final String HOST_DEFAULT_VALUE = "127.0.0.1";

  public static final String HELP_ARGS = "help";
  public static final String HELP_DESC = "Display help information";

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
  public static final String USE_SSL_DESC = "Use SSL statement. (optional)";

  public static final String TRUST_STORE_ARGS = "ts";
  public static final String TRUST_STORE_NAME = "trust_store";
  public static final String TRUST_STORE_DESC = "Trust store. (optional)";

  public static final String TRUST_STORE_PWD_ARGS = "tpw";
  public static final String TRUST_STORE_PWD_NAME = "trust_store_password";
  public static final String TRUST_STORE_PWD_DESC = "Trust store password. (optional)";

  public static final String FILE_TYPE_ARGS = "ft";
  public static final String FILE_TYPE_NAME = "file_type";
  public static final String FILE_TYPE_ARGS_NAME = "format";
  public static final String FILE_TYPE_DESC =
      "File type?You can choose tsfile)、csv) or sql).(required)";
  public static final String FILE_TYPE_DESC_EXPORT =
      "Export file type ?You can choose tsfile)、csv) or sql).(required)";
  public static final String FILE_TYPE_DESC_IMPORT =
      "Types of imported files: csv, sql, tsfile.(required)";

  public static final String TIME_FORMAT_ARGS = "tf";
  public static final String TIME_FORMAT_NAME = "time_format";
  public static final String TIME_FORMAT_DESC =
      "Output time Format in csv file. "
          + "You can choose 1) timestamp, number, long 2) ISO8601, default 3) "
          + "user-defined pattern like yyyy-MM-dd HH:mm:ss, default ISO8601.\n OutPut timestamp in sql file, No matter what time format is set(optional)";

  public static final String TIME_ZONE_ARGS = "tz";
  public static final String TIME_ZONE_NAME = "timezone";
  public static final String TIME_ZONE_DESC = "Time Zone eg. +08:00 or -01:00 .(optional)";

  public static final String TIMEOUT_ARGS = "timeout";
  public static final String TIMEOUT_NAME = "query_timeout";
  public static final String TIMEOUT_DESC = "Timeout for session query.(optional)";

  public static final String ALIGNED_ARGS = "aligned";
  public static final String ALIGNED_NAME = "use_aligned";
  public static final String ALIGNED_ARGS_NAME_EXPORT = "export aligned insert sql";
  public static final String ALIGNED_ARGS_NAME_IMPORT = "use the aligned interface";
  public static final String ALIGNED_EXPORT_DESC = "Whether export to sql of aligned.(optional)";
  public static final String ALIGNED_IMPORT_DESC =
      "Whether to use the interface of aligned.(optional)";

  public static final String SQL_DIALECT_ARGS = "sql_dialect";
  public static final String SQL_DIALECT_DESC =
      "Currently supports tree and table model, default tree. (optional)";
  public static final String SQL_DIALECT_VALUE_TREE = "tree";
  public static final String SQL_DIALECT_VALUE_TABLE = "table";

  public static final String DB_ARGS = "db";
  public static final String DB_NAME = "database";
  public static final String DB_DESC =
      "The database to be exported,only takes effect when sql_dialect is table and required when file_type is csv and tsfile.(optional)";

  public static final String TABLE_ARGS = "table";
  public static final String TABLE_DESC =
      "The table to be exported,only takes effect when sql_dialect is table.(optional)";
  public static final String TABLE_DESC_EXPORT =
      TABLE_DESC
          + ".If the '-q' parameter is specified, this parameter does not take effect. If the export type is tsfile or sql, this parameter is required. (optional)";
  public static final String TABLE_DESC_IMPORT = TABLE_DESC + " and file_type is csv. (optional)";

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
  public static final String EXPORT_CLI_PREFIX = "Export Data";
  public static final String EXPORT_SCHEMA_CLI_PREFIX = "ExportSchema";

  public static final String EXPORT_CLI_HEAD =
      "Please obtain help information for the corresponding data type based on different parameters, for example:\n"
          + "./export_data.sh -help tsfile\n"
          + "./export_data.sh -help sql\n"
          + "./export_data.sh -help csv";

  public static final String SCHEMA_CLI_CHECK_IN_HEAD =
      "Too few params input, please check the following hint.";
  public static final String START_TIME_ARGS = "start_time";
  public static final String START_TIME_DESC = "The start time to be exported (optional)";

  public static final String END_TIME_ARGS = "end_time";
  public static final String END_TIME_DESC = "The end time to be exported. (optional)";

  public static final String TARGET_DIR_ARGS = "t";
  public static final String TARGET_DIR_NAME = "target";
  public static final String TARGET_DIR_ARGS_NAME = "target_directory";
  public static final String TARGET_DIR_DESC = "Target file directory (required)";
  public static final String TARGET_DIR_SUBSCRIPTION_DESC =
      "Target file directory.default ./target (optional)";

  public static final String TARGET_PATH_ARGS = "path";
  public static final String TARGET_PATH_ARGS_NAME = "path_pattern";
  public static final String TARGET_PATH_NAME = "exportPathPattern";
  public static final String TARGET_PATH_DESC = "Export Path Pattern (optional)";

  public static final String QUERY_COMMAND_ARGS = "q";
  public static final String QUERY_COMMAND_NAME = "query";
  public static final String QUERY_COMMAND_ARGS_NAME = "query_command";
  public static final String QUERY_COMMAND_DESC =
      "The query command that you want to execute.If sql_dialect is table The 'q' parameter is only applicable to export types of CSV, and is not available for other types.If the '- q' parameter is not empty, then the parameters' creatTime ',' EndTime 'and' table 'are not effective.(optional)";

  public static final String TARGET_FILE_ARGS = "pfn";
  public static final String TARGET_FILE_NAME = "prefix_file_name";
  public static final String TARGET_FILE_DESC = "Export file name .(optional)";

  public static final String RPC_MAX_FRAME_SIZE_ARGS = "mfs";
  public static final String RPC_MAX_FRAME_SIZE_NAME = "rpc_max_frame_size";
  public static final String RPC_MAX_FRAME_SIZE_DESC =
      "The max frame size of RPC, default is 536870912 bytes.(optional)";

  public static final String DATA_TYPE_ARGS = "dt";
  public static final String DATA_TYPE_NAME = "datatype";
  public static final String DATA_TYPE_DESC =
      "Will the data type of timeseries be printed in the head line of the CSV file?"
          + '\n'
          + "You can choose true) or false) . (optional)";

  public static final String LINES_PER_FILE_ARGS = "lpf";
  public static final String LINES_PER_FILE_NAME = "lines_per_file";
  public static final String LINES_PER_FILE_DESC =
      "Lines per dump file,only effective in tree model.(optional)";

  public static final String DUMP_FILE_NAME_DEFAULT = "dump";

  public static final String queryTableParamRequired =
      "Either '-q' or '-table' is required when 'sql-dialect' is' table '";
  public static final String INSERT_CSV_MEET_ERROR_MSG = "Meet error when insert csv because ";
  public static final String INSERT_SQL_MEET_ERROR_MSG = "Meet error when insert sql because ";
  public static final String COLUMN_SQL_MEET_ERROR_MSG =
      "Meet error when get table columns information because ";
  public static final String TARGET_DATABASE_NOT_EXIST_MSG =
      "The target database %s does not exist";
  public static final String TARGET_TABLE_NOT_EXIST_MSG =
      "There are no tables or the target table %s does not exist";
  public static final String TARGET_TABLE_EMPTY_MSG =
      "There are no tables to export. Please check if the tables in the target database exist and if you have permission to access them.";

  public static final String[] TIME_FORMAT =
      new String[] {"default", "long", "number", "timestamp"};

  public static final String PATH_ARGS = "path";
  public static final String PATH_DESC =
      "The path to be exported,only takes effect when sql_dialect is tree.(optional)";

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

  public static final String SUBSCRIPTION_CLI_PREFIX = "Export TsFile";
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
  public static final String EXPORT_COMPLETELY = "Export completely!";
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
  public static final String IMPORT_CLI_PREFIX = "Import Data";

  public static final String IMPORT_CLI_HEAD =
      "Please obtain help information for the corresponding data type based on different parameters, for example:\n"
          + "./import_data.sh -help tsfile\n"
          + "./import_data.sh -help sql\n"
          + "./import_data.sh -help csv";

  public static final String FILE_ARGS = "s";
  public static final String FILE_NAME = "source";
  public static final String FILE_DESC =
      "The local directory path of the script file (folder) to be loaded. (required)";

  public static final String FAILED_FILE_ARGS = "fd";
  public static final String FAILED_FILE_NAME = "fail_dir";
  public static final String FAILED_FILE_ARGS_NAME = "failDir";
  public static final String FAILED_FILE_DESC =
      "Specifying a directory to save failed file, default YOUR_CSV_FILE_PATH (optional)";

  public static final String ON_SUCCESS_ARGS = "os";
  public static final String ON_SUCCESS_NAME = "on_success";
  public static final String ON_SUCCESS_DESC =
      "When loading tsfile successfully, do operation on tsfile (and its .resource and .mods files), "
          + "optional parameters are none, mv, cp, delete. (required)";

  public static final String SUCCESS_DIR_ARGS = "sd";
  public static final String SUCCESS_DIR_NAME = "success_dir";
  public static final String SUCCESS_DIR_DESC =
      "The target folder when 'os' is 'mv' or 'cp'.(optional)";

  public static final String FAIL_DIR_ARGS = "fd";
  public static final String FAIL_DIR_NAME = "fail_dir";
  public static final String FAIL_DIR_CSV_DESC =
      "Specifying a directory to save failed file, default YOUR_CSV_FILE_PATH.(optional)";
  public static final String FAIL_DIR_SQL_DESC =
      "Specifying a directory to save failed file, default YOUR_SQL_FILE_PATH.(optional)";
  public static final String FAIL_DIR_TSFILE_DESC =
      "The target folder when 'of' is 'mv' or 'cp'.(optional)";

  public static final String ON_FAIL_ARGS = "of";
  public static final String ON_FAIL_NAME = "on_fail";
  public static final String ON_FAIL_DESC =
      "When loading tsfile fail, do operation on tsfile (and its .resource and .mods files), "
          + "optional parameters are none, mv, cp, delete. (required)";

  public static final String THREAD_NUM_ARGS = "tn";
  public static final String THREAD_NUM_NAME = "thread_num";
  public static final String THREAD_NUM_DESC =
      "The number of threads used to import tsfile, default is 8.(optional)";

  public static final String BATCH_POINT_SIZE_ARGS = "batch";
  public static final String BATCH_POINT_SIZE_NAME = "batch_size";
  public static final String BATCH_POINT_SIZE_ARGS_NAME = "batch_size";
  public static final String BATCH_POINT_SIZE_DESC = "100000 (optional)";
  public static final String BATCH_POINT_SIZE_LIMIT_DESC =
      "10000 (only not aligned and sql_dialect tree optional)";

  public static final String TIMESTAMP_PRECISION_ARGS = "tp";
  public static final String TIMESTAMP_PRECISION_NAME = "timestamp_precision";
  public static final String TIMESTAMP_PRECISION_ARGS_NAME = "timestamp precision (ms/us/ns)";
  public static final String TIMESTAMP_PRECISION_DESC = "Timestamp precision (ms/us/ns).(optional)";

  public static final String TYPE_INFER_ARGS = "ti";
  public static final String TYPE_INFER_NAME = "type_infer";
  public static final String TYPE_INFER_DESC =
      "Define type info by option:\"boolean=text,int=long, ... (optional)";

  public static final String LINES_PER_FAILED_FILE_ARGS = "lpf";
  public static final String LINES_PER_FAILED_FILE_ARGS_NAME = "lines_per_failed_file";
  public static final String LINES_PER_FAILED_FILE_DESC =
      "Lines per failed file,only takes effect and required when sql_dialect is table .(option)";
  public static final String IMPORT_COMPLETELY = "Import completely!";
  public static final int BATCH_POINT_SIZE = 10000;

  public static final String IMPORT_INIT_MEET_ERROR_MSG = "Meet error when init import because ";
  public static final String REQUIRED_ARGS_ERROR_MSG =
      "Invalid args: Required values for option '%s' not provided";
}
