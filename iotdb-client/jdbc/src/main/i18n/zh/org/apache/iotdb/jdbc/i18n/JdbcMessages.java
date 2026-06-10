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

package org.apache.iotdb.jdbc.i18n;

public final class JdbcMessages {

  // IoTDBDriver
  public static final String REGISTER_DRIVER_ERROR =
      "注册 TsFile 驱动时发生错误";
  public static final String METHOD_NOT_SUPPORTED = "不支持此方法";

  // StringUtils
  public static final String TO_PLAIN_STRING_ERROR = "转换为纯文本字符串方法错误：";
  public static final String CONSISTENT_TO_STRING_ERROR = "一致性 toString 错误：";

  // GroupedLSBWatermarkEncoder
  public static final String CANNOT_FIND_MD5 = "错误：无法找到 MD5 算法！";
  public static final String MIN_BIT_BIGGER_THAN_MAX =
      "错误：minBitPosition 大于 maxBitPosition";

  // IoTDBTracingInfo
  public static final String INVALID_STATISTICS_NAME = "无效的统计名称！";

  // IoTDBStatement
  public static final String CANNOT_UNWRAP_TO = "无法转换为 ";
  public static final String CANCEL_STATEMENT_ERROR = "取消语句时发生错误。";
  public static final String CLOSE_STATEMENT_ERROR = "关闭语句时发生错误。";
  public static final String NOT_SUPPORT_CLOSE_ON_COMPLETION = "不支持 closeOnCompletion";
  public static final String QUERY_RESULT_SHOULD_NOT_BE_NULL =
      "execResp.queryResult 不应为 null。";
  public static final String DIRECTION_NOT_SUPPORTED = "不支持方向 %d！";
  public static final String FETCH_SIZE_MUST_BE_NON_NEGATIVE = "fetchSize %d 必须 >= 0！";
  public static final String NOT_SUPPORT_GET_GENERATED_KEYS = "不支持 getGeneratedKeys";
  public static final String NOT_SUPPORT_GET_MAX_FIELD_SIZE = "不支持 getMaxFieldSize";
  public static final String MAX_ROWS_MUST_BE_NON_NEGATIVE = "maxRows %d 必须 >= 0！";
  public static final String NOT_SUPPORT_GET_MORE_RESULTS = "不支持 getMoreResults";
  public static final String NOT_SUPPORT_GET_RESULT_SET_CONCURRENCY =
      "不支持 getResultSetConcurrency";
  public static final String NOT_SUPPORT_GET_RESULT_SET_HOLDABILITY =
      "不支持 getResultSetHoldability";
  public static final String NOT_SUPPORT_IS_CLOSE_ON_COMPLETION =
      "不支持 isCloseOnCompletion";
  public static final String NOT_SUPPORT_IS_POOLABLE = "不支持 isPoolable";
  public static final String NOT_SUPPORT_SET_POOLABLE = "不支持 setPoolable";
  public static final String NOT_SUPPORT_SET_CURSOR_NAME = "不支持 setCursorName";
  public static final String NOT_SUPPORT_SET_ESCAPE_PROCESSING =
      "不支持 setEscapeProcessing";
  public static final String CANNOT_AFTER_CONNECTION_CLOSED =
      "连接已关闭后无法执行 %s！";

  // IoTDBTablePreparedStatement
  public static final String FAILED_TO_PREPARE_STATEMENT = "预编译语句失败：";
  public static final String PARAMETER_UNSET = "参数 #%d 未设置";
  public static final String FAILED_TO_EXECUTE_PREPARED_STATEMENT =
      "执行预编译语句失败：";
  public static final String FAILED_TO_DEALLOCATE_PREPARED_STATEMENT =
      "释放预编译语句失败：{}";
  public static final String ERROR_DEALLOCATING_PREPARED_STATEMENT =
      "释放预编译语句时出错";
  public static final String FAILED_TO_GET_TIME_PRECISION =
      "获取时间精度失败：";
  public static final String FAILED_TO_READ_BINARY_STREAM =
      "读取二进制流失败：";

  // IoTDBPreparedStatement
  public static final String NO_TYPE_MATCHED = "没有匹配的类型";
  public static final String SQL_DEBUG = "SQL {}";
  public static final String PARAMETERS_DEBUG = "参数 {}";

  // IoTDBResultMetadata
  public static final String NO_COLUMN_EXISTS = "不存在任何列";
  public static final String COLUMN_DOES_NOT_EXIST = "列 %d 不存在";
  public static final String COLUMN_INDEX_START_FROM_1 = "列索引应从 1 开始";

  // IoTDBAbstractDatabaseMetadata
  public static final String NO_DATA_TYPE_MATCHED = "没有匹配的数据类型：{}";
  public static final String GET_READ_ONLY_ERROR = "获取只读模式错误：{}";
  public static final String CANNOT_GET_READ_ONLY_MODE = "无法获取只读模式";
  public static final String GET_SYSTEM_FUNCTIONS_ERROR = "获取系统函数错误：{}";
  public static final String GET_MAX_CONCURRENT_CLIENT_ERROR =
      "获取最大并发客户端数错误：{}";
  public static final String GET_MAX_STATEMENT_LENGTH_ERROR =
      "获取最大语句长度错误：{}";
  public static final String GET_PROCEDURES_ERROR = "获取存储过程错误：{}";
  public static final String GET_PROCEDURE_COLUMNS_ERROR = "获取存储过程列错误：{}";
  public static final String GET_BEST_ROW_IDENTIFIER_ERROR =
      "获取最佳行标识符错误：{}";
  public static final String GET_VERSION_COLUMNS_ERROR = "获取版本列错误：{}";
  public static final String GET_IMPORT_KEYS_ERROR = "获取导入键错误：{}";
  public static final String GET_EXPORTED_KEYS_ERROR = "获取导出键错误：{}";
  public static final String GET_CROSS_REFERENCE_ERROR = "获取交叉引用错误：{}";
  public static final String GET_INDEX_INFO_ERROR = "获取索引信息错误：{}";
  public static final String GET_UDTS_ERROR = "获取 UDT 错误：{}";
  public static final String GET_SUPER_TYPES_ERROR = "获取父类型错误：{}";
  public static final String GET_SUPER_TABLES_ERROR = "获取父表错误：{}";
  public static final String GET_ATTRIBUTES_ERROR = "获取属性错误：{}";
  public static final String GET_DB_MAJOR_VERSION_ERROR =
      "获取数据库主版本号错误：{}";
  public static final String GET_DB_MINOR_VERSION_ERROR =
      "获取数据库次版本号错误：{}";

  // IoTDBDatabaseMetadata
  public static final String INIT_SQL_KEYWORDS_ERROR =
      "初始化 SQL 关键字时出错：";
  public static final String GET_TABLES_SQL = "获取表：SQL：{}";
  public static final String GET_PRIMARY_KEYS_ERROR = "获取主键错误：{}";
  public static final String FAILED_TO_FETCH_METADATA_JSON =
      "获取 JSON 格式的元数据失败：";

  // IoTDBRelationalDatabaseMetadata
  public static final String RELATIONAL_INIT_SQL_KEYWORDS_ERROR =
      "初始化 SQL 关键字时出错：";
  public static final String RELATIONAL_GET_PRIMARY_KEYS_ERROR =
      "获取主键错误：{}";

  // IoTDBDataSource
  public static final String GET_CONNECTION_ERROR = "获取连接错误：";

  // IoTDBDataSourceFactory
  public static final String REMAINING_PROPERTIES = "剩余属性 {}";

  // IoTDBJDBCResultSet
  public static final String CLOSE_SERVER_SIDE_ERROR =
      "服务端关闭操作时发生错误 ";
  public static final String CLOSE_CONNECTING_ERROR =
      "连接服务器进行关闭操作时发生错误 ";
  public static final String GET_METADATA_ERROR = "获取元数据错误：{}";

  // IoTDBConnection
  public static final String INPUT_URL_NULL = "输入的 URL 不能为 null";
  public static final String NOT_SUPPORT_IS_WRAPPER_FOR = "不支持 isWrapperFor";
  public static final String NOT_SUPPORT_UNWRAP = "不支持 unwrap";
  public static final String NOT_SUPPORT_ABORT = "不支持 abort";
  public static final String NOT_SUPPORT_CREATE_ARRAY_OF = "不支持 createArrayOf";
  public static final String NOT_SUPPORT_CREATE_BLOB = "不支持 createBlob";
  public static final String NOT_SUPPORT_CREATE_CLOB = "不支持 createClob";
  public static final String NOT_SUPPORT_CREATE_NCLOB = "不支持 createNClob";
  public static final String NOT_SUPPORT_CREATE_SQLXML = "不支持 createSQLXML";
  public static final String CANNOT_CREATE_STATEMENT_CLOSED =
      "连接已关闭，无法创建语句";
  public static final String NOT_SUPPORT_CREATE_STATEMENT = "不支持 createStatement";
  public static final String NOT_SUPPORT_CREATE_STRUCT = "不支持 createStruct";
  public static final String NOT_SUPPORT_GET_CLIENT_INFO = "不支持 getClientInfo";
  public static final String NOT_SUPPORT_SET_CLIENT_INFO = "不支持 setClientInfo";
  public static final String NOT_SUPPORT_SET_HOLDABILITY = "不支持 setHoldability";
  public static final String NOT_SUPPORT_GET_SCHEMA = "不支持 getSchema";
  public static final String NOT_SUPPORT_SET_TRANSACTION_ISOLATION =
      "不支持 setTransactionIsolation";
  public static final String NOT_SUPPORT_GET_TYPE_MAP = "不支持 getTypeMap";
  public static final String NOT_SUPPORT_SET_TYPE_MAP = "不支持 setTypeMap";
  public static final String NOT_SUPPORT_READ_ONLY = "不支持 readOnly";
  public static final String NOT_SUPPORT_NATIVE_SQL = "不支持 nativeSQL";
  public static final String NOT_SUPPORT_RELEASE_SAVEPOINT =
      "不支持 releaseSavepoint";
  public static final String SET_TIMEZONE_ERROR = "设置时区错误：";
  public static final String NOT_SUPPORT_CLIENT_INFO_TYPE =
      "不支持此类型的客户端信息：";
  public static final String NOT_SUPPORT_SET_NETWORK_TIMEOUT =
      "不支持 setNetworkTimeout";
  public static final String QUERY_TIMEOUT_MUST_BE_NON_NEGATIVE =
      "queryTimeout %d 必须 >= 0！";
  public static final String NOT_SUPPORT_SET_SAVEPOINT = "不支持 setSavepoint";
  public static final String USE_DATABASE_ERROR = "使用数据库错误：{}";
  public static final String RECONNECT_INTERRUPTED = "重新连接被中断。";
  public static final String SINGLE_QUOTE = "'";
  public static final String RIGHT_PARENTHESIS = ")";

  private JdbcMessages() {}
  // ---------------------------------------------------------------------------
  // Additional auto-collected messages
  // ---------------------------------------------------------------------------
  public static final String EXCEPTION_CONNECTION_ERROR_PLEASE_CHECK_WHETHER_NETWORK_AVAILABLE_SERVER_CA72E0D6 = "连接 错误, please check whether the network is available or the server";
  public static final String EXCEPTION_HAS_STARTED_BD7BC366 = " has 开始ed.";
  public static final String EXCEPTION_ERROR_URL_FORMAT_URL_SHOULD_JDBC_IOTDB_ANYTHING_PORT_DATABASE_17D1DCFB =
      "错误 url format, url 应be jdbc:iotdb://anything:port/[数据库] or"
      + " jdbc:iotdb://anything:port[/数据库]?property1=value1&property2=value2, 当前url is ";
  public static final String EXCEPTION_FAIL_RECONNECT_SERVER_EXECUTING_ARG_PLEASE_CHECK_SERVER_STATUS_34668040 = "无法reconnect to server when executing %s. please check server 状态";
  public static final String EXCEPTION_FAIL_RECONNECT_SERVER_EXECUTING_BATCH_SQLS_PLEASE_CHECK_SERVER_STATUS_1E4C0C24 = "无法reconnect to server when executing batch sqls. please check server 状态";
  public static final String EXCEPTION_FAIL_RECONNECT_SERVER_EXECUTE_QUERY_B6F770F5 = "无法reconnect to server when execute 查询 ";
  public static final String EXCEPTION_PLEASE_CHECK_SERVER_STATUS_DA9E1E33 = ". please check server 状态";
  public static final String EXCEPTION_FAIL_RECONNECT_SERVER_EXECUTE_UPDATE_7F009AA4 = "无法reconnect to server when execute update ";
  public static final String EXCEPTION_CANNOT_GET_ID_STATEMENT_AFTER_RECONNECTING_PLEASE_CHECK_SERVER_STATUS_D4C1F67E = "无法get id for statement after reconnecting. please check server 状态";
  public static final String EXCEPTION_CAN_T_INFER_SQL_TYPE_INSTANCE_ARG_USE_SETOBJECT_EXPLICIT_F457F33A = "Can't infer the SQL type for an instance of %s. Use setObject() with explicit type.";
  public static final String EXCEPTION_PARAMETER_INDEX_OUT_RANGE_3DD066E0 = "参数 index out of range: ";
  public static final String EXCEPTION_EXPECTED_1_3F4E8D6E = " (expected 1-";
  public static final String LOG_SET_TIME_ERROR_IOTDB_PREPARED_STATEMENT_ARG_AAAACB25 = "set time 错误 when iotdb prepared statement :%s ";
  public static final String EXCEPTION_CAN_T_INFER_SQL_TYPE_USE_INSTANCE_ARG_USE_SETOBJECT_A5B1C1BD = "Can''t infer the SQL type to use for an instance of %s. Use setObject() with";
  public static final String EXCEPTION_EXPLICIT_TYPES_VALUE_SPECIFY_TYPE_USE_CD046EDA = " an explicit Types value to specify the type to use.";
  public static final String EXCEPTION_NO_CONVERSION_3F7E3A35 = "No conversion from ";
  public static final String EXCEPTION_TYPES_BOOLEAN_POSSIBLE_54D316E6 = " to Types.BOOLEAN possible.";
  public static final String EXCEPTION_CAN_T_SET_SCALE_5559DE62 = "Can't set scale of '";
  public static final String EXCEPTION_DECIMAL_ARGUMENT_504BC102 = "' for DECIMAL argument '";
  public static final String LOG_FAIL_GET_ALL_TIMESERIES_2A802516 = "无法get all timeseries ";
  public static final String LOG_INFO_AFTER_RECONNECTING_7E70A784 = "info after reconnecting.";
  public static final String LOG_PLEASE_CHECK_SERVER_STATUS_2049BB22 = " please check server 状态";
  public static final String LOG_FAIL_RECONNECT_SERVER_AC4C86AB = "无法reconnect to server ";
  public static final String LOG_GETTING_ALL_TIMESERIES_INFO_PLEASE_CHECK_SERVER_STATUS_009B5EFE = "when getting all timeseries info. please check server 状态";
  public static final String EXCEPTION_FAILED_FETCH_ALL_METADATA_JSON_5FB95E70 = "无法fetch all metadata in json ";
  public static final String EXCEPTION_AFTER_RECONNECTING_PLEASE_CHECK_SERVER_STATUS_DE1D65AC = "after reconnecting. Please check the server 状态.";
  public static final String EXCEPTION_FAILED_RECONNECT_SERVER_632A4B76 = "无法reconnect to the server ";
  public static final String EXCEPTION_FETCHING_ALL_METADATA_JSON_PLEASE_CHECK_SERVER_STATUS_1A0813B2 = "when fetching all metadata in json. Please check the server 状态.";
  public static final String LOG_PROTOCOL_DIFFER_CLIENT_VERSION_ARG_BUT_SERVER_VERSION_ARG_F0AA3D03 = "Protocol differ, Client version is {}, but Server version is {}";
  public static final String LOG_ARG_ARG_PLEASE_CHANGE_IT_TIME_VIA_ALTER_USER_STATEMENT_6B67087C = "{}{}, please change it in time via 'ALTER USER' statement";
  public static final String EXCEPTION_ERROR_OCCURS_CLOSING_SESSION_AT_SERVER_MAYBE_SERVER_DOWN_2BCE63C0 = "错误 occurs when closing 会话 at server. Maybe server is down.";
  public static final String EXCEPTION_STATEMENTS_RESULT_SET_CONCURRENCY_ARG_NOT_SUPPORTED_C6043E9A = "Statements with result set concurrency %d 不是supported";
  public static final String EXCEPTION_STATEMENTS_RESULTSET_TYPE_ARG_NOT_SUPPORTED_8BE22644 = "Statements with ResultSet type %d 不是supported";
  public static final String EXCEPTION_PROTOCOL_NOT_SUPPORTED_CLIENT_VERSION_ARG_BUT_SERVER_VERSION_ARG_53F892DC = "Protocol 不supported, Client version is %s, but Server version is %s";
  public static final String EXCEPTION_CAN_NOT_ESTABLISH_CONNECTION_ARG_YOU_MAY_TRY_CONNECT_OLD_8FC3703E =
      "不能establish 连接 with %s : You may try to connect an old version IoTDB instance using a client"
      + " with new version: %s. ";
  public static final String EXCEPTION_CAN_NOT_ESTABLISH_CONNECTION_ARG_ARG_D7246055 = "不能establish 连接 with %s : %s. ";

}
