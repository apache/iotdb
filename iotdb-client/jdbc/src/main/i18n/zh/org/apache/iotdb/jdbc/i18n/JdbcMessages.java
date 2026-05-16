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

  private JdbcMessages() {}
}
