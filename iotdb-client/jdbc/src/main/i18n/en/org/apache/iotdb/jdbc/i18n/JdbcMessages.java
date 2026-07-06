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
      "Error occurs when registering TsFile driver";
  public static final String METHOD_NOT_SUPPORTED = "Method not supported";

  // StringUtils
  public static final String TO_PLAIN_STRING_ERROR = "To plain String method Error:";
  public static final String CONSISTENT_TO_STRING_ERROR = "consistent to String Error:";

  // GroupedLSBWatermarkEncoder
  public static final String CANNOT_FIND_MD5 = "ERROR: Cannot find MD5 algorithm!";
  public static final String MIN_BIT_BIGGER_THAN_MAX =
      "Error: minBitPosition is bigger than maxBitPosition";

  // IoTDBTracingInfo
  public static final String INVALID_STATISTICS_NAME = "Invalid statistics name!";

  // IoTDBStatement
  public static final String CANNOT_UNWRAP_TO = "Cannot unwrap to ";
  public static final String CANCEL_STATEMENT_ERROR = "Error occurs when canceling statement.";
  public static final String CLOSE_STATEMENT_ERROR = "Error occurs when closing statement.";
  public static final String NOT_SUPPORT_CLOSE_ON_COMPLETION = "Not support closeOnCompletion";
  public static final String QUERY_RESULT_SHOULD_NOT_BE_NULL =
      "execResp.queryResult should never be null.";
  public static final String DIRECTION_NOT_SUPPORTED = "direction %d is not supported!";
  public static final String FETCH_SIZE_MUST_BE_NON_NEGATIVE = "fetchSize %d must be >= 0!";
  public static final String NOT_SUPPORT_GET_GENERATED_KEYS = "Not support getGeneratedKeys";
  public static final String NOT_SUPPORT_GET_MAX_FIELD_SIZE = "Not support getMaxFieldSize";
  public static final String MAX_ROWS_MUST_BE_NON_NEGATIVE = "maxRows %d must be >= 0!";
  public static final String NOT_SUPPORT_GET_MORE_RESULTS = "Not support getMoreResults";
  public static final String NOT_SUPPORT_GET_RESULT_SET_CONCURRENCY =
      "Not support getResultSetConcurrency";
  public static final String NOT_SUPPORT_GET_RESULT_SET_HOLDABILITY =
      "Not support getResultSetHoldability";
  public static final String NOT_SUPPORT_IS_CLOSE_ON_COMPLETION =
      "Not support isCloseOnCompletion";
  public static final String NOT_SUPPORT_IS_POOLABLE = "Not support isPoolable";
  public static final String NOT_SUPPORT_SET_POOLABLE = "Not support setPoolable";
  public static final String NOT_SUPPORT_SET_CURSOR_NAME = "Not support setCursorName";
  public static final String NOT_SUPPORT_SET_ESCAPE_PROCESSING =
      "Not support setEscapeProcessing";
  public static final String CANNOT_AFTER_CONNECTION_CLOSED =
      "Cannot %s after connection has been closed!";

  // IoTDBTablePreparedStatement
  public static final String FAILED_TO_PREPARE_STATEMENT = "Failed to prepare statement: ";
  public static final String PARAMETER_UNSET = "Parameter #%d is unset";
  public static final String FAILED_TO_EXECUTE_PREPARED_STATEMENT =
      "Failed to execute prepared statement: ";
  public static final String FAILED_TO_DEALLOCATE_PREPARED_STATEMENT =
      "Failed to deallocate prepared statement: {}";
  public static final String ERROR_DEALLOCATING_PREPARED_STATEMENT =
      "Error deallocating prepared statement";
  public static final String FAILED_TO_GET_TIME_PRECISION =
      "Failed to get time precision: ";
  public static final String FAILED_TO_READ_BINARY_STREAM =
      "Failed to read binary stream: ";

  // IoTDBPreparedStatement
  public static final String NO_TYPE_MATCHED = "No type was matched";
  public static final String SQL_DEBUG = "SQL {}";
  public static final String PARAMETERS_DEBUG = "parameters {}";

  // IoTDBResultMetadata
  public static final String NO_COLUMN_EXISTS = "No column exists";
  public static final String COLUMN_DOES_NOT_EXIST = "column %d does not exist";
  public static final String COLUMN_INDEX_START_FROM_1 = "column index should start from 1";

  // IoTDBAbstractDatabaseMetadata
  public static final String NO_DATA_TYPE_MATCHED = "No data type was matched: {}";
  public static final String GET_READ_ONLY_ERROR = "Get is readOnly error: {}";
  public static final String CANNOT_GET_READ_ONLY_MODE = "Can not get the read-only mode";
  public static final String GET_SYSTEM_FUNCTIONS_ERROR = "Get system functions error: {}";
  public static final String GET_MAX_CONCURRENT_CLIENT_ERROR =
      "Get max concurrentClientNUm error: {}";
  public static final String GET_MAX_STATEMENT_LENGTH_ERROR =
      "Get max statement length error: {}";
  public static final String GET_PROCEDURES_ERROR = "Get procedures error: {}";
  public static final String GET_PROCEDURE_COLUMNS_ERROR = "Get procedure columns error: {}";
  public static final String GET_BEST_ROW_IDENTIFIER_ERROR =
      "Get best row identifier error: {}";
  public static final String GET_VERSION_COLUMNS_ERROR = "Get version columns error: {}";
  public static final String GET_IMPORT_KEYS_ERROR = "Get import keys error: {}";
  public static final String GET_EXPORTED_KEYS_ERROR = "Get exported keys error: {}";
  public static final String GET_CROSS_REFERENCE_ERROR = "Get cross reference error: {}";
  public static final String GET_INDEX_INFO_ERROR = "Get index info error: {}";
  public static final String GET_UDTS_ERROR = "Get UDTS error: {}";
  public static final String GET_SUPER_TYPES_ERROR = "Get super types error: {}";
  public static final String GET_SUPER_TABLES_ERROR = "Get super tables error: {}";
  public static final String GET_ATTRIBUTES_ERROR = "Get attributes error: {}";
  public static final String GET_DB_MAJOR_VERSION_ERROR =
      "Get database major version error: {}";
  public static final String GET_DB_MINOR_VERSION_ERROR =
      "Get database minor version error: {}";

  // IoTDBDatabaseMetadata
  public static final String INIT_SQL_KEYWORDS_ERROR =
      "Error when initializing SQL keywords: ";
  public static final String GET_TABLES_SQL = "Get tables: sql: {}";
  public static final String GET_PRIMARY_KEYS_ERROR = "Get primary keys error: {}";
  public static final String FAILED_TO_FETCH_METADATA_JSON =
      "Failed to fetch metadata in json because: ";

  // IoTDBRelationalDatabaseMetadata
  public static final String RELATIONAL_INIT_SQL_KEYWORDS_ERROR =
      "Error when initializing SQL keywords: ";
  public static final String RELATIONAL_GET_PRIMARY_KEYS_ERROR =
      "Get primary keys error: {}";

  // IoTDBDataSource
  public static final String GET_CONNECTION_ERROR = "get connection error:";

  // IoTDBDataSourceFactory
  public static final String REMAINING_PROPERTIES = "Remaining properties {}";

  // IoTDBJDBCResultSet
  public static final String CLOSE_SERVER_SIDE_ERROR =
      "Error occurs for close operation in server side because ";
  public static final String CLOSE_CONNECTING_ERROR =
      "Error occurs when connecting to server for close operation ";
  public static final String GET_METADATA_ERROR = "get meta data error: {}";

  // IoTDBConnection
  public static final String INPUT_URL_NULL = "Input url cannot be null";
  public static final String NOT_SUPPORT_IS_WRAPPER_FOR = "Does not support isWrapperFor";
  public static final String NOT_SUPPORT_UNWRAP = "Does not support unwrap";
  public static final String NOT_SUPPORT_ABORT = "Does not support abort";
  public static final String NOT_SUPPORT_CREATE_ARRAY_OF = "Does not support createArrayOf";
  public static final String NOT_SUPPORT_CREATE_BLOB = "Does not support createBlob";
  public static final String NOT_SUPPORT_CREATE_CLOB = "Does not support createClob";
  public static final String NOT_SUPPORT_CREATE_NCLOB = "Does not suppport createNClob";
  public static final String NOT_SUPPORT_CREATE_SQLXML = "Does not support createSQLXML";
  public static final String CANNOT_CREATE_STATEMENT_CLOSED =
      "Cannot create statement because connection is closed";
  public static final String NOT_SUPPORT_CREATE_STATEMENT = "Does not support createStatement";
  public static final String NOT_SUPPORT_CREATE_STRUCT = "Does not support createStruct";
  public static final String NOT_SUPPORT_GET_CLIENT_INFO = "Does not support getClientInfo";
  public static final String NOT_SUPPORT_SET_CLIENT_INFO = "Does not support setClientInfo";
  public static final String NOT_SUPPORT_SET_HOLDABILITY = "Does not support setHoldability";
  public static final String NOT_SUPPORT_GET_SCHEMA = "Does not support getSchema";
  public static final String NOT_SUPPORT_SET_TRANSACTION_ISOLATION =
      "Does not support setTransactionIsolation";
  public static final String NOT_SUPPORT_GET_TYPE_MAP = "Does not support getTypeMap";
  public static final String NOT_SUPPORT_SET_TYPE_MAP = "Does not support setTypeMap";
  public static final String NOT_SUPPORT_READ_ONLY = "Does not support readOnly";
  public static final String NOT_SUPPORT_NATIVE_SQL = "Does not support nativeSQL";
  public static final String NOT_SUPPORT_RELEASE_SAVEPOINT =
      "Does not support releaseSavepoint";
  public static final String SET_TIMEZONE_ERROR = "Set time_zone error: ";
  public static final String NOT_SUPPORT_CLIENT_INFO_TYPE =
      "Does not support this type of client info: ";
  public static final String NOT_SUPPORT_SET_NETWORK_TIMEOUT =
      "Does not support setNetworkTimeout";
  public static final String QUERY_TIMEOUT_MUST_BE_NON_NEGATIVE =
      "queryTimeout %d must be >= 0!";
  public static final String NOT_SUPPORT_SET_SAVEPOINT = "Does not support setSavepoint";
  public static final String USE_DATABASE_ERROR = "Use database error: {}";
  public static final String RECONNECT_INTERRUPTED = "reconnect is interrupted.";
  public static final String SINGLE_QUOTE = "'";
  public static final String RIGHT_PARENTHESIS = ")";

  private JdbcMessages() {}
  // ---------------------------------------------------------------------------
  // Additional auto-collected messages
  // ---------------------------------------------------------------------------
  public static final String EXCEPTION_CONNECTION_ERROR_PLEASE_CHECK_WHETHER_NETWORK_AVAILABLE_SERVER_CA72E0D6 = "Connection Error, please check whether the network is available or the server";
  public static final String EXCEPTION_HAS_STARTED_BD7BC366 = " has started.";
  public static final String EXCEPTION_ERROR_URL_FORMAT_URL_SHOULD_JDBC_IOTDB_ANYTHING_PORT_DATABASE_17D1DCFB =
      "Error url format, url should be jdbc:iotdb://anything:port/[database] or"
      + " jdbc:iotdb://anything:port[/database]?property1=value1&property2=value2, current url is ";
  public static final String EXCEPTION_FAIL_RECONNECT_SERVER_EXECUTING_ARG_PLEASE_CHECK_SERVER_STATUS_34668040 = "Fail to reconnect to server when executing %s. please check server status";
  public static final String EXCEPTION_FAIL_RECONNECT_SERVER_EXECUTING_BATCH_SQLS_PLEASE_CHECK_SERVER_STATUS_1E4C0C24 = "Fail to reconnect to server when executing batch sqls. please check server status";
  public static final String EXCEPTION_FAIL_RECONNECT_SERVER_EXECUTE_QUERY_B6F770F5 = "Fail to reconnect to server when execute query ";
  public static final String EXCEPTION_PLEASE_CHECK_SERVER_STATUS_DA9E1E33 = ". please check server status";
  public static final String EXCEPTION_FAIL_RECONNECT_SERVER_EXECUTE_UPDATE_7F009AA4 = "Fail to reconnect to server when execute update ";
  public static final String EXCEPTION_CANNOT_GET_ID_STATEMENT_AFTER_RECONNECTING_PLEASE_CHECK_SERVER_STATUS_D4C1F67E = "Cannot get id for statement after reconnecting. please check server status";
  public static final String EXCEPTION_CAN_T_INFER_SQL_TYPE_INSTANCE_ARG_USE_SETOBJECT_EXPLICIT_F457F33A = "Can't infer the SQL type for an instance of %s. Use setObject() with explicit type.";
  public static final String EXCEPTION_PARAMETER_INDEX_OUT_RANGE_3DD066E0 = "Parameter index out of range: ";
  public static final String EXCEPTION_EXPECTED_1_3F4E8D6E = " (expected 1-";
  public static final String LOG_SET_TIME_ERROR_IOTDB_PREPARED_STATEMENT_ARG_AAAACB25 = "set time error when iotdb prepared statement :%s ";
  public static final String EXCEPTION_CAN_T_INFER_SQL_TYPE_USE_INSTANCE_ARG_USE_SETOBJECT_A5B1C1BD = "Can''t infer the SQL type to use for an instance of %s. Use setObject() with";
  public static final String EXCEPTION_EXPLICIT_TYPES_VALUE_SPECIFY_TYPE_USE_CD046EDA = " an explicit Types value to specify the type to use.";
  public static final String EXCEPTION_NO_CONVERSION_3F7E3A35 = "No conversion from ";
  public static final String EXCEPTION_TYPES_BOOLEAN_POSSIBLE_54D316E6 = " to Types.BOOLEAN possible.";
  public static final String EXCEPTION_CAN_T_SET_SCALE_5559DE62 = "Can't set scale of '";
  public static final String EXCEPTION_DECIMAL_ARGUMENT_504BC102 = "' for DECIMAL argument '";
  public static final String LOG_FAIL_GET_ALL_TIMESERIES_2A802516 = "Fail to get all timeseries ";
  public static final String LOG_INFO_AFTER_RECONNECTING_7E70A784 = "info after reconnecting.";
  public static final String LOG_PLEASE_CHECK_SERVER_STATUS_2049BB22 = " please check server status";
  public static final String LOG_FAIL_RECONNECT_SERVER_AC4C86AB = "Fail to reconnect to server ";
  public static final String LOG_GETTING_ALL_TIMESERIES_INFO_PLEASE_CHECK_SERVER_STATUS_009B5EFE = "when getting all timeseries info. please check server status";
  public static final String EXCEPTION_FAILED_FETCH_ALL_METADATA_JSON_5FB95E70 = "Failed to fetch all metadata in json ";
  public static final String EXCEPTION_AFTER_RECONNECTING_PLEASE_CHECK_SERVER_STATUS_DE1D65AC = "after reconnecting. Please check the server status.";
  public static final String EXCEPTION_FAILED_RECONNECT_SERVER_632A4B76 = "Failed to reconnect to the server ";
  public static final String EXCEPTION_FETCHING_ALL_METADATA_JSON_PLEASE_CHECK_SERVER_STATUS_1A0813B2 = "when fetching all metadata in json. Please check the server status.";
  public static final String LOG_PROTOCOL_DIFFER_CLIENT_VERSION_ARG_BUT_SERVER_VERSION_ARG_F0AA3D03 = "Protocol differ, Client version is {}, but Server version is {}";
  public static final String LOG_ARG_ARG_PLEASE_CHANGE_IT_TIME_VIA_ALTER_USER_STATEMENT_6B67087C = "{}{}, please change it in time via 'ALTER USER' statement";
  public static final String EXCEPTION_ERROR_OCCURS_CLOSING_SESSION_AT_SERVER_MAYBE_SERVER_DOWN_2BCE63C0 = "Error occurs when closing session at server. Maybe server is down.";
  public static final String EXCEPTION_STATEMENTS_RESULT_SET_CONCURRENCY_ARG_NOT_SUPPORTED_C6043E9A = "Statements with result set concurrency %d are not supported";
  public static final String EXCEPTION_STATEMENTS_RESULTSET_TYPE_ARG_NOT_SUPPORTED_8BE22644 = "Statements with ResultSet type %d are not supported";
  public static final String EXCEPTION_PROTOCOL_NOT_SUPPORTED_CLIENT_VERSION_ARG_BUT_SERVER_VERSION_ARG_53F892DC = "Protocol not supported, Client version is %s, but Server version is %s";
  public static final String EXCEPTION_CAN_NOT_ESTABLISH_CONNECTION_ARG_YOU_MAY_TRY_CONNECT_OLD_8FC3703E =
      "Can not establish connection with %s : You may try to connect an old version IoTDB instance"
      + " using a client with new version: %s. ";
  public static final String EXCEPTION_CAN_NOT_ESTABLISH_CONNECTION_ARG_ARG_D7246055 = "Can not establish connection with %s : %s. ";

}
