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
  public static final String PARAMETER_IS_UNSET_PREFIX = "Parameter #";
  public static final String PARAMETER_IS_UNSET_SUFFIX = " is unset";

  private JdbcMessages() {}
}
