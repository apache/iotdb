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

package org.apache.iotdb.session.i18n;

public final class SessionMessages {

  // Session
  public static final String NODE_URLS_EMPTY = "nodeUrls 不能为空。";
  public static final String REDIRECT_TWICE_LOG = "重定向了两次";
  public static final String REDIRECT_TWICE_LOG_WITH_SQL = "{} 重定向了两次";
  public static final String REDIRECT_TWICE_MSG = "重定向了两次，请重试。";
  public static final String REDIRECT_TWICE_MSG_WITH_SQL = "%s 重定向了两次，请重试。";
  public static final String FAILED_TO_EXECUTE_FOR_ENDPOINT = "在 {} 上执行 '{}' 失败";
  public static final String ALL_VALUES_NULL =
      "{} 的所有值均为 null，null 值为 {}";
  public static final String SOME_VALUES_NULL =
      "{} 的部分值为 null，null 值为 {}";
  public static final String MEET_ERROR_WHEN_ASYNC_INSERT = "异步写入时遇到错误！";
  public static final String MEASUREMENT_NON_NULL = "测点名称不能为 null";
  public static final String NO_TABLET_INSERTING = "没有可写入的 Tablet！";
  public static final String SESSION_NOT_OPEN =
      "Session 尚未打开，请先调用 Session.open()";
  public static final String ALL_INSERT_DATA_IS_NULL = "所有写入数据均为 null。";

  // SessionConnection
  public static final String CLUSTER_NO_NODES = "集群中没有可连接的节点";
  public static final String CLOSE_SESSION_ERROR =
      "关闭服务端 Session 时发生错误，服务器可能已宕机。";
  public static final String REDIRECT_QUERY_ERROR =
      "需要重定向查询，不应看到此错误。";
  public static final String RETRY_RECONNECTING =
      "第 {} 次重试，正在重连其他 DataNode";
  public static final String NODE_DOWN_TRY_NEXT =
      "当前节点 {} 可能已宕机，尝试下一个节点";
  public static final String LOGIN_FAILED = "登录失败，原因：{}";
  public static final String CLOSE_CONNECTION_FAILED = "关闭连接失败，{}";
  public static final String THREAD_INTERRUPTED_DURING_RETRY =
      "线程 {} 在第 {} 次重试等待 {} 毫秒时被中断，退出重试循环。";

  // ThriftConnection
  public static final String CLOSING_SESSION_FAILED =
      "关闭 Session-{} 与 {} 的连接失败。";

  // NodesSupplier
  public static final String FAILED_TO_CREATE_CONNECTION =
      "创建与 {} 的连接失败。";
  public static final String FAILED_TO_FETCH_DATA_NODE_LIST =
      "从 {} 获取 DataNode 列表失败。";

  // SessionUtils
  public static final String NODE_URLS_IS_NULL = "nodeUrls 为 null";

  // InternalNode (template)
  public static final String DUPLICATED_CHILD_IN_TEMPLATE =
      "模板中存在重复的子节点。";

  // SessionPool
  public static final String SESSION_POOL_IS_CLOSED = "Session 连接池已关闭";
  public static final String CLOSE_THE_SESSION_FAILED = "关闭 Session 失败。";
  public static final String TIMEOUT_TO_GET_CONNECTION =
      "从 %s 获取连接超时";
  public static final String INTERRUPTED = "被中断！";
  public static final String CLOSING_SESSION_POOL = "正在关闭 Session 连接池，清理队列...";

  // SessionPool - operation failed (warn)
  public static final String INSERT_TABLET_FAILED = "insertTablet 失败";
  public static final String INSERT_ALIGNED_TABLET_FAILED = "insertAlignedTablet 失败";
  public static final String INSERT_TABLETS_FAILED = "insertTablets 失败";
  public static final String INSERT_ALIGNED_TABLETS_FAILED = "insertAlignedTablets 失败";
  public static final String INSERT_RECORDS_FAILED = "insertRecords 失败";
  public static final String INSERT_ALIGNED_RECORDS_FAILED = "insertAlignedRecords 失败";
  public static final String INSERT_RECORD_FAILED = "insertRecord 失败";
  public static final String INSERT_ALIGNED_RECORD_FAILED = "insertAlignedRecord 失败";
  public static final String INSERT_RECORDS_OF_ONE_DEVICE_FAILED =
      "insertRecordsOfOneDevice 失败";
  public static final String INSERT_STRING_RECORDS_OF_ONE_DEVICE_FAILED =
      "insertStringRecordsOfOneDevice 失败";
  public static final String INSERT_ALIGNED_RECORDS_OF_ONE_DEVICE_FAILED =
      "insertAlignedRecordsOfOneDevice 失败";
  public static final String INSERT_ALIGNED_STRING_RECORDS_OF_ONE_DEVICE_FAILED =
      "insertAlignedStringRecordsOfOneDevice 失败";
  public static final String DELETE_DATA_FAILED = "deleteData 失败";
  public static final String DELETE_TIMESERIES_FAILED = "deleteTimeseries 失败";
  public static final String SET_STORAGE_GROUP_FAILED = "setStorageGroup 失败";
  public static final String DELETE_STORAGE_GROUP_FAILED = "deleteStorageGroup 失败";
  public static final String DELETE_STORAGE_GROUPS_FAILED = "deleteStorageGroups 失败";
  public static final String CREATE_DATABASE_FAILED = "createDatabase 失败";
  public static final String DELETE_DATABASE_FAILED = "deleteDatabase 失败";
  public static final String DELETE_DATABASES_FAILED = "deleteDatabases 失败";
  public static final String CREATE_TIMESERIES_FAILED = "createTimeseries 失败";
  public static final String CREATE_ALIGNED_TIMESERIES_FAILED = "createAlignedTimeseries 失败";
  public static final String CREATE_MULTI_TIMESERIES_FAILED = "createMultiTimeseries 失败";
  public static final String CHECK_TIMESERIES_EXISTS_FAILED = "checkTimeseriesExists 失败";
  public static final String EXECUTE_QUERY_STATEMENT_FAILED = "executeQueryStatement 失败";
  public static final String EXECUTE_NON_QUERY_STATEMENT_FAILED =
      "executeNonQueryStatement 失败";
  public static final String EXECUTE_RAW_DATA_QUERY_FAILED = "executeRawDataQuery 失败";
  public static final String EXECUTE_LAST_DATA_QUERY_FAILED = "executeLastDataQuery 失败";
  public static final String EXECUTE_AGGREGATION_QUERY_FAILED = "executeAggregationQuery 失败";
  public static final String GET_TIMESTAMP_PRECISION_FAILED = "getTimestampPrecision 失败";
  public static final String TEST_INSERT_TABLET_FAILED = "testInsertTablet 失败";
  public static final String TEST_INSERT_TABLETS_FAILED = "testInsertTablets 失败";
  public static final String TEST_INSERT_RECORD_FAILED = "testInsertRecord 失败";
  public static final String TEST_INSERT_RECORDS_FAILED = "testInsertRecords 失败";
  public static final String CREATE_SCHEMA_TEMPLATE_FAILED = "createSchemaTemplate 失败";
  public static final String ADD_ALIGNED_MEASUREMENTS_IN_TEMPLATE_FAILED =
      "addAlignedMeasurementsInTemplate 失败";
  public static final String ADD_ALIGNED_MEASUREMENT_IN_TEMPLATE_FAILED =
      "addAlignedMeasurementInTemplate 失败";
  public static final String ADD_UNALIGNED_MEASUREMENTS_IN_TEMPLATE_FAILED =
      "addUnalignedMeasurementsInTemplate 失败";
  public static final String ADD_UNALIGNED_MEASUREMENT_IN_TEMPLATE_FAILED =
      "addUnalignedMeasurementInTemplate 失败";
  public static final String DELETE_NODE_IN_TEMPLATE_FAILED = "deleteNodeInTemplate 失败";
  public static final String COUNT_MEASUREMENTS_IN_TEMPLATE_FAILED =
      "countMeasurementsInTemplate 失败";
  public static final String IS_MEASUREMENT_IN_TEMPLATE_FAILED =
      "isMeasurementInTemplate 失败";
  public static final String IS_PATH_EXIST_IN_TEMPLATE_FAILED =
      "isPathExistInTemplata 失败";
  public static final String SHOW_MEASUREMENTS_IN_TEMPLATE_FAILED =
      "showMeasurementsInTemplate 失败";
  public static final String SHOW_ALL_TEMPLATES_FAILED = "showAllTemplates 失败";
  public static final String SHOW_PATHS_TEMPLATE_SET_ON_FAILED =
      "showPathsTemplateSetOn 失败";
  public static final String SHOW_PATHS_TEMPLATE_USING_ON_FAILED =
      "showPathsTemplateUsingOn 失败";
  public static final String SET_SCHEMA_TEMPLATE_ON_FAILED =
      "setSchemaTemplate [{}] 在 [{}] 上失败";
  public static final String UNSET_SCHEMA_TEMPLATE_ON_FAILED =
      "unsetSchemaTemplate [{}] 在 [{}] 上失败";
  public static final String DROP_SCHEMA_TEMPLATE_FAILED =
      "dropSchemaTemplate [{}] 失败";
  public static final String CREATE_TIMESERIES_OF_SCHEMA_TEMPLATE_FAILED =
      "createTimeseriesOfSchemaTemplate {} 失败";
  public static final String SET_TIMEZONE_FAILED = "设置时区为 [{}] 失败";
  public static final String FETCH_ALL_CONNECTIONS_FAILED = "fetchAllConnections 失败";

  // SessionPool - unexpected error (error)
  public static final String UNEXPECTED_ERROR_IN_INSERT_TABLET =
      "insertTablet 发生意外错误";
  public static final String UNEXPECTED_ERROR_IN_INSERT_ALIGNED_TABLET =
      "insertAlignedTablet 发生意外错误";
  public static final String UNEXPECTED_ERROR_IN_INSERT_TABLETS =
      "insertTablets 发生意外错误";
  public static final String UNEXPECTED_ERROR_IN_INSERT_ALIGNED_TABLETS =
      "insertAlignedTablets 发生意外错误";
  public static final String UNEXPECTED_ERROR_IN_INSERT_RECORDS =
      "insertRecords 发生意外错误";
  public static final String UNEXPECTED_ERROR_IN_INSERT_ALIGNED_RECORDS =
      "insertAlignedRecords 发生意外错误";
  public static final String UNEXPECTED_ERROR_IN_INSERT_RECORD =
      "insertRecord 发生意外错误";
  public static final String UNEXPECTED_ERROR_IN_INSERT_ALIGNED_RECORD =
      "insertAlignedRecord 发生意外错误";
  public static final String UNEXPECTED_ERROR_IN_INSERT_RECORDS_OF_ONE_DEVICE =
      "insertRecordsOfOneDevice 发生意外错误";
  public static final String UNEXPECTED_ERROR_IN_INSERT_STRING_RECORDS_OF_ONE_DEVICE =
      "insertStringRecordsOfOneDevice 发生意外错误";
  public static final String UNEXPECTED_ERROR_IN_INSERT_ALIGNED_RECORDS_OF_ONE_DEVICE =
      "insertAlignedRecordsOfOneDevice 发生意外错误";
  public static final String UNEXPECTED_ERROR_IN_INSERT_ALIGNED_STRING_RECORDS_OF_ONE_DEVICE =
      "insertAlignedStringRecordsOfOneDevice 发生意外错误";
  public static final String UNEXPECTED_ERROR_IN_DELETE_DATA =
      "deleteData 发生意外错误";
  public static final String UNEXPECTED_ERROR_IN_DELETE_TIMESERIES =
      "deleteTimeseries 发生意外错误";
  public static final String UNEXPECTED_ERROR_IN_SET_STORAGE_GROUP =
      "setStorageGroup 发生意外错误";
  public static final String UNEXPECTED_ERROR_IN_DELETE_STORAGE_GROUP =
      "deleteStorageGroup 发生意外错误";
  public static final String UNEXPECTED_ERROR_IN_DELETE_STORAGE_GROUPS =
      "deleteStorageGroups 发生意外错误";
  public static final String UNEXPECTED_ERROR_IN_CREATE_DATABASE =
      "createDatabase 发生意外错误";
  public static final String UNEXPECTED_ERROR_IN_DELETE_DATABASE =
      "deleteDatabase 发生意外错误";
  public static final String UNEXPECTED_ERROR_IN_DELETE_DATABASES =
      "deleteDatabases 发生意外错误";
  public static final String UNEXPECTED_ERROR_IN_CREATE_TIMESERIES =
      "createTimeseries 发生意外错误";
  public static final String UNEXPECTED_ERROR_IN_CREATE_ALIGNED_TIMESERIES =
      "createAlignedTimeseries 发生意外错误";
  public static final String UNEXPECTED_ERROR_IN_CREATE_MULTI_TIMESERIES =
      "createMultiTimeseries 发生意外错误";
  public static final String UNEXPECTED_ERROR_IN_CHECK_TIMESERIES_EXISTS =
      "checkTimeseriesExists 发生意外错误";
  public static final String UNEXPECTED_ERROR_IN_EXECUTE_QUERY_STATEMENT =
      "executeQueryStatement 发生意外错误";
  public static final String UNEXPECTED_ERROR_IN_EXECUTE_NON_QUERY_STATEMENT =
      "executeNonQueryStatement 发生意外错误";
  public static final String UNEXPECTED_ERROR_IN_EXECUTE_RAW_DATA_QUERY =
      "executeRawDataQuery 发生意外错误";
  public static final String UNEXPECTED_ERROR_IN_EXECUTE_LAST_DATA_QUERY =
      "executeLastDataQuery 发生意外错误";
  public static final String UNEXPECTED_ERROR_IN_EXECUTE_AGGREGATION_QUERY =
      "executeAggregationQuery 发生意外错误";
  public static final String UNEXPECTED_ERROR_IN_GET_TIMESTAMP_PRECISION =
      "getTimestampPrecision 发生意外错误";
  public static final String UNEXPECTED_ERROR_IN_TEST_INSERT_TABLET =
      "testInsertTablet 发生意外错误";
  public static final String UNEXPECTED_ERROR_IN_TEST_INSERT_TABLETS =
      "testInsertTablets 发生意外错误";
  public static final String UNEXPECTED_ERROR_IN_TEST_INSERT_RECORD =
      "testInsertRecord 发生意外错误";
  public static final String UNEXPECTED_ERROR_IN_TEST_INSERT_RECORDS =
      "testInsertRecords 发生意外错误";
  public static final String UNEXPECTED_ERROR_IN_CREATE_SCHEMA_TEMPLATE =
      "createSchemaTemplate 发生意外错误";
  public static final String UNEXPECTED_ERROR_IN_ADD_ALIGNED_MEASUREMENTS_IN_TEMPLATE =
      "addAlignedMeasurementsInTemplate 发生意外错误";
  public static final String UNEXPECTED_ERROR_IN_ADD_ALIGNED_MEASUREMENT_IN_TEMPLATE =
      "addAlignedMeasurementInTemplate 发生意外错误";
  public static final String UNEXPECTED_ERROR_IN_ADD_UNALIGNED_MEASUREMENTS_IN_TEMPLATE =
      "addUnalignedMeasurementsInTemplate 发生意外错误";
  public static final String UNEXPECTED_ERROR_IN_ADD_UNALIGNED_MEASUREMENT_IN_TEMPLATE =
      "addUnalignedMeasurementInTemplate 发生意外错误";
  public static final String UNEXPECTED_ERROR_IN_DELETE_NODE_IN_TEMPLATE =
      "deleteNodeInTemplate 发生意外错误";
  public static final String UNEXPECTED_ERROR_IN_COUNT_MEASUREMENTS_IN_TEMPLATE =
      "countMeasurementsInTemplate 发生意外错误";
  public static final String UNEXPECTED_ERROR_IN_IS_MEASUREMENT_IN_TEMPLATE =
      "isMeasurementInTemplate 发生意外错误";
  public static final String UNEXPECTED_ERROR_IN_IS_PATH_EXIST_IN_TEMPLATE =
      "isPathExistInTemplate 发生意外错误";
  public static final String UNEXPECTED_ERROR_IN_SHOW_MEASUREMENTS_IN_TEMPLATE =
      "showMeasurementsInTemplate 发生意外错误";
  public static final String UNEXPECTED_ERROR_IN_SHOW_ALL_TEMPLATES =
      "showAllTemplates 发生意外错误";
  public static final String UNEXPECTED_ERROR_IN_SHOW_PATHS_TEMPLATE_SET_ON =
      "showPathsTemplateSetOn 发生意外错误";
  public static final String UNEXPECTED_ERROR_IN_SHOW_PATHS_TEMPLATE_USING_ON =
      "showPathsTemplateUsingOn 发生意外错误";
  public static final String UNEXPECTED_ERROR_IN_SET_SCHEMA_TEMPLATE =
      "setSchemaTemplate 发生意外错误";
  public static final String UNEXPECTED_ERROR_IN_UNSET_SCHEMA_TEMPLATE =
      "unsetSchemaTemplate 发生意外错误";
  public static final String UNEXPECTED_ERROR_IN_DROP_SCHEMA_TEMPLATE =
      "dropSchemaTemplate 发生意外错误";
  public static final String UNEXPECTED_ERROR_IN_CREATE_TIMESERIES_USING_SCHEMA_TEMPLATE =
      "createTimeseriesUsingSchemaTemplate 发生意外错误";

  private SessionMessages() {}
  // ---------------------------------------------------------------------------
  // Additional auto-collected messages
  // ---------------------------------------------------------------------------
  public static final String LOG_PROTOCOL_DIFFER_CLIENT_VERSION_ARG_BUT_SERVER_VERSION_ARG_9C8EC583 = "协议不同，客户端版本为 {}，服务器版本为 {}";
  public static final String EXCEPTION_PROTOCOL_NOT_SUPPORTED_CLIENT_VERSION_ARG_BUT_SERVER_VERSION_ARG_53F892DC = "协议不支持，客户端版本为 %s，服务器版本为 %s";
  public static final String LOG_RETRY_ATTEMPT_ARG_RESULT_ARG_EXCEPTION_ARG_20E5D9DA = "第 {} 次重试，结果 {}，异常 {}";
  public static final String LOG_ALL_VALUES_NULL_SUBMISSION_IGNORED_DEVICEID_ARG_TIME_ARG_MEASUREMENTS_07AFDDFE =
      "所有值均为 null，本次提交被忽略，deviceId 为 [{}]，time 为 [{}]，measurements 为 [{}]";
  public static final String EXCEPTION_DEVICEIDS_TIMES_MEASUREMENTSLIST_VALUESLIST_S_SIZE_SHOULD_EQUAL_EC87D88B = "deviceIds、times、measurementsList 和 valuesList 的大小应相等";
  public static final String EXCEPTION_PREFIXPATHS_TIMES_SUBMEASUREMENTSLIST_VALUESLIST_S_SIZE_SHOULD_EQUAL_1465011C = "prefixPaths、times、subMeasurementsList 和 valuesList 的大小应相等";
  public static final String EXCEPTION_TIMES_SUBMEASUREMENTSLIST_VALUESLIST_S_SIZE_SHOULD_EQUAL_002C539A = "times、subMeasurementsList 和 valuesList 的大小应相等";
  public static final String EXCEPTION_DIFFERENT_LENGTH_MEASUREMENTS_DATATYPES_ENCODINGS_ED354A24 = "measurements、datatypes、encodings 的长度不同 ";
  public static final String EXCEPTION_COMPRESSORS_CREATE_DEVICE_TEMPLATE_BBDBB28E = "或创建设备模板时的压缩器。";
  public static final String EXCEPTION_GIVEN_DEVICE_PATH_LIST_SHOULD_NOT_CONTAINS_NULL_E9132577 = "给定的设备路径列表不应为空，也不应包含 null。";
  public static final String EXCEPTION_YOU_SHOULD_SPECIFY_EITHER_NODEURLS_HOST_RPCPORT_BUT_NOT_BOTH_77E7B084 = "应指定 nodeUrls 或 (host + rpcPort)，但不能同时指定";
  public static final String LOG_CANNOT_PUT_VALUES_MEASUREMENT_ARG_TYPE_ARG_27AFC67B = "无法写入测点 {} 的值，类型={}";
  public static final String EXCEPTION_DATA_TYPE_ARG_NOT_SUPPORTED_31213160 = "不支持数据类型 %s。";
  public static final String LOG_FAILED_CHANGE_BACK_SQL_DIALECT_EXECUTING_SET_SQL_DIALECT_ARG_947F35E7 = "执行 set sql_dialect={} 时无法切回 sql_dialect";
  public static final String LOG_FAILED_CHANGE_BACK_DATABASE_EXECUTING_USE_ARG_274541CA = "执行 use {} 时无法切回数据库";
  public static final String LOG_SESSIONPOOL_HAS_WAIT_ARG_SECONDS_GET_NEW_CONNECTION_ARG_ARG_D053274A = "SessionPool 已等待 {} 秒以获取新连接：{}，参数 {}";
  public static final String LOG_CURRENT_OCCUPIED_SIZE_ARG_QUEUE_SIZE_ARG_CONSIDERED_SIZE_ARG_DE97C14E = "当前已占用大小 {}，队列大小 {}，纳入计算的大小 {} ";
  public static final String EXCEPTION_RETRY_EXECUTE_STATEMENT_ARG_FAILED_ARG_TIMES_ARG_216C6873 = "在 %s 上重试执行语句失败 %d 次：%s";
  public static final String EXCEPTION_SESSIONPOOL_DOESN_T_SUPPORT_EXECUTING_ARG_DIRECTLY_B778F701 = "SessionPool 不支持直接执行 %s";

}
