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
  public static final String NODE_URLS_EMPTY = "nodeUrls shouldn't be empty.";
  public static final String REDIRECT_TWICE = "{} redirect twice";
  public static final String REDIRECT_TWICE_EXCEPTION = "%s redirect twice, please try again.";
  public static final String FAILED_TO_EXECUTE_FOR_ENDPOINT = "failed to execute '{}' for {}";
  public static final String ALL_VALUES_NULL =
      "All values of the {} are null,null values are {}";
  public static final String SOME_VALUES_NULL =
      "Some values of {} are null,null values are {}";
  public static final String MEET_ERROR_WHEN_ASYNC_INSERT = "Meet error when async insert!";
  public static final String MEASUREMENT_NON_NULL = "measurement should be non null value";
  public static final String NO_TABLET_INSERTING = "No tablet is inserting!";
  public static final String SESSION_NOT_OPEN =
      "Session is not open, please invoke Session.open() first";
  public static final String ALL_INSERT_DATA_IS_NULL = "All inserted data is null.";

  // SessionConnection
  public static final String CLUSTER_NO_NODES = "Cluster has no nodes to connect";
  public static final String CLOSE_SESSION_ERROR =
      "Error occurs when closing session at server. Maybe server is down.";
  public static final String REDIRECT_QUERY_ERROR =
      "need to redirect query, should not see this.";
  public static final String RETRY_RECONNECTING =
      "Retry attempt #{}, Reconnecting to other datanode";
  public static final String NODE_DOWN_TRY_NEXT =
      "The current node may have been down {}, try next node";
  public static final String LOGIN_FAILED = "login in failed, because {}";
  public static final String CLOSE_CONNECTION_FAILED = "close connection failed, {}";
  public static final String THREAD_INTERRUPTED_DURING_RETRY =
      "Thread {} was interrupted during retry {} with wait time {} ms. Exiting retry loop.";

  // ThriftConnection
  public static final String CLOSING_SESSION_FAILED =
      "Closing Session-{} with {} failed.";

  // NodesSupplier
  public static final String FAILED_TO_CREATE_CONNECTION =
      "Failed to create connection with {}.";
  public static final String FAILED_TO_FETCH_DATA_NODE_LIST =
      "Failed to fetch data node list from {}.";

  // SessionUtils
  public static final String NODE_URLS_IS_NULL = "nodeUrls is null";

  // InternalNode (template)
  public static final String DUPLICATED_CHILD_IN_TEMPLATE =
      "Duplicated child of node in template.";

  // SessionPool
  public static final String SESSION_POOL_IS_CLOSED = "Session pool is closed";
  public static final String CLOSE_THE_SESSION_FAILED = "close the session failed.";
  public static final String TIMEOUT_TO_GET_CONNECTION =
      "timeout to get a connection from %s";
  public static final String INTERRUPTED = "Interrupted!";
  public static final String CLOSING_SESSION_POOL = "closing the session pool, cleaning queues...";

  // SessionPool - operation failed (warn)
  public static final String INSERT_TABLET_FAILED = "insertTablet failed";
  public static final String INSERT_ALIGNED_TABLET_FAILED = "insertAlignedTablet failed";
  public static final String INSERT_TABLETS_FAILED = "insertTablets failed";
  public static final String INSERT_ALIGNED_TABLETS_FAILED = "insertAlignedTablets failed";
  public static final String INSERT_RECORDS_FAILED = "insertRecords failed";
  public static final String INSERT_ALIGNED_RECORDS_FAILED = "insertAlignedRecords failed";
  public static final String INSERT_RECORD_FAILED = "insertRecord failed";
  public static final String INSERT_ALIGNED_RECORD_FAILED = "insertAlignedRecord failed";
  public static final String INSERT_RECORDS_OF_ONE_DEVICE_FAILED =
      "insertRecordsOfOneDevice failed";
  public static final String INSERT_STRING_RECORDS_OF_ONE_DEVICE_FAILED =
      "insertStringRecordsOfOneDevice failed";
  public static final String INSERT_ALIGNED_RECORDS_OF_ONE_DEVICE_FAILED =
      "insertAlignedRecordsOfOneDevice failed";
  public static final String INSERT_ALIGNED_STRING_RECORDS_OF_ONE_DEVICE_FAILED =
      "insertAlignedStringRecordsOfOneDevice failed";
  public static final String DELETE_DATA_FAILED = "deleteData failed";
  public static final String DELETE_TIMESERIES_FAILED = "deleteTimeseries failed";
  public static final String SET_STORAGE_GROUP_FAILED = "setStorageGroup failed";
  public static final String DELETE_STORAGE_GROUP_FAILED = "deleteStorageGroup failed";
  public static final String DELETE_STORAGE_GROUPS_FAILED = "deleteStorageGroups failed";
  public static final String CREATE_DATABASE_FAILED = "createDatabase failed";
  public static final String DELETE_DATABASE_FAILED = "deleteDatabase failed";
  public static final String DELETE_DATABASES_FAILED = "deleteDatabases failed";
  public static final String CREATE_TIMESERIES_FAILED = "createTimeseries failed";
  public static final String CREATE_ALIGNED_TIMESERIES_FAILED = "createAlignedTimeseries failed";
  public static final String CREATE_MULTI_TIMESERIES_FAILED = "createMultiTimeseries failed";
  public static final String CHECK_TIMESERIES_EXISTS_FAILED = "checkTimeseriesExists failed";
  public static final String EXECUTE_QUERY_STATEMENT_FAILED = "executeQueryStatement failed";
  public static final String EXECUTE_NON_QUERY_STATEMENT_FAILED =
      "executeNonQueryStatement failed";
  public static final String EXECUTE_RAW_DATA_QUERY_FAILED = "executeRawDataQuery failed";
  public static final String EXECUTE_LAST_DATA_QUERY_FAILED = "executeLastDataQuery failed";
  public static final String EXECUTE_AGGREGATION_QUERY_FAILED = "executeAggregationQuery failed";
  public static final String GET_TIMESTAMP_PRECISION_FAILED = "getTimestampPrecision failed";
  public static final String TEST_INSERT_TABLET_FAILED = "testInsertTablet failed";
  public static final String TEST_INSERT_TABLETS_FAILED = "testInsertTablets failed";
  public static final String TEST_INSERT_RECORD_FAILED = "testInsertRecord failed";
  public static final String TEST_INSERT_RECORDS_FAILED = "testInsertRecords failed";
  public static final String CREATE_SCHEMA_TEMPLATE_FAILED = "createSchemaTemplate failed";
  public static final String ADD_ALIGNED_MEASUREMENTS_IN_TEMPLATE_FAILED =
      "addAlignedMeasurementsInTemplate failed";
  public static final String ADD_ALIGNED_MEASUREMENT_IN_TEMPLATE_FAILED =
      "addAlignedMeasurementInTemplate failed";
  public static final String ADD_UNALIGNED_MEASUREMENTS_IN_TEMPLATE_FAILED =
      "addUnalignedMeasurementsInTemplate failed";
  public static final String ADD_UNALIGNED_MEASUREMENT_IN_TEMPLATE_FAILED =
      "addUnalignedMeasurementInTemplate failed";
  public static final String DELETE_NODE_IN_TEMPLATE_FAILED = "deleteNodeInTemplate failed";
  public static final String COUNT_MEASUREMENTS_IN_TEMPLATE_FAILED =
      "countMeasurementsInTemplate failed";
  public static final String IS_MEASUREMENT_IN_TEMPLATE_FAILED =
      "isMeasurementInTemplate failed";
  public static final String IS_PATH_EXIST_IN_TEMPLATE_FAILED =
      "isPathExistInTemplata failed";
  public static final String SHOW_MEASUREMENTS_IN_TEMPLATE_FAILED =
      "showMeasurementsInTemplate failed";
  public static final String SHOW_ALL_TEMPLATES_FAILED = "showAllTemplates failed";
  public static final String SHOW_PATHS_TEMPLATE_SET_ON_FAILED =
      "showPathsTemplateSetOn failed";
  public static final String SHOW_PATHS_TEMPLATE_USING_ON_FAILED =
      "showPathsTemplateUsingOn failed";
  public static final String SET_SCHEMA_TEMPLATE_ON_FAILED =
      "setSchemaTemplate [{}] on [{}] failed";
  public static final String UNSET_SCHEMA_TEMPLATE_ON_FAILED =
      "unsetSchemaTemplate [{}] on [{}] failed";
  public static final String DROP_SCHEMA_TEMPLATE_FAILED =
      "dropSchemaTemplate [{}] failed";
  public static final String CREATE_TIMESERIES_OF_SCHEMA_TEMPLATE_FAILED =
      "createTimeseriesOfSchemaTemplate {} failed";
  public static final String SET_TIMEZONE_FAILED = "setTimeZone to [{}] failed";
  public static final String FETCH_ALL_CONNECTIONS_FAILED = "fetchAllConnections failed";

  // SessionPool - unexpected error (error)
  public static final String UNEXPECTED_ERROR_IN_INSERT_TABLET =
      "unexpected error in insertTablet";
  public static final String UNEXPECTED_ERROR_IN_INSERT_ALIGNED_TABLET =
      "unexpected error in insertAlignedTablet";
  public static final String UNEXPECTED_ERROR_IN_INSERT_TABLETS =
      "unexpected error in insertTablets";
  public static final String UNEXPECTED_ERROR_IN_INSERT_ALIGNED_TABLETS =
      "unexpected error in insertAlignedTablets";
  public static final String UNEXPECTED_ERROR_IN_INSERT_RECORDS =
      "unexpected error in insertRecords";
  public static final String UNEXPECTED_ERROR_IN_INSERT_ALIGNED_RECORDS =
      "unexpected error in insertAlignedRecords";
  public static final String UNEXPECTED_ERROR_IN_INSERT_RECORD =
      "unexpected error in insertRecord";
  public static final String UNEXPECTED_ERROR_IN_INSERT_ALIGNED_RECORD =
      "unexpected error in insertAlignedRecord";
  public static final String UNEXPECTED_ERROR_IN_INSERT_RECORDS_OF_ONE_DEVICE =
      "unexpected error in insertRecordsOfOneDevice";
  public static final String UNEXPECTED_ERROR_IN_INSERT_STRING_RECORDS_OF_ONE_DEVICE =
      "unexpected error in insertStringRecordsOfOneDevice";
  public static final String UNEXPECTED_ERROR_IN_INSERT_ALIGNED_RECORDS_OF_ONE_DEVICE =
      "unexpected error in insertAlignedRecordsOfOneDevice";
  public static final String UNEXPECTED_ERROR_IN_INSERT_ALIGNED_STRING_RECORDS_OF_ONE_DEVICE =
      "unexpected error in insertAlignedStringRecordsOfOneDevice";
  public static final String UNEXPECTED_ERROR_IN_DELETE_DATA =
      "unexpected error in deleteData";
  public static final String UNEXPECTED_ERROR_IN_DELETE_TIMESERIES =
      "unexpected error in deleteTimeseries";
  public static final String UNEXPECTED_ERROR_IN_SET_STORAGE_GROUP =
      "unexpected error in setStorageGroup";
  public static final String UNEXPECTED_ERROR_IN_DELETE_STORAGE_GROUP =
      "unexpected error in deleteStorageGroup";
  public static final String UNEXPECTED_ERROR_IN_DELETE_STORAGE_GROUPS =
      "unexpected error in deleteStorageGroups";
  public static final String UNEXPECTED_ERROR_IN_CREATE_DATABASE =
      "unexpected error in createDatabase";
  public static final String UNEXPECTED_ERROR_IN_DELETE_DATABASE =
      "unexpected error in deleteDatabase";
  public static final String UNEXPECTED_ERROR_IN_DELETE_DATABASES =
      "unexpected error in deleteDatabases";
  public static final String UNEXPECTED_ERROR_IN_CREATE_TIMESERIES =
      "unexpected error in createTimeseries";
  public static final String UNEXPECTED_ERROR_IN_CREATE_ALIGNED_TIMESERIES =
      "unexpected error in createAlignedTimeseries";
  public static final String UNEXPECTED_ERROR_IN_CREATE_MULTI_TIMESERIES =
      "unexpected error in createMultiTimeseries";
  public static final String UNEXPECTED_ERROR_IN_CHECK_TIMESERIES_EXISTS =
      "unexpected error in checkTimeseriesExists";
  public static final String UNEXPECTED_ERROR_IN_EXECUTE_QUERY_STATEMENT =
      "unexpected error in executeQueryStatement";
  public static final String UNEXPECTED_ERROR_IN_EXECUTE_NON_QUERY_STATEMENT =
      "unexpected error in executeNonQueryStatement";
  public static final String UNEXPECTED_ERROR_IN_EXECUTE_RAW_DATA_QUERY =
      "unexpected error in executeRawDataQuery";
  public static final String UNEXPECTED_ERROR_IN_EXECUTE_LAST_DATA_QUERY =
      "unexpected error in executeLastDataQuery";
  public static final String UNEXPECTED_ERROR_IN_EXECUTE_AGGREGATION_QUERY =
      "unexpected error in executeAggregationQuery";
  public static final String UNEXPECTED_ERROR_IN_GET_TIMESTAMP_PRECISION =
      "unexpected error in getTimestampPrecision";
  public static final String UNEXPECTED_ERROR_IN_TEST_INSERT_TABLET =
      "unexpected error in testInsertTablet";
  public static final String UNEXPECTED_ERROR_IN_TEST_INSERT_TABLETS =
      "unexpected error in testInsertTablets";
  public static final String UNEXPECTED_ERROR_IN_TEST_INSERT_RECORD =
      "unexpected error in testInsertRecord";
  public static final String UNEXPECTED_ERROR_IN_TEST_INSERT_RECORDS =
      "unexpected error in testInsertRecords";
  public static final String UNEXPECTED_ERROR_IN_CREATE_SCHEMA_TEMPLATE =
      "unexpected error in createSchemaTemplate";
  public static final String UNEXPECTED_ERROR_IN_ADD_ALIGNED_MEASUREMENTS_IN_TEMPLATE =
      "unexpected error in addAlignedMeasurementsInTemplate";
  public static final String UNEXPECTED_ERROR_IN_ADD_ALIGNED_MEASUREMENT_IN_TEMPLATE =
      "unexpected error in addAlignedMeasurementInTemplate";
  public static final String UNEXPECTED_ERROR_IN_ADD_UNALIGNED_MEASUREMENTS_IN_TEMPLATE =
      "unexpected error in addUnalignedMeasurementsInTemplate";
  public static final String UNEXPECTED_ERROR_IN_ADD_UNALIGNED_MEASUREMENT_IN_TEMPLATE =
      "unexpected error in addUnalignedMeasurementInTemplate";
  public static final String UNEXPECTED_ERROR_IN_DELETE_NODE_IN_TEMPLATE =
      "unexpected error in deleteNodeInTemplate";
  public static final String UNEXPECTED_ERROR_IN_COUNT_MEASUREMENTS_IN_TEMPLATE =
      "unexpected error in countMeasurementsInTemplate";
  public static final String UNEXPECTED_ERROR_IN_IS_MEASUREMENT_IN_TEMPLATE =
      "unexpected error in isMeasurementInTemplate";
  public static final String UNEXPECTED_ERROR_IN_IS_PATH_EXIST_IN_TEMPLATE =
      "unexpected error in isPathExistInTemplate";
  public static final String UNEXPECTED_ERROR_IN_SHOW_MEASUREMENTS_IN_TEMPLATE =
      "unexpected error in showMeasurementsInTemplate";
  public static final String UNEXPECTED_ERROR_IN_SHOW_ALL_TEMPLATES =
      "unexpected error in showAllTemplates";
  public static final String UNEXPECTED_ERROR_IN_SHOW_PATHS_TEMPLATE_SET_ON =
      "unexpected error in showPathsTemplateSetOn";
  public static final String UNEXPECTED_ERROR_IN_SHOW_PATHS_TEMPLATE_USING_ON =
      "unexpected error in showPathsTemplateUsingOn";
  public static final String UNEXPECTED_ERROR_IN_SET_SCHEMA_TEMPLATE =
      "unexpected error in setSchemaTemplate";
  public static final String UNEXPECTED_ERROR_IN_UNSET_SCHEMA_TEMPLATE =
      "unexpected error in unsetSchemaTemplate";
  public static final String UNEXPECTED_ERROR_IN_DROP_SCHEMA_TEMPLATE =
      "unexpected error in dropSchemaTemplate";
  public static final String UNEXPECTED_ERROR_IN_CREATE_TIMESERIES_USING_SCHEMA_TEMPLATE =
      "unexpected error in createTimeseriesUsingSchemaTemplate";
  public static final String REDIRECT_TWICE_SUFFIX = " redirect twice, please try again.";

  private SessionMessages() {}
}
