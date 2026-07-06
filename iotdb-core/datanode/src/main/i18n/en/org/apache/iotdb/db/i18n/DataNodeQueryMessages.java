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

package org.apache.iotdb.db.i18n;

public final class DataNodeQueryMessages {

  // --- Common ---

  public static final String NO_MATCHED_DATABASE_PLEASE_CHECK_THE_PATH =
      "No matched database. Please check the path ";
  public static final String THIS_NODE_ISN_T_INSTANCE_OF_SCHEMAENTITYNODE =
      "This node isn't instance of SchemaEntityNode.";
  public static final String THIS_NODE_ISN_T_INSTANCE_OF_SCHEMAMEASUREMENTNODE =
      "This node isn't instance of SchemaMeasurementNode.";

  // --- Execution / Aggregation ---

  public static final String INVALID_AGGREGATION_FUNCTION =
      "Invalid Aggregation function: ";
  public static final String UNKNOWN_DATA_TYPE =
      "Unknown data type: ";
  public static final String COUNT_IF_WITH_SLIDINGWINDOW_IS_NOT_SUPPORTED_NOW =
      "COUNT_IF with slidingWindow is not supported now";
  public static final String TIME_DURATION_WITH_SLIDINGWINDOW_IS_NOT_SUPPORTED_NOW =
      "TIME_DURATION with slidingWindow is not supported now";
  public static final String MODE_WITH_SLIDINGWINDOW_IS_NOT_SUPPORTED_NOW =
      "MODE with slidingWindow is not supported now";
  public static final String INVALID_AGGREGATION_TYPE =
      "Invalid Aggregation Type: ";

  // --- Execution / Driver ---

  public static final String QUERYDATASOURCE_SHOULD_NEVER_BE_NULL =
      "QueryDataSource should never be null!";

  // --- Execution / Exchange ---

  public static final String SOURCE_HANDLE_FAILED_DUE_TO =
      "Source handle failed due to: ";
  public static final String SINK_FAILED_DUE_TO =
      "Sink failed due to";
  public static final String ISINKCHANNEL_FAILED_DUE_TO =
      "ISinkChannel failed due to";
  public static final String SINK_HANDLE_FAILED_DUE_TO =
      "Sink handle failed due to";
  public static final String MPPDATAEXCHANGEMANAGER_INIT_SUCCESSFULLY =
      "MPPDataExchangeManager init successfully";
  public static final String QUEUE_HAS_BEEN_DESTROYED =
      "queue has been destroyed";
  public static final String SINK_HANDLE_IS_BLOCKED =
      "Sink handle is blocked.";
  public static final String LOCALSINKCHANNEL_IS_ABORTED =
      "LocalSinkChannel is ABORTED.";
  public static final String ERROR_OCCURRED_WHEN_TRY_TO_ABORT_CHANNEL =
      "Error occurred when try to abort channel.";
  public static final String ERROR_OCCURRED_WHEN_TRY_TO_CLOSE_CHANNEL =
      "Error occurred when try to close channel.";
  public static final String SHUFFLESINKHANDLE_IS_ABORTED =
      "ShuffleSinkHandle is aborted.";
  public static final String UNSUPPORTED_TYPE_OF_SHUFFLE_STRATEGY =
      "Unsupported type of shuffle strategy";
  public static final String SINKCHANNEL_IS_ABORTED_OR_CLOSED =
      "SinkChannel is aborted or closed. ";
  public static final String THE_DATA_BLOCK_DOESN_T_EXIST_SEQUENCE_ID =
      "The data block doesn't exist. Sequence ID: ";
  public static final String THE_TSBLOCK_DOESNT_EXIST_SEQUENCE_ID_REMAINING =
      "The TsBlock doesn't exist. Sequence ID is {}, remaining map is {}";
  public static final String SINKCHANNEL_IS_ABORTED =
      "SinkChannel is aborted.";
  public static final String FAILED_TO_SEND_NEW_DATA_BLOCK_EVENT_ATTEMPT =
      "Failed to send new data block event, attempt times: {}";
  public static final String FAILED_TO_SEND_END_OF_DATA_BLOCK_EVENT =
      "Failed to send end of data block event, attempt times: {}";
  public static final String FAILED_TO_SEND_END_OF_DATA_BLOCK_EVENT_2 =
      "Failed to send end of data block event after all retry";
  public static final String SOURCE_HANDLE_IS_BLOCKED =
      "Source handle is blocked.";
  public static final String RESERVED_DATA_BLOCK_SIZE_IS_NULL =
      "Reserved data block size is null.";
  public static final String DATA_BLOCK_SIZE_IS_NULL =
      "Data block size is null.";
  public static final String SOURCE_HANDLE_IS_ABORTED =
      "Source handle is aborted.";
  public static final String SOURCEHANDLE_IS_CLOSED =
      "SourceHandle is closed.";

  // --- Execution / Executor ---

  public static final String EXECUTE_FRAGMENTINSTANCE_IN_CONSENSUSGROUP_FAILED =
      "Execute FragmentInstance in ConsensusGroup {} failed.";
  public static final String EXECUTE_FRAGMENTINSTANCE_IN_QUERYEXECUTOR_FAILED =
      "Execute FragmentInstance in QueryExecutor failed.";
  public static final String FAILED_IN_THE_WRITE_API_EXECUTING_THE_CONSENSUS =
      "Failed in the write API executing the consensus layer due to: ";

  // --- Execution / Fragment ---

  public static final String UNKNOWN_EXCEPTION =
      "[Unknown exception]: ";
  public static final String WAIT_MS_FOR_ALL_DRIVERS_CLOSED =
      "Wait {}ms for all Drivers closed";
  public static final String EXCEPTION_HAPPENED_WHEN_EXECUTING_UDTF =
      "Exception happened when executing UDTF: ";
  public static final String ERROR_WHEN_CREATE_FRAGMENTINSTANCEEXECUTION =
      "error when create FragmentInstanceExecution.";
  public static final String EXECUTE_ERROR_CAUSED_BY =
      "Execute error caused by ";

  // --- Execution / Memory ---

  public static final String FREE_MORE_MEMORY_THAN_HAS_BEEN_RESERVED =
      "Free more memory than has been reserved.";
  public static final String ESTIMATED_MODS_TREE_SIZE_DECREASED =
      "Estimated mods tree size decreased from %d to %d for TsFile %s.";
  public static final String RESULT_SET_COLUMN_METADATA_MEMORY_NOT_ENOUGH =
      "Not enough memory while analyzing metadata for query result columns. "
          + "The result set has too many columns. "
          + "Before the failure, IoTDB had matched %,d source columns for result-column "
          + "expansion, expanded %,d source columns, and generated %,d result-set columns. "
          + "%s"
          + "Current series pagination is %s. "
          + "Use SLIMIT/SOFFSET to reduce returned series%s, narrow the path pattern, "
          + "or increase query memory%s. "
          + "Memory details: source-column memory for result expansion %s, "
          + "generated-result-column memory %s, requested this time %s, current free memory %s. "
          + "Original error: %s";
  public static final String RESULT_SET_COLUMNS_EXCEED_MEMORY_CAPACITY =
      "The matched source columns exceed the estimated current memory capacity by "
          + "at least %,d columns. ";
  public static final String SCHEMA_FETCH_METADATA_MEMORY_NOT_ENOUGH =
      "Not enough memory while fetching metadata for query analysis. "
          + "The result set may have too many columns. "
          + "Before the failure, IoTDB had deserialized %,d time-series columns from schema "
          + "fetch results. Schema fetch memory may be reserved before safely deserializing "
          + "the whole fetched metadata, so this count can be lower than the matched schema "
          + "columns. %s"
          + "Current series pagination is %s. "
          + "Use SLIMIT/SOFFSET to reduce returned series%s, narrow the path pattern, "
          + "or increase query memory%s. "
          + "Memory details: fetched schema tree estimated memory %s, "
          + "fetched schema tree reserved memory %s, requested this time %s, "
          + "current free memory %s. Original error: %s";
  public static final String SCHEMA_FETCH_COLUMNS_EXCEED_MEMORY_CAPACITY =
      "The fetched schema columns exceed the estimated current memory capacity by "
          + "at least %,d columns. ";
  public static final String USE_ALIGN_BY_DEVICE_TO_REDUCE_RESULT_COLUMNS =
      ", use ALIGN BY DEVICE to reduce cross-device result columns";
  public static final String BY_AT_LEAST_MEMORY_SIZE = " by at least %s";
  public static final String FOR_QUERY_ENGINE_OPERATOR_MEMORY_POOL =
      " for the query engine/operator memory pool";
  public static final String SERIES_PAGINATION_FOR_DIAGNOSTICS = "SLIMIT=%s, SOFFSET=%,d";
  public static final String NOT_SET = "not set";
  public static final String UNKNOWN = "unknown";

  // --- Execution / Operator ---

  public static final String UNKNOWN_DATA_TYPE_2 =
      "Unknown data type ";
  public static final String ERROR_OCCURRED_WHEN_LOGGING_INTERMEDIATE_RESULT_OF_ANALYZE =
      "Error occurred when logging intermediate result of analyze.";

  // --- Execution / Operator / Process ---

  public static final String GETWRITTENCOUNT_MEASUREMENT_IS_NOT_SUPPORTED =
      "getWrittenCount(measurement) is not supported";
  public static final String GETWRITTENCOUNT_IS_NOT_SUPPORTED =
      "getWrittenCount() is not supported";
  public static final String THE_MEMORY_THRESHOLD_MUST_BE_GREATER_THAN_0 =
      "The memory threshold must be greater than 0.";
  public static final String FAILED_TO_CREATE_DIRECTORIES =
      "Failed to create directories: ";
  public static final String TARGET_FILE_ALREADY_EXISTS =
      "Target file already exists: ";
  public static final String FAILED_TO_CREATE_FILE =
      "Failed to create file: ";
  public static final String DATA_TYPE_OF_TARGET_TIME_COLUMN_IS_NOT =
      "Data type of target time column is not TIMESTAMP";
  public static final String DUPLICATE_COLUMN_NAMES_IN_QUERY_DATASET =
      "Duplicate column names in query dataset.";
  public static final String SOME_SPECIFIED_TAG_COLUMNS_ARE_NOT_EXIST_IN =
      "Some specified tag columns are not exist in query dataset.";
  public static final String NUMBER_OF_FIELD_COLUMNS_SHOULD_BE_LARGER_THAN =
      "Number of field columns should be larger than 0.";
  public static final String ALL_CHILD_SHOULD_HAVE_SAME_TIME_COLUMN_RESULT =
      "All child should have same time column result!";
  public static final String LAST_READ_RESULT_SHOULD_ONLY_HAVE_ONE_RECORD =
      "last read result should only have one record";

  // --- Execution / Operator / Schema ---

  public static final String FAILED_TO_CONVERT_NODE_PATH_TO_PARTIALPATH =
      "Failed to convert node path to PartialPath {}";

  // --- Execution / Operator / Source ---

  public static final String ERROR_OCCURS_WHEN_SCANNING_ACTIVE_TIME_SERIES =
      "Error occurs when scanning active time series.";
  public static final String ERROR_WHILE_SCANNING_THE_FILE =
      "Error while scanning the file";
  public static final String ERROR_HAPPENED_WHILE_SCANNING_THE_FILE =
      "Error happened while scanning the file";
  public static final String ALL_CACHED_CHUNKS_SHOULD_BE_CONSUMED_FIRST =
      "all cached chunks should be consumed first";
  public static final String OVERLAPPED_DATA_SHOULD_BE_CONSUMED_FIRST =
      "overlapped data should be consumed first";
  public static final String NO_MORE_BATCH_DATA =
      "No more batch data";
  public static final String GETALLSATISFIEDPAGEDATA_SHOULDN_T_BE_CALLED_HERE =
      "getAllSatisfiedPageData() shouldn't be called here";
  public static final String GETPAGEREADER_SHOULDN_T_BE_CALLED_HERE =
      "getPageReader() shouldn't be called here";
  public static final String UNSUPPORTED_COLUMN_TYPE =
      "Unsupported column type: ";
  public static final String FAIL_TO_CLOSE_CTEDATAREADER =
      "Fail to close CteDataReader";
  public static final String UNKNOWN_TABLE =
      "Unknown table: ";
  public static final String FAILED_TO_CLOSE_READER_IN_TABLEDISKUSAGESUPPLIER =
      "Failed to close reader in TableDiskUsageSupplier";
  public static final String UNSUPPORTED_CATEGORY =
      "Unsupported category: ";
  public static final String CURRENT_DEVICE_ENTRY_IN_TABLESCANOPERATOR_IS_EMPTY =
      "Current device entry in TableScanOperator is empty";
  public static final String UNEXPECTED_END_OF_EXTERNAL_TSFILE_DEVICE_TASK_READER_AT_DEVICE_INDEX =
      "Unexpected end of external TsFile device task reader at device index ";
  public static final String
      EXTERNAL_TSFILE_DEVICE_TASK_READER_IS_NOT_ALIGNED_WITH_DEVICE_ENTRIES =
          "External TsFile device task reader is not aligned with device entries at index %d:"
              + " expected %s but got %s";
  public static final String FAILED_TO_UPDATE_EXTERNAL_TSFILE_DEVICE_RESOURCES =
      "Failed to update external TsFile device resources";
  public static final String SCHEMA_FILTER_TYPE_IS_NOT_SUPPORTED =
      "The schema filter type %s is not supported";
  public static final String
      ATTRIBUTE_FILTER_IS_NOT_SUPPORTED_FOR_EXTERNAL_TSFILE_DEVICE_FILTERING =
          "Attribute filter is not supported for external TsFile device filtering";

  // --- Execution / Operator / Window ---

  public static final String UNSUPPORTED_INFERENCE_WINDOW_TYPE =
      "Unsupported inference window type: ";

  // --- Execution / Schedule ---

  public static final String EXECUTOR_FAILED_TO_POLL_DRIVER_TASK_FROM_QUEUE =
      "Executor {} failed to poll driver task from queue";
  public static final String DRIVERTASK_SHOULD_NEVER_BE_NULL =
      "DriverTask should never be null";
  public static final String EXECUTEFAILED =
      "[ExecuteFailed]";
  public static final String EXECUTOR_EXITS_BECAUSE_IT_IS_CLOSED =
      "Executor {} exits because it is closed.";
  public static final String CLEAR_DRIVERTASK_FAILED =
      "Clear DriverTask failed";


  // --- Execution / Warnings ---

  public static final String CODE_IS_NEGATIVE =
      "code is negative";

  // --- Metric ---

  public static final String UNSUPPORTED_STAGE_IN_TREE_MODEL =
      "Unsupported stage in tree model: ";
  public static final String UNSUPPORTED_STAGE_IN_TABLE_MODEL =
      "Unsupported stage in table model: ";

  // --- Plan ---

  public static final String TOPOLOGY_LATEST_VIEW_FROM_CONFIG_NODE =
      "[Topology] latest view from config-node for myself({}): {}";
  public static final String EXPIRED_QUERIES_INFO_CLEAR_THREAD_IS_SUCCESSFULLY_STARTED =
      "Expired-Queries-Info-Clear thread is successfully started.";
  public static final String COST_MS =
      "Cost: {} ms, {}";

  // --- Plan / Analyze ---

  public static final String COMPUTEDATAPARTITIONPARAMS_FOR =
      "computeDataPartitionParams for ";
  public static final String UNSUPPORTED_OPERATOR =
      "Unsupported operator: ";
  public static final String UNSUPPORTED_EXPRESSION =
      "Unsupported expression: ";
  public static final String ONLY_SUPPORT_AND_OPERATOR_IN_DELETION =
      "Only support AND operator in deletion";
  public static final String LEFT_HAND_EXPRESSION_IS_NOT_AN_IDENTIFIER =
      "Left hand expression is not an identifier: ";
  public static final String THE_LEFT_HAND_VALUE_MUST_BE_AN_IDENTIFIER =
      "The left hand value must be an identifier: ";
  public static final String THE_TABLE_S_DOES_NOT_CONTAIN_A_TIME_COLUMN =
      "The table '%s' does not contain a time column";
  public static final String THE_OPERATOR_OF_TAG_PREDICATE_MUST_BE_FOR =
      "The operator of tag predicate must be '=' for ";
  public static final String ONLY_TIME_FILTERS_ARE_SUPPORTED_IN_LAST_QUERY =
      "Only time filters are supported in LAST query";
  public static final String VIEWS_CANNOT_BE_USED_IN_GROUP_BY_TAGS =
      "Views cannot be used in GROUP BY TAGS query yet.";
  public static final String ONLY_TIME_FILTERS_ARE_SUPPORTED_IN_GROUP_BY =
      "Only time filters are supported in GROUP BY TAGS query";
  public static final String UNSUPPORTED_WINDOW_TYPE =
      "Unsupported window type";
  public static final String AGGREGATION_EXPRESSION_SHOULDN_T_EXIST_IN_GROUP_BY =
      "Aggregation expression shouldn't exist in group by clause";
  public static final String ONLY_SUPPORT_NUMERIC_TYPE_WHEN_DELTA_0 =
      "Only support numeric type when delta != 0";
  public static final String ONLY_SUPPORT_BOOLEAN_TYPE_IN_PREDICT_OF_GROUP =
      "Only support boolean type in predict of group by series";
  public static final String GROUP_BY_MONTH_DOESN_T_SUPPORT_ORDER_BY =
      "Group by month doesn't support order by time desc now.";
  public static final String NO_RUNNING_DATANODES =
      "no Running DataNodes";
  public static final String AN_ERROR_OCCURRED_WHEN_SERIALIZING_PATTERN_TREE =
      "An error occurred when serializing pattern tree";
  public static final String EXPRESSION_IN_GROUP_BY_SHOULD_INDICATE_ONE_VALUE =
      "Expression in group by should indicate one value";
  public static final String EXPRESSION_IN_ORDER_BY_SHOULD_INDICATE_ONE_VALUE =
      "Expression in order by should indicate one value";
  public static final String SHOULDN_T_ATTACH_HERE =
      "shouldn't attach here";
  public static final String SELECT_INTO_THE_I_OF_SHOULD_BE_AN =
      "select into: the i of ${i} should be an integer.";
  public static final String FAILED_TO_GET_DATABASE_MAP =
      "Failed to get database Map";
  public static final String UNKNOWN_DATABASE = "Unknown database %s";
  public static final String LOAD_ANALYSIS_STAGE_ALL_TSFILES_HAVE_BEEN_ANALYZED =
      "Load - Analysis Stage: all tsfiles have been analyzed.";
  public static final String ASYNC_LOAD_HAS_FAILED_AND_IS_NOW_TRYING =
      "Async Load has failed, and is now trying to load sync";
  public static final String TSFILE_IS_EMPTY =
      "TsFile {} is empty.";
  public static final String THE_ENCRYPTION_WAY_OF_THE_TSFILE_IS_NOT =
      "The encryption way of the TsFile is not supported.";
  public static final String EMPTY_FILE_DETECTED_WILL_SKIP_LOADING_THIS_FILE =
      "Empty file detected, will skip loading this file: {}";
  public static final String AUTO_CREATE_OR_VERIFY_SCHEMA_ERROR =
      "Auto create or verify schema error.";
  public static final String LOAD_TSFILE_DEVICE_SCHEMA_MISSING_AUTO_CREATE_DISABLED =
      "Device %s does not exist in IoTDB and can not be created. Please check whether auto-create-schema is enabled.";
  public static final String LOAD_TSFILE_MEASUREMENT_SCHEMA_MISSING_AUTO_CREATE_DISABLED =
      "Measurement %s does not exist in IoTDB and can not be created. Please check whether auto-create-schema is enabled.";
  public static final String PIPE_GENERATED_LOAD_TSFILE_WAITING_FOR_SCHEMA_METADATA =
      "Pipe generated LoadTsFile is waiting for schema metadata to be transferred. Detail: %s";
  public static final String FAILED_TO_FIND_TAG_COLUMN_MAPPING_FOR_TABLE =
      "Failed to find tag column mapping for table {}";
  public static final String AUTO_CREATE_DATABASE_FAILED_BECAUSE =
      "Auto create database failed because: ";

  // --- Plan / Execution ---

  public static final String REACHMAXRETRYCOUNT =
      "[ReachMaxRetryCount]";
  public static final String ERROR_WHEN_EXECUTING_QUERY =
      "error when executing query. {}";
  public static final String WAITBEFORERETRY_WAIT_MS =
      "[WaitBeforeRetry] wait {}ms.";
  public static final String INTERRUPTED_WHEN_WAITING_RETRY =
      "interrupted when waiting retry";
  public static final String RETRY_RETRY_COUNT_IS =
      "[Retry] retry count is: {}";
  public static final String RESULTHANDLEABORTED =
      "[ResultHandleAborted]";
  public static final String UNSUPPORTED_DATABASE_PROPERTY_KEY =
      "Unsupported database property key: ";
  public static final String A_TABLE_CANNOT_HAVE_MORE_THAN_ONE_TIME =
      "A table cannot have more than one time column";
  public static final String THE_TIME_COLUMN_S_TYPE_SHALL_BE_TIMESTAMP =
      "The time column's type shall be 'timestamp'.";
  public static final String THE_TABLE_S_OLD_NAME_SHALL_NOT_BE =
      "The table's old name shall not be equal to the new one.";
  public static final String ADDING_TIME_COLUMN_IS_NOT_SUPPORTED =
      "Adding TIME column is not supported.";
  public static final String THE_COLUMN_S_OLD_NAME_SHALL_NOT_BE =
      "The column's old name shall not be equal to the new one.";
  public static final String DUPLICATED_PROPERTY =
      "Duplicated property: ";
  public static final String TABLE_PROPERTY =
      "Table property '";
  public static final String UNKNOWN_TYPE =
      "Unknown type: %s";
  public static final String FAILED_TO_CHECK_CONFIG_ITEM_PERMISSION =
      "Failed to check config item permission";
  public static final String CONFIGTASK_IS_NOT_IMPLEMENTED_FOR =
      "ConfigTask is not implemented for: ";
  public static final String FAILED_TO_GET_EXECUTABLE_FOR_UDF_USING_URI =
      "Failed to get executable for UDF({}) using URI: {}.";
  public static final String FAILED_TO_DROP_FUNCTION =
      "[{}] Failed to drop function {}.";
  public static final String FAILED_TO_DROP_TRIGGER =
      "[{}] Failed to drop trigger {}.";
  public static final String CANNOT_REMOVE_INVALID_NODEIDS =
      "Cannot remove invalid nodeIds:{}";
  public static final String STARTING_TO_REMOVE_DATANODE_WITH_NODEIDS =
      "Starting to remove DataNode with nodeIds: {}";
  public static final String START_TO_REMOVE_DATANODE_REMOVED_DATANODES_ENDPOINT =
      "Start to remove datanode, removed DataNodes endpoint: {}";
  public static final String SUBMIT_REMOVE_DATANODES_RESULT =
      "Submit Remove DataNodes result {} ";
  public static final String STARTING_TO_REMOVE_CONFIGNODE_WITH_NODE_ID =
      "Starting to remove ConfigNode with node-id {}";
  public static final String CONFIGNODE_IS_REMOVED =
      "ConfigNode: {} is removed.";
  public static final String STARTING_TO_REMOVE_AINODE =
      "Starting to remove AINode";
  public static final String REMOVE_AINODE_FAILED_BECAUSE_THERE_IS_NO_AINODE =
      "Remove AINode failed because there is no AINode in the cluster.";
  public static final String AINODE_IN_THE_CLUSTER_IS_REMOVED =
      "AINode in the cluster is removed.";
  public static final String FAILED_TO_HANDLETRANSFERCONFIGPLAN_STATUS_IS =
      "Failed to handleTransferConfigPlan, status is {}.";
  public static final String FAILED_TO_FETCHTABLES_STATUS_IS =
      "Failed to fetchTables, status is {}.";
  public static final String FAILED_TO_HANDLEPIPECONFIGCLIENTEXIT_STATUS_IS =
      "Failed to handlePipeConfigClientExit, status is {}.";
  public static final String FAILED_TO_HANDLEPIPECONFIGCLIENTEXIT =
      "Failed to handlePipeConfigClientExit.";
  public static final String NOT_SUPPORT_CURRENT_STATEMENT =
      "Not support current statement";
  public static final String WRONG_REQUEST_TYPE =
      "Wrong request type";
  public static final String WRONG_UNIT_TYPE =
      "Wrong unit type";

  // --- Plan / Expression ---

  public static final String INVALID_EXPRESSION_TYPE =
      "Invalid expression type: ";
  public static final String UNSUPPORTED_EXPRESSION_TYPE =
      "Unsupported expression type: ";
  public static final String FUNCTION_CAST_MUST_SPECIFY_A_TARGET_DATA_TYPE =
      "Function Cast must specify a target data type.";
  public static final String FUNCTION_REPLACE_MUST_SPECIFY_FROM_AND_TO_COMPONENT =
      "Function REPLACE must specify from and to component.";
  public static final String PLEASE_ENSURE_INPUT_IS_CORRECT =
      "please ensure input[%s] is correct";
  public static final String CASE_EXPRESSION_CANNOT_BE_USED_WITH_NON_MAPPABLE =
      "CASE expression cannot be used with non-mappable UDF";
  public static final String UNSUPPORTED_TRANSFORMER_ACCESS_STRATEGY =
      "Unsupported transformer access strategy";
  public static final String AGGREGATE_FUNCTIONS_ARE_NOT_SUPPORTED_IN_WHERE_CLAUSE =
      "aggregate functions are not supported in WHERE clause";
  public static final String IS_NULL_CANNOT_BE_PUSHED_DOWN =
      "IS NULL cannot be pushed down";
  public static final String TIMESTAMP_DOES_NOT_SUPPORT_IS_NULL_IS_NOT =
      "TIMESTAMP does not support IS NULL/IS NOT NULL";
  public static final String TIMESTAMP_DOES_NOT_SUPPORT_LIKE_NOT_LIKE =
      "TIMESTAMP does not support LIKE/NOT LIKE";
  public static final String TIMESTAMP_DOES_NOT_SUPPORT_REGEXP_NOT_REGEXP =
      "TIMESTAMP does not support REGEXP/NOT REGEXP";
  public static final String GROUPBYTIME_FILTER_CANNOT_EXIST_IN_VALUE_FILTER =
      "GroupByTime filter cannot exist in value filter.";
  public static final String IS_NULL_CAN_BE_PUSHED_DOWN =
      "IS NULL can be pushed down";
  public static final String GROUP_BY_TIME_CANNOT_BE_REVERSED =
      "GROUP BY TIME cannot be reversed";

  // --- Plan / Optimization ---

  public static final String UNEXPECTED_PLAN_NODE =
      "Unexpected plan node: ";
  public static final String UNEXPECTED_PATH_TYPE =
      "unexpected path type";
  public static final String SOURCEPATH_MUST_BE_MEASUREMENTPATH_OR_ALIGNEDPATH =
      "sourcePath must be MeasurementPath or AlignedPath";

  // --- Plan / Parser ---

  public static final String DATATYPE_MUST_BE_DECLARED =
      "datatype must be declared";
  public static final String UNSUPPORTED_ENCODING =
      "Unsupported encoding: %s";
  public static final String UNSUPPORTED_COMPRESSION =
      "Unsupported compression: %s";
  public static final String UNSUPPORTED_ENCODING_2 =
      "unsupported encoding: %s";
  public static final String UNSUPPORTED_COMPRESSOR =
      "unsupported compressor: %s";
  public static final String CREATE_ALIGNED_TIMESERIES_PROPERTY_IS_NOT_SUPPORTED_YET =
      "create aligned timeseries: property is not supported yet.";
  public static final String UNSUPPORTED_COMPRESSOR_2 =
      "Unsupported compressor: %s";
  public static final String PROPERTY_IS_UNSUPPORTED_YET =
      "property %s is unsupported yet.";
  public static final String THE_TIMESERIES_SHALL_NOT_BE_ROOT =
      "The timeSeries shall not be root.";
  public static final String UNSUPPORTED_DATATYPE =
      "unsupported datatype: %s";
  public static final String UNEXPECTED_FILTER_KEY =
      "unexpected filter key";
  public static final String URI_IS_EMPTY_PLEASE_SPECIFY_THE_URI =
      "URI is empty, please specify the URI.";
  public static final String INVALID_URI =
      "Invalid URI: %s";
  public static final String TRIGGER_DOES_NOT_SUPPORT_DELETE_AS_TRIGGER_EVENT =
      "Trigger does not support DELETE as TRIGGER_EVENT for now.";
  public static final String PLEASE_SPECIFY_TRIGGER_TYPE_STATELESS_OR_STATEFUL =
      "Please specify trigger type: STATELESS or STATEFUL.";
  public static final String RENAMING_VIEW_IS_NOT_SUPPORTED =
      "Renaming view is not supported.";
  public static final String VIEW_DOESN_T_SUPPORT_ALIAS =
      "View doesn't support alias.";
  public static final String MODELID_SHOULD_BE_2_64_CHARACTERS =
      "ModelId should be 2-64 characters";
  public static final String MODELID_SHOULD_NOT_START_WITH =
      "ModelId should not start with '_'";
  public static final String MODELID_CAN_ONLY_CONTAIN_LETTERS_NUMBERS_AND_UNDERSCORES =
      "ModelId can only contain letters, numbers, and underscores";
  public static final String DEVICE_ID_SHOULD_BE_CPU_OR_INTEGER =
      "Device id should be 'cpu' or integer";
  public static final String DATA_SHOULD_NOT_BE_SET_FOR_MODEL_TRAINING =
      "data should not be set for model training";
  public static final String DUPLICATED_GROUP_BY_KEY_LEVEL =
      "duplicated group by key: LEVEL";
  public static final String DUPLICATED_GROUP_BY_KEY_TAGS =
      "duplicated group by key: TAGS";
  public static final String UNKNOWN_GROUP_BY_TYPE =
      "Unknown GROUP BY type.";
  public static final String DUPLICATE_ALIAS_IN_SELECT_CLAUSE =
      "duplicate alias in select clause";
  public static final String CONSTANT_OPERAND_IS_NOT_ALLOWED =
      "Constant operand is not allowed: ";
  public static final String THE_TIME_WINDOWS_MAY_EXCEED_10000_PLEASE_ENSURE =
      "The time windows may exceed 10000, please ensure your input.";
  public static final String START_TIME_SHOULD_BE_SMALLER_THAN_ENDTIME_IN =
      "Start time should be smaller than endTime in GroupBy";
  public static final String KEEP_THRESHOLD_IN_GROUP_BY_CONDITION_SHOULD_BE =
      "Keep threshold in group by condition should be set";
  public static final String DUPLICATED_KEY_IN_GROUP_BY_TAGS =
      "duplicated key in GROUP BY TAGS: ";
  public static final String UNKNOWN_FILL_TYPE =
      "Unknown FILL type.";
  public static final String UNSUPPORTED_CONSTANT_VALUE_IN_FILL =
      "Unsupported constant value in FILL: ";
  public static final String OUT_OF_RANGE_LIMIT_N_N_SHOULD_BE =
      "Out of range. LIMIT <N>: N should be Int64.";
  public static final String LIMIT_N_N_SHOULD_BE_GREATER_THAN_0 =
      "LIMIT <N>: N should be greater than 0.";
  public static final String OFFSET_OFFSETVALUE_OFFSETVALUE_SHOULD_0 =
      "OFFSET <OFFSETValue>: OFFSETValue should >= 0.";
  public static final String OUT_OF_RANGE_SLIMIT_SN_SN_SHOULD_BE =
      "Out of range. SLIMIT <SN>: SN should be Int32.";
  public static final String SLIMIT_SN_SN_SHOULD_BE_GREATER_THAN_0 =
      "SLIMIT <SN>: SN should be greater than 0.";
  public static final String SOFFSET_SOFFSETVALUE_SOFFSETVALUE_SHOULD_0 =
      "SOFFSET <SOFFSETValue>: SOFFSETValue should >= 0.";
  public static final String ONE_ROW_SHOULD_ONLY_HAVE_ONE_TIME_VALUE =
      "One row should only have one time value";
  public static final String INSERTSTATEMENT_SHOULD_CONTAIN_AT_LEAST_ONE_MEASUREMENT =
      "InsertStatement should contain at least one measurement";
  public static final String NEED_TIMESTAMPS_WHEN_INSERT_MULTI_ROWS =
      "need timestamps when insert multi rows";
  public static final String CAN_NOT_PARSE_TO_TIME =
      "Can not parse %s to time";
  public static final String PATH_CAN_NOT_START_WITH_ROOT_IN_SELECT =
      "Path can not start with root in select clause.";
  public static final String INPUT_TIMESTAMP_CANNOT_BE_EMPTY =
      "input timestamp cannot be empty";
  public static final String NOT_SUPPORT_FOR_THIS_ALIAS_PLEASE_ENCLOSE_IN =
      "Not support for this alias, Please enclose in back quotes.";
  public static final String STATEMENT_NEEDS_TARGET_PATHS =
      "Statement needs target paths";
  public static final String THE_DATATYPE_OF_TIMESTAMP_SHOULD_BE_LONG =
      "The datatype of timestamp should be LONG.";
  public static final String ATTRIBUTES_OF_FUNCTIONS_SHOULD_BE_QUOTED_WITH_OR =
      "Attributes of functions should be quoted with '' or \"\"";
  public static final String UNSUPPORTED_CONSTANT_VALUE =
      "Unsupported constant value: ";
  public static final String UNSUPPORTED_CONSTANT_OPERAND =
      "Unsupported constant operand: ";
  public static final String UNKNOWN_SYSTEM_STATUS_IN_SET_SYSTEM_COMMAND =
      "Unknown system status in set system command.";
  public static final String DEVICE_TEMPLATE_ALIAS_IS_NOT_SUPPORTED_YET =
      "Device Template: alias is not supported yet.";
  public static final String DEVICE_TEMPLATE_PROPERTY_IS_NOT_SUPPORTED_YET =
      "Device Template: property is not supported yet.";
  public static final String DEVICE_TEMPLATE_TAG_IS_NOT_SUPPORTED_YET =
      "Device Template: tag is not supported yet.";
  public static final String DEVICE_TEMPLATE_ATTRIBUTE_IS_NOT_SUPPORTED_YET =
      "Device Template: attribute is not supported yet.";
  public static final String EXPECTING_DATATYPE =
      "Expecting datatype";
  public static final String NOT_SUPPORT_FOR_THIS_SQL_IN_DROP_PIPE =
      "Not support for this sql in DROP PIPE, please enter pipename.";
  public static final String NOT_SUPPORT_FOR_THIS_SQL_IN_START_PIPE =
      "Not support for this sql in START PIPE, please enter pipename.";
  public static final String NOT_SUPPORT_FOR_THIS_SQL_IN_STOP_PIPE =
      "Not support for this sql in STOP PIPE, please enter pipename.";
  public static final String NOT_SUPPORT_FOR_THIS_SQL_IN_ALTER_TOPIC =
      "Not support for this sql in ALTER TOPIC, please enter topicName.";
  public static final String GET_REGION_ID_STATEMENT_EXPRESSION_MUST_BE_A =
      "Get region id statement‘ expression must be a time expression";
  public static final String WRONG_SPACE_QUOTA_TYPE =
      "Wrong space quota type: ";
  public static final String PLEASE_SET_THE_NUMBER_OF_DEVICES_GREATER_THAN =
      "Please set the number of devices greater than 0";
  public static final String PLEASE_SET_THE_NUMBER_OF_TIMESERIES_GREATER_THAN =
      "Please set the number of timeseries greater than 0";
  public static final String CANNOT_SET_THROTTLE_QUOTA_FOR_USER_ROOT =
      "Cannot set throttle quota for user root.";
  public static final String PLEASE_SET_THE_NUMBER_OF_REQUESTS_GREATER_THAN =
      "Please set the number of requests greater than 0";
  public static final String PLEASE_SET_THE_NUMBER_OF_CPU_GREATER_THAN =
      "Please set the number of cpu greater than 0";
  public static final String PLEASE_SET_THE_SIZE_GREATER_THAN_0 =
      "Please set the size greater than 0";
  public static final String PLEASE_SET_THE_DISK_SIZE_GREATER_THAN_0 =
      "Please set the disk size greater than 0";
  public static final String THERE_SHOULD_BE_ONLY_ONE_WINDOW_IN_CALL =
      "There should be only one window in CALL INFERENCE.";
  public static final String THE_CREATETABLEVIEW_IS_UNSUPPORTED_IN_TREE_SQL_DIALECT =
      "The 'CreateTableView' is unsupported in tree sql-dialect.";
  public static final String CURRENTLY_OTHER_EXPRESSIONS_ARE_NOT_SUPPORTED =
      "Currently other expressions are not supported";
  public static final String ALIGN_DESIGNATION_INCORRECT_AT =
      "Align designation incorrect at: ";

  // --- Plan / Relational / Analyzer ---

  public static final String COLUMN_NOT_IN_GROUP_BY_CLAUSE =
      "Column %s not in GROUP BY clause";
  public static final String DATABASE_IS_NOT_SPECIFIED_FOR_INSERT =
      "database is not specified for insert:";
  public static final String IDENTIFIER_NOT_ALLOWED_IN_THIS_CONTEXT =
      "<identifier>.* not allowed in this context";
  public static final String UNKNOWN_SIGN =
      "Unknown sign: ";
  public static final String DECIMALLITERAL_IS_NOT_SUPPORTED_YET =
      "DecimalLiteral is not supported yet.";
  public static final String GENERICLITERAL_IS_NOT_SUPPORTED_YET =
      "GenericLiteral is not supported yet.";
  public static final String DISTINCT_IS_NOT_SUPPORTED_FOR_NON_AGGREGATION_FUNCTIONS =
      "DISTINCT is not supported for non-aggregation functions";
  public static final String UNEXPECTED_PATTERN_RECOGNITION_FUNCTION =
      "unexpected pattern recognition function ";
  public static final String THE_INPUT_ARGUMENT_DOES_NOT_EXIST =
      "the input argument does not exist";
  public static final String MATCH_NUMBER_PATTERN_RECOGNITION_FUNCTION_TAKES_NO_ARGUMENTS =
      "MATCH_NUMBER pattern recognition function takes no arguments";
  public static final String UNEXPECTED_NAVIGATION_ANCHOR =
      "Unexpected navigation anchor: ";
  public static final String UNEXPECTED_MODE =
      "Unexpected mode: ";
  public static final String QUERY_TAKES_NO_PARAMETERS =
      "Query takes no parameters";
  public static final String NO_VALUE_PROVIDED_FOR_PARAMETER =
      "No value provided for parameter";
  public static final String CANNOT_EXTRACT_FROM =
      "Cannot extract from %s";
  public static final String UNKNOWN_IS_NOT_A_VALID_TYPE =
      "UNKNOWN is not a valid type";
  public static final String CANNOT_CAST_TO =
      "Cannot cast %s to %s";
  public static final String WINDOW_FRAME_START_CANNOT_BE_UNBOUNDED_FOLLOWING =
      "Window frame start cannot be UNBOUNDED FOLLOWING";
  public static final String WINDOW_FRAME_END_CANNOT_BE_UNBOUNDED_PRECEDING =
      "Window frame end cannot be UNBOUNDED PRECEDING";
  public static final String UNSUPPORTED_FRAME_TYPE =
      "Unsupported frame type: ";
  public static final String COLUMNS_ONLY_SUPPORT_TO_BE_USED_IN_SELECT =
      "Columns only support to be used in SELECT and WHERE clause";
  public static final String TYPE_MISMATCH_FMT =
      "%s: %s vs %s";
  public static final String UNKNOWN_PATTERN_RECOGNITION_FUNCTION =
      "Unknown pattern recognition function: ";
  public static final String CANNOT_ACCESS_PREANALYZED_TYPES =
      "Cannot access preanalyzed types";
  public static final String CANNOT_ACCESS_RESOLVED_WINDOWS =
      "Cannot access resolved windows";
  public static final String REFERENCE_IS_AMBIGUOUS =
      "Reference '%s' is ambiguous";
  public static final String COLUMN_IS_AMBIGUOUS =
      "Column '%s' is ambiguous";
  public static final String UNSUPPORTED_NODE_TYPE =
      "Unsupported node type: ";
  public static final String CREATE_DATABASE_STATEMENT_IS_NOT_SUPPORTED_YET =
      "Create Database statement is not supported yet.";
  public static final String ALTER_DATABASE_STATEMENT_IS_NOT_SUPPORTED_YET =
      "Alter Database statement is not supported yet.";
  public static final String DROP_DATABASE_STATEMENT_IS_NOT_SUPPORTED_YET =
      "Drop Database statement is not supported yet.";
  public static final String SHOW_DATABASE_STATEMENT_IS_NOT_SUPPORTED_YET =
      "Show Database statement is not supported yet.";
  public static final String SHOW_TABLES_STATEMENT_IS_NOT_SUPPORTED_YET =
      "Show Tables statement is not supported yet.";
  public static final String DESCRIBE_TABLE_STATEMENT_IS_NOT_SUPPORTED_YET =
      "Describe Table statement is not supported yet.";
  public static final String ADD_COLUMN_STATEMENT_IS_NOT_SUPPORTED_YET =
      "Add Column statement is not supported yet.";
  public static final String CREATE_INDEX_STATEMENT_IS_NOT_SUPPORTED_YET =
      "Create Index statement is not supported yet.";
  public static final String DROP_INDEX_STATEMENT_IS_NOT_SUPPORTED_YET =
      "Drop Index statement is not supported yet.";
  public static final String SHOW_INDEX_STATEMENT_IS_NOT_SUPPORTED_YET =
      "Show Index statement is not supported yet.";
  public static final String UPDATE_CAN_ONLY_SPECIFY_ATTRIBUTE_COLUMNS =
      "Update can only specify attribute columns.";
  public static final String DROP_FUNCTION_STATEMENT_IS_NOT_SUPPORTED_YET =
      "Drop Function statement is not supported yet.";
  public static final String SHOW_FUNCTION_STATEMENT_IS_NOT_SUPPORTED_YET =
      "Show Function statement is not supported yet.";
  public static final String USE_STATEMENT_IS_NOT_SUPPORTED_YET =
      "USE statement is not supported yet.";
  public static final String TARGET_TABLE_SCHEMA_MISSES_A_TIME_CATEGORY_COLUMN =
      "Target table schema misses a TIME category column";
  public static final String TIME_COLUMN_CAN_NOT_BE_NULL =
      "time column can not be null";
  public static final String NO_FIELD_COLUMN_PRESENT =
      "No Field column present";
  public static final String FETCH_FIRST_WITH_TIES_CLAUSE_REQUIRES_ORDER_BY =
      "FETCH FIRST WITH TIES clause requires ORDER BY";
  public static final String RECURSIVE_CTE_IS_NOT_SUPPORTED_YET =
      "recursive cte is not supported yet.";
  public static final String MISSING_COLUMN_ALIASES_IN_RECURSIVE_WITH_QUERY =
      "missing column aliases in recursive WITH query";
  public static final String NESTED_RECURSIVE_WITH_QUERY =
      "nested recursive WITH query";
  public static final String THERE_IS_AT_LEAST_ONE_RESULT_OF_EXPANDED =
      "There is at least one result of expanded";
  public static final String UNSUPPORTED_EXPRESSION_2 =
      "UnSupported Expression: ";
  public static final String RELATION_NOT_FOUND_OR_NOT_ALLOWED =
      "Relation not found or not allowed";
  public static final String COLUMNS_NOT_ALLOWED_FOR_RELATION_THAT_HAS_NO =
      "COLUMNS not allowed for relation that has no columns";
  public static final String UNKNOWN_COLUMNNAME =
      "Unknown ColumnName: ";
  public static final String INVALID_REGEX =
      "Invalid regex '%s'";
  public static final String COLUMNS_ARE_NOT_SUPPORTED_IN_DEREFERENCEEXPRESSION =
      "Columns are not supported in DereferenceExpression";
  public static final String SELECT_NOT_ALLOWED_FROM_RELATION_THAT_HAS_NO =
      "SELECT * not allowed from relation that has no columns";
  public static final String COLUMN_ALIASES_NOT_SUPPORTED =
      "Column aliases not supported";
  public static final String SELECT_NOT_ALLOWED_IN_QUERIES_WITHOUT_FROM_CLAUSE =
      "SELECT * not allowed in queries without FROM clause";
  public static final String MULTIPLE_DATE_BIN_GAPFILL_CALLS_NOT_ALLOWED =
      "multiple date_bin_gapfill calls not allowed";
  public static final String PATTERN_RECOGNITION_OUTPUT_TABLE_HAS_NO_COLUMNS =
      "pattern recognition output table has no columns";
  public static final String NATURAL_JOIN_NOT_SUPPORTED =
      "Natural join not supported";
  public static final String UNKNOWN_FILL_METHOD =
      "Unknown fill method: ";
  public static final String RECURSIVE_REFERENCE_IN_INTERSECT_ALL =
      "recursive reference in INTERSECT ALL";
  public static final String TABLE_PROPERTY_2 =
      "Table property ";
  public static final String THE_DATABASE_MUST_BE_SET =
      "The database must be set.";
  public static final String AT_MOST_ONE_TABLE_ARGUMENT_CAN_BE_PASSED =
      "At most one table argument can be passed to a table function";
  public static final String DUPLICATE_ARGUMENT_NAME =
      "Duplicate argument name: %s";
  public static final String SETTING_MONTHLY_INTERVALS_IS_NOT_SUPPORTED =
      "Setting monthly intervals is not supported.";
  public static final String FILTER_PUSH_DOWN_DOES_NOT_SUPPORT_CASE_WHEN =
      "Filter push down does not support CASE WHEN";
  public static final String FILTER_PUSH_DOWN_DOES_NOT_SUPPORT_IF =
      "Filter push down does not support IF";
  public static final String FILTER_PUSH_DOWN_DOES_NOT_SUPPORT_NULLIF =
      "Filter push down does not support NULLIF";
  public static final String EXPRESSION_SHOULD_BE_NUMERIC_ACTUAL_IS =
      "expression should be numeric, actual is ";
  public static final String TIMESTAMP_DOES_NOT_SUPPORT_IS_NULL =
      "TIMESTAMP does not support IS NULL";
  public static final String TIMESTAMP_DOES_NOT_SUPPORT_IS_NOT_NULL =
      "TIMESTAMP does not support IS NOT NULL";
  public static final String TIMESTAMP_DOES_NOT_SUPPORT_LIKE =
      "TIMESTAMP does not support LIKE";
  public static final String TIMESTAMP_DOES_NOT_CASE_WHEN =
      "TIMESTAMP does not CASE WHEN";
  public static final String TIMESTAMP_DOES_NOT_IF =
      "TIMESTAMP does not IF";
  public static final String TIMESTAMP_DOES_NOT_NULLIF =
      "TIMESTAMP does not NULLIF";
  public static final String SHOULD_NEVER_RETURN_NULL =
      "Should never return null.";
  public static final String IS_NULL_EXPRESSION_CAN_T_BE_PUSHED_DOWN =
      "IS NULL Expression can't be pushed down";
  public static final String NOT_EXPRESSION_CAN_T_BE_PUSHED_DOWN =
      "Not Expression can't be pushed down";
  public static final String UNSUPPORTED_OPERATOR_2 =
      "Unsupported operator ";
  public static final String THE_LOGICAL_EXPRESSION_HAS_NO_BOUNDED_COLUMN =
      "The logical expression has no bounded column";
  public static final String THE_NOT_EXPRESSION_HAS_NO_BOUNDED_COLUMN =
      "The not expression has no bounded column";

  // --- Plan / Relational / Metadata ---
  public static final String OBJECT_TYPE_IS_NOT_SUPPORTED_AS_RETURN_TYPE =
      "OBJECT type is not supported as return type";
  public static final String INVALID_FUNCTION_PARAMETERS =
      "Invalid function parameters: ";
  public static final String UNKNOWN_FUNCTION =
      "Unknown function: ";
  public static final String THE_OBJECT_TYPE_COLUMN_IS_NOT_SUPPORTED =
      "The object type column is not supported.";
  public static final String NO_COLUMN_OTHER_THAN_TIME_PRESENT_PLEASE_CHECK =
      "No column other than Time present, please check the request";
  public static final String NO_FIELD_COLUMN_PRESENT_PLEASE_CHECK_THE_REQUEST =
      "No Field column present, please check the request";
  public static final String AUTO_ADD_TABLE_COLUMN_FAILED =
      "Auto add table column failed.";
  public static final String TAG_COLUMN_ONLY_SUPPORT_DATA_TYPE_STRING =
      "Tag column only support data type STRING.";
  public static final String ATTRIBUTE_COLUMN_ONLY_SUPPORT_DATA_TYPE_STRING =
      "Attribute column only support data type STRING.";
  public static final String UNSUPPORTED_EXTERNAL_TSFILE_DEVICE_FILTER =
      "Unsupported external TsFile device filter: ";

  // --- Plan / Relational / Table Function ---

  public static final String NO_TABLE_SCHEMA_FOUND_IN_TSFILES =
      "No table schema found in TsFiles";
  public static final String NO_TABLE_SCHEMA_FOUND_FOR_TABLE_IN_TSFILES =
      "No table schema found for table %s in TsFiles";
  public static final String READ_TSFILE_MUST_BE_PLANNED_AS_EXTERNAL_TSFILE_SCAN_NODE =
      "readTsFile must be planned as an ExternalTsFileScanNode";
  public static final String MISSING_SCALAR_ARGUMENT =
      "Missing scalar argument: ";
  public static final String ARGUMENT_SHOULD_NOT_BE_EMPTY =
      "Argument %s should not be empty";
  public static final String INVALID_SCALAR_ARGUMENT =
      "Invalid scalar argument: ";
  public static final String ARGUMENT_SHOULD_BE_A_STRING =
      "Argument %s should be a string";
  public static final String ARGUMENT_SHOULD_CONTAIN_AT_LEAST_ONE_PATH =
      "Argument %s should contain at least one path";
  public static final String READ_TSFILE_PATH_IS_NOT_ALLOWED =
      "readTsFile path %s is not allowed because it may access IoTDB data directory %s";
  public static final String OUTPUT_COLUMN_NAMES_AND_TYPES_SIZE_MISMATCH =
      "Output column names and types size mismatch";
  public static final String OUTPUT_COLUMN_NAMES_AND_CATEGORIES_SIZE_MISMATCH =
      "Output column names and categories size mismatch";
  public static final String READ_TSFILE_TABLE_FUNCTION_HANDLE_DOES_NOT_SUPPORT_SERIALIZATION =
      "ReadTsFileTableFunctionHandle does not support serialization";
  public static final String READ_TSFILE_TABLE_FUNCTION_HANDLE_DOES_NOT_SUPPORT_DESERIALIZATION =
      "ReadTsFileTableFunctionHandle does not support deserialization";
  public static final String TSFILE_PATH_DOES_NOT_EXIST =
      "TsFile path does not exist: ";
  public static final String TSFILE_PATH_IS_NEITHER_A_FILE_NOR_A_DIRECTORY =
      "TsFile path is neither a file nor a directory: ";
  public static final String NO_VALID_TSFILES_FOUND =
      "No valid TsFiles found";
  public static final String FAILED_TO_SCAN_TSFILE_PATH =
      "Failed to scan TsFile path: ";
  public static final String CANNOT_INFER_TABLE_NAME_FROM_TSFILES_MULTIPLE_TABLES =
      "Cannot infer table name from TsFiles because multiple tables are found: %s and %s";
  public static final String CANNOT_INFER_TABLE_NAME_FROM_TSFILE_NO_TABLE_SCHEMA =
      "Cannot infer table name from TsFile because no table schema is found in ";
  public static final String CANNOT_INFER_TABLE_NAME_FROM_TSFILE_MULTIPLE_TABLES =
      "Cannot infer table name from TsFile because multiple tables are found in ";
  public static final String FILE_IS_NOT_A_VALID_TSFILE =
      "File is not a valid TsFile: ";
  public static final String FAILED_TO_READ_TABLE_SCHEMA_FROM_TSFILE =
      "Failed to read table schema from TsFile: ";
  public static final String MULTIPLE_TIME_COLUMNS_FOUND_WHEN_MERGING_TABLE_SCHEMA =
      "Multiple time columns found when merging table schema for table ";
  public static final String TIME_COLUMN_CONFLICTS_WHEN_MERGING_TABLE_SCHEMA =
      "Time column conflicts when merging table schema for table ";
  public static final String TAG_COLUMNS_CONFLICT_WHEN_MERGING_TABLE_SCHEMA =
      "Tag columns conflict when merging table schema for table ";
  public static final String FIELD_COLUMN_HAS_CONFLICTING_DATA_TYPES_WHEN_MERGING_TABLE_SCHEMA =
      "Field column %s has conflicting data types when merging table schema for table %s";
  public static final String COLUMN_HAS_CONFLICTING_CATEGORIES_WHEN_MERGING_TABLE_SCHEMA =
      "Column %s has conflicting categories when merging table schema for table %s";
  public static final String FAILED_TO_CREATE_EXTERNAL_TSFILE_DEVICE_TASK_RUN_READER =
      "Failed to create external TsFile device task run reader";
  public static final String UNKNOWN_EXTERNAL_TSFILE_DEVICE_TASK_PARTITION =
      "Unknown external TsFile device task partition: ";
  public static final String EXTERNAL_TSFILE_QUERY_RESOURCE_HAS_BEEN_CLOSED =
      "External TsFile query resource has been closed: ";
  public static final String EXTERNAL_TSFILE_QUERY_RESOURCE_HAS_ALREADY_BEEN_SET =
      "External TsFile query resource has already been set in this fragment instance";
  public static final String EXTERNAL_TSFILE_FRAGMENT_INSTANCE_USAGE_COUNT_CANNOT_BE_NEGATIVE =
      "External TsFile fragment instance usage count cannot be negative";
  public static final String FAILED_TO_DESERIALIZE_EXTERNAL_TSFILE_RESOURCE =
      "Failed to deserialize external TsFile resource: %s, %s";
  public static final String FAILED_TO_FLUSH_EXTERNAL_TSFILE_DEVICE_TASK_PARTITION =
      "Failed to flush external TsFile device task partition";
  public static final String EXTERNAL_TSFILE_DEVICE_TASK_PARTITION_COUNT_MUST_BE_POSITIVE =
      "External TsFile device task partition count must be positive";
  public static final String FAILED_TO_CREATE_EXTERNAL_TSFILE_DEVICE_COLLECTOR =
      "Failed to create external TsFile device collector";

  // --- Plan / Relational / Planner ---

  public static final String FAIL_TO_MATERIALIZE_CTE_BECAUSE =
      "Fail to materialize CTE because {}";
  public static final String BOTH_OBJECT_MUST_BE_TYPE_OF_NUMBER =
      "Both object must be type of number";
  public static final String NOT_YET_IMPLEMENTED =
      "not yet implemented: ";
  public static final String UNSUPPORTED_TYPE_IN_GENERICLITERAL =
      "Unsupported type in GenericLiteral: ";
  public static final String CANNOT_COERCE_TYPE =
      "Cannot coerce type ";
  public static final String UNKNOWN_TYPE_2 =
      "Unknown type: ";
  public static final String NODE_MUST_BE_A_LITERAL =
      "node must be a Literal";
  public static final String UNHANDLED_LITERAL_TYPE =
      "Unhandled literal type: ";
  public static final String NO_LITERAL_FORM_FOR_TYPE =
      "No literal form for type %s";
  public static final String WINDOW_FRAME_OFFSET_VALUE_MUST_NOT_BE_NEGATIVE =
      "Window frame offset value must not be negative or null";
  public static final String UNEXPECTED_TYPE =
      "unexpected type: ";
  public static final String FROM_CLAUSE_MUST_NOT_BE_EMPTY =
      "From clause must not be empty";
  public static final String COERCION_RESULT_IN_ANALYSIS_ONLY_CAN_BE_EMPTY =
      "Coercion result in analysis only can be empty";
  public static final String UNEXPECTED_RECURSIVE_CTE =
      "unexpected recursive cte";
  public static final String TABLE =
      "Table ";
  public static final String UNEXPECTED_JOIN_TYPE =
      "Unexpected Join Type: ";
  public static final String UNEXPECTED_ROWS_PER_MATCH =
      "Unexpected rows per match: ";
  public static final String UNEXPECTED_SKIP_TO_POSITION =
      "Unexpected skip to position: ";
  public static final String VALUES_IS_NOT_SUPPORTED_IN_CURRENT_VERSION =
      "Values is not supported in current version.";
  public static final String SUBSCRIPT_IS_NOT_SUPPORTED_IN_CURRENT_VERSION =
      "Subscript is not supported in current version";
  public static final String READ_TSFILE_TABLE_FUNCTION_HANDLE_IS_INVALID =
      "readTsFile table function handle is invalid";

  // --- Plan / Relational / Planner / IR ---

  public static final String ILLEGAL_STATE_IN_VISITLOGICALEXPRESSION =
      "Illegal state in visitLogicalExpression";
  public static final String UNSUPPORTED_LOGICALEXPRESSION_OPERATOR =
      "Unsupported LogicalExpression operator";
  public static final String UNEXPECTED_EXPRESSION =
      "Unexpected expression: ";
  public static final String FAILED_TO_FETCH_SUBQUERY_RESULT =
      "Failed to Fetch Subquery Result.";

  // --- Plan / Relational / Planner / Iterative ---

  public static final String UNEXPECTED_PATTERN =
      "Unexpected Pattern: ";
  public static final String TABLE_FUNCTION_DOES_NOT_SUPPORT_MULTIPLE_SOURCE_NOW =
      "table function does not support multiple source now.";

  // --- Plan / Relational / Planner / Node ---

  public static final String SHOULD_NEVER_PUSH_DOWN_LIMIT_TO_AGGREGATIONTABLESCANNODE =
      "Should never push down limit to AggregationTableScanNode.";
  public static final String SHOULD_NEVER_PUSH_DOWN_OFFSET_TO_AGGREGATIONTABLESCANNODE =
      "Should never push down offset to AggregationTableScanNode.";
  public static final String NOT_SUPPORTED_YET =
      "Not supported yet.";
  public static final String COPYTONODE_SHOULD_NOT_BE_SERIALIZED =
      "CopyToNode should not be serialized";
  public static final String
      EXTERNAL_TSFILE_AGGREGATION_SCAN_NODE_DEVICE_ENTRIES_MUST_BE_SET_BY_DEVICE_ENTRY_INDEXES =
          "ExternalTsFileAggregationScanNode device entries must be set by device entry indexes";
  public static final String EXTERNAL_TSFILE_AGGREGATION_SCAN_NODE_CANNOT_BE_SERIALIZED =
      "ExternalTsFileAggregationScanNode cannot be serialized because it reads local external TsFiles";
  public static final String
      EXTERNAL_TSFILE_SCAN_NODE_DEVICE_ENTRIES_MUST_BE_SET_BY_DEVICE_ENTRY_INDEXES =
          "ExternalTsFileScanNode device entries must be set by device entry indexes";
  public static final String EXTERNAL_TSFILE_SCAN_NODE_CANNOT_BE_SERIALIZED =
      "ExternalTsFileScanNode cannot be serialized because it reads local external TsFiles";

  // --- Plan / Relational / Planner / Optimizations ---

  public static final String LIST_PLANNODE_SIZE_SHOULD_1_BUT_NOW_IS =
      "List<PlanNode>.size should >= 1, but now is 0";
  public static final String UNSUPPORTED_JOIN_TYPE =
      "Unsupported Join Type: ";
  public static final String TOPK_IS_NOT_SUPPORTED_IN_CORRELATED_SUBQUERY_FOR =
      "TopK is not supported in correlated subquery for now";
  public static final String UNEXPECTED_VALUE =
      "Unexpected value: ";

  // --- Plan / Relational / Security ---

  public static final String USER_NOT_EXISTS =
      "User not exists";
  public static final String ONLY_THE_SUPERUSER_CAN_ALTER_HIM_HERSELF =
      "Only the superuser can alter him/herself.";
  public static final String DATABASE =
      "DATABASE ";
  public static final String TABLE_2 =
      "TABLE ";
  public static final String UNEXPECTED_VALUE_2 =
      "Unexpected value:";
  public static final String EACH_OPERATION_SHOULD_HAVE_PERMISSION_CHECK =
      "Each operation should have permission check.";
  public static final String UNKNOWN_AUTHORTYPE =
      "Unknown authorType: ";

  // --- Plan / Relational / SQL ---

  public static final String UNKNOWN_AUTHORTYPE_2 =
      "Unknown authorType:";
  public static final String THE_RENAMING_FOR_BASE_TABLE_COLUMN_IS_CURRENTLY =
      "The renaming for base table column is currently unsupported";
  public static final String THE_RENAMING_FOR_BASE_TABLE_IS_CURRENTLY_UNSUPPORTED =
      "The renaming for base table is currently unsupported";
  public static final String UNEXPECTED_EXPRESSION_2 =
      "unexpected expression: ";
  public static final String THE_TABLE_SHOULD_ONLY_HAVE_ONE_COLUMN_FOUND =
      "the table should only have one column found with TIME category";
  public static final String TIMESTAMP_CANNOT_BE_NULL =
      "Timestamp cannot be null";
  public static final String SHOW_REGION_ID_IS_NOT_SUPPORTED_YET =
      "SHOW REGION ID is not supported yet.";
  public static final String SHOW_TIME_SLOT_IS_NOT_SUPPORTED_YET =
      "SHOW TIME SLOT is not supported yet.";
  public static final String COUNT_TIME_SLOT_IS_NOT_SUPPORTED_YET =
      "COUNT TIME SLOT is not supported yet.";
  public static final String SHOW_SERIES_SLOT_IS_NOT_SUPPORTED_YET =
      "SHOW SERIES SLOT is not supported yet.";
  public static final String MISSING_LIMIT_VALUE =
      "Missing LIMIT value";
  public static final String DATABASE_IS_NOT_SET_YET =
      "Database is not set yet.";
  public static final String AUTHOR_STATEMENT_PARSER_ERROR =
      "author statement parser error";
  public static final String UNSUPPORTED_SET_OPERATION =
      "Unsupported set operation: ";
  public static final String UNSUPPORTED_JOIN_CRITERIA =
      "Unsupported join criteria";
  public static final String TOLERANCE_IN_ASOF_JOIN_ONLY_SUPPORTS_INNER_TYPE =
      "Tolerance in ASOF JOIN only supports INNER type now";
  public static final String UNSUPPORTED_SIGN =
      "Unsupported sign: ";
  public static final String UNSUPPORTED_WINDOW_FRAME_TYPE =
      "Unsupported window frame type: ";
  public static final String UNSUPPORTED_BOUNDED_TYPE =
      "Unsupported bounded type: ";
  public static final String UNSUPPORTED_TRIM_SPECIFICATION =
      "Unsupported trim specification: ";
  public static final String TARGET_DATA_IN_SQL_SHOULD_BE_SET_IN =
      "Target data in sql should be set in CREATE MODEL";
  public static final String THE_TREE_MODEL_DATABASE_SHALL_NOT_BE_SPECIFIED =
      "The tree model database shall not be specified in table model.";
  public static final String UNSUPPORTED_SPECIAL_FUNCTION =
      "Unsupported special function: ";
  public static final String UNSUPPORTED_ORDERING =
      "Unsupported ordering: ";
  public static final String UNSUPPORTED_QUANTIFIER =
      "Unsupported quantifier: ";
  public static final String NOT_YET_IMPLEMENTED_WILDCARD_TRANSITION =
      "not yet implemented: wildcard transition";
  public static final String UNKNOWN_TABLE_ELEMENT =
      "unknown table element: ";

  // --- Plan / Scheduler ---

  public static final String ERROR_HAPPENED_WHILE_FETCHING_QUERY_STATE =
      "error happened while fetching query state";
  public static final String INTERRUPTED_WHEN_DISPATCHING_READ_ASYNC =
      "Interrupted when dispatching read async";
  public static final String INTERRUPTED_WHEN_DISPATCHING_WRITE_ASYNC =
      "Interrupted when dispatching write async";
  public static final String DESERIALIZE_CONSENSUSGROUPID_FAILED =
      "Deserialize ConsensusGroupId failed. ";
  public static final String CAN_T_CONNECT_TO_NODE =
      "can't connect to node {}";
  public static final String CANCEL_QUERY_ON_NODE_FAILED =
      "cancel query {} on node {} failed.";
  public static final String CANNOT_DISPATCH_FI_FOR_LOAD_OPERATION =
      "cannot dispatch FI for load operation";
  public static final String RECEIVE_LOAD_NODE_FROM_UUID =
      "Receive load node from uuid {}.";
  public static final String LOAD_TSFILE_NODE_ERROR =
      "Load TsFile Node {} error.";
  public static final String SERIALIZE_TSFILERESOURCE_ERROR =
      "Serialize TsFileResource {} error.";
  public static final String LOAD_SKIP_TSFILE_BECAUSE_IT_HAS_NO_DATA =
      "Load skip TsFile {}, because it has no data.";
  public static final String LOADTSFILESCHEDULER_LOADS_TSFILE_ERROR =
      "LoadTsFileScheduler loads TsFile {} error";
  public static final String INTERRUPT_OR_EXECUTION_ERROR =
      "Interrupt or Execution error.";
  public static final String START_DISPATCHING_LOAD_COMMAND_FOR_UUID =
      "Start dispatching Load command for uuid {}";
  public static final String EXCEPTION_OCCURRED_DURING_SECOND_PHASE_OF_LOADING_TSFILE =
      "Exception occurred during second phase of loading TsFile {}.";
  public static final String START_LOAD_TSFILE_LOCALLY =
      "Start load TsFile {} locally.";
  public static final String LOAD_ALL_FAILED_TSFILES_ARE_CONVERTED_TO_TABLETS =
      "Load: all failed TsFiles are converted to tablets and inserted.";

  // --- Plan / Statement ---

  public static final String METHOD_NOT_IMPLEMENTED_YET =
      "Method not implemented yet";
  public static final String INSERTION_CONTAINS_DUPLICATED_MEASUREMENT =
      "Insertion contains duplicated measurement: ";
  public static final String UNSUPPORTED_DATA_TYPE =
      "Unsupported data type:";
  public static final String FAILED_TO_CONVERT_INSERTTABLETSTATEMENT_TO_TABLET =
      "Failed to convert InsertTabletStatement to Tablet";
  public static final String MODEL_INFERENCE_DOES_NOT_SUPPORT_ALIGN_BY_DEVICE =
      "Model inference does not support align by device now.";
  public static final String MODEL_INFERENCE_DOES_NOT_SUPPORT_SELECT_INTO_NOW =
      "Model inference does not support select into now.";
  public static final String GROUP_BY_CLAUSES_DOESN_T_SUPPORT_GROUP_BY =
      "GROUP BY CLAUSES doesn't support GROUP BY LEVEL now.";
  public static final String GROUP_BY_LEVEL_DOES_NOT_SUPPORT_ALIGN_BY =
      "GROUP BY LEVEL does not support align by device now.";
  public static final String GROUP_BY_TAGS_DOES_NOT_SUPPORT_ALIGN_BY =
      "GROUP BY TAGS does not support align by device now.";
  public static final String HAVING_CLAUSE_IS_NOT_SUPPORTED_YET_IN_GROUP =
      "Having clause is not supported yet in GROUP BY TAGS query";
  public static final String OUTPUT_COLUMN_IS_DUPLICATED_WITH_THE_TAG_KEY =
      "Output column is duplicated with the tag key: ";
  public static final String LIMIT_OR_SLIMIT_ARE_NOT_SUPPORTED_YET_IN =
      "Limit or slimit are not supported yet in GROUP BY TAGS";
  public static final String EXPRESSION_OF_HAVING_CLAUSE_MUST_TO_BE_AN =
      "Expression of HAVING clause must to be an Aggregation";
  public static final String WHEN_HAVING_USED_WITH_GROUPBYLEVEL =
      "When Having used with GroupByLevel: ";
  public static final String ALIGN_BY_DEVICE =
      "ALIGN BY DEVICE: ";
  public static final String SORTING_BY_TIMESERIES_IS_ONLY_SUPPORTED_IN_LAST =
      "Sorting by timeseries is only supported in last queries.";
  public static final String LAST_QUERY_DOESN_T_SUPPORT_ALIGN_BY_DEVICE =
      "Last query doesn't support align by device.";
  public static final String LAST_QUERIES_CAN_ONLY_BE_APPLIED_ON_RAW =
      "Last queries can only be applied on raw time series.";
  public static final String SLIMIT_AND_SOFFSET_CAN_NOT_BE_USED_IN =
      "SLIMIT and SOFFSET can not be used in LastQuery.";
  public static final String SELECT_INTO_SLIMIT_CLAUSES_ARE_NOT_SUPPORTED =
      "select into: slimit clauses are not supported.";
  public static final String SELECT_INTO_SOFFSET_CLAUSES_ARE_NOT_SUPPORTED =
      "select into: soffset clauses are not supported.";
  public static final String SELECT_INTO_LAST_CLAUSES_ARE_NOT_SUPPORTED =
      "select into: last clauses are not supported.";
  public static final String SELECT_INTO_GROUP_BY_TAGS_CLAUSE_ARE_NOT =
      "select into: GROUP BY TAGS clause are not supported.";
  public static final String UNKNOWN_LITERAL_TYPE =
      "Unknown literal type: %s";
  public static final String ILLEGAL_PATH =
      "illegal path: {}";
  public static final String CQ_THE_START_TIME_OFFSET_SHOULD_BE_GREATER =
      "CQ: The start time offset should be greater than 0.";
  public static final String CQ_THE_END_TIME_OFFSET_SHOULD_BE_GREATER =
      "CQ: The end time offset should be greater than or equal to 0.";
  public static final String CQ_THE_QUERY_BODY_MISSES_AN_INTO_CLAUSE =
      "CQ: The query body misses an INTO clause.";
  public static final String CQ_SPECIFYING_TIME_FILTERS_IN_THE_QUERY_BODY =
      "CQ: Specifying time filters in the query body is prohibited.";
  public static final String IS_NOT_A_LEGAL_PATH =
      "{} is not a legal path";

  // --- Plan / Tree Planner ---

  public static final String VALID_TREEDEVICEVIEWSCANNODE_IS_NOT_EXPECTED_HERE =
      "Valid TreeDeviceViewScanNode is not expected here.";
  public static final String MULTIPLE_COLUMNS_WITH_TIME_CATEGORY_FOUND =
      "Multiple columns with TIME category found";
  public static final String MISSING_TIME_CATEGORY_COLUMN =
      "Missing TIME category column";
  public static final String UNKNOWN_SQL_DIALECT =
      "Unknown sql dialect: %s";
  public static final String UNEXPECTED_PATH_TYPE_2 =
      "Unexpected path type";
  public static final String SHOULD_CALL_THE_CONCRETE_VISITXX_METHOD =
      "should call the concrete visitXX() method";
  public static final String OUTPUTCOLUMTYPES_SHOULD_NOT_BE_NULL_EMPTY =
      "OutputColumTypes should not be null/empty";
  public static final String UNKNOWN_FILL_POLICY =
      "Unknown fill policy: ";
  public static final String FILTER_CAN_NOT_CONTAIN_NON_MAPPABLE_UDF =
      "Filter can not contain Non-Mappable UDF";
  public static final String GROUPBYVARIATIONEXPRESSION_CAN_T_BE_NULL =
      "groupByVariationExpression can't be null";
  public static final String GROUPBYCONDITIONEXPRESSION_CAN_T_BE_NULL =
      "groupByConditionExpression can't be null";
  public static final String GROUPBYCOUNTEXPRESSION_CAN_T_BE_NULL =
      "groupByCountExpression can't be null";
  public static final String UNKNOWN_NODE_TYPE =
      "Unknown node type: ";
  public static final String UNSUPPORTED_COLUMN_GENERATOR_TYPE =
      "Unsupported column generator type: ";
  public static final String ROOT_NODE_MUST_RETURN_ONLY_ONE =
      "root node must return only one";
  public static final String SINGLEDEVICEVIEWNODE_HAVE_ONLY_ONE_CHILD =
      "SingleDeviceViewNode have only one child";
  public static final String AVAILABLE_REPLICAS =
      "available replicas: {}";
  public static final String UNEXPECTED_ERROR_OCCURS_WHEN_SERIALIZING_THIS_FRAGMENTINSTANCE =
      "Unexpected error occurs when serializing this FragmentInstance.";
  public static final String INVALID_NODE_TYPE =
      "Invalid node type: ";
  public static final String THIS_LASTQUERYSCANNODE_IS_DEPRECATED =
      "This LastQueryScanNode is deprecated";
  public static final String EXPLAINANALYZENODE_SHOULD_NOT_BE_SERIALIZED =
      "ExplainAnalyzeNode should not be serialized";
  public static final String EXPLAINANALYZENODE_SHOULD_NOT_BE_DESERIALIZED =
      "ExplainAnalyzeNode should not be deserialized";
  public static final String CLONE_OF_LOAD_SINGLE_TSFILE_IS_NOT_IMPLEMENTED =
      "clone of load single TsFile is not implemented";
  public static final String SPLIT_LOAD_SINGLE_TSFILE_IS_NOT_IMPLEMENTED =
      "split load single TsFile is not implemented";
  public static final String DELETE_AFTER_LOADING_ERROR =
      "Delete After Loading {} error.";
  public static final String CLONE_OF_LOAD_TSFILE_IS_NOT_IMPLEMENTED =
      "clone of load TsFile is not implemented";
  public static final String LOADTSFILE_STATEMENT_IS_NULL_DURING_TABLE_MODEL_SPLIT =
      "LoadTsFile statement is null during table model split.";
  public static final String CLONE_OF_LOAD_PIECE_TSFILE_IS_NOT_IMPLEMENTED =
      "clone of load piece TsFile is not implemented";
  public static final String SERIALIZE_TO_BYTEBUFFER_ERROR =
      "Serialize to ByteBuffer error.";
  public static final String SPLIT_LOAD_PIECE_TSFILE_IS_NOT_IMPLEMENTED =
      "split load piece TsFile is not implemented";
  public static final String DESERIALIZE_ERROR =
      "Deserialize {} error.";
  public static final String INVALID_LENGTH_FOR_SLICING =
      "Invalid length for slicing: ";
  public static final String CANNOT_DESERIALIZE_DEVICESSCHEMASCANNODE =
      "Cannot deserialize DevicesSchemaScanNode";
  public static final String CANNOT_DESERIALIZE_TIMESERIESSCHEMASCANNODE =
      "Cannot deserialize TimeSeriesSchemaScanNode";
  public static final String CLONE_OF_ALTERTIMESERIESNODE_IS_NOT_IMPLEMENTED =
      "Clone of AlterTimeSeriesNode is not implemented";
  public static final String CAN_NOT_DESERIALIZE_ALTERTIMESERIESNODE =
      "Can not deserialize AlterTimeSeriesNode";
  public static final String CLONE_OF_CREATEALIGNEDTIMESERIESNODE_IS_NOT_IMPLEMENTED =
      "Clone of CreateAlignedTimeSeriesNode is not implemented";
  public static final String CAN_NOT_DESERIALIZE_CREATEALIGNEDTIMESERIESNODE =
      "Can not deserialize CreateAlignedTimeSeriesNode";
  public static final String CLONE_OF_CREATEMULTITIMESERIESNODE_IS_NOT_IMPLEMENTED =
      "Clone of CreateMultiTimeSeriesNode is not implemented";
  public static final String CLONE_OF_CREATETIMESERIESNODE_IS_NOT_IMPLEMENTED =
      "Clone of CreateTimeSeriesNode is not implemented";
  public static final String CANNOT_DESERIALIZE_CREATETIMESERIESNODE =
      "Cannot deserialize CreateTimeSeriesNode";
  public static final String CLONE_OF_INTERNALCREATETIMESERIESNODE_IS_NOT_IMPLEMENTED =
      "Clone of InternalCreateTimeSeriesNode is not implemented";
  public static final String CLONE_OF_ALTERLOGICALNODE_IS_NOT_IMPLEMENTED =
      "Clone of AlterLogicalNode is not implemented";
  public static final String UNEXPECTED_DESCRIPTORTYPE =
      "Unexpected descriptorType: ";
  public static final String NO_CHILD_IS_ALLOWED_FOR_ALIGNEDSERIESSCANNODE =
      "no child is allowed for AlignedSeriesScanNode";
  public static final String DEVICEREGIONSCANNODE_HAS_NO_CHILDREN =
      "DeviceRegionScanNode has no children";
  public static final String NO_CHILD_IS_ALLOWED_FOR_SERIESSCANNODE =
      "no child is allowed for SeriesScanNode";
  public static final String NO_CHILD_IS_ALLOWED_FOR_SERIESAGGREGATESCANNODE =
      "no child is allowed for SeriesAggregateScanNode";
  public static final String NO_CHILD_IS_ALLOWED_FOR_SERIESSCANSOURCENODE =
      "no child is allowed for SeriesScanSourceNode";
  public static final String NO_CHILD_IS_ALLOWED_FOR_SHOWDISKUSAGENODE =
      "no child is allowed for ShowDiskUsageNode";
  public static final String NO_CHILD_IS_ALLOWED_FOR_SHOWQUERIESNODE =
      "no child is allowed for ShowQueriesNode";
  public static final String TIMESERIESREGIONSCANNODE_DOES_NOT_SUPPORT_ADDCHILD =
      "TimeseriesRegionScanNode does not support addChild";
  public static final String NOT_SUPPORTED =
      "Not supported.";
  public static final String CANNOT_DESERIALIZE_INSERTROWNODE =
      "Cannot deserialize InsertRowNode";
  public static final String UNEXPECTED_ERROR_OCCURS_WHEN_SERIALIZING_DELETEDATANODE =
      "Unexpected error occurs when serializing deleteDataNode.";
  public static final String DELETEDATANODES_IS_EMPTY =
      "deleteDataNodes is empty";
  public static final String INSERTMULTITABLETSNODE_NOT_SUPPORT_MERGE =
      "InsertMultiTabletsNode not support merge";
  public static final String CLONE_OF_INSERT_IS_NOT_IMPLEMENTED =
      "clone of Insert is not implemented";
  public static final String INSERTNODES_SHOULD_NEVER_BE_EMPTY =
      "insertNodes should never be empty";
  public static final String SERIALIZEATTRIBUTES_OF_INSERTNODE_IS_NOT_IMPLEMENTED =
      "serializeAttributes of InsertNode is not implemented";
  public static final String INSERTROWSOFONEDEVICENODE_NOT_SUPPORT_MERGE =
      "InsertRowsOfOneDeviceNode not support merge";
  public static final String CANNOT_DESERIALIZE_INSERTROWSOFONEDEVICENODE =
      "Cannot deserialize InsertRowsOfOneDeviceNode";
  public static final String CANNOT_DESERIALIZE_INSERTTABLETNODE =
      "Cannot deserialize InsertTabletNode";
  public static final String MERGE_IS_NOT_SUPPORTED =
      "Merge is not supported";
  public static final String FAILED_TO_SERIALIZE_MODENTRY_TO_WAL =
      "Failed to serialize modEntry to WAL";
  public static final String ALL_DATABASE_NAME_NEED_TO_BE_SAME =
      "All database name need to be same";
  public static final String INVALID_AGGREGATIONSTEP_TYPE =
      "Invalid AggregationStep type: ";

  // --- Transformation ---

  public static final String SIZE_IS_0 =
      "Size is 0";
  public static final String CAN_NOT_CALL_NEXT_ON_EMPTYROWITERATOR =
      "Can not call next on EmptyRowIterator";
  public static final String THE_EXPRESSION_CANNOT_BE_NULL =
      "The expression cannot be null";
  public static final String UNSUPPORTED_TYPE =
      "Unsupported type: ";
  public static final String UNSUPPORTED_DATA_TYPE_2 =
      "Unsupported data type: ";
  public static final String UNSUPPORTED_DATA_TYPE_3 =
      "unsupported data type: ";
  public static final String ERROR_OCCURRED_DURING_INFERRING_UDF_DATA_TYPE =
      "Error occurred during inferring UDF data type";
  public static final String ERROR_OCCURRED_DURING_GETTING_UDF_ACCESS_STRATEGY =
      "Error occurred during getting UDF access strategy";
  public static final String TRANSFORMUTILS_SHOULD_NOT_BE_INSTANTIATED =
      "TransformUtils should not be instantiated.";

  // --- Execution / Exchange (additional) ---

  public static final String ACK_TSBLOCK_FAILED =
      "ack TsBlock [{}, {}) failed.";
  public static final String CLOSE_CHANNEL_OF_SHUFFLESINKHANDLE_FAILED =
      "Close channel of ShuffleSinkHandle {}, index {} failed.";
  public static final String SHUFFLESINKHANDLE_ALREADY_IN_MAP =
      "ShuffleSinkHandle for ";
  public static final String IS_IN_THE_MAP =
      " is in the map.";
  public static final String SOURCE_HANDLE_FOR_PLAN_NODE_EXISTS_FMT =
      "Source handle for plan node %s of %s exists.";
  public static final String FAILED_TO_PULL_TSBLOCKS =
      "{} failed to pull TsBlocks [{}] to [{}] from SinkHandle {}, channel index {},";
  public static final String FAILED_TO_GET_DATA_BLOCK =
      "failed to get data block [{}, {}), attempt times: {}";
  public static final String FAILED_TO_SEND_ACK_DATA_BLOCK_EVENT =
      "failed to send ack data block event [{}, {}), attempt times: {}";
  public static final String SEND_CLOSE_SINK_CHANNEL_EVENT_FAILED =
      "[SendCloseSinkChannelEvent] to [ShuffleSinkHandle: {}, index: {}] failed.).";
  public static final String LOCAL_SINK_CHANNEL_STATE_IS =
      "LocalSinkChannel state is .";
  public static final String SCH_LISTENER_ON_FINISH =
      "[ScHListenerOnFinish]";
  public static final String SCH_LISTENER_ALREADY_RELEASED =
      "[ScHListenerAlreadyReleased]";
  public static final String SCH_LISTENER_ON_ABORT =
      "[ScHListenerOnAbort]";
  public static final String SHUFFLE_SINK_HANDLE_LISTENER_ON_FINISH =
      "[ShuffleSinkHandleListenerOnFinish]";
  public static final String SHUFFLE_SINK_HANDLE_LISTENER_ON_END_OF_TSBLOCKS =
      "[ShuffleSinkHandleListenerOnEndOfTsBlocks]";
  public static final String SHUFFLE_SINK_HANDLE_LISTENER_ON_ABORT =
      "[ShuffleSinkHandleListenerOnAbort]";
  public static final String SKH_LISTENER_ON_FINISH =
      "[SkHListenerOnFinish]";
  public static final String SKH_LISTENER_ON_END_OF_TSBLOCKS =
      "[SkHListenerOnEndOfTsBlocks]";
  public static final String SKH_LISTENER_ON_ABORT =
      "[SkHListenerOnAbort]";
  public static final String CLOSE_SHUFFLE_SINK_HANDLE =
      "Close ShuffleSinkHandle: {}";
  public static final String GET_SHARED_TSBLOCK_QUEUE_FROM_LOCAL_SOURCE_HANDLE =
      "Get SharedTsBlockQueue from local source handle";
  public static final String CREATE_SHARED_TSBLOCK_QUEUE =
      "Create SharedTsBlockQueue";
  public static final String CREATE_LOCAL_SINK_HANDLE_FOR =
      "Create local sink handle for {}";
  public static final String CREATE_LOCAL_SOURCE_HANDLE_FOR =
      "Create local source handle for {}";
  public static final String GET_SHARED_TSBLOCK_QUEUE_FROM_LOCAL_SINK_HANDLE =
      "Get SharedTsBlockQueue from local sink handle";
  public static final String START_FORCE_RELEASE_FI_DATA_EXCHANGE_RESOURCE =
      "[StartForceReleaseFIDataExchangeResource]";
  public static final String CLOSE_SOURCE_HANDLE =
      "[CloseSourceHandle] {}";
  public static final String END_FORCE_RELEASE_FI_DATA_EXCHANGE_RESOURCE =
      "[EndForceReleaseFIDataExchangeResource]";
  public static final String CREATE_LOCAL_SINK_HANDLE_TO_PLAN_NODE =
      "Create local sink handle to plan node {} of {} for {}";
  public static final String CREATE_SINK_HANDLE_TO_PLAN_NODE =
      "Create sink handle to plan node {} of {} for {}";
  public static final String CREATE_LOCAL_SOURCE_HANDLE_FROM =
      "Create local source handle from {} for plan node {} of {}";
  public static final String GET_SERIALIZED_TSBLOCK =
      "[GetSerializedTsBlock] TsBlock:{}";
  public static final String START_ABORT_LOCAL_SOURCE_HANDLE =
      "[StartAbortLocalSourceHandle]";
  public static final String END_ABORT_LOCAL_SOURCE_HANDLE =
      "[EndAbortLocalSourceHandle]";
  public static final String START_CLOSE_LOCAL_SOURCE_HANDLE =
      "[StartCloseLocalSourceHandle]";
  public static final String END_CLOSE_LOCAL_SOURCE_HANDLE =
      "[EndCloseLocalSourceHandle]";
  public static final String START_SET_NO_MORE_TSBLOCKS =
      "[StartSetNoMoreTsBlocks]";
  public static final String START_ABORT_SINK_CHANNEL =
      "[StartAbortSinkChannel]";
  public static final String END_ABORT_SINK_CHANNEL =
      "[EndAbortSinkChannel]";
  public static final String START_CLOSE_SINK_CHANNEL =
      "[StartCloseSinkChannel]";
  public static final String END_CLOSE_SINK_CHANNEL =
      "[EndCloseSinkChannel]";
  public static final String ACK_TSBLOCK =
      "[ACKTsBlock] {}.";
  public static final String NOTIFY_NO_MORE_TSBLOCK =
      "[NotifyNoMoreTsBlock]";
  public static final String START_SEND_TSBLOCK_ON_LOCAL =
      "[StartSendTsBlockOnLocal]";
  public static final String START_SET_NO_MORE_TSBLOCKS_ON_LOCAL =
      "[StartSetNoMoreTsBlocksOnLocal]";
  public static final String END_SET_NO_MORE_TSBLOCKS_ON_LOCAL =
      "[EndSetNoMoreTsBlocksOnLocal]";
  public static final String START_ABORT_LOCAL_SINK_CHANNEL =
      "[StartAbortLocalSinkChannel]";
  public static final String END_ABORT_LOCAL_SINK_CHANNEL =
      "[EndAbortLocalSinkChannel]";
  public static final String START_CLOSE_LOCAL_SINK_CHANNEL =
      "[StartCloseLocalSinkChannel]";
  public static final String END_CLOSE_LOCAL_SINK_CHANNEL =
      "[EndCloseLocalSinkChannel]";
  public static final String GET_TSBLOCK_FROM_BUFFER =
      "[GetTsBlockFromBuffer] sequenceId:{}, size:{}";
  public static final String WAIT_FOR_MORE_TSBLOCK =
      "[WaitForMoreTsBlock]";
  public static final String RECEIVE_NO_MORE_TSBLOCK_EVENT =
      "[ReceiveNoMoreTsBlockEvent]";
  public static final String END_PULL_TSBLOCKS_FROM_REMOTE =
      "[EndPullTsBlocksFromRemote] Count:{}";
  public static final String PUT_TSBLOCKS_INTO_BUFFER =
      "[PutTsBlocksIntoBuffer]";
  public static final String SEND_ACK_TSBLOCK =
      "[SendACKTsBlock] [{}, {}).";
  public static final String START_ABORT_SHUFFLE_SINK_HANDLE =
      "[StartAbortShuffleSinkHandle]";
  public static final String END_ABORT_SHUFFLE_SINK_HANDLE =
      "[EndAbortShuffleSinkHandle]";
  public static final String START_CLOSE_SHUFFLE_SINK_HANDLE =
      "[StartCloseShuffleSinkHandle]";
  public static final String END_CLOSE_SHUFFLE_SINK_HANDLE =
      "[EndCloseShuffleSinkHandle]";
  public static final String SIGNAL_NO_MORE_TSBLOCK_ON_QUEUE =
      "[SignalNoMoreTsBlockOnQueue]";
  public static final String QUEUE_DESTROYED_WHEN_SET_NO_MORE_TSBLOCKS =
      "The queue has been destroyed when calling setNoMoreTsBlocks.";
  public static final String ADD_TSBLOCK =
      "[addTsBlock] TsBlock:{}";

  // --- Plan (additional debug) ---

  public static final String QUERY_START_SQL =
      "[QueryStart] sql: {}";
  public static final String CLEAN_UP_QUERY =
      "[CleanUpQuery]]";
  public static final String RELEASE_QUERY_RESOURCE_STATE =
      "[ReleaseQueryResource] state is: {}";
  public static final String SKIP_EXECUTE =
      "[SkipExecute]";
  public static final String SKIP_EXECUTE_AFTER_LOGICAL_PLAN =
      "[SkipExecute After LogicalPlan]";
  public static final String RESULT_HANDLE_FINISHED =
      "[ResultHandleFinished]";

  // --- Execution / Operator / Source (additional debug) ---

  public static final String SERIES_SCAN_UTIL_PAGE_READER_IS_MODIFIED =
      "[SeriesScanUtil] pageReader.isModified() is {}";
  public static final String GET_ALL_SATISFIED_PAGE_DATA_TSBLOCK =
      "[getAllSatisfiedPageData] TsBlock:{}";

  // --- Plan / Relational / Metadata (additional debug) ---

  public static final String DEVICES_ARE_MISSING =
      "{} devices are missing";

  // --- Execution / Fragment (additional debug) ---

  public static final String STATE_CHANGED_TO =
      "[StateChanged] To {}";
  public static final String ENTER_THE_STATE_CHANGE_LISTENER =
      "Enter the stateChangeListener";

  // --- Execution / Fragment (additional) ---

  public static final String ERRORS_RELEASING_SINK =
      "Errors occurred while attempting to release sink, potentially leading to resource leakage.";
  public static final String ERRORS_DELETING_TMP_FILES =
      "Errors occurred while attempting to delete tmp files, potentially leading to resource leakage.";
  public static final String ERRORS_DEREGISTER_FI_FROM_MEMORY_POOL =
      "Errors occurred while attempting to deRegister FI from Memory Pool, potentially leading to resource leakage, status is {}.";
  public static final String ERRORS_RELEASING_MEMORY =
      "Errors occurred while attempting to release memory, potentially leading to resource leakage.";
  public static final String ERRORS_FINISHING_FI_PROCESS =
      "Errors occurred while attempting to finish the FI process, potentially leading to resource leakage.";

  // --- Plan (additional) ---

  public static final String CLEANING_UP_STALE_QUERY =
      "Cleaning up stale query with id {}, which has been running for {} ms, timeout duration is: {}ms";

  // --- Plan / Tree Planner (additional) ---

  public static final String ERROR_WHEN_READ_OBJECT_FILE =
      "Error when read object file {}.";

  // --- Additional Edge Cases ---

  public static final String JOIN_TYPE_IS_NOT_SUPPORTED =
      " Join type is not supported";
  public static final String COLON_S_VS_S =
      ": %s vs %s";
  public static final String IS_TOO_LARGE_STACK_OVERFLOW_WHILE_PARSING =
      " is too large (stack overflow while parsing)";

  public static final String ENTER_STATE_CHANGE_LISTENER = "Enter the stateChangeListener";

  // --- Analyzer / Planner ---
  public static final String NO_VALUE_PRESENT = "No value present";
  public static final String THE_INPUT_FIELD_DOES_NOT_EXIST = "the input field does not exist";
  public static final String THE_FIELD_IN_TABLE_DOES_NOT_HAVE_A_NAME =
      "the field in table does not have a name";
  public static final String SHOULD_HAVE_TWO_NUMERIC_OPERANDS =
      "Should have two numeric operands.";
  public static final String SHOULD_HAVE_ONE_NUMERIC_OPERANDS =
      "Should have one numeric operands.";
  public static final String SHOULD_HAVE_TWO_COMPARABLE_OPERANDS =
      "Should have two comparable operands.";
  public static final String JOIN_USING_CRITERIA_IS_EMPTY = "JoinUsing criteria is empty";
  public static final String S_IS_NOT_A_TABLE_REFERENCE = "%s is not a table reference";

    public static final String TOPOLOGY_DATANODE_REACHABILITY_CHANGED =
      "[Topology] DataNode {} is now {} to myself({})";
  public static final String NO_MAPPING_FOR_S =
      "No mapping for %s";
  public static final String CANCEL_STATE_TRACKING_TASK_FAILED =
      "cancel state tracking task failed. {}";
  public static final String TRACK_TASK_NOT_STARTED =
      "trackTask not started";
  public static final String PRINT_FI_STATE =
      "[PrintFIState] state is {}";
  public static final String START_FETCH_SCHEMA =
      "[StartFetchSchema]";
  public static final String END_FETCH_SCHEMA =
      "[EndFetchSchema]";
  public static final String CACHE_HIT =
      "[{} Cache] hit";
  public static final String PARTITION_CACHE_INVALID =
      "[Partition Cache] invalid";
  public static final String PARTITION_CACHE_IS_INVALID =
      "[Partition Cache] is invalid:{}";
  public static final String CANCEL_FI =
      "[CancelFI]";
  public static final String RENAME_VIEW_NOT_SUPPORT_WILDCARD =
      "Rename view doesn't support path pattern with wildcard.";
  public static final String REMOVE_CONFIG_NODE_FAILED =
      "Remove ConfigNode failed: ";

  public static final String CANT_CONNECT_TO_NODE_PREFIX = "can't connect to node ";
  public static final String REMOVE_AINODE_FAILED = "Remove AINode failed: ";

  public static final String QUERY_TIMEOUT_IN_FETCH_SCHEMA = "Query execution is time out while fetching schema";

  public static final String QUERY_EXECUTION_MISSING = "Query execution %s is missing during fetching device schema";

  // --- QueryEngine semantic messages (additional) ---
  public static final String PREPARED_STATEMENT_S_DOES_NOT_EXIST =
      "Prepared statement '%s' does not exist";
  public static final String CALL_INFERENCE_FUNCTION_SHOULD_NOT_CONTAIN_MORE_THAN_ONE_INPUT_COLUMN_FOUND_D_INPUT =
      "Call inference function should not contain more than one input column, found [%d] input columns.";
  public static final String DATA_TYPE_OF_TAG_COLUMN =
      "Data type of tag column ";
  public static final String IS_NOT_STRING =
      " is not STRING";
  public static final String THE_SOURCE_PATHS_S_OF_VIEW_S_ARE_MULTIPLE =
      "The source paths [%s] of view [%s] are multiple.";
  public static final String ERROR_OCCURRED_DURING_INFERRING_UDF_DATA_TYPE_S =
      "Error occurred during inferring UDF data type: %s";
  public static final String ERROR_OCCURRED_DURING_GETTING_UDF_ACCESS_STRATEGY_S =
      "Error occurred during getting UDF access strategy: %s";
  public static final String UNSUPPORTED_COMPRESSION_S =
      "unsupported compression: %s";
  public static final String TIMESERIES_CONDITION_AND_TIME_CONDITION_CANNOT_BE_USED_AT_THE_SAME_TIME =
      "TIMESERIES condition and TIME condition cannot be used at the same time.";
  public static final String LATEST_AND_ORDER_BY_TIMESERIES_CANNOT_BE_USED_AT_THE_SAME_TIME =
      "LATEST and ORDER BY TIMESERIES cannot be used at the same time.";
  public static final String DEVICE_CONDITION_AND_TIME_CONDITION_CANNOT_BE_USED_AT_THE_SAME_TIME =
      "DEVICE condition and TIME condition cannot be used at the same time.";
  public static final String TIME_CONDITION_AND_GROUP_BY_LEVEL_CANNOT_BE_USED_AT_THE_SAME_TIME =
      "TIME condition and GROUP BY LEVEL cannot be used at the same time.";
  public static final String CQ_AT_LEAST_ONE_OF_THE_PARAMETERS_EVERY_INTERVAL_AND_GROUP_BY_INTERVAL_NEEDS_TO_BE =
      "CQ: At least one of the parameters `every_interval` and `group_by_interval` needs to be specified.";
  public static final String CAN_NOT_USE_CHAR_DOLLAR_OR_INTO_ITEM_IN_ALTER_VIEW_STATEMENT =
      "Can not use char '$' or into item in alter view statement.";
  public static final String TIME_COLUMN_IS_NO_NEED_TO_APPEAR_IN_SELECT_CLAUSE_EXPLICITLY_IT_WILL_ALWAYS_BE_RETURNED =
      "Time column is no need to appear in SELECT Clause explicitly, it will always be returned if possible";
  public static final String THE_SECOND_PARAMETER_TIME_INTERVAL_SHOULD_BE_A_POSITIVE_INTEGER =
      "The second parameter time interval should be a positive integer.";
  public static final String THE_THIRD_PARAMETER_TIME_SLIDINGSTEP_SHOULD_BE_A_POSITIVE_INTEGER =
      "The third parameter time slidingStep should be a positive integer.";
  public static final String CONSTANT_OPERAND_S_IS_NOT_ALLOWED_IN_GROUP_BY_VARIATION_THERE_SHOULD_BE_AN_EXPRESSION =
      "Constant operand [%s] is not allowed in group by variation, there should be an expression";
  public static final String CONSTANT_OPERAND_S_IS_NOT_ALLOWED_IN_GROUP_BY_COUNT_THERE_SHOULD_BE_AN_EXPRESSION =
      "Constant operand [%s] is not allowed in group by count, there should be an expression";
  public static final String ORDER_BY_SORT_KEY_S_IS_NOT_CONTAINED_IN_S =
      "ORDER BY: sort key[%s] is not contained in '%s'";
  public static final String ORDER_BY_EXPRESSION_IS_NOT_SUPPORTED_FOR_CURRENT_STATEMENT_SUPPORTED_SORT_KEY =
      "ORDER BY expression is not supported for current statement, supported sort key: ";
  public static final String ONLY_FILL_PREVIOUS_SUPPORT_SPECIFYING_THE_TIME_DURATION_THRESHOLD =
      "Only FILL(PREVIOUS) support specifying the time duration threshold.";
  public static final String OUT_OF_RANGE_OFFSET_LT_OFFSETVALUE_GT_OFFSETVALUE_SHOULD_BE_INT64 =
      "Out of range. OFFSET <OFFSETValue>: OFFSETValue should be Int64.";
  public static final String OUT_OF_RANGE_SOFFSET_LT_SOFFSETVALUE_GT_SOFFSETVALUE_SHOULD_BE_INT32 =
      "Out of range. SOFFSET <SOFFSETValue>: SOFFSETValue should be Int32.";
  public static final String FAILED_TO_PARSE_THE_TIMESTAMP =
      "Failed to parse the timestamp: ";
  public static final String CURRENT_SYSTEM_TIMESTAMP_PRECISION_IS_S =
      "Current system timestamp precision is %s, ";
  public static final String PLEASE_CHECK_WHETHER_THE_TIMESTAMP_S_IS_CORRECT =
      "please check whether the timestamp %s is correct.";
  public static final String LOAD_TSFILE_FORMAT_S_ERROR_PLEASE_INPUT_AUTOREGISTER_SGLEVEL_VERIFY =
      "Load tsfile format %s error, please input AUTOREGISTER | SGLEVEL | VERIFY.";
  public static final String S_IS_ILLEGAL_UNQUOTED_NODE_NAME_CAN_ONLY_CONSIST_OF_DIGITS_CHARACTERS_AND_UNDERSCORE_OR =
      "%s is illegal, unquoted node name can only consist of digits, characters and underscore, or start or end with wildcard";
  public static final String S_IS_ILLEGAL_UNQUOTED_NODE_NAME_IN_SELECT_INTO_CLAUSE_CAN_ONLY_CONSIST_OF_DIGITS =
      "%s is illegal, unquoted node name in select into clause can only consist of digits, characters, $, { and }";
  public static final String S_IS_ILLEGAL_IDENTIFIER_NOT_ENCLOSED_WITH_BACKTICKS_CAN_ONLY_CONSIST_OF_DIGITS =
      "%s is illegal, identifier not enclosed with backticks can only consist of digits, characters and underscore.";
  public static final String INPUT_TIME_FORMAT_S_ERROR =
      "Input time format %s error. ";
  public static final String INPUT_LIKE_YYYY_MM_DD_HH_MM_SS_YYYY_MM_DDTHH_MM_SS_OR =
      "Input like yyyy-MM-dd HH:mm:ss, yyyy-MM-ddTHH:mm:ss or ";
  public static final String REFER_TO_USER_DOCUMENT_FOR_MORE_INFO =
      "refer to user document for more info.";
  public static final String GRANT_OPTION_IS_DISABLED_PLEASE_CHECK_THE_PARAMETER_ENABLE_GRANT_OPTION =
      "Grant Option is disabled, Please check the parameter enable_grant_option.";
  public static final String S_CAN_ONLY_BE_SET_ON_PATH_ROOT_STAR_STAR =
      "[%s] can only be set on path: root.**";
  public static final String PRIVILEGE_TYPE =
      "Privilege type ";
  public static final String IS_DEPRECATED_USE =
      " is deprecated, use ";
  public static final String TO_INSTEAD_IT =
      " to instead it";
  public static final String INVALID_FUNCTION_EXPRESSION_ALL_THE_ARGUMENTS_ARE_CONSTANT_OPERANDS =
      "Invalid function expression, all the arguments are constant operands: ";
  public static final String ERROR_SIZE_OF_INPUT_EXPRESSIONS_EXPRESSION_S_ACTUAL_SIZE_S_EXPECTED_SIZE_S =
      "Error size of input expressions. expression: %s, actual size: %s, expected size: %s.";
  public static final String CAN_NOT_PARSE_S_TO_LONG_VALUE =
      "Can not parse %s to long value";
  public static final String THERE_S_DUPLICATE_S_IN_TAG_OR_ATTRIBUTE_CLAUSE =
      "There's duplicate [%s] in tag or attribute clause.";
  public static final String NOT_SUPPORT_FOR_THIS_SQL_IN_CREATE_PIPE_PLEASE_ENTER_PIPE_NAME =
      "Not support for this sql in CREATE PIPE, please enter pipe name.";
  public static final String NOT_SUPPORT_FOR_THIS_SQL_IN_ALTER_PIPE_PLEASE_ENTER_PIPE_NAME =
      "Not support for this sql in ALTER PIPE, please enter pipe name.";
  public static final String NOT_SUPPORT_FOR_THIS_SQL_IN_CREATE_TOPIC_PLEASE_ENTER_TOPICNAME =
      "Not support for this sql in CREATE TOPIC, please enter topicName.";
  public static final String NOT_SUPPORT_FOR_THIS_SQL_IN_DROP_TOPIC_PLEASE_ENTER_TOPICNAME =
      "Not support for this sql in DROP TOPIC, please enter topicName.";
  public static final String NOT_SUPPORT_FOR_THIS_SQL_IN_DROP_SUBSCRIPTION_PLEASE_ENTER_SUBSCRIPTIONID =
      "Not support for this sql in DROP SUBSCRIPTION, please enter subscriptionId.";
  public static final String PLEASE_SET_THE_CORRECT_REQUEST_TYPE =
      "Please set the correct request type: ";
  public static final String WHEN_SETTING_THE_REQUEST_THE_UNIT_IS_INCORRECT_PLEASE_USE_SEC_MIN_HOUR_DAY_AS_THE_UNIT =
      "When setting the request, the unit is incorrect. Please use 'sec', 'min', 'hour', 'day' as the unit";
  public static final String WHEN_SETTING_THE_SIZE_TIME_THE_UNIT_IS_INCORRECT_PLEASE_USE_B_K_M_G_P_T_AS_THE_UNIT =
      "When setting the size/time, the unit is incorrect. Please use 'B', 'K', 'M', 'G', 'P', 'T' as the unit";
  public static final String WHEN_SETTING_THE_DISK_SIZE_THE_UNIT_IS_INCORRECT_PLEASE_USE_M_G_P_T_AS_THE_UNIT =
      "When setting the disk size, the unit is incorrect. Please use 'M', 'G', 'P', 'T' as the unit";
  public static final String WINDOW_FUNCTION_E_G_HEAD_TAIL_COUNT_SHOULD_BE_SET_IN_VALUE_WHEN_KEY_IS_WINDOW_IN_CALL =
      "Window Function(e.g. HEAD, TAIL, COUNT) should be set in value when key is 'WINDOW' in CALL INFERENCE";
  public static final String THE_OUTPUT_TYPE_OF_THE_EXPRESSION_IN_HAVING_CLAUSE_SHOULD_BE_BOOLEAN_ACTUAL_DATA_TYPE_S =
      "The output type of the expression in HAVING clause should be BOOLEAN, actual data type: %s.";
  public static final String IN =
      " in ";
  public static final String START_TIME_D_IS_GREATER_THAN_END_TIME_D =
      "Start time %d is greater than end time %d";
  public static final String THE_COLUMN =
      "The column '";
  public static final String DOES_NOT_EXIST_OR_IS_NOT_A_TAG_COLUMN =
      "' does not exist or is not a tag column";
  public static final String THE_RIGHT_HAND_VALUE_OF_TIME_PREDICATE_MUST_BE_A_LONG =
      "The right hand value of time predicate must be a long: ";
  public static final String THE_OPERATOR_OF_TIME_PREDICATE_MUST_BE_LT_LT_EQ_GT_OR_GT_EQ =
      "The operator of time predicate must be <, <=, >, or >=: ";
  public static final String THE_RIGHT_HAND_VALUE_OF_TAG_PREDICATE_CANNOT_BE_NULL_WITH_EQ_OPERATOR_PLEASE_USE_IS_NULL =
      "The right hand value of tag predicate cannot be null with '=' operator, please use 'IS NULL' instead";
  public static final String THE_RIGHT_HAND_VALUE_OF_TAG_PREDICATE_MUST_BE_A_STRING =
      "The right hand value of tag predicate must be a string: ";
  public static final String SELECT_INTO_PLACEHOLDER_CAN_ONLY_BE_USED_AT_THE_END_OF_THE_PATH =
      "select into: placeholder `::` can only be used at the end of the path.";
  public static final String SELECT_INTO_THE_I_OF_DOLLAR_I_SHOULD_BE_GREATER_THAN_0_AND_EQUAL_TO_OR_LESS_THAN_THE =
      "select into: the i of ${i} should be greater than 0 and equal to or less than the length of queried path prefix.";
  public static final String ALIAS_S_CAN_ONLY_BE_MATCHED_WITH_ONE_RESULT_COLUMN =
      "alias '%s' can only be matched with one result column";
  public static final String RESULT_COLUMN_S_WITH_MORE_THAN_ONE_ALIAS_S_S =
      "Result column %s with more than one alias[%s, %s]";
  public static final String THERE_ARE_TOO_MANY_CONJUNCTS_MORE_THAN_1000_IN_PREDICATE_AFTER_REWRITING_THIS_MAY_BE =
      "There are too many conjuncts (more than 1000) in predicate after rewriting, this may be caused by too many devices in query, try to use ALIGN BY DEVICE";
  public static final String CASE_EXPRESSION_TEXT_AND_OTHER_TYPES_CANNOT_EXIST_AT_THE_SAME_TIME =
      "CASE expression: TEXT and other types cannot exist at the same time";
  public static final String CASE_EXPRESSION_BOOLEAN_AND_OTHER_TYPES_CANNOT_EXIST_AT_THE_SAME_TIME =
      "CASE expression: BOOLEAN and other types cannot exist at the same time";
  public static final String THE_EXPRESSION_IN_THE_WHEN_CLAUSE_MUST_RETURN_BOOLEAN_EXPRESSION_S_ACTUAL_DATA_TYPE_S =
      "The expression in the WHEN clause must return BOOLEAN. expression: %s, actual data type: %s.";
  public static final String INVALID_INPUT_EXPRESSION_DATA_TYPE_EXPRESSION_S_ACTUAL_DATA_TYPE_S_EXPECTED_DATA_TYPE_S =
      "Invalid input expression data type. expression: %s, actual data type: %s, expected data type(s): %s.";
  public static final String S_IN_ORDER_BY_CLAUSE_DOESN_T_EXIST =
      "%s in order by clause doesn't exist.";
  public static final String S_IN_ORDER_BY_CLAUSE_SHOULDN_T_REFER_TO_MORE_THAN_ONE_TIMESERIES =
      "%s in order by clause shouldn't refer to more than one timeseries.";
  public static final String THE_DATA_TYPE_OF_S_IS_NOT_COMPARABLE =
      "The data type of %s is not comparable";
  public static final String GROUP_BY_LEVEL_THE_DATA_TYPES_OF_THE_SAME_OUTPUT_COLUMN_S_SHOULD_BE_THE_SAME =
      "GROUP BY LEVEL: the data types of the same output column[%s] should be the same.";
  public static final String CROSS_DEVICE_QUERIES_ARE_NOT_SUPPORTED_IN_ALIGN_BY_DEVICE_QUERIES =
      "Cross-device queries are not supported in ALIGN BY DEVICE queries.";
  public static final String VIEWS_OR_MEASUREMENT_ALIASES_REPRESENTING_THE_SAME_DATA_SOURCE =
      "Views or measurement aliases representing the same data source ";
  public static final String CANNOT_BE_QUERIED_CONCURRENTLY_IN_ALIGN_BY_DEVICE_QUERIES =
      "cannot be queried concurrently in ALIGN BY DEVICE queries.";
  public static final String THE_TYPE_OF_SQL_RESULT_COLUMN_S_IN_D_SHOULD_BE_NUMERIC_WHEN_INFERENCE =
      "The type of SQL result column [%s in %d] should be numeric when inference";
  public static final String S_IN_ORDER_BY_CLAUSE_DOESN_T_EXIST_IN_THE_RESULT_OF_LAST_QUERY =
      "%s in order by clause doesn't exist in the result of last query.";
  public static final String S_IN_GROUP_BY_CLAUSE_DOESN_T_EXIST =
      "%s in group by clause doesn't exist.";
  public static final String S_IN_GROUP_BY_CLAUSE_SHOULDN_T_REFER_TO_MORE_THAN_ONE_TIMESERIES =
      "%s in group by clause shouldn't refer to more than one timeseries.";
  public static final String PLEASE_CHECK_THE_KEEP_CONDITION_S =
      "Please check the keep condition ([%s]), ";
  public static final String IT_NEED_TO_BE_A_CONSTANT_OR_A_COMPARE_EXPRESSION_CONSTRUCTED_BY_KEEP_AND_A_LONG_NUMBER =
      "it need to be a constant or a compare expression constructed by 'keep' and a long number.";
  public static final String THE_QUERY_TIME_RANGE_SHOULD_BE_SPECIFIED_IN_THE_GROUP_BY_TIME_CLAUSE =
      "The query time range should be specified in the GROUP BY TIME clause.";
  public static final String VIEW_PATH_S_OF_SOURCE_COLUMN_S_IS_ILLEGAL_PATH =
      "View path %s of source column %s is illegal path";
  public static final String ALIGN_BY_DEVICE_THE_DATA_TYPES_OF_THE_SAME_MEASUREMENT_COLUMN_SHOULD_BE_THE_SAME_ACROSS =
      "ALIGN BY DEVICE: the data types of the same measurement column should be the same across devices.";
  public static final String ALIAS_S_CAN_ONLY_BE_MATCHED_WITH_ONE_TIME_SERIES =
      "alias '%s' can only be matched with one time series";
  public static final String TAG_AND_ATTRIBUTE_SHOULDN_T_HAVE_THE_SAME_PROPERTY_KEY_S =
      "Tag and attribute shouldn't have the same property key [%s]";
  public static final String S_IS_NOT_A_LEGAL_PROP =
      "%s is not a legal prop.";
  public static final String MEASUREMENT_UNDER_AN_ALIGNED_DEVICE_IS_NOT_ALLOWED_TO_HAVE_THE_SAME_MEASUREMENT_NAME =
      "Measurement under an aligned device is not allowed to have the same measurement name";
  public static final String VALUE_FILTER_CAN_T_EXIST_IN_THE_CONDITION_OF_SHOW_COUNT_CLAUSE_ONLY_TIME_CONDITION =
      "Value Filter can't exist in the condition of SHOW/COUNT clause, only time condition supported";
  public static final String TIME_CONDITION_CAN_T_BE_EMPTY_IN_THE_CONDITION_OF_SHOW_COUNT_CLAUSE =
      "Time condition can't be empty in the condition of SHOW/COUNT clause";
  public static final String MEASUREMENT_UNDER_TEMPLATE_IS_NOT_ALLOWED_TO_HAVE_THE_SAME_MEASUREMENT_NAME =
      "Measurement under template is not allowed to have the same measurement name";
  public static final String THE_SUFFIX_PATHS_CAN_ONLY_BE_MEASUREMENT_OR_ONE_LEVEL_WILDCARD =
      "the suffix paths can only be measurement or one-level wildcard";
  public static final String AGGREGATION_RESULTS_CANNOT_BE_AS_INPUT_OF_THE_AGGREGATION_FUNCTION =
      "Aggregation results cannot be as input of the aggregation function.";
  public static final String INPUT_OF_S_IS_ILLEGAL =
      "Input of '%s' is illegal.";
  public static final String RAW_DATA_AND_AGGREGATION_RESULT_HYBRID_INPUT_OF_S_IS_NOT_SUPPORTED =
      "Raw data and aggregation result hybrid input of '%s' is not supported.";
  public static final String ONLY_WRITABLE_VIEW_TIMESERIES_ARE_SUPPORTED_IN_ALIGN_BY_DEVICE_QUERIES =
      "Only writable view timeseries are supported in ALIGN BY DEVICE queries.";
  public static final String INPUT_SERIES_OF_SCALAR_FUNCTION_DIFF_ONLY_SUPPORTS_NUMERIC_DATA_TYPES_INT32_INT64_FLOAT =
      "Input series of Scalar function [DIFF] only supports numeric data types [INT32, INT64, FLOAT, DOUBLE]";
  public static final String ARGUMENT_EXCEPTION_THE_SCALAR_FUNCTION_SUBSTRING_NEEDS_AT_LEAST_ONE_ARGUMENT_IT_MUST_BE =
      "Argument exception,the scalar function [SUBSTRING] needs at least one argument,it must be a signed integer";
  public static final String SYNTAX_ERROR_PLEASE_CHECK_THAT_THE_PARAMETERS_OF_THE_FUNCTION_ARE_CORRECT =
      "Syntax error,please check that the parameters of the function are correct";
  public static final String UNSUPPORTED_DATA_TYPE_S_FOR_FUNCTION_SUBSTRING =
      "Unsupported data type %s for function SUBSTRING.";
  public static final String ARGUMENT_EXCEPTION_THE_SCALAR_FUNCTION_SUBSTRING_BEGINPOSITION_AND_LENGTH_MUST_BE =
      "Argument exception,the scalar function [SUBSTRING] beginPosition and length must be greater than 0";
  public static final String INPUT_SERIES_OF_SCALAR_FUNCTION_ROUND_ONLY_SUPPORTS_NUMERIC_DATA_TYPES_INT32_INT64_FLOAT =
      "Input series of Scalar function [ROUND] only supports numeric data types [INT32, INT64, FLOAT, DOUBLE]";
  public static final String UNSUPPORTED_DATA_TYPE_S_FOR_FUNCTION_REPLACE =
      "Unsupported data type %s for function REPLACE.";
  public static final String TIMESERIES_UNDER_THIS_DEVICE_ISS_ALIGNED_PLEASE_USE_CREATESTIMESERIES_OR_CHANGE_DEVICE =
      "TimeSeries under this device is%s aligned, please use create%sTimeSeries or change device. (Path: %s)";
  public static final String NOT =
      " not";
  public static final String ALIGNED =
      "Aligned";
  public static final String AUTO_CREATE_OR_VERIFY_SCHEMA_ERROR_DETAIL_S =
      "Auto create or verify schema error.  Detail: %s.";
  public static final String THE_FILE_S_IS_NOT_A_VALID_TSFILE_PLEASE_CHECK_THE_INPUT_FILE =
      "The file %s is not a valid tsfile. Please check the input file.";
  public static final String AUTO_CREATE_OR_VERIFY_SCHEMA_ERROR_WHEN_EXECUTING_STATEMENT_S_DETAIL_S =
      "Auto create or verify schema error when executing statement %s.  Detail: %s.";
  public static final String TTL_VALUE_MUST_BE_INF_OR_A_LONG_LITERAL_BUT_NOW_IS =
      "ttl value must be 'INF' or a long literal, but now is: ";
  public static final String COLUMNS_IN_TABLE_SHALL_NOT_SHARE_THE_SAME_NAME_S =
      "Columns in table shall not share the same name: '%s'.";
  public static final String THE_DUPLICATED_SOURCE_MEASUREMENT_S_IS_UNSUPPORTED_YET =
      "The duplicated source measurement %s is unsupported yet.";
  public static final String THE_LENGTH_OF_DATABASE_NAME_SHALL_NOT_EXCEED =
      "the length of database name shall not exceed ";
  public static final String THE_DATABASE_NAME_CAN_ONLY_CONTAIN_ENGLISH_OR_CHINESE_CHARACTERS_NUMBERS_BACKTICKS_AND =
      "the database name can only contain english or chinese characters, numbers, backticks and underscores.";
  public static final String IS_CURRENTLY_NOT_ALLOWED =
      "' is currently not allowed.";
  public static final String VALUE_MUST_BE_A_LONGLITERAL_BUT_NOW_IS =
      " value must be a LongLiteral, but now is ";
  public static final String VALUE =
      ", value: ";
  public static final String VALUE_MUST_BE_EQUAL_TO_OR_GREATER_THAN_0_BUT_NOW_IS =
      " value must be equal to or greater than 0, but now is: ";
  public static final String VALUE_MUST_BE_LOWER_THAN =
      " value must be lower than ";
  public static final String BUT_NOW_IS =
      ", but now is: ";
  public static final String FAILED_TO_CREATE_PIPE_S_SETTING_S_IS_NOT_ALLOWED =
      "Failed to create pipe %s, setting %s is not allowed.";
  public static final String FAILED_TO_S_PIPE_S_IN_IOTDB_SOURCE_PASSWORD_MUST_BE_SET_WHEN_THE_USERNAME_IS_SPECIFIED =
      "Failed to %s pipe %s, in iotdb-source, password must be set when the username is specified.";
  public static final String ALTER =
      "alter";
  public static final String CREATE =
      "create";
  public static final String FAILED_TO_S_PIPE_S_IN_WRITE_BACK_SINK_PASSWORD_MUST_BE_SET_WHEN_THE_USERNAME_IS =
      "Failed to %s pipe %s, in write-back-sink, password must be set when the username is specified.";
  public static final String FAILED_TO_ALTER_PIPE_S_MODIFYING_S_IS_NOT_ALLOWED =
      "Failed to alter pipe %s, modifying %s is not allowed.";
  public static final String FAILED_TO_ALTER_PIPE_THE_SOURCE_PLUGIN_OF_THE_PIPE_CANNOT_BE_CHANGED_FROM_S_TO_S =
      "Failed to alter pipe, the source plugin of the pipe cannot be changed from %s to %s";
  public static final String FAILED_TO_ALTER_PIPE_S_IN_IOTDB_SOURCE_PASSWORD_MUST_BE_SET_WHEN_THE_USERNAME_IS =
      "Failed to alter pipe %s, in iotdb-source, password must be set when the username is specified.";
  public static final String FAILED_TO_ALTER_PIPE_S_IN_WRITE_BACK_SINK_PASSWORD_MUST_BE_SET_WHEN_THE_USERNAME_IS =
      "Failed to alter pipe %s, in write-back-sink, password must be set when the username is specified.";
  public static final String PREPARED_STATEMENT_S_ALREADY_EXISTS =
      "Prepared statement '%s' already exists";
  public static final String INSUFFICIENT_MEMORY_FOR_PREPAREDSTATEMENT_S =
      "Insufficient memory for PreparedStatement '%s'. ";
  public static final String PLEASE_DEALLOCATE_SOME_PREPAREDSTATEMENTS_AND_TRY_AGAIN =
      "Please deallocate some PreparedStatements and try again.";
  public static final String THE_TABLE =
      "The table ";
  public static final String IS_A_BASE_TABLE_DOES_NOT_SUPPORT_SHOW_CREATE_VIEW =
      " is a base table, does not support show create view.";
  public static final String THE_PARAMETERS =
      "The parameters '";
  public static final String MUST_BE_CONSISTENT_ACROSS_THE_ENTIRE_CLUSTER_AND_ONLY_ONE_CAN_BE_SET_AT_A_TIME =
      "'  must be consistent across the entire cluster and only one can be set at a time.";
  public static final String CANNOT_INSERT_INTO_MULTIPLE_DATABASES_WITHIN_ONE_STATEMENT_PLEASE_SPLIT_THEM_MANUALLY =
      "Cannot insert into multiple databases within one statement, please split them manually";
  public static final String THE_MEASUREMENTLIST_S_SIZE_D_IS_NOT_CONSISTENT_WITH_THE_VALUELIST_S_SIZE_D =
      "the measurementList's size %d is not consistent with the valueList's size %d";
  public static final String THE_MEASUREMENTLIST_S_SIZE_D_IS_NOT_CONSISTENT_WITH_THE_COLUMNLIST_S_SIZE_D =
      "the measurementList's size %d is not consistent with the columnList's size %d";
  public static final String MEASUREMENT_CONTAINS_NULL_OR_EMPTY_STRING =
      "Measurement contains null or empty string: ";
  public static final String CAN_T_BE_USED_IN_GROUP_BY_TAG_IT_WILL_BE_SUPPORTED_IN_THE_FUTURE =
      " can't be used in group by tag. It will be supported in the future.";
  public static final String COMMON_QUERIES_AND_AGGREGATED_QUERIES_ARE_NOT_ALLOWED_TO_APPEAR_AT_THE_SAME_TIME =
      "Common queries and aggregated queries are not allowed to appear at the same time";
  public static final String EXPRESSION_OF_HAVING_CLAUSE_CAN_NOT_BE_USED_IN_NONAGGREGATIONQUERY =
      "Expression of HAVING clause can not be used in NonAggregationQuery";
  public static final String SORTING_BY_DEVICE_IS_ONLY_SUPPORTED_IN_ALIGN_BY_DEVICE_QUERIES =
      "Sorting by device is only supported in ALIGN BY DEVICE queries.";
  public static final String CQ_EVERY_INTERVAL_D_SHOULD_NOT_BE_LOWER_THAN_THE_CONTINUOUS_QUERY_MINIMUM_EVERY_INTERVAL =
      "CQ: Every interval [%d] should not be lower than the `continuous_query_minimum_every_interval` [%d] configured.";
  public static final String CQ_THE_START_TIME_OFFSET_SHOULD_BE_GREATER_THAN_END_TIME_OFFSET =
      "CQ: The start time offset should be greater than end time offset.";
  public static final String CQ_THE_START_TIME_OFFSET_SHOULD_BE_GREATER_THAN_OR_EQUAL_TO_EVERY_INTERVAL =
      "CQ: The start time offset should be greater than or equal to every interval.";
  public static final String CQ_SPECIFYING_TIME_RANGE_IN_GROUP_BY_TIME_CLAUSE_IS_PROHIBITED =
      "CQ: Specifying time range in GROUP BY TIME clause is prohibited.";
  public static final String CANNOT_CREATE_VIEWS_USING_DATA_SOURCES_WITH_CALCULATED_EXPRESSIONS_WHILE_USING_INTO_ITEM =
      "Cannot create views using data sources with calculated expressions while using into item.";
  public static final String TREE_DEVICE_VIEW_WITH_MULTIPLE_DATABASES =
      "Tree device view with multiple databases(";
  public static final String IS_UNSUPPORTED_YET =
      ") is unsupported yet.";
  public static final String COMPLEX_ASOF_MAIN_JOIN_EXPRESSION_S_IS_NOT_SUPPORTED =
      "Complex ASOF main join expression [%s] is not supported";
  public static final String UNEXPECTED_DESCRIPTOR_TYPE =
      "Unexpected descriptor type: ";
  public static final String WHEN_CLAUSE_OPERAND_TYPE_MUST_MATCH_CASE_OPERAND_TYPE_S_VS_S =
      "WHEN clause operand type must match CASE operand type: %s vs %s";
  public static final String ALL_RESULT_TYPES_MUST_BE_THE_SAME_S =
      "All result types must be the same: %s";
  public static final String DEFAULT_RESULT_TYPE_MUST_BE_THE_SAME_AS_WHEN_RESULT_TYPES_S_VS_S =
      "Default result type must be the same as WHEN result types: %s vs %s";
  public static final String ALL_OPERANDS_MUST_HAVE_THE_SAME_TYPE_S =
      "All operands must have the same type: %s";
  public static final String TO =
      " to ";
  public static final String SCALAR_FUNCTION =
      "Scalar function ";
  public static final String ONLY_SUPPORTS_ONE_NUMERIC_DATA_TYPES_INT32_INT64_FLOAT_DOUBLE_AND_ONE_BOOLEAN =
      " only supports one numeric data types [INT32, INT64, FLOAT, DOUBLE] and one boolean";
  public static final String ONLY_SUPPORTS_TWO_NUMERIC_DATA_TYPES_INT32_INT64_FLOAT_DOUBLE =
      " only supports two numeric data types [INT32, INT64, FLOAT, DOUBLE]";
  public static final String ONLY_ACCEPTS_TWO_OR_THREE_ARGUMENTS_AND_THEY_MUST_BE_TEXT_OR_STRING_DATA_TYPE =
      " only accepts two or three arguments and they must be text or string data type.";
  public static final String ONLY_ACCEPTS_TWO_OR_THREE_ARGUMENTS_AND_FIRST_MUST_BE_TEXT_OR_STRING_DATA_TYPE_SECOND =
      " only accepts two or three arguments and first must be text or string data type, second and third must be numeric data types [INT32, INT64]";
  public static final String ONLY_ACCEPTS_ONE_ARGUMENT_AND_IT_MUST_BE_TEXT_OR_STRING_OR_BLOB_OR_OBJECT_DATA_TYPE =
      " only accepts one argument and it must be text or string or blob or object data type.";
  public static final String ONLY_ACCEPTS_ONE_ARGUMENT_AND_IT_MUST_BE_TEXT_OR_STRING_DATA_TYPE =
      " only accepts one argument and it must be text or string data type.";
  public static final String ONLY_ACCEPTS_ONE_OR_TWO_ARGUMENTS_AND_THEY_MUST_BE_TEXT_OR_STRING_DATA_TYPE =
      " only accepts one or two arguments and they must be text or string data type.";
  public static final String ONLY_ACCEPTS_TWO_ARGUMENTS_AND_THEY_MUST_BE_TEXT_OR_STRING_DATA_TYPE =
      " only accepts two arguments and they must be text or string data type.";
  public static final String ONLY_ACCEPTS_TWO_OR_MORE_ARGUMENTS_AND_THEY_MUST_BE_TEXT_OR_STRING_DATA_TYPE =
      " only accepts two or more arguments and they must be text or string data type.";
  public static final String ONLY_ACCEPTS_ONE_ARGUMENT_AND_IT_MUST_BE_DOUBLE_FLOAT_INT32_OR_INT64_DATA_TYPE =
      " only accepts one argument and it must be Double, Float, Int32 or Int64 data type.";
  public static final String ACCEPTS_NO_ARGUMENT =
      " accepts no argument.";
  public static final String ONLY_ACCEPTS_TWO_OR_THREE_ARGUMENTS_AND_THE_SECOND_AND_THIRD_MUST_BE_TIMESTAMP_DATA_TYPE =
      " only accepts two or three arguments and the second and third must be TimeStamp data type.";
  public static final String MUST_HAVE_AT_LEAST_TWO_ARGUMENTS_AND_FIRST_ARGUMENT_PATTERN_MUST_BE_TEXT_OR_STRING_TYPE =
      " must have at least two arguments, and first argument pattern must be TEXT or STRING type.";
  public static final String MUST_HAVE_AT_LEAST_TWO_ARGUMENTS_AND_ALL_TYPE_MUST_BE_THE_SAME =
      " must have at least two arguments, and all type must be the same.";
  public static final String SCALAR_FUNCTION_S_ONLY_ACCEPTS_TWO_ARGUMENTS_AND_THEY_MUST_BE_INT32_OR_INT64_DATA_TYPE =
      "Scalar function %s only accepts two arguments and they must be Int32 or Int64 data type.";
  public static final String SCALAR_FUNCTION_S_ONLY_ACCEPTS_ONE_ARGUMENT_AND_IT_MUST_BE_INT32_OR_INT64_DATA_TYPE =
      "Scalar function %s only accepts one argument and it must be Int32 or Int64 data type.";
  public static final String SCALAR_FUNCTION_S_ONLY_ACCEPTS_ONE_ARGUMENT_AND_IT_MUST_BE_TEXT_STRING_OR_BLOB_DATA_TYPE =
      "Scalar function %s only accepts one argument and it must be TEXT, STRING, or BLOB data type.";
  public static final String SCALAR_FUNCTION_S_ONLY_ACCEPTS_ONE_ARGUMENT_AND_IT_MUST_BE_TEXT_OR_STRING_DATA_TYPE =
      "Scalar function %s only accepts one argument and it must be TEXT or STRING data type.";
  public static final String SCALAR_FUNCTION_S_ONLY_ACCEPTS_ONE_ARGUMENT_AND_IT_MUST_BE_TEXT_STRING_OR_BLOB_DATA_TYPE_2 =
      "Scalar function %s only accepts one argument and it must be TEXT, STRING, or BlOB data type.";
  public static final String SCALAR_FUNCTION_S_ONLY_ACCEPTS_TWO_ARGUMENTS_FIRST_ARGUMENT_MUST_BE_TEXT_STRING_OR_BLOB =
      "Scalar function %s only accepts two arguments, first argument must be TEXT, STRING, or BlOB type, second argument must be STRING OR TEXT type.";
  public static final String SCALAR_FUNCTION_S_ONLY_ACCEPTS_ONE_ARGUMENT_AND_IT_MUST_BE_INT32_DATA_TYPE =
      "Scalar function %s only accepts one argument and it must be Int32 data type.";
  public static final String SCALAR_FUNCTION_S_ONLY_ACCEPTS_ONE_ARGUMENT_AND_IT_MUST_BE_BLOB_DATA_TYPE =
      "Scalar function %s only accepts one argument and it must be BLOB data type.";
  public static final String SCALAR_FUNCTION_S_ONLY_ACCEPTS_ONE_ARGUMENT_AND_IT_MUST_BE_INT64_DATA_TYPE =
      "Scalar function %s only accepts one argument and it must be Int64 data type.";
  public static final String SCALAR_FUNCTION_S_ONLY_ACCEPTS_ONE_ARGUMENT_AND_IT_MUST_BE_FLOAT_DATA_TYPE =
      "Scalar function %s only accepts one argument and it must be Float data type.";
  public static final String SCALAR_FUNCTION_S_ONLY_ACCEPTS_ONE_ARGUMENT_AND_IT_MUST_BE_DOUBLE_DATA_TYPE =
      "Scalar function %s only accepts one argument and it must be Double data type.";
  public static final String SCALAR_FUNCTION_S_ONLY_ACCEPTS_THREE_ARGUMENTS_FIRST_ARGUMENT_MUST_BE_BLOB_TYPE =
      "Scalar function %s only accepts three arguments, first argument must be BlOB type, ";
  public static final String SECOND_ARGUMENT_MUST_BE_INT32_OR_INT64_TYPE_THIRD_ARGUMENT_MUST_BE_BLOB_TYPE =
      "second argument must be int32 or int64 type, third argument must be BLOB type.";
  public static final String AGGREGATE_FUNCTIONS_S_SHOULD_ONLY_HAVE_ONE_ARGUMENT =
      "Aggregate functions [%s] should only have one argument";
  public static final String AGGREGATE_FUNCTIONS_S_ONLY_SUPPORT_NUMERIC_DATA_TYPES_INT32_INT64_FLOAT_DOUBLE =
      "Aggregate functions [%s] only support numeric data types [INT32, INT64, FLOAT, DOUBLE]";
  public static final String ERROR_SIZE_OF_INPUT_EXPRESSIONS_EXPRESSION_S_ACTUAL_SIZE_S_EXPECTED_SIZE_2 =
      "Error size of input expressions. expression: %s, actual size: %s, expected size: [2].";
  public static final String AGGREGATE_FUNCTIONS_S_ONLY_SUPPORT_NUMERIC_DATA_TYPES_INT32_INT64_FLOAT_DOUBLE_TIMESTAMP =
      "Aggregate functions [%s] only support numeric data types [INT32, INT64, FLOAT, DOUBLE, TIMESTAMP]";
  public static final String ERROR_SIZE_OF_INPUT_EXPRESSIONS_EXPRESSION_S_ACTUAL_SIZE_S_EXPECTED_SIZE_1 =
      "Error size of input expressions. expression: %s, actual size: %s, expected size: [1].";
  public static final String AGGREGATE_FUNCTIONS_S_SHOULD_ONLY_HAVE_ONE_BOOLEAN_EXPRESSION_AS_ARGUMENT =
      "Aggregate functions [%s] should only have one boolean expression as argument";
  public static final String AGGREGATE_FUNCTIONS_S_SHOULD_ONLY_HAVE_ONE_OR_TWO_ARGUMENTS =
      "Aggregate functions [%s] should only have one or two arguments";
  public static final String SECOND_ARGUMENT_OF_AGGREGATE_FUNCTIONS_S_SHOULD_BE_ORDERABLE =
      "Second argument of Aggregate functions [%s] should be orderable";
  public static final String AGGREGATE_FUNCTIONS_S_SHOULD_ONLY_HAVE_TWO_OR_THREE_ARGUMENTS =
      "Aggregate functions [%s] should only have two or three arguments";
  public static final String AGGREGATE_FUNCTIONS_S_SHOULD_ONLY_HAVE_TWO_ARGUMENTS =
      "Aggregate functions [%s] should only have two arguments";
  public static final String SECOND_ARGUMENT_OF_AGGREGATE_FUNCTIONS_S_SHOULD_BE_NUMBERIC_TYPE_AND_DO_NOT_USE =
      "Second argument of Aggregate functions [%s] should be numberic type and do not use expression";
  public static final String AGGREGATION_FUNCTIONS_S_SHOULD_ONLY_HAVE_THREE_ARGUMENTS =
      "Aggregation functions [%s] should only have three arguments";
  public static final String AGGREGATION_FUNCTIONS_S_SHOULD_ONLY_HAVE_TWO_OR_THREE_ARGUMENTS =
      "Aggregation functions [%s] should only have two or three arguments";
  public static final String AGGREGATION_FUNCTIONS_S_SHOULD_HAVE_VALUE_COLUMN_AS_NUMERIC_TYPE_INT32_INT64_FLOAT =
      "Aggregation functions [%s] should have value column as numeric type [INT32, INT64, FLOAT, DOUBLE, TIMESTAMP]";
  public static final String AGGREGATION_FUNCTIONS_S_SHOULD_HAVE_PERCENTAGE_AS_DECIMAL_TYPE =
      "Aggregation functions [%s] should have percentage as decimal type";
  public static final String AGGREGATION_FUNCTIONS_S_DO_NOT_SUPPORT_WEIGHT_AS_S_TYPE =
      "Aggregation functions [%s] do not support weight as %s type";
  public static final String WINDOW_FUNCTION_S_SHOULD_ONLY_HAVE_ONE_ARGUMENT =
      "Window function [%s] should only have one argument";
  public static final String WINDOW_FUNCTION_NTH_VALUE_SHOULD_ONLY_HAVE_TWO_ARGUMENT_AND_SECOND_ARGUMENT_MUST_BE =
      "Window function [nth_value] should only have two argument, and second argument must be integer type";
  public static final String WINDOW_FUNCTION_S_SHOULD_ONLY_HAVE_ONE_TO_THREE_ARGUMENT =
      "Window function [%s] should only have one to three argument";
  public static final String WINDOW_FUNCTION_S_S_SECOND_ARGUMENT_MUST_BE_INTEGER_TYPE =
      "Window function [%s]'s second argument must be integer type";
  public static final String UPDATE_ATTRIBUTE_SHALL_SPECIFY_A_ATTRIBUTE_ONLY_ONCE =
      "Update attribute shall specify a attribute only once.";
  public static final String CANNOT_BE_RESOLVED =
      "cannot be resolved";
  public static final String IS_NOT_AN_ATTRIBUTE_OR_TAG_COLUMN =
      "is not an attribute or tag column";
  public static final String UPDATE_S_ATTRIBUTE_VALUE_MUST_BE_STRING_TEXT_OR_NULL =
      "Update's attribute value must be STRING, TEXT or null.";
  public static final String MULTIPLE_COLUMNS_FOUND_WITH_TIME_CATEGORY_IN_TABLE_SCHEMA =
      "Multiple columns found with TIME category in table schema";
  public static final String INSERT_COLUMN_NAME_DOES_NOT_EXIST_IN_TARGET_TABLE_S =
      "Insert column name does not exist in target table: %s";
  public static final String INSERT_COLUMN_NAME_IS_SPECIFIED_MORE_THAN_ONCE_S =
      "Insert column name is specified more than once: %s";
  public static final String INSERT_QUERY_HAS_MISMATCHED_COLUMN_TYPES_TABLE_S_QUERY_S =
      "Insert query has mismatched column types: Table: [%s], Query: [%s]";
  public static final String WITH_QUERY_NAME_S_SPECIFIED_MORE_THAN_ONCE =
      "WITH query name '%s' specified more than once";
  public static final String WITH_TABLE_NAME_IS_REFERENCED_IN_THE_BASE_RELATION_OF_RECURSION =
      "WITH table name is referenced in the base relation of recursion";
  public static final String MULTIPLE_RECURSIVE_REFERENCES_IN_THE_STEP_RELATION_OF_RECURSION =
      "multiple recursive references in the step relation of recursion";
  public static final String FETCH_FIRST_LIMIT_CLAUSE_IN_THE_STEP_RELATION_OF_RECURSION =
      "FETCH FIRST / LIMIT clause in the step relation of recursion";
  public static final String RECURSIVE_REFERENCE_OUTSIDE_OF_FROM_CLAUSE_OF_THE_STEP_RELATION_OF_RECURSION =
      "recursive reference outside of FROM clause of the step relation of recursion";
  public static final String IMMEDIATE_WITH_CLAUSE_IN_RECURSIVE_QUERY_IS_NOT_SUPPORTED =
      "immediate WITH clause in recursive query is not supported";
  public static final String IMMEDIATE_FILL_CLAUSE_IN_RECURSIVE_QUERY_IS_NOT_SUPPORTED =
      "immediate FILL clause in recursive query is not supported";
  public static final String IMMEDIATE_ORDER_BY_CLAUSE_IN_RECURSIVE_QUERY_IS_NOT_SUPPORTED =
      "immediate ORDER BY clause in recursive query is not supported";
  public static final String IMMEDIATE_OFFSET_CLAUSE_IN_RECURSIVE_QUERY_IS_NOT_SUPPORTED =
      "immediate OFFSET clause in recursive query is not supported";
  public static final String IMMEDIATE_FETCH_FIRST_LIMIT_CLAUSE_IN_RECURSIVE_QUERY_IS_NOT_SUPPORT =
      "immediate FETCH FIRST / LIMIT clause in recursive query is not support";
  public static final String BASE_AND_STEP_RELATIONS_OF_RECURSION_HAVE_DIFFERENT_NUMBER_OF_FIELDS_S_S =
      "base and step relations of recursion have different number of fields: %s, %s";
  public static final String RECURSION_STEP_RELATION_OUTPUT_TYPE_S_IS_NOT_COERCIBLE_TO_RECURSION_BASE_RELATION_OUTPUT =
      "recursion step relation output type (%s) is not coercible to recursion base relation output type (%s) at column %s";
  public static final String CANNOT_NEST_WINDOW_FUNCTIONS_OR_ROW_PATTERN_MEASURES_INSIDE_WINDOW_FUNCTION_ARGUMENTS =
      "Cannot nest window functions or row pattern measures inside window function arguments";
  public static final String DISTINCT_IN_WINDOW_FUNCTION_PARAMETERS_NOT_YET_SUPPORTED_S =
      "DISTINCT in window function parameters not yet supported: %s";
  public static final String S_FUNCTION_REQUIRES_AN_ORDER_BY_WINDOW_CLAUSE =
      "%s function requires an ORDER BY window clause";
  public static final String CANNOT_SPECIFY_WINDOW_FRAME_FOR_S_FUNCTION =
      "Cannot specify window frame for %s function";
  public static final String WINDOW_NAME_S_SPECIFIED_MORE_THAN_ONCE =
      "WINDOW name '%s' specified more than once";
  public static final String CANNOT_RESOLVE_WINDOW_NAME_S =
      "Cannot resolve WINDOW name %s";
  public static final String WINDOW_SPECIFICATION_WITH_NAMED_WINDOW_REFERENCE_CANNOT_SPECIFY_PARTITION_BY =
      "WINDOW specification with named WINDOW reference cannot specify PARTITION BY";
  public static final String CANNOT_SPECIFY_ORDER_BY_IF_REFERENCED_NAMED_WINDOW_SPECIFIES_ORDER_BY =
      "Cannot specify ORDER BY if referenced named WINDOW specifies ORDER BY";
  public static final String CANNOT_REFERENCE_NAMED_WINDOW_CONTAINING_FRAME_SPECIFICATION =
      "Cannot reference named WINDOW containing frame specification";
  public static final String WHERE_CLAUSE_MUST_EVALUATE_TO_A_BOOLEAN_ACTUAL_TYPE_S =
      "WHERE clause must evaluate to a boolean: actual type %s";
  public static final String MULTIPLE_DIFFERENT_COLUMNS_IN_THE_SAME_EXPRESSION_ARE_NOT_SUPPORTED =
      "Multiple different COLUMNS in the same expression are not supported";
  public static final String NO_MATCHING_COLUMNS_FOUND_THAT_MATCH_REGEX_S =
      "No matching columns found that match regex '%s'";
  public static final String S_ARE_NOT_SUPPORTED_NOW =
      "%s are not supported now";
  public static final String UNABLE_TO_RESOLVE_REFERENCE_S =
      "Unable to resolve reference %s";
  public static final String IDENTIFIERCHAINBASIS_GET_GETBASISTYPE_EQ_EQ_FIELD_OR_TARGET_EXPRESSION_ISN_T_A =
      "identifierChainBasis.get().getBasisType == FIELD or target expression isn't a QualifiedName";
  public static final String SELECT_STAR_FROM_OUTER_SCOPE_TABLE_NOT_SUPPORTED_WITH_ANONYMOUS_COLUMNS =
      "SELECT * from outer scope table not supported with anonymous columns";
  public static final String DISTINCT_CAN_ONLY_BE_APPLIED_TO_COMPARABLE_TYPES_ACTUAL_S =
      "DISTINCT can only be applied to comparable types (actual: %s)";
  public static final String DISTINCT_CAN_ONLY_BE_APPLIED_TO_COMPARABLE_TYPES_ACTUAL_S_S =
      "DISTINCT can only be applied to comparable types (actual: %s): %s";
  public static final String GROUP_BY_POSITION_S_IS_NOT_IN_SELECT_LIST =
      "GROUP BY position %s is not in select list";
  public static final String GROUP_BY_EXPRESSION_MUST_BE_A_COLUMN_REFERENCE_S =
      "GROUP BY expression must be a column reference: %s";
  public static final String S_IS_NOT_COMPARABLE_AND_THEREFORE_CANNOT_BE_USED_IN_GROUP_BY =
      "%s is not comparable, and therefore cannot be used in GROUP BY";
  public static final String GROUP_BY_HAS_MORE_THAN_S_GROUPING_SETS =
      "GROUP BY has more than %s grouping sets";
  public static final String HAVING_CLAUSE_MUST_EVALUATE_TO_A_BOOLEAN_ACTUAL_TYPE_S =
      "HAVING clause must evaluate to a boolean: actual type %s";
  public static final String S_QUERY_HAS_DIFFERENT_NUMBER_OF_FIELDS_D_D =
      "%s query has different number of fields: %d, %d";
  public static final String COLUMN_D_IN_S_QUERY_HAS_INCOMPATIBLE_TYPES_S_S =
      "column %d in %s query has incompatible types: %s, %s";
  public static final String TYPE_S_IS_NOT_COMPARABLE_AND_THEREFORE_CANNOT_BE_USED_IN_SS =
      "Type %s is not comparable and therefore cannot be used in %s%s";
  public static final String DISTINCT =
      " DISTINCT";
  public static final String AMBIGUOUS_COLUMN_S_IN_ROW_PATTERN_INPUT_RELATION =
      "ambiguous column: %s in row pattern input relation";
  public static final String S_IS_NOT_COMPARABLE_AND_THEREFORE_CANNOT_BE_USED_IN_PARTITION_BY =
      "%s is not comparable, and therefore cannot be used in PARTITION BY";
  public static final String S_IS_NOT_ORDERABLE_AND_THEREFORE_CANNOT_BE_USED_IN_ORDER_BY =
      "%s is not orderable, and therefore cannot be used in ORDER BY";
  public static final String EXPRESSION_DEFINING_A_LABEL_MUST_BE_BOOLEAN_ACTUAL_TYPE_S =
      "Expression defining a label must be boolean (actual type: %s)";
  public static final String VALUES_ROWS_HAVE_MISMATCHED_SIZES_S_VS_S =
      "Values rows have mismatched sizes: %s vs %s";
  public static final String TYPE_OF_ROW_D_COLUMN_D_IS_MISMATCHED_EXPECTED_S_ACTUAL_S =
      "Type of row %d column %d is mismatched, expected: %s, actual: %s";
  public static final String TYPE_OF_ROW_D_IS_MISMATCHED_EXPECTED_S_ACTUAL_S =
      "Type of row %d is mismatched, expected: %s, actual: %s";
  public static final String COLUMN_ALIAS_LIST_HAS_S_ENTRIES_BUT_S_HAS_S_COLUMNS_AVAILABLE =
      "Column alias list has %s entries but '%s' has %s columns available";
  public static final String JOIN_ON_CLAUSE_MUST_EVALUATE_TO_A_BOOLEAN_ACTUAL_TYPE_S =
      "JOIN ON clause must evaluate to a boolean: actual type %s";
  public static final String ASOF_MAIN_JOIN_EXPRESSION_MUST_EVALUATE_TO_A_BOOLEAN_ACTUAL_TYPE_S =
      "ASOF main JOIN expression must evaluate to a boolean: actual type %s";
  public static final String LEFT_CHILD_TYPE_OF_ASOF_MAIN_JOIN_EXPRESSION_MUST_BE_TIMESTAMP_ACTUAL_TYPE_S =
      "left child type of ASOF main JOIN expression must be TIMESTAMP: actual type %s";
  public static final String RIGHT_CHILD_TYPE_OF_ASOF_MAIN_JOIN_EXPRESSION_MUST_BE_TIMESTAMP_ACTUAL_TYPE_S =
      "right child type of ASOF main JOIN expression must be TIMESTAMP: actual type %s";
  public static final String COLUMN_S_APPEARS_MULTIPLE_TIMES_IN_USING_CLAUSE =
      "Column '%s' appears multiple times in USING clause";
  public static final String COLUMN_S_IS_MISSING_FROM_LEFT_SIDE_OF_JOIN =
      "Column '%s' is missing from left side of join";
  public static final String COLUMN_S_IS_MISSING_FROM_RIGHT_SIDE_OF_JOIN =
      "Column '%s' is missing from right side of join";
  public static final String COLUMN_TYPES_OF_LEFT_AND_RIGHT_SIDE_ARE_DIFFERENT_LEFT_IS_S_RIGHT_IS_S =
      "Column Types of left and right side are different: left is %s, right is %s";
  public static final String CANNOT_INFER_TIME_COLUMN_FOR_S_FILL_THERE_EXISTS_NO_COLUMN_WHOSE_TYPE_IS_TIMESTAMP =
      "Cannot infer TIME_COLUMN for %s FILL, there exists no column whose type is TIMESTAMP";
  public static final String S_FILL_TIME_COLUMN_POSITION_S_IS_NOT_IN_SELECT_LIST =
      "%s FILL TIME_COLUMN position %s is not in select list";
  public static final String TYPE_OF_TIME_COLUMN_FOR_S_FILL_SHOULD_ONLY_BE_TIMESTAMP_BUT_TYPE_OF_THE_COLUMN_YOU =
      "Type of TIME_COLUMN for %s FILL should only be TIMESTAMP, but type of the column you specify is %s";
  public static final String S_FILL_FILL_GROUP_POSITION_S_IS_NOT_IN_SELECT_LIST =
      "%s FILL FILL_GROUP position %s is not in select list";
  public static final String TYPE_S_IS_NOT_ORDERABLE_AND_THEREFORE_CANNOT_BE_USED_IN_FILL_GROUP_S =
      "Type %s is not orderable, and therefore cannot be used in FILL_GROUP: %s";
  public static final String ORDER_BY_POSITION_S_IS_NOT_IN_SELECT_LIST =
      "ORDER BY position %s is not in select list";
  public static final String TYPE_S_IS_NOT_ORDERABLE_AND_THEREFORE_CANNOT_BE_USED_IN_ORDER_BY_S =
      "Type %s is not orderable, and therefore cannot be used in ORDER BY: %s";
  public static final String OFFSET_ROW_COUNT_MUST_BE_GREATER_OR_EQUAL_TO_0_ACTUAL_VALUE_S =
      "OFFSET row count must be greater or equal to 0 (actual value: %s)";
  public static final String LIMIT_ROW_COUNT_MUST_BE_GREATER_OR_EQUAL_TO_0_ACTUAL_VALUE_S =
      "LIMIT row count must be greater or equal to 0 (actual value: %s)";
  public static final String NON_CONSTANT_PARAMETER_VALUE_FOR_S_S =
      "Non constant parameter value for %s: %s";
  public static final String PARAMETER_VALUE_PROVIDED_FOR_S_IS_NULL_S =
      "Parameter value provided for %s is NULL: %s";
  public static final String RECURSIVE_REFERENCE_IN_LEFT_SOURCE_OF_S_JOIN =
      "recursive reference in left source of %s join";
  public static final String RECURSIVE_REFERENCE_IN_RIGHT_SOURCE_OF_S_JOIN =
      "recursive reference in right source of %s join";
  public static final String RECURSIVE_REFERENCE_IN_RIGHT_RELATION_OF_EXCEPT_S =
      "recursive reference in right relation of EXCEPT %s";
  public static final String DISTINCT_2 =
      "DISTINCT";
  public static final String ALL =
      "ALL";
  public static final String RECURSIVE_REFERENCE_IN_LEFT_RELATION_OF_EXCEPT_ALL =
      "recursive reference in left relation of EXCEPT ALL";
  public static final String FOR_SELECT_DISTINCT_ORDER_BY_EXPRESSIONS_MUST_APPEAR_IN_SELECT_LIST =
      "For SELECT DISTINCT, ORDER BY expressions must appear in select list";
  public static final String IS_CURRENTLY_NOT_ALLOWED_2 =
      " is currently not allowed.";
  public static final String DUPLICATE_PROPERTY_S =
      "Duplicate property: %s";
  public static final String TTL_VALUE_MUST_BE_A_INF_OR_A_LONGLITERAL_BUT_NOW_IS =
      "TTL' value must be a 'INF' or a LongLiteral, but now is: ";
  public static final String COLUMN_NAME_NOT_SPECIFIED_AT_POSITION_S =
      "Column name not specified at position %s";
  public static final String COLUMN_NAME_S_SPECIFIED_MORE_THAN_ONCE =
      "Column name '%s' specified more than once";
  public static final String COLUMN_ALIAS_LIST_HAS_S_ENTRIES_BUT_RELATION_HAS_S_COLUMNS =
      "Column alias list has %s entries but relation has %s columns";
  public static final String TABLE_FUNCTION_S_SPECIFIES_REQUIRED_COLUMNS_FROM_TABLE_ARGUMENT_S_WHICH_CANNOT_BE_FOUND =
      "Table function %s specifies required columns from table argument %s which cannot be found";
  public static final String TABLE_FUNCTION_S_SPECIFIES_EMPTY_LIST_OF_REQUIRED_COLUMNS_FROM_TABLE_ARGUMENT_S =
      "Table function %s specifies empty list of required columns from table argument %s";
  public static final String TABLE_FUNCTION_S_SPECIFIES_NEGATIVE_INDEX_OF_REQUIRED_COLUMN_FROM_TABLE_ARGUMENT_S =
      "Table function %s specifies negative index of required column from table argument %s";
  public static final String INDEX_S_OF_REQUIRED_COLUMN_FROM_TABLE_ARGUMENT_S_IS_OUT_OF_BOUNDS_FOR_TABLE_WITH_S =
      "Index %s of required column from table argument %s is out of bounds for table with %s columns";
  public static final String TABLE_FUNCTION_S_DOES_NOT_SPECIFY_REQUIRED_INPUT_COLUMNS_FROM_TABLE_ARGUMENT_S =
      "Table function %s does not specify required input columns from table argument %s";
  public static final String TOO_MANY_ARGUMENTS_EXPECTED_AT_MOST_S_ARGUMENTS_GOT_S_ARGUMENTS =
      "Too many arguments. Expected at most %s arguments, got %s arguments";
  public static final String ALL_ARGUMENTS_MUST_BE_PASSED_BY_NAME_OR_ALL_MUST_BE_PASSED_POSITIONALLY =
      "All arguments must be passed by name or all must be passed positionally";
  public static final String UNEXPECTED_ARGUMENT_NAME_S =
      "Unexpected argument name: %s";
  public static final String UNEXPECTED_TABLE_FUNCTION_ARGUMENT_TYPE_S =
      "Unexpected table function argument type: %s";
  public static final String INVALID_ARGUMENT_S_EXPECTED_TABLE_ARGUMENT_GOT_S =
      "Invalid argument %s. Expected table argument, got %s";
  public static final String INVALID_ARGUMENT_S_EXPECTED_SCALAR_ARGUMENT_GOT_S =
      "Invalid argument %s. Expected scalar argument, got %s";
  public static final String INVALID_ARGUMENT_S_PARTITIONING_CAN_NOT_BE_SPECIFIED_FOR_TABLE_ARGUMENT_WITH_ROW =
      "Invalid argument %s. Partitioning can not be specified for table argument with row semantics";
  public static final String INVALID_ARGUMENT_S_ORDERING_CAN_NOT_BE_SPECIFIED_FOR_TABLE_ARGUMENT_WITH_ROW_SEMANTICS =
      "Invalid argument %s. Ordering can not be specified for table argument with row semantics";
  public static final String INVALID_SCALAR_ARGUMENT_S_EXPECTED_TYPE_S_GOT_S =
      "Invalid scalar argument '%s'. Expected type %s, got %s";
  public static final String INVALID_SCALAR_ARGUMENT_S_S =
      "Invalid scalar argument %s, %s";
  public static final String MISSING_REQUIRED_ARGUMENT_S =
      "Missing required argument: %s";
  public static final String EXPECTED_COLUMN_REFERENCE_ACTUAL_S =
      "Expected column reference. Actual: %s";
  public static final String COLUMN_S_IS_NOT_PRESENT_IN_THE_INPUT_RELATION =
      "Column %s is not present in the input relation";
  public static final String S_CANNOT_CONTAIN_AGGREGATIONS_WINDOW_FUNCTIONS_OR_GROUPING_OPERATIONS_S =
      "%s cannot contain aggregations, window functions or grouping operations: %s";
  public static final String S_MUST_BE_AN_AGGREGATE_EXPRESSION_OR_APPEAR_IN_GROUP_BY_CLAUSE =
      "'%s' must be an aggregate expression or appear in GROUP BY clause";
  public static final String SUBQUERY_USES_S_WHICH_MUST_APPEAR_IN_GROUP_BY_CLAUSE =
      "Subquery uses '%s' which must appear in GROUP BY clause";
  public static final String CANNOT_NEST_AGGREGATIONS_INSIDE_AGGREGATION_S_S =
      "Cannot nest aggregations inside aggregation '%s': %s";
  public static final String UNION_PATTERN_VARIABLE_NAME_S_IS_A_DUPLICATE_OF_PRIMARY_PATTERN_VARIABLE_NAME =
      "union pattern variable name: %s is a duplicate of primary pattern variable name";
  public static final String UNION_PATTERN_VARIABLE_NAME_S_IS_DECLARED_TWICE =
      "union pattern variable name: %s is declared twice";
  public static final String SUBSET_ELEMENT_S_IS_NOT_A_PRIMARY_PATTERN_VARIABLE =
      "subset element: %s is not a primary pattern variable";
  public static final String DEFINED_VARIABLE_S_IS_NOT_A_PRIMARY_PATTERN_VARIABLE =
      "defined variable: %s is not a primary pattern variable";
  public static final String PATTERN_VARIABLE_WITH_NAME_S_IS_DEFINED_TWICE =
      "pattern variable with name: %s is defined twice";
  public static final String FINAL_SEMANTICS_IS_NOT_SUPPORTED_IN_DEFINE_CLAUSE =
      "FINAL semantics is not supported in DEFINE clause";
  public static final String PATTERN_QUANTIFIER_LOWER_BOUND_MUST_BE_GREATER_THAN_OR_EQUAL_TO_0 =
      "Pattern quantifier lower bound must be greater than or equal to 0";
  public static final String PATTERN_QUANTIFIER_LOWER_BOUND_MUST_NOT_EXCEED =
      "Pattern quantifier lower bound must not exceed ";
  public static final String PATTERN_QUANTIFIER_UPPER_BOUND_MUST_BE_GREATER_THAN_OR_EQUAL_TO_1 =
      "Pattern quantifier upper bound must be greater than or equal to 1";
  public static final String PATTERN_QUANTIFIER_UPPER_BOUND_MUST_NOT_EXCEED =
      "Pattern quantifier upper bound must not exceed ";
  public static final String PATTERN_QUANTIFIER_LOWER_BOUND_MUST_NOT_EXCEED_UPPER_BOUND =
      "Pattern quantifier lower bound must not exceed upper bound";
  public static final String S_IS_NOT_A_PRIMARY_OR_UNION_PATTERN_VARIABLE =
      "%s is not a primary or union pattern variable";
  public static final String NESTED_ROW_PATTERN_RECOGNITION_IN_ROW_PATTERN_RECOGNITION =
      "nested row pattern recognition in row pattern recognition";
  public static final String PATTERN_EXCLUSION_SYNTAX_IS_NOT_ALLOWED_WHEN_ALL_ROWS_PER_MATCH_WITH_UNMATCHED_ROWS_IS =
      "Pattern exclusion syntax is not allowed when ALL ROWS PER MATCH WITH UNMATCHED ROWS is specified";
  public static final String COLUMN_S_CANNOT_BE_RESOLVED =
      "Column '%s' cannot be resolved";
  public static final String REFERENCE_TO_COLUMN_S_FROM_OUTER_SCOPE_NOT_ALLOWED_IN_THIS_CONTEXT =
      "Reference to column '%s' from outer scope not allowed in this context";
  public static final String COLUMN_S_PREFIXED_WITH_LABEL_S_CANNOT_BE_RESOLVED =
      "Column %s prefixed with label %s cannot be resolved";
  public static final String EXPRESSION_S_IS_NOT_OF_TYPE_ROW =
      "Expression %s is not of type ROW";
  public static final String AMBIGUOUS_ROW_FIELD_REFERENCE_S =
      "Ambiguous row field reference: %s";
  public static final String TYPES_ARE_NOT_COMPARABLE_WITH_NULLIF_S_VS_S =
      "Types are not comparable with NULLIF: %s vs %s";
  public static final String CASE_OPERAND_TYPE_DOES_NOT_MATCH_WHEN_CLAUSE_OPERAND_TYPE_S_VS_S =
      "CASE operand type does not match WHEN clause operand type: %s vs %s";
  public static final String UNARY_OPERATOR_CANNOT_BY_APPLIED_TO_S_TYPE =
      "Unary '+' operator cannot by applied to %s type";
  public static final String LEFT_SIDE_OF_LIKE_EXPRESSION_MUST_EVALUATE_TO_TEXT_OR_STRING_TYPE_ACTUAL_S =
      "Left side of LIKE expression must evaluate to TEXT or STRING Type (actual: %s)";
  public static final String PATTERN_FOR_LIKE_EXPRESSION_MUST_EVALUATE_TO_TEXT_OR_STRING_TYPE_ACTUAL_S =
      "Pattern for LIKE expression must evaluate to TEXT or STRING Type (actual: %s)";
  public static final String ESCAPE_FOR_LIKE_EXPRESSION_MUST_EVALUATE_TO_TEXT_OR_STRING_TYPE_ACTUAL_S =
      "Escape for LIKE expression must evaluate to TEXT or STRING Type (actual: %s)";
  public static final String LABEL_STAR_SYNTAX_IS_ONLY_SUPPORTED_AS_THE_ONLY_ARGUMENT_OF_ROW_PATTERN_COUNT_FUNCTION =
      "label.* syntax is only supported as the only argument of row pattern count function";
  public static final String CANNOT_USE_DISTINCT_WITH_AGGREGATE_FUNCTION_IN_PATTERN_RECOGNITION_CONTEXT =
      "Cannot use DISTINCT with aggregate function in pattern recognition context";
  public static final String S_SEMANTICS_IS_NOT_SUPPORTED_OUT_OF_PATTERN_RECOGNITION_CONTEXT =
      "%s semantics is not supported out of pattern recognition context";
  public static final String S_SEMANTICS_IS_SUPPORTED_ONLY_FOR_FIRST_LAST_AND_AGGREGATION_FUNCTIONS_ACTUAL_S =
      "%s semantics is supported only for FIRST(), LAST() and aggregation functions. Actual: %s";
  public static final String THE_SECOND_ARGUMENT_OF_S_FUNCTION_MUST_BE_ACTUAL_TIME_NAME =
      "The second argument of %s function must be actual time name";
  public static final String THE_THIRD_ARGUMENT_OF_S_FUNCTION_MUST_BE_ACTUAL_TIME_NAME =
      "The third argument of %s function must be actual time name";
  public static final String TOO_MANY_ARGUMENTS_FOR_FUNCTION_CALL_S =
      "Too many arguments for function call %s()";
  public static final String S_IS_NOT_A_PRIMARY_PATTERN_VARIABLE_OR_SUBSET_NAME =
      "%s is not a primary pattern variable or subset name";
  public static final String MISSING_VALID_TIME_COLUMN_THE_TABLE_MUST_CONTAIN_EITHER_A_COLUMN_WITH_THE_TIME_CATEGORY =
      "Missing valid time column. The table must contain either a column with the TIME category or at least one TIMESTAMP column.";
  public static final String CLASSIFIER_PATTERN_RECOGNITION_FUNCTION_TAKES_NO_ARGUMENTS_OR_1_ARGUMENT =
      "CLASSIFIER pattern recognition function takes no arguments or 1 argument";
  public static final String CLASSIFIER_FUNCTION_ARGUMENT_SHOULD_BE_PRIMARY_PATTERN_VARIABLE_OR_SUBSET_NAME_ACTUAL_S =
      "CLASSIFIER function argument should be primary pattern variable or subset name. Actual: %s";
  public static final String CANNOT_USE_DISTINCT_WITH_S_PATTERN_RECOGNITION_FUNCTION =
      "Cannot use DISTINCT with %s pattern recognition function";
  public static final String S_SEMANTICS_IS_NOT_SUPPORTED_WITH_S_PATTERN_RECOGNITION_FUNCTION =
      "%s semantics is not supported with %s pattern recognition function";
  public static final String S_PATTERN_RECOGNITION_FUNCTION_REQUIRES_1_OR_2_ARGUMENTS =
      "%s pattern recognition function requires 1 or 2 arguments";
  public static final String S_PATTERN_RECOGNITION_NAVIGATION_FUNCTION_REQUIRES_A_NUMBER_AS_THE_SECOND_ARGUMENT =
      "%s pattern recognition navigation function requires a number as the second argument";
  public static final String S_PATTERN_RECOGNITION_NAVIGATION_FUNCTION_REQUIRES_A_NON_NEGATIVE_NUMBER_AS_THE_SECOND =
      "%s pattern recognition navigation function requires a non-negative number as the second argument (actual: %s)";
  public static final String THE_SECOND_ARGUMENT_OF_S_PATTERN_RECOGNITION_NAVIGATION_FUNCTION_MUST_NOT_EXCEED_S =
      "The second argument of %s pattern recognition navigation function must not exceed %s (actual: %s)";
  public static final String CANNOT_NEST_S_PATTERN_NAVIGATION_FUNCTION_INSIDE_S_PATTERN_NAVIGATION_FUNCTION =
      "Cannot nest %s pattern navigation function inside %s pattern navigation function";
  public static final String CANNOT_NEST_MULTIPLE_PATTERN_NAVIGATION_FUNCTIONS_INSIDE_S_PATTERN_NAVIGATION_FUNCTION =
      "Cannot nest multiple pattern navigation functions inside %s pattern navigation function";
  public static final String IMMEDIATE_NESTING_IS_REQUIRED_FOR_PATTERN_NAVIGATION_FUNCTIONS =
      "Immediate nesting is required for pattern navigation functions";
  public static final String ALL_LABELS_AND_CLASSIFIERS_INSIDE_THE_CALL_TO_S_MUST_MATCH =
      "All labels and classifiers inside the call to '%s' must match";
  public static final String ALL_AGGREGATE_FUNCTION_ARGUMENTS_MUST_APPLY_TO_ROWS_MATCHED_WITH_THE_SAME_LABEL =
      "All aggregate function arguments must apply to rows matched with the same label";
  public static final String CANNOT_NEST_S_AGGREGATE_FUNCTION_INSIDE_S_FUNCTION =
      "Cannot nest %s aggregate function inside %s function";
  public static final String CANNOT_NEST_S_PATTERN_NAVIGATION_FUNCTION_INSIDE_S_FUNCTION =
      "Cannot nest %s pattern navigation function inside %s function";
  public static final String INVALID_PARAMETER_INDEX_S_MAX_VALUE_IS_S =
      "Invalid parameter index %s, max value is %s";
  public static final String CANNOT_CHECK_IF_S_IS_BETWEEN_S_AND_S =
      "Cannot check if %s is BETWEEN %s and %s";
  public static final String S_IS_NOT_COMPARABLE_AND_THEREFORE_CANNOT_BE_USED_IN_WINDOW_FUNCTION_PARTITION_BY =
      "%s is not comparable, and therefore cannot be used in window function PARTITION BY";
  public static final String S_IS_NOT_ORDERABLE_AND_THEREFORE_CANNOT_BE_USED_IN_WINDOW_FUNCTION_ORDER_BY =
      "%s is not orderable, and therefore cannot be used in window function ORDER BY";
  public static final String WINDOW_FRAME_STARTING_FROM_CURRENT_ROW_CANNOT_END_WITH_PRECEDING =
      "Window frame starting from CURRENT ROW cannot end with PRECEDING";
  public static final String WINDOW_FRAME_STARTING_FROM_FOLLOWING_CANNOT_END_WITH_PRECEDING =
      "Window frame starting from FOLLOWING cannot end with PRECEDING";
  public static final String WINDOW_FRAME_STARTING_FROM_FOLLOWING_CANNOT_END_WITH_CURRENT_ROW =
      "Window frame starting from FOLLOWING cannot end with CURRENT ROW";
  public static final String WINDOW_FRAME_ROWS_START_VALUE_TYPE_MUST_BE_EXACT_NUMERIC_TYPE_WITH_SCALE_0_ACTUAL_S =
      "Window frame ROWS start value type must be exact numeric type with scale 0 (actual %s)";
  public static final String WINDOW_FRAME_ROWS_END_VALUE_TYPE_MUST_BE_EXACT_NUMERIC_TYPE_WITH_SCALE_0_ACTUAL_S =
      "Window frame ROWS end value type must be exact numeric type with scale 0 (actual %s)";
  public static final String WINDOW_FRAME_OF_TYPE_GROUPS_PRECEDING_OR_FOLLOWING_REQUIRES_ORDER_BY =
      "Window frame of type GROUPS PRECEDING or FOLLOWING requires ORDER BY";
  public static final String WINDOW_FRAME_GROUPS_START_VALUE_TYPE_MUST_BE_EXACT_NUMERIC_TYPE_WITH_SCALE_0_ACTUAL_S =
      "Window frame GROUPS start value type must be exact numeric type with scale 0 (actual %s)";
  public static final String WINDOW_FRAME_OF_TYPE_RANGE_PRECEDING_OR_FOLLOWING_REQUIRES_ORDER_BY =
      "Window frame of type RANGE PRECEDING or FOLLOWING requires ORDER BY";
  public static final String WINDOW_FRAME_OF_TYPE_RANGE_PRECEDING_OR_FOLLOWING_REQUIRES_SINGLE_SORT_ITEM_IN_ORDER_BY =
      "Window frame of type RANGE PRECEDING or FOLLOWING requires single sort item in ORDER BY (actual: %s)";
  public static final String WINDOW_FRAME_OF_TYPE_RANGE_PRECEDING_OR_FOLLOWING_REQUIRES_THAT_SORT_ITEM_TYPE_BE =
      "Window frame of type RANGE PRECEDING or FOLLOWING requires that sort item type be numeric, datetime or interval (actual: %s)";
  public static final String WINDOW_FRAME_RANGE_VALUE_TYPE_S_NOT_COMPATIBLE_WITH_SORT_ITEM_TYPE_S =
      "Window frame RANGE value type (%s) not compatible with sort item type (%s)";
  public static final String TYPE_S_MUST_BE_ORDERABLE_IN_ORDER_TO_BE_USED_IN_QUANTIFIED_COMPARISON =
      "Type [%s] must be orderable in order to be used in quantified comparison";
  public static final String TYPE_S_MUST_BE_COMPARABLE_IN_ORDER_TO_BE_USED_IN_QUANTIFIED_COMPARISON =
      "Type [%s] must be comparable in order to be used in quantified comparison";
  public static final String NOT_YET_IMPLEMENTED_S =
      "not yet implemented: %s";
  public static final String S_MUST_EVALUATE_TO_A_S_ACTUAL_S =
      "%s must evaluate to a %s (actual: %s)";
  public static final String S_MUST_BE_THE_SAME_TYPE_OR_COERCIBLE_TO_A_COMMON_TYPE_CANNOT_FIND_COMMON_TYPE_BETWEEN_S =
      "%s must be the same type or coercible to a common type. Cannot find common type between %s and %s, all types (without duplicates): %s";
  public static final String PATTERN_RECOGNITION_FUNCTION_NAME_MUST_NOT_BE_QUALIFIED =
      "Pattern recognition function name must not be qualified: ";
  public static final String PATTERN_RECOGNITION_FUNCTION_NAME_MUST_NOT_BE_DELIMITED =
      "Pattern recognition function name must not be delimited: ";
  public static final String PATTERN_RECOGNITION_FUNCTION_NAMES_CANNOT_BE_LAST_OR_FIRST_USE_RPR_LAST_OR_RPR_FIRST =
      "Pattern recognition function names cannot be LAST or FIRST, use RPR_LAST or RPR_FIRST instead.";
  public static final String PARAMETER_NODE_MUST_HAVE_A_LOCATION =
      "Parameter node must have a location";
  public static final String INVALID_NUMBER_OF_PARAMETERS_EXPECTED_D_GOT_D =
      "Invalid number of parameters: expected %d, got %d";
  public static final String CANNOT_INSERT_IDENTIFIER_S_PLEASE_USE_STRING_LITERAL =
      "Cannot insert identifier %s, please use string literal";
  public static final String EXPRESSIONS_AND_COLUMNS_DO_NOT_MATCH_EXPRESSIONS_SIZE =
      "expressions and columns do not match, expressions size: ";
  public static final String COLUMNS_SIZE =
      ", columns size: ";
  public static final String TIMECOLUMNINDEX_OUT_OF_BOUND_D_D =
      "TimeColumnIndex out of bound: %d-%d";
  public static final String INCONSISTENT_NUMBERS_OF_NON_TIME_COLUMN_NAMES_AND_VALUES_D_D =
      "Inconsistent numbers of non-time column names and values: %d-%d";
  public static final String IS_NOT_SUPPORTED_FOR_PROPERTY_VALUE_OF_SET_CONFIGURATION =
      " is not supported for property value of 'set configuration'. ";
  public static final String NOTE_THAT_THE_SYNTAX_FOR_SET_CONFIGURATION_IN_THE_TREE_MODEL_IS_NOT_EXACTLY_THE_SAME_AS =
      "Note that the syntax for 'set configuration' in the tree model is not exactly the same as that in the table model.";
  public static final String UNSUPPORTED_COPY_TO_FORMAT_S_SUPPORTED_FORMATS_S =
      "Unsupported COPY TO format '%s'. Supported formats: %s";
  public static final String SIMULTANEOUS_SETTING_OF_MONTHLY_AND_NON_MONTHLY_INTERVALS_IS_NOT_SUPPORTED =
      "Simultaneous setting of monthly and non-monthly intervals is not supported.";
  public static final String DON_T_NEED_TO_SPECIFY_TIME_COLUMN_WHILE_EITHER_TIME_BOUND_OR_FILL_GROUP_PARAMETER_IS_NOT =
      "Don't need to specify TIME_COLUMN while either TIME_BOUND or FILL_GROUP parameter is not specified";
  public static final String MONTH_OR_YEAR_INTERVAL_IN_TOLERANCE_IS_NOT_SUPPORTED_NOW =
      "Month or year interval in tolerance is not supported now.";
  public static final String ASOF_JOIN_DOES_NOT_SUPPORT_S_TYPE_NOW =
      "ASOF JOIN does not support %s type now";
  public static final String THE_SECOND_ARGUMENT_OF_APPROX_COUNT_DISTINCT_FUNCTION_MUST_BE_A_LITERAL =
      "The second argument of 'approx_count_distinct' function must be a literal";
  public static final String THE_SECOND_AND_THIRD_ARGUMENT_OF_APPROX_MOST_FREQUENT_FUNCTION_MUST_BE_POSITIVE_INTEGER =
      "The second and third argument of 'approx_most_frequent' function must be positive integer literal";
  public static final String THE_SECOND_ARGUMENT_OF_APPROX_PERCENTILE_FUNCTION_PERCENTAGE_MUST_BE_A_DOUBLE_LITERAL =
      "The second argument of 'approx_percentile' function percentage must be a double literal";
  public static final String THE_THIRD_ARGUMENT_OF_APPROX_PERCENTILE_FUNCTION_PERCENTAGE_MUST_BE_A_DOUBLE_LITERAL =
      "The third argument of 'approx_percentile' function percentage must be a double literal";
  public static final String INCONSISTENT_COLUMN_CATEGORY_OF_COLUMN_S_S_S =
      "Inconsistent column category of column %s: %s/%s";
  public static final String COLUMN =
      "Column ";
  public static final String DOES_NOT_EXISTS_OR_FAILS_TO_BE =
      " does not exists or fails to be ";
  public static final String CREATED =
      "created";
  public static final String INCOMPATIBLE_DATA_TYPE_OF_COLUMN_S_S_S =
      "Incompatible data type of column %s: %s/%s";
  public static final String INLIST_LITERAL_FOR_TIMESTAMP_CAN_ONLY_BE_LONGLITERAL_DOUBLELITERAL_AND_GENERICLITERAL =
      "InList Literal for TIMESTAMP can only be LongLiteral, DoubleLiteral and GenericLiteral, current is ";
  public static final String THE_TIME_FIELD_COLUMNS_ARE_CURRENTLY_NOT_ALLOWED_IN_DEVICES_RELATED_OPERATIONS =
      "The TIME/FIELD columns are currently not allowed in devices related operations";
  public static final String UNKNOWN_COLUMN_CATEGORY_FOR_S_CANNOT_AUTO_CREATE_COLUMN =
      "Unknown column category for %s. Cannot auto create column.";
  public static final String UNKNOWN_COLUMN_DATA_TYPE_FOR_S_CANNOT_AUTO_CREATE_COLUMN =
      "Unknown column data type for %s. Cannot auto create column.";
  public static final String WRONG_CATEGORY_AT_COLUMN_S =
      "Wrong category at column %s.";
  public static final String MISSING_COLUMNS_S =
      "Missing columns %s.";
  public static final String DATATYPE_OF_TAG_COLUMN_SHOULD_ONLY_BE_STRING_CURRENT_IS =
      "DataType of TAG Column should only be STRING, current is ";
  public static final String DATATYPE_OF_ATTRIBUTE_COLUMN_SHOULD_ONLY_BE_STRING_CURRENT_IS =
      "DataType of ATTRIBUTE Column should only be STRING, current is ";
  public static final String DECORRELATION_FOR_LIMIT_WITH_ROW_COUNT_GREATER_THAN_1_IS_NOT_SUPPORTED_YET =
      "Decorrelation for LIMIT with row count greater than 1 is not supported yet";
  public static final String GIVEN_CORRELATED_SUBQUERY_IS_NOT_SUPPORTED =
      "Given correlated subquery is not supported";
  public static final String GIVEN_QUERIED_DATABASE_S_IS_NOT_EXIST =
      "Given queried database: %s is not exist!";

  // --- QueryEngine log messages (additional) ---
  public static final String INITIALIZED_SHARED_MEMORYBLOCK_COORDINATOR_WITH_ALL_AVAILABLE_MEMORY_ARG_BYTES =
      "Initialized shared MemoryBlock 'Coordinator' with all available memory: {} bytes";
  public static final String ERROR_OCCURRED_DURING_EXECUTING_UDAF_PLEASE_CHECK_WHETHER_THE_IMPLEMENTATION_OF_UDF_IS =
      "Error occurred during executing UDAF, please check whether the implementation of UDF is correct according to the udf-api description.";
  public static final String PROCESSGETTSBLOCKREQUEST_SEQUENCE_ID_IN_ARG_ARG =
      "[ProcessGetTsBlockRequest] sequence ID in [{}, {})";
  public static final String RECEIVED_ACKNOWLEDGEDATABLOCKEVENT_FOR_TSBLOCKS_WHOSE_SEQUENCE_ID_ARE_IN_ARG_ARG_FROM =
      "Received AcknowledgeDataBlockEvent for TsBlocks whose sequence ID are in [{}, {}) from {}.";
  public static final String RECEIVED_ACK_EVENT_BUT_TARGET_FRAGMENTINSTANCE_ARG_IS_NOT_FOUND =
      "received ACK event but target FragmentInstance[{}] is not found.";
  public static final String CLOSED_SOURCE_HANDLE_OF_SHUFFLESINKHANDLE_ARG_CHANNEL_INDEX_ARG =
      "Closed source handle of ShuffleSinkHandle {}, channel index: {}.";
  public static final String RECEIVED_CLOSESINKCHANNELEVENT_BUT_TARGET_FRAGMENTINSTANCE_ARG_IS_NOT_FOUND =
      "received CloseSinkChannelEvent but target FragmentInstance[{}] is not found.";
  public static final String NEW_DATA_BLOCK_EVENT_RECEIVED_FOR_PLAN_NODE_ARG_OF_ARG_FROM_ARG =
      "New data block event received, for plan node {} of {} from {}.";
  public static final String RECEIVED_NEWDATABLOCKEVENT_BUT_THE_DOWNSTREAM_FRAGMENTINSTANCE_ARG_IS_NOT_FOUND =
      "received NewDataBlockEvent but the downstream FragmentInstance[{}] is not found";
  public static final String END_OF_DATA_BLOCK_EVENT_RECEIVED_FOR_PLAN_NODE_ARG_OF_ARG_FROM_ARG =
      "End of data block event received, for plan node {} of {} from {}.";
  public static final String RECEIVED_ONENDOFDATABLOCKEVENT_BUT_THE_DOWNSTREAM_FRAGMENTINSTANCE_ARG_IS_NOT_FOUND =
      "received onEndOfDataBlockEvent but the downstream FragmentInstance[{}] is not found";
  public static final String CREATE_SOURCE_HANDLE_FROM_ARG_FOR_PLAN_NODE_ARG_OF_ARG =
      "Create source handle from {} for plan node {} of {}";
  public static final String THE_TASK_ARG_IS_ABORTED_ALL_OTHER_TASKS_IN_THE_SAME_QUERY_WILL_BE_CANCELLED =
      "The task {} is aborted. All other tasks in the same query will be cancelled";
  public static final String DRIVERTASKTIMEOUT_CURRENT_TIME_IS_ARG_DDL_OF_TASK_IS_ARG =
      "[DriverTaskTimeout] Current time is {}, ddl of task is {}";
  public static final String EXECUTOR_ARG_EXITS_BECAUSE_IT_S_INTERRUPTED_WE_WILL_PRODUCE_ANOTHER_THREAD_TO_REPLACE =
      "Executor {} exits because it's interrupted. We will produce another thread to replace.";
  public static final String CANNOT_RESERVE_ARG_MAX_ARG_BYTES_MEMORY_FROM_MEMORYPOOL_FOR_PLANNODEIDARG =
      "Cannot reserve {}(Max: {}) bytes memory from MemoryPool for planNodeId{}";
  public static final String BLOCKED_RESERVE_REQUEST_ARG_BYTES_MEMORY_FOR_PLANNODEIDARG =
      "Blocked reserve request: {} bytes memory for planNodeId{}";
  public static final String FAILED_TO_DO_THE_INITIALIZATION_FOR_DRIVER_ARG =
      "Failed to do the initialization for driver {} ";
  public static final String FAILED_TO_ACQUIRE_THE_READ_LOCK_OF_DATAREGION_ARG_FOR_ARG_TIMES =
      "Failed to acquire the read lock of DataRegion-{} for {} times";
  public static final String INTERRUPTED_WHEN_AWAIT_ON_ALLDRIVERSCLOSED_FRAGMENTINSTANCE_ID_IS_ARG =
      "Interrupted when await on allDriversClosed, FragmentInstance Id is {}";
  public static final String RELEASE_TVLIST_OWNED_BY_QUERY_ALLOCATE_SIZE_ARG_RELEASE_SIZE_ARG =
      "Release TVList owned by query: allocate size {}, release size {}";
  public static final String TVLIST_ARG_IS_RELEASED_BY_THE_QUERY_FRAGMENTINSTANCE_ID_IS_ARG =
      "TVList {} is released by the query, FragmentInstance Id is {}";
  public static final String MEMORYNOTENOUGHEXCEPTION_WHEN_TRANSFERRING_TVLIST_OWNERSHIP_FROM_QUERY_ARG_TO_ANOTHER =
      "MemoryNotEnoughException when transferring TVList ownership from query {} to another query {}.";
  public static final String UNEXPECTED_EXCEPTION_WHEN_TRANSFERRING_TVLIST_OWNERSHIP_FROM_QUERY_ARG_TO_ANOTHER_QUERY =
      "Unexpected Exception when transferring TVList ownership from query {} to another query {}.";
  public static final String TVLIST_ARG_IS_NOW_OWNED_BY_ANOTHER_QUERY_FRAGMENTINSTANCE_ID_IS_ARG =
      "TVList {} is now owned by another query, FragmentInstance Id is {}";
  public static final String GETTSBLOCKFROMQUEUE_TSBLOCK_ARG_SIZE_ARG =
      "[GetTsBlockFromQueue] TsBlock:{} size:{}";
  public static final String RECEIVENEWTSBLOCKNOTIFICATION_ARG_ARG_EACH_SIZE_IS_ARG =
      "[ReceiveNewTsBlockNotification] [{}, {}), each size is: {}";
  public static final String STARTPULLTSBLOCKSFROMREMOTE_ARG_ARG_ARG_ARG =
      "[StartPullTsBlocksFromRemote] {}-{} [{}, {}) ";
  public static final String SENDCLOSESINKCHANNELEVENT_TO_SHUFFLESINKHANDLE_ARG_INDEX_ARG =
      "[SendCloseSinkChannelEvent] to [ShuffleSinkHandle: {}, index: {}]).";
  public static final String SINKCHANNEL_STILL_RECEIVE_GETTING_TSBLOCK_REQUEST_AFTER_BEING_ABORTED_ARG_OR_CLOSED_ARG =
      "SinkChannel still receive getting TsBlock request after being aborted={} or closed={}";
  public static final String NOTIFYNEWTSBLOCK_ARG_ARG_TO_ARG_ARG =
      "[NotifyNewTsBlock] [{}, {}) to {}.{}";
  public static final String PLAINSHUFFLESTRATEGY_NEEDS_TO_DO_NOTHING_CURRENT_CHANNEL_INDEX_IS_ARG =
      "PlainShuffleStrategy needs to do nothing, current channel index is {}";
  public static final String LAYERROWWINDOWREADER_INDEX_OVERFLOW_BEGININDEX_ARG_ENDINDEX_ARG_WINDOWSIZE_ARG =
      "LayerRowWindowReader index overflow. beginIndex: {}, endIndex: {}, windowSize: {}.";
  public static final String CONSUMEMEMORY_CONSUME_ARG_CURRENT_REMAINING_MEMORY_ARG =
      "[ConsumeMemory] consume: {}, current remaining memory: {}";
  public static final String RELEASEMEMORY_RELEASE_ARG_CURRENT_REMAINING_MEMORY_ARG =
      "[ReleaseMemory] release: {}, current remaining memory: {}";
  public static final String MAXBYTESONEHANDLECANRESERVE_FOR_EXCHANGEOPERATOR_IS_ARG_EXCHANGESUMNUM_IS_ARG =
      "MaxBytesOneHandleCanReserve for ExchangeOperator is {}, exchangeSumNum is {}.";
  public static final String STATE_TRACKER_STARTS =
      "state tracker starts";
  public static final String DISPATCH_WRITE_FAILED_STATUS_ARG_CODE_ARG_MESSAGE_ARG_NODE_ARG =
      "Dispatch write failed. status: {}, code: {}, message: {}, node {}";
  public static final String CAN_T_EXECUTE_REQUEST_ON_NODE_ARG_IN_SECOND_TRY_ERROR_MSG_IS_ARG =
      "can't execute request on node  {} in second try, error msg is {}.";
  public static final String CAN_T_EXECUTE_REQUEST_ON_NODE_ARG_ERROR_MSG_IS_ARG_AND_WE_TRY_TO_RECONNECT_THIS_NODE =
      "can't execute request on node {}, error msg is {}, and we try to reconnect this node.";
  public static final String WRITE_LOCALLY_FAILED_TSSTATUS_ARG_MESSAGE_ARG =
      "write locally failed. TSStatus: {}, message: {}";
  public static final String DISPATCH_WRITE_FAILED_MESSAGE_ARG_NODE_ARG =
      "dispatch write failed. message: {}, node {}";
  public static final String DISPATCH_WRITE_FAILED_STATUS_ARG_CODE_ARG_MESSAGE_ARG_NODE_ARG_2 =
      "dispatch write failed. status: {}, code: {}, message: {}, node {}";
  public static final String LOGICAL_PLAN_IS_ARG =
      "logical plan is: \n {}";
  public static final String DISTRIBUTION_PLAN_DONE_FRAGMENT_INSTANCE_COUNT_IS_ARG_DETAILS_IS_ARG =
      "distribution plan done. Fragment instance count is {}, details is: \n {}";
  public static final String FAILED_TO_CHECK_TABLE_SCHEMA_WILL_SKIP_BECAUSE_SKIPFAILEDTABLESCHEMACHECK_IS_SET_TO_TRUE =
      "Failed to check table schema, will skip because skipFailedTableSchemaCheck is set to true, message: {}";
  public static final String FAILED_TO_CHECK_IF_DEVICE_ARG_IS_DELETED_BY_MODS_WILL_SEE_IT_AS_NOT_DELETED =
      "Failed to check if device {} is deleted by mods. Will see it as not deleted.";
  public static final String COLUMN_ARG_IN_TABLE_ARG_IS_NOT_FOUND_IN_IOTDB_WHILE_LOADING_TSFILE =
      "Column {} in table {} is not found in IoTDB while loading TsFile.";
  public static final String DEVICE_ARG_IS_NOT_IN_THE_TSFILEDEVICE2ISALIGNED_CACHE_ARG =
      "Device {} is not in the tsFileDevice2IsAligned cache {}.";
  public static final String LOADTSFILEANALYZER_CURRENT_DATANODE_IS_READ_ONLY_WILL_TRY_TO_CONVERT_TO_TABLETS_AND =
      "LoadTsFileAnalyzer: Current datanode is read only, will try to convert to tablets and insert later.";
  public static final String LOAD_ANALYSIS_STAGE_ARG_ARG_TSFILES_HAVE_BEEN_ANALYZED_PROGRESS_ARG_PERCENT =
      "Load - Analysis Stage: {}/{} tsfiles have been analyzed, progress: {}%";
  public static final String THE_FILE_ARG_IS_NOT_A_VALID_TSFILE_PLEASE_CHECK_THE_INPUT_FILE =
      "The file {} is not a valid tsfile. Please check the input file.";
  public static final String TSFILE_ARG_IS_A_ARG_MODEL_FILE =
      "TsFile {} is a {}-model file.";
  public static final String LOAD_FAILED_TO_CONVERT_MINI_TSFILE_ARG_TO_TABLETS_FROM_STATEMENT_ARG_STATUS_ARG =
      "Load: Failed to convert mini tsfile {} to tablets from statement {}. Status: {}.";
  public static final String LOAD_FAILED_TO_CONVERT_TO_TABLETS_FROM_STATEMENT_ARG_BECAUSE_FAILED_TO_READ_MODEL_INFO =
      "Load: Failed to convert to tablets from statement {} because failed to read model info from file, message: {}.";
  public static final String LOAD_FAILED_TO_CONVERT_TO_TABLETS_FROM_STATEMENT_ARG_STATUS_IS_NULL =
      "Load: Failed to convert to tablets from statement {}. Status is null.";
  public static final String LOAD_FAILED_TO_CONVERT_TO_TABLETS_FROM_STATEMENT_ARG_STATUS_ARG =
      "Load: Failed to convert to tablets from statement {}. Status: {}";
  public static final String LOAD_FAILED_TO_CONVERT_TO_TABLETS_FROM_STATEMENT_ARG_BECAUSE_EXCEPTION_ARG =
      "Load: Failed to convert to tablets from statement {} because exception: {}";
  public static final String FAILED_TO_CHECK_IF_DEVICE_ARG_TIMESERIES_ARG_IS_DELETED_BY_MODS_WILL_SEE_IT_AS_NOT =
      "Failed to check if device {}, timeseries {} is deleted by mods. Will see it as not deleted.";
  public static final String CREATE_DATABASE_ERROR_STATEMENT_ARG_RESULT_STATUS_IS_ARG =
      "Create database error, statement: {}, result status is: {}";
  public static final String MEASUREMENT_ARGARGARG_DATATYPE_NOT_MATCH_TSFILE_ARG_IOTDB_ARG =
      "Measurement {}{}{} datatype not match, TsFile: {}, IoTDB: {}";
  public static final String ENCODING_TYPE_NOT_MATCH_MEASUREMENT_ARGARGARG =
      "Encoding type not match, measurement: {}{}{}, ";
  public static final String TSFILE_ENCODING_ARG_IOTDB_ENCODING_ARG =
      "TsFile encoding: {}, IoTDB encoding: {}";
  public static final String COMPRESSOR_NOT_MATCH_MEASUREMENT_ARGARGARG =
      "Compressor not match, measurement: {}{}{}, ";
  public static final String TSFILE_COMPRESSOR_ARG_IOTDB_COMPRESSOR_ARG =
      "TsFile compressor: {}, IoTDB compressor: {}";
  public static final String ARG_CACHE_FAILED_TO_CREATE_DATABASE_ARG =
      "[{} Cache] failed to create database {}";
  public static final String ARG_CACHE_MISS_WHEN_SEARCH_DEVICE_ARG =
      "[{} Cache] miss when search device {}";
  public static final String ARG_CACHE_HIT_WHEN_SEARCH_DEVICE_ARG =
      "[{} Cache] hit when search device {}";
  public static final String UNEXPECTED_ERROR_WHEN_GETREGIONREPLICASET_STATUS_ARG_REGIONMAP_ARG =
      "Unexpected error when getRegionReplicaSet: status {}， regionMap: {}";
  public static final String ARG_CACHE_MISS_WHEN_SEARCH_DATABASE_ARG =
      "[{} Cache] miss when search database {}";
  public static final String ARG_CACHE_MISS_WHEN_SEARCH_TIME_PARTITION_ARG =
      "[{} Cache] miss when search time partition {}";
  public static final String FAILURES_HAPPENED_DURING_RUNNING_CONFIGEXECUTION_WHEN_EXECUTING_ARG_MESSAGE_ARG_STATUS =
      "Failures happened during running ConfigExecution when executing {}, message: {}, status: {}";
  public static final String FAILURES_HAPPENED_DURING_RUNNING_CONFIGEXECUTION_WHEN_EXECUTING_ARG =
      "Failures happened during running ConfigExecution when executing {}.";
  public static final String FAILED_TO_EXECUTE_CREATE_DATABASE_ARG_IN_CONFIG_NODE_STATUS_IS_ARG =
      "Failed to execute create database {} in config node, status is {}.";
  public static final String FAILED_TO_EXECUTE_ALTER_DATABASE_ARG_IN_CONFIG_NODE_STATUS_IS_ARG =
      "Failed to execute alter database {} in config node, status is {}.";
  public static final String FAILED_TO_EXECUTE_DELETE_DATABASE_ARG_IN_CONFIG_NODE_STATUS_IS_ARG =
      "Failed to execute delete database {} in config node, status is {}.";
  public static final String FAILED_TO_CREATE_FUNCTION_WHEN_TRY_TO_CREATE_ARG_ARG_INSTANCE_FIRST =
      "Failed to create function when try to create {}({}) instance first.";
  public static final String FAILED_TO_GET_EXECUTABLE_FOR_TRIGGER_ARG_USING_URI_ARG =
      "Failed to get executable for Trigger({}) using URI: {}.";
  public static final String FAILED_TO_CREATE_TRIGGER_WHEN_TRY_TO_CREATE_TRIGGER_ARG_INSTANCE_FIRST =
      "Failed to create trigger when try to create trigger({}) instance first.";
  public static final String ARG_FAILED_TO_CREATE_TRIGGER_ARG_TSSTATUS_IS_ARG =
      "[{}] Failed to create trigger {}. TSStatus is {}";
  public static final String FAILED_TO_GET_EXECUTABLE_FOR_PIPEPLUGIN_ARG_USING_URI_ARG =
      "Failed to get executable for PipePlugin({}) using URI: {}.";
  public static final String FAILED_TO_CREATE_PIPEPLUGIN_ARG_BECAUSE_THIS_PLUGIN_IS_NOT_DESIGNED_FOR_ARG_MODEL =
      "Failed to create PipePlugin({}) because this plugin is not designed for {} model.";
  public static final String FAILED_TO_CREATE_FUNCTION_WHEN_TRY_TO_CREATE_PIPEPLUGIN_ARG_INSTANCE_FIRST =
      "Failed to create function when try to create PipePlugin({}) instance first.";
  public static final String FAILED_TO_CREATE_PIPEPLUGIN_ARG_ARG_BECAUSE_ARG =
      "Failed to create PipePlugin {}({}) because {}";
  public static final String ARG_FAILED_TO_DROP_PIPE_PLUGIN_ARG =
      "[{}] Failed to drop pipe plugin {}.";
  public static final String FAILED_TO_EXECUTE_ARG_ARG_IN_CONFIG_NODE_STATUS_IS_ARG =
      "Failed to execute {} {} in config node, status is {}.";
  public static final String FAILED_TO_EXECUTE_ALTER_VIEW_ARG_BY_PIPE_STATUS_IS_ARG =
      "Failed to execute alter view {} by pipe, status is {}.";
  public static final String THE_DATANODE_TO_BE_REMOVED_IS_NOT_IN_THE_CLUSTER_OR_THE_INPUT_FORMAT_IS_INCORRECT =
      "The DataNode to be removed is not in the cluster, or the input format is incorrect.";
  public static final String SUBMIT_REMOVE_DATANODE_REQUEST_SUCCESSFULLY_BUT_THE_PROCESS_MAY_FAIL =
      "Submit remove-datanode request successfully, but the process may fail. ";
  public static final String MORE_DETAILS_ARE_SHOWN_IN_THE_LOGS_OF_CONFIGNODE_LEADER_AND_REMOVED_DATANODE =
      "more details are shown in the logs of confignode-leader and removed-datanode, ";
  public static final String AND_AFTER_THE_PROCESS_OF_REMOVING_DATANODE_ENDS_SUCCESSFULLY =
      "and after the process of removing datanode ends successfully, ";
  public static final String YOU_ARE_SUPPOSED_TO_DELETE_DIRECTORY_AND_DATA_OF_THE_REMOVED_DATANODE_MANUALLY =
      "you are supposed to delete directory and data of the removed-datanode manually";
  public static final String THE_CONFIGNODE_TO_BE_REMOVED_IS_NOT_IN_THE_CLUSTER_OR_THE_INPUT_FORMAT_IS_INCORRECT =
      "The ConfigNode to be removed is not in the cluster, or the input format is incorrect.";
  public static final String FAILED_TO_DROP_DATABASE_ARG_BECAUSE_IT_DOESN_T_EXIST =
      "Failed to DROP DATABASE {}, because it doesn't exist";
  public static final String FAILED_TO_ALLOCATE_ARG_BYTES_FROM_SHARED_MEMORYBLOCK_ARG_FOR_PREPAREDSTATEMENT_ARG =
      "Failed to allocate {} bytes from shared MemoryBlock '{}' for PreparedStatement '{}'";
  public static final String ALLOCATED_ARG_BYTES_FOR_PREPAREDSTATEMENT_ARG_FROM_SHARED_MEMORYBLOCK_ARG =
      "Allocated {} bytes for PreparedStatement '{}' from shared MemoryBlock '{}'. ";
  public static final String RELEASED_ARG_BYTES_FROM_SHARED_MEMORYBLOCK_ARG_FOR_PREPAREDSTATEMENT =
      "Released {} bytes from shared MemoryBlock '{}' for PreparedStatement. ";
  public static final String ATTEMPTED_TO_RELEASE_MEMORY_FROM_SHARED_MEMORYBLOCK_ARG_BUT_IT_IS_RELEASED =
      "Attempted to release memory from shared MemoryBlock '{}' but it is released";
  public static final String RELEASED_ARG_PREPAREDSTATEMENT_S_ARG_BYTES_TOTAL_FOR_SESSION_ARG =
      "Released {} PreparedStatement(s) ({} bytes total) for session {}";
  public static final String THE_PREFIX_OF_SOURCEKEY_IS_NOT_SOURCE_PLEASE_CHECK_THE_PARAMETERS_PASSED_IN_ARG =
      "The prefix of sourceKey is not 'source.'. Please check the parameters passed in: {}";
  public static final String LOADTSFILESCHEDULER_REGION_MIGRATION_WAS_DETECTED_DURING_LOADING_TSFILE_ARG_WILL_CONVERT =
      "LoadTsFileScheduler: Region migration was detected during loading TsFile {}, will convert to insertion to avoid data loss";
  public static final String LOAD_TSFILE_ARG_SUCCESSFULLY_LOAD_PROCESS_ARG_ARG =
      "Load TsFile {} Successfully, load process [{}/{}]";
  public static final String CAN_NOT_LOAD_TSFILE_ARG_LOAD_PROCESS_ARG_ARG =
      "Can not Load TsFile {}, load process [{}/{}]";
  public static final String LOAD_TSFILE_S_FAILED_WILL_TRY_TO_CONVERT_TO_TABLETS_AND_INSERT_FAILED_TSFILES_ARG =
      "Load TsFile(s) failed, will try to convert to tablets and insert. Failed TsFiles: {}";
  public static final String DISPATCH_TSFILEDATA_ERROR_WHEN_PARSING_TSFILE_S =
      "Dispatch TsFileData error when parsing TsFile %s.";
  public static final String PARSE_OR_SEND_TSFILE_S_ERROR =
      "Parse or send TsFile %s error.";
  public static final String DISPATCH_ONE_PIECE_TO_REPLICASET_ARG_ERROR_RESULT_STATUS_CODE_ARG =
      "Dispatch one piece to ReplicaSet {} error. Result status code {}. ";
  public static final String RESULT_STATUS_MESSAGE_ARG_DISPATCH_PIECE_NODE_ERROR_PERCENT_NARG =
      "Result status message {}. Dispatch piece node error:%n{}";
  public static final String SUB_STATUS_CODE_ARG_SUB_STATUS_MESSAGE_ARG =
      "Sub status code {}. Sub status message {}.";
  public static final String WAIT_FOR_LOADING_S_TIME_OUT =
      "Wait for loading %s time out.";
  public static final String DISPATCH_LOAD_COMMAND_ARG_OF_TSFILE_ARG_ERROR_TO_REPLICASETS_ARG_ERROR =
      "Dispatch load command {} of TsFile {} error to replicaSets {} error. ";
  public static final String RESULT_STATUS_CODE_ARG_RESULT_STATUS_MESSAGE_ARG =
      "Result status code {}. Result status message {}.";
  public static final String DISPATCH_TSFILE_S_ERROR_TO_LOCAL_ERROR_RESULT_STATUS_CODE_S =
      "Dispatch tsFile %s error to local error. Result status code %s. ";
  public static final String RESULT_STATUS_MESSAGE_S =
      "Result status message %s.";
  public static final String LOAD_SUCCESSFULLY_CONVERTED_TSFILE_ARG_INTO_TABLETS_AND_INSERTED =
      "Load: Successfully converted TsFile {} into tablets and inserted.";
  public static final String LOAD_FAILED_TO_CONVERT_TO_TABLETS_FROM_TSFILE_ARG_STATUS_ARG =
      "Load: Failed to convert to tablets from TsFile {}. Status: {}";
  public static final String LOAD_FAILED_TO_CONVERT_TO_TABLETS_FROM_TSFILE_ARG_EXCEPTION_ARG =
      "Load: Failed to convert to tablets from TsFile {}. Exception: {}";
  public static final String DISPATCH_PIECE_NODE_ARG_OF_TSFILE_ARG_ERROR =
      "Dispatch piece node {} of TsFile {} error.";
  public static final String CANNOT_DISPATCH_LOADCOMMAND_FOR_LOAD_OPERATION_ARG =
      "Cannot dispatch LoadCommand for load operation {}";
  public static final String LOAD_REMOTE_PROCEDURE_CALL_CONNECTION_TIMEOUT_IS_ADJUSTED_TO_ARG_MS_ARG_MINS =
      "Load remote procedure call connection timeout is adjusted to {} ms ({} mins)";
  public static final String DATA_TYPE_OF_ARG_ARG_IS_NOT_CONSISTENT =
      "data type of {}.{} is not consistent, ";
  public static final String REGISTERED_TYPE_ARG_INSERTING_TIMESTAMP_ARG_VALUE_ARG =
      "registered type {}, inserting timestamp {}, value {}";
  public static final String TIMES_ARRAY_IS_NULL_OR_TOO_SMALL_TIMES_LENGTH_ARG_ROWSIZE_ARG_DEVICEID_ARG =
      "Times array is null or too small. times.length={}, rowSize={}, deviceId={}";
  public static final String SERIALIZE_DATA_OF_TSFILE_S_ERROR_SKIP_TSFILEDATA_S =
      "Serialize data of TsFile %s error, skip TsFileData %s";
  public static final String FAIL_TO_MATERIALIZE_CTE_BECAUSE_THE_DATA_SIZE_EXCEEDED_MEMORY_OR_THE_ROW_COUNT_THRESHOLD =
      "Fail to materialize CTE because the data size exceeded memory or the row count threshold";
  public static final String UNEXPECTED_FAILURE_WHEN_HANDLING_PARSING_ERROR_THIS_IS_LIKELY_A_BUG_IN_THE =
      "Unexpected failure when handling parsing error. This is likely a bug in the implementation";
  public static final String AND_EXPRESSION_ENCOUNTERED_DURING_TAG_DETERMINED_CHECKING_WILL_BE_CLASSIFIED_INTO_FUZZY =
      "And expression encountered during tag-determined checking, will be classified into fuzzy expression. Sql: {}";
  public static final String LOGICAL_EXPRESSION_TYPE_ENCOUNTERED_IN_NOT_EXPRESSION_CHILD_DURING_TAG_DETERMINED =
      "Logical expression type encountered in not expression child during tag-determined checking, will be classified into fuzzy expression. Sql: {}";
  public static final String VALIDATING_DEVICE_SCHEMA_ARG_ARG_AND_OTHER_ARG_DEVICES =
      "Validating device schema {}.{} and other {} devices";
  public static final String ILLEGAL_TABLEID_ARG_FOUND_IN_CACHE_WHEN_INVALIDATING_BY_PATH_ARG_INVALIDATE_IT_ANYWAY =
      "Illegal tableID {} found in cache when invalidating by path {}, invalidate it anyway";
  public static final String ILLEGAL_DEVICEID_ARG_FOUND_IN_CACHE_WHEN_INVALIDATING_BY_PATH_ARG_INVALIDATE_IT_ANYWAY =
      "Illegal deviceID {} found in cache when invalidating by path {}, invalidate it anyway";
  public static final String RULE_S_BEFORE_S_AFTER_S =
      "Rule: {}\nBefore:\n{}\nAfter:\n{}";

  private DataNodeQueryMessages() {}
  // ---------------------------------------------------------------------------
  // Additional exception messages
  // ---------------------------------------------------------------------------
  public static final String QUERY_EXCEPTION_FAILED_TO_SERIALIZE_INTERMEDIATE_RESULT_FOR_MAXBYACCUMULATOR_2F18B6E7 =
      "Failed to serialize intermediate result for MaxByAccumulator.";
  public static final String QUERY_EXCEPTION_UNSUPPORTED_DATA_TYPE_IN_AGGREGATION_AVG_S_D1DAD6A6 =
      "Unsupported data type in aggregation AVG : %s";
  public static final String QUERY_EXCEPTION_UNSUPPORTED_DATA_TYPE_IN_MINVALUE_S_BC092694 =
      "Unsupported data type in MinValue: %s";
  public static final String QUERY_EXCEPTION_UNSUPPORTED_DATA_TYPE_IN_LASTVALUE_S_02ECF8E4 =
      "Unsupported data type in LastValue: %s";
  public static final String QUERY_EXCEPTION_UNSUPPORTED_DATA_TYPE_IN_EXTREME_S_84B651D3 =
      "Unsupported data type in Extreme: %s";
  public static final String QUERY_EXCEPTION_UNSUPPORTED_DATA_TYPE_IN_FIRSTVALUE_S_97025F25 =
      "Unsupported data type in FirstValue: %s";
  public static final String QUERY_EXCEPTION_ERROR_OCCURRED_DURING_EXECUTING_UDAF_S_S_PLEASE_CHECK_WHETHER_9E9D20C6 =
      "Error occurred during executing UDAF#%s: %s, please check whether the implementation of UDF "
          + "is correct according to the udf-api description.";
  public static final String QUERY_EXCEPTION_UNSUPPORTED_EXPRESSION_TYPE_S_FD6F5B7C =
      "unsupported expression type: %s";
  public static final String QUERY_EXCEPTION_UNSUPPORTED_DATA_TYPE_IN_MAXVALUE_S_521AC345 =
      "Unsupported data type in MaxValue: %s";
  public static final String QUERY_EXCEPTION_THERE_IS_NOT_ENOUGH_CPU_TO_EXECUTE_CURRENT_FRAGMENT_INSTANCE_E7719FB8 =
      "There is not enough cpu to execute current fragment instance";
  public static final String QUERY_EXCEPTION_THERE_IS_NO_ENOUGH_MEMORY_TO_EXECUTE_CURRENT_FRAGMENT_INSTANCE_CB632843 =
      "There is no enough memory to execute current fragment instance";
  public static final String QUERY_EXCEPTION_PLANNODE_RELATED_MEMORY_IS_NOT_ZERO_WHEN_TRYING_TO_DEREGISTER_E01109C5 =
      "PlanNode related memory is not zero when trying to deregister FI from query memory pool. "
          + "QueryId is : %s, FragmentInstanceId is : %s, Non-zero PlanNode related memory is : %s.";
  public static final String QUERY_EXCEPTION_QUERY_IS_ABORTED_SINCE_IT_REQUESTS_MORE_MEMORY_THAN_CAN_D77C2921 =
      "Query is aborted since it requests more memory than can be allocated, bytesToReserve: %sB, "
          + "maxBytesCanReserve: %sB";
  public static final String QUERY_EXCEPTION_RELATEDMEMORYRESERVED_CAN_T_BE_NULL_WHEN_FREEING_MEMORY_C80009F2 =
      "RelatedMemoryReserved can't be null when freeing memory";
  public static final String QUERY_EXCEPTION_INTERRUPTED_BY_92FAED2D = "Interrupted By";
  public static final String QUERY_EXCEPTION_DRIVER_WAS_INTERRUPTED_737358E4 =
      "Driver was interrupted";
  public static final String QUERY_EXCEPTION_UNSUPPORTED_QUERY_DATA_SOURCE_TYPE_S_7424E63F =
      "Unsupported query data source type: %s";
  public static final String QUERY_EXCEPTION_REPEATED_RPC_CALL_DETECTED_FOR_FRAGMENTINSTANCE_S_REJECT_BF609A26 =
      "Repeated RPC call detected for FragmentInstance %s, reject the duplicated dispatch.";
  public static final String QUERY_EXCEPTION_QUERY_HAS_EXECUTED_MORE_THAN_SMS_AND_NOW_IS_IN_FLUSHING_4BF7535B =
      "Query has executed more than %sms, and now is in flushing state";
  public static final String QUERY_EXCEPTION_THE_QUERYCONTEXT_DOES_NOT_SUPPORT_ROW_LEVEL_FILTERING_D4CD0678 =
      "the QueryContext does not support row level filtering";
  public static final String QUERY_EXCEPTION_S_IS_NOT_VIEW_B5840A3C = "%s is not view.";
  public static final String QUERY_EXCEPTION_THE_TIMESERIES_S_USED_NEW_TYPE_S_IS_NOT_COMPATIBLE_WITH_455D4D4A =
      "The timeseries %s used new type %s is not compatible with the existing one %s.";
  public static final String QUERY_EXCEPTION_ALL_CACHED_PAGES_SHOULD_BE_CONSUMED_FIRST_UNSEQPAGEREADERS_55898EFB =
      "all cached pages should be consumed first unSeqPageReaders.isEmpty() is %s firstPageReader "
          + "!= null is %s mergeReader.hasNextTimeValuePair() = %s";
  public static final String QUERY_EXCEPTION_NOT_SUPPORT_THIS_TYPE_OF_AGGREGATION_WINDOW_S_604F93D0 =
      "Not support this type of aggregation window :%s";
  public static final String QUERY_EXCEPTION_UNSUPPORTED_DATA_TYPE_IN_EQUAL_EVENT_AGGREGATION_S_5076ACFE =
      "Unsupported data type in equal event aggregation : %s";
  public static final String QUERY_EXCEPTION_UNSUPPORTED_DATA_TYPE_IN_VARIATION_EVENT_AGGREGATION_S_47341532 =
      "Unsupported data type in variation event aggregation : %s";
  public static final String QUERY_EXCEPTION_THE_OPERATOR_CANNOT_CONTINUE_UNTIL_THE_LAST_WRITE_OPERATION_1F241343 =
      "The operator cannot continue until the last write operation is done.";
  public static final String QUERY_EXCEPTION_DATA_TYPE_S_IS_NOT_SUPPORTED_5D5C02E4 =
      "Data type %s is not supported.";
  public static final String QUERY_EXCEPTION_DATA_TYPE_S_IS_NOT_SUPPORTED_WHEN_CONVERT_DATA_AT_CLIENT_405429CC =
      "data type %s is not supported when convert data at client";
  public static final String QUERY_EXCEPTION_CHILD_SIZE_OF_INNERTIMEJOINOPERATOR_SHOULD_BE_LARGER_THAN_4E7CF105 =
      "Child size of InnerTimeJoinOperator should be larger than 1.";
  public static final String QUERY_EXCEPTION_THE_OPERATOR_CANNOT_CONTINUE_UNTIL_THE_FORECAST_EXECUTION_AF8A3145 =
      "The operator cannot continue until the forecast execution is done.";
  public static final String QUERY_EXCEPTION_RESULT_TYPE_MISMATCH_FOR_ATTRIBUTE_S_EXPECTED_S_ACTUAL_S_E5637B91 =
      "Result type mismatch for attribute '%s', expected %s, actual %s";
  public static final String QUERY_EXCEPTION_DEVICE_ENTRIES_OF_INDEX_S_IS_EMPTY_BCFB0644 =
      "Device entries of index %s is empty";
  public static final String QUERY_EXCEPTION_SHOULD_NOT_CALL_GETRESULTDATATYPES_METHOD_IN_DEVICEITERATORSCANOPERATOR_E915A153 =
      "Should not call getResultDataTypes() method in DeviceIteratorScanOperator";
  public static final String QUERY_EXCEPTION_UNEXPECTED_COLUMN_CATEGORY_S_6E60A44E =
      "Unexpected column category: %s";
  public static final String QUERY_EXCEPTION_DEVICE_ENTRIES_OF_INDEX_S_IN_TABLESCANOPERATOR_IS_EMPTY_FDEB574F =
      "Device entries of index %s in TableScanOperator is empty";
  public static final String QUERY_EXCEPTION_MULTILEVELPRIORITYQUEUE_DOES_NOT_SUPPORT_ACCESS_ELEMENT_02FE5AC9 =
      "MultilevelPriorityQueue does not support access element by get.";
  public static final String QUERY_EXCEPTION_ASCENDING_IS_NOT_SUPPORTED_WHEN_SLIDING_STEP_CONTAINS_MONTH_3446C0DC =
      "Ascending is not supported when sliding step contains month.";
  public static final String QUERY_EXCEPTION_THIS_OPERATION_IS_NOT_SUPPORTED_IN_SCHEMAMEASUREMENTNODE_93A81AE3 =
      "This operation is not supported in SchemaMeasurementNode.";
  public static final String QUERY_EXCEPTION_REMOVE_CHILD_OPERATION_IS_NOT_SUPPORTED_IN_SCHEMAMEASUREMENTNODE_940D080F =
      "Remove child operation is not supported in SchemaMeasurementNode.";
  public static final String QUERY_EXCEPTION_DO_NOT_SUPPORT_CREATE_COLUMNBUILDER_WITH_DATA_TYPE_S_1672578A =
      "Do not support create ColumnBuilder with data type %s";
  public static final String QUERY_EXCEPTION_INVALID_CONSTANT_OPERAND_S_939F3B8D =
      "Invalid constant operand: %s";
  public static final String QUERY_EXCEPTION_THE_DATA_TYPE_OF_THE_STATE_WINDOW_STRATEGY_IS_NOT_VALID_DFFBF210 =
      "The data type of the state window strategy is not valid.";
  public static final String QUERY_EXCEPTION_STATEWINDOWACCESSSTRATEGY_DOES_NOT_SUPPORT_PURE_CONSTANT_B09D811B =
      "StateWindowAccessStrategy does not support pure constant input.";
  public static final String QUERY_EXCEPTION_UNEXPECTED_ACCESS_STRATEGY_S_92EA9D64 =
      "Unexpected access strategy: %s";
  public static final String QUERY_EXCEPTION_STATEWINDOWACCESSSTRATEGY_ONLY_SUPPORT_ONE_INPUT_SERIES_6856E52C =
      "StateWindowAccessStrategy only support one input series for now.";
  public static final String QUERY_EXCEPTION_UNSUPPORTED_SOURCE_DATATYPE_S_EA03E121 =
      "Unsupported source dataType: %s";
  public static final String QUERY_EXCEPTION_UNSUPPORTED_TARGET_DATATYPE_S_8DEFDAE6 =
      "Unsupported target dataType: %s";
  public static final String QUERY_EXCEPTION_UNSUPPORTED_STATEMENT_TYPE_S_FBCA7305 =
      "Unsupported statement type: %s";
  public static final String QUERY_EXCEPTION_IDENTITYSINKNODE_SHOULD_ONLY_HAVE_ONE_CHILD_IN_TABLE_MODEL_5E995EB3 =
      "IdentitySinkNode should only have one child in table model.";
  public static final String QUERY_EXCEPTION_TREE_DB_NAME_SHOULD_AT_LEAST_BE_TWO_LEVEL_S_772B6832 =
      "tree db name should at least be two level: %s";
  public static final String QUERY_EXCEPTION_PUSHDOWNOFFSET_SHOULD_NOT_BE_SET_WHEN_ISPUSHLIMITTOEACHDEVICE_9B6D5144 =
      "PushDownOffset should not be set when isPushLimitToEachDevice is true.";
  public static final String QUERY_EXCEPTION_DEVICE_ENTRIES_OF_INDEX_S_IN_S_IS_EMPTY_68D1DB60 =
      "Device entries of index %s in %s is empty";
  public static final String QUERY_EXCEPTION_THE_AGGREGATIONTREEDEVICEVIEWSCANNODE_SHOULD_HAS_BEEN_TRANSFERRED_76A35037 =
      "The AggregationTreeDeviceViewScanNode should has been transferred to its child class node";
  public static final String QUERY_EXCEPTION_GROUPING_KEY_MUST_BE_ID_OR_ATTRIBUTE_IN_AGGREGATIONTABLESCAN_7B592AE6 =
      "grouping key must be ID or Attribute in AggregationTableScan";
  public static final String QUERY_EXCEPTION_CANNOT_FIND_COLUMN_S_IN_CHILD_S_OUTPUT_10FBE4C8 =
      "Cannot find column [%s] in child's output";
  public static final String QUERY_EXCEPTION_DESCRIPTOR_S_INPUT_EXPRESSION_MUST_BE_TIMESERIESOPERAND_F4F66475 =
      "descriptor's input expression must be TimeSeriesOperand/TimestampOperand, current is %s";
  public static final String QUERY_EXCEPTION_AGGREGATIONMERGESORTNODE_WITHOUT_ORDER_BY_DEVICE_SHOULD_7AED85D1 =
      "AggregationMergeSortNode without order by device should not appear here";
  public static final String QUERY_EXCEPTION_UNEXPECTED_PLANNODE_IN_GETOUTPUTCOLUMNTYPESOFTIMEJOINNODE_00FAAEED =
      "Unexpected PlanNode in getOutputColumnTypesOfTimeJoinNode, type: %s";
  public static final String QUERY_EXCEPTION_THE_SIZE_OF_MEASUREMENTLIST_AND_TIMESERIESSCHEMAINFOLIST_A6649661 =
      "The size of measurementList and timeseriesSchemaInfoList should be equal in aligned path.";
  public static final String QUERY_EXCEPTION_THE_PLANNODE_IS_NULL_DURING_LOCAL_EXECUTION_MAYBE_CAUSED_C5B942CA =
      "The planNode is null during local execution, maybe caused by closing of the current dataNode";
  public static final String QUERY_EXCEPTION_THERE_IS_NOT_ENOUGH_MEMORY_TO_EXECUTE_CURRENT_FRAGMENT_INSTANCE_6071A581 =
      "There is not enough memory to execute current fragment instance, current remaining free "
          + "memory is %dB, estimated memory usage for current fragment instance is %dB";
  public static final String QUERY_EXCEPTION_BYTES_TO_RESERVE_FROM_FREE_MEMORY_FOR_OPERATORS_SHOULD_BE_4DC404D5 =
      "Bytes to reserve from free memory for operators should be larger than 0";
  public static final String QUERY_EXCEPTION_THERE_IS_NOT_ENOUGH_MEMORY_FOR_QUERY_S_THE_CONTEXTHOLDER_546CDD02 =
      "There is not enough memory for Query %s, the contextHolder is %s,current remaining free "
          + "memory is %dB, already reserved memory for this context in total is %dB, the memory "
          + "requested this time is %dB";
  public static final String QUERY_EXCEPTION_BYTES_TO_RELEASE_TO_FREE_MEMORY_FOR_OPERATORS_SHOULD_BE_3E5B0CB1 =
      "Bytes to release to free memory for operators should be larger than 0";
  public static final String QUERY_EXCEPTION_INVALID_AGGREGATION_EXPRESSION_S_B28EB91B =
      "Invalid Aggregation Expression: %s";
  public static final String QUERY_EXCEPTION_ILLEGAL_DEVICE_PATH_S_IN_AGGREGATIONPUSHDOWN_RULE_60D5F633 =
      "Illegal device path: %s in AggregationPushDown rule.";
  public static final String QUERY_EXCEPTION_AGGREGATION_DESCRIPTORS_WITH_NON_ALIGNED_TEMPLATE_ARE_NOT_6D3C7C0F =
      "Aggregation descriptors with non aligned template are not supported";
  public static final String QUERY_EXCEPTION_FRAGMENTINSTANCE_S_IS_FAILED_S_MAY_BE_CAUSED_BY_DN_RESTARTING_45D7D52A =
      "FragmentInstance[%s] is failed. %s, may be caused by DN restarting.";
  public static final String QUERY_EXCEPTION_FRAGMENTINSTANCE_S_IS_FAILED_S_566B0005 =
      "FragmentInstance[%s] is failed. %s";
  public static final String QUERY_EXCEPTION_LINE_S_S_S_7CA5F0E1 = "line %s:%s %s";
  public static final String QUERY_EXCEPTION_MISSING_OR_INVALID_COLUMN_CATEGORIES_FOR_TABLE_INSERTION_5DF990B9 =
      "Missing or invalid column categories for table insertion";
  public static final String QUERY_EXCEPTION_THE_NAME_OF_A_MEASUREMENT_IN_SCHEMA_TEMPLATE_SHALL_NOT_BE_937264BD =
      "The name of a measurement in schema template shall not be null.";
  public static final String QUERY_EXCEPTION_UNSUPPORTED_COLUMNCATEGORY_S_1260CFFD =
      "Unsupported ColumnCategory: %s";
  public static final String QUERY_EXCEPTION_UNSUPPORTED_RAWEXPRESSION_TYPE_S_CDBBD685 =
      "unsupported rawExpression type: %s";
  public static final String QUERY_EXCEPTION_AN_ERROR_OCCURRED_WHEN_EXECUTING_GETSCHEMAPARTITION_S_A0156043 =
      "An error occurred when executing getSchemaPartition():%s";
  public static final String QUERY_EXCEPTION_AN_ERROR_OCCURRED_WHEN_EXECUTING_GETORCREATESCHEMAPARTITION_4D22BE9B =
      "An error occurred when executing getOrCreateSchemaPartition():%s";
  public static final String QUERY_EXCEPTION_AN_ERROR_OCCURRED_WHEN_EXECUTING_GETSCHEMANODEMANAGEMENTPARTITION_84AC8509 =
      "An error occurred when executing getSchemaNodeManagementPartition():%s";
  public static final String QUERY_EXCEPTION_AN_ERROR_OCCURRED_WHEN_EXECUTING_GETDATAPARTITION_S_D21A0011 =
      "An error occurred when executing getDataPartition():%s";
  public static final String QUERY_EXCEPTION_AN_ERROR_OCCURRED_WHEN_EXECUTING_GETORCREATEDATAPARTITION_2EB2EBBE =
      "An error occurred when executing getOrCreateDataPartition():%s";
  public static final String QUERY_EXCEPTION_THE_TYPE_OF_INPUT_EXPRESSION_S_IS_UNKNOWN_841AC714 =
      "The type of input expression %s is unknown";
  public static final String QUERY_EXCEPTION_MEET_ERROR_WHEN_ANALYZING_THE_QUERY_STATEMENT_S_AD732908 =
      "Meet error when analyzing the query statement: %s";
  public static final String QUERY_EXCEPTION_UNSUPPORTED_EXPRESSION_TYPE_FOR_SOURCE_EXPRESSION_S_FB5583E7 =
      "unsupported expression type for source expression: %s";
  public static final String QUERY_EXCEPTION_UNSUPPORTED_EXPRESSION_TYPE_S_737846D6 =
      "Unsupported Expression Type: %s";
  public static final String QUERY_EXCEPTION_UNKNOWN_EXPRESSION_TYPE_S_PERHAPS_IT_HAS_NON_EXISTENT_MEASUREMENT_B6705F86 =
      "Unknown expression type: %s, perhaps it has non existent measurement.";
  public static final String QUERY_EXCEPTION_UNSUPPORTED_EXPRESSION_TYPE_IN_TRANSFORMTOVIEWEXPRESSIONVISITOR_0871FB56 =
      "Unsupported expression type in TransformToViewExpressionVisitor: %s";
  public static final String QUERY_EXCEPTION_CAN_NOT_CONSTRUCT_EXPRESSION_USING_NON_VIEW_PATH_IN_TRANSFORMVIEWPATH_A9CCB5B1 =
      "Can not construct expression using non view path in transformViewPath!";
  public static final String QUERY_EXCEPTION_UNSUPPORTED_EXPRESSION_TYPE_S_7C6F99A9 =
      "Unsupported expression type %s";
  public static final String QUERY_EXCEPTION_S_CANNOT_BE_CAST_TO_S_DABC2DA0 =
      "\"%s\" cannot be cast to [%s]";
  public static final String QUERY_EXCEPTION_UNSUPPORTED_DATA_TYPE_S_4CB21D47 =
      "Unsupported data type %s";
  public static final String QUERY_EXCEPTION_MEASUREMENT_S_DOES_NOT_EXIST_23D2B5BE =
      "Measurement %s does not exist";
  public static final String QUERY_EXCEPTION_INVALID_SCALAR_FUNCTION_S_4DC1ED95 =
      "Invalid scalar function [%s].";
  public static final String QUERY_EXCEPTION_FETCH_SCHEMA_FAILED_BECAUSE_S_BE584DCE =
      "Fetch Schema failed, because %s";
  public static final String QUERY_EXCEPTION_FETCH_SCHEMA_FAILED_S_1C7B0050 =
      "Fetch Schema failed: %s";
  public static final String QUERY_EXCEPTION_FAILED_TO_VALIDATE_SCHEMA_FOR_TABLE_S_S_D7031B7B =
      "Failed to validate schema for table {%s, %s}";
  public static final String QUERY_EXCEPTION_THE_DATABASE_S_DOES_NOT_EXIST_PLEASE_ENABLE_ENABLE_AUTO_B6683D0E =
      "The database %s does not exist, please enable 'enable_auto_create_schema' to enable auto "
          + "creation.";
  public static final String QUERY_EXCEPTION_AUTO_CREATE_DATABASE_FAILED_S_STATUS_CODE_S_D8EB60FA =
      "Auto create database failed: %s, status code: %s";
  public static final String QUERY_EXCEPTION_TAG_COLUMN_S_IN_TSFILE_IS_NOT_FOUND_IN_IOTDB_TABLE_S_12E8C1EF =
      "Tag column %s in TsFile is not found in IoTDB table %s";
  public static final String QUERY_EXCEPTION_DUPLICATED_MEASUREMENTS_S_IN_DEVICE_S_438713CD =
      "Duplicated measurements %s in device %s.";
  public static final String QUERY_EXCEPTION_DATABASE_LEVEL_D_IS_LONGER_THAN_DEVICE_S_9B34DD2F =
      "Database level %d is longer than device %s.";
  public static final String QUERY_EXCEPTION_CREATE_DATABASE_ERROR_STATEMENT_S_RESULT_STATUS_IS_S_5C4AFD58 =
      "Create database error, statement: %s, result status is: %s";
  public static final String QUERY_EXCEPTION_DEVICE_S_DOES_NOT_EXIST_IN_IOTDB_AND_CAN_NOT_BE_CREATED_5171DE45 =
      "Device %s does not exist in IoTDB and can not be created. Please check weather "
          + "auto-create-schema is enabled.";
  public static final String QUERY_EXCEPTION_DEVICE_S_IN_TSFILE_IS_S_BUT_IN_IOTDB_IS_S_350D5903 =
      "Device %s in TsFile is %s, but in IoTDB is %s.";
  public static final String QUERY_EXCEPTION_MEASUREMENT_S_DOES_NOT_EXIST_IN_IOTDB_AND_CAN_NOT_BE_CREATED_B1F446A5 =
      "Measurement %s does not exist in IoTDB and can not be created. Please check weather "
          + "auto-create-schema is enabled.";
  public static final String QUERY_EXCEPTION_AN_ERROR_OCCURRED_WHEN_EXECUTING_GETDEVICETODATABASE_S_CCA611CC =
      "An error occurred when executing getDeviceToDatabase():%s";
  public static final String QUERY_EXCEPTION_FAILED_TO_GET_REPLICASET_OF_CONSENSUS_GROUPS_IDS_S_CC30C7A6 =
      "Failed to get replicaSet of consensus groups[ids= %s]";
  public static final String QUERY_EXCEPTION_AN_ERROR_OCCURRED_WHEN_EXECUTING_GETREGIONREPLICASET_S_370D5526 =
      "An error occurred when executing getRegionReplicaSet():%s";
  public static final String QUERY_EXCEPTION_SUBSCRIPTION_IS_NOT_ENABLED_7F43DCBB =
      "Subscription is not enabled.";
  public static final String QUERY_EXCEPTION_FAILED_TO_CREATE_UDF_S_THE_GIVEN_FUNCTION_NAME_CONFLICTS_6FBB1136 =
      "Failed to create UDF [%s], the given function name conflicts with the built-in function "
          + "name.";
  public static final String QUERY_EXCEPTION_THE_SCHEME_OF_URI_IS_NOT_SET_PLEASE_SPECIFY_THE_SCHEME_OF_225DFB9E =
      "The scheme of URI is not set, please specify the scheme of URI.";
  public static final String QUERY_EXCEPTION_FAILED_TO_GET_EXECUTABLE_FOR_UDF_S_PLEASE_CHECK_THE_URI_F4D87A1E =
      "Failed to get executable for UDF '%s', please check the URI.";
  public static final String QUERY_EXCEPTION_FAILED_TO_CREATE_FUNCTION_S_BECAUSE_THERE_IS_DUPLICATE_ARGUMENT_7905BC09 =
      "Failed to create function '%s', because there is duplicate argument name '%s'.";
  public static final String QUERY_EXCEPTION_FAILED_TO_CREATE_FUNCTION_S_BECAUSE_THERE_IS_AN_ARGUMENT_E7A0B1D6 =
      "Failed to create function '%s', because there is an argument with OBJECT type '%s'.";
  public static final String QUERY_EXCEPTION_FAILED_TO_LOAD_CLASS_S_BECAUSE_IT_S_NOT_FOUND_IN_JAR_FILE_E467D08D =
      "Failed to load class '%s', because it's not found in jar file or is invalid: %s";
  public static final String QUERY_EXCEPTION_BUILT_IN_FUNCTION_S_CAN_NOT_BE_DEREGISTERED_1CC7D3C3 =
      "Built-in function %s can not be deregistered.";
  public static final String QUERY_EXCEPTION_FAILED_TO_GET_EXECUTABLE_FOR_TRIGGER_S_PLEASE_CHECK_THE_DA49134A =
      "Failed to get executable for Trigger '%s', please check the URI.";
  public static final String QUERY_EXCEPTION_FAILED_TO_CREATE_PIPE_PLUGIN_BECAUSE_THE_URI_IS_EMPTY_7FCB6EF4 =
      "Failed to create pipe plugin, because the URI is empty.";
  public static final String QUERY_EXCEPTION_FAILED_TO_GET_EXECUTABLE_FOR_PIPEPLUGIN_S_PLEASE_CHECK_THE_FAC5DCB7 =
      "Failed to get executable for PipePlugin %s, please check the URI.";
  public static final String QUERY_EXCEPTION_FAILED_TO_CREATE_PIPEPLUGIN_S_BECAUSE_THIS_PLUGIN_IS_NOT_F5A284B4 =
      "Failed to create PipePlugin '%s', because this plugin is not designed for %s model.";
  public static final String QUERY_EXCEPTION_NOT_ALL_SG_IS_READY_9F51CF3E = "not all sg is ready";
  public static final String QUERY_EXCEPTION_CANNOT_START_REPAIR_TASK_BECAUSE_COMPACTION_IS_NOT_ENABLED_975C8DCD =
      "cannot start repair task because compaction is not enabled";
  public static final String QUERY_EXCEPTION_PLEASE_ENSURE_YOUR_INPUT_QUERYID_IS_CORRECT_D86C841E =
      "Please ensure your input <queryId> is correct";
  public static final String QUERY_EXCEPTION_DUPLICATED_MEASUREMENT_S_IN_DEVICE_TEMPLATE_ALTER_REQUEST_963FE4A6 =
      "Duplicated measurement [%s] in device template alter request";
  public static final String QUERY_EXCEPTION_FAILED_TO_CREATE_PIPE_S_BECAUSE_TSFILE_IS_CONFIGURED_WITH_2F8CD704 =
      "Failed to create Pipe %s because TSFile is configured with encryption, which prohibits the "
          + "use of Pipe";
  public static final String QUERY_EXCEPTION_FAILED_TO_CREATE_PIPE_S_PIPE_NAME_STARTING_WITH_S_ARE_NOT_201FE8C3 =
      "Failed to create pipe %s, pipe name starting with \"%s\" are not allowed to be created.";
  public static final String QUERY_EXCEPTION_FAILED_TO_ALTER_PIPE_S_PIPE_NAME_STARTING_WITH_S_ARE_NOT_03D99ECF =
      "Failed to alter pipe %s, pipe name starting with \"%s\" are not allowed to be altered.";
  public static final String QUERY_EXCEPTION_FAILED_TO_GET_PIPE_INFO_FROM_CONFIG_NODE_STATUS_IS_S_FE797D7B =
      "Failed to get pipe info from config node, status is %s.";
  public static final String QUERY_EXCEPTION_FAILED_TO_ALTER_PIPE_S_PIPE_NOT_FOUND_IN_SYSTEM_63B5D3CC =
      "Failed to alter pipe %s, pipe not found in system.";
  public static final String QUERY_EXCEPTION_FAILED_TO_ALTER_PIPE_S_BECAUSE_S_A1823289 =
      "Failed to alter pipe %s, because %s";
  public static final String QUERY_EXCEPTION_FAILED_TO_START_PIPE_S_PIPE_NAME_STARTING_WITH_S_ARE_NOT_F16D488F =
      "Failed to start pipe %s, pipe name starting with \"%s\" are not allowed to be started.";
  public static final String QUERY_EXCEPTION_FAILED_TO_DROP_PIPE_S_PIPE_NAME_STARTING_WITH_S_ARE_NOT_840E238B =
      "Failed to drop pipe %s, pipe name starting with \"%s\" are not allowed to be dropped.";
  public static final String QUERY_EXCEPTION_FAILED_TO_STOP_PIPE_S_PIPE_NAME_STARTING_WITH_S_ARE_NOT_C78DFC3D =
      "Failed to stop pipe %s, pipe name starting with \"%s\" are not allowed to be stopped.";
  public static final String QUERY_EXCEPTION_UNKNOWN_DATABASE_S_5AB61128 = "Unknown database %s";
  public static final String QUERY_EXCEPTION_DATABASE_S_DOESN_T_EXIST_5A8EE8CA =
      "Database %s doesn't exist";
  public static final String QUERY_EXCEPTION_DATABASE_S_ALREADY_EXISTS_D8BE5332 =
      "Database %s already exists";
  public static final String QUERY_EXCEPTION_DATA_TYPE_CANNOT_BE_NULL_EXECUTING_THE_STATEMENT_THAT_ALTER_4C959B2F =
      "Data type cannot be null executing the statement that alter timeseries %s set data type";
  public static final String QUERY_EXCEPTION_NO_CURRENT_SESSION_AVAILABLE_FOR_PREPARE_STATEMENT_36717E9B =
      "No current session available for PREPARE statement";
  public static final String QUERY_EXCEPTION_NO_CURRENT_SESSION_AVAILABLE_FOR_DEALLOCATE_STATEMENT_1EA5DE79 =
      "No current session available for DEALLOCATE statement";
  public static final String QUERY_EXCEPTION_TSFILE_S_IS_LOADING_BY_ANOTHER_SCHEDULER_55077B82 =
      "TsFile %s is loading by another scheduler.";
  public static final String QUERY_EXCEPTION_SERIALIZE_PROGRESS_INDEX_ERROR_ISFIRSTPHASESUCCESS_S_UUID_690F0419 =
      "Serialize Progress Index error, isFirstPhaseSuccess: %s, uuid: %s, tsFile: %s";
  public static final String QUERY_EXCEPTION_UNSUPPORTED_TSFILEDATATYPE_S_374475FA =
      "Unsupported TsFileDataType %s.";
  public static final String QUERY_EXCEPTION_UNKNOWN_SORT_KEY_S_37965711 = "Unknown sort key %s";
  public static final String QUERY_EXCEPTION_CAN_NOT_FIND_S_ON_THIS_MACHINE_NOTICE_THAT_LOAD_CAN_ONLY_B7886C0E =
      "Can not find %s on this machine, notice that load can only handle files on this machine.";
  public static final String QUERY_EXCEPTION_LOAD_TSFILE_SOURCE_PATH_S_IS_OUTSIDE_ALLOWED_DIRECTORIES_85A6019F =
      "Load TsFile source path %s is outside allowed directories %s.";
  public static final String QUERY_EXCEPTION_FAILED_TO_RESOLVE_CANONICAL_PATH_FOR_LOAD_TSFILE_SOURCE_09CC9AC6 =
      "Failed to resolve canonical path for Load TsFile source %s: %s";
  public static final String QUERY_EXCEPTION_DATA_TYPE_IS_NOT_CONSISTENT_INPUT_S_REGISTERED_S_AE9DBDC0 =
      "data type is not consistent, input %s, registered %s";
  public static final String QUERY_EXCEPTION_REGIONREPLICASET_IS_INVALID_S_1C2671AD =
      "regionReplicaSet is invalid: %s";
  public static final String QUERY_EXCEPTION_PLANNODE_SHOULD_BE_IWRITEPLANNODE_IN_WRITE_OPERATION_S_36501D8A =
      "PlanNode should be IWritePlanNode in WRITE operation:%s";
  public static final String QUERY_EXCEPTION_SIZE_OF_DEVICES_AND_ITS_CHILDREN_IN_DEVICEVIEWNODE_SHOULD_10709A84 =
      "size of devices and its children in DeviceViewNode should be same";
  public static final String QUERY_EXCEPTION_IN_NON_CROSS_DATA_REGION_DEVICE_VIEW_SITUATION_EACH_DEVICE_3A76445B =
      "In non-cross data region device-view situation, each device should only have on data "
          + "partition.";
  public static final String QUERY_EXCEPTION_IN_NON_CROSS_DATA_REGION_AGGREGATION_DEVICE_VIEW_SITUATION_557AE5D2 =
      "In non-cross data region aggregation device-view situation, each rewrite child node of "
          + "DeviceView should only be one.";
  public static final String QUERY_EXCEPTION_ALL_CHILD_NODES_OF_INNERTIMEJOINNODE_SHOULD_BE_SERIESSOURCENODE_B92B181D =
      "All child nodes of InnerTimeJoinNode should be SeriesSourceNode";
  public static final String QUERY_EXCEPTION_YOU_SHOULD_NEVER_SEE_CONTINUOUSSAMESEARCHINDEXSEPARATORNODE_F380A4B6 =
      "You should never see ContinuousSameSearchIndexSeparatorNode in this function, because "
          + "ContinuousSameSearchIndexSeparatorNode should never be used in network transmission.";
  public static final String QUERY_EXCEPTION_AGGREGATIONTREEDEVICEVIEWSCANNODE_SHOULD_NOT_BE_DESERIALIZED_11788F1B =
      "AggregationTreeDeviceViewScanNode should not be deserialized";
  public static final String QUERY_EXCEPTION_CONTINUOUSSAMESEARCHINDEXSEPARATORNODE_NOT_SUPPORT_MERGE_FD194976 =
      "ContinuousSameSearchIndexSeparatorNode not support merge";
  public static final String QUERY_EXCEPTION_DELETEDATANODES_WHICH_START_TIME_OR_END_TIME_ARE_NOT_SAME_F396951C =
      "DeleteDataNodes which start time or end time are not same cannot be merged";
  public static final String QUERY_EXCEPTION_NO_CHILD_IS_ALLOWED_FOR_ALIGNEDSERIESAGGREGATIONSCANNODE_41654FE2 =
      "no child is allowed for AlignedSeriesAggregationScanNode";
  public static final String QUERY_EXCEPTION_DEVICES_IN_TSFILE_S_IS_EMPTY_THIS_SHOULD_NOT_HAPPEN_HERE_BC1BE63C =
      "Devices in TsFile %s is empty, this should not happen here.";
  public static final String QUERY_EXCEPTION_DEVICEMERGENODE_SHOULD_HAVE_ONLY_ONE_LOCAL_CHILD_IN_SINGLE_D1A2E6CF =
      "DeviceMergeNode should have only one local child in single data region.";
  public static final String QUERY_EXCEPTION_GETOUTPUTCOLUMNNAMES_OF_CREATEMULTITIMESERIESNODE_IS_NOT_9D02257A =
      "getOutputColumnNames of CreateMultiTimeSeriesNode is not implemented";
  public static final String QUERY_EXCEPTION_GETOUTPUTCOLUMNNAMES_OF_ALTERLOGICALVIEWNODE_IS_NOT_IMPLEMENTED_D2294789 =
      "getOutputColumnNames of AlterLogicalViewNode is not implemented";
  public static final String QUERY_EXCEPTION_THE_DATABASE_S_IS_READ_ONLY_CB6732CE =
      "The database '%s' is read-only.";
  public static final String QUERY_EXCEPTION_THE_DATABASE_S_CAN_ONLY_BE_QUERIED_BY_AUDIT_ADMIN_4A510F66 =
      "The database '%s' can only be queried by AUDIT admin.";
  public static final String QUERY_EXCEPTION_UNEXPECTED_WINDOW_FRAME_TYPE_S_F06F81B8 =
      "unexpected window frame type: %s";
  public static final String QUERY_EXCEPTION_SIZE_OF_COLUMN_NAMES_AND_COLUMN_DATA_TYPES_DO_NOT_MATCH_9333D273 =
      "Size of column names and column data types do not match";
  public static final String QUERY_EXCEPTION_FAILED_TO_FETCH_FRAGMENT_INSTANCE_STATISTICS_45176795 =
      "Failed to fetch fragment instance statistics";
  public static final String QUERY_EXCEPTION_UNABLE_TO_REMOVE_FIRST_NODE_WHEN_A_NODE_HAS_MULTIPLE_CHILDREN_FB6E81C5 =
      "Unable to remove first node when a node has multiple children, use removeAll instead";
  public static final String QUERY_EXCEPTION_UNABLE_TO_REPLACE_FIRST_NODE_WHEN_A_NODE_HAS_MULTIPLE_CHILDREN_2C3D0E9E =
      "Unable to replace first node when a node has multiple children, use replaceAll instead";
  public static final String QUERY_EXCEPTION_UNSUPPORTED_EXPRESSION_IN_EXTRACTGLOBALTIMEPREDICATE_S_083B1BFA =
      "Unsupported expression in extractGlobalTimePredicate: %s";
  public static final String QUERY_EXCEPTION_NOT_A_VALID_IR_EXPRESSION_S_03C41ADD =
      "Not a valid IR expression: %s";
  public static final String QUERY_EXCEPTION_UNKNOWN_ALIGNEDDEVICEENTRY_TYPE_S_370A0039 =
      "Unknown AlignedDeviceEntry Type: %s";
  public static final String QUERY_EXCEPTION_UNSUPPORTED_SELECTITEM_TYPE_S_4B0B155A =
      "Unsupported SelectItem type: %s";
  public static final String QUERY_EXCEPTION_THIS_VISITOR_ONLY_SUPPORTED_PROCESS_OF_EXPRESSION_7CEB79CB =
      "This Visitor only supported process of Expression";
  public static final String QUERY_EXCEPTION_UNSUPPORTED_GROUPING_ELEMENT_TYPE_S_B3C526E6 =
      "Unsupported grouping element type: %s";
  public static final String QUERY_EXCEPTION_UNSUPPORTED_JOIN_CRITERIA_S_311289C1 =
      "Unsupported join criteria: %s";
  public static final String QUERY_EXCEPTION_DUPLICATE_ARGUMENT_SPECIFICATION_FOR_NAME_S_F804F3DC =
      "Duplicate argument specification for name: %s";
  public static final String QUERY_EXCEPTION_FORECASTTABLEFUNCTION_MUST_CONTAIN_FORECASTTABLEFUNCTION_DA5828E9 =
      "ForecastTableFunction must contain ForecastTableFunction.TIMECOL_PARAMETER_NAME";
  public static final String QUERY_EXCEPTION_UNEXPECTED_ARGUMENT_SPECIFICATION_S_830154B1 =
      "Unexpected argument specification: %s";
  public static final String QUERY_EXCEPTION_UNEXPECTED_PARTITIONBY_EXPRESSION_S_8D74EB2D =
      "Unexpected partitionBy expression: %s";
  public static final String QUERY_EXCEPTION_UNEXPECTED_ORDERBY_EXPRESSION_S_9301B69E =
      "Unexpected orderBy expression: %s";
  public static final String QUERY_EXCEPTION_AGGREGATION_ANALYSIS_NOT_YET_IMPLEMENTED_FOR_S_38B64170 =
      "aggregation analysis not yet implemented for: %s";
  public static final String QUERY_EXCEPTION_UNEXPECTED_VALUE_LIST_TYPE_FOR_INPREDICATE_S_3D50B78B =
      "Unexpected value list type for InPredicate: %s";
  public static final String QUERY_EXCEPTION_UNEXPECTED_COMPARISON_TYPE_S_5D101FCB =
      "Unexpected comparison type: %s";
  public static final String QUERY_EXCEPTION_ZERO_LENGTH_DELIMITED_IDENTIFIER_NOT_ALLOWED_00C9ADEC =
      "Zero-length delimited identifier not allowed";
  public static final String QUERY_EXCEPTION_BACKQUOTED_IDENTIFIERS_ARE_NOT_SUPPORTED_USE_DOUBLE_QUOTES_78BC7EE3 =
      "backquoted identifiers are not supported; use double quotes to quote identifiers";
  public static final String QUERY_EXCEPTION_UNSUPPORTED_UNBOUNDED_TYPE_S_2D943211 =
      "Unsupported unbounded type: %s";
  public static final String QUERY_EXCEPTION_ONLY_SUPPORT_MEASUREMENT_COLUMN_IN_FILTER_S_140800D9 =
      "Only support measurement column in filter: %s";
  public static final String QUERY_EXCEPTION_UNSUPPORTED_COMPARISON_OPERATOR_S_8357E642 =
      "Unsupported comparison operator %s";
  public static final String QUERY_EXCEPTION_UNSUPPORTED_EXTRACT_COMPARISON_OPERATOR_S_38A9CDFA =
      "Unsupported extract comparison operator %s";
  public static final String QUERY_EXCEPTION_UNSUPPORTED_LOGICAL_OPERATOR_S_FDC60986 =
      "Unsupported logical operator %s";
  public static final String QUERY_EXCEPTION_S_IS_NOT_SUPPORTED_IN_VALUE_PUSH_DOWN_DD54E38A =
      "%s is not supported in value push down";
  public static final String QUERY_EXCEPTION_SHOULD_NOT_REACH_HERE_BEFORE_PREDICATECOMBINEINTOTABLESCANCHECKER_C591ED7D =
      "Should not reach here before PredicateCombineIntoTableScanChecker support Extract push-down "
          + "in third child";
  public static final String QUERY_EXCEPTION_COLUMNSCHEMA_OF_SYMBOL_S_ISN_T_SAVED_IN_SCHEMAMAP_3A172EBC =
      "ColumnSchema of Symbol %s isn't saved in schemaMap";
  public static final String QUERY_EXCEPTION_SHOULD_NEVER_RETURN_NULL_IN_PREDICATECOMBINEINTOTABLESCANCHECKER_2A687052 =
      "Should never return null in PredicateCombineIntoTableScanChecker.";
  public static final String QUERY_EXCEPTION_EITHER_LEFT_OR_RIGHT_OPERAND_OF_COMPARISONEXPRESSION_SHOULD_FC89CE57 =
      "Either left or right operand of ComparisonExpression should have time column.";
  public static final String QUERY_EXCEPTION_SHOULD_NOT_REACH_HERE_BEFORE_GLOBALTIMEPREDICATEEXTRACTVISITOR_3ECC819B =
      "Should not reach here before GlobalTimePredicateExtractVisitor support Extract push-down in "
          + "second child";
  public static final String QUERY_EXCEPTION_SHOULD_NOT_REACH_HERE_BEFORE_GLOBALTIMEPREDICATEEXTRACTVISITOR_FB8489E8 =
      "Should not reach here before GlobalTimePredicateExtractVisitor support Extract push-down in "
          + "third child";
  public static final String QUERY_EXCEPTION_THREE_OPERAND_OF_BETWEEN_EXPRESSION_SHOULD_HAVE_TIME_COLUMN_25ED881D =
      "Three operand of between expression should have time column.";
  public static final String QUERY_EXCEPTION_FETCH_TABLE_DEVICE_SCHEMA_FAILED_BECAUSE_S_20B7D6C2 =
      "Fetch Table Device Schema failed because %s";
  public static final String QUERY_EXCEPTION_THE_SCHEMA_FILTER_TYPE_S_IS_NOT_SUPPORTED_200D1E0B =
      "The schema filter type %s is not supported";
  public static final String QUERY_EXCEPTION_AUTO_CREATE_TABLE_SUCCEED_BUT_CANNOT_GET_TABLE_SCHEMA_IN_74985A8E =
      "auto create table succeed, but cannot get table schema in current node's "
          + "DataNodeTableCache, may be caused by concurrently auto creating table";
  public static final String QUERY_EXCEPTION_CAN_NOT_CREATE_TABLE_BECAUSE_INCOMING_TABLE_HAS_NO_LESS_D3D33555 =
      "Can not create table because incoming table has no less tag columns than existing table, "
          + "and the existing tag columns are not the prefix of the incoming tag columns. Existing tag "
          + "column: %s, index in existing table: %s, index in incoming table: %s";
  public static final String QUERY_EXCEPTION_CAN_NOT_CREATE_TABLE_BECAUSE_EXISTING_TABLE_HAS_MORE_TAG_8364B675 =
      "Can not create table because existing table has more tag columns than incoming table, and "
          + "the incoming tag columns are not the prefix of the existing tag columns. Incoming tag "
          + "column: %s, index in existing table: %s, index in incoming table: %s";
  public static final String QUERY_EXCEPTION_AUTO_ADD_TABLE_COLUMN_FAILED_S_S_02F3DD19 =
      "Auto add table column failed: %s.%s";
  public static final String QUERY_EXCEPTION_CANNOT_CREATE_COLUMN_S_CATEGORY_IS_NOT_PROVIDED_E5410BD3 =
      "Cannot create column %s category is not provided";
  public static final String QUERY_EXCEPTION_CANNOT_CREATE_COLUMN_S_DATATYPE_IS_NOT_PROVIDED_2A7D27FA =
      "Cannot create column %s datatype is not provided";
  public static final String QUERY_EXCEPTION_UNKNOWN_COLUMNCATEGORY_FOR_ADDING_COLUMN_S_ED1BF7FA =
      "Unknown ColumnCategory for adding column: %s";
  public static final String QUERY_EXCEPTION_VISIT_NOT_IMPLEMENTED_FOR_S_1A798A4D =
      "visit() not implemented for %s";
  public static final String QUERY_EXCEPTION_UNSUPPORTED_DATA_TYPE_FOR_SCALAR_SUBQUERY_RESULT_S_4381E489 =
      "Unsupported data type for scalar subquery result: %s";
  public static final String QUERY_EXCEPTION_TOPKNODE_MUST_BE_APPEARED_AFTER_PUSHLIMITOFFSETINTOTABLESCAN_844A065D =
      "TopKNode must be appeared after PushLimitOffsetIntoTableScan";
  public static final String QUERY_EXCEPTION_TABLE_MODEL_CAN_ONLY_PROCESS_DATA_ONLY_IN_ONE_DATABASE_YET_AB8C1EF5 =
      "Table model can only process data only in one database yet!";
  public static final String QUERY_EXCEPTION_UNSUPPORTED_JOIN_TYPE_IN_PREDICATE_PUSH_DOWN_S_4493D86C =
      "Unsupported join type in predicate push down: %s";
  public static final String QUERY_EXCEPTION_UNSUPPORTED_PLAN_NODE_S_72DD2270 =
      "Unsupported plan node %s";
  public static final String QUERY_EXCEPTION_AN_ERROR_OCCURRED_WHEN_EXECUTING_GETREADABLEDATAREGIONS_A884A6BB =
      "An error occurred when executing getReadableDataRegions():%s";
  public static final String QUERY_EXCEPTION_AN_ERROR_OCCURRED_WHEN_EXECUTING_GETREADABLEDATANODELOCATIONS_54CBD60D =
      "An error occurred when executing getReadableDataNodeLocations():%s";
  public static final String QUERY_EXCEPTION_UNSUPPORTED_QUANTIFIED_COMPARISON_S_C3700430 =
      "Unsupported quantified comparison: %s";
  public static final String QUERY_EXCEPTION_UNEXPECTED_QUANTIFIEDCOMPARISON_S_F13E4EB2 =
      "Unexpected quantifiedComparison: %s";
  public static final String QUERY_EXCEPTION_UNEXPECTED_QUANTIFIER_S_62214B74 =
      "Unexpected Quantifier: %s";
  public static final String QUERY_EXCEPTION_THIS_OPTIMIZER_SHOULD_BE_USED_BEFORE_OPTIMIZER_OF_PUSHAGGREGATIONINTOTABLESCAN_9F6016E3 =
      "This optimizer should be used before optimizer of PushAggregationIntoTableScan";
  public static final String QUERY_EXCEPTION_UNSUPPORTED_VALUEPOINTER_TYPE_S_4147FFFB =
      "Unsupported ValuePointer type: %s";
  public static final String QUERY_EXCEPTION_SYMBOL_S_IS_NOT_EXIST_IN_FETYPEPROVIDER_WITH_S_5CBBFB8B =
      "Symbol: %s is not exist in feTypeProvider with %s";
  public static final String QUERY_EXCEPTION_RIGHT_JOIN_SHOULD_BE_TRANSFORMED_TO_LEFT_JOIN_IN_PREVIOUS_D6B56B1F =
      "RIGHT Join should be transformed to LEFT Join in previous process";
  public static final String QUERY_EXCEPTION_NO_AVAILABLE_DATANODES_MAY_BE_THE_CLUSTER_IS_CLOSING_E13B8C50 =
      "No available dataNodes, may be the cluster is closing";
  public static final String QUERY_EXCEPTION_SHOULD_NEVER_REACH_HERE_CHILD_ORDERING_S_PREGROUPEDSYMBOLS_79A94AB5 =
      "Should never reach here. Child ordering: %s. PreGroupedSymbols: %s";
  public static final String QUERY_EXCEPTION_UNSUPPORTED_TO_SERIALIZE_S_484DAAAF =
      "Unsupported to serialize: %s";
  public static final String QUERY_EXCEPTION_UNSUPPORTED_TO_DESERIALIZE_S_4641CD63 =
      "Unsupported to deserialize: %s";
  public static final String QUERY_EXCEPTION_INSERT_INTO_TABLE_COLUMNS_S_SIZE_SHOULD_BE_SAME_AS_QUERY_E7437397 =
      "insert into table columns's size should be same as query result";
  public static final String QUERY_EXCEPTION_THE_TABLEDEVICEFETCHNODE_S_CLONE_METHOD_SHALL_NOT_BE_CALLED_977C41FD =
      "The TableDeviceFetchNode's clone() method shall not be called.";
  public static final String QUERY_EXCEPTION_UNKNOWN_TABLESCANNODE_TYPE_S_6246EF1E =
      "Unknown TableScanNode type: %s";
  public static final String QUERY_EXCEPTION_UNEXPECTED_SETOPERATION_NODE_TYPE_S_3AE3EECA =
      "unexpected setOperation node type: %s";
  public static final String QUERY_EXCEPTION_UNSUPPORTED_PATTERN_QUANTIFIER_TYPE_S_4ED427E9 =
      "unsupported pattern quantifier type: %s";
  public static final String FAILED_TO_CREATE_PIPE_PLUGIN_PREFIX_FMT =
      "Failed to create pipe plugin %s. ";
  public static final String FAILED_TO_CREATE_PIPE_PREFIX_FMT = "Failed to create pipe %s, ";
  public static final String STATE_ABORTED = "ABORTED";
  public static final String STATE_CLOSED = "CLOSED";
  public static final String LOCAL_SINK_CHANNEL_STATE_IS_WITH_STATE_FMT =
      "LocalSinkChannel state is %s.";
  public static final String UNKNOWN_READ_TYPE_FMT = "unknown read type [%s]";
  public static final String DESERIALIZE_CONSENSUSGROUPID_FAILED_WITH_REASON_FMT =
      "Deserialize ConsensusGroupId failed: %s";
  public static final String CANNOT_ALTER_TEMPLATE_TIMESERIES_TEMPLATE_ALREADY_SET_FMT =
      "Cannot alter template timeseries [%s] since device template [%s] already set on path [%s].";
  public static final String PATH_HAS_NOT_BEEN_SET_ANY_TEMPLATE_FMT =
      "Path [%s] has not been set any template.";
  public static final String FAILED_TO_FETCH_SCHEMA_BECAUSE_OF_UNRECOGNIZED_DATA =
      "Failed to fetch schema because of unrecognized data";
  public static final String ALIGNMENT_ALIGNED = "aligned";
  public static final String ALIGNMENT_NOT_ALIGNED = "not aligned";
  public static final String TREE_MODEL_DATABASE_NAME_MUST_START_WITH_ROOT =
      "the database name in tree model must start with 'root.'.";
  public static final String DATABASE_NAME_LENGTH_SHALL_NOT_EXCEED_FMT =
      "the length of database name shall not exceed %d";
  public static final String MODEL_TABLE = "table";
  public static final String MODEL_TREE = "tree";
  public static final String NO_SYMBOL_MAPPING_FOR_NODE_FMT =
      "No symbol mapping for node '%s' (%s)";
  public static final String NO_MAPPING_FOR_EXPRESSION_WITH_IDENTITY_FMT =
      "No mapping for expression: %s (%s)";
  public static final String UNEXPECTED_QUANTIFIED_COMPARISON_FMT =
      "Unexpected quantified comparison: '%s %s'";
  public static final String TABLE_HAS_NO_PREFIX_FMT = "Table %s has no prefix!";
  public static final String ANALYSIS_DOES_NOT_CONTAIN_INFORMATION_FOR_NODE_FMT =
      "Analysis does not contain information for node: %s";
  public static final String AUTO_CREATE_TABLE_FAILED = "Auto create table failed.";
  public static final String AUTO_CREATE_TABLE_COLUMN_FAILED = "Auto create table column failed.";
  public static final String AUTO_ADD_TABLE_COLUMN_FAILED_WITH_COLUMNS_FMT =
      "Auto add table column failed: %s.%s, %s";
  public static final String QUERY_STATEMENT_NOT_ALLOWED_IN_BATCH_FMT =
      "Query statement not allowed in batch: [%s]";
  public static final String TABLE_LOST_UNEXPECTED_FMT =
      "Table %s in the database %s is lost unexpected.";
  public static final String LOAD_FILE_CROSSES_PARTITIONS_FMT =
      "The data of file %s crosses partitions";
  public static final String LOAD_FILE_CROSSES_PARTITIONS =
      "The data of file crosses partitions";
  public static final String REGION_REPLICA_SET_CHANGED_DURING_LOAD_FMT =
      "Region replica set changed from %s to %s during loading TsFile, maybe due to region migration";
  public static final String REGION_REPLICA_SET_CHANGED_DURING_LOAD =
      "Region replica set changed during loading TsFile, maybe due to region migration";
  public static final String OUT_OF_TTL_FMT =
      "Insertion time [%s] is less than ttl time bound [%s]";
  public static final String DRIVER_TASK_ABORTED_FMT = "DriverTask %s is aborted by %s";
  public static final String DRIVER_TASK_ABORTED_BY_TIMEOUT = "timeout";
  public static final String DRIVER_TASK_ABORTED_BY_FRAGMENT_ABORT_CALLED = "called";
  public static final String DRIVER_TASK_ABORTED_BY_QUERY_CASCADING_ABORTED =
      "query cascading aborted";
  public static final String DRIVER_TASK_ABORTED_BY_ALREADY_BEING_CANCELLED =
      "already being cancelled";
  public static final String DRIVER_TASK_ABORTED_BY_INTERNAL_ERROR_SCHEDULED =
      "internal error scheduled";
  public static final String DRIVER_TASK_ABORTED_BY_MEMORY_NOT_ENOUGH =
      "Memory is not enough to execute the query task.";
  public static final String ROOT_FRAGMENT_INSTANCE_PLACEMENT_ERROR_FMT =
      "Root FragmentInstance placement error: %s";
  public static final String ALL_REPLICA_CANNOT_BE_REACHED_FMT =
      "All replica cannot be reached:%s";
  public static final String LOAD_READ_ONLY_MESSAGE =
      "Current system mode is read only, does not support load file";
  public static final String QUERY_KILLED_BY_OTHERS = "Query was killed by others";
  public static final String QUERY_TIMEOUT_EXCEPTION_MESSAGE =
      "Current query is time out, query start time is %d, ddl is %d, current time is %d, "
          + "please check your statement or modify timeout parameter.";
  public static final String NON_RESERVED_CAN_ONLY_CONTAIN_TOKENS_FOUND_NESTED_RULE =
      "nonReserved can only contain tokens. Found nested rule: ";

  // ---------------------------------------------------------------------------
  // Additional auto-collected messages
  // ---------------------------------------------------------------------------
  public static final String MESSAGE_FAILED_FETCH_STATE_HAS_RETRIED_ARG_TIMES_E7572C66 = "Failed to fetch state, has retried %s times";
  public static final String MESSAGE_IGNORED_CONFIG_ITEMS_FE28ADBC = "ignored config items: ";
  public static final String MESSAGE_BECAUSE_THEY_IMMUTABLE_UNDEFINED_07C04F65 = " because they are immutable or undefined.";
  public static final String MESSAGE_LOAD_ARG_PIECE_ERROR_1ST_PHASE_BECAUSE_F3D9672C = "Load %s piece error in 1st phase. Because ";
  public static final String MESSAGE_LOAD_ARG_ERROR_SECOND_PHASE_BECAUSE_ARG_FIRST_PHASE_ARG_CBA980FC = "Load %s error in second phase. Because %s, first phase is %s";
  public static final String MESSAGE_SUCCESS_260CA9DD = "success";
  public static final String MESSAGE_FAILED_26934EB3 = "failed";
  public static final String MESSAGE_AUDIT_PERMISSION_NEEDED_ALTER_ENCODING_COMPRESSOR_DATABASE_ARG_CC06994D = "'AUDIT' permission is needed to alter the encoding and compressor of database %s";
  public static final String EXCEPTION_QUERYID_IS_NULL_056E92E4 = "queryId is null";
  public static final String EXCEPTION_EXPECTED_TWO_IDS_BUT_GOT_COLON_ARG_020F9D13 = "Expected two ids but got: %s";
  public static final String EXCEPTION_ID_IS_NULL_9D5D27B1 = "id is null";
  public static final String EXCEPTION_NAME_IS_NULL_C8B35959 = "name is null";
  public static final String EXCEPTION_EXPECTEDPARTS_MUST_BE_AT_LEAST_1_B867DB08 = "expectedParts must be at least 1";
  public static final String EXCEPTION_INVALID_ARG_ARG_2946DBE5 = "Invalid %s %s";
  public static final String EXCEPTION_ID_IS_EMPTY_28C94FC0 = "id is empty";
  public static final String EXCEPTION_EXECUTOR_IS_NULL_7FBE03A4 = "executor is null";
  public static final String EXCEPTION_INITIALSTATE_IS_NULL_8992A39F = "initialState is null";
  public static final String EXCEPTION_TERMINALSTATES_IS_NULL_E0FC2A93 = "terminalStates is null";
  public static final String EXCEPTION_EXPECTEDSTATE_IS_NULL_5E8C2F32 = "expectedState is null";
  public static final String EXCEPTION_CURRENTSTATE_IS_NULL_AEDB20DB = "currentState is null";
  public static final String EXCEPTION_STATECHANGELISTENER_IS_NULL_635AE7D2 = "stateChangeListener is null";
  public static final String EXCEPTION_ARG_CANNOT_TRANSITION_FROM_ARG_TO_ARG_8C680D30 = "%s cannot transition from %s to %s";
  public static final String EXCEPTION_CANNOT_FIRE_STATE_CHANGE_EVENT_WHILE_HOLDING_THE_LOCK_35243BC4 = "Cannot fire state change event while holding the lock";
  public static final String EXCEPTION_CANNOT_NOTIFY_WHILE_HOLDING_THE_LOCK_15625D48 = "Cannot notify while holding the lock";
  public static final String EXCEPTION_CANNOT_WAIT_FOR_STATE_CHANGE_WHILE_HOLDING_THE_LOCK_CBD9F784 = "Cannot wait for state change while holding the lock";
  public static final String EXCEPTION_DONESTATE_IS_NULL_D88F77E5 = "doneState is null";
  public static final String EXCEPTION_DONESTATE_ARG_IS_NOT_A_DONE_STATE_8724C618 = "doneState %s is not a done state";
  public static final String EXCEPTION_DATANODEID_SHOULD_BE_INIT_FIRST_13B19A85 = "DataNodeId should be init first!";
  public static final String EXCEPTION_LENGTH_OF_INPUT_COLUMN_LEFT_BRACKET_RIGHT_BRACKET_FOR_MAXBY_SLASH_MINBY_SHOULD_B_1F3F2F1C = "Length of input Column[] for MaxBy/MinBy should be 3";
  public static final String EXCEPTION_PARTIALRESULT_OF_MAXBY_SLASH_MINBY_SHOULD_BE_1_BF0078F4 = "partialResult of MaxBy/MinBy should be 1";
  public static final String EXCEPTION_PARTIALRESULT_OF_MAXVALUE_SHOULD_BE_1_659B6D42 = "partialResult of MaxValue should be 1";
  public static final String EXCEPTION_PARTIALRESULT_OF_AVG_SHOULD_BE_2_7A8C375E = "partialResult of Avg should be 2";
  public static final String EXCEPTION_PARTIALRESULT_OF_MINVALUE_SHOULD_BE_1_C9DAF94D = "partialResult of MinValue should be 1";
  public static final String EXCEPTION_PARTIALRESULT_OF_LASTVALUE_SHOULD_BE_2_68963ECE = "partialResult of LastValue should be 2";
  public static final String EXCEPTION_PARTIALRESULT_OF_MINTIME_SHOULD_BE_1_B7EFE10B = "partialResult of MinTime should be 1";
  public static final String EXCEPTION_PARTIALRESULT_OF_COUNT_SHOULD_BE_1_972B9219 = "partialResult of Count should be 1";
  public static final String EXCEPTION_PARTIALRESULT_OF_FIRSTVALUE_SHOULD_BE_2_3FB20C54 = "partialResult of FirstValue should be 2";
  public static final String EXCEPTION_PARTIALRESULT_OF_COUNT_IF_SHOULD_BE_1_70D01652 = "partialResult of count_if should be 1";
  public static final String EXCEPTION_STEP_IN_SERIESAGGREGATESCANOPERATOR_AND_RAWDATAAGGREGATEOPERATOR_CAN_ONLY_PROCES_5575BD95 = "Step in SeriesAggregateScanOperator and RawDataAggregateOperator can only process raw input";
  public static final String EXCEPTION_RAWDATAAGGREGATEOPERATOR_CAN_ONLY_PROCESS_ONE_TSBLOCK_INPUT_DOT_5ABCB8C0 = "RawDataAggregateOperator can only process one tsBlock input.";
  public static final String EXCEPTION_STEP_IN_AGGREGATEOPERATOR_CANNOT_PROCESS_RAW_INPUT_22620F61 = "Step in AggregateOperator cannot process raw input";
  public static final String EXCEPTION_FINAL_OUTPUT_CAN_ONLY_BE_SINGLE_COLUMN_6D82F9E0 = "Final output can only be single column";
  public static final String EXCEPTION_PARTIALRESULT_OF_SUM_SHOULD_BE_1_40E85216 = "partialResult of Sum should be 1";
  public static final String EXCEPTION_INPUT_OF_SUM_SHOULD_BE_1_D5C11EC8 = "input of Sum should be 1";
  public static final String EXCEPTION_PARTIALRESULT_OF_UDAF_SHOULD_BE_1_E094029D = "partialResult of UDAF should be 1";
  public static final String EXCEPTION_PARTIALRESULT_OF_EXTREMEVALUE_SHOULD_BE_1_A7713D8A = "partialResult of ExtremeValue should be 1";
  public static final String EXCEPTION_WRONG_INPUTDATATYPES_SIZE_DOT_675FF289 = "Wrong inputDataTypes size.";
  public static final String EXCEPTION_INPUT_OF_COUNT_SHOULD_BE_1_C7EEEC46 = "input of Count should be 1";
  public static final String EXCEPTION_PARTIALRESULT_OF_MAXTIME_SHOULD_BE_1_877F7EAC = "partialResult of MaxTime should be 1";
  public static final String EXCEPTION_LOCALMEMORYMANAGER_IS_NULL_DOT_69FE497A = "localMemoryManager is null.";
  public static final String EXCEPTION_TSBLOCKSERDEFACTORY_IS_NULL_DOT_32EB5BD2 = "tsBlockSerdeFactory is null.";
  public static final String EXCEPTION_EXECUTORSERVICE_IS_NULL_DOT_7B057909 = "executorService is null.";
  public static final String EXCEPTION_MPPDATAEXCHANGESERVICECLIENTMANAGER_IS_NULL_DOT_F31E746C = "mppDataExchangeServiceClientManager is null.";
  public static final String EXCEPTION_SHAREDTSBLOCKQUEUE_IS_FULL_87493E26 = "SharedTsBlockQueue is full";
  public static final String EXCEPTION_FRAGMENT_INSTANCE_ID_CANNOT_BE_NULL_4BE84F40 = "fragment instance ID cannot be null";
  public static final String EXCEPTION_PLANNODE_ID_CANNOT_BE_NULL_F91303CD = "PlanNode ID cannot be null";
  public static final String EXCEPTION_LOCAL_MEMORY_MANAGER_CANNOT_BE_NULL_54701481 = "local memory manager cannot be null";
  public static final String EXCEPTION_EXECUTORSERVICE_CAN_NOT_BE_NULL_DOT_220C966B = "ExecutorService can not be null.";
  public static final String EXCEPTION_TSBLOCK_CANNOT_BE_NULL_E7EA3BDA = "TsBlock cannot be null";
  public static final String EXCEPTION_BYTESTORESERVE_SHOULD_BE_GREATER_THAN_ZERO_DOT_56D15DE0 = "bytesToReserve should be greater than zero.";
  public static final String EXCEPTION_MAXBYTESCANRESERVE_SHOULD_BE_GREATER_THAN_ZERO_DOT_E9F7D365 = "maxBytesCanReserve should be greater than zero.";
  public static final String EXCEPTION_MAX_BYTES_SHOULD_BE_GREATER_THAN_ZERO_COLON_ARG_EA1FB495 = "max bytes should be greater than zero: %d";
  public static final String EXCEPTION_MAX_BYTES_PER_FI_SHOULD_BE_IN_LEFT_PAREN_0_COMMA_MAXBYTES_RIGHT_BRACKET_DOT_MAXB_4D37C457 = "max bytes per FI should be in (0,maxBytes]. maxBytesPerFI: %d, maxBytes: %d";
  public static final String EXCEPTION_BYTESTORESERVE_SHOULD_BE_IN_LEFT_PAREN_0_COMMA_MAXBYTESPERFI_RIGHT_BRACKET_DOT_M_0753BB69 = "bytesToReserve should be in (0,maxBytesPerFI]. maxBytesPerFI: %d, bytesToReserve: %d";
  public static final String EXCEPTION_INVALID_FUTURE_TYPE_14507EF5 = "invalid future type ";
  public static final String EXCEPTION_QUERYID_CANNOT_BE_NULL_861D7663 = "queryId cannot be null";
  public static final String EXCEPTION_FRAGMENTINSTANCEID_CANNOT_BE_NULL_C722F460 = "fragmentInstanceId cannot be null";
  public static final String EXCEPTION_PLANNODEID_CANNOT_BE_NULL_4533C72B = "planNodeId cannot be null";
  public static final String EXCEPTION_ID_CAN_NOT_BE_NULL_DOT_BDD2AD7D = "id can not be null.";
  public static final String EXCEPTION_QUERYID_CAN_NOT_BE_NULL_DOT_16639DBE = "queryId can not be null.";
  public static final String EXCEPTION_FRAGMENTINSTANCEID_CAN_NOT_BE_NULL_DOT_E88CF18B = "fragmentInstanceId can not be null.";
  public static final String EXCEPTION_PLANNODEID_CAN_NOT_BE_NULL_DOT_44027620 = "planNodeId can not be null.";
  public static final String EXCEPTION_THE_FUTURE_TO_BE_CANCELLED_CAN_NOT_BE_NULL_DOT_73CE402A = "The future to be cancelled can not be null.";
  public static final String EXCEPTION_LOCK_IS_NOT_REENTRANT_7967C13E = "Lock is not reentrant";
  public static final String EXCEPTION_CURRENT_THREAD_DOES_NOT_HOLD_LOCK_68FFB1D9 = "Current thread does not hold lock";
  public static final String EXCEPTION_ROOT_OPERATOR_SHOULD_NOT_BE_NULL_F4890A7A = "root Operator should not be null";
  public static final String EXCEPTION_SINK_SHOULD_NOT_BE_NULL_3CC4F006 = "Sink should not be null";
  public static final String EXCEPTION_INITIALCOUNT_SHOULDN_QUOTE_T_BE_NULL_HERE_8B333953 = "initialCount shouldn't be null here";
  public static final String EXCEPTION_COUNT_SHOULDN_QUOTE_T_BE_NULL_HERE_1EBA9339 = "count shouldn't be null here";
  public static final String EXCEPTION_TASKID_IS_NULL_E1221EB2 = "taskId is null";
  public static final String EXCEPTION_INSTANCEID_IS_NULL_343234DC = "instanceId is null";
  public static final String EXCEPTION_FRAGMENTINSTANCEID_IS_NULL_4D371DB4 = "fragmentInstanceId is null";
  public static final String EXCEPTION_CURRENT_STATE_IS_ALREADY_DONE_19FC56DC = "Current state is already done";
  public static final String EXCEPTION_SUPPRESSED_IS_NULL_F4CD280B = "suppressed is null";
  public static final String EXCEPTION_STACK_IS_NULL_6844D421 = "stack is null";
  public static final String EXCEPTION_ARG_IS_A_NON_MINUS_DONE_FAILURE_STATE_B167E915 = "%s is a non-done failure state";
  public static final String EXCEPTION_WARNINGCODE_IS_NULL_3CCAE5B7 = "warningCode is null";
  public static final String EXCEPTION_MESSAGE_IS_NULL_D2D078AA = "message is null";
  public static final String EXCEPTION_WARNING_IS_NULL_E5A7C3C1 = "warning is null";
  public static final String EXCEPTION_NO_FIRST_FILE_F5F2E276 = "no first file";
  public static final String EXCEPTION_NO_FIRST_CHUNK_7DCEB14C = "no first chunk";
  public static final String EXCEPTION_CAN_QUOTE_T_INIT_NULL_CHUNKMETA_15C12BEE = "Can't init null chunkMeta";
  public static final String EXCEPTION_OPERATORCONTEXT_IS_NULL_D15B1EDB = "operatorContext is null";
  public static final String EXCEPTION_CHILD_OPERATOR_IS_NULL_8860113C = "child operator is null";
  public static final String EXCEPTION_GROUPBYTIMEPARAMETER_CANNOT_BE_NULL_IN_SLIDINGWINDOWAGGREGATIONOPERATOR_BA42E30D = "GroupByTimeParameter cannot be null in SlidingWindowAggregationOperator";
  public static final String EXCEPTION_REMAININGINPUTLOCATIONS_IS_NULL_CEBBA2C1 = "remainingInputLocations is null";
  public static final String EXCEPTION_LAST_QUERY_RESULT_SHOULD_ONLY_HAVE_ONE_RECORD_EDFEE635 = "last query result should only have one record";
  public static final String EXCEPTION_OUTPUTPATHS_SHOULDN_QUOTE_T_BE_NULL_BF3F5FB4 = "outputPaths shouldn't be null";
  public static final String EXCEPTION_CHILD_SIZE_OF_INNERTIMEJOINOPERATOR_SHOULD_BE_LARGER_THAN_1_37EB7D74 = "child size of InnerTimeJoinOperator should be larger than 1";
  public static final String EXCEPTION_CHILD_SIZE_OF_VERTICALLYCONCATOPERATOR_SHOULD_BE_LARGER_THAN_0_14A2513A = "child size of VerticallyConcatOperator should be larger than 0";
  public static final String EXCEPTION_CHILD_SIZE_OF_TIMEJOINOPERATOR_SHOULD_BE_LARGER_THAN_0_EDED9CB8 = "child size of TimeJoinOperator should be larger than 0";
  public static final String EXCEPTION_DATASTORE_IS_NULL_D9972B2E = "dataStore is null";
  public static final String EXCEPTION_LASTVALUESCACHERESULTS_SHOULDN_QUOTE_T_BE_NULL_HERE_0DCD5841 = "lastValuesCacheResults shouldn't be null here";
  public static final String EXCEPTION_ACCUMULATOR_SHOULD_BE_LASTDESCACCUMULATOR_WHEN_REACH_HERE_CE38F96A = "Accumulator should be LastDescAccumulator when reach here";
  public static final String EXCEPTION_DRIVERTASK_TO_BE_PUSHED_IS_NULL_7581A0E3 = "DriverTask to be pushed is null";
  public static final String EXCEPTION_DRIVERTASK_IS_NULL_A13D4AF9 = "driverTask is null";
  public static final String EXCEPTION_SELECTED_LEVEL_CAN_NOT_EQUAL_TO_MINUS_1_1DA93B14 = "selected level can not equal to -1";
  public static final String EXCEPTION_RESULT_DRIVERTASK_CANNOT_BE_NULL_30A06DB1 = "result driverTask cannot be null";
  public static final String EXCEPTION_DRIVERTASKQUEUE_IS_NULL_7C3C8B7B = "driverTaskQueue is null";
  public static final String EXCEPTION_MAXDRIVERSPERTASK_IS_NULL_8408F9B7 = "maxDriversPerTask is null";
  public static final String EXCEPTION_QUEUE_CAN_NOT_BE_NULL_DOT_9BB286B1 = "queue can not be null.";
  public static final String EXCEPTION_SOURCEHANDLELISTENER_CAN_NOT_BE_NULL_DOT_01817F52 = "sourceHandleListener can not be null.";
  public static final String EXCEPTION_LOCALFRAGMENTINSTANCEID_CAN_NOT_BE_NULL_DOT_37F5917D = "localFragmentInstanceId can not be null.";
  public static final String EXCEPTION_LOCALPLANNODEID_CAN_NOT_BE_NULL_DOT_44A34A33 = "localPlanNodeId can not be null.";
  public static final String EXCEPTION_START_SEQUENCE_ID_SHOULD_BE_GREATER_THAN_OR_EQUAL_TO_ZERO_DOT_START_SEQUENCE_ID__D3C0AAB7 = "Start sequence ID should be greater than or equal to zero. Start sequence ID: ";
  public static final String EXCEPTION_COMMA_END_SEQUENCE_ID_COLON_DB1AF173 = ", end sequence ID: ";
  public static final String EXCEPTION_END_SEQUENCE_ID_SHOULD_BE_GREATER_THAN_THE_START_SEQUENCE_ID_DOT_START_SEQUENCE__DF1AA2A1 = "End sequence ID should be greater than the start sequence ID. Start sequence ID: ";
  public static final String EXCEPTION_RESERVED_BYTES_SHOULD_BE_GREATER_THAN_ZERO_DOT_64086BE5 = "Reserved bytes should be greater than zero.";
  public static final String EXCEPTION_REMOTEENDPOINT_CAN_NOT_BE_NULL_DOT_DE2B5885 = "remoteEndpoint can not be null.";
  public static final String EXCEPTION_REMOTEFRAGMENTINSTANCEID_CAN_NOT_BE_NULL_DOT_C2449A29 = "remoteFragmentInstanceId can not be null.";
  public static final String EXCEPTION_LOCALMEMORYMANAGER_CAN_NOT_BE_NULL_DOT_7A46C6CE = "localMemoryManager can not be null.";
  public static final String EXCEPTION_EXECUTORSERVICE_CAN_NOT_BE_NULL_DOT_BC459BD4 = "executorService can not be null.";
  public static final String EXCEPTION_SERDE_CAN_NOT_BE_NULL_DOT_D46F66E7 = "serde can not be null.";
  public static final String EXCEPTION_DOT_9D9B854A = ".";
  public static final String EXCEPTION_START_SEQUENCE_ID_SHOULD_BE_GREATER_THAN_OR_EQUAL_TO_ZERO_COMMA_BUT_WAS_COLON_4D2D708E = "Start sequence ID should be greater than or equal to zero, but was: ";
  public static final String EXCEPTION_REMOTEENDPOINT_CAN_NOT_BE_NULL_DOT_83488ACF = "remoteEndPoint can not be null.";
  public static final String EXCEPTION_REMOTEPLANNODEID_CAN_NOT_BE_NULL_DOT_03956DE2 = "remotePlanNodeId can not be null.";
  public static final String EXCEPTION_SINKLISTENER_CAN_NOT_BE_NULL_DOT_32C9E7C0 = "sinkListener can not be null.";
  public static final String EXCEPTION_TSBLOCKS_IS_NULL_02287FD8 = "tsBlocks is null";
  public static final String EXCEPTION_DOWNSTREAMCHANNELLIST_CAN_NOT_BE_NULL_DOT_417AD5A3 = "downStreamChannelList can not be null.";
  public static final String EXCEPTION_DOWNSTREAMCHANNELINDEX_CAN_NOT_BE_NULL_DOT_A1D5A266 = "downStreamChannelIndex can not be null.";
  public static final String EXCEPTION_STEP_IN_SLIDINGWINDOWAGGREGATIONOPERATOR_CAN_ONLY_PROCESS_PARTIAL_RESULT_E221A2C5 = "Step in SlidingWindowAggregationOperator can only process partial result";
  public static final String EXCEPTION_SLIDINGWINDOWAGGREGATIONOPERATOR_CAN_ONLY_PROCESS_ONE_TSBLOCK_INPUT_DOT_7B9FCAB7 = "SlidingWindowAggregationOperator can only process one tsBlock input.";
  public static final String EXCEPTION_IS_NULL_97AAF381 = " is null";
  public static final String EXCEPTION_MPP_DATA_EXCHANGE_MANAGER_SHOULD_NOT_BE_NULL_44D7141E = "MPP_DATA_EXCHANGE_MANAGER should not be null";
  public static final String EXCEPTION_ROOTOPERATOR_IS_NULL_050A1E79 = "rootOperator is null";
  public static final String EXCEPTION_DRIVERCONTEXT_IS_NULL_4FEBE55F = "driverContext is null";
  public static final String EXCEPTION_QUERY_CONTEXT_CANNOT_BE_NULL_C4809234 = "Query context cannot be null";
  public static final String EXCEPTION_DESCRIPTOR_QUOTE_S_INPUT_EXPRESSION_SIZE_IS_NOT_1_DA4BED50 = "descriptor's input expression size is not 1";
  public static final String EXCEPTION_GROUPBYLEVEL_DESCRIPTORLIST_CANNOT_BE_EMPTY_34604314 = "GroupByLevel descriptorList cannot be empty";
  public static final String EXCEPTION_GROUPBYTAG_TAG_KEYS_CANNOT_BE_EMPTY_5D649624 = "GroupByTag tag keys cannot be empty";
  public static final String EXCEPTION_GROUPBYTAG_AGGREGATION_DESCRIPTORS_CANNOT_BE_EMPTY_82EC14EB = "GroupByTag aggregation descriptors cannot be empty";
  public static final String EXCEPTION_AGGREGATION_DESCRIPTORLIST_CANNOT_BE_EMPTY_490C1740 = "Aggregation descriptorList cannot be empty";
  public static final String EXCEPTION_PUSH_DOWN_PREDICATE_IS_NOT_SUPPORTED_YET_178F04A1 = "Push down predicate is not supported yet";
  public static final String EXCEPTION_SINK_IS_NULL_E33854B4 = "sink is null";
  public static final String EXCEPTION_THERE_MUST_BE_AT_MOST_ONE_SINKNODE_A965AFE7 = "There must be at most one SinkNode";
  public static final String EXCEPTION_QUERY_CONTEXT_CANNOT_BE_NULL_DOT_2D3369FE = "Query context cannot be null.";
  public static final String EXCEPTION_UNEXPECTED_NODE_TYPE_COLON_41FBCBF3 = "Unexpected node type: ";
  public static final String EXCEPTION_TEMPLATEDINFO_SHOULD_NOT_BE_NULL_B5898568 = "TemplatedInfo should not be null";
  public static final String EXCEPTION_RESULTHANDLE_IN_COORDINATOR_SHOULD_BE_INIT_FIRSTLY_DOT_0F44159B = "ResultHandle in Coordinator should be init firstly.";
  public static final String EXCEPTION_EXPRESSION_IS_NOT_ANALYZED_COLON_ARG_7D34C49A = "Expression is not analyzed: %s";
  public static final String EXCEPTION_PATH_QUOTE_ARG_QUOTE_IS_NOT_ANALYZED_IN_GROUPBYLEVELHELPER_DOT_BCEE9D39 = "path '%s' is not analyzed in GroupByLevelHelper.";
  public static final String EXCEPTION_SYMBOL_IS_NULL_AE539B31 = "symbol is null";
  public static final String EXCEPTION_NO_TYPE_FOUND_FOR_SYMBOL_QUOTE_ARG_QUOTE_IN_TYPEPROVIDER_F4DD9DF7 = "no type found for symbol '%s' in TypeProvider";
  public static final String EXCEPTION_OUTPUT_COLUMN_QUOTE_ARG_QUOTE_IS_NOT_STORED_IN_ARG_2DE3176D = "output column '%s' is not stored in %s";
  public static final String EXCEPTION_PATTERNSTRING_CANNOT_BE_NULL_8A2903F8 = "patternString cannot be null";
  public static final String EXCEPTION_THE_LENGTH_OF_CASEWHENTHENEXPRESSION_QUOTE_S_WHENTHENLIST_MUST_GREATER_THAN_0_069775ED = "the length of CaseWhenThenExpression's whenThenList must greater than 0";
  public static final String EXCEPTION_QUERYCONTEXT_IS_NULL_C2344379 = "QueryContext is null";
  public static final String EXCEPTION_PREDICATE_LEFT_BRACKET_ARG_RIGHT_BRACKET_SHOULD_BE_SIMPLIFIED_IN_PREVIOUS_STEP_9262C154 = "Predicate [%s] should be simplified in previous step";
  public static final String EMPTY_MESSAGE = "";
  public static final String EXCEPTION_SEMICOLON_0FAEF84A = "; ";
  public static final String EXCEPTION_THE_TSBLOCK_SHOULD_NOT_BE_NULL_WHEN_CONSTRUCTING_MEMORYSOURCEHANDLE_8D205293 = "the TsBlock should not be null when constructing MemorySourceHandle";
  public static final String EXCEPTION_THE_TIME_ORDER_IS_NOT_SPECIFIED_DOT_624A7526 = "The time order is not specified.";
  public static final String EXCEPTION_THE_TIMESERIES_ORDER_IS_NOT_SPECIFIED_DOT_68EE3875 = "The timeseries order is not specified.";
  public static final String EXCEPTION_THE_DEVICE_ORDER_IS_NOT_SPECIFIED_DOT_D3FB9559 = "The device order is not specified.";
  public static final String EXCEPTION_SLASH_BC35AB27 = "/";
  public static final String EXCEPTION_INFORMATIONSCHEMATABLESCANNODE_MUST_HAVE_REGIONREPLICASET_0411DBCB = "InformationSchemaTableScanNode must have regionReplicaSet";
  public static final String EXCEPTION_EACH_INFORMATIONSCHEMATABLESCANNODE_HAVE_ONLY_ONE_DATANODELOCATION_FA3E82C4 = "each InformationSchemaTableScanNode have only one DataNodeLocation";
  public static final String EXCEPTION_CHILD_OF_EXCHANGENODE_MUST_BE_MULTICHILDRENSINKNODE_1BF715FD = "child of ExchangeNode must be MultiChildrenSinkNode";
  public static final String EXCEPTION_SIZE_OF_SOURCELOCATIONS_SHOULD_BE_LARGER_THAN_0_2EC41A23 = "size of sourceLocations should be larger than 0";
  public static final String EXCEPTION_INDEX_IS_NOT_VALID_2AB4FB3A = "index is not valid";
  public static final String EXCEPTION_LINES_OF_BOX_STRING_SHOULD_BE_GREATER_THAN_0_5DB8C047 = "Lines of box string should be greater than 0";
  public static final String EXCEPTION_WRONG_NUMBER_OF_NEW_CHILDREN_817AF800 = "wrong number of new children";
  public static final String EXCEPTION_TRANSLATIONS_IS_NULL_37D62ADC = "translations is null";
  public static final String EXCEPTION_ROOT_IS_NULL_ECC8987D = "root is null";
  public static final String EXCEPTION_SYMBOLHINT_IS_NULL_CE874C40 = "symbolHint is null";
  public static final String EXCEPTION_TYPE_IS_NULL_16A3D3EB = "type is null";
  public static final String EXCEPTION_SYMBOLHINT_NOT_IN_SYMBOLS_MAP_B0D67E43 = "symbolHint not in symbols map";
  public static final String EXCEPTION_PLANNERCONTEXT_IS_NULL_B7C7DE50 = "plannerContext is null";
  public static final String EXCEPTION_OBJECTS_IS_NULL_819EE879 = "objects is null";
  public static final String EXCEPTION_TYPES_IS_NULL_E4B2309D = "types is null";
  public static final String EXCEPTION_OBJECTS_AND_TYPES_DO_NOT_HAVE_THE_SAME_SIZE_8B51E17B = "objects and types do not have the same size";
  public static final String EXCEPTION_OUTERCONTEXT_IS_NULL_031CD366 = "outerContext is null";
  public static final String EXCEPTION_SCOPE_IS_NULL_4F364BA2 = "scope is null";
  public static final String EXCEPTION_ANALYSIS_IS_NULL_66666A58 = "analysis is null";
  public static final String EXCEPTION_FIELDSYMBOLS_IS_NULL_5130E49C = "fieldSymbols is null";
  public static final String EXCEPTION_ASTTOSYMBOLS_IS_NULL_80B3970F = "astToSymbols is null";
  public static final String EXCEPTION_TOO_FEW_PARAMETER_VALUES_2F7358C6 = "Too few parameter values";
  public static final String EXCEPTION_SYMBOLALLOCATOR_IS_NULL_E2BE1908 = "symbolAllocator is null";
  public static final String EXCEPTION_QUERYCONTEXT_IS_NULL_761DB539 = "queryContext is null";
  public static final String EXCEPTION_SESSION_IS_NULL_6CF0F47D = "session is null";
  public static final String EXCEPTION_RECURSIVESUBQUERIES_IS_NULL_6AD8A180 = "recursiveSubqueries is null";
  public static final String EXCEPTION_PREDICATEWITHUNCORRELATEDSCALARSUBQUERYRECONSTRUCTOR_IS_NULL_B264FEBC = "predicateWithUncorrelatedScalarSubqueryReconstructor is null";
  public static final String EXCEPTION_ONLY_SUPPORT_ONE_GROUPINGSET_NOW_A1277FA4 = "Only support one groupingSet now";
  public static final String EXCEPTION_EXPRESSION_IS_NULL_16C079B5 = "expression is null";
  public static final String EXCEPTION_EXPRESSIONTYPES_IS_NULL_4107A4A2 = "expressionTypes is null";
  public static final String EXCEPTION_TYPE_NOT_FOUND_FOR_EXPRESSION_COLON_ARG_C26C9237 = "Type not found for expression: %s";
  public static final String EXCEPTION_VALUE_REACH_HERE_MUST_BE_EXPRESSION_C7CA1971 = "Value reach here must be Expression";
  public static final String EXCEPTION_PLAN_IS_NULL_717C9DF7 = "plan is null";
  public static final String EXCEPTION_LOOKUP_IS_NULL_B8FD7E65 = "lookup is null";
  public static final String EXCEPTION_CONSUMER_IS_NULL_B6207072 = "consumer is null";
  public static final String EXCEPTION_FIELDMAPPINGS_IS_NULL_C3681969 = "fieldMappings is null";
  public static final String EXCEPTION_NO_FIELD_MINUS_GREATER_THAN_SYMBOL_MAPPING_FOR_FIELD_ARG_698FDF06 = "No field->symbol mapping for field %s";
  public static final String EXCEPTION_FOR_NOW_COMMA_ONLY_SINGLE_COLUMN_SUBQUERIES_ARE_SUPPORTED_AD9593BE = "For now, only single column subqueries are supported";
  public static final String EXCEPTION_FOR_NOW_COMMA_ONLY_SINGLE_COLUMN_SUBQUERIES_ARE_SUPPORTED_DOT_068B1A66 = "For now, only single column subqueries are supported.";
  public static final String EXCEPTION_CLUSTER_IS_EMPTY_22299EED = "Cluster is empty";
  public static final String EXCEPTION_CLUSTER_CONTAINS_EXPRESSIONS_THAT_ARE_NOT_EQUIVALENT_TO_EACH_OTHER_7AD9A0E3 = "Cluster contains expressions that are not equivalent to each other";
  public static final String EXCEPTION_WARNINGCOLLECTOR_IS_NULL_7A524A68 = "warningCollector is null";
  public static final String EXCEPTION_MEASURES_IS_NULL_EC9D2431 = "measures is null";
  public static final String EXCEPTION_MEASUREOUTPUTS_IS_NULL_923F7C4B = "measureOutputs is null";
  public static final String EXCEPTION_SKIPTOPOSITION_IS_NULL_EFBA10CA = "skipToPosition is null";
  public static final String EXCEPTION_PATTERN_IS_NULL_AC4E239A = "pattern is null";
  public static final String EXCEPTION_VARIABLEDEFINITIONS_IS_NULL_5F7B8ED4 = "variableDefinitions is null";
  public static final String EXCEPTION_NO_RELATIONS_SPECIFIED_FOR_UNION_70CE42C4 = "No relations specified for UNION";
  public static final String EXCEPTION_NO_RELATIONS_SPECIFIED_FOR_INTERSECT_76B0ED3B = "No relations specified for intersect";
  public static final String EXCEPTION_NO_RELATIONS_SPECIFIED_FOR_EXCEPT_C8E4B4AA = "No relations specified for except";
  public static final String EXCEPTION_NODE_IS_NULL_C1479F4A = "node is null";
  public static final String EXCEPTION_EXPRESSIONS_MUST_BE_IN_THE_SAME_LOCAL_SCOPE_CCAD793E = "Expressions must be in the same local scope";
  public static final String EXCEPTION_WHERE_IS_NULL_A1A3FCBC = "where is null";
  public static final String EXCEPTION_SKIPONLY_IS_NULL_80DB0703 = "skipOnly is null";
  public static final String EXCEPTION_UNABLE_TO_REMOVE_PLAN_NODE_AS_IT_CONTAINS_0_OR_MORE_THAN_1_CHILDREN_6F26E194 = "Unable to remove plan node as it contains 0 or more than 1 children";
  public static final String EXCEPTION_FIELDS_IS_NULL_DE209DBF = "fields is null";
  public static final String EXCEPTION_CANNOT_RESOLVE_SYMBOL_ARG_79F76FA6 = "Cannot resolve symbol %s";
  public static final String EXCEPTION_COLUMNREFERENCES_IS_NULL_124955C5 = "columnReferences is null";
  public static final String EXCEPTION_SCOPEEQUALITIES_IS_NULL_22388B2C = "scopeEqualities is null";
  public static final String EXCEPTION_SCOPECOMPLEMENTEQUALITIES_IS_NULL_B9080FC7 = "scopeComplementEqualities is null";
  public static final String EXCEPTION_SCOPESTRADDLINGEQUALITIES_IS_NULL_F6B979AE = "scopeStraddlingEqualities is null";
  public static final String EXCEPTION_SYMBOLTYPES_IS_NULL_DD16EA83 = "symbolTypes is null";
  public static final String EXCEPTION_EXPRESSION_CANNOT_BE_NULL_EFF1A99C = "expression cannot be null";
  public static final String EXCEPTION_TYPE_CANNOT_BE_NULL_97A0A8D3 = "type cannot be null";
  public static final String EXCEPTION_NO_TYPE_FOR_COLON_ARG_9E34AD76 = "No type for: %s";
  public static final String EXCEPTION_CONDITION_MUST_BE_BOOLEAN_COLON_ARG_806C2960 = "Condition must be boolean: %s";
  public static final String EXCEPTION_TYPES_MUST_BE_EQUAL_COLON_ARG_VS_ARG_098424AD = "Types must be equal: %s vs %s";
  public static final String EXCEPTION_ELEMENTS_IS_NULL_3451C1DA = "elements is null";
  public static final String EXCEPTION_PREDICATE_IS_NULL_22E687A9 = "predicate is null";
  public static final String EXCEPTION_MAPPER_IS_NULL_1D7789D1 = "mapper is null";
  public static final String EXCEPTION_FUNCTION_IS_NULL_E0FA4B62 = "function is null";
  public static final String EXCEPTION_N_MUST_BE_GREATER_THAN_OR_EQUAL_TO_ZERO_C4CE8BF0 = "n must be greater than or equal to zero";
  public static final String EXCEPTION_LOOKUPTYPE_IS_NULL_190054FA = "lookupType is null";
  public static final String EXCEPTION_OPERATORTYPE_IS_NULL_CEA6E3D3 = "operatorType is null";
  public static final String EXCEPTION_ARGUMENTTYPES_IS_NULL_1E377BFD = "argumentTypes is null";
  public static final String EXCEPTION_RETURNTYPE_IS_NULL_07C7C6A5 = "returnType is null";
  public static final String EXCEPTION_TABLE_IS_NULL_8DDD9098 = "table is null";
  public static final String EXCEPTION_COLUMNS_IS_NULL_6C8F32B3 = "columns is null";
  public static final String EXCEPTION_COMMENT_IS_NULL_0AD46118 = "comment is null";
  public static final String EXCEPTION_STATEMENTANALYZERFACTORY_IS_NULL_D309BAB5 = "statementAnalyzerFactory is null";
  public static final String EXCEPTION_FIELD_IS_NULL_80E8CE23 = "field is null";
  public static final String EXCEPTION_PARAMETERS_IS_NULL_418C7892 = "parameters is null";
  public static final String EXCEPTION_TABLEREFERENCE_IS_NULL_C02D3A8F = "tableReference is null";
  public static final String EXCEPTION_QUERY_IS_NULL_689B7978 = "query is null";
  public static final String EXCEPTION_RECURSIVEREFERENCE_IS_NULL_24B9D5DC = "recursiveReference is null";
  public static final String EXCEPTION_ROOT_STATEMENT_IS_ANALYSIS_IS_NULL_36BCD4D1 = "root statement is analysis is null";
  public static final String EXCEPTION_ACCESSCONTROL_IS_NULL_F534EBDD = "accessControl is null";
  public static final String EXCEPTION_IDENTITY_IS_NULL_846265BA = "identity is null";
  public static final String EXCEPTION_HANDLE_IS_NULL_E82FA480 = "handle is null";
  public static final String EXCEPTION_TABLENAME_IS_NULL_20708596 = "tableName is null";
  public static final String EXCEPTION_COLUMNNAME_IS_NULL_81635BA6 = "columnName is null";
  public static final String EXCEPTION_AUTHORIZATION_IS_NULL_7CCA692F = "authorization is null";
  public static final String EXCEPTION_VALUETYPE_IS_NULL_A8582B5F = "valueType is null";
  public static final String EXCEPTION_VALUECOERCION_IS_NULL_E1A004BF = "valueCoercion is null";
  public static final String EXCEPTION_SUBQUERYCOERCION_IS_NULL_33646290 = "subqueryCoercion is null";
  public static final String EXCEPTION_PARTITIONBY_IS_NULL_84791B6B = "partitionBy is null";
  public static final String EXCEPTION_ORDERBY_IS_NULL_AA2494DE = "orderBy is null";
  public static final String EXCEPTION_FRAME_IS_NULL_5A92D609 = "frame is null";
  public static final String EXCEPTION_ATLEAST_IS_NULL_2FE8D701 = "atLeast is null";
  public static final String EXCEPTION_ATMOST_IS_NULL_778B3B3A = "atMost is null";
  public static final String EXCEPTION_FILLEDVALUE_IS_NULL_1FA907D6 = "filledValue is null";
  public static final String EXCEPTION_FIELDREFERENCE_IS_NULL_0B07EA50 = "fieldReference is null";
  public static final String EXCEPTION_QUERY_IS_NOT_REGISTERED_AS_EXPANDABLE_FAAD8FC9 = "query is not registered as expandable";
  public static final String EXCEPTION_EXPRESSION_NOT_ANALYZED_COLON_ARG_D397B665 = "Expression not analyzed: %s";
  public static final String EXCEPTION_EXPRESSION_IS_NOT_A_COLUMN_REFERENCE_COLON_ARG_7957E705 = "Expression is not a column reference: %s";
  public static final String EXCEPTION_EXPECTED_JOIN_FIELDS_FOR_LEFT_AND_RIGHT_TO_HAVE_THE_SAME_SIZE_21BFD449 = "Expected join fields for left and right to have the same size";
  public static final String EXCEPTION_NO_COLUMNS_GIVEN_TO_INSERT_52C42E47 = "No columns given to insert";
  public static final String EXCEPTION_MISSING_FILLANALYSIS_FOR_NODE_ARG_7E5B19A4 = "missing FillAnalysis for node %s";
  public static final String EXCEPTION_MISSING_OFFSET_VALUE_FOR_NODE_ARG_4B107520 = "missing OFFSET value for node %s";
  public static final String EXCEPTION_MISSING_LIMIT_VALUE_FOR_NODE_ARG_DD5FD777 = "missing LIMIT value for node %s";
  public static final String EXCEPTION_MISSING_RANGE_FOR_QUANTIFIER_ARG_03461228 = "missing range for quantifier %s";
  public static final String EXCEPTION_MISSING_UNDEFINED_LABELS_FOR_ARG_CA615EC2 = "missing undefined labels for %s";
  public static final String EXCEPTION_NO_FIELD_FOR_97419CB1 = "No Field for ";
  public static final String EXCEPTION_RELATIONID_IS_NULL_C4683108 = "relationId is null";
  public static final String EXCEPTION_FIELDINDEX_MUST_BE_NON_MINUS_NEGATIVE_COMMA_GOT_COLON_ARG_09C2C06D = "fieldIndex must be non-negative, got: %s";
  public static final String EXCEPTION_TYPEMANAGER_IS_NULL_12A72016 = "typeManager is null";
  public static final String EXCEPTION_NODES_IS_NULL_7AB3C1D7 = "nodes is null";
  public static final String EXCEPTION_CLAZZ_IS_NULL_7F710E3E = "clazz is null";
  public static final String EXCEPTION_GROUPBYEXPRESSIONS_IS_NULL_BFDC07D2 = "groupByExpressions is null";
  public static final String EXCEPTION_SOURCESCOPE_IS_NULL_4B3626A7 = "sourceScope is null";
  public static final String EXCEPTION_ORDERBYSCOPE_IS_NULL_A6017E73 = "orderByScope is null";
  public static final String EXCEPTION_NO_FIELD_FOR_E99DCE9A = "No field for ";
  public static final String EXCEPTION_INVALID_PARAMETER_NUMBER_ARG_COMMA_MAX_VALUES_IS_ARG_B3F5C4E8 = "Invalid parameter number %s, max values is %s";
  public static final String EXCEPTION_GROUPING_FIELD_ARG_SHOULD_ORIGINATE_FROM_ARG_6DBBCE6B = "Grouping field %s should originate from %s";
  public static final String EXCEPTION_ALLLABELS_IS_NULL_9F240FB5 = "allLabels is null";
  public static final String EXCEPTION_DESCRIPTOR_IS_NULL_E6EC1F14 = "descriptor is null";
  public static final String EXCEPTION_ARGUMENTS_IS_NULL_B1F6D4F2 = "arguments is null";
  public static final String EXCEPTION_MODE_IS_NULL_54A948DB = "mode is null";
  public static final String EXCEPTION_LABELS_IS_NULL_F4FBBECE = "labels is null";
  public static final String EXCEPTION_MATCHNUMBERCALLS_IS_NULL_EC08D0D0 = "matchNumberCalls is null";
  public static final String EXCEPTION_CLASSIFIERCALLS_IS_NULL_92AA8B77 = "classifierCalls is null";
  public static final String EXCEPTION_LABEL_IS_NULL_B21FE26B = "label is null";
  public static final String EXCEPTION_NAVIGATION_IS_NULL_3D0CBEE1 = "navigation is null";
  public static final String EXCEPTION_ANCHOR_IS_NULL_4AF93E60 = "anchor is null";
  public static final String EXCEPTION_FIELD_CANNOT_BE_NULL_09155004 = "field cannot be null";
  public static final String EXCEPTION_FIELD_QUOTE_ARG_QUOTE_NOT_FOUND_1BC2FDED = "Field '%s' not found";
  public static final String EXCEPTION_PARENT_IS_NULL_ED81BAD8 = "parent is null";
  public static final String EXCEPTION_RELATION_IS_NULL_890596ED = "relation is null";
  public static final String EXCEPTION_NAMEDQUERIES_IS_NULL_AFDE9A4A = "namedQueries is null";
  public static final String EXCEPTION_TABLES_IS_NULL_2012309E = "tables is null";
  public static final String EXCEPTION_RELATIONTYPE_IS_NULL_62DDF9C1 = "relationType is null";
  public static final String EXCEPTION_BASISTYPE_IS_NULL_33E4F842 = "basisType is null";
  public static final String EXCEPTION_PARENT_IS_ALREADY_SET_835DE0A5 = "parent is already set";
  public static final String EXCEPTION_QUERY_QUOTE_ARG_QUOTE_IS_ALREADY_ADDED_F3D47DBD = "Query '%s' is already added";
  public static final String EXCEPTION_MISSING_SCOPE_D573869F = "missing scope";
  public static final String EXCEPTION_MISSING_RELATIONTYPE_679D3CFA = "missing relationType";
  public static final String EXCEPTION_SUBQUERYINPREDICATES_IS_NULL_5A37F1C8 = "subqueryInPredicates is null";
  public static final String EXCEPTION_SUBQUERIES_IS_NULL_0D5EA053 = "subqueries is null";
  public static final String EXCEPTION_EXISTSSUBQUERIES_IS_NULL_5EA140F5 = "existsSubqueries is null";
  public static final String EXCEPTION_QUANTIFIEDCOMPARISONS_IS_NULL_A30F5121 = "quantifiedComparisons is null";
  public static final String EXCEPTION_WINDOWFUNCTIONS_IS_NULL_D77C3CD5 = "windowFunctions is null";
  public static final String EXCEPTION_ORIGINTABLE_IS_NULL_18AC52C3 = "originTable is null";
  public static final String EXCEPTION_RELATIONALIAS_IS_NULL_C363AD25 = "relationAlias is null";
  public static final String EXCEPTION_ORIGINCOLUMNNAME_IS_NULL_98607162 = "originColumnName is null";
  public static final String EXCEPTION_METADATA_IS_NULL_6F8F9BA0 = "metadata is null";
  public static final String EXCEPTION_CONTEXT_IS_NULL_E329B664 = "context is null";
  public static final String EXCEPTION_GETRESOLVEDWINDOW_IS_NULL_2438758C = "getResolvedWindow is null";
  public static final String EXCEPTION_GETPREANALYZEDTYPE_IS_NULL_FBB2EC7D = "getPreanalyzedType is null";
  public static final String EXCEPTION_BASESCOPE_IS_NULL_ABE8F618 = "baseScope is null";
  public static final String EXCEPTION_PATTERNRECOGNITIONCONTEXT_IS_NULL_59C665F1 = "patternRecognitionContext is null";
  public static final String EXCEPTION_CORRELATIONSUPPORT_IS_NULL_E0D669BF = "correlationSupport is null";
  public static final String EXCEPTION_FUNCTIONINPUTTYPES_IS_NULL_3030658F = "functionInputTypes is null";
  public static final String EXCEPTION_COLUMN_IS_NULL_0C404041 = "column is null";
  public static final String EXCEPTION_EXPRESSION_NOT_YET_ANALYZED_COLON_ARG_0F4F19B7 = "Expression not yet analyzed: %s";
  public static final String EXCEPTION_ARG_ALREADY_KNOWN_TO_REFER_TO_ARG_8C8B4F24 = "%s already known to refer to %s";
  public static final String EXCEPTION_NO_RESOLVED_WINDOW_FOR_COLON_AED19667 = "no resolved window for: ";
  public static final String EXCEPTION_NO_LABEL_AVAILABLE_8508CE32 = "no label available";
  public static final String EXCEPTION_REWRITES_IS_NULL_4E5AD77A = "rewrites is null";
  public static final String EXCEPTION_STATEMENT_REWRITE_RETURNED_NULL_AB1E89EA = "Statement rewrite returned null";
  public static final String EXCEPTION_ATN_IS_NULL_48BE0D3E = "atn is null";
  public static final String EXCEPTION_LEXER_IS_NULL_88834E18 = "lexer is null";
  public static final String EXCEPTION_PARSER_IS_NULL_AE8E5D6F = "parser is null";
  public static final String EXCEPTION_LEXER_ATN_MISMATCH_COLON_EXPECTED_ARG_COMMA_FOUND_ARG_8ED22CF1 = "Lexer ATN mismatch: expected %s, found %s";
  public static final String EXCEPTION_PARSER_ATN_MISMATCH_COLON_EXPECTED_ARG_COMMA_FOUND_ARG_FF75D61B = "Parser ATN mismatch: expected %s, found %s";
  public static final String EXCEPTION_INITIALIZER_IS_NULL_2EEC3764 = "initializer is null";
  public static final String EXCEPTION_TERMINALNODE_IS_NULL_578F27FD = "terminalNode is null";
  public static final String EXCEPTION_PARSERRULECONTEXT_IS_NULL_9E0DD3B5 = "parserRuleContext is null";
  public static final String EXCEPTION_TOKEN_IS_NULL_43094C56 = "token is null";
  public static final String EXCEPTION_VALUE_IS_NULL_192F6BFF = "value is null";
  public static final String EXCEPTION_LOCATION_IS_NULL_F134D388 = "location is null";
  public static final String EXCEPTION_TOPIC_NAME_CAN_NOT_BE_NULL_EA4ED0BF = "topic name can not be null";
  public static final String EXCEPTION_SOURCE_NAME_IS_NULL_287E475D = "source name is null";
  public static final String EXCEPTION_TARGET_NAME_IS_NULL_A5F701C6 = "target name is null";
  public static final String EXCEPTION_DBNAME_IS_NULL_4521C4EE = "dbName is null";
  public static final String EXCEPTION_PROPERTIES_IS_NULL_57B88B49 = "properties is null";
  public static final String EXCEPTION_TARGET_IS_NULL_240F0372 = "target is null";
  public static final String EXCEPTION_INDEXNAME_IS_NULL_2525299C = "indexName is null";
  public static final String EXCEPTION_COLUMNLIST_IS_NULL_DADE6825 = "columnList is null";
  public static final String EXCEPTION_SIZE_OF_COLUMNLIST_SHOULD_BE_LARGER_THAN_1_7EB80E55 = "size of columnList should be larger than 1";
  public static final String EXCEPTION_ASSIGNMENTS_IS_NULL_1FD6142D = "assignments is null";
  public static final String EXCEPTION_PIPE_NAME_CAN_NOT_BE_NULL_14570979 = "pipe name can not be null";
  public static final String EXCEPTION_SQL_IS_NULL_BEDB2B7A = "sql is null";
  public static final String EXCEPTION_SERVICENAME_IS_NULL_1009BA39 = "serviceName is null";
  public static final String EXCEPTION_CANNOT_GET_NON_MINUS_DEFAULT_VALUE_OF_PROPERTY_ARG_SINCE_ITS_VALUE_IS_SET_TO_DEF_E7D3185F = "Cannot get non-default value of property %s since its value is set to DEFAULT";
  public static final String EXCEPTION_STATEMENTNAME_IS_NULL_C03BB8D4 = "statementName is null";
  public static final String EXCEPTION_CATALOGNAME_IS_NULL_2E3C3C6B = "catalogName is null";
  public static final String EXCEPTION_SOURCE_IS_NULL_45946547 = "source is null";
  public static final String EXCEPTION_SUBSCRIPTION_ID_CAN_NOT_BE_NULL_0CDFFD7D = "subscription id can not be null";
  public static final String EXCEPTION_DB_IS_NULL_E1AD1B58 = "db is null";
  public static final String EXCEPTION_TOPIC_ATTRIBUTES_CAN_NOT_BE_NULL_791A8FED = "topic attributes can not be null";
  public static final String EXCEPTION_EXTRACTOR_SLASH_SOURCE_ATTRIBUTES_CAN_NOT_BE_NULL_2B3A656B = "extractor/source attributes can not be null";
  public static final String EXCEPTION_PROCESSOR_ATTRIBUTES_CAN_NOT_BE_NULL_FFF91008 = "processor attributes can not be null";
  public static final String EXCEPTION_CONNECTOR_ATTRIBUTES_CAN_NOT_BE_NULL_7AF2F613 = "connector attributes can not be null";
  public static final String EXCEPTION_CLASSNAME_IS_NULL_3902B37C = "className is null";
  public static final String EXCEPTION_PLUGIN_NAME_CAN_NOT_BE_NULL_92F0F4D6 = "plugin name can not be null";
  public static final String EXCEPTION_CLASS_NAME_CAN_NOT_BE_NULL_1D276677 = "class name can not be null";
  public static final String EXCEPTION_URI_CAN_NOT_BE_NULL_B3535EDC = "uri can not be null";
  public static final String EXCEPTION_STATEMENT_IS_NULL_693A0622 = "statement is null";
  public static final String EXCEPTION_UDFNAME_IS_NULL_83E9039B = "udfName is null";
  public static final String EXCEPTION_URISTRING_IS_NULL_E7458C6A = "uriString is null";
  public static final String EXCEPTION_FILEPATH_IS_NULL_84CE8A66 = "filePath is null";
  public static final String EXCEPTION_DETAILS_IS_NULL_8EDEEA03 = "details is null";
  public static final String EXCEPTION_COLUMNCATEGORY_IS_NULL_0075924B = "columnCategory is null";
  public static final String EXCEPTION_ARGUMENTNAME_IS_NULL_7F8F665F = "argumentName is null";
  public static final String EXCEPTION_PASSEDARGUMENTS_IS_NULL_98D8CB1F = "passedArguments is null";
  public static final String EXCEPTION_TABLEARGUMENTANALYSES_IS_NULL_C8724E40 = "tableArgumentAnalyses is null";
  public static final String EXCEPTION_ARGUMENT_IS_NULL_0CBBD22B = "argument is null";
  public static final String EXCEPTION_TABLEARGUMENTANALYSIS_IS_NULL_CF9F0E25 = "tableArgumentAnalysis is null";
  public static final String EXCEPTION_RULE_IS_NULL_5387C8CC = "rule is null";
  public static final String EXCEPTION_CANNOT_MERGE_STATS_FOR_DIFFERENT_RULES_COLON_ARG_AND_ARG_F0A5D5E6 = "Cannot merge stats for different rules: %s and %s";
  public static final String EXCEPTION_SCALAR_SUBQUERY_RESULT_SHOULD_ONLY_HAVE_ONE_COLUMN_DOT_893F76CB = "Scalar Subquery result should only have one column.";
  public static final String EXCEPTION_SCALAR_SUBQUERY_RESULT_SHOULD_ONLY_HAVE_ONE_ROW_DOT_F9007BBC = "Scalar Subquery result should only have one row.";
  public static final String EXCEPTION_SCALAR_SUBQUERY_RESULT_SHOULD_NOT_GET_NULL_DATATYPE_OR_NULL_COLUMN_DOT_616056F4 = "Scalar Subquery result should not get null dataType or null column.";
  public static final String EXCEPTION_OPERATOR_IS_NULL_F5BB9F59 = "operator is null";
  public static final String EXCEPTION_EXPRESSIONS_IS_NULL_C44D9384 = "expressions is null";
  public static final String EXCEPTION_CORRELATION_IS_NULL_F8327EAD = "correlation is null";
  public static final String EXCEPTION_CORRELATEDPREDICATES_IS_NULL_5FCB8011 = "correlatedPredicates is null";
  public static final String EXCEPTION_GROUPING_KEYS_WERE_CORRELATED_EE1C8406 = "grouping keys were correlated";
  public static final String EXCEPTION_EXPECTED_CONSTANT_SYMBOLS_TO_CONTAIN_ALL_CORRELATED_SYMBOLS_LOCAL_EQUIVALENTS_E20CB055 = "Expected constant symbols to contain all correlated symbols local equivalents";
  public static final String EXCEPTION_EXPECTED_SYMBOLS_TO_PROPAGATE_TO_CONTAIN_ALL_CONSTANT_SYMBOLS_C9D876E4 = "Expected symbols to propagate to contain all constant symbols";
  public static final String EXCEPTION_CARDINALITYRANGE_IS_NULL_8FDEE0B4 = "cardinalityRange is null";
  public static final String EXCEPTION_METADATAEXPRESSIONS_IS_NULL_3752914C = "metadataExpressions is null";
  public static final String EXCEPTION_EXPRESSIONSCANPUSHDOWN_IS_NULL_DC8DFEB3 = "expressionsCanPushDown is null";
  public static final String EXCEPTION_EXPRESSIONSCANNOTPUSHDOWN_IS_NULL_63BC9AF9 = "expressionsCannotPushDown is null";
  public static final String EXCEPTION_FILTER_PREDICATE_OF_FILTERNODE_IS_NULL_C3964179 = "Filter predicate of FilterNode is null";
  public static final String EXCEPTION_UNSUPPORTED_JOIN_TYPE_COLON_ARG_9FB6751B = "Unsupported join type: %s";
  public static final String EXCEPTION_UNIQUEID_IN_PREDICATE_IS_NOT_YET_SUPPORTED_7B5D2EAF = "UniqueId in predicate is not yet supported";
  public static final String EXCEPTION_MAPPERPROVIDER_IS_NULL_472725D5 = "mapperProvider is null";
  public static final String EXCEPTION_CORRELATIONMAPPING_IS_NULL_9D595C82 = "correlationMapping is null";
  public static final String EXCEPTION_MAPPINGS_IS_NULL_23BD9025 = "mappings is null";
  public static final String EXCEPTION_AGGREGATE_WITH_ORDER_BY_DOES_NOT_SUPPORT_PARTIAL_AGGREGATION_D5BDD21F = "Aggregate with ORDER BY does not support partial aggregation";
  public static final String EXCEPTION_LEFTEFFECTIVEPREDICATE_MUST_ONLY_CONTAIN_SYMBOLS_FROM_LEFTSYMBOLS_DB3259B8 = "leftEffectivePredicate must only contain symbols from leftSymbols";
  public static final String EXCEPTION_RIGHTEFFECTIVEPREDICATE_MUST_ONLY_CONTAIN_SYMBOLS_FROM_RIGHTSYMBOLS_4B97238D = "rightEffectivePredicate must only contain symbols from rightSymbols";
  public static final String EXCEPTION_OUTEREFFECTIVEPREDICATE_MUST_ONLY_CONTAIN_SYMBOLS_FROM_OUTERSYMBOLS_99FC2AA9 = "outerEffectivePredicate must only contain symbols from outerSymbols";
  public static final String EXCEPTION_INNEREFFECTIVEPREDICATE_MUST_ONLY_CONTAIN_SYMBOLS_FROM_INNERSYMBOLS_ECB7C6A2 = "innerEffectivePredicate must only contain symbols from innerSymbols";
  public static final String EXCEPTION_IDALLOCATOR_IS_NULL_752B308D = "idAllocator is null";
  public static final String EXCEPTION_PLANOPTIMIZERSSTATSCOLLECTOR_IS_NULL_9DA4B0CC = "planOptimizersStatsCollector is null";
  public static final String EXCEPTION_SUBQUERY_RESULT_TYPE_MUST_BE_ORDERABLE_82AF0EFA = "Subquery result type must be orderable";
  public static final String EXCEPTION_ALL_THE_NON_CORRELATED_SUBQUERIES_SHOULD_BE_REWRITTEN_AT_THIS_POINT_B4614541 = "All the non correlated subqueries should be rewritten at this point";
  public static final String EXCEPTION_CHANGEDPLANNODES_IS_NULL_5ECBDE28 = "changedPlanNodes is null";
  public static final String EXCEPTION_MAPPINGFUNCTION_IS_NULL_212D6109 = "mappingFunction is null";
  public static final String EXCEPTION_ROOT_NODE_MUST_RETURN_ONLY_ONE_FF42061C = "Root node must return only one";
  public static final String EXCEPTION_SIZE_OF_TOPKNODE_CAN_ONLY_BE_1_IN_LOGICAL_PLAN_DOT_DB32E3C5 = "Size of TopKNode can only be 1 in logical plan.";
  public static final String EXCEPTION_THE_SIZE_OF_LEFT_CHILDREN_NODE_OF_JOINNODE_SHOULD_BE_1_F3437368 = "The size of left children node of JoinNode should be 1";
  public static final String EXCEPTION_THE_SIZE_OF_RIGHT_CHILDREN_NODE_OF_JOINNODE_SHOULD_BE_1_6BA167CF = "The size of right children node of JoinNode should be 1";
  public static final String EXCEPTION_THE_SIZE_OF_LEFT_CHILDREN_NODE_OF_SEMIJOINNODE_SHOULD_BE_1_FFEE3F41 = "The size of left children node of SemiJoinNode should be 1";
  public static final String EXCEPTION_THE_SIZE_OF_RIGHT_CHILDREN_NODE_OF_SEMIJOINNODE_SHOULD_BE_1_AE90C4B8 = "The size of right children node of SemiJoinNode should be 1";
  public static final String EXCEPTION_CHILDRENNODES_SHOULD_NOT_BE_NULL_DOT_0C93B063 = "childrenNodes should not be null.";
  public static final String EXCEPTION_CHILDRENNODES_SHOULD_NOT_BE_EMPTY_DOT_E5555FD9 = "childrenNodes should not be empty.";
  public static final String EXCEPTION_SIZE_OF_TOPKRANKINGNODE_CAN_ONLY_BE_1_IN_LOGICAL_PLAN_DOT_20D6A513 = "Size of TopKRankingNode can only be 1 in logical plan.";
  public static final String EXCEPTION_STATS_IS_NULL_D3627E6A = "stats is null";
  public static final String EXCEPTION_USELEGACYRULES_IS_NULL_0AD13CAB = "useLegacyRules is null";
  public static final String EXCEPTION_RULES_IS_NULL_DF243716 = "rules is null";
  public static final String EXCEPTION_EXPECTED_CHILD_TO_BE_A_GROUP_REFERENCE_DOT_FOUND_COLON_EC01971C = "Expected child to be a group reference. Found: ";
  public static final String EXCEPTION_TIMEOUT_HAS_TO_BE_A_NON_MINUS_NEGATIVE_NUMBER_LEFT_BRACKET_MILLISECONDS_RIGHT_BR_5201D8B3 = "Timeout has to be a non-negative number [milliseconds]";
  public static final String EXCEPTION_NODE_QUOTE_ARG_QUOTE_IS_NOT_A_GROUPREFERENCE_73C8C127 = "Node '%s' is not a GroupReference";
  public static final String EXCEPTION_MEMBER_IS_NULL_466D8670 = "member is null";
  public static final String EXCEPTION_INVALID_GROUP_COLON_ARG_C0BAD253 = "Invalid group: %s";
  public static final String EXCEPTION_ARG_COLON_TRANSFORMED_EXPRESSION_DOESN_QUOTE_T_PRODUCE_SAME_OUTPUTS_COLON_ARG_VS_F9BAF138 = "%s: transformed expression doesn't produce same outputs: %s vs %s";
  public static final String EXCEPTION_CANNOT_DELETE_GROUP_THAT_HAS_INCOMING_REFERENCES_83C9D700 = "Cannot delete group that has incoming references";
  public static final String EXCEPTION_REFERENCE_TO_REMOVE_NOT_FOUND_2EB93289 = "Reference to remove not found";
  public static final String EXCEPTION_TRANSFORMEDPLAN_IS_NULL_83B2099A = "transformedPlan is null";
  public static final String EXCEPTION_NEWCHILDREN_IS_NOT_EMPTY_170FCE18 = "newChildren is not empty";
  public static final String EXCEPTION_GROUPINGSETS_IS_NULL_8EE6D9BF = "groupingSets is null";
  public static final String EXCEPTION_PREGROUPEDSYMBOLS_IS_NULL_DC24FF7B = "preGroupedSymbols is null";
  public static final String EXCEPTION_GROUPING_COLUMNS_DOES_NOT_CONTAIN_GROUPID_COLUMN_83976C83 = "Grouping columns does not contain groupId column";
  public static final String EXCEPTION_ORDER_BY_DOES_NOT_SUPPORT_DISTRIBUTED_AGGREGATION_05109B26 = "ORDER BY does not support distributed aggregation";
  public static final String EXCEPTION_DATE_BIN_FUNCTION_MUST_BE_THE_LAST_GROUPINGKEY_EE955FF5 = "date_bin function must be the last GroupingKey";
  public static final String EXCEPTION_PRE_MINUS_GROUPED_SYMBOLS_MUST_BE_A_SUBSET_OF_THE_GROUPING_KEYS_AFC6C33D = "Pre-grouped symbols must be a subset of the grouping keys";
  public static final String EXCEPTION_EXPECTED_AGGREGATION_TO_HAVE_DISTINCT_INPUT_EC6AF059 = "Expected aggregation to have DISTINCT input";
  public static final String EXCEPTION_MISMATCHED_CHILD_LEFT_PAREN_ARG_RIGHT_PAREN_AND_PERMITTED_OUTPUTS_LEFT_PAREN_ARG_57801144 = "Mismatched child (%s) and permitted outputs (%s) sizes";
  public static final String EXCEPTION_MISSING_TYPE_FOR_EXPRESSION_3D66D302 = "missing type for expression";
  public static final String EXCEPTION_EXCEPTNODE_TRANSLATION_RESULT_HAS_NO_COUNT_SYMBOLS_8653930E = "ExceptNode translation result has no count symbols";
  public static final String EXCEPTION_UNEXPECTED_CORRELATED_JOIN_TYPE_COLON_ARG_27E8EC42 = "unexpected correlated join type: %s";
  public static final String EXCEPTION_CORRELATION_IN_ARG_JOIN_2F78ACC3 = "correlation in %s JOIN";
  public static final String EXCEPTION_PLANNODE_IS_NULL_49FBBFCF = "planNode is null";
  public static final String EXCEPTION_COUNTSYMBOLS_IS_NULL_416D96FB = "countSymbols is null";
  public static final String EXCEPTION_ROWNUMBERSYMBOL_IS_NULL_BA30E0AA = "rowNumberSymbol is null";
  public static final String EXCEPTION_CANNOT_SIMPLIFY_A_UNIONNODE_9D5B09A7 = "Cannot simplify a UnionNode";
  public static final String EXCEPTION_THE_SIZE_OF_MARKERS_SHOULD_BE_SAME_AS_THE_SIZE_OF_COUNT_OUTPUT_SYMBOLS_6DBDD287 = "the size of markers should be same as the size of count output symbols";
  public static final String EXCEPTION_ROWNUMBERSYMBOL_IS_EMPTY_34FE9565 = "rowNumberSymbol is empty";
  public static final String EXCEPTION_EXPECTED_SUBQUERY_OUTPUT_SYMBOLS_TO_BE_PRUNED_13B84182 = "Expected subquery output symbols to be pruned";
  public static final String EXCEPTION_TYPEANALYZER_IS_NULL_3106B188 = "typeAnalyzer is null";
  public static final String EXCEPTION_UNEXPECTED_CORRELATEDJOIN_TYPE_COLON_47A368C1 = "unexpected CorrelatedJoin type: ";
  public static final String EXCEPTION_UNEXPECTED_NULL_LITERAL_WITHOUT_A_CAST_TO_BOOLEAN_D399CFCB = "Unexpected null literal without a cast to boolean";
  public static final String EXCEPTION_REWRITER_IS_NULL_B0D8CC88 = "rewriter is null";
  public static final String EXCEPTION_UNEXPECTED_NODE_TYPE_COLON_ARG_B1C0328F = "unexpected node type: %s";
  public static final String EXCEPTION_RESULT_IS_NULL_031E2F89 = "result is null";
  public static final String EXCEPTION_DISTINCT_NOT_SUPPORTED_0E97D0BB = "distinct not supported";
  public static final String EXCEPTION_EXPRESSION_IS_NOT_ANALYZED_LEFT_PAREN_ARG_RIGHT_PAREN_COLON_ARG_DAE760B6 = "Expression is not analyzed (%s): %s";
  public static final String EXCEPTION_SYMBOL_REFERENCES_ARE_NOT_ALLOWED_93779D6C = "symbol references are not allowed";
  public static final String EXCEPTION_EXPRESSION_INTERPRETER_RETURNED_AN_UNRESOLVED_EXPRESSION_5BCE9A51 = "Expression interpreter returned an unresolved expression";
  public static final String EXCEPTION_NULL_OPERAND_SHOULD_HAVE_BEEN_REMOVED_BY_RECURSIVE_COALESCE_PROCESSING_B6D4D443 = "Null operand should have been removed by recursive coalesce processing";
  public static final String EXCEPTION_NULL_VALUE_IS_EXPECTED_TO_BE_REPRESENTED_AS_NULL_COMMA_NOT_NULLLITERAL_9B96D25A = "Null value is expected to be represented as null, not NullLiteral";
  public static final String EXCEPTION_AGGREGATION_FUNCTIONS_ARG_SHOULD_ONLY_HAVE_TWO_ARGUMENTS_3D12DCFD = "Aggregation functions [%s] should only have two arguments";
  public static final String EXCEPTION_AGGREGATION_FUNCTIONS_ARG_SHOULD_HAVE_VALUE_COLUMN_AS_NUMERIC_TYPE_INT32_INT64_FLOAT_DOUBLE_TIMESTAMP_97A6CA87 = "Aggregation functions [%s] should have value column as numeric type [INT32, INT64, FLOAT, DOUBLE, TIMESTAMP]";
  public static final String EXCEPTION_AGGREGATION_FUNCTIONS_ARG_SHOULD_HAVE_PERCENTAGE_AS_DECIMAL_TYPE_57033ADF = "Aggregation functions [%s] should have percentage as decimal type";
  public static final String EXCEPTION_CANNOT_NEST_AGGREGATIONS_INSIDE_AGGREGATION_ARG_ARG_6E5073A4 = "Cannot nest aggregations inside aggregation '%s': %s";
  public static final String EXCEPTION_CANNOT_NEST_WINDOW_FUNCTIONS_INSIDE_AGGREGATION_ARG_ARG_8F94A897 = "Cannot nest window functions inside aggregation '%s': %s";
  public static final String EXCEPTION_PARTITION_BY_EXPRESSION_ARG_MUST_BE_AN_AGGREGATE_EXPRESSION_OR_APPEAR_IN_GROUP_BY_CLAUSE_E3C696D6 = "PARTITION BY expression '%s' must be an aggregate expression or appear in GROUP BY clause";
  public static final String EXCEPTION_ORDER_BY_EXPRESSION_ARG_MUST_BE_AN_AGGREGATE_EXPRESSION_OR_APPEAR_IN_GROUP_BY_CLAUSE_7FF8267B = "ORDER BY expression '%s' must be an aggregate expression or appear in GROUP BY clause";
  public static final String EXCEPTION_WINDOW_FRAME_START_MUST_BE_AN_AGGREGATE_EXPRESSION_OR_APPEAR_IN_GROUP_BY_CLAUSE_74E30A63 = "Window frame start must be an aggregate expression or appear in GROUP BY clause";
  public static final String EXCEPTION_WINDOW_FRAME_END_MUST_BE_AN_AGGREGATE_EXPRESSION_OR_APPEAR_IN_GROUP_BY_CLAUSE_31784B55 = "Window frame end must be an aggregate expression or appear in GROUP BY clause";
  public static final String EXCEPTION_INVALID_EXPLAIN_FORMAT_3FD91910 = "Invalid EXPLAIN format: ";
  public static final String EXCEPTION_INVALID_EXPLAIN_ANALYZE_FORMAT_0879A025 = "Invalid EXPLAIN ANALYZE format: ";
  public static final String EXCEPTION_INVALID_EXPLAIN_FORMAT_ARG_SUPPORTED_FORMATS_GRAPHVIZ_JSON_441A1D48 =
      "Invalid EXPLAIN format: %s. Supported formats: GRAPHVIZ, JSON";
  public static final String EXCEPTION_INVALID_EXPLAIN_ANALYZE_FORMAT_ARG_SUPPORTED_FORMATS_TEXT_JSON_77FA30EB =
      "Invalid EXPLAIN ANALYZE format: %s. Supported formats: TEXT, JSON";
  public static final String EXCEPTION_GROUP_BY_ALL_CANNOT_BE_COMBINED_WITH_EXPLICIT_GROUPING_ELEMENTS_4CFE411D = "GROUP BY ALL cannot be combined with explicit grouping elements";
  public static final String EXCEPTION_THE_SECOND_ARGUMENT_OF_PERCENTILE_FUNCTION_PERCENTAGE_MUST_BE_A_DOUBLE_LITERAL_D9464B46 = "The second argument of 'percentile' function percentage must be a double literal";
  public static final String EXCEPTION_DATA_TYPE_MISMATCH_FOR_MEASUREMENT_ARGARGARG_TYPE_IN_TSFILE_ARG_TYPE_IN_IOTDB_ARG_C5BA7DBD = "Data type mismatch for measurement %s%s%s, type in TsFile: %s, type in IoTDB: %s";
  public static final String MESSAGE_FAILED_TO_RELEASE_EXTERNAL_TSFILE_QUERY_RESOURCE_712EE978 = "Failed to release external TsFile query resource";
  public static final String EXCEPTION_OUTER_QUERY_TIMEOUT_EXCEEDED_BEFORE_IOTDBLOCAL_QUERY_STARTS_800BFA63 = "Outer query timeout exceeded before IoTDBLocal query starts";
  public static final String MESSAGE_FAILED_TO_CLOSE_UDF_RESULT_SET_AT_INDEX_ARG_A293B7EC = "Failed to close UDF result set at index {}";
  public static final String EXCEPTION_INTERNAL_QUERY_EXECUTION_NOT_FOUND_62642542 = "Internal query execution not found";
  public static final String EXCEPTION_ONLY_QUERY_IS_SUPPORTED_FOR_IOTDBLOCAL_QUERY_INTERFACE_09195570 = "Only query is supported for IoTDBLocal query interface";
  public static final String EXCEPTION_FAILED_TO_CLOSE_INTERNAL_QUERY_RESULT_ED86BF5E = "Failed to close internal query result";
  public static final String EXCEPTION_UDFRESULTSET_IS_ALREADY_CLOSED_E1ACABE3 = "UDFResultSet is already closed";

  public static final String EXCEPTION_LATERALCOLUMNALIASREFERENCES_IS_NULL_0CC58C78 = "lateralColumnAliasReferences is null";
  public static final String EXCEPTION_OUTPUTEXPRESSIONS_IS_NULL_7BD39A8F = "outputExpressions is null";
  public static final String EXCEPTION_ALIASES_IS_NULL_6D98FA6C = "aliases is null";
  public static final String EXCEPTION_SINGLECOLUMNEXPRESSIONS_IS_NULL_EFB296FD = "singleColumnExpressions is null";
  public static final String EXCEPTION_CANONICALNAME_IS_NULL_DDFE6DB1 = "canonicalName is null";
  public static final String EXCEPTION_REWRITTENEXPRESSION_IS_NULL_6EE23088 = "rewrittenExpression is null";
  public static final String EXCEPTION_ALIASESBYCANONICALNAME_IS_NULL_635C7ED3 = "aliasesByCanonicalName is null";
  public static final String EXCEPTION_ALIAS_IS_NULL_862D23B1 = "alias is null";
  public static final String EXCEPTION_REFERENCES_IS_NULL_25A7E3AF = "references is null";
  public static final String EXCEPTION_VISIBLEALIASES_IS_NULL_630B27F1 = "visibleAliases is null";

}
