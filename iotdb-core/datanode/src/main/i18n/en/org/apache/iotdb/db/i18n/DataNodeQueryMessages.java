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

  private DataNodeQueryMessages() {}
}
