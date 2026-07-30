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

package org.apache.iotdb.commons.i18n;

public final class CommonMessages {

  // --- startup / shutdown ---
  public static final String STARTUP_FAILED = "Failed to start [%s], because [%s]";

  // --- path ---
  public static final String ILLEGAL_PATH = "%s is not a legal path";
  public static final String ILLEGAL_PATH_WITH_REASON = "%s is not a legal path, because %s";
  public static final String PATH_NOT_MEASUREMENT = "This path doesn't represent a measurement";
  public static final String PATH_DUPLICATED = "Path duplicated: %s";
  public static final String OBJECT_TYPE_COLUMN_NOT_SUPPORTED =
      "The object type column is not supported.";

  // --- cluster ---
  public static final String NODE_TYPE_NOT_EXIST = "NodeType %s doesn't exist.";
  public static final String NODE_STATUS_NOT_EXIST = "NodeStatus %s doesn't exist.";
  public static final String UNKNOWN_NODE_STATUS = "Unknown NodeStatus %s.";

  // --- consensus ---
  public static final String UNRECOGNIZED_CONSENSUS_GROUP_ID =
      "Unrecognized ConsensusGroupId: %s";
  public static final String IOTV2_BG_NOT_TERMINATED =
      "IoTV2 background service did not terminate within {}s";
  public static final String IOTV2_BG_STILL_RUNNING =
      "IoTV2 background Thread still doesn't exit after 30s";

  // --- cq ---
  public static final String UNKNOWN_TIMEOUT_POLICY = "Unknown TimeoutPolicy: %s";
  public static final String UNKNOWN_CQ_STATE = "Unknown CQState: %s";

  // --- memory ---
  public static final String MEMORY_ALLOC_INTERRUPTED =
      "exactAllocate: interrupted while waiting for available memory";
  public static final String MEMORY_RELEASE_FAILED =
      "releaseWithOutNotify: failed to close memory block {}";
  public static final String MEMORY_RELEASE_FAILED_NO_DETAIL =
      "releaseWithOutNotify: failed to close memory block";
  public static final String MEMORY_SIZE_SHOULD_BE_POSITIVE =
      "getOrCreateMemoryManager {}: sizeInBytes should be positive";

  // --- file ---
  public static final String SHOULD_NEVER_TOUCH_HERE = "Should never touch here";

  // --- externalservice ---
  public static final String UNKNOWN_SERVICE_TYPE = "Unknown ServiceType: %s";
  public static final String UNKNOWN_STATE = "Unknown State: %s";

  // --- subscription ---
  public static final String CONFIG_PRINT = "{}: {}";

  // --- log ---
  public static final String LOG_LOGGERPERIODICALLOGREDUCER_IS_ALLOCATED_TO_ARG_BYTES_C8373CF5 =
      "LoggerPeriodicalLogReducer is allocated to {} bytes.";

  // --- partition ---
  public static final String DATABASE_NOT_EXISTS_AND_AUTO_CREATE_DISABLED =
      "Database %s not exists and failed to create automatically because enable_auto_create_schema is FALSE.";
  public static final String DATA_PARTITION_EMPTY =
      "Data partition is empty. device: %s, seriesSlot: %s, database: %s";
  public static final String TARGET_REGION_LIST_EMPTY =
      "targetRegionList is empty. device: %s, timeSlot: %s";

  // --- concurrent ---
  public static final String EXCEPTION_IN_THREAD = "Exception in thread {}-{}";
  public static final String INTERRUPTED_WHILE_AWAITING = "Interrupted while awaiting condition";
  public static final String EXCEPTION_WHILE_EVALUATING = "Exception while evaluating condition";
  public static final String UNKNOWN_THREAD_NAME = "Unknown thread name: {}";
  public static final String TASK_CANCELLED_IN_POOL = "task is cancelled in thread pool {}";
  public static final String EXCEPTION_IN_THREAD_POOL = "Exception in thread pool {}";
  public static final String SCHEDULE_TASK_FAILED = "Schedule task failed";
  public static final String RUN_THREAD_FAILED = "Run thread failed";

  // --- enums ---
  public static final String SYSTEM_READ_ONLY = "System mode is set to READ_ONLY";
  public static final String UNRECOVERABLE_ERROR =
      "Unrecoverable error occurs! Shutdown system directly.";

  // --- udf ---
  public static final String UNKNOWN_FUNCTION_TYPE = "Unknown FunctionType: %s";
  public static final String INVALID_INPUT = "Invalid input: %s";
  public static final String UDF_LIB_ROOT = "UDF lib root: {}";
  public static final String UDF_MD5_READ_ERROR = "Error occurred when trying to read md5 of {}";
  public static final String VALUE_NOT_NUMERIC =
      "The value of the input time series is not numeric.\n";
  public static final String FAIL_GET_DATA_TYPE = "Fail to get data type in row %s";
  public static final String UDTF_ABS_SET_TRANSFORMER = "UDTFAbs#setTransformer()";
  public static final String BASE_VALUE_SHOULD_NOT_BE_NULL =
      "When comparing, base value should never be null";
  public static final String SIZE_MUST_BE_POSITIVE = "Size must be greater than 0";

  // --- sync ---
  public static final String UNEXPECTED_SERIALIZATION_ERROR =
      "Unexpected error occurred when serializing PipeInfo.";

  // --- security ---
  public static final String ENCRYPT_PASSWORD_ERROR = "meet error while encrypting password.";
  public static final String CLASSLOADER_NOT_DETERMINED =
      "A ClassLoader to load the class could not be determined.";

  // --- binaryallocator ---
  public static final String BINARY_ALLOCATOR_RUNNING_GC_EVICTION =
      "Binary allocator running GC eviction";
  public static final String BINARY_ALLOCATOR_SHUTTING_DOWN_HIGH_GC =
      "Binary allocator is shutting down because of high GC time percentage {}%.";
  public static final String AUTO_RELEASER_EXIT_INTERRUPTED =
      "{} exits due to interruptedException.";
  public static final String STOPPING_COMPONENT = "Stopping {}";
  public static final String UNABLE_TO_STOP_AUTO_RELEASER =
      "unable to stop auto releaser after {} ms";
  public static final String UNABLE_TO_STOP_EVICTOR = "unable to stop evictor after {} ms";

  private CommonMessages() {}

  public static final String COLLECTION_MUST_NOT_BE_NULL = "Collection must not be null.";
  public static final String MAP_MUST_NOT_BE_NULL = "Map must not be null.";
  public static final String MAP_ENTRY_MUST_NOT_BE_NULL = "Map Entry must not be null.";
  public static final String ITERATOR_MUST_NOT_BE_NULL = "Iterator must not be null";
  public static final String ITERATOR_REMOVE_ONLY_AFTER_NEXT = "Iterator remove() can only be called once after next()";
  public static final String FAIL_TO_GET_DATA_TYPE_IN_ROW = "Fail to get data type in row ";
  // ---------------------------------------------------------------------------
  // Additional auto-collected messages
  // ---------------------------------------------------------------------------
  public static final String LOG_STEP_METRICS_ARG_ARG_TOTAL_ARG_SUM_2FMS_AVG_ARG_87491AB0 = "step metrics [%d]-[%s] - Total: %d, SUM: %.2fms, AVG: %fms, Last%dAVG: %fms";
  public static final String LOG_ERROR_OCCURRED_DURING_TRANSFERRING_FILE_ARG_BYTEBUFFER_CAUSE_ARG_FEDC38A3 = "Error occurred during transferring file{} to ByteBuffer, the cause is {}";
  public static final String LOG_ERROR_OCCURRED_DURING_WRITING_BYTEBUFFER_ARG_CAUSE_ARG_F3AD2DA0 = "Error occurred during writing bytebuffer to {} , the cause is {}";
  public static final String EXCEPTION_SIZE_FILE_EXCEED_ARG_BYTES_C60F1149 = "Size of file exceed %d bytes";
  public static final String EXCEPTION_UNRECOGNIZED_TCONSENSUSGROUPTYPE_9204FF8E = "Unrecognized TConsensusGroupType: ";
  public static final String EXCEPTION_ID_1F238F51 = " with id = ";
  public static final String LOG_MEMORY_COST_RELEASED_LARGER_THAN_MEMORY_COST_MEMORY_BLOCK_ARG_00DD9DA9 = "The memory cost to be released is larger than the memory cost of memory block {}";
  public static final String LOG_EXACTALLOCATEIFSUFFICIENT_FAILED_ALLOCATE_MEMORY_A47897D9 = "exactAllocateIfSufficient: failed to allocate memory, ";
  public static final String LOG_TOTAL_MEMORY_SIZE_ARG_BYTES_USED_MEMORY_SIZE_ARG_BYTES_5FB5059F = "total memory size {} bytes, used memory size {} bytes, ";
  public static final String LOG_REQUESTED_MEMORY_SIZE_ARG_BYTES_USED_THRESHOLD_ARG_D7061DEB = "requested memory size {} bytes, used threshold {}";
  public static final String LOG_TRYALLOCATE_ALLOCATED_MEMORY_B3D564D9 = "tryAllocate: allocated memory, ";
  public static final String LOG_ORIGINAL_REQUESTED_MEMORY_SIZE_ARG_BYTES_03D28A6B = "original requested memory size {} bytes, ";
  public static final String LOG_ACTUAL_REQUESTED_MEMORY_SIZE_ARG_BYTES_62760058 = "actual requested memory size {} bytes";
  public static final String LOG_TRYALLOCATE_FAILED_ALLOCATE_MEMORY_838FA6FB = "tryAllocate: failed to allocate memory, ";
  public static final String LOG_REQUESTED_MEMORY_SIZE_ARG_BYTES_BF9CEF81 = "requested memory size {} bytes";
  public static final String LOG_GETORREGISTERMEMORYBLOCK_FAILED_MEMORY_BLOCK_ARG_ALREADY_EXISTS_42CA8914 = "getOrRegisterMemoryBlock failed: memory block {} already exists, ";
  public static final String LOG_IT_S_SIZE_ARG_REQUESTED_SIZE_ARG_AF8F04B2 = "it's size is {}, requested size is {}";
  public static final String LOG_GETMEMORYMANAGER_MEMORY_MANAGER_ARG_ALREADY_EXISTS_IT_S_SIZE_ARG_0102560A = "getMemoryManager: memory manager {} already exists, it's size is {}, enabled is {}";
  public static final String LOG_GETORCREATEMEMORYMANAGER_FAILED_TOTAL_MEMORY_SIZE_ARG_BYTES_LESS_THAN_ALLOCATED_3D110256 =
      "getOrCreateMemoryManager failed: total memory size {} bytes is less than allocated memory"
      + " size {} bytes";
  public static final String EXCEPTION_EXACTALLOCATE_FAILED_ALLOCATE_MEMORY_AFTER_ARG_RETRIES_957A647B = "exactAllocate: failed to allocate memory after %d retries, ";
  public static final String EXCEPTION_TOTAL_MEMORY_SIZE_ARG_BYTES_USED_MEMORY_SIZE_ARG_BYTES_9FC9A9C6 = "total memory size %d bytes, used memory size %d bytes, ";
  public static final String EXCEPTION_REQUESTED_MEMORY_SIZE_ARG_BYTES_E6340842 = "requested memory size %d bytes";
  public static final String EXCEPTION_REGISTER_MEMORY_BLOCK_ARG_FAILED_SIZEINBYTES_SHOULD_NON_NEGATIVE_EC54AA75 = "register memory block %s failed: sizeInBytes should be non-negative";
  public static final String LOG_DELETE_SYSTEM_PROPERTIES_TMP_FILE_FAIL_YOU_MAY_MANUALLY_DELETE_F81C4A53 = "Delete system.properties tmp file fail, you may manually delete it: {}";
  public static final String LOG_FAILED_DELETE_SYSTEM_PROPERTIES_FILE_YOU_SHOULD_MANUALLY_DELETE_THEM_77F91A98 = "Failed to delete system.properties file, you should manually delete them: {}, {}";
  public static final String EXCEPTION_LENGTH_PARAMETERS_SHOULD_EVENLY_DIVIDED_2_BUT_ACTUAL_LENGTH_E9A792D9 = "Length of parameters should be evenly divided by 2, but the actual length is ";
  public static final String EXCEPTION_TMP_SYSTEM_PROPERTIES_FILE_MUST_EXIST_CALL_REPLACEFORMALFILE_FA63B976 = "Tmp system properties file must exist when call replaceFormalFile";
  public static final String LOG_UNRECOVERABLE_ERROR_OCCURS_CHANGE_SYSTEM_STATUS_READ_ONLY_BECAUSE_HANDLE_05C9AD1A =
      "Unrecoverable error occurs! Change system status to read-only because handle_system_error is"
      + " CHANGE_TO_READ_ONLY. Only query statements are permitted!";
  public static final String LOG_UNRECOVERABLE_ERROR_OCCURS_SHUTDOWN_SYSTEM_DIRECTLY_BECAUSE_HANDLE_SYSTEM_ERROR_14FC06C9 = "Unrecoverable error occurs! Shutdown system directly because handle_system_error is SHUTDOWN.";
  public static final String EXCEPTION_TYPE_ARG_NOT_SUPPORTED_PIPE_RATE_AVERAGE_F74694AD = "The type %s is not supported in pipe rate average.";
  public static final String EXCEPTION_UNKNOWN_UDFTYPE_9A8D1B23 = "Unknown UDFType:";
  public static final String EXCEPTION_8S_5F5F831F = "%8s";
  public static final String EXCEPTION_CAN_NOT_RECOGNIZE_PIPETYPE_ARG_8850A249 = "Can not recognize PipeType %s.";
  public static final String EXCEPTION_TARGETREGIONLIST_EMPTY_DEVICE_ARG_TIMESLOT_ARG_E7E5818C = "targetRegionList is empty. device: %s, timeSlot: %s";
  public static final String EXCEPTION_DATABASE_18F8303F = "Database ";
  public static final String EXCEPTION_NOT_EXISTS_FAILED_CREATE_AUTOMATICALLY_BECAUSE_ENABLE_AUTO_CREATE_SCHEMA_80DE1A4B = " not exists and failed to create automatically because enable_auto_create_schema is FALSE.";
  public static final String EXCEPTION_PATH_DOES_NOT_EXIST_737CB95D = "Path does not exist. ";
  public static final String EXCEPTION_CAN_T_GET_NEXT_FOLDER_ARG_BECAUSE_THEY_ALL_FULL_A105BB2D = "Can't get next folder from [%s], because they are all full.";
  public static final String EXCEPTION_PARAMETER_ARG_CAN_NOT_ARG_PLEASE_SET_ARG_BECAUSE_ARG_749738D1 = "Parameter %s can not be %s, please set to: %s. Because %s";
  public static final String EXCEPTION_QUERY_EXECUTION_TIME_OUT_A5DC7BFB = "Query execution is time out";
  public static final String EXCEPTION_OBJECT_FILE_ARG_DOES_NOT_EXIST_7EA8CB1C = "Object file %s does not exist";
  public static final String EXCEPTION_ARG_NOT_LEGAL_PRIVILEGE_504838E8 = "%s is not a legal privilege";
  public static final String EXCEPTION_SOME_PORTS_OCCUPIED_77ED044D = "Some ports are occupied";
  public static final String EXCEPTION_PORTS_ARG_OCCUPIED_B462E9DA = "Ports %s are occupied";
  public static final String EXCEPTION_UNEXPECTED_ERROR_OCCURS_SERIALIZATION_A6B2E222 = "Unexpected error occurs in serialization";
  public static final String EXCEPTION_COLUMN_ARG_TABLE_ARG_ARG_DOES_NOT_EXIST_D8145581 = "Column %s in table '%s.%s' does not exist.";
  public static final String EXCEPTION_TABLE_ARG_ARG_DOES_NOT_EXIST_796E503B = "Table '%s.%s' does not exist.";
  public static final String EXCEPTION_TABLE_ARG_ARG_ALREADY_EXISTS_D4BDF4B5 = "Table '%s.%s' already exists.";
  public static final String EXCEPTION_COULDN_T_CONSTRUCTOR_SERIESPARTITIONEXECUTOR_CLASS_ARG_34FB9F45 = "Couldn't Constructor SeriesPartitionExecutor class: %s";
  public static final String EXCEPTION_CANNOT_USE_SETVALUE_OBJECT_BEING_SET_ALREADY_MAP_676ED3BF = "Cannot use setValue() when the object being set is already in the map";
  public static final String EXCEPTION_ITERATOR_GETKEY_CAN_ONLY_CALLED_AFTER_NEXT_BEFORE_REMOVE_009C456B = "Iterator getKey() can only be called after next() and before remove()";
  public static final String EXCEPTION_ITERATOR_GETVALUE_CAN_ONLY_CALLED_AFTER_NEXT_BEFORE_REMOVE_927A88A2 = "Iterator getValue() can only be called after next() and before remove()";
  public static final String EXCEPTION_ITERATOR_SETVALUE_CAN_ONLY_CALLED_AFTER_NEXT_BEFORE_REMOVE_51505AD1 = "Iterator setValue() can only be called after next() and before remove()";
  public static final String LOG_FAILED_CLOSE_UDFCLASSLOADER_QUERYID_ARG_BECAUSE_ARG_8B1C3739 = "Failed to close UDFClassLoader (queryId: {}), because {}";
  public static final String EXCEPTION_ATTRIBUTE_ARG_ARG_REQUIRED_BUT_WAS_NOT_PROVIDED_CD090883 = "attribute \"%s\"/\"%s\" is required but was not provided.";
  public static final String EXCEPTION_USE_ATTRIBUTE_ARG_ARG_ONLY_ONE_AT_TIME_B431468C = "use attribute \"%s\" or \"%s\" only one at a time.";
  public static final String EXCEPTION_ILLEGAL_OUTLIER_METHOD_OUTLIER_TYPE_SHOULD_AVG_STENDIS_COS_PRENEXTDIS_91D1C70A = "Illegal outlier method. Outlier type should be avg, stendis, cos or prenextdis.";
  public static final String EXCEPTION_ILLEGAL_AGGREGATION_METHOD_AGGREGATION_TYPE_SHOULD_AVG_MIN_MAX_SUM_2D7BEC96 = "Illegal aggregation method. Aggregation type should be avg, min, max, sum, extreme, variance.";
  public static final String EXCEPTION_CUMULATIVE_TABLE_FUNCTION_REQUIRES_SIZE_MUST_INTEGRAL_MULTIPLE_STEP_D8A9DA94 = "Cumulative table function requires size must be an integral multiple of step.";
  public static final String EXCEPTION_COLUMN_TYPE_MUST_NUMERIC_IF_DELTA_NOT_0_F7864D4E = " The column type must be numeric if DELTA is not 0.";
  public static final String EXCEPTION_TYPE_COLUMN_ARG_NOT_AS_EXPECTED_7A81636E = "The type of the column [%s] is not as expected.";
  public static final String EXCEPTION_REQUIRED_COLUMN_ARG_NOT_FOUND_SOURCE_TABLE_ARGUMENT_993E1C08 = "Required column [%s] not found in the source table argument.";
  public static final String EXCEPTION_UNSUPPORTED_PROGRESS_INDEX_TYPE_ARG_A84CDFF9 = "Unsupported progress index type %s.";
  public static final String EXCEPTION_TIMEWINDOWSTATEPROGRESSINDEX_DOES_NOT_SUPPORT_TOPOLOGICAL_SORTING_897C8976 = "TimeWindowStateProgressIndex does not support topological sorting";
  public static final String EXCEPTION_INTENDED_READ_LENGTH_ARG_BUT_ARG_ACTUALLY_READ_DESERIALIZING_TIMEPROGRESSINDEX_63CD54E4 =
      "The intended read length is %s but %s is actually read when deserializing TimeProgressIndex,"
      + " ProgressIndex: %s";
  public static final String EXCEPTION_COLON_3A291246 = " : ";
  public static final String EXCEPTION_DATAPARTITIONMAP_IS_NULL_B764418A = "dataPartitionMap is null";
  public static final String EXCEPTION_ARG_634FCEDB = "%s";
  public static final String EXCEPTION_TABLE_ARGUMENT_WITH_SET_SEMANTICS_REQUIRES_AN_ORDER_BY_CLAUSE_10C986D9 = "Table argument with set semantics requires an ORDER BY clause.";
  public static final String EXCEPTION_THE_TYPE_OF_THE_COLUMN_ARG_IS_NOT_COMPARABLE_E3098096 = "The type of the column [%s] is not comparable.";
  public static final String EXCEPTION_NO_COMPARABLE_COLUMNS_FOUND_FOR_M4_CALCULATION_4E5A3092 = "No comparable columns found for M4 calculation.";
  public static final String EXCEPTION_INVALID_SCALAR_ARGUMENT_SLIDE_SHOULD_BE_A_POSITIVE_VALUE_F019E091 = "Invalid scalar argument SLIDE, should be a positive value";
  public static final String EXCEPTION_THE_ORDER_BY_CLAUSE_OF_THE_DATA_ARGUMENT_MUST_CONTAIN_EXACTLY_THE_TIME_COLUMN_SPECIFIED_BY_THE_TIMECOL_ARGUMENT_4375BAE9 = "The ORDER BY clause of the DATA argument must contain exactly the time column specified by the TIMECOL argument.";
  public static final String EXCEPTION_UNSUPPORTED_M4_VALUE_TYPE_AF0EF286 = "Unsupported M4 value type: ";
  public static final String EXCEPTION_DISK_SPACE_WARNING_THRESHOLD_MUST_BE_IN_0_1_BUT_WAS_7B345766 = "disk_space_warning_threshold must be in [0, 1), but was ";

}
