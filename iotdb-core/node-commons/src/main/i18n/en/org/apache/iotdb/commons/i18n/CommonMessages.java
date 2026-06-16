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
}
