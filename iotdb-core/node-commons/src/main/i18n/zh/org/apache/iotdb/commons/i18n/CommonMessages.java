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
  public static final String STARTUP_FAILED = "启动 [%s] 失败，原因：[%s]";

  // --- path ---
  public static final String ILLEGAL_PATH = "%s 不是合法路径";
  public static final String ILLEGAL_PATH_WITH_REASON = "%s 不是合法路径，原因：%s";
  public static final String PATH_NOT_MEASUREMENT = "该路径不代表一个测点";
  public static final String PATH_DUPLICATED = "路径重复：%s";
  public static final String OBJECT_TYPE_COLUMN_NOT_SUPPORTED = "不支持 object 类型的列。";

  // --- cluster ---
  public static final String NODE_TYPE_NOT_EXIST = "NodeType %s 不存在。";
  public static final String NODE_STATUS_NOT_EXIST = "NodeStatus %s 不存在。";
  public static final String UNKNOWN_NODE_STATUS = "未知 NodeStatus %s。";

  // --- consensus ---
  public static final String UNRECOGNIZED_CONSENSUS_GROUP_ID =
      "无法识别的 ConsensusGroupId：%s";
  public static final String IOTV2_BG_NOT_TERMINATED =
      "IoTV2 后台服务在 {}s 内未终止";
  public static final String IOTV2_BG_STILL_RUNNING =
      "IoTV2 后台线程在 30s 后仍未退出";

  // --- cq ---
  public static final String UNKNOWN_TIMEOUT_POLICY = "未知 TimeoutPolicy：%s";
  public static final String UNKNOWN_CQ_STATE = "未知 CQState：%s";

  // --- memory ---
  public static final String MEMORY_ALLOC_INTERRUPTED =
      "exactAllocate：等待可用内存时被中断";
  public static final String MEMORY_RELEASE_FAILED =
      "releaseWithOutNotify：关闭内存块 {} 失败";
  public static final String MEMORY_RELEASE_FAILED_NO_DETAIL =
      "releaseWithOutNotify：关闭内存块失败";
  public static final String MEMORY_SIZE_SHOULD_BE_POSITIVE =
      "getOrCreateMemoryManager {}：sizeInBytes 应为正数";

  // --- file ---
  public static final String SHOULD_NEVER_TOUCH_HERE = "不应执行到此处";

  // --- externalservice ---
  public static final String UNKNOWN_SERVICE_TYPE = "未知 ServiceType：%s";
  public static final String UNKNOWN_STATE = "未知 State：%s";

  // --- subscription ---
  public static final String CONFIG_PRINT = "{}：{}";

  // --- concurrent ---
  public static final String EXCEPTION_IN_THREAD = "线程 {}-{} 中发生异常";
  public static final String INTERRUPTED_WHILE_AWAITING = "等待条件时被中断";
  public static final String EXCEPTION_WHILE_EVALUATING = "计算条件时发生异常";
  public static final String UNKNOWN_THREAD_NAME = "未知线程名：{}";
  public static final String TASK_CANCELLED_IN_POOL = "线程池 {} 中的任务已取消";
  public static final String EXCEPTION_IN_THREAD_POOL = "线程池 {} 中发生异常";
  public static final String SCHEDULE_TASK_FAILED = "调度任务失败";
  public static final String RUN_THREAD_FAILED = "运行线程失败";

  // --- enums ---
  public static final String SYSTEM_READ_ONLY = "系统模式已设为只读（READ_ONLY）";
  public static final String UNRECOVERABLE_ERROR = "发生不可恢复的错误！直接关闭系统。";

  // --- udf ---
  public static final String UNKNOWN_FUNCTION_TYPE = "未知 FunctionType：%s";
  public static final String INVALID_INPUT = "无效输入：%s";
  public static final String UDF_LIB_ROOT = "UDF lib 根目录：{}";
  public static final String UDF_MD5_READ_ERROR = "读取 {} 的 md5 时出错";
  public static final String VALUE_NOT_NUMERIC = "输入时间序列的值不是数值类型。\n";
  public static final String FAIL_GET_DATA_TYPE = "获取第 %s 行的数据类型失败";
  public static final String UDTF_ABS_SET_TRANSFORMER = "UDTFAbs#setTransformer()";
  public static final String BASE_VALUE_SHOULD_NOT_BE_NULL =
      "比较时基准值不应为 null";
  public static final String SIZE_MUST_BE_POSITIVE = "Size 必须大于 0";

  // --- sync ---
  public static final String UNEXPECTED_SERIALIZATION_ERROR =
      "序列化 PipeInfo 时发生意外错误。";

  // --- security ---
  public static final String ENCRYPT_PASSWORD_ERROR = "加密密码时出错。";
  public static final String CLASSLOADER_NOT_DETERMINED = "无法确定用于加载类的 ClassLoader。";

  // --- binaryallocator ---
  public static final String BINARY_ALLOCATOR_RUNNING_GC_EVICTION =
      "二进制分配器正在执行 GC 驱逐";
  public static final String BINARY_ALLOCATOR_SHUTTING_DOWN_HIGH_GC =
      "由于 GC 时间百分比过高 ({}%)，二进制分配器正在关闭。";
  public static final String AUTO_RELEASER_EXIT_INTERRUPTED =
      "{} 因 InterruptedException 退出。";
  public static final String STOPPING_COMPONENT = "正在停止 {}";
  public static final String UNABLE_TO_STOP_AUTO_RELEASER =
      "在 {} 毫秒后仍无法停止自动释放器";
  public static final String UNABLE_TO_STOP_EVICTOR = "在 {} 毫秒后仍无法停止驱逐器";

  private CommonMessages() {}

  public static final String COLLECTION_MUST_NOT_BE_NULL = "集合不能为空。";
  public static final String MAP_MUST_NOT_BE_NULL = "Map 不能为空。";
  public static final String MAP_ENTRY_MUST_NOT_BE_NULL = "Map 条目不能为空。";
  public static final String ITERATOR_MUST_NOT_BE_NULL = "迭代器不能为空";
  public static final String ITERATOR_REMOVE_ONLY_AFTER_NEXT = "Iterator remove() 只能在 next() 之后调用一次";
  public static final String FAIL_TO_GET_DATA_TYPE_IN_ROW = "获取行中数据类型失败，行时间：";
  // ---------------------------------------------------------------------------
  // Additional auto-collected messages
  // ---------------------------------------------------------------------------
  public static final String LOG_STEP_METRICS_ARG_ARG_TOTAL_ARG_SUM_2FMS_AVG_ARG_87491AB0 = "step metrics [%d]-[%s] - Total: %d, SUM: %.2fms, AVG: %fms, Last%dAVG: %fms";
  public static final String LOG_ERROR_OCCURRED_DURING_TRANSFERRING_FILE_ARG_BYTEBUFFER_CAUSE_ARG_FEDC38A3 = "错误 occurred during transferring 文件{} to ByteBuffer, the cause is {}";
  public static final String LOG_ERROR_OCCURRED_DURING_WRITING_BYTEBUFFER_ARG_CAUSE_ARG_F3AD2DA0 = "错误 occurred during writing bytebuffer to {} , the cause is {}";
  public static final String EXCEPTION_SIZE_FILE_EXCEED_ARG_BYTES_C60F1149 = "Size of 文件 exceed %d bytes";
  public static final String EXCEPTION_UNRECOGNIZED_TCONSENSUSGROUPTYPE_9204FF8E = "Unrecognized TConsensusGroupType: ";
  public static final String EXCEPTION_ID_1F238F51 = " with id = ";
  public static final String LOG_MEMORY_COST_RELEASED_LARGER_THAN_MEMORY_COST_MEMORY_BLOCK_ARG_00DD9DA9 = "The memory cost to be released is larger than the memory cost of memory block {}";
  public static final String LOG_EXACTALLOCATEIFSUFFICIENT_FAILED_ALLOCATE_MEMORY_A47897D9 = "exactAllocateIfSufficient: 无法allocate memory, ";
  public static final String LOG_TOTAL_MEMORY_SIZE_ARG_BYTES_USED_MEMORY_SIZE_ARG_BYTES_5FB5059F = "total memory size {} bytes, used memory size {} bytes, ";
  public static final String LOG_REQUESTED_MEMORY_SIZE_ARG_BYTES_USED_THRESHOLD_ARG_D7061DEB = "requested memory size {} bytes, used threshold {}";
  public static final String LOG_TRYALLOCATE_ALLOCATED_MEMORY_B3D564D9 = "tryAllocate: allocated memory, ";
  public static final String LOG_ORIGINAL_REQUESTED_MEMORY_SIZE_ARG_BYTES_03D28A6B = "original requested memory size {} bytes, ";
  public static final String LOG_ACTUAL_REQUESTED_MEMORY_SIZE_ARG_BYTES_62760058 = "actual requested memory size {} bytes";
  public static final String LOG_TRYALLOCATE_FAILED_ALLOCATE_MEMORY_838FA6FB = "tryAllocate: 无法allocate memory, ";
  public static final String LOG_REQUESTED_MEMORY_SIZE_ARG_BYTES_BF9CEF81 = "requested memory size {} bytes";
  public static final String LOG_GETORREGISTERMEMORYBLOCK_FAILED_MEMORY_BLOCK_ARG_ALREADY_EXISTS_42CA8914 = "getOr注册MemoryBlock 失败: memory block {} 已经exists, ";
  public static final String LOG_IT_S_SIZE_ARG_REQUESTED_SIZE_ARG_AF8F04B2 = "it's size is {}, requested size is {}";
  public static final String LOG_GETMEMORYMANAGER_MEMORY_MANAGER_ARG_ALREADY_EXISTS_IT_S_SIZE_ARG_0102560A = "getMemoryManager: memory manager {} 已经exists, it's size is {}, enabled is {}";
  public static final String LOG_GETORCREATEMEMORYMANAGER_FAILED_TOTAL_MEMORY_SIZE_ARG_BYTES_LESS_THAN_ALLOCATED_3D110256 =
      "getOr创建MemoryManager 失败: total memory size {} bytes is less than allocated memory size {}"
      + " bytes";
  public static final String EXCEPTION_EXACTALLOCATE_FAILED_ALLOCATE_MEMORY_AFTER_ARG_RETRIES_957A647B = "exactAllocate: 无法allocate memory after %d retries, ";
  public static final String EXCEPTION_TOTAL_MEMORY_SIZE_ARG_BYTES_USED_MEMORY_SIZE_ARG_BYTES_9FC9A9C6 = "total memory size %d bytes, used memory size %d bytes, ";
  public static final String EXCEPTION_REQUESTED_MEMORY_SIZE_ARG_BYTES_E6340842 = "requested memory size %d bytes";
  public static final String EXCEPTION_REGISTER_MEMORY_BLOCK_ARG_FAILED_SIZEINBYTES_SHOULD_NON_NEGATIVE_EC54AA75 = "注册 memory block %s 失败: sizeInBytes 应be non-negative";
  public static final String LOG_DELETE_SYSTEM_PROPERTIES_TMP_FILE_FAIL_YOU_MAY_MANUALLY_DELETE_F81C4A53 = "删除 system.properties tmp 文件 fail, you may manually 删除 it: {}";
  public static final String LOG_FAILED_DELETE_SYSTEM_PROPERTIES_FILE_YOU_SHOULD_MANUALLY_DELETE_THEM_77F91A98 = "无法删除 system.properties 文件, you 应manually 删除 them: {}, {}";
  public static final String EXCEPTION_LENGTH_PARAMETERS_SHOULD_EVENLY_DIVIDED_2_BUT_ACTUAL_LENGTH_E9A792D9 = "Length of 参数s 应be evenly divided by 2, but the actual length is ";
  public static final String EXCEPTION_TMP_SYSTEM_PROPERTIES_FILE_MUST_EXIST_CALL_REPLACEFORMALFILE_FA63B976 = "Tmp system properties 文件 必须exist when call replaceFormal文件";
  public static final String LOG_UNRECOVERABLE_ERROR_OCCURS_CHANGE_SYSTEM_STATUS_READ_ONLY_BECAUSE_HANDLE_05C9AD1A =
      "Unrecoverable 错误 occurs! Change system 状态 to 读取-only，原因：handle_system_错误 is"
      + " CHANGE_TO_READ_ONLY. Only 查询 statements are permitted!";
  public static final String LOG_UNRECOVERABLE_ERROR_OCCURS_SHUTDOWN_SYSTEM_DIRECTLY_BECAUSE_HANDLE_SYSTEM_ERROR_14FC06C9 = "Unrecoverable 错误 occurs! Shutdown system directly，原因：handle_system_错误 is SHUTDOWN.";
  public static final String EXCEPTION_TYPE_ARG_NOT_SUPPORTED_PIPE_RATE_AVERAGE_F74694AD = "The type %s 不是supported in pipe rate average.";
  public static final String EXCEPTION_UNKNOWN_UDFTYPE_9A8D1B23 = "未知的UDFType:";
  public static final String EXCEPTION_8S_5F5F831F = "%8s";
  public static final String EXCEPTION_CAN_NOT_RECOGNIZE_PIPETYPE_ARG_8850A249 = "不能recognize PipeType %s.";
  public static final String EXCEPTION_TARGETREGIONLIST_EMPTY_DEVICE_ARG_TIMESLOT_ARG_E7E5818C = "targetRegionList is 为空. device: %s, timeSlot: %s";
  public static final String EXCEPTION_DATABASE_18F8303F = "数据库 ";
  public static final String EXCEPTION_NOT_EXISTS_FAILED_CREATE_AUTOMATICALLY_BECAUSE_ENABLE_AUTO_CREATE_SCHEMA_80DE1A4B = " 不exists and 无法创建 automatically，原因：enable_auto_创建_模式 is FALSE.";
  public static final String EXCEPTION_PATH_DOES_NOT_EXIST_737CB95D = "路径 不exist. ";
  public static final String EXCEPTION_CAN_T_GET_NEXT_FOLDER_ARG_BECAUSE_THEY_ALL_FULL_A105BB2D = "Can't get next folder from [%s],，原因：they are all full.";
  public static final String EXCEPTION_PARAMETER_ARG_CAN_NOT_ARG_PLEASE_SET_ARG_BECAUSE_ARG_749738D1 = "参数 %s 不能be %s, please set to: %s. Because %s";
  public static final String EXCEPTION_QUERY_EXECUTION_TIME_OUT_A5DC7BFB = "查询 execution is time out";
  public static final String EXCEPTION_OBJECT_FILE_ARG_DOES_NOT_EXIST_7EA8CB1C = "Object 文件 %s 不exist";
  public static final String EXCEPTION_ARG_NOT_LEGAL_PRIVILEGE_504838E8 = "%s 不是a legal 权限";
  public static final String EXCEPTION_SOME_PORTS_OCCUPIED_77ED044D = "Some ports are occupied";
  public static final String EXCEPTION_PORTS_ARG_OCCUPIED_B462E9DA = "Ports %s are occupied";
  public static final String EXCEPTION_UNEXPECTED_ERROR_OCCURS_SERIALIZATION_A6B2E222 = "Unexpected 错误 occurs in serialization";
  public static final String EXCEPTION_COLUMN_ARG_TABLE_ARG_ARG_DOES_NOT_EXIST_D8145581 = "Column %s in table '%s.%s' 不exist.";
  public static final String EXCEPTION_TABLE_ARG_ARG_DOES_NOT_EXIST_796E503B = "Table '%s.%s' 不exist.";
  public static final String EXCEPTION_TABLE_ARG_ARG_ALREADY_EXISTS_D4BDF4B5 = "Table '%s.%s' 已经exists.";
  public static final String EXCEPTION_COULDN_T_CONSTRUCTOR_SERIESPARTITIONEXECUTOR_CLASS_ARG_34FB9F45 = "Couldn't Constructor SeriesPartitionExecutor class: %s";
  public static final String EXCEPTION_CANNOT_USE_SETVALUE_OBJECT_BEING_SET_ALREADY_MAP_676ED3BF = "无法use setValue() when the object being set is 已经in the map";
  public static final String EXCEPTION_ITERATOR_GETKEY_CAN_ONLY_CALLED_AFTER_NEXT_BEFORE_REMOVE_009C456B = "Iterator getKey() can only be called after next() and before 移除()";
  public static final String EXCEPTION_ITERATOR_GETVALUE_CAN_ONLY_CALLED_AFTER_NEXT_BEFORE_REMOVE_927A88A2 = "Iterator getValue() can only be called after next() and before 移除()";
  public static final String EXCEPTION_ITERATOR_SETVALUE_CAN_ONLY_CALLED_AFTER_NEXT_BEFORE_REMOVE_51505AD1 = "Iterator setValue() can only be called after next() and before 移除()";
  public static final String LOG_FAILED_CLOSE_UDFCLASSLOADER_QUERYID_ARG_BECAUSE_ARG_8B1C3739 = "无法关闭 UDFClass加载er (查询Id: {}),，原因：{}";
  public static final String EXCEPTION_ATTRIBUTE_ARG_ARG_REQUIRED_BUT_WAS_NOT_PROVIDED_CD090883 = "attribute \"%s\"/\"%s\" is required but was 不provided.";
  public static final String EXCEPTION_USE_ATTRIBUTE_ARG_ARG_ONLY_ONE_AT_TIME_B431468C = "use attribute \"%s\" or \"%s\" only one at a time.";
  public static final String EXCEPTION_ILLEGAL_OUTLIER_METHOD_OUTLIER_TYPE_SHOULD_AVG_STENDIS_COS_PRENEXTDIS_91D1C70A = "Illegal outlier method. Outlier type 应be avg, stendis, cos or prenextdis.";
  public static final String EXCEPTION_ILLEGAL_AGGREGATION_METHOD_AGGREGATION_TYPE_SHOULD_AVG_MIN_MAX_SUM_2D7BEC96 = "Illegal aggregation method. Aggregation type 应be avg, min, max, sum, extreme, variance.";
  public static final String EXCEPTION_CUMULATIVE_TABLE_FUNCTION_REQUIRES_SIZE_MUST_INTEGRAL_MULTIPLE_STEP_D8A9DA94 = "Cumulative table function requires size 必须be an integral multiple of step.";
  public static final String EXCEPTION_COLUMN_TYPE_MUST_NUMERIC_IF_DELTA_NOT_0_F7864D4E = " The column type 必须be numeric if DELTA 不是0.";
  public static final String EXCEPTION_TYPE_COLUMN_ARG_NOT_AS_EXPECTED_7A81636E = "The type of the column [%s] 不是as expected.";
  public static final String EXCEPTION_REQUIRED_COLUMN_ARG_NOT_FOUND_SOURCE_TABLE_ARGUMENT_993E1C08 = "Required column [%s] 不found in the source table argument.";
  public static final String EXCEPTION_UNSUPPORTED_PROGRESS_INDEX_TYPE_ARG_A84CDFF9 = "不支持的progress index type %s.";
  public static final String EXCEPTION_TIMEWINDOWSTATEPROGRESSINDEX_DOES_NOT_SUPPORT_TOPOLOGICAL_SORTING_897C8976 = "TimeWindowStateProgressIndex 不support topological sorting";
  public static final String EXCEPTION_INTENDED_READ_LENGTH_ARG_BUT_ARG_ACTUALLY_READ_DESERIALIZING_TIMEPROGRESSINDEX_63CD54E4 =
      "The intended 读取 length is %s but %s is actually 读取 when deserializing TimeProgressIndex,"
      + " ProgressIndex: %s";
  public static final String EXCEPTION_COLON_3A291246 = " : ";
  public static final String EXCEPTION_DATAPARTITIONMAP_IS_NULL_B764418A = "dataPartitionMap 不能为空";
  public static final String EXCEPTION_ARG_634FCEDB = "%s";

}
