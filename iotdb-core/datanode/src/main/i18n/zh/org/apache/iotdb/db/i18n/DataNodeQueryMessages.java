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
      "未找到匹配的数据库，请检查路径 ";
  public static final String THIS_NODE_ISN_T_INSTANCE_OF_SCHEMAENTITYNODE =
      "该节点不是 SchemaEntityNode 实例。";
  public static final String THIS_NODE_ISN_T_INSTANCE_OF_SCHEMAMEASUREMENTNODE =
      "该节点不是 SchemaMeasurementNode 实例。";

  // --- Execution ---

  public static final String ERROR_SETTING_FUTURE_STATE_FOR =
      "为 {} 设置 future 状态时出错";
  public static final String ERROR_NOTIFYING_STATE_CHANGE_LISTENER_FOR =
      "通知 {} 的状态变更监听器时出错";
  public static final String SERVER_IS_SHUTTING_DOWN =
      "服务器正在关闭";

  // --- Execution / Aggregation ---

  public static final String INVALID_AGGREGATION_FUNCTION =
      "无效的聚合函数：";
  public static final String UNKNOWN_DATA_TYPE =
      "未知的数据类型：";
  public static final String COUNT_IF_WITH_SLIDINGWINDOW_IS_NOT_SUPPORTED_NOW =
      "目前不支持 COUNT_IF 与滑动窗口组合使用";
  public static final String TIME_DURATION_WITH_SLIDINGWINDOW_IS_NOT_SUPPORTED_NOW =
      "目前不支持 TIME_DURATION 与滑动窗口组合使用";
  public static final String MODE_WITH_SLIDINGWINDOW_IS_NOT_SUPPORTED_NOW =
      "目前不支持 MODE 与滑动窗口组合使用";
  public static final String INVALID_AGGREGATION_TYPE =
      "无效的聚合类型：";

  // --- Execution / Driver ---

  public static final String QUERYDATASOURCE_SHOULD_NEVER_BE_NULL =
      "QueryDataSource 不应为 null！";

  // --- Execution / Exchange ---

  public static final String SOURCE_HANDLE_FAILED_DUE_TO =
      "Source handle 失败，原因：";
  public static final String SINK_FAILED_DUE_TO =
      "Sink 失败，原因";
  public static final String ISINKCHANNEL_FAILED_DUE_TO =
      "ISinkChannel 失败，原因";
  public static final String SINK_HANDLE_FAILED_DUE_TO =
      "Sink handle 失败，原因";
  public static final String MPPDATAEXCHANGEMANAGER_INIT_SUCCESSFULLY =
      "MPPDataExchangeManager 初始化成功";
  public static final String QUEUE_HAS_BEEN_DESTROYED =
      "队列已被销毁";
  public static final String SINK_HANDLE_IS_BLOCKED =
      "Sink handle 已被阻塞。";
  public static final String LOCALSINKCHANNEL_IS_ABORTED =
      "LocalSinkChannel 已中止。";
  public static final String ERROR_OCCURRED_WHEN_TRY_TO_ABORT_CHANNEL =
      "尝试中止通道时发生错误。";
  public static final String ERROR_OCCURRED_WHEN_TRY_TO_CLOSE_CHANNEL =
      "尝试关闭通道时发生错误。";
  public static final String SHUFFLESINKHANDLE_IS_ABORTED =
      "ShuffleSinkHandle 已中止。";
  public static final String UNSUPPORTED_TYPE_OF_SHUFFLE_STRATEGY =
      "不支持的 shuffle 策略类型";
  public static final String SINKCHANNEL_IS_ABORTED_OR_CLOSED =
      "SinkChannel 已中止或关闭。";
  public static final String THE_DATA_BLOCK_DOESN_T_EXIST_SEQUENCE_ID =
      "数据块不存在。序列 ID：";
  public static final String THE_TSBLOCK_DOESNT_EXIST_SEQUENCE_ID_REMAINING =
      "TsBlock 不存在。序列 ID 为 {}，剩余映射为 {}";
  public static final String SINKCHANNEL_IS_ABORTED =
      "SinkChannel 已中止。";
  public static final String FAILED_TO_SEND_NEW_DATA_BLOCK_EVENT_ATTEMPT =
      "发送新数据块事件失败，尝试次数：{}";
  public static final String FAILED_TO_SEND_END_OF_DATA_BLOCK_EVENT =
      "发送数据块结束事件失败，尝试次数：{}";
  public static final String FAILED_TO_SEND_END_OF_DATA_BLOCK_EVENT_2 =
      "所有重试后仍无法发送数据块结束事件";
  public static final String SOURCE_HANDLE_IS_BLOCKED =
      "Source handle 已被阻塞。";
  public static final String RESERVED_DATA_BLOCK_SIZE_IS_NULL =
      "预留的数据块大小为 null。";
  public static final String DATA_BLOCK_SIZE_IS_NULL =
      "数据块大小为 null。";
  public static final String SOURCE_HANDLE_IS_ABORTED =
      "Source handle 已中止。";
  public static final String SOURCEHANDLE_IS_CLOSED =
      "SourceHandle 已关闭。";

  // --- Execution / Executor ---

  public static final String EXECUTE_FRAGMENTINSTANCE_IN_CONSENSUSGROUP_FAILED =
      "在共识组 {} 中执行 FragmentInstance 失败。";
  public static final String EXECUTE_FRAGMENTINSTANCE_IN_QUERYEXECUTOR_FAILED =
      "在 QueryExecutor 中执行 FragmentInstance 失败。";
  public static final String FAILED_IN_THE_WRITE_API_EXECUTING_THE_CONSENSUS =
      "写入 API 执行共识层时失败，原因：";

  // --- Execution / Fragment ---

  public static final String UNKNOWN_EXCEPTION =
      "[未知异常]：";
  public static final String WAIT_MS_FOR_ALL_DRIVERS_CLOSED =
      "等待 {} 毫秒以关闭所有 Driver";
  public static final String EXCEPTION_HAPPENED_WHEN_EXECUTING_UDTF =
      "执行 UDTF 时发生异常：";
  public static final String ERROR_WHEN_CREATE_FRAGMENTINSTANCEEXECUTION =
      "创建 FragmentInstanceExecution 时出错。";
  public static final String EXECUTE_ERROR_CAUSED_BY =
      "执行错误，原因：";

  // --- Execution / Memory ---

  public static final String FREE_MORE_MEMORY_THAN_HAS_BEEN_RESERVED =
      "释放的内存超过已预留的量。";
  public static final String ESTIMATED_MODS_TREE_SIZE_DECREASED =
      "估算的 mods tree 大小从 %d 减少到 %d，TsFile：%s。";

  // --- Execution / Operator ---

  public static final String UNKNOWN_DATA_TYPE_2 =
      "未知的数据类型 ";
  public static final String ERROR_OCCURRED_WHEN_LOGGING_INTERMEDIATE_RESULT_OF_ANALYZE =
      "记录分析中间结果时发生错误。";

  // --- Execution / Operator / Process ---

  public static final String GETWRITTENCOUNT_MEASUREMENT_IS_NOT_SUPPORTED =
      "不支持 getWrittenCount(measurement) 操作";
  public static final String GETWRITTENCOUNT_IS_NOT_SUPPORTED =
      "不支持 getWrittenCount() 操作";
  public static final String THE_MEMORY_THRESHOLD_MUST_BE_GREATER_THAN_0 =
      "内存阈值必须大于 0。";
  public static final String FAILED_TO_CREATE_DIRECTORIES =
      "创建目录失败：";
  public static final String TARGET_FILE_ALREADY_EXISTS =
      "目标文件已存在：";
  public static final String FAILED_TO_CREATE_FILE =
      "创建文件失败：";
  public static final String DATA_TYPE_OF_TARGET_TIME_COLUMN_IS_NOT =
      "目标时间列的数据类型不是 TIMESTAMP";
  public static final String DUPLICATE_COLUMN_NAMES_IN_QUERY_DATASET =
      "查询数据集中存在重复的列名。";
  public static final String SOME_SPECIFIED_TAG_COLUMNS_ARE_NOT_EXIST_IN =
      "部分指定的标签列在查询数据集中不存在。";
  public static final String NUMBER_OF_FIELD_COLUMNS_SHOULD_BE_LARGER_THAN =
      "字段列的数量应大于 0。";
  public static final String ALL_CHILD_SHOULD_HAVE_SAME_TIME_COLUMN_RESULT =
      "所有子节点应具有相同的时间列结果！";
  public static final String LAST_READ_RESULT_SHOULD_ONLY_HAVE_ONE_RECORD =
      "Last 读取结果应只有一条记录";

  // --- Execution / Operator / Schema ---

  public static final String FAILED_TO_CONVERT_NODE_PATH_TO_PARTIALPATH =
      "将节点路径转换为 PartialPath {} 失败";

  // --- Execution / Operator / Source ---

  public static final String ERROR_OCCURS_WHEN_SCANNING_ACTIVE_TIME_SERIES =
      "扫描活跃时间序列时发生错误。";
  public static final String ERROR_WHILE_SCANNING_THE_FILE =
      "扫描文件时发生错误";
  public static final String ERROR_HAPPENED_WHILE_SCANNING_THE_FILE =
      "扫描文件时发生错误";
  public static final String ALL_CACHED_CHUNKS_SHOULD_BE_CONSUMED_FIRST =
      "所有缓存的 chunk 应先被消费";
  public static final String OVERLAPPED_DATA_SHOULD_BE_CONSUMED_FIRST =
      "重叠数据应先被消费";
  public static final String NO_MORE_BATCH_DATA =
      "没有更多的批次数据";
  public static final String GETALLSATISFIEDPAGEDATA_SHOULDN_T_BE_CALLED_HERE =
      "此处不应调用 getAllSatisfiedPageData()";
  public static final String GETPAGEREADER_SHOULDN_T_BE_CALLED_HERE =
      "此处不应调用 getPageReader()";
  public static final String UNSUPPORTED_COLUMN_TYPE =
      "不支持的列类型：";
  public static final String FAIL_TO_CLOSE_CTEDATAREADER =
      "关闭 CteDataReader 失败";
  public static final String UNKNOWN_TABLE =
      "未知的表：";
  public static final String FAILED_TO_CLOSE_READER_IN_TABLEDISKUSAGESUPPLIER =
      "在 TableDiskUsageSupplier 中关闭 reader 失败";
  public static final String UNSUPPORTED_CATEGORY =
      "不支持的列类别：";

  // --- Execution / Operator / Window ---

  public static final String UNSUPPORTED_INFERENCE_WINDOW_TYPE =
      "不支持的推理窗口类型：";

  // --- Execution / Schedule ---

  public static final String EXECUTOR_FAILED_TO_POLL_DRIVER_TASK_FROM_QUEUE =
      "执行器 {} 从队列中获取驱动任务失败";
  public static final String DRIVERTASK_SHOULD_NEVER_BE_NULL =
      "DriverTask 不应为 null";
  public static final String EXECUTEFAILED =
      "[执行失败]";
  public static final String EXECUTOR_EXITS_BECAUSE_IT_IS_CLOSED =
      "执行器 {} 因已关闭而退出。";
  public static final String CLEAR_DRIVERTASK_FAILED =
      "清除 DriverTask 失败";

  // --- Execution / Warnings ---

  public static final String CODE_IS_NEGATIVE =
      "code 为负数";

  // --- Metric ---

  public static final String UNSUPPORTED_STAGE_IN_TREE_MODEL =
      "树模型中不支持的阶段：";
  public static final String UNSUPPORTED_STAGE_IN_TABLE_MODEL =
      "表模型中不支持的阶段：";

  // --- Plan ---

  public static final String TOPOLOGY_LATEST_VIEW_FROM_CONFIG_NODE =
      "[拓扑] 来自 ConfigNode 的最新视图：{}";
  public static final String EXPIRED_QUERIES_INFO_CLEAR_THREAD_IS_SUCCESSFULLY_STARTED =
      "过期查询信息清理线程已成功启动。";
  public static final String COST_MS =
      "耗时：{} 毫秒，{}";

  // --- Plan / Analyze ---

  public static final String COMPUTEDATAPARTITIONPARAMS_FOR =
      "计算数据分区参数，目标：";
  public static final String UNSUPPORTED_OPERATOR =
      "不支持的运算符：";
  public static final String UNSUPPORTED_EXPRESSION =
      "不支持的表达式：";
  public static final String ONLY_SUPPORT_AND_OPERATOR_IN_DELETION =
      "删除操作仅支持 AND 运算符";
  public static final String LEFT_HAND_EXPRESSION_IS_NOT_AN_IDENTIFIER =
      "左侧表达式不是标识符：";
  public static final String THE_LEFT_HAND_VALUE_MUST_BE_AN_IDENTIFIER =
      "左侧值必须是标识符：";
  public static final String THE_TABLE_S_DOES_NOT_CONTAIN_A_TIME_COLUMN =
      "表 '%s' 不包含时间列";
  public static final String THE_OPERATOR_OF_TAG_PREDICATE_MUST_BE_FOR =
      "标签谓词的运算符必须为 '='，目标：";
  public static final String ONLY_TIME_FILTERS_ARE_SUPPORTED_IN_LAST_QUERY =
      "LAST 查询中仅支持时间过滤器";
  public static final String VIEWS_CANNOT_BE_USED_IN_GROUP_BY_TAGS =
      "视图暂不支持在 GROUP BY TAGS 查询中使用。";
  public static final String ONLY_TIME_FILTERS_ARE_SUPPORTED_IN_GROUP_BY =
      "GROUP BY TAGS 查询中仅支持时间过滤器";
  public static final String UNSUPPORTED_WINDOW_TYPE =
      "不支持的窗口类型";
  public static final String AGGREGATION_EXPRESSION_SHOULDN_T_EXIST_IN_GROUP_BY =
      "GROUP BY 子句中不应包含聚合表达式";
  public static final String ONLY_SUPPORT_NUMERIC_TYPE_WHEN_DELTA_0 =
      "当 delta != 0 时仅支持数值类型";
  public static final String ONLY_SUPPORT_BOOLEAN_TYPE_IN_PREDICT_OF_GROUP =
      "GROUP BY SERIES 的谓词中仅支持布尔类型";
  public static final String GROUP_BY_MONTH_DOESN_T_SUPPORT_ORDER_BY =
      "按月分组目前不支持按时间降序排列。";
  public static final String NO_RUNNING_DATANODES =
      "没有运行中的 DataNode";
  public static final String AN_ERROR_OCCURRED_WHEN_SERIALIZING_PATTERN_TREE =
      "序列化模式树时发生错误";
  public static final String EXPRESSION_IN_GROUP_BY_SHOULD_INDICATE_ONE_VALUE =
      "GROUP BY 中的表达式应指定一个值";
  public static final String EXPRESSION_IN_ORDER_BY_SHOULD_INDICATE_ONE_VALUE =
      "ORDER BY 中的表达式应指定一个值";
  public static final String SHOULDN_T_ATTACH_HERE =
      "不应在此处附加";
  public static final String SELECT_INTO_THE_I_OF_SHOULD_BE_AN =
      "SELECT INTO：${i} 中的 i 应为整数。";
  public static final String FAILED_TO_GET_DATABASE_MAP =
      "获取数据库映射失败";
  public static final String LOAD_ANALYSIS_STAGE_ALL_TSFILES_HAVE_BEEN_ANALYZED =
      "加载 - 分析阶段：所有 TsFile 已分析完毕。";
  public static final String ASYNC_LOAD_HAS_FAILED_AND_IS_NOW_TRYING =
      "异步加载失败，正在尝试同步加载";
  public static final String TSFILE_IS_EMPTY =
      "TsFile {} 为空。";
  public static final String THE_ENCRYPTION_WAY_OF_THE_TSFILE_IS_NOT =
      "不支持该 TsFile 的加密方式。";
  public static final String EMPTY_FILE_DETECTED_WILL_SKIP_LOADING_THIS_FILE =
      "检测到空文件，将跳过加载此文件：{}";
  public static final String AUTO_CREATE_OR_VERIFY_SCHEMA_ERROR =
      "自动创建或验证 schema 出错。";
  public static final String FAILED_TO_FIND_TAG_COLUMN_MAPPING_FOR_TABLE =
      "未找到表 {} 的标签列映射";
  public static final String AUTO_CREATE_DATABASE_FAILED_BECAUSE =
      "自动创建数据库失败，原因：";

  // --- Plan / Execution ---

  public static final String REACHMAXRETRYCOUNT =
      "[已达最大重试次数]";
  public static final String ERROR_WHEN_EXECUTING_QUERY =
      "执行查询时出错。{}";
  public static final String WAITBEFORERETRY_WAIT_MS =
      "[重试前等待] 等待 {} 毫秒。";
  public static final String INTERRUPTED_WHEN_WAITING_RETRY =
      "等待重试时被中断";
  public static final String RETRY_RETRY_COUNT_IS =
      "[重试] 重试次数：{}";
  public static final String RESULTHANDLEABORTED =
      "[结果句柄已中止]";
  public static final String UNSUPPORTED_DATABASE_PROPERTY_KEY =
      "不支持的数据库属性键：";
  public static final String A_TABLE_CANNOT_HAVE_MORE_THAN_ONE_TIME =
      "一个表不能有多于一个时间列";
  public static final String THE_TIME_COLUMN_S_TYPE_SHALL_BE_TIMESTAMP =
      "时间列的类型应为 'timestamp'。";
  public static final String THE_TABLE_S_OLD_NAME_SHALL_NOT_BE =
      "表的旧名称不应与新名称相同。";
  public static final String ADDING_TIME_COLUMN_IS_NOT_SUPPORTED =
      "不支持添加 TIME 列。";
  public static final String THE_COLUMN_S_OLD_NAME_SHALL_NOT_BE =
      "列的旧名称不应与新名称相同。";
  public static final String DUPLICATED_PROPERTY =
      "重复的属性：";
  public static final String TABLE_PROPERTY =
      "表属性 '";
  public static final String UNKNOWN_TYPE =
      "未知的类型：%s";
  public static final String FAILED_TO_CHECK_CONFIG_ITEM_PERMISSION =
      "检查配置项权限失败";
  public static final String CONFIGTASK_IS_NOT_IMPLEMENTED_FOR =
      "ConfigTask 未针对以下内容实现：";
  public static final String FAILED_TO_GET_EXECUTABLE_FOR_UDF_USING_URI =
      "无法使用 URI {} 获取 UDF({}) 的可执行文件。";
  public static final String FAILED_TO_DROP_FUNCTION =
      "[{}] 删除函数 {} 失败。";
  public static final String FAILED_TO_DROP_TRIGGER =
      "[{}] 删除触发器 {} 失败。";
  public static final String CANNOT_REMOVE_INVALID_NODEIDS =
      "无法移除无效的节点 ID：{}";
  public static final String STARTING_TO_REMOVE_DATANODE_WITH_NODEIDS =
      "开始移除 DataNode，节点 ID：{}";
  public static final String START_TO_REMOVE_DATANODE_REMOVED_DATANODES_ENDPOINT =
      "开始移除 DataNode，已移除的 DataNode 端点：{}";
  public static final String SUBMIT_REMOVE_DATANODES_RESULT =
      "提交移除 DataNode 结果 {} ";
  public static final String STARTING_TO_REMOVE_CONFIGNODE_WITH_NODE_ID =
      "开始移除 ConfigNode，节点 ID：{}";
  public static final String CONFIGNODE_IS_REMOVED =
      "ConfigNode {} 已移除。";
  public static final String STARTING_TO_REMOVE_AINODE =
      "开始移除 AINode";
  public static final String REMOVE_AINODE_FAILED_BECAUSE_THERE_IS_NO_AINODE =
      "移除 AINode 失败，因为集群中没有 AINode。";
  public static final String AINODE_IN_THE_CLUSTER_IS_REMOVED =
      "集群中的 AINode 已移除。";
  public static final String FAILED_TO_HANDLETRANSFERCONFIGPLAN_STATUS_IS =
      "handleTransferConfigPlan 失败，状态为 {}。";
  public static final String FAILED_TO_FETCHTABLES_STATUS_IS =
      "fetchTables 失败，状态为 {}。";
  public static final String FAILED_TO_HANDLEPIPECONFIGCLIENTEXIT_STATUS_IS =
      "handlePipeConfigClientExit 失败，状态为 {}。";
  public static final String FAILED_TO_HANDLEPIPECONFIGCLIENTEXIT =
      "handlePipeConfigClientExit 失败。";
  public static final String NOT_SUPPORT_CURRENT_STATEMENT =
      "不支持当前语句";
  public static final String WRONG_REQUEST_TYPE =
      "错误的请求类型";
  public static final String WRONG_UNIT_TYPE =
      "错误的单位类型";

  // --- Plan / Expression ---

  public static final String INVALID_EXPRESSION_TYPE =
      "无效的表达式类型：";
  public static final String UNSUPPORTED_EXPRESSION_TYPE =
      "不支持的表达式类型：";
  public static final String FUNCTION_CAST_MUST_SPECIFY_A_TARGET_DATA_TYPE =
      "CAST 函数必须指定目标数据类型。";
  public static final String FUNCTION_REPLACE_MUST_SPECIFY_FROM_AND_TO_COMPONENT =
      "REPLACE 函数必须指定 from 和 to 参数。";
  public static final String PLEASE_ENSURE_INPUT_IS_CORRECT =
      "请确保输入 [%s] 正确";
  public static final String CASE_EXPRESSION_CANNOT_BE_USED_WITH_NON_MAPPABLE =
      "CASE 表达式不能与非映射型 UDF 一起使用";
  public static final String UNSUPPORTED_TRANSFORMER_ACCESS_STRATEGY =
      "不支持的转换器访问策略";
  public static final String AGGREGATE_FUNCTIONS_ARE_NOT_SUPPORTED_IN_WHERE_CLAUSE =
      "WHERE 子句中不支持聚合函数";
  public static final String IS_NULL_CANNOT_BE_PUSHED_DOWN =
      "IS NULL 不能下推";
  public static final String TIMESTAMP_DOES_NOT_SUPPORT_IS_NULL_IS_NOT =
      "TIMESTAMP 不支持 IS NULL/IS NOT NULL";
  public static final String TIMESTAMP_DOES_NOT_SUPPORT_LIKE_NOT_LIKE =
      "TIMESTAMP 不支持 LIKE/NOT LIKE";
  public static final String TIMESTAMP_DOES_NOT_SUPPORT_REGEXP_NOT_REGEXP =
      "TIMESTAMP 不支持 REGEXP/NOT REGEXP";
  public static final String GROUPBYTIME_FILTER_CANNOT_EXIST_IN_VALUE_FILTER =
      "GroupByTime 过滤器不能存在于值过滤器中。";
  public static final String IS_NULL_CAN_BE_PUSHED_DOWN =
      "IS NULL 可以下推";
  public static final String GROUP_BY_TIME_CANNOT_BE_REVERSED =
      "GROUP BY TIME 不能反转";

  // --- Plan / Optimization ---

  public static final String UNEXPECTED_PLAN_NODE =
      "意外的计划节点：";
  public static final String UNEXPECTED_PATH_TYPE =
      "意外的路径类型";
  public static final String SOURCEPATH_MUST_BE_MEASUREMENTPATH_OR_ALIGNEDPATH =
      "sourcePath 必须为 MeasurementPath 或 AlignedPath";

  // --- Plan / Parser ---

  public static final String DATATYPE_MUST_BE_DECLARED =
      "必须声明数据类型";
  public static final String UNSUPPORTED_ENCODING =
      "不支持的编码：%s";
  public static final String UNSUPPORTED_COMPRESSION =
      "不支持的压缩方式：%s";
  public static final String UNSUPPORTED_ENCODING_2 =
      "不支持的编码：%s";
  public static final String UNSUPPORTED_COMPRESSOR =
      "不支持的压缩器：%s";
  public static final String CREATE_ALIGNED_TIMESERIES_PROPERTY_IS_NOT_SUPPORTED_YET =
      "创建对齐时间序列：暂不支持 property。";
  public static final String UNSUPPORTED_COMPRESSOR_2 =
      "不支持的压缩器：%s";
  public static final String PROPERTY_IS_UNSUPPORTED_YET =
      "暂不支持属性 %s。";
  public static final String THE_TIMESERIES_SHALL_NOT_BE_ROOT =
      "时间序列不应为 root。";
  public static final String UNSUPPORTED_DATATYPE =
      "不支持的数据类型：%s";
  public static final String UNEXPECTED_FILTER_KEY =
      "意外的过滤键";
  public static final String URI_IS_EMPTY_PLEASE_SPECIFY_THE_URI =
      "URI 为空，请指定 URI。";
  public static final String INVALID_URI =
      "无效的 URI：%s";
  public static final String TRIGGER_DOES_NOT_SUPPORT_DELETE_AS_TRIGGER_EVENT =
      "触发器目前不支持 DELETE 作为触发事件。";
  public static final String PLEASE_SPECIFY_TRIGGER_TYPE_STATELESS_OR_STATEFUL =
      "请指定触发器类型：STATELESS 或 STATEFUL。";
  public static final String RENAMING_VIEW_IS_NOT_SUPPORTED =
      "不支持重命名视图。";
  public static final String VIEW_DOESN_T_SUPPORT_ALIAS =
      "视图不支持别名。";
  public static final String MODELID_SHOULD_BE_2_64_CHARACTERS =
      "ModelId 应为 2-64 个字符";
  public static final String MODELID_SHOULD_NOT_START_WITH =
      "ModelId 不应以 '_' 开头";
  public static final String MODELID_CAN_ONLY_CONTAIN_LETTERS_NUMBERS_AND_UNDERSCORES =
      "ModelId 只能包含字母、数字和下划线";
  public static final String DEVICE_ID_SHOULD_BE_CPU_OR_INTEGER =
      "设备 ID 应为 'cpu' 或整数";
  public static final String DATA_SHOULD_NOT_BE_SET_FOR_MODEL_TRAINING =
      "模型训练时不应设置数据";
  public static final String DUPLICATED_GROUP_BY_KEY_LEVEL =
      "重复的 GROUP BY 键：LEVEL";
  public static final String DUPLICATED_GROUP_BY_KEY_TAGS =
      "重复的 GROUP BY 键：TAGS";
  public static final String UNKNOWN_GROUP_BY_TYPE =
      "未知的 GROUP BY 类型。";
  public static final String DUPLICATE_ALIAS_IN_SELECT_CLAUSE =
      "SELECT 子句中存在重复的别名";
  public static final String CONSTANT_OPERAND_IS_NOT_ALLOWED =
      "不允许使用常量操作数：";
  public static final String THE_TIME_WINDOWS_MAY_EXCEED_10000_PLEASE_ENSURE =
      "时间窗口可能超过 10000 个，请确认输入。";
  public static final String START_TIME_SHOULD_BE_SMALLER_THAN_ENDTIME_IN =
      "GroupBy 中的起始时间应小于结束时间";
  public static final String KEEP_THRESHOLD_IN_GROUP_BY_CONDITION_SHOULD_BE =
      "应设置 GROUP BY 条件中的保持阈值";
  public static final String DUPLICATED_KEY_IN_GROUP_BY_TAGS =
      "GROUP BY TAGS 中存在重复的键：";
  public static final String UNKNOWN_FILL_TYPE =
      "未知的 FILL 类型。";
  public static final String UNSUPPORTED_CONSTANT_VALUE_IN_FILL =
      "FILL 中不支持的常量值：";
  public static final String OUT_OF_RANGE_LIMIT_N_N_SHOULD_BE =
      "超出范围。LIMIT <N>：N 应为 Int64。";
  public static final String LIMIT_N_N_SHOULD_BE_GREATER_THAN_0 =
      "LIMIT <N>：N 应大于 0。";
  public static final String OFFSET_OFFSETVALUE_OFFSETVALUE_SHOULD_0 =
      "OFFSET <OFFSETValue>：OFFSETValue 应 >= 0。";
  public static final String OUT_OF_RANGE_SLIMIT_SN_SN_SHOULD_BE =
      "超出范围。SLIMIT <SN>：SN 应为 Int32。";
  public static final String SLIMIT_SN_SN_SHOULD_BE_GREATER_THAN_0 =
      "SLIMIT <SN>：SN 应大于 0。";
  public static final String SOFFSET_SOFFSETVALUE_SOFFSETVALUE_SHOULD_0 =
      "SOFFSET <SOFFSETValue>：SOFFSETValue 应 >= 0。";
  public static final String ONE_ROW_SHOULD_ONLY_HAVE_ONE_TIME_VALUE =
      "一行数据应只有一个时间值";
  public static final String INSERTSTATEMENT_SHOULD_CONTAIN_AT_LEAST_ONE_MEASUREMENT =
      "InsertStatement 应至少包含一个测量值";
  public static final String NEED_TIMESTAMPS_WHEN_INSERT_MULTI_ROWS =
      "插入多行时需要时间戳";
  public static final String CAN_NOT_PARSE_TO_TIME =
      "无法将 %s 解析为时间";
  public static final String PATH_CAN_NOT_START_WITH_ROOT_IN_SELECT =
      "SELECT 子句中的路径不能以 root 开头。";
  public static final String INPUT_TIMESTAMP_CANNOT_BE_EMPTY =
      "输入时间戳不能为空";
  public static final String NOT_SUPPORT_FOR_THIS_ALIAS_PLEASE_ENCLOSE_IN =
      "不支持此别名，请使用反引号括起来。";
  public static final String STATEMENT_NEEDS_TARGET_PATHS =
      "语句需要目标路径";
  public static final String THE_DATATYPE_OF_TIMESTAMP_SHOULD_BE_LONG =
      "时间戳的数据类型应为 LONG。";
  public static final String ATTRIBUTES_OF_FUNCTIONS_SHOULD_BE_QUOTED_WITH_OR =
      "函数的属性应使用 '' 或 \"\" 引起来";
  public static final String UNSUPPORTED_CONSTANT_VALUE =
      "不支持的常量值：";
  public static final String UNSUPPORTED_CONSTANT_OPERAND =
      "不支持的常量操作数：";
  public static final String UNKNOWN_SYSTEM_STATUS_IN_SET_SYSTEM_COMMAND =
      "SET SYSTEM 命令中的系统状态未知。";
  public static final String DEVICE_TEMPLATE_ALIAS_IS_NOT_SUPPORTED_YET =
      "设备模板：暂不支持别名。";
  public static final String DEVICE_TEMPLATE_PROPERTY_IS_NOT_SUPPORTED_YET =
      "设备模板：暂不支持属性。";
  public static final String DEVICE_TEMPLATE_TAG_IS_NOT_SUPPORTED_YET =
      "设备模板：暂不支持标签。";
  public static final String DEVICE_TEMPLATE_ATTRIBUTE_IS_NOT_SUPPORTED_YET =
      "设备模板：暂不支持属性。";
  public static final String EXPECTING_DATATYPE =
      "需要数据类型";
  public static final String NOT_SUPPORT_FOR_THIS_SQL_IN_DROP_PIPE =
      "DROP PIPE 不支持此 SQL，请输入管道名。";
  public static final String NOT_SUPPORT_FOR_THIS_SQL_IN_START_PIPE =
      "START PIPE 不支持此 SQL，请输入管道名。";
  public static final String NOT_SUPPORT_FOR_THIS_SQL_IN_STOP_PIPE =
      "STOP PIPE 不支持此 SQL，请输入管道名。";
  public static final String GET_REGION_ID_STATEMENT_EXPRESSION_MUST_BE_A =
      "GET REGION ID 语句的表达式必须是时间表达式";
  public static final String WRONG_SPACE_QUOTA_TYPE =
      "错误的空间配额类型：";
  public static final String PLEASE_SET_THE_NUMBER_OF_DEVICES_GREATER_THAN =
      "请将设备数设置为大于 0";
  public static final String PLEASE_SET_THE_NUMBER_OF_TIMESERIES_GREATER_THAN =
      "请将时间序列数设置为大于 0";
  public static final String CANNOT_SET_THROTTLE_QUOTA_FOR_USER_ROOT =
      "不能为 root 用户设置限流配额。";
  public static final String PLEASE_SET_THE_NUMBER_OF_REQUESTS_GREATER_THAN =
      "请将请求数设置为大于 0";
  public static final String PLEASE_SET_THE_NUMBER_OF_CPU_GREATER_THAN =
      "请将 CPU 数量设置为大于 0";
  public static final String PLEASE_SET_THE_SIZE_GREATER_THAN_0 =
      "请将大小设置为大于 0";
  public static final String PLEASE_SET_THE_DISK_SIZE_GREATER_THAN_0 =
      "请将磁盘大小设置为大于 0";
  public static final String THERE_SHOULD_BE_ONLY_ONE_WINDOW_IN_CALL =
      "CALL INFERENCE 中应只有一个窗口。";
  public static final String THE_CREATETABLEVIEW_IS_UNSUPPORTED_IN_TREE_SQL_DIALECT =
      "树模型 SQL 方言中不支持 'CreateTableView'。";
  public static final String CURRENTLY_OTHER_EXPRESSIONS_ARE_NOT_SUPPORTED =
      "目前不支持其他表达式";
  public static final String ALIGN_DESIGNATION_INCORRECT_AT =
      "对齐指定不正确，位于：";

  // --- Plan / Relational / Analyzer ---

  public static final String COLUMN_NOT_IN_GROUP_BY_CLAUSE =
      "列 %s 不在 GROUP BY 子句中";
  public static final String DATABASE_IS_NOT_SPECIFIED_FOR_INSERT =
      "未指定插入操作的数据库：";
  public static final String IDENTIFIER_NOT_ALLOWED_IN_THIS_CONTEXT =
      "此上下文中不允许 <identifier>.*";
  public static final String UNKNOWN_SIGN =
      "未知的符号：";
  public static final String DECIMALLITERAL_IS_NOT_SUPPORTED_YET =
      "暂不支持 DecimalLiteral。";
  public static final String GENERICLITERAL_IS_NOT_SUPPORTED_YET =
      "暂不支持 GenericLiteral。";
  public static final String DISTINCT_IS_NOT_SUPPORTED_FOR_NON_AGGREGATION_FUNCTIONS =
      "非聚合函数不支持 DISTINCT";
  public static final String UNEXPECTED_PATTERN_RECOGNITION_FUNCTION =
      "意外的模式识别函数 ";
  public static final String THE_INPUT_ARGUMENT_DOES_NOT_EXIST =
      "输入参数不存在";
  public static final String MATCH_NUMBER_PATTERN_RECOGNITION_FUNCTION_TAKES_NO_ARGUMENTS =
      "MATCH_NUMBER 模式识别函数不接受参数";
  public static final String UNEXPECTED_NAVIGATION_ANCHOR =
      "意外的导航锚点：";
  public static final String UNEXPECTED_MODE =
      "意外的模式：";
  public static final String QUERY_TAKES_NO_PARAMETERS =
      "查询不接受参数";
  public static final String NO_VALUE_PROVIDED_FOR_PARAMETER =
      "未提供参数值";
  public static final String CANNOT_EXTRACT_FROM =
      "无法从 %s 中提取";
  public static final String UNKNOWN_IS_NOT_A_VALID_TYPE =
      "UNKNOWN 不是有效的类型";
  public static final String CANNOT_CAST_TO =
      "无法将 %s 转换为 %s";
  public static final String WINDOW_FRAME_START_CANNOT_BE_UNBOUNDED_FOLLOWING =
      "窗口帧起始位置不能为 UNBOUNDED FOLLOWING";
  public static final String WINDOW_FRAME_END_CANNOT_BE_UNBOUNDED_PRECEDING =
      "窗口帧结束位置不能为 UNBOUNDED PRECEDING";
  public static final String UNSUPPORTED_FRAME_TYPE =
      "不支持的帧类型：";
  public static final String COLUMNS_ONLY_SUPPORT_TO_BE_USED_IN_SELECT =
      "Columns 仅支持在 SELECT 和 WHERE 子句中使用";
  public static final String TYPE_MISMATCH_FMT =
      "%s：%s 与 %s";
  public static final String UNKNOWN_PATTERN_RECOGNITION_FUNCTION =
      "未知的模式识别函数：";
  public static final String CANNOT_ACCESS_PREANALYZED_TYPES =
      "无法访问预分析类型";
  public static final String CANNOT_ACCESS_RESOLVED_WINDOWS =
      "无法访问已解析的窗口";
  public static final String REFERENCE_IS_AMBIGUOUS =
      "引用 '%s' 有歧义";
  public static final String COLUMN_IS_AMBIGUOUS =
      "列 '%s' 有歧义";
  public static final String UNSUPPORTED_NODE_TYPE =
      "不支持的节点类型：";
  public static final String CREATE_DATABASE_STATEMENT_IS_NOT_SUPPORTED_YET =
      "暂不支持 Create Database 语句。";
  public static final String ALTER_DATABASE_STATEMENT_IS_NOT_SUPPORTED_YET =
      "暂不支持 Alter Database 语句。";
  public static final String DROP_DATABASE_STATEMENT_IS_NOT_SUPPORTED_YET =
      "暂不支持 Drop Database 语句。";
  public static final String SHOW_DATABASE_STATEMENT_IS_NOT_SUPPORTED_YET =
      "暂不支持 Show Database 语句。";
  public static final String SHOW_TABLES_STATEMENT_IS_NOT_SUPPORTED_YET =
      "暂不支持 Show Tables 语句。";
  public static final String DESCRIBE_TABLE_STATEMENT_IS_NOT_SUPPORTED_YET =
      "暂不支持 Describe Table 语句。";
  public static final String ADD_COLUMN_STATEMENT_IS_NOT_SUPPORTED_YET =
      "暂不支持 Add Column 语句。";
  public static final String CREATE_INDEX_STATEMENT_IS_NOT_SUPPORTED_YET =
      "暂不支持 Create Index 语句。";
  public static final String DROP_INDEX_STATEMENT_IS_NOT_SUPPORTED_YET =
      "暂不支持 Drop Index 语句。";
  public static final String SHOW_INDEX_STATEMENT_IS_NOT_SUPPORTED_YET =
      "暂不支持 Show Index 语句。";
  public static final String UPDATE_CAN_ONLY_SPECIFY_ATTRIBUTE_COLUMNS =
      "UPDATE 只能指定属性列。";
  public static final String DROP_FUNCTION_STATEMENT_IS_NOT_SUPPORTED_YET =
      "暂不支持 Drop Function 语句。";
  public static final String SHOW_FUNCTION_STATEMENT_IS_NOT_SUPPORTED_YET =
      "暂不支持 Show Function 语句。";
  public static final String USE_STATEMENT_IS_NOT_SUPPORTED_YET =
      "暂不支持 USE 语句。";
  public static final String TARGET_TABLE_SCHEMA_MISSES_A_TIME_CATEGORY_COLUMN =
      "目标表结构缺少 TIME 类别的列";
  public static final String TIME_COLUMN_CAN_NOT_BE_NULL =
      "时间列不能为 null";
  public static final String NO_FIELD_COLUMN_PRESENT =
      "没有 Field 列";
  public static final String FETCH_FIRST_WITH_TIES_CLAUSE_REQUIRES_ORDER_BY =
      "FETCH FIRST WITH TIES 子句需要 ORDER BY";
  public static final String RECURSIVE_CTE_IS_NOT_SUPPORTED_YET =
      "暂不支持递归 CTE。";
  public static final String MISSING_COLUMN_ALIASES_IN_RECURSIVE_WITH_QUERY =
      "递归 WITH 查询中缺少列别名";
  public static final String NESTED_RECURSIVE_WITH_QUERY =
      "嵌套的递归 WITH 查询";
  public static final String THERE_IS_AT_LEAST_ONE_RESULT_OF_EXPANDED =
      "至少存在一个展开后的结果";
  public static final String UNSUPPORTED_EXPRESSION_2 =
      "不支持的表达式：";
  public static final String RELATION_NOT_FOUND_OR_NOT_ALLOWED =
      "关系未找到或不允许访问";
  public static final String COLUMNS_NOT_ALLOWED_FOR_RELATION_THAT_HAS_NO =
      "无列的关系不允许使用 COLUMNS";
  public static final String UNKNOWN_COLUMNNAME =
      "未知的列名：";
  public static final String INVALID_REGEX =
      "无效的正则表达式 '%s'";
  public static final String COLUMNS_ARE_NOT_SUPPORTED_IN_DEREFERENCEEXPRESSION =
      "DereferenceExpression 中不支持 Columns";
  public static final String SELECT_NOT_ALLOWED_FROM_RELATION_THAT_HAS_NO =
      "不允许对无列的关系使用 SELECT *";
  public static final String COLUMN_ALIASES_NOT_SUPPORTED =
      "不支持列别名";
  public static final String SELECT_NOT_ALLOWED_IN_QUERIES_WITHOUT_FROM_CLAUSE =
      "没有 FROM 子句的查询中不允许使用 SELECT *";
  public static final String MULTIPLE_DATE_BIN_GAPFILL_CALLS_NOT_ALLOWED =
      "不允许多次调用 date_bin_gapfill";
  public static final String PATTERN_RECOGNITION_OUTPUT_TABLE_HAS_NO_COLUMNS =
      "模式识别输出表没有列";
  public static final String NATURAL_JOIN_NOT_SUPPORTED =
      "不支持自然连接";
  public static final String UNKNOWN_FILL_METHOD =
      "未知的填充方法：";
  public static final String RECURSIVE_REFERENCE_IN_INTERSECT_ALL =
      "INTERSECT ALL 中存在递归引用";
  public static final String TABLE_PROPERTY_2 =
      "表属性 ";
  public static final String THE_DATABASE_MUST_BE_SET =
      "必须设置数据库。";
  public static final String AT_MOST_ONE_TABLE_ARGUMENT_CAN_BE_PASSED =
      "最多只能向表函数传递一个表参数";
  public static final String DUPLICATE_ARGUMENT_NAME =
      "重复的参数名：%s";
  public static final String SETTING_MONTHLY_INTERVALS_IS_NOT_SUPPORTED =
      "不支持设置按月间隔。";
  public static final String FILTER_PUSH_DOWN_DOES_NOT_SUPPORT_CASE_WHEN =
      "过滤下推不支持 CASE WHEN";
  public static final String FILTER_PUSH_DOWN_DOES_NOT_SUPPORT_IF =
      "过滤下推不支持 IF";
  public static final String FILTER_PUSH_DOWN_DOES_NOT_SUPPORT_NULLIF =
      "过滤下推不支持 NULLIF";
  public static final String EXPRESSION_SHOULD_BE_NUMERIC_ACTUAL_IS =
      "表达式应为数值类型，实际为 ";
  public static final String TIMESTAMP_DOES_NOT_SUPPORT_IS_NULL =
      "TIMESTAMP 不支持 IS NULL";
  public static final String TIMESTAMP_DOES_NOT_SUPPORT_IS_NOT_NULL =
      "TIMESTAMP 不支持 IS NOT NULL";
  public static final String TIMESTAMP_DOES_NOT_SUPPORT_LIKE =
      "TIMESTAMP 不支持 LIKE";
  public static final String TIMESTAMP_DOES_NOT_CASE_WHEN =
      "TIMESTAMP 不支持 CASE WHEN";
  public static final String TIMESTAMP_DOES_NOT_IF =
      "TIMESTAMP 不支持 IF";
  public static final String TIMESTAMP_DOES_NOT_NULLIF =
      "TIMESTAMP 不支持 NULLIF";
  public static final String SHOULD_NEVER_RETURN_NULL =
      "不应返回 null。";
  public static final String IS_NULL_EXPRESSION_CAN_T_BE_PUSHED_DOWN =
      "IS NULL 表达式不能下推";
  public static final String NOT_EXPRESSION_CAN_T_BE_PUSHED_DOWN =
      "NOT 表达式不能下推";
  public static final String UNSUPPORTED_OPERATOR_2 =
      "不支持的运算符 ";
  public static final String THE_LOGICAL_EXPRESSION_HAS_NO_BOUNDED_COLUMN =
      "逻辑表达式没有绑定的列";
  public static final String THE_NOT_EXPRESSION_HAS_NO_BOUNDED_COLUMN =
      "NOT 表达式没有绑定的列";

  // --- Plan / Relational / Metadata ---
  public static final String OBJECT_TYPE_IS_NOT_SUPPORTED_AS_RETURN_TYPE =
      "不支持 OBJECT 类型作为返回值类型";
  public static final String INVALID_FUNCTION_PARAMETERS =
      "无效的函数参数：";
  public static final String UNKNOWN_FUNCTION =
      "未知的函数：";
  public static final String THE_OBJECT_TYPE_COLUMN_IS_NOT_SUPPORTED =
      "不支持 object 类型的列。";
  public static final String NO_COLUMN_OTHER_THAN_TIME_PRESENT_PLEASE_CHECK =
      "除时间列外没有其他列，请检查请求";
  public static final String NO_FIELD_COLUMN_PRESENT_PLEASE_CHECK_THE_REQUEST =
      "没有 Field 列，请检查请求";
  public static final String AUTO_ADD_TABLE_COLUMN_FAILED =
      "自动添加表列失败。";
  public static final String TAG_COLUMN_ONLY_SUPPORT_DATA_TYPE_STRING =
      "标签列仅支持 STRING 数据类型。";
  public static final String ATTRIBUTE_COLUMN_ONLY_SUPPORT_DATA_TYPE_STRING =
      "属性列仅支持 STRING 数据类型。";

  // --- Plan / Relational / Planner ---

  public static final String FAIL_TO_MATERIALIZE_CTE_BECAUSE =
      "物化 CTE 失败，原因：{}";
  public static final String BOTH_OBJECT_MUST_BE_TYPE_OF_NUMBER =
      "两个对象都必须为数值类型";
  public static final String NOT_YET_IMPLEMENTED =
      "尚未实现：";
  public static final String UNSUPPORTED_TYPE_IN_GENERICLITERAL =
      "GenericLiteral 中不支持的类型：";
  public static final String CANNOT_COERCE_TYPE =
      "无法将类型 ";
  public static final String UNKNOWN_TYPE_2 =
      "未知的类型：";
  public static final String NODE_MUST_BE_A_LITERAL =
      "节点必须为 Literal";
  public static final String UNHANDLED_LITERAL_TYPE =
      "未处理的字面量类型：";
  public static final String NO_LITERAL_FORM_FOR_TYPE =
      "类型 %s 没有字面量形式";
  public static final String WINDOW_FRAME_OFFSET_VALUE_MUST_NOT_BE_NEGATIVE =
      "窗口帧偏移值不能为负数或 null";
  public static final String UNEXPECTED_TYPE =
      "意外的类型：";
  public static final String FROM_CLAUSE_MUST_NOT_BE_EMPTY =
      "FROM 子句不能为空";
  public static final String COERCION_RESULT_IN_ANALYSIS_ONLY_CAN_BE_EMPTY =
      "分析中的类型转换结果只能为空";
  public static final String UNEXPECTED_RECURSIVE_CTE =
      "意外的递归 CTE";
  public static final String TABLE =
      "表 ";
  public static final String UNEXPECTED_JOIN_TYPE =
      "意外的 Join 类型：";
  public static final String UNEXPECTED_ROWS_PER_MATCH =
      "意外的 rowsPerMatch：";
  public static final String UNEXPECTED_SKIP_TO_POSITION =
      "意外的 skipTo 位置：";
  public static final String VALUES_IS_NOT_SUPPORTED_IN_CURRENT_VERSION =
      "当前版本不支持 Values。";
  public static final String SUBSCRIPT_IS_NOT_SUPPORTED_IN_CURRENT_VERSION =
      "当前版本不支持下标操作";

  // --- Plan / Relational / Planner / IR ---

  public static final String ILLEGAL_STATE_IN_VISITLOGICALEXPRESSION =
      "visitLogicalExpression 中的非法状态";
  public static final String UNSUPPORTED_LOGICALEXPRESSION_OPERATOR =
      "不支持的逻辑表达式运算符";
  public static final String UNEXPECTED_EXPRESSION =
      "意外的表达式：";
  public static final String FAILED_TO_FETCH_SUBQUERY_RESULT =
      "获取子查询结果失败。";

  // --- Plan / Relational / Planner / Iterative ---

  public static final String UNEXPECTED_PATTERN =
      "意外的 Pattern：";
  public static final String TABLE_FUNCTION_DOES_NOT_SUPPORT_MULTIPLE_SOURCE_NOW =
      "表函数目前不支持多个数据源。";

  // --- Plan / Relational / Planner / Node ---

  public static final String SHOULD_NEVER_PUSH_DOWN_LIMIT_TO_AGGREGATIONTABLESCANNODE =
      "不应将 limit 下推到 AggregationTableScanNode。";
  public static final String SHOULD_NEVER_PUSH_DOWN_OFFSET_TO_AGGREGATIONTABLESCANNODE =
      "不应将 offset 下推到 AggregationTableScanNode。";
  public static final String NOT_SUPPORTED_YET =
      "暂不支持。";
  public static final String COPYTONODE_SHOULD_NOT_BE_SERIALIZED =
      "CopyToNode 不应被序列化";

  // --- Plan / Relational / Planner / Optimizations ---

  public static final String LIST_PLANNODE_SIZE_SHOULD_1_BUT_NOW_IS =
      "List<PlanNode>.size 应 >= 1，但当前为 0";
  public static final String UNSUPPORTED_JOIN_TYPE =
      "不支持的 Join 类型：";
  public static final String TOPK_IS_NOT_SUPPORTED_IN_CORRELATED_SUBQUERY_FOR =
      "目前不支持在关联子查询中使用 TopK";
  public static final String UNEXPECTED_VALUE =
      "意外的值：";

  // --- Plan / Relational / Security ---

  public static final String USER_NOT_EXISTS =
      "用户不存在";
  public static final String ONLY_THE_SUPERUSER_CAN_ALTER_HIM_HERSELF =
      "仅超级用户可以修改自身信息。";
  public static final String DATABASE =
      "数据库 ";
  public static final String TABLE_2 =
      "表 ";
  public static final String UNEXPECTED_VALUE_2 =
      "意外的值：";
  public static final String EACH_OPERATION_SHOULD_HAVE_PERMISSION_CHECK =
      "每个操作都应进行权限检查。";
  public static final String UNKNOWN_AUTHORTYPE =
      "未知的授权类型：";

  // --- Plan / Relational / SQL ---

  public static final String UNKNOWN_AUTHORTYPE_2 =
      "未知的授权类型：";
  public static final String THE_RENAMING_FOR_BASE_TABLE_COLUMN_IS_CURRENTLY =
      "目前不支持重命名基表列";
  public static final String THE_RENAMING_FOR_BASE_TABLE_IS_CURRENTLY_UNSUPPORTED =
      "目前不支持重命名基表";
  public static final String UNEXPECTED_EXPRESSION_2 =
      "意外的表达式：";
  public static final String THE_TABLE_SHOULD_ONLY_HAVE_ONE_COLUMN_FOUND =
      "表中应只有一个 TIME 类别的列";
  public static final String TIMESTAMP_CANNOT_BE_NULL =
      "时间戳不能为 null";
  public static final String SHOW_REGION_ID_IS_NOT_SUPPORTED_YET =
      "暂不支持 SHOW REGION ID。";
  public static final String SHOW_TIME_SLOT_IS_NOT_SUPPORTED_YET =
      "暂不支持 SHOW TIME SLOT。";
  public static final String COUNT_TIME_SLOT_IS_NOT_SUPPORTED_YET =
      "暂不支持 COUNT TIME SLOT。";
  public static final String SHOW_SERIES_SLOT_IS_NOT_SUPPORTED_YET =
      "暂不支持 SHOW SERIES SLOT。";
  public static final String MISSING_LIMIT_VALUE =
      "缺少 LIMIT 值";
  public static final String DATABASE_IS_NOT_SET_YET =
      "尚未设置数据库。";
  public static final String AUTHOR_STATEMENT_PARSER_ERROR =
      "授权语句解析错误";
  public static final String UNSUPPORTED_SET_OPERATION =
      "不支持的集合操作：";
  public static final String UNSUPPORTED_JOIN_CRITERIA =
      "不支持的 join 条件";
  public static final String TOLERANCE_IN_ASOF_JOIN_ONLY_SUPPORTS_INNER_TYPE =
      "ASOF JOIN 中的容差目前仅支持 INNER 类型";
  public static final String UNSUPPORTED_SIGN =
      "不支持的符号：";
  public static final String UNSUPPORTED_WINDOW_FRAME_TYPE =
      "不支持的窗口帧类型：";
  public static final String UNSUPPORTED_BOUNDED_TYPE =
      "不支持的边界类型：";
  public static final String UNSUPPORTED_TRIM_SPECIFICATION =
      "不支持的 TRIM 规范：";
  public static final String TARGET_DATA_IN_SQL_SHOULD_BE_SET_IN =
      "SQL 中的目标数据应在 CREATE MODEL 中设置";
  public static final String THE_TREE_MODEL_DATABASE_SHALL_NOT_BE_SPECIFIED =
      "表模型中不应指定树模型数据库。";
  public static final String UNSUPPORTED_SPECIAL_FUNCTION =
      "不支持的特殊函数：";
  public static final String UNSUPPORTED_ORDERING =
      "不支持的排序方式：";
  public static final String UNSUPPORTED_QUANTIFIER =
      "不支持的量词：";
  public static final String NOT_YET_IMPLEMENTED_WILDCARD_TRANSITION =
      "尚未实现：通配符转换";
  public static final String UNKNOWN_TABLE_ELEMENT =
      "未知的表元素：";

  // --- Plan / Scheduler ---

  public static final String ERROR_HAPPENED_WHILE_FETCHING_QUERY_STATE =
      "获取查询状态时发生错误";
  public static final String INTERRUPTED_WHEN_DISPATCHING_READ_ASYNC =
      "异步分发读取操作时被中断";
  public static final String INTERRUPTED_WHEN_DISPATCHING_WRITE_ASYNC =
      "异步分发写入操作时被中断";
  public static final String DESERIALIZE_CONSENSUSGROUPID_FAILED =
      "反序列化 ConsensusGroupId 失败。";
  public static final String CAN_T_CONNECT_TO_NODE =
      "无法连接到节点 {}";
  public static final String CANCEL_QUERY_ON_NODE_FAILED =
      "在节点 {} 上取消查询 {} 失败。";
  public static final String CANNOT_DISPATCH_FI_FOR_LOAD_OPERATION =
      "无法为加载操作分发 FI";
  public static final String RECEIVE_LOAD_NODE_FROM_UUID =
      "接收来自 uuid {} 的加载节点。";
  public static final String LOAD_TSFILE_NODE_ERROR =
      "加载 TsFile 节点 {} 出错。";
  public static final String SERIALIZE_TSFILERESOURCE_ERROR =
      "序列化 TsFileResource {} 出错。";
  public static final String LOAD_SKIP_TSFILE_BECAUSE_IT_HAS_NO_DATA =
      "跳过加载 TsFile {}，因为没有数据。";
  public static final String LOADTSFILESCHEDULER_LOADS_TSFILE_ERROR =
      "LoadTsFileScheduler 加载 TsFile {} 出错";
  public static final String INTERRUPT_OR_EXECUTION_ERROR =
      "中断或执行错误。";
  public static final String START_DISPATCHING_LOAD_COMMAND_FOR_UUID =
      "开始分发 uuid {} 的加载命令";
  public static final String EXCEPTION_OCCURRED_DURING_SECOND_PHASE_OF_LOADING_TSFILE =
      "加载 TsFile {} 的第二阶段发生异常。";
  public static final String START_LOAD_TSFILE_LOCALLY =
      "开始本地加载 TsFile {}。";
  public static final String LOAD_ALL_FAILED_TSFILES_ARE_CONVERTED_TO_TABLETS =
      "加载：所有失败的 TsFile 已转换为 Tablet 并插入。";

  // --- Plan / Statement ---

  public static final String METHOD_NOT_IMPLEMENTED_YET =
      "方法尚未实现";
  public static final String INSERTION_CONTAINS_DUPLICATED_MEASUREMENT =
      "插入操作包含重复的测量值：";
  public static final String UNSUPPORTED_DATA_TYPE =
      "不支持的数据类型：";
  public static final String FAILED_TO_CONVERT_INSERTTABLETSTATEMENT_TO_TABLET =
      "将 InsertTabletStatement 转换为 Tablet 失败";
  public static final String MODEL_INFERENCE_DOES_NOT_SUPPORT_ALIGN_BY_DEVICE =
      "模型推理目前不支持按设备对齐。";
  public static final String MODEL_INFERENCE_DOES_NOT_SUPPORT_SELECT_INTO_NOW =
      "模型推理目前不支持 SELECT INTO。";
  public static final String GROUP_BY_CLAUSES_DOESN_T_SUPPORT_GROUP_BY =
      "GROUP BY 子句目前不支持 GROUP BY LEVEL。";
  public static final String GROUP_BY_LEVEL_DOES_NOT_SUPPORT_ALIGN_BY =
      "GROUP BY LEVEL 目前不支持按设备对齐。";
  public static final String GROUP_BY_TAGS_DOES_NOT_SUPPORT_ALIGN_BY =
      "GROUP BY TAGS 目前不支持按设备对齐。";
  public static final String HAVING_CLAUSE_IS_NOT_SUPPORTED_YET_IN_GROUP =
      "GROUP BY TAGS 查询中暂不支持 HAVING 子句";
  public static final String OUTPUT_COLUMN_IS_DUPLICATED_WITH_THE_TAG_KEY =
      "输出列与标签键重复：";
  public static final String LIMIT_OR_SLIMIT_ARE_NOT_SUPPORTED_YET_IN =
      "GROUP BY TAGS 中暂不支持 LIMIT 或 SLIMIT";
  public static final String EXPRESSION_OF_HAVING_CLAUSE_MUST_TO_BE_AN =
      "HAVING 子句的表达式必须是聚合函数";
  public static final String WHEN_HAVING_USED_WITH_GROUPBYLEVEL =
      "当 HAVING 与 GroupByLevel 一起使用时：";
  public static final String ALIGN_BY_DEVICE =
      "按设备对齐：";
  public static final String SORTING_BY_TIMESERIES_IS_ONLY_SUPPORTED_IN_LAST =
      "按时间序列排序仅在 LAST 查询中支持。";
  public static final String LAST_QUERY_DOESN_T_SUPPORT_ALIGN_BY_DEVICE =
      "LAST 查询不支持按设备对齐。";
  public static final String LAST_QUERIES_CAN_ONLY_BE_APPLIED_ON_RAW =
      "LAST 查询只能应用于原始时间序列。";
  public static final String SLIMIT_AND_SOFFSET_CAN_NOT_BE_USED_IN =
      "LAST 查询中不能使用 SLIMIT 和 SOFFSET。";
  public static final String SELECT_INTO_SLIMIT_CLAUSES_ARE_NOT_SUPPORTED =
      "SELECT INTO：不支持 SLIMIT 子句。";
  public static final String SELECT_INTO_SOFFSET_CLAUSES_ARE_NOT_SUPPORTED =
      "SELECT INTO：不支持 SOFFSET 子句。";
  public static final String SELECT_INTO_LAST_CLAUSES_ARE_NOT_SUPPORTED =
      "SELECT INTO：不支持 LAST 子句。";
  public static final String SELECT_INTO_GROUP_BY_TAGS_CLAUSE_ARE_NOT =
      "SELECT INTO：不支持 GROUP BY TAGS 子句。";
  public static final String UNKNOWN_LITERAL_TYPE =
      "未知的字面量类型：%s";
  public static final String ILLEGAL_PATH =
      "非法路径：{}";
  public static final String CQ_THE_START_TIME_OFFSET_SHOULD_BE_GREATER =
      "连续查询：起始时间偏移量应大于 0。";
  public static final String CQ_THE_END_TIME_OFFSET_SHOULD_BE_GREATER =
      "连续查询：结束时间偏移量应大于或等于 0。";
  public static final String CQ_THE_QUERY_BODY_MISSES_AN_INTO_CLAUSE =
      "连续查询：查询体缺少 INTO 子句。";
  public static final String CQ_SPECIFYING_TIME_FILTERS_IN_THE_QUERY_BODY =
      "连续查询：禁止在查询体中指定时间过滤器。";
  public static final String IS_NOT_A_LEGAL_PATH =
      "{} 不是合法路径";

  // --- Plan / Tree Planner ---

  public static final String VALID_TREEDEVICEVIEWSCANNODE_IS_NOT_EXPECTED_HERE =
      "此处不应出现有效的 TreeDeviceViewScanNode。";
  public static final String MULTIPLE_COLUMNS_WITH_TIME_CATEGORY_FOUND =
      "发现多个 TIME 类别的列";
  public static final String MISSING_TIME_CATEGORY_COLUMN =
      "缺少 TIME 类别的列";
  public static final String UNKNOWN_SQL_DIALECT =
      "未知的 SQL 方言：%s";
  public static final String UNEXPECTED_PATH_TYPE_2 =
      "意外的路径类型";
  public static final String SHOULD_CALL_THE_CONCRETE_VISITXX_METHOD =
      "应调用具体的 visitXX() 方法";
  public static final String OUTPUTCOLUMTYPES_SHOULD_NOT_BE_NULL_EMPTY =
      "OutputColumTypes 不应为 null 或空";
  public static final String UNKNOWN_FILL_POLICY =
      "未知的填充策略：";
  public static final String FILTER_CAN_NOT_CONTAIN_NON_MAPPABLE_UDF =
      "过滤器不能包含非映射型 UDF";
  public static final String GROUPBYVARIATIONEXPRESSION_CAN_T_BE_NULL =
      "groupByVariationExpression 不能为 null";
  public static final String GROUPBYCONDITIONEXPRESSION_CAN_T_BE_NULL =
      "groupByConditionExpression 不能为 null";
  public static final String GROUPBYCOUNTEXPRESSION_CAN_T_BE_NULL =
      "groupByCountExpression 不能为 null";
  public static final String UNKNOWN_NODE_TYPE =
      "未知的节点类型：";
  public static final String UNSUPPORTED_COLUMN_GENERATOR_TYPE =
      "不支持的列生成器类型：";
  public static final String ROOT_NODE_MUST_RETURN_ONLY_ONE =
      "根节点必须只返回一个结果";
  public static final String SINGLEDEVICEVIEWNODE_HAVE_ONLY_ONE_CHILD =
      "SingleDeviceViewNode 只有一个子节点";
  public static final String AVAILABLE_REPLICAS =
      "可用副本：{}";
  public static final String UNEXPECTED_ERROR_OCCURS_WHEN_SERIALIZING_THIS_FRAGMENTINSTANCE =
      "序列化此 FragmentInstance 时发生意外错误。";
  public static final String INVALID_NODE_TYPE =
      "无效的节点类型：";
  public static final String THIS_LASTQUERYSCANNODE_IS_DEPRECATED =
      "此 LastQueryScanNode 已弃用";
  public static final String EXPLAINANALYZENODE_SHOULD_NOT_BE_SERIALIZED =
      "ExplainAnalyzeNode 不应被序列化";
  public static final String EXPLAINANALYZENODE_SHOULD_NOT_BE_DESERIALIZED =
      "ExplainAnalyzeNode 不应被反序列化";
  public static final String CLONE_OF_LOAD_SINGLE_TSFILE_IS_NOT_IMPLEMENTED =
      "单 TsFile 加载的 clone 未实现";
  public static final String SPLIT_LOAD_SINGLE_TSFILE_IS_NOT_IMPLEMENTED =
      "单 TsFile 加载的 split 未实现";
  public static final String DELETE_AFTER_LOADING_ERROR =
      "加载后删除 {} 出错。";
  public static final String CLONE_OF_LOAD_TSFILE_IS_NOT_IMPLEMENTED =
      "TsFile 加载的 clone 未实现";
  public static final String LOADTSFILE_STATEMENT_IS_NULL_DURING_TABLE_MODEL_SPLIT =
      "表模型拆分期间 LoadTsFile 语句为 null。";
  public static final String CLONE_OF_LOAD_PIECE_TSFILE_IS_NOT_IMPLEMENTED =
      "TsFile 分片加载的 clone 未实现";
  public static final String SERIALIZE_TO_BYTEBUFFER_ERROR =
      "序列化到 ByteBuffer 出错。";
  public static final String SPLIT_LOAD_PIECE_TSFILE_IS_NOT_IMPLEMENTED =
      "TsFile 分片加载的 split 未实现";
  public static final String DESERIALIZE_ERROR =
      "反序列化 {} 出错。";
  public static final String INVALID_LENGTH_FOR_SLICING =
      "无效的切片长度：";
  public static final String CANNOT_DESERIALIZE_DEVICESSCHEMASCANNODE =
      "无法反序列化 DevicesSchemaScanNode";
  public static final String CANNOT_DESERIALIZE_TIMESERIESSCHEMASCANNODE =
      "无法反序列化 TimeSeriesSchemaScanNode";
  public static final String CLONE_OF_ALTERTIMESERIESNODE_IS_NOT_IMPLEMENTED =
      "AlterTimeSeriesNode 的 clone 未实现";
  public static final String CAN_NOT_DESERIALIZE_ALTERTIMESERIESNODE =
      "无法反序列化 AlterTimeSeriesNode";
  public static final String CLONE_OF_CREATEALIGNEDTIMESERIESNODE_IS_NOT_IMPLEMENTED =
      "CreateAlignedTimeSeriesNode 的 clone 未实现";
  public static final String CAN_NOT_DESERIALIZE_CREATEALIGNEDTIMESERIESNODE =
      "无法反序列化 CreateAlignedTimeSeriesNode";
  public static final String CLONE_OF_CREATEMULTITIMESERIESNODE_IS_NOT_IMPLEMENTED =
      "CreateMultiTimeSeriesNode 的 clone 未实现";
  public static final String CLONE_OF_CREATETIMESERIESNODE_IS_NOT_IMPLEMENTED =
      "CreateTimeSeriesNode 的 clone 未实现";
  public static final String CANNOT_DESERIALIZE_CREATETIMESERIESNODE =
      "无法反序列化 CreateTimeSeriesNode";
  public static final String CLONE_OF_INTERNALCREATETIMESERIESNODE_IS_NOT_IMPLEMENTED =
      "InternalCreateTimeSeriesNode 的 clone 未实现";
  public static final String CLONE_OF_ALTERLOGICALNODE_IS_NOT_IMPLEMENTED =
      "AlterLogicalNode 的 clone 未实现";
  public static final String UNEXPECTED_DESCRIPTORTYPE =
      "意外的 descriptorType：";
  public static final String NO_CHILD_IS_ALLOWED_FOR_ALIGNEDSERIESSCANNODE =
      "AlignedSeriesScanNode 不允许有子节点";
  public static final String DEVICEREGIONSCANNODE_HAS_NO_CHILDREN =
      "DeviceRegionScanNode 没有子节点";
  public static final String NO_CHILD_IS_ALLOWED_FOR_SERIESSCANNODE =
      "SeriesScanNode 不允许有子节点";
  public static final String NO_CHILD_IS_ALLOWED_FOR_SERIESAGGREGATESCANNODE =
      "SeriesAggregateScanNode 不允许有子节点";
  public static final String NO_CHILD_IS_ALLOWED_FOR_SERIESSCANSOURCENODE =
      "SeriesScanSourceNode 不允许有子节点";
  public static final String NO_CHILD_IS_ALLOWED_FOR_SHOWDISKUSAGENODE =
      "ShowDiskUsageNode 不允许有子节点";
  public static final String NO_CHILD_IS_ALLOWED_FOR_SHOWQUERIESNODE =
      "ShowQueriesNode 不允许有子节点";
  public static final String TIMESERIESREGIONSCANNODE_DOES_NOT_SUPPORT_ADDCHILD =
      "TimeseriesRegionScanNode 不支持 addChild";
  public static final String NOT_SUPPORTED =
      "不支持。";
  public static final String CANNOT_DESERIALIZE_INSERTROWNODE =
      "无法反序列化 InsertRowNode";
  public static final String UNEXPECTED_ERROR_OCCURS_WHEN_SERIALIZING_DELETEDATANODE =
      "序列化 deleteDataNode 时发生意外错误。";
  public static final String DELETEDATANODES_IS_EMPTY =
      "deleteDataNodes 为空";
  public static final String INSERTMULTITABLETSNODE_NOT_SUPPORT_MERGE =
      "InsertMultiTabletsNode 不支持合并";
  public static final String CLONE_OF_INSERT_IS_NOT_IMPLEMENTED =
      "Insert 的 clone 未实现";
  public static final String INSERTNODES_SHOULD_NEVER_BE_EMPTY =
      "insertNodes 不应为空";
  public static final String SERIALIZEATTRIBUTES_OF_INSERTNODE_IS_NOT_IMPLEMENTED =
      "InsertNode 的 serializeAttributes 未实现";
  public static final String INSERTROWSOFONEDEVICENODE_NOT_SUPPORT_MERGE =
      "InsertRowsOfOneDeviceNode 不支持合并";
  public static final String CANNOT_DESERIALIZE_INSERTROWSOFONEDEVICENODE =
      "无法反序列化 InsertRowsOfOneDeviceNode";
  public static final String CANNOT_DESERIALIZE_INSERTTABLETNODE =
      "无法反序列化 InsertTabletNode";
  public static final String MERGE_IS_NOT_SUPPORTED =
      "不支持合并";
  public static final String FAILED_TO_SERIALIZE_MODENTRY_TO_WAL =
      "将 modEntry 序列化到 WAL 失败";
  public static final String ALL_DATABASE_NAME_NEED_TO_BE_SAME =
      "所有数据库名称必须相同";
  public static final String INVALID_AGGREGATIONSTEP_TYPE =
      "无效的 AggregationStep 类型：";

  // --- Transformation ---

  public static final String SIZE_IS_0 =
      "大小为 0";
  public static final String CAN_NOT_CALL_NEXT_ON_EMPTYROWITERATOR =
      "不能在 EmptyRowIterator 上调用 next";
  public static final String THE_EXPRESSION_CANNOT_BE_NULL =
      "表达式不能为 null";
  public static final String UNSUPPORTED_TYPE =
      "不支持的类型：";
  public static final String UNSUPPORTED_DATA_TYPE_2 =
      "不支持的数据类型：";
  public static final String UNSUPPORTED_DATA_TYPE_3 =
      "不支持的数据类型：";
  public static final String ERROR_OCCURRED_DURING_INFERRING_UDF_DATA_TYPE =
      "推断 UDF 数据类型时发生错误";
  public static final String ERROR_OCCURRED_DURING_GETTING_UDF_ACCESS_STRATEGY =
      "获取 UDF 访问策略时发生错误";
  public static final String TRANSFORMUTILS_SHOULD_NOT_BE_INSTANTIATED =
      "TransformUtils 不应被实例化。";

  // --- Execution / Exchange (additional) ---

  public static final String ACK_TSBLOCK_FAILED =
      "确认 TsBlock [{}, {}) 失败。";
  public static final String CLOSE_CHANNEL_OF_SHUFFLESINKHANDLE_FAILED =
      "关闭 ShuffleSinkHandle {} 的通道（索引 {}）失败。";
  public static final String SHUFFLESINKHANDLE_ALREADY_IN_MAP =
      "ShuffleSinkHandle ";
  public static final String IS_IN_THE_MAP =
      " 已存在于映射中。";
  public static final String SOURCE_HANDLE_FOR_PLAN_NODE_EXISTS_FMT =
      "计划节点 %s 的 %s 的 Source handle 已存在。";
  public static final String FAILED_TO_PULL_TSBLOCKS =
      "{} 从 SinkHandle {} 的通道索引 {} 拉取 TsBlocks [{}] 到 [{}] 失败，";
  public static final String FAILED_TO_GET_DATA_BLOCK =
      "获取数据块 [{}, {}) 失败，尝试次数：{}";
  public static final String FAILED_TO_SEND_ACK_DATA_BLOCK_EVENT =
      "发送数据块确认事件 [{}, {}) 失败，尝试次数：{}";
  public static final String SEND_CLOSE_SINK_CHANNEL_EVENT_FAILED =
      "[发送关闭SinkChannel事件] 到 [ShuffleSinkHandle: {}, 索引: {}] 失败。";
  public static final String LOCAL_SINK_CHANNEL_STATE_IS =
      "LocalSinkChannel 状态为 .";
  public static final String SCH_LISTENER_ON_FINISH =
      "[ScH监听器完成]";
  public static final String SCH_LISTENER_ALREADY_RELEASED =
      "[ScH监听器已释放]";
  public static final String SCH_LISTENER_ON_ABORT =
      "[ScH监听器中止]";
  public static final String SHUFFLE_SINK_HANDLE_LISTENER_ON_FINISH =
      "[ShuffleSinkHandle监听器完成]";
  public static final String SHUFFLE_SINK_HANDLE_LISTENER_ON_END_OF_TSBLOCKS =
      "[ShuffleSinkHandle监听器TsBlock结束]";
  public static final String SHUFFLE_SINK_HANDLE_LISTENER_ON_ABORT =
      "[ShuffleSinkHandle监听器中止]";
  public static final String SKH_LISTENER_ON_FINISH =
      "[SkH监听器完成]";
  public static final String SKH_LISTENER_ON_END_OF_TSBLOCKS =
      "[SkH监听器TsBlock结束]";
  public static final String SKH_LISTENER_ON_ABORT =
      "[SkH监听器中止]";
  public static final String CLOSE_SHUFFLE_SINK_HANDLE =
      "关闭 ShuffleSinkHandle: {}";
  public static final String GET_SHARED_TSBLOCK_QUEUE_FROM_LOCAL_SOURCE_HANDLE =
      "从本地源句柄获取 SharedTsBlockQueue";
  public static final String CREATE_SHARED_TSBLOCK_QUEUE =
      "创建 SharedTsBlockQueue";
  public static final String CREATE_LOCAL_SINK_HANDLE_FOR =
      "为 {} 创建本地 Sink 句柄";
  public static final String CREATE_LOCAL_SOURCE_HANDLE_FOR =
      "为 {} 创建本地 Source 句柄";
  public static final String GET_SHARED_TSBLOCK_QUEUE_FROM_LOCAL_SINK_HANDLE =
      "从本地 Sink 句柄获取 SharedTsBlockQueue";
  public static final String START_FORCE_RELEASE_FI_DATA_EXCHANGE_RESOURCE =
      "[开始强制释放FI数据交换资源]";
  public static final String CLOSE_SOURCE_HANDLE =
      "[关闭SourceHandle] {}";
  public static final String END_FORCE_RELEASE_FI_DATA_EXCHANGE_RESOURCE =
      "[结束强制释放FI数据交换资源]";
  public static final String CREATE_LOCAL_SINK_HANDLE_TO_PLAN_NODE =
      "为计划节点 {} 的 {} 创建本地 Sink 句柄，目标 {}";
  public static final String CREATE_SINK_HANDLE_TO_PLAN_NODE =
      "为计划节点 {} 的 {} 创建 Sink 句柄，目标 {}";
  public static final String CREATE_LOCAL_SOURCE_HANDLE_FROM =
      "从 {} 为计划节点 {} 的 {} 创建本地 Source 句柄";
  public static final String GET_SERIALIZED_TSBLOCK =
      "[获取序列化TsBlock] TsBlock:{}";
  public static final String START_ABORT_LOCAL_SOURCE_HANDLE =
      "[开始中止LocalSourceHandle]";
  public static final String END_ABORT_LOCAL_SOURCE_HANDLE =
      "[结束中止LocalSourceHandle]";
  public static final String START_CLOSE_LOCAL_SOURCE_HANDLE =
      "[开始关闭LocalSourceHandle]";
  public static final String END_CLOSE_LOCAL_SOURCE_HANDLE =
      "[结束关闭LocalSourceHandle]";
  public static final String START_SET_NO_MORE_TSBLOCKS =
      "[开始设置无更多TsBlock]";
  public static final String START_ABORT_SINK_CHANNEL =
      "[开始中止SinkChannel]";
  public static final String END_ABORT_SINK_CHANNEL =
      "[结束中止SinkChannel]";
  public static final String START_CLOSE_SINK_CHANNEL =
      "[开始关闭SinkChannel]";
  public static final String END_CLOSE_SINK_CHANNEL =
      "[结束关闭SinkChannel]";
  public static final String ACK_TSBLOCK =
      "[确认TsBlock] {}.";
  public static final String NOTIFY_NO_MORE_TSBLOCK =
      "[通知无更多TsBlock]";
  public static final String START_SEND_TSBLOCK_ON_LOCAL =
      "[开始在本地发送TsBlock]";
  public static final String START_SET_NO_MORE_TSBLOCKS_ON_LOCAL =
      "[开始在本地设置无更多TsBlock]";
  public static final String END_SET_NO_MORE_TSBLOCKS_ON_LOCAL =
      "[结束在本地设置无更多TsBlock]";
  public static final String START_ABORT_LOCAL_SINK_CHANNEL =
      "[开始中止LocalSinkChannel]";
  public static final String END_ABORT_LOCAL_SINK_CHANNEL =
      "[结束中止LocalSinkChannel]";
  public static final String START_CLOSE_LOCAL_SINK_CHANNEL =
      "[开始关闭LocalSinkChannel]";
  public static final String END_CLOSE_LOCAL_SINK_CHANNEL =
      "[结束关闭LocalSinkChannel]";
  public static final String GET_TSBLOCK_FROM_BUFFER =
      "[从缓冲区获取TsBlock] sequenceId:{}, size:{}";
  public static final String WAIT_FOR_MORE_TSBLOCK =
      "[等待更多TsBlock]";
  public static final String RECEIVE_NO_MORE_TSBLOCK_EVENT =
      "[收到无更多TsBlock事件]";
  public static final String END_PULL_TSBLOCKS_FROM_REMOTE =
      "[结束从远端拉取TsBlock] 数量:{}";
  public static final String PUT_TSBLOCKS_INTO_BUFFER =
      "[将TsBlock放入缓冲区]";
  public static final String SEND_ACK_TSBLOCK =
      "[发送确认TsBlock] [{}, {}).";
  public static final String START_ABORT_SHUFFLE_SINK_HANDLE =
      "[开始中止ShuffleSinkHandle]";
  public static final String END_ABORT_SHUFFLE_SINK_HANDLE =
      "[结束中止ShuffleSinkHandle]";
  public static final String START_CLOSE_SHUFFLE_SINK_HANDLE =
      "[开始关闭ShuffleSinkHandle]";
  public static final String END_CLOSE_SHUFFLE_SINK_HANDLE =
      "[结束关闭ShuffleSinkHandle]";
  public static final String SIGNAL_NO_MORE_TSBLOCK_ON_QUEUE =
      "[队列信号无更多TsBlock]";
  public static final String QUEUE_DESTROYED_WHEN_SET_NO_MORE_TSBLOCKS =
      "调用 setNoMoreTsBlocks 时队列已被销毁。";
  public static final String ADD_TSBLOCK =
      "[添加TsBlock] TsBlock:{}";

  // --- Plan (additional debug) ---

  public static final String QUERY_START_SQL =
      "[查询开始] sql: {}";
  public static final String CLEAN_UP_QUERY =
      "[清理查询]";
  public static final String RELEASE_QUERY_RESOURCE_STATE =
      "[释放查询资源] 状态为: {}";
  public static final String SKIP_EXECUTE =
      "[跳过执行]";
  public static final String SKIP_EXECUTE_AFTER_LOGICAL_PLAN =
      "[逻辑计划后跳过执行]";
  public static final String RESULT_HANDLE_FINISHED =
      "[结果句柄已完成]";

  // --- Execution / Operator / Source (additional debug) ---

  public static final String SERIES_SCAN_UTIL_PAGE_READER_IS_MODIFIED =
      "[SeriesScanUtil] pageReader.isModified() 为 {}";
  public static final String GET_ALL_SATISFIED_PAGE_DATA_TSBLOCK =
      "[getAllSatisfiedPageData] TsBlock:{}";

  // --- Plan / Relational / Metadata (additional debug) ---

  public static final String DEVICES_ARE_MISSING =
      "{} 个设备缺失";

  // --- Execution / Fragment (additional debug) ---

  public static final String STATE_CHANGED_TO =
      "[状态变更] 变更为 {}";
  public static final String ENTER_THE_STATE_CHANGE_LISTENER =
      "进入状态变更监听器";

  // --- Execution / Fragment (additional) ---

  public static final String ERRORS_RELEASING_SINK =
      "尝试释放 Sink 时发生错误，可能导致资源泄漏。";
  public static final String ERRORS_DELETING_TMP_FILES =
      "尝试删除临时文件时发生错误，可能导致资源泄漏。";
  public static final String ERRORS_DEREGISTER_FI_FROM_MEMORY_POOL =
      "尝试从内存池注销分片实例时发生错误，可能导致资源泄漏，状态为 {}。";
  public static final String ERRORS_RELEASING_MEMORY =
      "尝试释放内存时发生错误，可能导致资源泄漏。";
  public static final String ERRORS_FINISHING_FI_PROCESS =
      "尝试完成分片实例流程时发生错误，可能导致资源泄漏。";

  // --- Plan (additional) ---

  public static final String CLEANING_UP_STALE_QUERY =
      "正在清理过期查询，ID: {}，已运行 {} 毫秒，超时时间：{} 毫秒";

  // --- Plan / Tree Planner (additional) ---

  public static final String ERROR_WHEN_READ_OBJECT_FILE =
      "读取对象文件 {} 时出错。";

  // --- Additional Edge Cases ---

  public static final String JOIN_TYPE_IS_NOT_SUPPORTED =
      " Join 类型不受支持";
  public static final String COLON_S_VS_S =
      "：%s 与 %s";
  public static final String IS_TOO_LARGE_STACK_OVERFLOW_WHILE_PARSING =
      " 过大（解析时发生栈溢出）";

  public static final String ENTER_STATE_CHANGE_LISTENER = "进入状态变更监听器";

  // --- Analyzer / Planner ---
  public static final String NO_VALUE_PRESENT = "值不存在";
  public static final String THE_INPUT_FIELD_DOES_NOT_EXIST = "输入字段不存在";
  public static final String THE_FIELD_IN_TABLE_DOES_NOT_HAVE_A_NAME =
      "表中的字段没有名称";
  public static final String SHOULD_HAVE_TWO_NUMERIC_OPERANDS =
      "应有两个数值类型操作数。";
  public static final String SHOULD_HAVE_ONE_NUMERIC_OPERANDS =
      "应有一个数值类型操作数。";
  public static final String SHOULD_HAVE_TWO_COMPARABLE_OPERANDS =
      "应有两个可比较类型操作数。";
  public static final String JOIN_USING_CRITERIA_IS_EMPTY = "JoinUsing 条件为空";
  public static final String S_IS_NOT_A_TABLE_REFERENCE = "%s 不是表引用";

    public static final String TOPOLOGY_DATANODE_REACHABILITY_CHANGED =
      "[Topology] DataNode {} 现在对 myself({}) 为 {}";
  public static final String NO_MAPPING_FOR_S =
      "找不到 %s 的映射";
  public static final String CANCEL_STATE_TRACKING_TASK_FAILED =
      "取消状态跟踪任务失败。{}";
  public static final String TRACK_TASK_NOT_STARTED =
      "trackTask 未启动";
  public static final String PRINT_FI_STATE =
      "[PrintFIState] 状态为 {}";
  public static final String START_FETCH_SCHEMA =
      "[StartFetchSchema]";
  public static final String END_FETCH_SCHEMA =
      "[EndFetchSchema]";
  public static final String CACHE_HIT =
      "[{} Cache] 命中";
  public static final String PARTITION_CACHE_INVALID =
      "[Partition Cache] 无效";
  public static final String PARTITION_CACHE_IS_INVALID =
      "[Partition Cache] 无效：{}";
  public static final String CANCEL_FI =
      "[CancelFI]";
  public static final String RENAME_VIEW_NOT_SUPPORT_WILDCARD =
      "重命名视图不支持带通配符的路径模式。";
  public static final String REMOVE_CONFIG_NODE_FAILED =
      "移除 ConfigNode 失败：";

  public static final String CANT_CONNECT_TO_NODE_PREFIX = "无法连接到节点 ";
  public static final String REMOVE_AINODE_FAILED = "移除 AINode 失败：";

  public static final String QUERY_TIMEOUT_IN_FETCH_SCHEMA = "查询在拉取元数据时，执行超时";

  public static final String QUERY_EXECUTION_MISSING = "查询执行实体 %s 在拉取元数据期间丢失";


  // --- QueryEngine semantic messages (additional) ---
  public static final String PREPARED_STATEMENT_S_DOES_NOT_EXIST =
      "预编译语句 '%s' 不存在";
  public static final String CALL_INFERENCE_FUNCTION_SHOULD_NOT_CONTAIN_MORE_THAN_ONE_INPUT_COLUMN_FOUND_D_INPUT =
      "Call inference 函数 应 不 contain more than one input 列, found [%d] input 列s.";
  public static final String DATA_TYPE_OF_TAG_COLUMN =
      "数据类型 的 tag 列 ";
  public static final String IS_NOT_STRING =
      " 为 不 STRING";
  public static final String THE_SOURCE_PATHS_S_OF_VIEW_S_ARE_MULTIPLE =
      "source 路径s [%s] 的 view [%s] 为 multiple.";
  public static final String ERROR_OCCURRED_DURING_INFERRING_UDF_DATA_TYPE_S =
      "发生错误 during inferring UDF 数据类型: %s";
  public static final String ERROR_OCCURRED_DURING_GETTING_UDF_ACCESS_STRATEGY_S =
      "发生错误 during getting UDF access strategy: %s";
  public static final String UNSUPPORTED_COMPRESSION_S =
      "不支持 compression: %s";
  public static final String TIMESERIES_CONDITION_AND_TIME_CONDITION_CANNOT_BE_USED_AT_THE_SAME_TIME =
      "TIMESERIES 条件和 TIME 条件不能同时使用。";
  public static final String LATEST_AND_ORDER_BY_TIMESERIES_CANNOT_BE_USED_AT_THE_SAME_TIME =
      "LATEST 和 ORDER BY TIMESERIES 不能同时使用。";
  public static final String DEVICE_CONDITION_AND_TIME_CONDITION_CANNOT_BE_USED_AT_THE_SAME_TIME =
      "DEVICE 条件和 TIME 条件不能同时使用。";
  public static final String TIME_CONDITION_AND_GROUP_BY_LEVEL_CANNOT_BE_USED_AT_THE_SAME_TIME =
      "TIME 条件和 GROUP BY LEVEL 不能同时使用。";
  public static final String CQ_AT_LEAST_ONE_OF_THE_PARAMETERS_EVERY_INTERVAL_AND_GROUP_BY_INTERVAL_NEEDS_TO_BE =
      "CQ: At least one 的 参数s `every_interval` 和 `group_by_interval` needs 到 be specified.";
  public static final String CAN_NOT_USE_CHAR_DOLLAR_OR_INTO_ITEM_IN_ALTER_VIEW_STATEMENT =
      "不能 use char '$' 或 into item 在 alter view 语句.";
  public static final String TIME_COLUMN_IS_NO_NEED_TO_APPEAR_IN_SELECT_CLAUSE_EXPLICITLY_IT_WILL_ALWAYS_BE_RETURNED =
      "时间 列 为 no need 到 appear 在 SELECT Clause explicitly, it will always be returned if possible";
  public static final String THE_SECOND_PARAMETER_TIME_INTERVAL_SHOULD_BE_A_POSITIVE_INTEGER =
      "second 参数 时间 interval 应为 positive integer.";
  public static final String THE_THIRD_PARAMETER_TIME_SLIDINGSTEP_SHOULD_BE_A_POSITIVE_INTEGER =
      "third 参数 时间 slidingStep 应为 positive integer.";
  public static final String CONSTANT_OPERAND_S_IS_NOT_ALLOWED_IN_GROUP_BY_VARIATION_THERE_SHOULD_BE_AN_EXPRESSION =
      "Constant operand [%s] 为 不允许 在 group 由 variation, there 应为 表达式";
  public static final String CONSTANT_OPERAND_S_IS_NOT_ALLOWED_IN_GROUP_BY_COUNT_THERE_SHOULD_BE_AN_EXPRESSION =
      "Constant operand [%s] 为 不允许 在 group 由 count, there 应为 表达式";
  public static final String ORDER_BY_SORT_KEY_S_IS_NOT_CONTAINED_IN_S =
      "ORDER BY: sort key[%s] 为 不 contained 在 '%s'";
  public static final String ORDER_BY_EXPRESSION_IS_NOT_SUPPORTED_FOR_CURRENT_STATEMENT_SUPPORTED_SORT_KEY =
      "ORDER BY 表达式 为 不支持 用于 当前 语句, supported sort key: ";
  public static final String ONLY_FILL_PREVIOUS_SUPPORT_SPECIFYING_THE_TIME_DURATION_THRESHOLD =
      "只有 FILL(PREVIOUS) 支持指定时间持续阈值。";
  public static final String OUT_OF_RANGE_OFFSET_LT_OFFSETVALUE_GT_OFFSETVALUE_SHOULD_BE_INT64 =
      "Out 的 range. OFFSET <OFFSET值>: OFFSET值 应为 Int64.";
  public static final String OUT_OF_RANGE_SOFFSET_LT_SOFFSETVALUE_GT_SOFFSETVALUE_SHOULD_BE_INT32 =
      "Out 的 range. SOFFSET <SOFFSET值>: SOFFSET值 应为 Int32.";
  public static final String FAILED_TO_PARSE_THE_TIMESTAMP =
      "未能 parse 时间stamp: ";
  public static final String CURRENT_SYSTEM_TIMESTAMP_PRECISION_IS_S =
      "Current system 时间stamp precision 为 %s, ";
  public static final String PLEASE_CHECK_WHETHER_THE_TIMESTAMP_S_IS_CORRECT =
      "请检查 whether 时间stamp %s 为 correct.";
  public static final String LOAD_TSFILE_FORMAT_S_ERROR_PLEASE_INPUT_AUTOREGISTER_SGLEVEL_VERIFY =
      "Load tsfile format %s error, 请输入 AUTOREGISTER | SGLEVEL | VERIFY.";
  public static final String S_IS_ILLEGAL_UNQUOTED_NODE_NAME_CAN_ONLY_CONSIST_OF_DIGITS_CHARACTERS_AND_UNDERSCORE_OR =
      "%s 为 illegal, unquoted node name 只能 consist 的 digits, characters 和 underscore, 或 start 或 end 与 wildcard";
  public static final String S_IS_ILLEGAL_UNQUOTED_NODE_NAME_IN_SELECT_INTO_CLAUSE_CAN_ONLY_CONSIST_OF_DIGITS =
      "%s 为 illegal, unquoted node name 在 select into clause 只能 consist 的 digits, characters, $, { 和 }";
  public static final String S_IS_ILLEGAL_IDENTIFIER_NOT_ENCLOSED_WITH_BACKTICKS_CAN_ONLY_CONSIST_OF_DIGITS =
      "%s 为 illegal, identifier 不 enclosed 与 backticks 只能 consist 的 digits, characters 和 underscore.";
  public static final String INPUT_TIME_FORMAT_S_ERROR =
      "Input 时间 format %s error. ";
  public static final String INPUT_LIKE_YYYY_MM_DD_HH_MM_SS_YYYY_MM_DDTHH_MM_SS_OR =
      "Input like yyyy-MM-dd HH:mm:ss, yyyy-MM-ddTHH:mm:ss 或 ";
  public static final String REFER_TO_USER_DOCUMENT_FOR_MORE_INFO =
      "refer 到 user document 用于 more info.";
  public static final String GRANT_OPTION_IS_DISABLED_PLEASE_CHECK_THE_PARAMETER_ENABLE_GRANT_OPTION =
      "Grant Option 已禁用，请检查参数 enable_grant_option。";
  public static final String S_CAN_ONLY_BE_SET_ON_PATH_ROOT_STAR_STAR =
      "[%s] 只能 be set on 路径: root.**";
  public static final String PRIVILEGE_TYPE =
      "权限 类型 ";
  public static final String IS_DEPRECATED_USE =
      " 为 deprecated, use ";
  public static final String TO_INSTEAD_IT =
      " 到 instead it";
  public static final String INVALID_FUNCTION_EXPRESSION_ALL_THE_ARGUMENTS_ARE_CONSTANT_OPERANDS =
      "无效 函数 表达式, all 参数s 为 constant operands: ";
  public static final String ERROR_SIZE_OF_INPUT_EXPRESSIONS_EXPRESSION_S_ACTUAL_SIZE_S_EXPECTED_SIZE_S =
      "Error size 的 input 表达式s. 表达式: %s, 实际 size: %s, 期望 size: %s.";
  public static final String CAN_NOT_PARSE_S_TO_LONG_VALUE =
      "不能 parse %s 到 long 值";
  public static final String THERE_S_DUPLICATE_S_IN_TAG_OR_ATTRIBUTE_CLAUSE =
      "There's 重复 [%s] 在 tag 或 attribute clause.";
  public static final String NOT_SUPPORT_FOR_THIS_SQL_IN_CREATE_PIPE_PLEASE_ENTER_PIPE_NAME =
      "Not support 用于 this sql 在 CREATE PIPE, please enter pipe name.";
  public static final String NOT_SUPPORT_FOR_THIS_SQL_IN_ALTER_PIPE_PLEASE_ENTER_PIPE_NAME =
      "Not support 用于 this sql 在 ALTER PIPE, please enter pipe name.";
  public static final String NOT_SUPPORT_FOR_THIS_SQL_IN_CREATE_TOPIC_PLEASE_ENTER_TOPICNAME =
      "Not support 用于 this sql 在 CREATE TOPIC, please enter topicName.";
  public static final String NOT_SUPPORT_FOR_THIS_SQL_IN_DROP_TOPIC_PLEASE_ENTER_TOPICNAME =
      "Not support 用于 this sql 在 DROP TOPIC, please enter topicName.";
  public static final String NOT_SUPPORT_FOR_THIS_SQL_IN_DROP_SUBSCRIPTION_PLEASE_ENTER_SUBSCRIPTIONID =
      "Not support 用于 this sql 在 DROP SUBSCRIPTION, please enter subscriptionId.";
  public static final String PLEASE_SET_THE_CORRECT_REQUEST_TYPE =
      "Please set correct request 类型: ";
  public static final String WHEN_SETTING_THE_REQUEST_THE_UNIT_IS_INCORRECT_PLEASE_USE_SEC_MIN_HOUR_DAY_AS_THE_UNIT =
      "When setting request, unit 为 incorrect. Please use 'sec', 'min', 'hour', 'day' 作为 unit";
  public static final String WHEN_SETTING_THE_SIZE_TIME_THE_UNIT_IS_INCORRECT_PLEASE_USE_B_K_M_G_P_T_AS_THE_UNIT =
      "When setting size/时间, unit 为 incorrect. Please use 'B', 'K', 'M', 'G', 'P', 'T' 作为 unit";
  public static final String WHEN_SETTING_THE_DISK_SIZE_THE_UNIT_IS_INCORRECT_PLEASE_USE_M_G_P_T_AS_THE_UNIT =
      "When setting disk size, unit 为 incorrect. Please use 'M', 'G', 'P', 'T' 作为 unit";
  public static final String WINDOW_FUNCTION_E_G_HEAD_TAIL_COUNT_SHOULD_BE_SET_IN_VALUE_WHEN_KEY_IS_WINDOW_IN_CALL =
      "Window 函数(e.g. HEAD, TAIL, COUNT) 应为 set 在 值 when key 为 'WINDOW' 在 CALL INFERENCE";
  public static final String THE_OUTPUT_TYPE_OF_THE_EXPRESSION_IN_HAVING_CLAUSE_SHOULD_BE_BOOLEAN_ACTUAL_DATA_TYPE_S =
      "output 类型 的 表达式 在 HAVING clause 应为 BOOLEAN, 实际 数据类型: %s.";
  public static final String IN =
      " 在 ";
  public static final String START_TIME_D_IS_GREATER_THAN_END_TIME_D =
      "Start 时间 %d 为 大于 end 时间 %d";
  public static final String THE_COLUMN =
      "列 '";
  public static final String DOES_NOT_EXIST_OR_IS_NOT_A_TAG_COLUMN =
      "' 不存在 或 为 不 tag 列";
  public static final String THE_RIGHT_HAND_VALUE_OF_TIME_PREDICATE_MUST_BE_A_LONG =
      "right hand 值 的 时间 predicate 必须为 long: ";
  public static final String THE_OPERATOR_OF_TIME_PREDICATE_MUST_BE_LT_LT_EQ_GT_OR_GT_EQ =
      "操作符 的 时间 predicate 必须为 <, <=, >, 或 >=: ";
  public static final String THE_RIGHT_HAND_VALUE_OF_TAG_PREDICATE_CANNOT_BE_NULL_WITH_EQ_OPERATOR_PLEASE_USE_IS_NULL =
      "right hand 值 的 tag predicate 不能 be null 与 '=' 操作符, 请使用 'IS NULL' instead";
  public static final String THE_RIGHT_HAND_VALUE_OF_TAG_PREDICATE_MUST_BE_A_STRING =
      "right hand 值 的 tag predicate 必须为 string: ";
  public static final String SELECT_INTO_PLACEHOLDER_CAN_ONLY_BE_USED_AT_THE_END_OF_THE_PATH =
      "select into: placeholder `::` 只能 be used 在 end 的 路径.";
  public static final String SELECT_INTO_THE_I_OF_DOLLAR_I_SHOULD_BE_GREATER_THAN_0_AND_EQUAL_TO_OR_LESS_THAN_THE =
      "select into: i 的 ${i} 应为 大于 0 和 等于 或 小于 length 的 queried 路径 prefix.";
  public static final String ALIAS_S_CAN_ONLY_BE_MATCHED_WITH_ONE_RESULT_COLUMN =
      "alias '%s' 只能 be matched 与 one result 列";
  public static final String RESULT_COLUMN_S_WITH_MORE_THAN_ONE_ALIAS_S_S =
      "Result 列 %s 与 more than one alias[%s, %s]";
  public static final String THERE_ARE_TOO_MANY_CONJUNCTS_MORE_THAN_1000_IN_PREDICATE_AFTER_REWRITING_THIS_MAY_BE =
      "There 为 too many conjuncts (more than 1000) 在 predicate after rewriting, this may be caused 由 too many 设备s 在 查询, try 到 use ALIGN BY DEVICE";
  public static final String CASE_EXPRESSION_TEXT_AND_OTHER_TYPES_CANNOT_EXIST_AT_THE_SAME_TIME =
      "CASE 表达式: TEXT 和 other 类型s 不能 exist 同时";
  public static final String CASE_EXPRESSION_BOOLEAN_AND_OTHER_TYPES_CANNOT_EXIST_AT_THE_SAME_TIME =
      "CASE 表达式: BOOLEAN 和 other 类型s 不能 exist 同时";
  public static final String THE_EXPRESSION_IN_THE_WHEN_CLAUSE_MUST_RETURN_BOOLEAN_EXPRESSION_S_ACTUAL_DATA_TYPE_S =
      "表达式 在 WHEN clause 必须 return BOOLEAN. 表达式: %s, 实际 数据类型: %s.";
  public static final String INVALID_INPUT_EXPRESSION_DATA_TYPE_EXPRESSION_S_ACTUAL_DATA_TYPE_S_EXPECTED_DATA_TYPE_S =
      "无效 input 表达式 数据类型. 表达式: %s, 实际 数据类型: %s, 期望 数据类型(s): %s.";
  public static final String S_IN_ORDER_BY_CLAUSE_DOESN_T_EXIST =
      "%s 在 order 由 clause 不存在.";
  public static final String S_IN_ORDER_BY_CLAUSE_SHOULDN_T_REFER_TO_MORE_THAN_ONE_TIMESERIES =
      "%s 在 order 由 clause shouldn't refer 到 more than one 时间series.";
  public static final String THE_DATA_TYPE_OF_S_IS_NOT_COMPARABLE =
      "数据类型 的 %s 为 不 comparable";
  public static final String GROUP_BY_LEVEL_THE_DATA_TYPES_OF_THE_SAME_OUTPUT_COLUMN_S_SHOULD_BE_THE_SAME =
      "GROUP BY LEVEL: 数据类型s 的 same output 列[%s] 应为 same.";
  public static final String CROSS_DEVICE_QUERIES_ARE_NOT_SUPPORTED_IN_ALIGN_BY_DEVICE_QUERIES =
      "Cross-设备 queries 为 不支持 在 ALIGN BY DEVICE queries.";
  public static final String VIEWS_OR_MEASUREMENT_ALIASES_REPRESENTING_THE_SAME_DATA_SOURCE =
      "Views 或 measurement aliases representing same data source ";
  public static final String CANNOT_BE_QUERIED_CONCURRENTLY_IN_ALIGN_BY_DEVICE_QUERIES =
      "不能 be queried con当前ly 在 ALIGN BY DEVICE queries.";
  public static final String THE_TYPE_OF_SQL_RESULT_COLUMN_S_IN_D_SHOULD_BE_NUMERIC_WHEN_INFERENCE =
      "类型 的 SQL result 列 [%s 在 %d] 应为 numeric when inference";
  public static final String S_IN_ORDER_BY_CLAUSE_DOESN_T_EXIST_IN_THE_RESULT_OF_LAST_QUERY =
      "%s 在 order 由 clause 不存在 在 result 的 last 查询.";
  public static final String S_IN_GROUP_BY_CLAUSE_DOESN_T_EXIST =
      "%s 在 group 由 clause 不存在.";
  public static final String S_IN_GROUP_BY_CLAUSE_SHOULDN_T_REFER_TO_MORE_THAN_ONE_TIMESERIES =
      "%s 在 group 由 clause shouldn't refer 到 more than one 时间series.";
  public static final String PLEASE_CHECK_THE_KEEP_CONDITION_S =
      "请检查 keep condition ([%s]), ";
  public static final String IT_NEED_TO_BE_A_CONSTANT_OR_A_COMPARE_EXPRESSION_CONSTRUCTED_BY_KEEP_AND_A_LONG_NUMBER =
      "it need 到 be constant 或 compare 表达式 constructed 由 'keep' 和 long number.";
  public static final String THE_QUERY_TIME_RANGE_SHOULD_BE_SPECIFIED_IN_THE_GROUP_BY_TIME_CLAUSE =
      "查询 时间 range 应为 specified 在 GROUP BY TIME clause.";
  public static final String VIEW_PATH_S_OF_SOURCE_COLUMN_S_IS_ILLEGAL_PATH =
      "View 路径 %s 的 source 列 %s 为 illegal 路径";
  public static final String ALIGN_BY_DEVICE_THE_DATA_TYPES_OF_THE_SAME_MEASUREMENT_COLUMN_SHOULD_BE_THE_SAME_ACROSS =
      "ALIGN BY DEVICE: 数据类型s 的 same measurement 列 应为 same across 设备s.";
  public static final String ALIAS_S_CAN_ONLY_BE_MATCHED_WITH_ONE_TIME_SERIES =
      "alias '%s' 只能 be matched 与 one 时间 series";
  public static final String TAG_AND_ATTRIBUTE_SHOULDN_T_HAVE_THE_SAME_PROPERTY_KEY_S =
      "Tag 和 attribute shouldn't have same 属性 key [%s]";
  public static final String S_IS_NOT_A_LEGAL_PROP =
      "%s 为 不 legal prop.";
  public static final String MEASUREMENT_UNDER_AN_ALIGNED_DEVICE_IS_NOT_ALLOWED_TO_HAVE_THE_SAME_MEASUREMENT_NAME =
      "Measurement under aligned 设备 为 不允许 到 have same measurement name";
  public static final String VALUE_FILTER_CAN_T_EXIST_IN_THE_CONDITION_OF_SHOW_COUNT_CLAUSE_ONLY_TIME_CONDITION =
      "值 Filter can't exist 在 condition 的 SHOW/COUNT clause, only 时间 condition supported";
  public static final String TIME_CONDITION_CAN_T_BE_EMPTY_IN_THE_CONDITION_OF_SHOW_COUNT_CLAUSE =
      "时间 condition can't be empty 在 condition 的 SHOW/COUNT clause";
  public static final String MEASUREMENT_UNDER_TEMPLATE_IS_NOT_ALLOWED_TO_HAVE_THE_SAME_MEASUREMENT_NAME =
      "Measurement under template 为 不允许 到 have same measurement name";
  public static final String THE_SUFFIX_PATHS_CAN_ONLY_BE_MEASUREMENT_OR_ONE_LEVEL_WILDCARD =
      "the suffix 路径s 只能 be measurement 或 one-level wildcard";
  public static final String AGGREGATION_RESULTS_CANNOT_BE_AS_INPUT_OF_THE_AGGREGATION_FUNCTION =
      "Aggregation results 不能 be 作为 input 的 aggregation 函数.";
  public static final String INPUT_OF_S_IS_ILLEGAL =
      "Input 的 '%s' 为 illegal.";
  public static final String RAW_DATA_AND_AGGREGATION_RESULT_HYBRID_INPUT_OF_S_IS_NOT_SUPPORTED =
      "Raw data 和 aggregation result hybrid input 的 '%s' 为 不支持.";
  public static final String ONLY_WRITABLE_VIEW_TIMESERIES_ARE_SUPPORTED_IN_ALIGN_BY_DEVICE_QUERIES =
      "ALIGN BY DEVICE 查询仅支持可写视图时间序列。";
  public static final String INPUT_SERIES_OF_SCALAR_FUNCTION_DIFF_ONLY_SUPPORTS_NUMERIC_DATA_TYPES_INT32_INT64_FLOAT =
      "Input series 的 标量函数 [DIFF] 仅支持 numeric 数据类型s [INT32, INT64, FLOAT, DOUBLE]";
  public static final String ARGUMENT_EXCEPTION_THE_SCALAR_FUNCTION_SUBSTRING_NEEDS_AT_LEAST_ONE_ARGUMENT_IT_MUST_BE =
      "Argument exception,the scalar 函数 [SUBSTRING] needs 在 least one 参数,it 必须为 signed integer";
  public static final String SYNTAX_ERROR_PLEASE_CHECK_THAT_THE_PARAMETERS_OF_THE_FUNCTION_ARE_CORRECT =
      "Syntax error,请检查 that 参数s 的 函数 为 correct";
  public static final String UNSUPPORTED_DATA_TYPE_S_FOR_FUNCTION_SUBSTRING =
      "不支持 数据类型 %s 用于 函数 SUBSTRING.";
  public static final String ARGUMENT_EXCEPTION_THE_SCALAR_FUNCTION_SUBSTRING_BEGINPOSITION_AND_LENGTH_MUST_BE =
      "Argument exception,the scalar 函数 [SUBSTRING] beginPosition 和 length 必须为 大于 0";
  public static final String INPUT_SERIES_OF_SCALAR_FUNCTION_ROUND_ONLY_SUPPORTS_NUMERIC_DATA_TYPES_INT32_INT64_FLOAT =
      "Input series 的 标量函数 [ROUND] 仅支持 numeric 数据类型s [INT32, INT64, FLOAT, DOUBLE]";
  public static final String UNSUPPORTED_DATA_TYPE_S_FOR_FUNCTION_REPLACE =
      "不支持 数据类型 %s 用于 函数 REPLACE.";
  public static final String TIMESERIES_UNDER_THIS_DEVICE_ISS_ALIGNED_PLEASE_USE_CREATESTIMESERIES_OR_CHANGE_DEVICE =
      "时间Series under this 设备 is%s aligned, 请使用 create%s时间Series 或 change 设备. (路径: %s)";
  public static final String NOT =
      " not";
  public static final String ALIGNED =
      "Aligned";
  public static final String AUTO_CREATE_OR_VERIFY_SCHEMA_ERROR_DETAIL_S =
      "Auto create 或 verify schema error. Detail: %s.";
  public static final String THE_FILE_S_IS_NOT_A_VALID_TSFILE_PLEASE_CHECK_THE_INPUT_FILE =
      "file %s 为 不 valid tsfile. 请检查 input file.";
  public static final String AUTO_CREATE_OR_VERIFY_SCHEMA_ERROR_WHEN_EXECUTING_STATEMENT_S_DETAIL_S =
      "Auto create 或 verify schema error when executing 语句 %s. Detail: %s.";
  public static final String TTL_VALUE_MUST_BE_INF_OR_A_LONG_LITERAL_BUT_NOW_IS =
      "ttl 值 必须为 'INF' 或 long 字面量, but now is: ";
  public static final String COLUMNS_IN_TABLE_SHALL_NOT_SHARE_THE_SAME_NAME_S =
      "列s 在 表 shall 不 share same name: '%s'.";
  public static final String THE_DUPLICATED_SOURCE_MEASUREMENT_S_IS_UNSUPPORTED_YET =
      "重复d source measurement %s 为 不支持 yet.";
  public static final String THE_LENGTH_OF_DATABASE_NAME_SHALL_NOT_EXCEED =
      "the length 的 数据库 name shall 不 exceed ";
  public static final String THE_DATABASE_NAME_CAN_ONLY_CONTAIN_ENGLISH_OR_CHINESE_CHARACTERS_NUMBERS_BACKTICKS_AND =
      "the 数据库 name 只能 contain english 或 chinese characters, numbers, backticks 和 underscores.";
  public static final String IS_CURRENTLY_NOT_ALLOWED =
      "' 为 当前ly 不允许.";
  public static final String VALUE_MUST_BE_A_LONGLITERAL_BUT_NOW_IS =
      " 值 必须为 Long字面量, but now 为 ";
  public static final String VALUE =
      ", 值: ";
  public static final String VALUE_MUST_BE_EQUAL_TO_OR_GREATER_THAN_0_BUT_NOW_IS =
      " 值 必须为 等于 或 大于 0, but now is: ";
  public static final String VALUE_MUST_BE_LOWER_THAN =
      " 值 必须为 lower than ";
  public static final String BUT_NOW_IS =
      ", but now is: ";
  public static final String FAILED_TO_CREATE_PIPE_S_SETTING_S_IS_NOT_ALLOWED =
      "未能 create pipe %s, setting %s 为 不允许.";
  public static final String FAILED_TO_S_PIPE_S_IN_IOTDB_SOURCE_PASSWORD_MUST_BE_SET_WHEN_THE_USERNAME_IS_SPECIFIED =
      "未能 %s pipe %s, 在 iotdb-source, password 必须为 set when username 为 specified.";
  public static final String ALTER =
      "修改";
  public static final String CREATE =
      "创建";
  public static final String FAILED_TO_S_PIPE_S_IN_WRITE_BACK_SINK_PASSWORD_MUST_BE_SET_WHEN_THE_USERNAME_IS =
      "未能 %s pipe %s, 在 write-back-sink, password 必须为 set when username 为 specified.";
  public static final String FAILED_TO_ALTER_PIPE_S_MODIFYING_S_IS_NOT_ALLOWED =
      "未能 alter pipe %s, modifying %s 为 不允许.";
  public static final String FAILED_TO_ALTER_PIPE_THE_SOURCE_PLUGIN_OF_THE_PIPE_CANNOT_BE_CHANGED_FROM_S_TO_S =
      "未能 alter pipe, source plugin 的 pipe 不能 be changed 来自 %s 到 %s";
  public static final String FAILED_TO_ALTER_PIPE_S_IN_IOTDB_SOURCE_PASSWORD_MUST_BE_SET_WHEN_THE_USERNAME_IS =
      "未能 alter pipe %s, 在 iotdb-source, password 必须为 set when username 为 specified.";
  public static final String FAILED_TO_ALTER_PIPE_S_IN_WRITE_BACK_SINK_PASSWORD_MUST_BE_SET_WHEN_THE_USERNAME_IS =
      "未能 alter pipe %s, 在 write-back-sink, password 必须为 set when username 为 specified.";
  public static final String PREPARED_STATEMENT_S_ALREADY_EXISTS =
      "Prepared 语句 '%s' already exists";
  public static final String INSUFFICIENT_MEMORY_FOR_PREPAREDSTATEMENT_S =
      "Insufficient memory 用于 Prepared语句 '%s'. ";
  public static final String PLEASE_DEALLOCATE_SOME_PREPAREDSTATEMENTS_AND_TRY_AGAIN =
      "Please deallocate some Prepared语句s 和 try again.";
  public static final String THE_TABLE =
      "表 ";
  public static final String IS_A_BASE_TABLE_DOES_NOT_SUPPORT_SHOW_CREATE_VIEW =
      " 为 base 表, does 不 support show create view.";
  public static final String THE_PARAMETERS =
      "参数s '";
  public static final String MUST_BE_CONSISTENT_ACROSS_THE_ENTIRE_CLUSTER_AND_ONLY_ONE_CAN_BE_SET_AT_A_TIME =
      "' 必须为 consistent across entire cluster 和 only one can be set 在 时间.";
  public static final String CANNOT_INSERT_INTO_MULTIPLE_DATABASES_WITHIN_ONE_STATEMENT_PLEASE_SPLIT_THEM_MANUALLY =
      "不能在一条语句中写入多个数据库，请手动拆分。";
  public static final String THE_MEASUREMENTLIST_S_SIZE_D_IS_NOT_CONSISTENT_WITH_THE_VALUELIST_S_SIZE_D =
      "the measurementList's size %d 为 不 consistent 与 值List's size %d";
  public static final String THE_MEASUREMENTLIST_S_SIZE_D_IS_NOT_CONSISTENT_WITH_THE_COLUMNLIST_S_SIZE_D =
      "the measurementList's size %d 为 不 consistent 与 列List's size %d";
  public static final String MEASUREMENT_CONTAINS_NULL_OR_EMPTY_STRING =
      "Measurement contains null 或 empty string: ";
  public static final String CAN_T_BE_USED_IN_GROUP_BY_TAG_IT_WILL_BE_SUPPORTED_IN_THE_FUTURE =
      " can't be used 在 group 由 tag. It will be supported 在 future.";
  public static final String COMMON_QUERIES_AND_AGGREGATED_QUERIES_ARE_NOT_ALLOWED_TO_APPEAR_AT_THE_SAME_TIME =
      "Common queries 和 aggregated queries 为 不允许 到 appear 同时";
  public static final String EXPRESSION_OF_HAVING_CLAUSE_CAN_NOT_BE_USED_IN_NONAGGREGATIONQUERY =
      "表达式 的 HAVING clause 不能 be used 在 NonAggregation查询";
  public static final String SORTING_BY_DEVICE_IS_ONLY_SUPPORTED_IN_ALIGN_BY_DEVICE_QUERIES =
      "仅 ALIGN BY DEVICE 查询支持按设备排序。";
  public static final String CQ_EVERY_INTERVAL_D_SHOULD_NOT_BE_LOWER_THAN_THE_CONTINUOUS_QUERY_MINIMUM_EVERY_INTERVAL =
      "CQ: Every interval [%d] 应 不 be lower than `continuous_查询_minimum_every_interval` [%d] configured.";
  public static final String CQ_THE_START_TIME_OFFSET_SHOULD_BE_GREATER_THAN_END_TIME_OFFSET =
      "CQ: start 时间 offset 应为 大于 end 时间 offset.";
  public static final String CQ_THE_START_TIME_OFFSET_SHOULD_BE_GREATER_THAN_OR_EQUAL_TO_EVERY_INTERVAL =
      "CQ: start 时间 offset 应为 大于 或 等于 every interval.";
  public static final String CQ_SPECIFYING_TIME_RANGE_IN_GROUP_BY_TIME_CLAUSE_IS_PROHIBITED =
      "CQ: Specifying 时间 range 在 GROUP BY TIME clause 为 prohibited.";
  public static final String CANNOT_CREATE_VIEWS_USING_DATA_SOURCES_WITH_CALCULATED_EXPRESSIONS_WHILE_USING_INTO_ITEM =
      "不能 create views using data sources 与 calculated 表达式s while using into item.";
  public static final String TREE_DEVICE_VIEW_WITH_MULTIPLE_DATABASES =
      "Tree 设备 view 与 multiple 数据库s(";
  public static final String IS_UNSUPPORTED_YET =
      ") 为 不支持 yet.";
  public static final String COMPLEX_ASOF_MAIN_JOIN_EXPRESSION_S_IS_NOT_SUPPORTED =
      "Complex ASOF main join 表达式 [%s] 为 不支持";
  public static final String UNEXPECTED_DESCRIPTOR_TYPE =
      "Un期望 descriptor 类型: ";
  public static final String WHEN_CLAUSE_OPERAND_TYPE_MUST_MATCH_CASE_OPERAND_TYPE_S_VS_S =
      "WHEN clause operand 类型 必须 match CASE operand 类型: %s vs %s";
  public static final String ALL_RESULT_TYPES_MUST_BE_THE_SAME_S =
      "All result 类型s 必须为 same: %s";
  public static final String DEFAULT_RESULT_TYPE_MUST_BE_THE_SAME_AS_WHEN_RESULT_TYPES_S_VS_S =
      "Default result 类型 必须为 same 作为 WHEN result 类型s: %s vs %s";
  public static final String ALL_OPERANDS_MUST_HAVE_THE_SAME_TYPE_S =
      "All operands 必须 have same 类型: %s";
  public static final String TO =
      " 到 ";
  public static final String SCALAR_FUNCTION =
      "标量函数 ";
  public static final String ONLY_SUPPORTS_ONE_NUMERIC_DATA_TYPES_INT32_INT64_FLOAT_DOUBLE_AND_ONE_BOOLEAN =
      " 仅支持 one numeric 数据类型s [INT32, INT64, FLOAT, DOUBLE] 和 one boolean";
  public static final String ONLY_SUPPORTS_TWO_NUMERIC_DATA_TYPES_INT32_INT64_FLOAT_DOUBLE =
      " 仅支持 two numeric 数据类型s [INT32, INT64, FLOAT, DOUBLE]";
  public static final String ONLY_ACCEPTS_TWO_OR_THREE_ARGUMENTS_AND_THEY_MUST_BE_TEXT_OR_STRING_DATA_TYPE =
      " 仅接受 two 或 three 参数s 和 they 必须为 text 或 string 数据类型.";
  public static final String ONLY_ACCEPTS_TWO_OR_THREE_ARGUMENTS_AND_FIRST_MUST_BE_TEXT_OR_STRING_DATA_TYPE_SECOND =
      " 仅接受 two 或 three 参数s 和 first 必须为 text 或 string 数据类型, second 和 third 必须为 numeric 数据类型s [INT32, INT64]";
  public static final String ONLY_ACCEPTS_ONE_ARGUMENT_AND_IT_MUST_BE_TEXT_OR_STRING_OR_BLOB_OR_OBJECT_DATA_TYPE =
      " 仅接受 one 参数 和 it 必须为 text 或 string 或 blob 或 object 数据类型.";
  public static final String ONLY_ACCEPTS_ONE_ARGUMENT_AND_IT_MUST_BE_TEXT_OR_STRING_DATA_TYPE =
      " 仅接受一个参数，且参数必须为 text 或 string 数据类型。";
  public static final String ONLY_ACCEPTS_ONE_OR_TWO_ARGUMENTS_AND_THEY_MUST_BE_TEXT_OR_STRING_DATA_TYPE =
      " 仅接受一个或两个参数，且参数必须为 text 或 string 数据类型。";
  public static final String ONLY_ACCEPTS_TWO_ARGUMENTS_AND_THEY_MUST_BE_TEXT_OR_STRING_DATA_TYPE =
      " 仅接受两个参数，且参数必须为 text 或 string 数据类型。";
  public static final String ONLY_ACCEPTS_TWO_OR_MORE_ARGUMENTS_AND_THEY_MUST_BE_TEXT_OR_STRING_DATA_TYPE =
      " 仅接受 two 或 more 参数s 和 they 必须为 text 或 string 数据类型.";
  public static final String ONLY_ACCEPTS_ONE_ARGUMENT_AND_IT_MUST_BE_DOUBLE_FLOAT_INT32_OR_INT64_DATA_TYPE =
      " 仅接受一个参数，且参数必须为 Double、Float、Int32 或 Int64 数据类型。";
  public static final String ACCEPTS_NO_ARGUMENT =
      " 不接受参数。";
  public static final String ONLY_ACCEPTS_TWO_OR_THREE_ARGUMENTS_AND_THE_SECOND_AND_THIRD_MUST_BE_TIMESTAMP_DATA_TYPE =
      " 仅接受 two 或 three 参数s 和 second 和 third 必须为 时间Stamp 数据类型.";
  public static final String MUST_HAVE_AT_LEAST_TWO_ARGUMENTS_AND_FIRST_ARGUMENT_PATTERN_MUST_BE_TEXT_OR_STRING_TYPE =
      " 必须 have 在 least two 参数s, 和 first 参数 pattern 必须为 TEXT 或 STRING 类型.";
  public static final String MUST_HAVE_AT_LEAST_TWO_ARGUMENTS_AND_ALL_TYPE_MUST_BE_THE_SAME =
      " 必须 have 在 least two 参数s, 和 all 类型 必须为 same.";
  public static final String SCALAR_FUNCTION_S_ONLY_ACCEPTS_TWO_ARGUMENTS_AND_THEY_MUST_BE_INT32_OR_INT64_DATA_TYPE =
      "标量函数 %s 仅接受 two 参数s 和 they 必须为 Int32 或 Int64 数据类型.";
  public static final String SCALAR_FUNCTION_S_ONLY_ACCEPTS_ONE_ARGUMENT_AND_IT_MUST_BE_INT32_OR_INT64_DATA_TYPE =
      "标量函数 %s 仅接受 one 参数 和 it 必须为 Int32 或 Int64 数据类型.";
  public static final String SCALAR_FUNCTION_S_ONLY_ACCEPTS_ONE_ARGUMENT_AND_IT_MUST_BE_TEXT_STRING_OR_BLOB_DATA_TYPE =
      "标量函数 %s 仅接受 one 参数 和 it 必须为 TEXT, STRING, 或 BLOB 数据类型.";
  public static final String SCALAR_FUNCTION_S_ONLY_ACCEPTS_ONE_ARGUMENT_AND_IT_MUST_BE_TEXT_OR_STRING_DATA_TYPE =
      "标量函数 %s 仅接受 one 参数 和 it 必须为 TEXT 或 STRING 数据类型.";
  public static final String SCALAR_FUNCTION_S_ONLY_ACCEPTS_ONE_ARGUMENT_AND_IT_MUST_BE_TEXT_STRING_OR_BLOB_DATA_TYPE_2 =
      "标量函数 %s 仅接受 one 参数 和 it 必须为 TEXT, STRING, 或 BlOB 数据类型.";
  public static final String SCALAR_FUNCTION_S_ONLY_ACCEPTS_TWO_ARGUMENTS_FIRST_ARGUMENT_MUST_BE_TEXT_STRING_OR_BLOB =
      "标量函数 %s 仅接受 two 参数s, first 参数 必须为 TEXT, STRING, 或 BlOB 类型, second 参数 必须为 STRING OR TEXT 类型.";
  public static final String SCALAR_FUNCTION_S_ONLY_ACCEPTS_ONE_ARGUMENT_AND_IT_MUST_BE_INT32_DATA_TYPE =
      "标量函数 %s 仅接受 one 参数 和 it 必须为 Int32 数据类型.";
  public static final String SCALAR_FUNCTION_S_ONLY_ACCEPTS_ONE_ARGUMENT_AND_IT_MUST_BE_BLOB_DATA_TYPE =
      "标量函数 %s 仅接受 one 参数 和 it 必须为 BLOB 数据类型.";
  public static final String SCALAR_FUNCTION_S_ONLY_ACCEPTS_ONE_ARGUMENT_AND_IT_MUST_BE_INT64_DATA_TYPE =
      "标量函数 %s 仅接受 one 参数 和 it 必须为 Int64 数据类型.";
  public static final String SCALAR_FUNCTION_S_ONLY_ACCEPTS_ONE_ARGUMENT_AND_IT_MUST_BE_FLOAT_DATA_TYPE =
      "标量函数 %s 仅接受 one 参数 和 it 必须为 Float 数据类型.";
  public static final String SCALAR_FUNCTION_S_ONLY_ACCEPTS_ONE_ARGUMENT_AND_IT_MUST_BE_DOUBLE_DATA_TYPE =
      "标量函数 %s 仅接受 one 参数 和 it 必须为 Double 数据类型.";
  public static final String SCALAR_FUNCTION_S_ONLY_ACCEPTS_THREE_ARGUMENTS_FIRST_ARGUMENT_MUST_BE_BLOB_TYPE =
      "标量函数 %s 仅接受 three 参数s, first 参数 必须为 BlOB 类型, ";
  public static final String SECOND_ARGUMENT_MUST_BE_INT32_OR_INT64_TYPE_THIRD_ARGUMENT_MUST_BE_BLOB_TYPE =
      "second 参数 必须为 int32 或 int64 类型, third 参数 必须为 BLOB 类型.";
  public static final String AGGREGATE_FUNCTIONS_S_SHOULD_ONLY_HAVE_ONE_ARGUMENT =
      "聚合函数 [%s] 只能 have one 参数";
  public static final String AGGREGATE_FUNCTIONS_S_ONLY_SUPPORT_NUMERIC_DATA_TYPES_INT32_INT64_FLOAT_DOUBLE =
      "聚合函数 [%s] 仅支持 numeric 数据类型s [INT32, INT64, FLOAT, DOUBLE]";
  public static final String ERROR_SIZE_OF_INPUT_EXPRESSIONS_EXPRESSION_S_ACTUAL_SIZE_S_EXPECTED_SIZE_2 =
      "Error size 的 input 表达式s. 表达式: %s, 实际 size: %s, 期望 size: [2].";
  public static final String AGGREGATE_FUNCTIONS_S_ONLY_SUPPORT_NUMERIC_DATA_TYPES_INT32_INT64_FLOAT_DOUBLE_TIMESTAMP =
      "聚合函数 [%s] 仅支持 numeric 数据类型s [INT32, INT64, FLOAT, DOUBLE, TIMESTAMP]";
  public static final String ERROR_SIZE_OF_INPUT_EXPRESSIONS_EXPRESSION_S_ACTUAL_SIZE_S_EXPECTED_SIZE_1 =
      "Error size 的 input 表达式s. 表达式: %s, 实际 size: %s, 期望 size: [1].";
  public static final String AGGREGATE_FUNCTIONS_S_SHOULD_ONLY_HAVE_ONE_BOOLEAN_EXPRESSION_AS_ARGUMENT =
      "聚合函数 [%s] 只能 have one boolean 表达式 作为 参数";
  public static final String AGGREGATE_FUNCTIONS_S_SHOULD_ONLY_HAVE_ONE_OR_TWO_ARGUMENTS =
      "聚合函数 [%s] 只能 have one 或 two 参数s";
  public static final String SECOND_ARGUMENT_OF_AGGREGATE_FUNCTIONS_S_SHOULD_BE_ORDERABLE =
      "Second 参数 的 聚合函数 [%s] 应为 orderable";
  public static final String AGGREGATE_FUNCTIONS_S_SHOULD_ONLY_HAVE_TWO_OR_THREE_ARGUMENTS =
      "聚合函数 [%s] 只能 have two 或 three 参数s";
  public static final String AGGREGATE_FUNCTIONS_S_SHOULD_ONLY_HAVE_TWO_ARGUMENTS =
      "聚合函数 [%s] 只能 have two 参数s";
  public static final String SECOND_ARGUMENT_OF_AGGREGATE_FUNCTIONS_S_SHOULD_BE_NUMBERIC_TYPE_AND_DO_NOT_USE =
      "Second 参数 的 聚合函数 [%s] 应为 numberic 类型 和 do 不 use 表达式";
  public static final String AGGREGATION_FUNCTIONS_S_SHOULD_ONLY_HAVE_THREE_ARGUMENTS =
      "Aggregation 函数s [%s] 只能 have three 参数s";
  public static final String AGGREGATION_FUNCTIONS_S_SHOULD_ONLY_HAVE_TWO_OR_THREE_ARGUMENTS =
      "Aggregation 函数s [%s] 只能 have two 或 three 参数s";
  public static final String AGGREGATION_FUNCTIONS_S_SHOULD_HAVE_VALUE_COLUMN_AS_NUMERIC_TYPE_INT32_INT64_FLOAT =
      "Aggregation 函数s [%s] 应 have 值 列 作为 numeric 类型 [INT32, INT64, FLOAT, DOUBLE, TIMESTAMP]";
  public static final String AGGREGATION_FUNCTIONS_S_SHOULD_HAVE_PERCENTAGE_AS_DECIMAL_TYPE =
      "Aggregation 函数s [%s] 应 have percentage 作为 decimal 类型";
  public static final String AGGREGATION_FUNCTIONS_S_DO_NOT_SUPPORT_WEIGHT_AS_S_TYPE =
      "Aggregation 函数s [%s] do 不 support weight 作为 %s 类型";
  public static final String WINDOW_FUNCTION_S_SHOULD_ONLY_HAVE_ONE_ARGUMENT =
      "窗口函数 [%s] 只能 have one 参数";
  public static final String WINDOW_FUNCTION_NTH_VALUE_SHOULD_ONLY_HAVE_TWO_ARGUMENT_AND_SECOND_ARGUMENT_MUST_BE =
      "窗口函数 [nth_值] 只能 have two 参数, 和 second 参数 必须为 integer 类型";
  public static final String WINDOW_FUNCTION_S_SHOULD_ONLY_HAVE_ONE_TO_THREE_ARGUMENT =
      "窗口函数 [%s] 只能 have one 到 three 参数";
  public static final String WINDOW_FUNCTION_S_S_SECOND_ARGUMENT_MUST_BE_INTEGER_TYPE =
      "窗口函数 [%s]'s second 参数 必须为 integer 类型";
  public static final String UPDATE_ATTRIBUTE_SHALL_SPECIFY_A_ATTRIBUTE_ONLY_ONCE =
      "UPDATE 属性只能指定一次。";
  public static final String CANNOT_BE_RESOLVED =
      "无法解析";
  public static final String IS_NOT_AN_ATTRIBUTE_OR_TAG_COLUMN =
      "不是属性列或标签列";
  public static final String UPDATE_S_ATTRIBUTE_VALUE_MUST_BE_STRING_TEXT_OR_NULL =
      "UPDATE 的属性值必须为 STRING、TEXT 或 null。";
  public static final String MULTIPLE_COLUMNS_FOUND_WITH_TIME_CATEGORY_IN_TABLE_SCHEMA =
      "Multiple 列s found 与 TIME category 在 表 schema";
  public static final String INSERT_COLUMN_NAME_DOES_NOT_EXIST_IN_TARGET_TABLE_S =
      "Insert 列 name 不存在 在 target 表: %s";
  public static final String INSERT_COLUMN_NAME_IS_SPECIFIED_MORE_THAN_ONCE_S =
      "Insert 列 name 为 specified 多次: %s";
  public static final String INSERT_QUERY_HAS_MISMATCHED_COLUMN_TYPES_TABLE_S_QUERY_S =
      "Insert 查询 has 不匹配 列 类型s: 表: [%s], 查询: [%s]";
  public static final String WITH_QUERY_NAME_S_SPECIFIED_MORE_THAN_ONCE =
      "WITH 查询 name '%s' specified 多次";
  public static final String WITH_TABLE_NAME_IS_REFERENCED_IN_THE_BASE_RELATION_OF_RECURSION =
      "WITH 表 name 为 referenced 在 base relation 的 recursion";
  public static final String MULTIPLE_RECURSIVE_REFERENCES_IN_THE_STEP_RELATION_OF_RECURSION =
      "multiple recursive references 在 step relation 的 recursion";
  public static final String FETCH_FIRST_LIMIT_CLAUSE_IN_THE_STEP_RELATION_OF_RECURSION =
      "FETCH FIRST / LIMIT clause 在 step relation 的 recursion";
  public static final String RECURSIVE_REFERENCE_OUTSIDE_OF_FROM_CLAUSE_OF_THE_STEP_RELATION_OF_RECURSION =
      "recursive reference outside 的 FROM clause 的 step relation 的 recursion";
  public static final String IMMEDIATE_WITH_CLAUSE_IN_RECURSIVE_QUERY_IS_NOT_SUPPORTED =
      "immediate WITH clause 在 recursive 查询 为 不支持";
  public static final String IMMEDIATE_FILL_CLAUSE_IN_RECURSIVE_QUERY_IS_NOT_SUPPORTED =
      "immediate FILL clause 在 recursive 查询 为 不支持";
  public static final String IMMEDIATE_ORDER_BY_CLAUSE_IN_RECURSIVE_QUERY_IS_NOT_SUPPORTED =
      "immediate ORDER BY clause 在 recursive 查询 为 不支持";
  public static final String IMMEDIATE_OFFSET_CLAUSE_IN_RECURSIVE_QUERY_IS_NOT_SUPPORTED =
      "immediate OFFSET clause 在 recursive 查询 为 不支持";
  public static final String IMMEDIATE_FETCH_FIRST_LIMIT_CLAUSE_IN_RECURSIVE_QUERY_IS_NOT_SUPPORT =
      "immediate FETCH FIRST / LIMIT clause 在 recursive 查询 为 不 support";
  public static final String BASE_AND_STEP_RELATIONS_OF_RECURSION_HAVE_DIFFERENT_NUMBER_OF_FIELDS_S_S =
      "base 和 step relations 的 recursion have different number 的 fields: %s, %s";
  public static final String RECURSION_STEP_RELATION_OUTPUT_TYPE_S_IS_NOT_COERCIBLE_TO_RECURSION_BASE_RELATION_OUTPUT =
      "recursion step relation output 类型 (%s) 为 不 coercible 到 recursion base relation output 类型 (%s) 在 列 %s";
  public static final String CANNOT_NEST_WINDOW_FUNCTIONS_OR_ROW_PATTERN_MEASURES_INSIDE_WINDOW_FUNCTION_ARGUMENTS =
      "不能 nest 窗口函数s 或 row pattern measures inside 窗口函数 参数s";
  public static final String DISTINCT_IN_WINDOW_FUNCTION_PARAMETERS_NOT_YET_SUPPORTED_S =
      "DISTINCT 在 窗口函数 参数s 不 yet supported: %s";
  public static final String S_FUNCTION_REQUIRES_AN_ORDER_BY_WINDOW_CLAUSE =
      "%s 函数 requires ORDER BY window clause";
  public static final String CANNOT_SPECIFY_WINDOW_FRAME_FOR_S_FUNCTION =
      "不能 specify window frame 用于 %s 函数";
  public static final String WINDOW_NAME_S_SPECIFIED_MORE_THAN_ONCE =
      "WINDOW name '%s' specified 多次";
  public static final String CANNOT_RESOLVE_WINDOW_NAME_S =
      "不能 resolve WINDOW name %s";
  public static final String WINDOW_SPECIFICATION_WITH_NAMED_WINDOW_REFERENCE_CANNOT_SPECIFY_PARTITION_BY =
      "WINDOW specification 与 named WINDOW reference 不能 specify PARTITION BY";
  public static final String CANNOT_SPECIFY_ORDER_BY_IF_REFERENCED_NAMED_WINDOW_SPECIFIES_ORDER_BY =
      "不能 specify ORDER BY if referenced named WINDOW specifies ORDER BY";
  public static final String CANNOT_REFERENCE_NAMED_WINDOW_CONTAINING_FRAME_SPECIFICATION =
      "不能 reference named WINDOW containing frame specification";
  public static final String WHERE_CLAUSE_MUST_EVALUATE_TO_A_BOOLEAN_ACTUAL_TYPE_S =
      "WHERE clause 必须 evaluate 到 boolean: 实际 类型 %s";
  public static final String MULTIPLE_DIFFERENT_COLUMNS_IN_THE_SAME_EXPRESSION_ARE_NOT_SUPPORTED =
      "Multiple different COLUMNS 在 same 表达式 为 不支持";
  public static final String NO_MATCHING_COLUMNS_FOUND_THAT_MATCH_REGEX_S =
      "No matching 列s found that match regex '%s'";
  public static final String S_ARE_NOT_SUPPORTED_NOW =
      "%s 为 不支持 now";
  public static final String UNABLE_TO_RESOLVE_REFERENCE_S =
      "Unable 到 resolve reference %s";
  public static final String IDENTIFIERCHAINBASIS_GET_GETBASISTYPE_EQ_EQ_FIELD_OR_TARGET_EXPRESSION_ISN_T_A =
      "identifierChainBasis.get().getBasis类型 == FIELD 或 target 表达式 isn't QualifiedName";
  public static final String SELECT_STAR_FROM_OUTER_SCOPE_TABLE_NOT_SUPPORTED_WITH_ANONYMOUS_COLUMNS =
      "SELECT * 来自 outer scope 表 不支持 与 anonymous 列s";
  public static final String DISTINCT_CAN_ONLY_BE_APPLIED_TO_COMPARABLE_TYPES_ACTUAL_S =
      "DISTINCT 只能 be applied 到 comparable 类型s (实际: %s)";
  public static final String DISTINCT_CAN_ONLY_BE_APPLIED_TO_COMPARABLE_TYPES_ACTUAL_S_S =
      "DISTINCT 只能 be applied 到 comparable 类型s (实际: %s): %s";
  public static final String GROUP_BY_POSITION_S_IS_NOT_IN_SELECT_LIST =
      "GROUP BY position %s 为 不 在 select list";
  public static final String GROUP_BY_EXPRESSION_MUST_BE_A_COLUMN_REFERENCE_S =
      "GROUP BY 表达式 必须为 列 reference: %s";
  public static final String S_IS_NOT_COMPARABLE_AND_THEREFORE_CANNOT_BE_USED_IN_GROUP_BY =
      "%s 为 不 comparable, 和 therefore 不能 be used 在 GROUP BY";
  public static final String GROUP_BY_HAS_MORE_THAN_S_GROUPING_SETS =
      "GROUP BY has more than %s grouping sets";
  public static final String HAVING_CLAUSE_MUST_EVALUATE_TO_A_BOOLEAN_ACTUAL_TYPE_S =
      "HAVING clause 必须 evaluate 到 boolean: 实际 类型 %s";
  public static final String S_QUERY_HAS_DIFFERENT_NUMBER_OF_FIELDS_D_D =
      "%s 查询 has different number 的 fields: %d, %d";
  public static final String COLUMN_D_IN_S_QUERY_HAS_INCOMPATIBLE_TYPES_S_S =
      "列 %d 在 %s 查询 has 不兼容 类型s: %s, %s";
  public static final String TYPE_S_IS_NOT_COMPARABLE_AND_THEREFORE_CANNOT_BE_USED_IN_SS =
      "类型 %s 为 不 comparable 和 therefore 不能 be used 在 %s%s";
  public static final String DISTINCT =
      " DISTINCT";
  public static final String AMBIGUOUS_COLUMN_S_IN_ROW_PATTERN_INPUT_RELATION =
      "ambiguous 列: %s 在 row pattern input relation";
  public static final String S_IS_NOT_COMPARABLE_AND_THEREFORE_CANNOT_BE_USED_IN_PARTITION_BY =
      "%s 为 不 comparable, 和 therefore 不能 be used 在 PARTITION BY";
  public static final String S_IS_NOT_ORDERABLE_AND_THEREFORE_CANNOT_BE_USED_IN_ORDER_BY =
      "%s 为 不 orderable, 和 therefore 不能 be used 在 ORDER BY";
  public static final String EXPRESSION_DEFINING_A_LABEL_MUST_BE_BOOLEAN_ACTUAL_TYPE_S =
      "表达式 defining label 必须为 boolean (实际 类型: %s)";
  public static final String VALUES_ROWS_HAVE_MISMATCHED_SIZES_S_VS_S =
      "值s rows have 不匹配 sizes: %s vs %s";
  public static final String TYPE_OF_ROW_D_COLUMN_D_IS_MISMATCHED_EXPECTED_S_ACTUAL_S =
      "类型 的 row %d 列 %d 为 不匹配, 期望: %s, 实际: %s";
  public static final String TYPE_OF_ROW_D_IS_MISMATCHED_EXPECTED_S_ACTUAL_S =
      "类型 的 row %d 为 不匹配, 期望: %s, 实际: %s";
  public static final String COLUMN_ALIAS_LIST_HAS_S_ENTRIES_BUT_S_HAS_S_COLUMNS_AVAILABLE =
      "列 alias list has %s entries but '%s' has %s 列s available";
  public static final String JOIN_ON_CLAUSE_MUST_EVALUATE_TO_A_BOOLEAN_ACTUAL_TYPE_S =
      "JOIN ON clause 必须 evaluate 到 boolean: 实际 类型 %s";
  public static final String ASOF_MAIN_JOIN_EXPRESSION_MUST_EVALUATE_TO_A_BOOLEAN_ACTUAL_TYPE_S =
      "ASOF main JOIN 表达式 必须 evaluate 到 boolean: 实际 类型 %s";
  public static final String LEFT_CHILD_TYPE_OF_ASOF_MAIN_JOIN_EXPRESSION_MUST_BE_TIMESTAMP_ACTUAL_TYPE_S =
      "left child 类型 的 ASOF main JOIN 表达式 必须为 TIMESTAMP: 实际 类型 %s";
  public static final String RIGHT_CHILD_TYPE_OF_ASOF_MAIN_JOIN_EXPRESSION_MUST_BE_TIMESTAMP_ACTUAL_TYPE_S =
      "right child 类型 的 ASOF main JOIN 表达式 必须为 TIMESTAMP: 实际 类型 %s";
  public static final String COLUMN_S_APPEARS_MULTIPLE_TIMES_IN_USING_CLAUSE =
      "列 '%s' appears multiple 时间s 在 USING clause";
  public static final String COLUMN_S_IS_MISSING_FROM_LEFT_SIDE_OF_JOIN =
      "列 '%s' 为 缺少 来自 left side 的 join";
  public static final String COLUMN_S_IS_MISSING_FROM_RIGHT_SIDE_OF_JOIN =
      "列 '%s' 为 缺少 来自 right side 的 join";
  public static final String COLUMN_TYPES_OF_LEFT_AND_RIGHT_SIDE_ARE_DIFFERENT_LEFT_IS_S_RIGHT_IS_S =
      "列 类型s 的 left 和 right side 为 different: left 为 %s, right 为 %s";
  public static final String CANNOT_INFER_TIME_COLUMN_FOR_S_FILL_THERE_EXISTS_NO_COLUMN_WHOSE_TYPE_IS_TIMESTAMP =
      "不能 infer TIME_COLUMN 用于 %s FILL, there exists no 列 whose 类型 为 TIMESTAMP";
  public static final String S_FILL_TIME_COLUMN_POSITION_S_IS_NOT_IN_SELECT_LIST =
      "%s FILL TIME_COLUMN position %s 为 不 在 select list";
  public static final String TYPE_OF_TIME_COLUMN_FOR_S_FILL_SHOULD_ONLY_BE_TIMESTAMP_BUT_TYPE_OF_THE_COLUMN_YOU =
      "类型 的 TIME_COLUMN 用于 %s FILL 只能 be TIMESTAMP, but 类型 的 列 you specify 为 %s";
  public static final String S_FILL_FILL_GROUP_POSITION_S_IS_NOT_IN_SELECT_LIST =
      "%s FILL FILL_GROUP position %s 为 不 在 select list";
  public static final String TYPE_S_IS_NOT_ORDERABLE_AND_THEREFORE_CANNOT_BE_USED_IN_FILL_GROUP_S =
      "类型 %s 为 不 orderable, 和 therefore 不能 be used 在 FILL_GROUP: %s";
  public static final String ORDER_BY_POSITION_S_IS_NOT_IN_SELECT_LIST =
      "ORDER BY position %s 为 不 在 select list";
  public static final String TYPE_S_IS_NOT_ORDERABLE_AND_THEREFORE_CANNOT_BE_USED_IN_ORDER_BY_S =
      "类型 %s 为 不 orderable, 和 therefore 不能 be used 在 ORDER BY: %s";
  public static final String OFFSET_ROW_COUNT_MUST_BE_GREATER_OR_EQUAL_TO_0_ACTUAL_VALUE_S =
      "OFFSET row count 必须为 greater 或 等于 0 (实际 值: %s)";
  public static final String LIMIT_ROW_COUNT_MUST_BE_GREATER_OR_EQUAL_TO_0_ACTUAL_VALUE_S =
      "LIMIT row count 必须为 greater 或 等于 0 (实际 值: %s)";
  public static final String NON_CONSTANT_PARAMETER_VALUE_FOR_S_S =
      "Non constant 参数 值 用于 %s: %s";
  public static final String PARAMETER_VALUE_PROVIDED_FOR_S_IS_NULL_S =
      "Parameter 值 provided 用于 %s 为 NULL: %s";
  public static final String RECURSIVE_REFERENCE_IN_LEFT_SOURCE_OF_S_JOIN =
      "recursive reference 在 left source 的 %s join";
  public static final String RECURSIVE_REFERENCE_IN_RIGHT_SOURCE_OF_S_JOIN =
      "recursive reference 在 right source 的 %s join";
  public static final String RECURSIVE_REFERENCE_IN_RIGHT_RELATION_OF_EXCEPT_S =
      "recursive reference 在 right relation 的 EXCEPT %s";
  public static final String DISTINCT_2 =
      "DISTINCT";
  public static final String ALL =
      "ALL";
  public static final String RECURSIVE_REFERENCE_IN_LEFT_RELATION_OF_EXCEPT_ALL =
      "recursive reference 在 left relation 的 EXCEPT ALL";
  public static final String FOR_SELECT_DISTINCT_ORDER_BY_EXPRESSIONS_MUST_APPEAR_IN_SELECT_LIST =
      "For SELECT DISTINCT, ORDER BY 表达式s 必须 appear 在 select list";
  public static final String IS_CURRENTLY_NOT_ALLOWED_2 =
      " 为 当前ly 不允许.";
  public static final String DUPLICATE_PROPERTY_S =
      "重复 属性: %s";
  public static final String TTL_VALUE_MUST_BE_A_INF_OR_A_LONGLITERAL_BUT_NOW_IS =
      "TTL' 值 必须为 'INF' 或 Long字面量, but now is: ";
  public static final String COLUMN_NAME_NOT_SPECIFIED_AT_POSITION_S =
      "列 name 不 specified 在 position %s";
  public static final String COLUMN_NAME_S_SPECIFIED_MORE_THAN_ONCE =
      "列 name '%s' specified 多次";
  public static final String COLUMN_ALIAS_LIST_HAS_S_ENTRIES_BUT_RELATION_HAS_S_COLUMNS =
      "列 alias list has %s entries but relation has %s 列s";
  public static final String TABLE_FUNCTION_S_SPECIFIES_REQUIRED_COLUMNS_FROM_TABLE_ARGUMENT_S_WHICH_CANNOT_BE_FOUND =
      "表 函数 %s specifies required 列s 来自 表 参数 %s which 不能 be found";
  public static final String TABLE_FUNCTION_S_SPECIFIES_EMPTY_LIST_OF_REQUIRED_COLUMNS_FROM_TABLE_ARGUMENT_S =
      "表 函数 %s specifies empty list 的 required 列s 来自 表 参数 %s";
  public static final String TABLE_FUNCTION_S_SPECIFIES_NEGATIVE_INDEX_OF_REQUIRED_COLUMN_FROM_TABLE_ARGUMENT_S =
      "表 函数 %s specifies negative index 的 required 列 来自 表 参数 %s";
  public static final String INDEX_S_OF_REQUIRED_COLUMN_FROM_TABLE_ARGUMENT_S_IS_OUT_OF_BOUNDS_FOR_TABLE_WITH_S =
      "Index %s 的 required 列 来自 表 参数 %s 为 out 的 bounds 用于 表 与 %s 列s";
  public static final String TABLE_FUNCTION_S_DOES_NOT_SPECIFY_REQUIRED_INPUT_COLUMNS_FROM_TABLE_ARGUMENT_S =
      "表 函数 %s does 不 specify required input 列s 来自 表 参数 %s";
  public static final String TOO_MANY_ARGUMENTS_EXPECTED_AT_MOST_S_ARGUMENTS_GOT_S_ARGUMENTS =
      "Too many 参数s. Expected 在 most %s 参数s, got %s 参数s";
  public static final String ALL_ARGUMENTS_MUST_BE_PASSED_BY_NAME_OR_ALL_MUST_BE_PASSED_POSITIONALLY =
      "All 参数s 必须为 passed 由 name 或 all 必须为 passed positionally";
  public static final String UNEXPECTED_ARGUMENT_NAME_S =
      "Un期望 参数 name: %s";
  public static final String UNEXPECTED_TABLE_FUNCTION_ARGUMENT_TYPE_S =
      "Un期望 表 函数 参数 类型: %s";
  public static final String INVALID_ARGUMENT_S_EXPECTED_TABLE_ARGUMENT_GOT_S =
      "无效 参数 %s. Expected 表 参数, got %s";
  public static final String INVALID_ARGUMENT_S_EXPECTED_SCALAR_ARGUMENT_GOT_S =
      "无效 参数 %s. Expected scalar 参数, got %s";
  public static final String INVALID_ARGUMENT_S_PARTITIONING_CAN_NOT_BE_SPECIFIED_FOR_TABLE_ARGUMENT_WITH_ROW =
      "无效 参数 %s. Partitioning 不能 be specified 用于 表 参数 与 row semantics";
  public static final String INVALID_ARGUMENT_S_ORDERING_CAN_NOT_BE_SPECIFIED_FOR_TABLE_ARGUMENT_WITH_ROW_SEMANTICS =
      "无效 参数 %s. Ordering 不能 be specified 用于 表 参数 与 row semantics";
  public static final String INVALID_SCALAR_ARGUMENT_S_EXPECTED_TYPE_S_GOT_S =
      "无效 scalar 参数 '%s'. Expected 类型 %s, got %s";
  public static final String INVALID_SCALAR_ARGUMENT_S_S =
      "无效 scalar 参数 %s, %s";
  public static final String MISSING_REQUIRED_ARGUMENT_S =
      "缺少 required 参数: %s";
  public static final String EXPECTED_COLUMN_REFERENCE_ACTUAL_S =
      "Expected 列 reference. Actual: %s";
  public static final String COLUMN_S_IS_NOT_PRESENT_IN_THE_INPUT_RELATION =
      "列 %s 为 不 present 在 input relation";
  public static final String S_CANNOT_CONTAIN_AGGREGATIONS_WINDOW_FUNCTIONS_OR_GROUPING_OPERATIONS_S =
      "%s 不能 contain aggregations, 窗口函数s 或 grouping operations: %s";
  public static final String S_MUST_BE_AN_AGGREGATE_EXPRESSION_OR_APPEAR_IN_GROUP_BY_CLAUSE =
      "'%s' 必须为 aggregate 表达式 或 appear 在 GROUP BY clause";
  public static final String SUBQUERY_USES_S_WHICH_MUST_APPEAR_IN_GROUP_BY_CLAUSE =
      "Sub查询 uses '%s' which 必须 appear 在 GROUP BY clause";
  public static final String CANNOT_NEST_AGGREGATIONS_INSIDE_AGGREGATION_S_S =
      "不能 nest aggregations inside aggregation '%s': %s";
  public static final String UNION_PATTERN_VARIABLE_NAME_S_IS_A_DUPLICATE_OF_PRIMARY_PATTERN_VARIABLE_NAME =
      "union pattern variable name: %s 为 重复 的 primary pattern variable name";
  public static final String UNION_PATTERN_VARIABLE_NAME_S_IS_DECLARED_TWICE =
      "union pattern variable name: %s 为 declared twice";
  public static final String SUBSET_ELEMENT_S_IS_NOT_A_PRIMARY_PATTERN_VARIABLE =
      "subset element: %s 为 不 primary pattern variable";
  public static final String DEFINED_VARIABLE_S_IS_NOT_A_PRIMARY_PATTERN_VARIABLE =
      "defined variable: %s 为 不 primary pattern variable";
  public static final String PATTERN_VARIABLE_WITH_NAME_S_IS_DEFINED_TWICE =
      "pattern variable 与 name: %s 为 defined twice";
  public static final String FINAL_SEMANTICS_IS_NOT_SUPPORTED_IN_DEFINE_CLAUSE =
      "FINAL semantics 为 不支持 在 DEFINE clause";
  public static final String PATTERN_QUANTIFIER_LOWER_BOUND_MUST_BE_GREATER_THAN_OR_EQUAL_TO_0 =
      "Pattern quantifier lower bound 必须为 大于 或 等于 0";
  public static final String PATTERN_QUANTIFIER_LOWER_BOUND_MUST_NOT_EXCEED =
      "Pattern quantifier lower bound 必须 不 exceed ";
  public static final String PATTERN_QUANTIFIER_UPPER_BOUND_MUST_BE_GREATER_THAN_OR_EQUAL_TO_1 =
      "Pattern quantifier upper bound 必须为 大于 或 等于 1";
  public static final String PATTERN_QUANTIFIER_UPPER_BOUND_MUST_NOT_EXCEED =
      "Pattern quantifier upper bound 必须 不 exceed ";
  public static final String PATTERN_QUANTIFIER_LOWER_BOUND_MUST_NOT_EXCEED_UPPER_BOUND =
      "Pattern quantifier lower bound 必须 不 exceed upper bound";
  public static final String S_IS_NOT_A_PRIMARY_OR_UNION_PATTERN_VARIABLE =
      "%s 为 不 primary 或 union pattern variable";
  public static final String NESTED_ROW_PATTERN_RECOGNITION_IN_ROW_PATTERN_RECOGNITION =
      "nested row 模式识别 在 row 模式识别";
  public static final String PATTERN_EXCLUSION_SYNTAX_IS_NOT_ALLOWED_WHEN_ALL_ROWS_PER_MATCH_WITH_UNMATCHED_ROWS_IS =
      "Pattern exclusion syntax 为 不允许 when ALL ROWS PER MATCH WITH UNMATCHED ROWS 为 specified";
  public static final String COLUMN_S_CANNOT_BE_RESOLVED =
      "列 '%s' 不能 be resolved";
  public static final String REFERENCE_TO_COLUMN_S_FROM_OUTER_SCOPE_NOT_ALLOWED_IN_THIS_CONTEXT =
      "Reference 到 列 '%s' 来自 outer scope 不允许 在 this context";
  public static final String COLUMN_S_PREFIXED_WITH_LABEL_S_CANNOT_BE_RESOLVED =
      "列 %s prefixed 与 label %s 不能 be resolved";
  public static final String EXPRESSION_S_IS_NOT_OF_TYPE_ROW =
      "表达式 %s 为 不 的 类型 ROW";
  public static final String AMBIGUOUS_ROW_FIELD_REFERENCE_S =
      "Ambiguous row field reference: %s";
  public static final String TYPES_ARE_NOT_COMPARABLE_WITH_NULLIF_S_VS_S =
      "类型s 为 不 comparable 与 NULLIF: %s vs %s";
  public static final String CASE_OPERAND_TYPE_DOES_NOT_MATCH_WHEN_CLAUSE_OPERAND_TYPE_S_VS_S =
      "CASE operand 类型 不匹配 WHEN clause operand 类型: %s vs %s";
  public static final String UNARY_OPERATOR_CANNOT_BY_APPLIED_TO_S_TYPE =
      "Unary '+' 操作符 不能 由 applied 到 %s 类型";
  public static final String LEFT_SIDE_OF_LIKE_EXPRESSION_MUST_EVALUATE_TO_TEXT_OR_STRING_TYPE_ACTUAL_S =
      "Left side 的 LIKE 表达式 必须 evaluate 到 TEXT 或 STRING 类型 (实际: %s)";
  public static final String PATTERN_FOR_LIKE_EXPRESSION_MUST_EVALUATE_TO_TEXT_OR_STRING_TYPE_ACTUAL_S =
      "Pattern 用于 LIKE 表达式 必须 evaluate 到 TEXT 或 STRING 类型 (实际: %s)";
  public static final String ESCAPE_FOR_LIKE_EXPRESSION_MUST_EVALUATE_TO_TEXT_OR_STRING_TYPE_ACTUAL_S =
      "Escape 用于 LIKE 表达式 必须 evaluate 到 TEXT 或 STRING 类型 (实际: %s)";
  public static final String LABEL_STAR_SYNTAX_IS_ONLY_SUPPORTED_AS_THE_ONLY_ARGUMENT_OF_ROW_PATTERN_COUNT_FUNCTION =
      "label.* syntax 为 仅支持ed 作为 only 参数 的 row pattern count 函数";
  public static final String CANNOT_USE_DISTINCT_WITH_AGGREGATE_FUNCTION_IN_PATTERN_RECOGNITION_CONTEXT =
      "不能 use DISTINCT 与 aggregate 函数 在 模式识别 context";
  public static final String S_SEMANTICS_IS_NOT_SUPPORTED_OUT_OF_PATTERN_RECOGNITION_CONTEXT =
      "%s semantics 为 不支持 out 的 模式识别 context";
  public static final String S_SEMANTICS_IS_SUPPORTED_ONLY_FOR_FIRST_LAST_AND_AGGREGATION_FUNCTIONS_ACTUAL_S =
      "%s semantics 为 supported only 用于 FIRST(), LAST() 和 aggregation 函数s. Actual: %s";
  public static final String THE_SECOND_ARGUMENT_OF_S_FUNCTION_MUST_BE_ACTUAL_TIME_NAME =
      "second 参数 的 %s 函数 必须为 实际 时间 name";
  public static final String THE_THIRD_ARGUMENT_OF_S_FUNCTION_MUST_BE_ACTUAL_TIME_NAME =
      "third 参数 的 %s 函数 必须为 实际 时间 name";
  public static final String TOO_MANY_ARGUMENTS_FOR_FUNCTION_CALL_S =
      "Too many 参数s 用于 函数 call %s()";
  public static final String S_IS_NOT_A_PRIMARY_PATTERN_VARIABLE_OR_SUBSET_NAME =
      "%s 为 不 主模式变量或子集名称";
  public static final String MISSING_VALID_TIME_COLUMN_THE_TABLE_MUST_CONTAIN_EITHER_A_COLUMN_WITH_THE_TIME_CATEGORY =
      "缺少 valid 时间 列. 表 必须 contain either 列 与 TIME category 或 在 least one TIMESTAMP 列.";
  public static final String CLASSIFIER_PATTERN_RECOGNITION_FUNCTION_TAKES_NO_ARGUMENTS_OR_1_ARGUMENT =
      "CLASSIFIER 模式识别 函数 takes no 参数s 或 1 参数";
  public static final String CLASSIFIER_FUNCTION_ARGUMENT_SHOULD_BE_PRIMARY_PATTERN_VARIABLE_OR_SUBSET_NAME_ACTUAL_S =
      "CLASSIFIER 函数 参数 应为 主模式变量或子集名称. Actual: %s";
  public static final String CANNOT_USE_DISTINCT_WITH_S_PATTERN_RECOGNITION_FUNCTION =
      "不能 use DISTINCT 与 %s 模式识别 函数";
  public static final String S_SEMANTICS_IS_NOT_SUPPORTED_WITH_S_PATTERN_RECOGNITION_FUNCTION =
      "%s semantics 为 不支持 与 %s 模式识别 函数";
  public static final String S_PATTERN_RECOGNITION_FUNCTION_REQUIRES_1_OR_2_ARGUMENTS =
      "%s 模式识别 函数 requires 1 或 2 参数s";
  public static final String S_PATTERN_RECOGNITION_NAVIGATION_FUNCTION_REQUIRES_A_NUMBER_AS_THE_SECOND_ARGUMENT =
      "%s 模式识别 navigation 函数 requires number 作为 second 参数";
  public static final String S_PATTERN_RECOGNITION_NAVIGATION_FUNCTION_REQUIRES_A_NON_NEGATIVE_NUMBER_AS_THE_SECOND =
      "%s 模式识别 navigation 函数 requires non-negative number 作为 second 参数 (实际: %s)";
  public static final String THE_SECOND_ARGUMENT_OF_S_PATTERN_RECOGNITION_NAVIGATION_FUNCTION_MUST_NOT_EXCEED_S =
      "second 参数 的 %s 模式识别 navigation 函数 必须 不 exceed %s (实际: %s)";
  public static final String CANNOT_NEST_S_PATTERN_NAVIGATION_FUNCTION_INSIDE_S_PATTERN_NAVIGATION_FUNCTION =
      "不能 nest %s 模式导航函数 inside %s 模式导航函数";
  public static final String CANNOT_NEST_MULTIPLE_PATTERN_NAVIGATION_FUNCTIONS_INSIDE_S_PATTERN_NAVIGATION_FUNCTION =
      "不能 nest multiple 模式导航函数s inside %s 模式导航函数";
  public static final String IMMEDIATE_NESTING_IS_REQUIRED_FOR_PATTERN_NAVIGATION_FUNCTIONS =
      "Immediate nesting 为 required 用于 模式导航函数s";
  public static final String ALL_LABELS_AND_CLASSIFIERS_INSIDE_THE_CALL_TO_S_MUST_MATCH =
      "All labels 和 classifiers inside call 到 '%s' 必须 match";
  public static final String ALL_AGGREGATE_FUNCTION_ARGUMENTS_MUST_APPLY_TO_ROWS_MATCHED_WITH_THE_SAME_LABEL =
      "All aggregate 函数 参数s 必须 apply 到 rows matched 与 same label";
  public static final String CANNOT_NEST_S_AGGREGATE_FUNCTION_INSIDE_S_FUNCTION =
      "不能 nest %s aggregate 函数 inside %s 函数";
  public static final String CANNOT_NEST_S_PATTERN_NAVIGATION_FUNCTION_INSIDE_S_FUNCTION =
      "不能 nest %s 模式导航函数 inside %s 函数";
  public static final String INVALID_PARAMETER_INDEX_S_MAX_VALUE_IS_S =
      "无效 参数 index %s, max 值 为 %s";
  public static final String CANNOT_CHECK_IF_S_IS_BETWEEN_S_AND_S =
      "不能 check if %s 为 BETWEEN %s 和 %s";
  public static final String S_IS_NOT_COMPARABLE_AND_THEREFORE_CANNOT_BE_USED_IN_WINDOW_FUNCTION_PARTITION_BY =
      "%s 为 不 comparable, 和 therefore 不能 be used 在 窗口函数 PARTITION BY";
  public static final String S_IS_NOT_ORDERABLE_AND_THEREFORE_CANNOT_BE_USED_IN_WINDOW_FUNCTION_ORDER_BY =
      "%s 为 不 orderable, 和 therefore 不能 be used 在 窗口函数 ORDER BY";
  public static final String WINDOW_FRAME_STARTING_FROM_CURRENT_ROW_CANNOT_END_WITH_PRECEDING =
      "Window frame starting 来自 CURRENT ROW 不能 end 与 PRECEDING";
  public static final String WINDOW_FRAME_STARTING_FROM_FOLLOWING_CANNOT_END_WITH_PRECEDING =
      "Window frame starting 来自 FOLLOWING 不能 end 与 PRECEDING";
  public static final String WINDOW_FRAME_STARTING_FROM_FOLLOWING_CANNOT_END_WITH_CURRENT_ROW =
      "Window frame starting 来自 FOLLOWING 不能 end 与 CURRENT ROW";
  public static final String WINDOW_FRAME_ROWS_START_VALUE_TYPE_MUST_BE_EXACT_NUMERIC_TYPE_WITH_SCALE_0_ACTUAL_S =
      "Window frame ROWS start 值 类型 必须为 exact numeric 类型 与 scale 0 (实际 %s)";
  public static final String WINDOW_FRAME_ROWS_END_VALUE_TYPE_MUST_BE_EXACT_NUMERIC_TYPE_WITH_SCALE_0_ACTUAL_S =
      "Window frame ROWS end 值 类型 必须为 exact numeric 类型 与 scale 0 (实际 %s)";
  public static final String WINDOW_FRAME_OF_TYPE_GROUPS_PRECEDING_OR_FOLLOWING_REQUIRES_ORDER_BY =
      "Window frame 的 类型 GROUPS PRECEDING 或 FOLLOWING requires ORDER BY";
  public static final String WINDOW_FRAME_GROUPS_START_VALUE_TYPE_MUST_BE_EXACT_NUMERIC_TYPE_WITH_SCALE_0_ACTUAL_S =
      "Window frame GROUPS start 值 类型 必须为 exact numeric 类型 与 scale 0 (实际 %s)";
  public static final String WINDOW_FRAME_OF_TYPE_RANGE_PRECEDING_OR_FOLLOWING_REQUIRES_ORDER_BY =
      "Window frame 的 类型 RANGE PRECEDING 或 FOLLOWING requires ORDER BY";
  public static final String WINDOW_FRAME_OF_TYPE_RANGE_PRECEDING_OR_FOLLOWING_REQUIRES_SINGLE_SORT_ITEM_IN_ORDER_BY =
      "Window frame 的 类型 RANGE PRECEDING 或 FOLLOWING requires single sort item 在 ORDER BY (实际: %s)";
  public static final String WINDOW_FRAME_OF_TYPE_RANGE_PRECEDING_OR_FOLLOWING_REQUIRES_THAT_SORT_ITEM_TYPE_BE =
      "Window frame 的 类型 RANGE PRECEDING 或 FOLLOWING requires that sort item 类型 be numeric, date时间 或 interval (实际: %s)";
  public static final String WINDOW_FRAME_RANGE_VALUE_TYPE_S_NOT_COMPATIBLE_WITH_SORT_ITEM_TYPE_S =
      "Window frame RANGE 值 类型 (%s) 不 compatible 与 sort item 类型 (%s)";
  public static final String TYPE_S_MUST_BE_ORDERABLE_IN_ORDER_TO_BE_USED_IN_QUANTIFIED_COMPARISON =
      "类型 [%s] 必须为 orderable 在 order 到 be used 在 quantified comparison";
  public static final String TYPE_S_MUST_BE_COMPARABLE_IN_ORDER_TO_BE_USED_IN_QUANTIFIED_COMPARISON =
      "类型 [%s] 必须为 comparable 在 order 到 be used 在 quantified comparison";
  public static final String NOT_YET_IMPLEMENTED_S =
      "not yet implemented: %s";
  public static final String S_MUST_EVALUATE_TO_A_S_ACTUAL_S =
      "%s 必须 evaluate 到 %s (实际: %s)";
  public static final String S_MUST_BE_THE_SAME_TYPE_OR_COERCIBLE_TO_A_COMMON_TYPE_CANNOT_FIND_COMMON_TYPE_BETWEEN_S =
      "%s 必须为 same 类型 或 coercible 到 common 类型. 不能 find common 类型 介于 %s 和 %s, all 类型s (without 重复s): %s";
  public static final String PATTERN_RECOGNITION_FUNCTION_NAME_MUST_NOT_BE_QUALIFIED =
      "Pattern recognition 函数 name 必须 不 be qualified: ";
  public static final String PATTERN_RECOGNITION_FUNCTION_NAME_MUST_NOT_BE_DELIMITED =
      "Pattern recognition 函数 name 必须 不 be delimited: ";
  public static final String PATTERN_RECOGNITION_FUNCTION_NAMES_CANNOT_BE_LAST_OR_FIRST_USE_RPR_LAST_OR_RPR_FIRST =
      "Pattern recognition 函数 names 不能 be LAST 或 FIRST, use RPR_LAST 或 RPR_FIRST instead.";
  public static final String PARAMETER_NODE_MUST_HAVE_A_LOCATION =
      "参数节点必须包含位置信息";
  public static final String INVALID_NUMBER_OF_PARAMETERS_EXPECTED_D_GOT_D =
      "无效 number 的 参数s: 期望 %d, got %d";
  public static final String CANNOT_INSERT_IDENTIFIER_S_PLEASE_USE_STRING_LITERAL =
      "不能 insert identifier %s, 请使用 string 字面量";
  public static final String EXPRESSIONS_AND_COLUMNS_DO_NOT_MATCH_EXPRESSIONS_SIZE =
      "表达式s 和 列s 不匹配, 表达式s size: ";
  public static final String COLUMNS_SIZE =
      ", 列s size: ";
  public static final String TIMECOLUMNINDEX_OUT_OF_BOUND_D_D =
      "时间列Index out 的 bound: %d-%d";
  public static final String INCONSISTENT_NUMBERS_OF_NON_TIME_COLUMN_NAMES_AND_VALUES_D_D =
      "Inconsistent numbers 的 non-时间 列 names 和 值s: %d-%d";
  public static final String IS_NOT_SUPPORTED_FOR_PROPERTY_VALUE_OF_SET_CONFIGURATION =
      " 为 不支持 用于 属性 值 的 'set configuration'. ";
  public static final String NOTE_THAT_THE_SYNTAX_FOR_SET_CONFIGURATION_IN_THE_TREE_MODEL_IS_NOT_EXACTLY_THE_SAME_AS =
      "Note that syntax 用于 'set configuration' 在 tree model 为 不 exactly same 作为 that 在 表 model.";
  public static final String UNSUPPORTED_COPY_TO_FORMAT_S_SUPPORTED_FORMATS_S =
      "不支持 COPY TO format '%s'. Supported formats: %s";
  public static final String SIMULTANEOUS_SETTING_OF_MONTHLY_AND_NON_MONTHLY_INTERVALS_IS_NOT_SUPPORTED =
      "不支持同时设置月级和非月级时间间隔。";
  public static final String DON_T_NEED_TO_SPECIFY_TIME_COLUMN_WHILE_EITHER_TIME_BOUND_OR_FILL_GROUP_PARAMETER_IS_NOT =
      "未指定 TIME_BOUND 或 FILL_GROUP 参数时，无需指定 TIME_COLUMN";
  public static final String MONTH_OR_YEAR_INTERVAL_IN_TOLERANCE_IS_NOT_SUPPORTED_NOW =
      "目前不支持在 tolerance 中使用月或年时间间隔。";
  public static final String ASOF_JOIN_DOES_NOT_SUPPORT_S_TYPE_NOW =
      "ASOF JOIN does 不 support %s 类型 now";
  public static final String THE_SECOND_ARGUMENT_OF_APPROX_COUNT_DISTINCT_FUNCTION_MUST_BE_A_LITERAL =
      "second 参数 的 'approx_count_distinct' 函数 必须为 字面量";
  public static final String THE_SECOND_AND_THIRD_ARGUMENT_OF_APPROX_MOST_FREQUENT_FUNCTION_MUST_BE_POSITIVE_INTEGER =
      "second 和 third 参数 的 'approx_most_frequent' 函数 必须为 positive integer 字面量";
  public static final String THE_SECOND_ARGUMENT_OF_APPROX_PERCENTILE_FUNCTION_PERCENTAGE_MUST_BE_A_DOUBLE_LITERAL =
      "second 参数 的 'approx_percentile' 函数 percentage 必须为 double 字面量";
  public static final String THE_THIRD_ARGUMENT_OF_APPROX_PERCENTILE_FUNCTION_PERCENTAGE_MUST_BE_A_DOUBLE_LITERAL =
      "third 参数 的 'approx_percentile' 函数 percentage 必须为 double 字面量";
  public static final String INCONSISTENT_COLUMN_CATEGORY_OF_COLUMN_S_S_S =
      "Inconsistent 列 category 的 列 %s: %s/%s";
  public static final String COLUMN =
      "列 ";
  public static final String DOES_NOT_EXISTS_OR_FAILS_TO_BE =
      " 不存在s 或 fails 到 be ";
  public static final String CREATED =
      "created";
  public static final String INCOMPATIBLE_DATA_TYPE_OF_COLUMN_S_S_S =
      "不兼容 数据类型 的 列 %s: %s/%s";
  public static final String INLIST_LITERAL_FOR_TIMESTAMP_CAN_ONLY_BE_LONGLITERAL_DOUBLELITERAL_AND_GENERICLITERAL =
      "InList 字面量 用于 TIMESTAMP 只能 be Long字面量, Double字面量 和 Generic字面量, 当前 为 ";
  public static final String THE_TIME_FIELD_COLUMNS_ARE_CURRENTLY_NOT_ALLOWED_IN_DEVICES_RELATED_OPERATIONS =
      "TIME/FIELD 列s 为 当前ly 不允许 在 设备s related operations";
  public static final String UNKNOWN_COLUMN_CATEGORY_FOR_S_CANNOT_AUTO_CREATE_COLUMN =
      "列 %s 的类别未知，无法自动创建列。";
  public static final String UNKNOWN_COLUMN_DATA_TYPE_FOR_S_CANNOT_AUTO_CREATE_COLUMN =
      "未知 列 数据类型 用于 %s. 不能 auto create 列.";
  public static final String WRONG_CATEGORY_AT_COLUMN_S =
      "列 %s 的类别错误。";
  public static final String MISSING_COLUMNS_S =
      "缺少列 %s。";
  public static final String DATATYPE_OF_TAG_COLUMN_SHOULD_ONLY_BE_STRING_CURRENT_IS =
      "TAG 列的数据类型只能是 STRING，当前为 ";
  public static final String DATATYPE_OF_ATTRIBUTE_COLUMN_SHOULD_ONLY_BE_STRING_CURRENT_IS =
      "ATTRIBUTE 列的数据类型只能是 STRING，当前为 ";
  public static final String DECORRELATION_FOR_LIMIT_WITH_ROW_COUNT_GREATER_THAN_1_IS_NOT_SUPPORTED_YET =
      "Decorrelation 用于 LIMIT 与 row count 大于 1 为 不支持 yet";
  public static final String GIVEN_CORRELATED_SUBQUERY_IS_NOT_SUPPORTED =
      "Given correlated sub查询 为 不支持";
  public static final String GIVEN_QUERIED_DATABASE_S_IS_NOT_EXIST =
      "查询的数据库 %s 不存在！";

  // --- QueryEngine log messages (additional) ---
  public static final String INITIALIZED_SHARED_MEMORYBLOCK_COORDINATOR_WITH_ALL_AVAILABLE_MEMORY_ARG_BYTES =
      "已使用全部可用内存初始化共享 MemoryBlock 'Coordinator'：{} 字节";
  public static final String ERROR_OCCURRED_DURING_EXECUTING_UDAF_PLEASE_CHECK_WHETHER_THE_IMPLEMENTATION_OF_UDF_IS =
      "执行 UDAF 时发生错误，请根据 udf-api 描述检查 UDF 实现是否正确。";
  public static final String PROCESSGETTSBLOCKREQUEST_SEQUENCE_ID_IN_ARG_ARG =
      "[ProcessGetTsBlockRequest] sequence ID 在 [{}, {})";
  public static final String RECEIVED_ACKNOWLEDGEDATABLOCKEVENT_FOR_TSBLOCKS_WHOSE_SEQUENCE_ID_ARE_IN_ARG_ARG_FROM =
      "已收到 AcknowledgeDataBlockEvent 用于 TsBlocks whose sequence ID 为 在 [{}, {}) 来自 {}.";
  public static final String RECEIVED_ACK_EVENT_BUT_TARGET_FRAGMENTINSTANCE_ARG_IS_NOT_FOUND =
      "已收到 ACK event but target FragmentInstance[{}] 为 未找到.";
  public static final String CLOSED_SOURCE_HANDLE_OF_SHUFFLESINKHANDLE_ARG_CHANNEL_INDEX_ARG =
      "已关闭 source handle 的 ShuffleSinkHandle {}, channel index: {}.";
  public static final String RECEIVED_CLOSESINKCHANNELEVENT_BUT_TARGET_FRAGMENTINSTANCE_ARG_IS_NOT_FOUND =
      "已收到 CloseSinkChannelEvent but target FragmentInstance[{}] 为 未找到.";
  public static final String NEW_DATA_BLOCK_EVENT_RECEIVED_FOR_PLAN_NODE_ARG_OF_ARG_FROM_ARG =
      "New data block event 已收到, 用于 plan 节点 {} 的 {} 来自 {}.";
  public static final String RECEIVED_NEWDATABLOCKEVENT_BUT_THE_DOWNSTREAM_FRAGMENTINSTANCE_ARG_IS_NOT_FOUND =
      "已收到 NewDataBlockEvent but the downstream FragmentInstance[{}] 为 未找到";
  public static final String END_OF_DATA_BLOCK_EVENT_RECEIVED_FOR_PLAN_NODE_ARG_OF_ARG_FROM_ARG =
      "End 的 data block event 已收到, 用于 plan 节点 {} 的 {} 来自 {}.";
  public static final String RECEIVED_ONENDOFDATABLOCKEVENT_BUT_THE_DOWNSTREAM_FRAGMENTINSTANCE_ARG_IS_NOT_FOUND =
      "已收到 onEndOfDataBlockEvent but the downstream FragmentInstance[{}] 为 未找到";
  public static final String CREATE_SOURCE_HANDLE_FROM_ARG_FOR_PLAN_NODE_ARG_OF_ARG =
      "创建 source handle 来自 {} 用于 plan 节点 {} 的 {}";
  public static final String THE_TASK_ARG_IS_ABORTED_ALL_OTHER_TASKS_IN_THE_SAME_QUERY_WILL_BE_CANCELLED =
      "任务 {} 已中止，同一查询中的其他任务也将被取消";
  public static final String DRIVERTASKTIMEOUT_CURRENT_TIME_IS_ARG_DDL_OF_TASK_IS_ARG =
      "[Driver任务Timeout] 当前 time 为 {}, ddl 的 任务 为 {}";
  public static final String EXECUTOR_ARG_EXITS_BECAUSE_IT_S_INTERRUPTED_WE_WILL_PRODUCE_ANOTHER_THREAD_TO_REPLACE =
      "Executor {} exits 原因 it's interrupted. We will produce another thread 到 replace.";
  public static final String CANNOT_RESERVE_ARG_MAX_ARG_BYTES_MEMORY_FROM_MEMORYPOOL_FOR_PLANNODEIDARG =
      "Cannot reserve {}(Max: {}) 字节 内存 来自 内存Pool 用于 plan节点Id{}";
  public static final String BLOCKED_RESERVE_REQUEST_ARG_BYTES_MEMORY_FOR_PLANNODEIDARG =
      "Blocked reserve request: {} 字节 内存 用于 plan节点Id{}";
  public static final String FAILED_TO_DO_THE_INITIALIZATION_FOR_DRIVER_ARG =
      "未能 do the initialization 用于 driver {} ";
  public static final String FAILED_TO_ACQUIRE_THE_READ_LOCK_OF_DATAREGION_ARG_FOR_ARG_TIMES =
      "未能 acquire the read lock 的 DataRegion-{} 用于 {} times";
  public static final String INTERRUPTED_WHEN_AWAIT_ON_ALLDRIVERSCLOSED_FRAGMENTINSTANCE_ID_IS_ARG =
      "Interrupted 当 await on allDrivers已关闭, FragmentInstance Id 为 {}";
  public static final String RELEASE_TVLIST_OWNED_BY_QUERY_ALLOCATE_SIZE_ARG_RELEASE_SIZE_ARG =
      "Release TVList owned by 查询: allocate size {}, release size {}";
  public static final String TVLIST_ARG_IS_RELEASED_BY_THE_QUERY_FRAGMENTINSTANCE_ID_IS_ARG =
      "TVList {} 为 released by the 查询, FragmentInstance Id 为 {}";
  public static final String MEMORYNOTENOUGHEXCEPTION_WHEN_TRANSFERRING_TVLIST_OWNERSHIP_FROM_QUERY_ARG_TO_ANOTHER =
      "内存NotEnough异常 当 transferring TVList ownership 来自 查询 {} 到 another 查询 {}.";
  public static final String UNEXPECTED_EXCEPTION_WHEN_TRANSFERRING_TVLIST_OWNERSHIP_FROM_QUERY_ARG_TO_ANOTHER_QUERY =
      "Unexpected 异常 当 transferring TVList ownership 来自 查询 {} 到 another 查询 {}.";
  public static final String TVLIST_ARG_IS_NOW_OWNED_BY_ANOTHER_QUERY_FRAGMENTINSTANCE_ID_IS_ARG =
      "TVList {} 为 now owned by another 查询, FragmentInstance Id 为 {}";
  public static final String GETTSBLOCKFROMQUEUE_TSBLOCK_ARG_SIZE_ARG =
      "[GetTsBlockFromQueue] TsBlock:{} size:{}";
  public static final String RECEIVENEWTSBLOCKNOTIFICATION_ARG_ARG_EACH_SIZE_IS_ARG =
      "[ReceiveNewTsBlockNotification] [{}, {}), each size is: {}";
  public static final String STARTPULLTSBLOCKSFROMREMOTE_ARG_ARG_ARG_ARG =
      "[StartPullTsBlocksFromRemote] {}-{} [{}, {}) ";
  public static final String SENDCLOSESINKCHANNELEVENT_TO_SHUFFLESINKHANDLE_ARG_INDEX_ARG =
      "[SendCloseSinkChannelEvent] 到 [ShuffleSinkHandle: {}, index: {}]).";
  public static final String SINKCHANNEL_STILL_RECEIVE_GETTING_TSBLOCK_REQUEST_AFTER_BEING_ABORTED_ARG_OR_CLOSED_ARG =
      "SinkChannel still receive getting TsBlock request after being 中止={} 或 关闭={}";
  public static final String NOTIFYNEWTSBLOCK_ARG_ARG_TO_ARG_ARG =
      "[NotifyNewTsBlock] [{}, {}) 到 {}.{}";
  public static final String PLAINSHUFFLESTRATEGY_NEEDS_TO_DO_NOTHING_CURRENT_CHANNEL_INDEX_IS_ARG =
      "PlainShuffleStrategy needs 到 do nothing, 当前 channel index 为 {}";
  public static final String LAYERROWWINDOWREADER_INDEX_OVERFLOW_BEGININDEX_ARG_ENDINDEX_ARG_WINDOWSIZE_ARG =
      "LayerRowWindowReader index overflow. beginIndex: {}, endIndex: {}, windowSize: {}.";
  public static final String CONSUMEMEMORY_CONSUME_ARG_CURRENT_REMAINING_MEMORY_ARG =
      "[Consume内存] consume: {}, 当前 remaining 内存: {}";
  public static final String RELEASEMEMORY_RELEASE_ARG_CURRENT_REMAINING_MEMORY_ARG =
      "[Release内存] release: {}, 当前 remaining 内存: {}";
  public static final String MAXBYTESONEHANDLECANRESERVE_FOR_EXCHANGEOPERATOR_IS_ARG_EXCHANGESUMNUM_IS_ARG =
      "MaxBytesOneHandleCanReserve 用于 ExchangeOperator 为 {}, exchangeSumNum 为 {}.";
  public static final String STATE_TRACKER_STARTS =
      "状态跟踪器启动";
  public static final String DISPATCH_WRITE_FAILED_STATUS_ARG_CODE_ARG_MESSAGE_ARG_NODE_ARG =
      "分发 write failed. 状态: {}, code: {}, 消息: {}, 节点 {}";
  public static final String CAN_T_EXECUTE_REQUEST_ON_NODE_ARG_IN_SECOND_TRY_ERROR_MSG_IS_ARG =
      "can't execute request on 节点 {} 在 second try, error msg 为 {}.";
  public static final String CAN_T_EXECUTE_REQUEST_ON_NODE_ARG_ERROR_MSG_IS_ARG_AND_WE_TRY_TO_RECONNECT_THIS_NODE =
      "can't execute request on 节点 {}, error msg 为 {}, 和 we try 到 reconnect this 节点.";
  public static final String WRITE_LOCALLY_FAILED_TSSTATUS_ARG_MESSAGE_ARG =
      "write locally failed. TS状态: {}, 消息: {}";
  public static final String DISPATCH_WRITE_FAILED_MESSAGE_ARG_NODE_ARG =
      "分发 write failed. 消息: {}, 节点 {}";
  public static final String DISPATCH_WRITE_FAILED_STATUS_ARG_CODE_ARG_MESSAGE_ARG_NODE_ARG_2 =
      "分发 write failed. 状态: {}, code: {}, 消息: {}, 节点 {}";
  public static final String LOGICAL_PLAN_IS_ARG =
      "logical plan is: \n {}";
  public static final String DISTRIBUTION_PLAN_DONE_FRAGMENT_INSTANCE_COUNT_IS_ARG_DETAILS_IS_ARG =
      "distribution plan done. Fragment instance count 为 {}, details is: \n {}";
  public static final String FAILED_TO_CHECK_TABLE_SCHEMA_WILL_SKIP_BECAUSE_SKIPFAILEDTABLESCHEMACHECK_IS_SET_TO_TRUE =
      "未能 check 表 元数据, will skip 原因 skipFailed表元数据Check 为 set 到 true, 消息: {}";
  public static final String FAILED_TO_CHECK_IF_DEVICE_ARG_IS_DELETED_BY_MODS_WILL_SEE_IT_AS_NOT_DELETED =
      "未能 check if 设备 {} 为 deleted by mods. Will see it as not deleted.";
  public static final String COLUMN_ARG_IN_TABLE_ARG_IS_NOT_FOUND_IN_IOTDB_WHILE_LOADING_TSFILE =
      "Column {} 在 表 {} 为 未找到 在 IoTDB 当 加载ing TsFile.";
  public static final String DEVICE_ARG_IS_NOT_IN_THE_TSFILEDEVICE2ISALIGNED_CACHE_ARG =
      "设备 {} 为 不在 the tsFile设备2IsAligned cache {}.";
  public static final String LOADTSFILEANALYZER_CURRENT_DATANODE_IS_READ_ONLY_WILL_TRY_TO_CONVERT_TO_TABLETS_AND =
      "加载TsFileAnalyzer: 当前 data节点 为 read only, 将尝试 到 convert 到 表ts 和 insert later.";
  public static final String LOAD_ANALYSIS_STAGE_ARG_ARG_TSFILES_HAVE_BEEN_ANALYZED_PROGRESS_ARG_PERCENT =
      "加载 - Analysis Stage: {}/{} tsfiles have been analyzed, 进度: {}%";
  public static final String THE_FILE_ARG_IS_NOT_A_VALID_TSFILE_PLEASE_CHECK_THE_INPUT_FILE =
      "The file {} 为 not a valid tsfile. Please check the input file.";
  public static final String TSFILE_ARG_IS_A_ARG_MODEL_FILE =
      "TsFile {} 为 a {}-model file.";
  public static final String LOAD_FAILED_TO_CONVERT_MINI_TSFILE_ARG_TO_TABLETS_FROM_STATEMENT_ARG_STATUS_ARG =
      "加载: 未能 convert mini tsfile {} 到 表ts 来自 statement {}. 状态: {}.";
  public static final String LOAD_FAILED_TO_CONVERT_TO_TABLETS_FROM_STATEMENT_ARG_BECAUSE_FAILED_TO_READ_MODEL_INFO =
      "加载: 未能 convert 到 表ts 来自 statement {} 原因 未能 read model info 来自 file, 消息: {}.";
  public static final String LOAD_FAILED_TO_CONVERT_TO_TABLETS_FROM_STATEMENT_ARG_STATUS_IS_NULL =
      "加载: 未能 convert 到 表ts 来自 statement {}. 状态 为 null.";
  public static final String LOAD_FAILED_TO_CONVERT_TO_TABLETS_FROM_STATEMENT_ARG_STATUS_ARG =
      "加载: 未能 convert 到 表ts 来自 statement {}. 状态: {}";
  public static final String LOAD_FAILED_TO_CONVERT_TO_TABLETS_FROM_STATEMENT_ARG_BECAUSE_EXCEPTION_ARG =
      "加载: 未能 convert 到 表ts 来自 statement {} 原因 异常: {}";
  public static final String FAILED_TO_CHECK_IF_DEVICE_ARG_TIMESERIES_ARG_IS_DELETED_BY_MODS_WILL_SEE_IT_AS_NOT =
      "未能 check if 设备 {}, timeseries {} 为 deleted by mods. Will see it as not deleted.";
  public static final String CREATE_DATABASE_ERROR_STATEMENT_ARG_RESULT_STATUS_IS_ARG =
      "创建 数据库 error, statement: {}, result 状态 is: {}";
  public static final String MEASUREMENT_ARGARGARG_DATATYPE_NOT_MATCH_TSFILE_ARG_IOTDB_ARG =
      "Measurement {}{}{} datatype 不匹配, TsFile: {}, IoTDB: {}";
  public static final String ENCODING_TYPE_NOT_MATCH_MEASUREMENT_ARGARGARG =
      "Encoding type 不匹配, measurement: {}{}{}, ";
  public static final String TSFILE_ENCODING_ARG_IOTDB_ENCODING_ARG =
      "TsFile encoding: {}, IoTDB encoding: {}";
  public static final String COMPRESSOR_NOT_MATCH_MEASUREMENT_ARGARGARG =
      "Compressor 不匹配, measurement: {}{}{}, ";
  public static final String TSFILE_COMPRESSOR_ARG_IOTDB_COMPRESSOR_ARG =
      "TsFile compressor: {}, IoTDB compressor: {}";
  public static final String ARG_CACHE_FAILED_TO_CREATE_DATABASE_ARG =
      "[{} Cache] 未能 创建 数据库 {}";
  public static final String ARG_CACHE_MISS_WHEN_SEARCH_DEVICE_ARG =
      "[{} Cache] miss 当 search 设备 {}";
  public static final String ARG_CACHE_HIT_WHEN_SEARCH_DEVICE_ARG =
      "[{} Cache] hit 当 search 设备 {}";
  public static final String UNEXPECTED_ERROR_WHEN_GETREGIONREPLICASET_STATUS_ARG_REGIONMAP_ARG =
      "发生意外错误 当 getRegionReplicaSet: 状态 {}， regionMap: {}";
  public static final String ARG_CACHE_MISS_WHEN_SEARCH_DATABASE_ARG =
      "[{} Cache] miss 当 search 数据库 {}";
  public static final String ARG_CACHE_MISS_WHEN_SEARCH_TIME_PARTITION_ARG =
      "[{} Cache] miss 当 search time partition {}";
  public static final String FAILURES_HAPPENED_DURING_RUNNING_CONFIGEXECUTION_WHEN_EXECUTING_ARG_MESSAGE_ARG_STATUS =
      "发生失败 期间 running ConfigExecution 当 executing {}, 消息: {}, 状态: {}";
  public static final String FAILURES_HAPPENED_DURING_RUNNING_CONFIGEXECUTION_WHEN_EXECUTING_ARG =
      "发生失败 期间 running ConfigExecution 当 executing {}.";
  public static final String FAILED_TO_EXECUTE_CREATE_DATABASE_ARG_IN_CONFIG_NODE_STATUS_IS_ARG =
      "未能 execute 创建 数据库 {} 在 config 节点, 状态 为 {}.";
  public static final String FAILED_TO_EXECUTE_ALTER_DATABASE_ARG_IN_CONFIG_NODE_STATUS_IS_ARG =
      "未能 execute alter 数据库 {} 在 config 节点, 状态 为 {}.";
  public static final String FAILED_TO_EXECUTE_DELETE_DATABASE_ARG_IN_CONFIG_NODE_STATUS_IS_ARG =
      "未能 execute delete 数据库 {} 在 config 节点, 状态 为 {}.";
  public static final String FAILED_TO_CREATE_FUNCTION_WHEN_TRY_TO_CREATE_ARG_ARG_INSTANCE_FIRST =
      "未能 创建 function 当 try 到 创建 {}({}) instance first.";
  public static final String FAILED_TO_GET_EXECUTABLE_FOR_TRIGGER_ARG_USING_URI_ARG =
      "未能 get execu表 用于 Trigger({}) using URI: {}.";
  public static final String FAILED_TO_CREATE_TRIGGER_WHEN_TRY_TO_CREATE_TRIGGER_ARG_INSTANCE_FIRST =
      "未能 创建 trigger 当 try 到 创建 trigger({}) instance first.";
  public static final String ARG_FAILED_TO_CREATE_TRIGGER_ARG_TSSTATUS_IS_ARG =
      "[{}] 未能 创建 trigger {}. TS状态 为 {}";
  public static final String FAILED_TO_GET_EXECUTABLE_FOR_PIPEPLUGIN_ARG_USING_URI_ARG =
      "未能 get execu表 用于 PipePlugin({}) using URI: {}.";
  public static final String FAILED_TO_CREATE_PIPEPLUGIN_ARG_BECAUSE_THIS_PLUGIN_IS_NOT_DESIGNED_FOR_ARG_MODEL =
      "未能 创建 PipePlugin({}) 原因 this plugin 为 not designed 用于 {} model.";
  public static final String FAILED_TO_CREATE_FUNCTION_WHEN_TRY_TO_CREATE_PIPEPLUGIN_ARG_INSTANCE_FIRST =
      "未能 创建 function 当 try 到 创建 PipePlugin({}) instance first.";
  public static final String FAILED_TO_CREATE_PIPEPLUGIN_ARG_ARG_BECAUSE_ARG =
      "未能 创建 PipePlugin {}({}) 原因 {}";
  public static final String ARG_FAILED_TO_DROP_PIPE_PLUGIN_ARG =
      "[{}] 未能 drop pipe plugin {}.";
  public static final String FAILED_TO_EXECUTE_ARG_ARG_IN_CONFIG_NODE_STATUS_IS_ARG =
      "未能 execute {} {} 在 config 节点, 状态 为 {}.";
  public static final String FAILED_TO_EXECUTE_ALTER_VIEW_ARG_BY_PIPE_STATUS_IS_ARG =
      "未能 execute alter view {} by pipe, 状态 为 {}.";
  public static final String THE_DATANODE_TO_BE_REMOVED_IS_NOT_IN_THE_CLUSTER_OR_THE_INPUT_FORMAT_IS_INCORRECT =
      "The Data节点 到 be removed 为 不在 the cluster, 或 the input format 为 incorrect.";
  public static final String SUBMIT_REMOVE_DATANODE_REQUEST_SUCCESSFULLY_BUT_THE_PROCESS_MAY_FAIL =
      "Submit remove-data节点 request 成功, but the process may fail. ";
  public static final String MORE_DETAILS_ARE_SHOWN_IN_THE_LOGS_OF_CONFIGNODE_LEADER_AND_REMOVED_DATANODE =
      "more details 为 shown 在 the logs 的 config节点-leader 和 removed-data节点, ";
  public static final String AND_AFTER_THE_PROCESS_OF_REMOVING_DATANODE_ENDS_SUCCESSFULLY =
      "and after the process 的 removing data节点 ends 成功, ";
  public static final String YOU_ARE_SUPPOSED_TO_DELETE_DIRECTORY_AND_DATA_OF_THE_REMOVED_DATANODE_MANUALLY =
      "you 为 supposed 到 delete directory 和 data 的 the removed-data节点 manually";
  public static final String THE_CONFIGNODE_TO_BE_REMOVED_IS_NOT_IN_THE_CLUSTER_OR_THE_INPUT_FORMAT_IS_INCORRECT =
      "The Config节点 到 be removed 为 不在 the cluster, 或 the input format 为 incorrect.";
  public static final String FAILED_TO_DROP_DATABASE_ARG_BECAUSE_IT_DOESN_T_EXIST =
      "未能 DROP DATABASE {}, 原因 it doesn't exist";
  public static final String FAILED_TO_ALLOCATE_ARG_BYTES_FROM_SHARED_MEMORYBLOCK_ARG_FOR_PREPAREDSTATEMENT_ARG =
      "未能 allocate {} 字节 来自 shared 内存Block '{}' 用于 PreparedStatement '{}'";
  public static final String ALLOCATED_ARG_BYTES_FOR_PREPAREDSTATEMENT_ARG_FROM_SHARED_MEMORYBLOCK_ARG =
      "Allocated {} 字节 用于 PreparedStatement '{}' 来自 shared 内存Block '{}'. ";
  public static final String RELEASED_ARG_BYTES_FROM_SHARED_MEMORYBLOCK_ARG_FOR_PREPAREDSTATEMENT =
      "Released {} 字节 来自 shared 内存Block '{}' 用于 PreparedStatement. ";
  public static final String ATTEMPTED_TO_RELEASE_MEMORY_FROM_SHARED_MEMORYBLOCK_ARG_BUT_IT_IS_RELEASED =
      "Attempted 到 release 内存 来自 shared 内存Block '{}' but it 为 released";
  public static final String RELEASED_ARG_PREPAREDSTATEMENT_S_ARG_BYTES_TOTAL_FOR_SESSION_ARG =
      "Released {} PreparedStatement(s) ({} 字节 total) 用于 session {}";
  public static final String THE_PREFIX_OF_SOURCEKEY_IS_NOT_SOURCE_PLEASE_CHECK_THE_PARAMETERS_PASSED_IN_ARG =
      "The prefix 的 sourceKey 为 not 'source.'. Please check the parameters passed in: {}";
  public static final String LOADTSFILESCHEDULER_REGION_MIGRATION_WAS_DETECTED_DURING_LOADING_TSFILE_ARG_WILL_CONVERT =
      "加载TsFileScheduler: Region migration was detected 期间 加载ing TsFile {}, will convert 到 insertion 到 avoid data loss";
  public static final String LOAD_TSFILE_ARG_SUCCESSFULLY_LOAD_PROCESS_ARG_ARG =
      "成功加载 TsFile {}，加载进度 [{}/{}]";
  public static final String CAN_NOT_LOAD_TSFILE_ARG_LOAD_PROCESS_ARG_ARG =
      "Can not 加载 TsFile {}, 加载 process [{}/{}]";
  public static final String LOAD_TSFILE_S_FAILED_WILL_TRY_TO_CONVERT_TO_TABLETS_AND_INSERT_FAILED_TSFILES_ARG =
      "加载 TsFile 失败，将尝试转换为 tablet 并写入。失败的 TsFile：{}";
  public static final String DISPATCH_TSFILEDATA_ERROR_WHEN_PARSING_TSFILE_S =
      "分发 TsFileData error 当 parsing TsFile %s.";
  public static final String PARSE_OR_SEND_TSFILE_S_ERROR =
      "Parse 或 send TsFile %s error.";
  public static final String DISPATCH_ONE_PIECE_TO_REPLICASET_ARG_ERROR_RESULT_STATUS_CODE_ARG =
      "分发 one piece 到 ReplicaSet {} error. Result 状态 code {}. ";
  public static final String RESULT_STATUS_MESSAGE_ARG_DISPATCH_PIECE_NODE_ERROR_PERCENT_NARG =
      "Result 状态 消息 {}. 分发 piece 节点 error:%n{}";
  public static final String SUB_STATUS_CODE_ARG_SUB_STATUS_MESSAGE_ARG =
      "Sub 状态 code {}. Sub 状态 消息 {}.";
  public static final String WAIT_FOR_LOADING_S_TIME_OUT =
      "Wait 用于 加载ing %s time out.";
  public static final String DISPATCH_LOAD_COMMAND_ARG_OF_TSFILE_ARG_ERROR_TO_REPLICASETS_ARG_ERROR =
      "分发 加载 command {} 的 TsFile {} error 到 replicaSets {} error. ";
  public static final String RESULT_STATUS_CODE_ARG_RESULT_STATUS_MESSAGE_ARG =
      "Result 状态 code {}. Result 状态 消息 {}.";
  public static final String DISPATCH_TSFILE_S_ERROR_TO_LOCAL_ERROR_RESULT_STATUS_CODE_S =
      "分发 tsFile %s error 到 local error. Result 状态 code %s. ";
  public static final String RESULT_STATUS_MESSAGE_S =
      "Result 状态 消息 %s.";
  public static final String LOAD_SUCCESSFULLY_CONVERTED_TSFILE_ARG_INTO_TABLETS_AND_INSERTED =
      "加载: 成功 converted TsFile {} into 表ts 和 inserted.";
  public static final String LOAD_FAILED_TO_CONVERT_TO_TABLETS_FROM_TSFILE_ARG_STATUS_ARG =
      "加载: 未能 convert 到 表ts 来自 TsFile {}. 状态: {}";
  public static final String LOAD_FAILED_TO_CONVERT_TO_TABLETS_FROM_TSFILE_ARG_EXCEPTION_ARG =
      "加载: 未能 convert 到 表ts 来自 TsFile {}. 异常: {}";
  public static final String DISPATCH_PIECE_NODE_ARG_OF_TSFILE_ARG_ERROR =
      "分发 piece 节点 {} 的 TsFile {} error.";
  public static final String CANNOT_DISPATCH_LOADCOMMAND_FOR_LOAD_OPERATION_ARG =
      "Cannot 分发 加载Command 用于 加载 operation {}";
  public static final String LOAD_REMOTE_PROCEDURE_CALL_CONNECTION_TIMEOUT_IS_ADJUSTED_TO_ARG_MS_ARG_MINS =
      "加载 remote procedure call connection timeout 为 adjusted 到 {} ms ({} mins)";
  public static final String DATA_TYPE_OF_ARG_ARG_IS_NOT_CONSISTENT =
      "data type 的 {}.{} 为 not consistent, ";
  public static final String REGISTERED_TYPE_ARG_INSERTING_TIMESTAMP_ARG_VALUE_ARG =
      "registered type {}, inserting timestamp {}, value {}";
  public static final String TIMES_ARRAY_IS_NULL_OR_TOO_SMALL_TIMES_LENGTH_ARG_ROWSIZE_ARG_DEVICEID_ARG =
      "Times array 为 null 或 too small. times.length={}, rowSize={}, 设备Id={}";
  public static final String SERIALIZE_DATA_OF_TSFILE_S_ERROR_SKIP_TSFILEDATA_S =
      "Serialize data 的 TsFile %s error, skip TsFileData %s";
  public static final String FAIL_TO_MATERIALIZE_CTE_BECAUSE_THE_DATA_SIZE_EXCEEDED_MEMORY_OR_THE_ROW_COUNT_THRESHOLD =
      "Fail 到 materialize CTE 原因 the data size exceeded 内存 或 the row count threshold";
  public static final String UNEXPECTED_FAILURE_WHEN_HANDLING_PARSING_ERROR_THIS_IS_LIKELY_A_BUG_IN_THE =
      "Unexpected failure 当 handling parsing error. This 为 likely a bug 在 the implementation";
  public static final String AND_EXPRESSION_ENCOUNTERED_DURING_TAG_DETERMINED_CHECKING_WILL_BE_CLASSIFIED_INTO_FUZZY =
      "And expression encountered 期间 tag-determined checking, will be classified into fuzzy expression. Sql: {}";
  public static final String LOGICAL_EXPRESSION_TYPE_ENCOUNTERED_IN_NOT_EXPRESSION_CHILD_DURING_TAG_DETERMINED =
      "Logical expression type encountered 在 not expression child 期间 tag-determined checking, will be classified into fuzzy expression. Sql: {}";
  public static final String VALIDATING_DEVICE_SCHEMA_ARG_ARG_AND_OTHER_ARG_DEVICES =
      "Validating 设备 元数据 {}.{} 和 other {} 设备s";
  public static final String ILLEGAL_TABLEID_ARG_FOUND_IN_CACHE_WHEN_INVALIDATING_BY_PATH_ARG_INVALIDATE_IT_ANYWAY =
      "Illegal 表ID {} found 在 cache 当 invalidating by path {}, invalidate it anyway";
  public static final String ILLEGAL_DEVICEID_ARG_FOUND_IN_CACHE_WHEN_INVALIDATING_BY_PATH_ARG_INVALIDATE_IT_ANYWAY =
      "Illegal 设备ID {} found 在 cache 当 invalidating by path {}, invalidate it anyway";
  public static final String RULE_S_BEFORE_S_AFTER_S =
      "Rule: %s\nBefore:\n%s\nAfter:\n%s";

  private DataNodeQueryMessages() {}
  // ---------------------------------------------------------------------------
  // 补充异常消息
  // ---------------------------------------------------------------------------
  public static final String QUERY_EXCEPTION_FAILED_TO_SERIALIZE_INTERMEDIATE_RESULT_FOR_MAXBYACCUMULATOR_2F18B6E7 =
      "无法serialize intermediate result for MaxByAccumulator.";
  public static final String QUERY_EXCEPTION_UNSUPPORTED_DATA_TYPE_IN_AGGREGATION_AVG_S_D1DAD6A6 =
      "不支持的data type in aggregation AVG ：%s";
  public static final String QUERY_EXCEPTION_UNSUPPORTED_DATA_TYPE_IN_MINVALUE_S_BC092694 =
      "不支持的data type in MinValue：%s";
  public static final String QUERY_EXCEPTION_UNSUPPORTED_DATA_TYPE_IN_LASTVALUE_S_02ECF8E4 =
      "不支持的data type in LastValue：%s";
  public static final String QUERY_EXCEPTION_UNSUPPORTED_DATA_TYPE_IN_EXTREME_S_84B651D3 =
      "不支持的data type in Extreme：%s";
  public static final String QUERY_EXCEPTION_UNSUPPORTED_DATA_TYPE_IN_FIRSTVALUE_S_97025F25 =
      "不支持的data type in FirstValue：%s";
  public static final String QUERY_EXCEPTION_ERROR_OCCURRED_DURING_EXECUTING_UDAF_S_S_PLEASE_CHECK_WHETHER_9E9D20C6 =
      "在以下过程发生错误：executing UDAF#%s：%s, please check whether the implementation of UDF is correct "
          + "according to the udf-api description.";
  public static final String QUERY_EXCEPTION_UNSUPPORTED_EXPRESSION_TYPE_S_FD6F5B7C =
      "unsupported expression type：%s";
  public static final String QUERY_EXCEPTION_UNSUPPORTED_DATA_TYPE_IN_MAXVALUE_S_521AC345 =
      "不支持的data type in MaxValue：%s";
  public static final String QUERY_EXCEPTION_THERE_IS_NOT_ENOUGH_CPU_TO_EXECUTE_CURRENT_FRAGMENT_INSTANCE_E7719FB8 =
      "There is not enough cpu to execute 当前fragment instance";
  public static final String QUERY_EXCEPTION_THERE_IS_NO_ENOUGH_MEMORY_TO_EXECUTE_CURRENT_FRAGMENT_INSTANCE_CB632843 =
      "There is no enough 内存 to execute 当前fragment instance";
  public static final String QUERY_EXCEPTION_PLANNODE_RELATED_MEMORY_IS_NOT_ZERO_WHEN_TRYING_TO_DEREGISTER_E01109C5 =
      "PlanNode related 内存 is not zero when trying to deregister FI from query 内存 pool. QueryId is "
          + "：%s, FragmentInstanceId is ：%s, Non-zero PlanNode related 内存 is ：%s.";
  public static final String QUERY_EXCEPTION_QUERY_IS_ABORTED_SINCE_IT_REQUESTS_MORE_MEMORY_THAN_CAN_D77C2921 =
      "Query is aborted since it 请求s more 内存 than can be allocated, bytesToReserve：%sB, "
          + "maxBytesCanReserve：%sB";
  public static final String QUERY_EXCEPTION_RELATEDMEMORYRESERVED_CAN_T_BE_NULL_WHEN_FREEING_MEMORY_C80009F2 =
      "Related内存Reserved can't be null when freeing 内存";
  public static final String QUERY_EXCEPTION_INTERRUPTED_BY_92FAED2D = "Interrupted By";
  public static final String QUERY_EXCEPTION_DRIVER_WAS_INTERRUPTED_737358E4 =
      "Driver was interrupted";
  public static final String QUERY_EXCEPTION_UNSUPPORTED_QUERY_DATA_SOURCE_TYPE_S_7424E63F =
      "不支持的query data source type：%s";
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
      "不支持的data type in equal event aggregation ：%s";
  public static final String QUERY_EXCEPTION_UNSUPPORTED_DATA_TYPE_IN_VARIATION_EVENT_AGGREGATION_S_47341532 =
      "不支持的data type in variation event aggregation ：%s";
  public static final String QUERY_EXCEPTION_THE_OPERATOR_CANNOT_CONTINUE_UNTIL_THE_LAST_WRITE_OPERATION_1F241343 =
      "The operator cannot continue until the last 写入operation is done.";
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
      "Device entries of index %s 为空";
  public static final String QUERY_EXCEPTION_SHOULD_NOT_CALL_GETRESULTDATATYPES_METHOD_IN_DEVICEITERATORSCANOPERATOR_E915A153 =
      "Should not call getResultDataTypes() method in DeviceIteratorScanOperator";
  public static final String QUERY_EXCEPTION_UNEXPECTED_COLUMN_CATEGORY_S_6E60A44E =
      "Unexpected column category：%s";
  public static final String QUERY_EXCEPTION_DEVICE_ENTRIES_OF_INDEX_S_IN_TABLESCANOPERATOR_IS_EMPTY_FDEB574F =
      "Device entries of index %s in TableScanOperator 为空";
  public static final String QUERY_EXCEPTION_MULTILEVELPRIORITYQUEUE_DOES_NOT_SUPPORT_ACCESS_ELEMENT_02FE5AC9 =
      "MultilevelPriorityQueue does not support access element by get.";
  public static final String QUERY_EXCEPTION_ASCENDING_IS_NOT_SUPPORTED_WHEN_SLIDING_STEP_CONTAINS_MONTH_3446C0DC =
      "Ascending is not supported when sliding step contains month.";
  public static final String QUERY_EXCEPTION_THIS_OPERATION_IS_NOT_SUPPORTED_IN_SCHEMAMEASUREMENTNODE_93A81AE3 =
      "This operation is not supported in SchemaMeasurementNode.";
  public static final String QUERY_EXCEPTION_REMOVE_CHILD_OPERATION_IS_NOT_SUPPORTED_IN_SCHEMAMEASUREMENTNODE_940D080F =
      "移除child operation is not supported in SchemaMeasurementNode.";
  public static final String QUERY_EXCEPTION_DO_NOT_SUPPORT_CREATE_COLUMNBUILDER_WITH_DATA_TYPE_S_1672578A =
      "Do not support 创建ColumnBuilder with data type %s";
  public static final String QUERY_EXCEPTION_INVALID_CONSTANT_OPERAND_S_939F3B8D =
      "无效的constant operand：%s";
  public static final String QUERY_EXCEPTION_THE_DATA_TYPE_OF_THE_STATE_WINDOW_STRATEGY_IS_NOT_VALID_DFFBF210 =
      "The data type of the state window strategy is not valid.";
  public static final String QUERY_EXCEPTION_STATEWINDOWACCESSSTRATEGY_DOES_NOT_SUPPORT_PURE_CONSTANT_B09D811B =
      "StateWindowAccessStrategy does not support pure constant input.";
  public static final String QUERY_EXCEPTION_UNEXPECTED_ACCESS_STRATEGY_S_92EA9D64 =
      "Unexpected access strategy：%s";
  public static final String QUERY_EXCEPTION_STATEWINDOWACCESSSTRATEGY_ONLY_SUPPORT_ONE_INPUT_SERIES_6856E52C =
      "StateWindowAccessStrategy only support one input series for now.";
  public static final String QUERY_EXCEPTION_UNSUPPORTED_SOURCE_DATATYPE_S_EA03E121 =
      "不支持的source dataType：%s";
  public static final String QUERY_EXCEPTION_UNSUPPORTED_TARGET_DATATYPE_S_8DEFDAE6 =
      "不支持的target dataType：%s";
  public static final String QUERY_EXCEPTION_UNSUPPORTED_STATEMENT_TYPE_S_FBCA7305 =
      "不支持的statement type：%s";
  public static final String QUERY_EXCEPTION_IDENTITYSINKNODE_SHOULD_ONLY_HAVE_ONE_CHILD_IN_TABLE_MODEL_5E995EB3 =
      "IdentitySinkNode should only have one child in table model.";
  public static final String QUERY_EXCEPTION_TREE_DB_NAME_SHOULD_AT_LEAST_BE_TWO_LEVEL_S_772B6832 =
      "tree db name should at least be two level：%s";
  public static final String QUERY_EXCEPTION_PUSHDOWNOFFSET_SHOULD_NOT_BE_SET_WHEN_ISPUSHLIMITTOEACHDEVICE_9B6D5144 =
      "PushDownOffset should not be set when isPushLimitToEachDevice is true.";
  public static final String QUERY_EXCEPTION_DEVICE_ENTRIES_OF_INDEX_S_IN_S_IS_EMPTY_68D1DB60 =
      "Device entries of index %s in %s 为空";
  public static final String QUERY_EXCEPTION_THE_AGGREGATIONTREEDEVICEVIEWSCANNODE_SHOULD_HAS_BEEN_TRANSFERRED_76A35037 =
      "The AggregationTreeDeviceViewScanNode should has been transferred to its child class node";
  public static final String QUERY_EXCEPTION_GROUPING_KEY_MUST_BE_ID_OR_ATTRIBUTE_IN_AGGREGATIONTABLESCAN_7B592AE6 =
      "grouping key must be ID or Attribute in AggregationTableScan";
  public static final String QUERY_EXCEPTION_CANNOT_FIND_COLUMN_S_IN_CHILD_S_OUTPUT_10FBE4C8 =
      "无法find column [%s] in child's output";
  public static final String QUERY_EXCEPTION_DESCRIPTOR_S_INPUT_EXPRESSION_MUST_BE_TIMESERIESOPERAND_F4F66475 =
      "descriptor's input expression must be TimeSeriesOperand/TimestampOperand, 当前is %s";
  public static final String QUERY_EXCEPTION_AGGREGATIONMERGESORTNODE_WITHOUT_ORDER_BY_DEVICE_SHOULD_7AED85D1 =
      "AggregationMergeSortNode without order by device should not appear here";
  public static final String QUERY_EXCEPTION_UNEXPECTED_PLANNODE_IN_GETOUTPUTCOLUMNTYPESOFTIMEJOINNODE_00FAAEED =
      "Unexpected PlanNode in getOutputColumnTypesOfTimeJoinNode, type：%s";
  public static final String QUERY_EXCEPTION_THE_SIZE_OF_MEASUREMENTLIST_AND_TIMESERIESSCHEMAINFOLIST_A6649661 =
      "The size of measurementList and timeseriesSchemaInfoList should be equal in aligned path.";
  public static final String QUERY_EXCEPTION_THE_PLANNODE_IS_NULL_DURING_LOCAL_EXECUTION_MAYBE_CAUSED_C5B942CA =
      "The planNode is null during local execution, maybe caused by closing of the 当前dataNode";
  public static final String QUERY_EXCEPTION_THERE_IS_NOT_ENOUGH_MEMORY_TO_EXECUTE_CURRENT_FRAGMENT_INSTANCE_6071A581 =
      "There is not enough 内存 to execute 当前fragment instance, 当前remaining free 内存 is %dB, "
          + "estimated 内存 usage for 当前fragment instance is %dB";
  public static final String QUERY_EXCEPTION_BYTES_TO_RESERVE_FROM_FREE_MEMORY_FOR_OPERATORS_SHOULD_BE_4DC404D5 =
      "Bytes to reserve from free 内存 for operators should be larger than 0";
  public static final String QUERY_EXCEPTION_THERE_IS_NOT_ENOUGH_MEMORY_FOR_QUERY_S_THE_CONTEXTHOLDER_546CDD02 =
      "There is not enough 内存 for Query %s, the contextHolder is %s,当前remaining free 内存 is %dB, "
          + "already reserved 内存 for this context in total is %dB, the 内存 请求ed this time is %dB";
  public static final String QUERY_EXCEPTION_BYTES_TO_RELEASE_TO_FREE_MEMORY_FOR_OPERATORS_SHOULD_BE_3E5B0CB1 =
      "Bytes to release to free 内存 for operators should be larger than 0";
  public static final String QUERY_EXCEPTION_INVALID_AGGREGATION_EXPRESSION_S_B28EB91B =
      "无效的Aggregation Expression：%s";
  public static final String QUERY_EXCEPTION_ILLEGAL_DEVICE_PATH_S_IN_AGGREGATIONPUSHDOWN_RULE_60D5F633 =
      "非法的device path：%s in AggregationPushDown rule.";
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
      "不支持的ColumnCategory：%s";
  public static final String QUERY_EXCEPTION_UNSUPPORTED_RAWEXPRESSION_TYPE_S_CDBBD685 =
      "unsupported rawExpression type：%s";
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
      "Meet error when analyzing the query statement：%s";
  public static final String QUERY_EXCEPTION_UNSUPPORTED_EXPRESSION_TYPE_FOR_SOURCE_EXPRESSION_S_FB5583E7 =
      "unsupported expression type for source expression：%s";
  public static final String QUERY_EXCEPTION_UNSUPPORTED_EXPRESSION_TYPE_S_737846D6 =
      "不支持的Expression Type：%s";
  public static final String QUERY_EXCEPTION_UNKNOWN_EXPRESSION_TYPE_S_PERHAPS_IT_HAS_NON_EXISTENT_MEASUREMENT_B6705F86 =
      "未知的expression type：%s, perhaps it has non existent measurement.";
  public static final String QUERY_EXCEPTION_UNSUPPORTED_EXPRESSION_TYPE_IN_TRANSFORMTOVIEWEXPRESSIONVISITOR_0871FB56 =
      "不支持的expression type in TransformToViewExpressionVisitor：%s";
  public static final String QUERY_EXCEPTION_CAN_NOT_CONSTRUCT_EXPRESSION_USING_NON_VIEW_PATH_IN_TRANSFORMVIEWPATH_A9CCB5B1 =
      "无法construct expression using non view path in transformViewPath!";
  public static final String QUERY_EXCEPTION_UNSUPPORTED_EXPRESSION_TYPE_S_7C6F99A9 =
      "不支持的expression type %s";
  public static final String QUERY_EXCEPTION_S_CANNOT_BE_CAST_TO_S_DABC2DA0 =
      "\"%s\" cannot be cast to [%s]";
  public static final String QUERY_EXCEPTION_UNSUPPORTED_DATA_TYPE_S_4CB21D47 = "不支持的data type %s";
  public static final String QUERY_EXCEPTION_MEASUREMENT_S_DOES_NOT_EXIST_23D2B5BE =
      "Measurement %s 不存在";
  public static final String QUERY_EXCEPTION_INVALID_SCALAR_FUNCTION_S_4DC1ED95 =
      "无效的scalar function [%s].";
  public static final String QUERY_EXCEPTION_FETCH_SCHEMA_FAILED_BECAUSE_S_BE584DCE =
      "Fetch Schema failed, 原因：%s";
  public static final String QUERY_EXCEPTION_FETCH_SCHEMA_FAILED_S_1C7B0050 =
      "Fetch Schema failed：%s";
  public static final String QUERY_EXCEPTION_FAILED_TO_VALIDATE_SCHEMA_FOR_TABLE_S_S_D7031B7B =
      "无法validate schema for table {%s, %s}";
  public static final String QUERY_EXCEPTION_THE_DATABASE_S_DOES_NOT_EXIST_PLEASE_ENABLE_ENABLE_AUTO_B6683D0E =
      "The 数据库 %s 不存在, please enable 'enable_auto_create_schema' to enable auto creation.";
  public static final String QUERY_EXCEPTION_AUTO_CREATE_DATABASE_FAILED_S_STATUS_CODE_S_D8EB60FA =
      "Auto 创建数据库 failed：%s, status code：%s";
  public static final String QUERY_EXCEPTION_TAG_COLUMN_S_IN_TSFILE_IS_NOT_FOUND_IN_IOTDB_TABLE_S_12E8C1EF =
      "Tag column %s in Ts文件 is 未找到 in IoTDB table %s";
  public static final String QUERY_EXCEPTION_DUPLICATED_MEASUREMENTS_S_IN_DEVICE_S_438713CD =
      "Duplicated measurements %s in device %s.";
  public static final String QUERY_EXCEPTION_DATABASE_LEVEL_D_IS_LONGER_THAN_DEVICE_S_9B34DD2F =
      "数据库 level %d is longer than device %s.";
  public static final String QUERY_EXCEPTION_CREATE_DATABASE_ERROR_STATEMENT_S_RESULT_STATUS_IS_S_5C4AFD58 =
      "创建数据库 error, statement：%s, result status is：%s";
  public static final String QUERY_EXCEPTION_DEVICE_S_DOES_NOT_EXIST_IN_IOTDB_AND_CAN_NOT_BE_CREATED_5171DE45 =
      "Device %s 不存在 in IoTDB and can not be 已创建. Please check weather auto-create-schema is "
          + "enabled.";
  public static final String QUERY_EXCEPTION_DEVICE_S_IN_TSFILE_IS_S_BUT_IN_IOTDB_IS_S_350D5903 =
      "Device %s in Ts文件 is %s, but in IoTDB is %s.";
  public static final String QUERY_EXCEPTION_MEASUREMENT_S_DOES_NOT_EXIST_IN_IOTDB_AND_CAN_NOT_BE_CREATED_B1F446A5 =
      "Measurement %s 不存在 in IoTDB and can not be 已创建. Please check weather auto-create-schema is "
          + "enabled.";
  public static final String QUERY_EXCEPTION_AN_ERROR_OCCURRED_WHEN_EXECUTING_GETDEVICETODATABASE_S_CCA611CC =
      "An error occurred when executing getDeviceTo数据库():%s";
  public static final String QUERY_EXCEPTION_FAILED_TO_GET_REPLICASET_OF_CONSENSUS_GROUPS_IDS_S_CC30C7A6 =
      "无法get replicaSet of consensus groups[ids= %s]";
  public static final String QUERY_EXCEPTION_AN_ERROR_OCCURRED_WHEN_EXECUTING_GETREGIONREPLICASET_S_370D5526 =
      "An error occurred when executing getRegionReplicaSet():%s";
  public static final String QUERY_EXCEPTION_SUBSCRIPTION_IS_NOT_ENABLED_7F43DCBB =
      "Subscription is not enabled.";
  public static final String QUERY_EXCEPTION_FAILED_TO_CREATE_UDF_S_THE_GIVEN_FUNCTION_NAME_CONFLICTS_6FBB1136 =
      "无法创建UDF [%s], the given function name conflicts with the built-in function name.";
  public static final String QUERY_EXCEPTION_THE_SCHEME_OF_URI_IS_NOT_SET_PLEASE_SPECIFY_THE_SCHEME_OF_225DFB9E =
      "The scheme of URI is not set, please specify the scheme of URI.";
  public static final String QUERY_EXCEPTION_FAILED_TO_GET_EXECUTABLE_FOR_UDF_S_PLEASE_CHECK_THE_URI_F4D87A1E =
      "无法get executable for UDF '%s', please check the URI.";
  public static final String QUERY_EXCEPTION_FAILED_TO_CREATE_FUNCTION_S_BECAUSE_THERE_IS_DUPLICATE_ARGUMENT_7905BC09 =
      "无法创建function '%s', 原因：there is duplicate argument name '%s'.";
  public static final String QUERY_EXCEPTION_FAILED_TO_CREATE_FUNCTION_S_BECAUSE_THERE_IS_AN_ARGUMENT_E7A0B1D6 =
      "无法创建function '%s', 原因：there is an argument with OBJECT type '%s'.";
  public static final String QUERY_EXCEPTION_FAILED_TO_LOAD_CLASS_S_BECAUSE_IT_S_NOT_FOUND_IN_JAR_FILE_E467D08D =
      "无法加载class '%s', 原因：it's 未找到 in jar 文件 or is invalid：%s";
  public static final String QUERY_EXCEPTION_BUILT_IN_FUNCTION_S_CAN_NOT_BE_DEREGISTERED_1CC7D3C3 =
      "Built-in function %s can not be deregistered.";
  public static final String QUERY_EXCEPTION_FAILED_TO_GET_EXECUTABLE_FOR_TRIGGER_S_PLEASE_CHECK_THE_DA49134A =
      "无法get executable for Trigger '%s', please check the URI.";
  public static final String QUERY_EXCEPTION_FAILED_TO_CREATE_PIPE_PLUGIN_BECAUSE_THE_URI_IS_EMPTY_7FCB6EF4 =
      "无法创建pipe plugin, 原因：the URI 为空.";
  public static final String QUERY_EXCEPTION_FAILED_TO_GET_EXECUTABLE_FOR_PIPEPLUGIN_S_PLEASE_CHECK_THE_FAC5DCB7 =
      "无法get executable for PipePlugin %s, please check the URI.";
  public static final String QUERY_EXCEPTION_FAILED_TO_CREATE_PIPEPLUGIN_S_BECAUSE_THIS_PLUGIN_IS_NOT_F5A284B4 =
      "无法创建PipePlugin '%s', 原因：this plugin is not designed for %s model.";
  public static final String QUERY_EXCEPTION_NOT_ALL_SG_IS_READY_9F51CF3E = "not all sg is ready";
  public static final String QUERY_EXCEPTION_CANNOT_START_REPAIR_TASK_BECAUSE_COMPACTION_IS_NOT_ENABLED_975C8DCD =
      "cannot start repair task 原因：compaction is not enabled";
  public static final String QUERY_EXCEPTION_PLEASE_ENSURE_YOUR_INPUT_QUERYID_IS_CORRECT_D86C841E =
      "Please ensure your input <queryId> is correct";
  public static final String QUERY_EXCEPTION_DUPLICATED_MEASUREMENT_S_IN_DEVICE_TEMPLATE_ALTER_REQUEST_963FE4A6 =
      "Duplicated measurement [%s] in device template alter 请求";
  public static final String QUERY_EXCEPTION_FAILED_TO_CREATE_PIPE_S_BECAUSE_TSFILE_IS_CONFIGURED_WITH_2F8CD704 =
      "无法创建Pipe %s 原因：TS文件 is configured with encryption, which prohibits the use of Pipe";
  public static final String QUERY_EXCEPTION_FAILED_TO_CREATE_PIPE_S_PIPE_NAME_STARTING_WITH_S_ARE_NOT_201FE8C3 =
      "无法创建pipe %s, pipe name starting with \"%s\" are not allowed to be 已创建.";
  public static final String QUERY_EXCEPTION_FAILED_TO_ALTER_PIPE_S_PIPE_NAME_STARTING_WITH_S_ARE_NOT_03D99ECF =
      "无法alter pipe %s, pipe name starting with \"%s\" are not allowed to be altered.";
  public static final String QUERY_EXCEPTION_FAILED_TO_GET_PIPE_INFO_FROM_CONFIG_NODE_STATUS_IS_S_FE797D7B =
      "无法get pipe info from config node, status is %s.";
  public static final String QUERY_EXCEPTION_FAILED_TO_ALTER_PIPE_S_PIPE_NOT_FOUND_IN_SYSTEM_63B5D3CC =
      "无法alter pipe %s, pipe 未找到 in system.";
  public static final String QUERY_EXCEPTION_FAILED_TO_ALTER_PIPE_S_BECAUSE_S_A1823289 =
      "无法alter pipe %s, 原因：%s";
  public static final String QUERY_EXCEPTION_FAILED_TO_START_PIPE_S_PIPE_NAME_STARTING_WITH_S_ARE_NOT_F16D488F =
      "无法start pipe %s, pipe name starting with \"%s\" are not allowed to be started.";
  public static final String QUERY_EXCEPTION_FAILED_TO_DROP_PIPE_S_PIPE_NAME_STARTING_WITH_S_ARE_NOT_840E238B =
      "无法drop pipe %s, pipe name starting with \"%s\" are not allowed to be dropped.";
  public static final String QUERY_EXCEPTION_FAILED_TO_STOP_PIPE_S_PIPE_NAME_STARTING_WITH_S_ARE_NOT_C78DFC3D =
      "无法停止pipe %s, pipe name starting with \"%s\" are not allowed to be 已停止.";
  public static final String QUERY_EXCEPTION_UNKNOWN_DATABASE_S_5AB61128 = "未知的数据库 %s";
  public static final String QUERY_EXCEPTION_DATABASE_S_DOESN_T_EXIST_5A8EE8CA =
      "数据库 %s doesn't exist";
  public static final String QUERY_EXCEPTION_DATABASE_S_ALREADY_EXISTS_D8BE5332 = "数据库 %s 已存在";
  public static final String QUERY_EXCEPTION_DATA_TYPE_CANNOT_BE_NULL_EXECUTING_THE_STATEMENT_THAT_ALTER_4C959B2F =
      "Data type cannot be null executing the statement that alter timeseries %s set data type";
  public static final String QUERY_EXCEPTION_NO_CURRENT_SESSION_AVAILABLE_FOR_PREPARE_STATEMENT_36717E9B =
      "No 当前session available for PREPARE statement";
  public static final String QUERY_EXCEPTION_NO_CURRENT_SESSION_AVAILABLE_FOR_DEALLOCATE_STATEMENT_1EA5DE79 =
      "No 当前session available for DEALLOCATE statement";
  public static final String QUERY_EXCEPTION_TSFILE_S_IS_LOADING_BY_ANOTHER_SCHEDULER_55077B82 =
      "Ts文件 %s is loading by another scheduler.";
  public static final String QUERY_EXCEPTION_SERIALIZE_PROGRESS_INDEX_ERROR_ISFIRSTPHASESUCCESS_S_UUID_690F0419 =
      "Serialize Progress Index error, isFirstPhase成功：%s, uuid：%s, ts文件：%s";
  public static final String QUERY_EXCEPTION_UNSUPPORTED_TSFILEDATATYPE_S_374475FA =
      "不支持的Ts文件DataType %s.";
  public static final String QUERY_EXCEPTION_UNKNOWN_SORT_KEY_S_37965711 = "未知的sort key %s";
  public static final String QUERY_EXCEPTION_CAN_NOT_FIND_S_ON_THIS_MACHINE_NOTICE_THAT_LOAD_CAN_ONLY_B7886C0E =
      "无法find %s on this machine, notice that 加载can only handle 文件s on this machine.";
  public static final String QUERY_EXCEPTION_LOAD_TSFILE_SOURCE_PATH_S_IS_OUTSIDE_ALLOWED_DIRECTORIES_85A6019F =
      "加载Ts文件 source path %s is outside allowed directories %s.";
  public static final String QUERY_EXCEPTION_FAILED_TO_RESOLVE_CANONICAL_PATH_FOR_LOAD_TSFILE_SOURCE_09CC9AC6 =
      "无法resolve canonical path for 加载Ts文件 source %s：%s";
  public static final String QUERY_EXCEPTION_DATA_TYPE_IS_NOT_CONSISTENT_INPUT_S_REGISTERED_S_AE9DBDC0 =
      "data type is not consistent, input %s, registered %s";
  public static final String QUERY_EXCEPTION_REGIONREPLICASET_IS_INVALID_S_1C2671AD =
      "regionReplicaSet is invalid：%s";
  public static final String QUERY_EXCEPTION_PLANNODE_SHOULD_BE_IWRITEPLANNODE_IN_WRITE_OPERATION_S_36501D8A =
      "PlanNode should be IWritePlanNode in WRITE operation:%s";
  public static final String QUERY_EXCEPTION_SIZE_OF_DEVICES_AND_ITS_CHILDREN_IN_DEVICEVIEWNODE_SHOULD_10709A84 =
      "size of devices and its children in DeviceViewNode should be same";
  public static final String QUERY_EXCEPTION_IN_NON_CROSS_DATA_REGION_DEVICE_VIEW_SITUATION_EACH_DEVICE_3A76445B =
      "In non-cross data region device-view situation, each device should only have on data "
          + "partition.";
  public static final String QUERY_EXCEPTION_IN_NON_CROSS_DATA_REGION_AGGREGATION_DEVICE_VIEW_SITUATION_557AE5D2 =
      "In non-cross data region aggregation device-view situation, each re写入child node of "
          + "DeviceView should only be one.";
  public static final String QUERY_EXCEPTION_ALL_CHILD_NODES_OF_INNERTIMEJOINNODE_SHOULD_BE_SERIESSOURCENODE_B92B181D =
      "All child nodes of InnerTimeJoinNode should be SeriesSourceNode";
  public static final String QUERY_EXCEPTION_YOU_SHOULD_NEVER_SEE_CONTINUOUSSAMESEARCHINDEXSEPARATORNODE_F380A4B6 =
      "You should never see ContinuousSameSearchIndexSeparatorNode in this function, "
          + "原因：ContinuousSameSearchIndexSeparatorNode should never be used in network transmission.";
  public static final String QUERY_EXCEPTION_AGGREGATIONTREEDEVICEVIEWSCANNODE_SHOULD_NOT_BE_DESERIALIZED_11788F1B =
      "AggregationTreeDeviceViewScanNode should not be deserialized";
  public static final String QUERY_EXCEPTION_CONTINUOUSSAMESEARCHINDEXSEPARATORNODE_NOT_SUPPORT_MERGE_FD194976 =
      "ContinuousSameSearchIndexSeparatorNode not support merge";
  public static final String QUERY_EXCEPTION_DELETEDATANODES_WHICH_START_TIME_OR_END_TIME_ARE_NOT_SAME_F396951C =
      "DeleteDataNodes which start time or end time are not same cannot be merged";
  public static final String QUERY_EXCEPTION_NO_CHILD_IS_ALLOWED_FOR_ALIGNEDSERIESAGGREGATIONSCANNODE_41654FE2 =
      "no child is allowed for AlignedSeriesAggregationScanNode";
  public static final String QUERY_EXCEPTION_DEVICES_IN_TSFILE_S_IS_EMPTY_THIS_SHOULD_NOT_HAPPEN_HERE_BC1BE63C =
      "Devices in Ts文件 %s 为空, this should not happen here.";
  public static final String QUERY_EXCEPTION_DEVICEMERGENODE_SHOULD_HAVE_ONLY_ONE_LOCAL_CHILD_IN_SINGLE_D1A2E6CF =
      "DeviceMergeNode should have only one local child in single data region.";
  public static final String QUERY_EXCEPTION_GETOUTPUTCOLUMNNAMES_OF_CREATEMULTITIMESERIESNODE_IS_NOT_9D02257A =
      "getOutputColumnNames of CreateMultiTimeSeriesNode is not implemented";
  public static final String QUERY_EXCEPTION_GETOUTPUTCOLUMNNAMES_OF_ALTERLOGICALVIEWNODE_IS_NOT_IMPLEMENTED_D2294789 =
      "getOutputColumnNames of AlterLogicalViewNode is not implemented";
  public static final String QUERY_EXCEPTION_THE_DATABASE_S_IS_READ_ONLY_CB6732CE =
      "The 数据库 '%s' is read-only.";
  public static final String QUERY_EXCEPTION_THE_DATABASE_S_CAN_ONLY_BE_QUERIED_BY_AUDIT_ADMIN_4A510F66 =
      "The 数据库 '%s' can only be queried by AUDIT admin.";
  public static final String QUERY_EXCEPTION_UNEXPECTED_WINDOW_FRAME_TYPE_S_F06F81B8 =
      "unexpected window frame type：%s";
  public static final String QUERY_EXCEPTION_SIZE_OF_COLUMN_NAMES_AND_COLUMN_DATA_TYPES_DO_NOT_MATCH_9333D273 =
      "Size of column names and column data types do not match";
  public static final String QUERY_EXCEPTION_FAILED_TO_FETCH_FRAGMENT_INSTANCE_STATISTICS_45176795 =
      "无法fetch fragment instance statistics";
  public static final String QUERY_EXCEPTION_UNABLE_TO_REMOVE_FIRST_NODE_WHEN_A_NODE_HAS_MULTIPLE_CHILDREN_FB6E81C5 =
      "无法移除first node when a node has multiple children, use removeAll instead";
  public static final String QUERY_EXCEPTION_UNABLE_TO_REPLACE_FIRST_NODE_WHEN_A_NODE_HAS_MULTIPLE_CHILDREN_2C3D0E9E =
      "无法replace first node when a node has multiple children, use replaceAll instead";
  public static final String QUERY_EXCEPTION_UNSUPPORTED_EXPRESSION_IN_EXTRACTGLOBALTIMEPREDICATE_S_083B1BFA =
      "不支持的expression in extractGlobalTimePredicate：%s";
  public static final String QUERY_EXCEPTION_NOT_A_VALID_IR_EXPRESSION_S_03C41ADD =
      "Not a valid IR expression：%s";
  public static final String QUERY_EXCEPTION_UNKNOWN_ALIGNEDDEVICEENTRY_TYPE_S_370A0039 =
      "未知的AlignedDeviceEntry Type：%s";
  public static final String QUERY_EXCEPTION_UNSUPPORTED_SELECTITEM_TYPE_S_4B0B155A =
      "不支持的SelectItem type：%s";
  public static final String QUERY_EXCEPTION_THIS_VISITOR_ONLY_SUPPORTED_PROCESS_OF_EXPRESSION_7CEB79CB =
      "This Visitor only supported process of Expression";
  public static final String QUERY_EXCEPTION_UNSUPPORTED_GROUPING_ELEMENT_TYPE_S_B3C526E6 =
      "不支持的grouping element type：%s";
  public static final String QUERY_EXCEPTION_UNSUPPORTED_JOIN_CRITERIA_S_311289C1 =
      "不支持的join criteria：%s";
  public static final String QUERY_EXCEPTION_DUPLICATE_ARGUMENT_SPECIFICATION_FOR_NAME_S_F804F3DC =
      "Duplicate argument specification for name：%s";
  public static final String QUERY_EXCEPTION_FORECASTTABLEFUNCTION_MUST_CONTAIN_FORECASTTABLEFUNCTION_DA5828E9 =
      "ForecastTableFunction must contain ForecastTableFunction.TIMECOL_PARAMETER_NAME";
  public static final String QUERY_EXCEPTION_UNEXPECTED_ARGUMENT_SPECIFICATION_S_830154B1 =
      "Unexpected argument specification：%s";
  public static final String QUERY_EXCEPTION_UNEXPECTED_PARTITIONBY_EXPRESSION_S_8D74EB2D =
      "Unexpected partitionBy expression：%s";
  public static final String QUERY_EXCEPTION_UNEXPECTED_ORDERBY_EXPRESSION_S_9301B69E =
      "Unexpected orderBy expression：%s";
  public static final String QUERY_EXCEPTION_AGGREGATION_ANALYSIS_NOT_YET_IMPLEMENTED_FOR_S_38B64170 =
      "aggregation analysis not yet implemented for：%s";
  public static final String QUERY_EXCEPTION_UNEXPECTED_VALUE_LIST_TYPE_FOR_INPREDICATE_S_3D50B78B =
      "Unexpected value list type for InPredicate：%s";
  public static final String QUERY_EXCEPTION_UNEXPECTED_COMPARISON_TYPE_S_5D101FCB =
      "Unexpected comparison type：%s";
  public static final String QUERY_EXCEPTION_ZERO_LENGTH_DELIMITED_IDENTIFIER_NOT_ALLOWED_00C9ADEC =
      "Zero-length delimited identifier not allowed";
  public static final String QUERY_EXCEPTION_BACKQUOTED_IDENTIFIERS_ARE_NOT_SUPPORTED_USE_DOUBLE_QUOTES_78BC7EE3 =
      "backquoted identifiers are not supported; use double quotes to quote identifiers";
  public static final String QUERY_EXCEPTION_UNSUPPORTED_UNBOUNDED_TYPE_S_2D943211 =
      "不支持的unbounded type：%s";
  public static final String QUERY_EXCEPTION_ONLY_SUPPORT_MEASUREMENT_COLUMN_IN_FILTER_S_140800D9 =
      "Only support measurement column in filter：%s";
  public static final String QUERY_EXCEPTION_UNSUPPORTED_COMPARISON_OPERATOR_S_8357E642 =
      "不支持的comparison operator %s";
  public static final String QUERY_EXCEPTION_UNSUPPORTED_EXTRACT_COMPARISON_OPERATOR_S_38A9CDFA =
      "不支持的extract comparison operator %s";
  public static final String QUERY_EXCEPTION_UNSUPPORTED_LOGICAL_OPERATOR_S_FDC60986 =
      "不支持的logical operator %s";
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
      "Fetch Table Device Schema failed 原因：%s";
  public static final String QUERY_EXCEPTION_THE_SCHEMA_FILTER_TYPE_S_IS_NOT_SUPPORTED_200D1E0B =
      "The schema filter type %s is not supported";
  public static final String QUERY_EXCEPTION_AUTO_CREATE_TABLE_SUCCEED_BUT_CANNOT_GET_TABLE_SCHEMA_IN_74985A8E =
      "auto 创建table succeed, but cannot get table schema in 当前node's DataNodeTableCache, may be "
          + "caused by concurrently auto creating table";
  public static final String QUERY_EXCEPTION_CAN_NOT_CREATE_TABLE_BECAUSE_INCOMING_TABLE_HAS_NO_LESS_D3D33555 =
      "无法创建table 原因：incoming table has no less tag columns than existing table, and the existing "
          + "tag columns are not the prefix of the incoming tag columns. Existing tag column：%s, index "
          + "in existing table：%s, index in incoming table：%s";
  public static final String QUERY_EXCEPTION_CAN_NOT_CREATE_TABLE_BECAUSE_EXISTING_TABLE_HAS_MORE_TAG_8364B675 =
      "无法创建table 原因：existing table has more tag columns than incoming table, and the incoming tag "
          + "columns are not the prefix of the existing tag columns. Incoming tag column：%s, index in "
          + "existing table：%s, index in incoming table：%s";
  public static final String QUERY_EXCEPTION_AUTO_ADD_TABLE_COLUMN_FAILED_S_S_02F3DD19 =
      "Auto add table column failed：%s.%s";
  public static final String QUERY_EXCEPTION_CANNOT_CREATE_COLUMN_S_CATEGORY_IS_NOT_PROVIDED_E5410BD3 =
      "无法创建column %s category is not provided";
  public static final String QUERY_EXCEPTION_CANNOT_CREATE_COLUMN_S_DATATYPE_IS_NOT_PROVIDED_2A7D27FA =
      "无法创建column %s datatype is not provided";
  public static final String QUERY_EXCEPTION_UNKNOWN_COLUMNCATEGORY_FOR_ADDING_COLUMN_S_ED1BF7FA =
      "未知的ColumnCategory for adding column：%s";
  public static final String QUERY_EXCEPTION_VISIT_NOT_IMPLEMENTED_FOR_S_1A798A4D =
      "visit() not implemented for %s";
  public static final String QUERY_EXCEPTION_UNSUPPORTED_DATA_TYPE_FOR_SCALAR_SUBQUERY_RESULT_S_4381E489 =
      "不支持的data type for scalar subquery result：%s";
  public static final String QUERY_EXCEPTION_TOPKNODE_MUST_BE_APPEARED_AFTER_PUSHLIMITOFFSETINTOTABLESCAN_844A065D =
      "TopKNode must be appeared after PushLimitOffsetIntoTableScan";
  public static final String QUERY_EXCEPTION_TABLE_MODEL_CAN_ONLY_PROCESS_DATA_ONLY_IN_ONE_DATABASE_YET_AB8C1EF5 =
      "Table model can only process data only in one 数据库 yet!";
  public static final String QUERY_EXCEPTION_UNSUPPORTED_JOIN_TYPE_IN_PREDICATE_PUSH_DOWN_S_4493D86C =
      "不支持的join type in predicate push down：%s";
  public static final String QUERY_EXCEPTION_UNSUPPORTED_PLAN_NODE_S_72DD2270 = "不支持的plan node %s";
  public static final String QUERY_EXCEPTION_AN_ERROR_OCCURRED_WHEN_EXECUTING_GETREADABLEDATAREGIONS_A884A6BB =
      "An error occurred when executing getReadableDataRegions():%s";
  public static final String QUERY_EXCEPTION_AN_ERROR_OCCURRED_WHEN_EXECUTING_GETREADABLEDATANODELOCATIONS_54CBD60D =
      "An error occurred when executing getReadableDataNodeLocations():%s";
  public static final String QUERY_EXCEPTION_UNSUPPORTED_QUANTIFIED_COMPARISON_S_C3700430 =
      "不支持的quantified comparison：%s";
  public static final String QUERY_EXCEPTION_UNEXPECTED_QUANTIFIEDCOMPARISON_S_F13E4EB2 =
      "Unexpected quantifiedComparison：%s";
  public static final String QUERY_EXCEPTION_UNEXPECTED_QUANTIFIER_S_62214B74 =
      "Unexpected Quantifier：%s";
  public static final String QUERY_EXCEPTION_THIS_OPTIMIZER_SHOULD_BE_USED_BEFORE_OPTIMIZER_OF_PUSHAGGREGATIONINTOTABLESCAN_9F6016E3 =
      "This optimizer should be used before optimizer of PushAggregationIntoTableScan";
  public static final String QUERY_EXCEPTION_UNSUPPORTED_VALUEPOINTER_TYPE_S_4147FFFB =
      "不支持的ValuePointer type：%s";
  public static final String QUERY_EXCEPTION_SYMBOL_S_IS_NOT_EXIST_IN_FETYPEPROVIDER_WITH_S_5CBBFB8B =
      "Symbol：%s is 不存在 in feTypeProvider with %s";
  public static final String QUERY_EXCEPTION_RIGHT_JOIN_SHOULD_BE_TRANSFORMED_TO_LEFT_JOIN_IN_PREVIOUS_D6B56B1F =
      "RIGHT Join should be transformed to LEFT Join in previous process";
  public static final String QUERY_EXCEPTION_NO_AVAILABLE_DATANODES_MAY_BE_THE_CLUSTER_IS_CLOSING_E13B8C50 =
      "No available dataNodes, may be the cluster is closing";
  public static final String QUERY_EXCEPTION_SHOULD_NEVER_REACH_HERE_CHILD_ORDERING_S_PREGROUPEDSYMBOLS_79A94AB5 =
      "Should never reach here. Child ordering：%s. PreGroupedSymbols：%s";
  public static final String QUERY_EXCEPTION_UNSUPPORTED_TO_SERIALIZE_S_484DAAAF =
      "不支持的to serialize：%s";
  public static final String QUERY_EXCEPTION_UNSUPPORTED_TO_DESERIALIZE_S_4641CD63 =
      "不支持的to deserialize：%s";
  public static final String QUERY_EXCEPTION_INSERT_INTO_TABLE_COLUMNS_S_SIZE_SHOULD_BE_SAME_AS_QUERY_E7437397 =
      "insert into table columns's size should be same as query result";
  public static final String QUERY_EXCEPTION_THE_TABLEDEVICEFETCHNODE_S_CLONE_METHOD_SHALL_NOT_BE_CALLED_977C41FD =
      "The TableDeviceFetchNode's clone() method shall not be called.";
  public static final String QUERY_EXCEPTION_UNKNOWN_TABLESCANNODE_TYPE_S_6246EF1E =
      "未知的TableScanNode type：%s";
  public static final String QUERY_EXCEPTION_UNEXPECTED_SETOPERATION_NODE_TYPE_S_3AE3EECA =
      "unexpected setOperation node type：%s";
  public static final String QUERY_EXCEPTION_UNSUPPORTED_PATTERN_QUANTIFIER_TYPE_S_4ED427E9 =
      "unsupported pattern quantifier type：%s";
  public static final String FAILED_TO_CREATE_PIPE_PLUGIN_PREFIX_FMT =
      "创建 pipe plugin %s 失败。";
  public static final String FAILED_TO_CREATE_PIPE_PREFIX_FMT = "创建 pipe %s 失败，";
  public static final String STATE_ABORTED = "已中止";
  public static final String STATE_CLOSED = "已关闭";
  public static final String LOCAL_SINK_CHANNEL_STATE_IS_WITH_STATE_FMT =
      "LocalSinkChannel 状态为 %s。";
  public static final String UNKNOWN_READ_TYPE_FMT = "未知的读取类型 [%s]";
  public static final String DESERIALIZE_CONSENSUSGROUPID_FAILED_WITH_REASON_FMT =
      "反序列化 ConsensusGroupId 失败：%s";
  public static final String CANNOT_ALTER_TEMPLATE_TIMESERIES_TEMPLATE_ALREADY_SET_FMT =
      "无法修改模板时间序列 [%s]，因为设备模板 [%s] 已设置在路径 [%s] 上。";
  public static final String PATH_HAS_NOT_BEEN_SET_ANY_TEMPLATE_FMT =
      "路径 [%s] 未设置任何模板。";
  public static final String FAILED_TO_FETCH_SCHEMA_BECAUSE_OF_UNRECOGNIZED_DATA =
      "由于存在无法识别的数据，获取 schema 失败";
  public static final String ALIGNMENT_ALIGNED = "对齐";
  public static final String ALIGNMENT_NOT_ALIGNED = "非对齐";
  public static final String TREE_MODEL_DATABASE_NAME_MUST_START_WITH_ROOT =
      "树模型中的数据库名必须以 'root.' 开头。";
  public static final String DATABASE_NAME_LENGTH_SHALL_NOT_EXCEED_FMT =
      "数据库名长度不能超过 %d";
  public static final String MODEL_TABLE = "表";
  public static final String MODEL_TREE = "树";
  public static final String NO_SYMBOL_MAPPING_FOR_NODE_FMT =
      "节点 '%s' (%s) 没有 symbol 映射";
  public static final String NO_MAPPING_FOR_EXPRESSION_WITH_IDENTITY_FMT =
      "表达式没有映射：%s (%s)";
  public static final String UNEXPECTED_QUANTIFIED_COMPARISON_FMT =
      "意外的 quantified comparison：'%s %s'";
  public static final String TABLE_HAS_NO_PREFIX_FMT = "表 %s 没有前缀！";
  public static final String ANALYSIS_DOES_NOT_CONTAIN_INFORMATION_FOR_NODE_FMT =
      "Analysis 不包含节点信息：%s";
  public static final String AUTO_CREATE_TABLE_FAILED = "自动创建表失败。";
  public static final String AUTO_CREATE_TABLE_COLUMN_FAILED = "自动创建表列失败。";
  public static final String AUTO_ADD_TABLE_COLUMN_FAILED_WITH_COLUMNS_FMT =
      "自动添加表列失败：%s.%s，%s";
  public static final String QUERY_STATEMENT_NOT_ALLOWED_IN_BATCH_FMT =
      "批处理中不允许查询语句：[%s]";
  public static final String TABLE_LOST_UNEXPECTED_FMT =
      "数据库 %s 中的表 %s 意外丢失。";
  public static final String LOAD_FILE_CROSSES_PARTITIONS_FMT =
      "文件 %s 的数据跨越了分区";
  public static final String LOAD_FILE_CROSSES_PARTITIONS = "文件数据跨越了分区";
  public static final String REGION_REPLICA_SET_CHANGED_DURING_LOAD_FMT =
      "加载 TsFile 期间 Region 副本集从 %s 变为 %s，可能是 region migration 导致";
  public static final String REGION_REPLICA_SET_CHANGED_DURING_LOAD =
      "加载 TsFile 期间 Region 副本集发生变化，可能是 region migration 导致";
  public static final String OUT_OF_TTL_FMT =
      "插入时间 [%s] 小于 ttl 时间下界 [%s]";
  public static final String DRIVER_TASK_ABORTED_FMT = "DriverTask %s 被 %s 中止";
  public static final String DRIVER_TASK_ABORTED_BY_TIMEOUT = "超时";
  public static final String DRIVER_TASK_ABORTED_BY_FRAGMENT_ABORT_CALLED = "被调用中止";
  public static final String DRIVER_TASK_ABORTED_BY_QUERY_CASCADING_ABORTED =
      "查询级联中止";
  public static final String DRIVER_TASK_ABORTED_BY_ALREADY_BEING_CANCELLED =
      "已经在取消中";
  public static final String DRIVER_TASK_ABORTED_BY_INTERNAL_ERROR_SCHEDULED =
      "内部错误调度";
  public static final String DRIVER_TASK_ABORTED_BY_MEMORY_NOT_ENOUGH =
      "内存不足，无法执行查询任务。";
  public static final String ROOT_FRAGMENT_INSTANCE_PLACEMENT_ERROR_FMT =
      "Root FragmentInstance 放置错误：%s";
  public static final String ALL_REPLICA_CANNOT_BE_REACHED_FMT =
      "所有副本均不可达：%s";
  public static final String LOAD_READ_ONLY_MESSAGE =
      "当前系统模式为只读，不支持加载文件";
  public static final String QUERY_KILLED_BY_OTHERS = "查询被其他操作终止";
  public static final String QUERY_TIMEOUT_EXCEPTION_MESSAGE =
      "当前查询超时，查询开始时间为 %d，ddl 为 %d，当前时间为 %d，请检查语句或修改超时参数。";
  public static final String NON_RESERVED_CAN_ONLY_CONTAIN_TOKENS_FOUND_NESTED_RULE =
      "nonReserved 只能包含 token，发现嵌套规则：";

  // ---------------------------------------------------------------------------
  // Additional auto-collected messages
  // ---------------------------------------------------------------------------
  public static final String MESSAGE_FAILED_FETCH_STATE_HAS_RETRIED_ARG_TIMES_E7572C66 = "无法fetch state, has retried %s times";
  public static final String MESSAGE_IGNORED_CONFIG_ITEMS_FE28ADBC = "ignored config items: ";
  public static final String MESSAGE_BECAUSE_THEY_IMMUTABLE_UNDEFINED_07C04F65 = "，原因：they are immutable or undefined.";
  public static final String MESSAGE_LOAD_ARG_PIECE_ERROR_1ST_PHASE_BECAUSE_F3D9672C = "加载 %s piece 错误 in 1st phase. Because ";
  public static final String MESSAGE_LOAD_ARG_ERROR_SECOND_PHASE_BECAUSE_ARG_FIRST_PHASE_ARG_CBA980FC = "加载 %s 错误 in second phase. Because %s, first phase is %s";
  public static final String MESSAGE_SUCCESS_260CA9DD = "成功";
  public static final String MESSAGE_FAILED_26934EB3 = "失败";
  public static final String MESSAGE_AUDIT_PERMISSION_NEEDED_ALTER_ENCODING_COMPRESSOR_DATABASE_ARG_CC06994D = "'AUDIT' 权限 is needed to alter the encoding and compressor of 数据库 %s";
  public static final String EXCEPTION_QUERYID_IS_NULL_056E92E4 = "queryId 不能为空";
  public static final String EXCEPTION_EXPECTED_TWO_IDS_BUT_GOT_COLON_ARG_020F9D13 = "期望 two ids but got: %s";
  public static final String EXCEPTION_ID_IS_NULL_9D5D27B1 = "id 不能为空";
  public static final String EXCEPTION_NAME_IS_NULL_C8B35959 = "name 不能为空";
  public static final String EXCEPTION_EXPECTEDPARTS_MUST_BE_AT_LEAST_1_B867DB08 = "期望Parts 必须为 at least 1";
  public static final String EXCEPTION_INVALID_ARG_ARG_2946DBE5 = "无效的 %s %s";
  public static final String EXCEPTION_ID_IS_EMPTY_28C94FC0 = "id is empty";
  public static final String EXCEPTION_EXECUTOR_IS_NULL_7FBE03A4 = "executor 不能为空";
  public static final String EXCEPTION_INITIALSTATE_IS_NULL_8992A39F = "initialState 不能为空";
  public static final String EXCEPTION_TERMINALSTATES_IS_NULL_E0FC2A93 = "terminalStates 不能为空";
  public static final String EXCEPTION_EXPECTEDSTATE_IS_NULL_5E8C2F32 = "期望State 不能为空";
  public static final String EXCEPTION_CURRENTSTATE_IS_NULL_AEDB20DB = "currentState 不能为空";
  public static final String EXCEPTION_STATECHANGELISTENER_IS_NULL_635AE7D2 = "stateChangeListener 不能为空";
  public static final String EXCEPTION_ARG_CANNOT_TRANSITION_FROM_ARG_TO_ARG_8C680D30 = "%s 无法 transition 来自 %s 到 %s";
  public static final String EXCEPTION_CANNOT_FIRE_STATE_CHANGE_EVENT_WHILE_HOLDING_THE_LOCK_35243BC4 = "无法 fire state change event while holding the lock";
  public static final String EXCEPTION_CANNOT_NOTIFY_WHILE_HOLDING_THE_LOCK_15625D48 = "无法 notify while holding the lock";
  public static final String EXCEPTION_CANNOT_WAIT_FOR_STATE_CHANGE_WHILE_HOLDING_THE_LOCK_CBD9F784 = "无法 wait for state change while holding the lock";
  public static final String EXCEPTION_DONESTATE_IS_NULL_D88F77E5 = "doneState 不能为空";
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
  public static final String EXCEPTION_STEP_IN_SERIESAGGREGATESCANOPERATOR_AND_RAWDATAAGGREGATEOPERATOR_CAN_ONLY_PROCES_5575BD95 = "Step in SeriesAggregateScanOperator 和 RawDataAggregateOperator can only process raw input";
  public static final String EXCEPTION_RAWDATAAGGREGATEOPERATOR_CAN_ONLY_PROCESS_ONE_TSBLOCK_INPUT_DOT_5ABCB8C0 = "RawDataAggregateOperator can only process one tsBlock input.";
  public static final String EXCEPTION_STEP_IN_AGGREGATEOPERATOR_CANNOT_PROCESS_RAW_INPUT_22620F61 = "Step in AggregateOperator 无法 process raw input";
  public static final String EXCEPTION_FINAL_OUTPUT_CAN_ONLY_BE_SINGLE_COLUMN_6D82F9E0 = "Final output can only be single column";
  public static final String EXCEPTION_PARTIALRESULT_OF_SUM_SHOULD_BE_1_40E85216 = "partialResult of Sum should be 1";
  public static final String EXCEPTION_INPUT_OF_SUM_SHOULD_BE_1_D5C11EC8 = "input of Sum should be 1";
  public static final String EXCEPTION_PARTIALRESULT_OF_UDAF_SHOULD_BE_1_E094029D = "partialResult of UDAF should be 1";
  public static final String EXCEPTION_PARTIALRESULT_OF_EXTREMEVALUE_SHOULD_BE_1_A7713D8A = "partialResult of ExtremeValue should be 1";
  public static final String EXCEPTION_WRONG_INPUTDATATYPES_SIZE_DOT_675FF289 = "Wrong inputDataTypes size.";
  public static final String EXCEPTION_INPUT_OF_COUNT_SHOULD_BE_1_C7EEEC46 = "input of Count should be 1";
  public static final String EXCEPTION_PARTIALRESULT_OF_MAXTIME_SHOULD_BE_1_877F7EAC = "partialResult of MaxTime should be 1";
  public static final String EXCEPTION_LOCALMEMORYMANAGER_IS_NULL_DOT_69FE497A = "localMemoryManager 不能为空.";
  public static final String EXCEPTION_TSBLOCKSERDEFACTORY_IS_NULL_DOT_32EB5BD2 = "tsBlockSerdeFactory 不能为空.";
  public static final String EXCEPTION_EXECUTORSERVICE_IS_NULL_DOT_7B057909 = "executorService 不能为空.";
  public static final String EXCEPTION_MPPDATAEXCHANGESERVICECLIENTMANAGER_IS_NULL_DOT_F31E746C = "mppDataExchangeServiceClientManager 不能为空.";
  public static final String EXCEPTION_SHAREDTSBLOCKQUEUE_IS_FULL_87493E26 = "SharedTsBlockQueue is full";
  public static final String EXCEPTION_FRAGMENT_INSTANCE_ID_CANNOT_BE_NULL_4BE84F40 = "fragment instance ID 无法 be null";
  public static final String EXCEPTION_PLANNODE_ID_CANNOT_BE_NULL_F91303CD = "PlanNode ID 无法 be null";
  public static final String EXCEPTION_LOCAL_MEMORY_MANAGER_CANNOT_BE_NULL_54701481 = "local memory manager 无法 be null";
  public static final String EXCEPTION_EXECUTORSERVICE_CAN_NOT_BE_NULL_DOT_220C966B = "ExecutorService can not be null.";
  public static final String EXCEPTION_TSBLOCK_CANNOT_BE_NULL_E7EA3BDA = "TsBlock 无法 be null";
  public static final String EXCEPTION_BYTESTORESERVE_SHOULD_BE_GREATER_THAN_ZERO_DOT_56D15DE0 = "bytesToReserve should be greater than zero.";
  public static final String EXCEPTION_MAXBYTESCANRESERVE_SHOULD_BE_GREATER_THAN_ZERO_DOT_E9F7D365 = "maxBytesCanReserve should be greater than zero.";
  public static final String EXCEPTION_MAX_BYTES_SHOULD_BE_GREATER_THAN_ZERO_COLON_ARG_EA1FB495 = "max bytes should be greater than zero: %d";
  public static final String EXCEPTION_MAX_BYTES_PER_FI_SHOULD_BE_IN_LEFT_PAREN_0_COMMA_MAXBYTES_RIGHT_BRACKET_DOT_MAXB_4D37C457 = "max bytes per FI should be in (0,maxBytes]. maxBytesPerFI: %d, maxBytes: %d";
  public static final String EXCEPTION_BYTESTORESERVE_SHOULD_BE_IN_LEFT_PAREN_0_COMMA_MAXBYTESPERFI_RIGHT_BRACKET_DOT_M_0753BB69 = "bytesToReserve should be in (0,maxBytesPerFI]. maxBytesPerFI: %d, bytesToReserve: %d";
  public static final String EXCEPTION_INVALID_FUTURE_TYPE_14507EF5 = "无效的 future type ";
  public static final String EXCEPTION_QUERYID_CANNOT_BE_NULL_861D7663 = "queryId 无法 be null";
  public static final String EXCEPTION_FRAGMENTINSTANCEID_CANNOT_BE_NULL_C722F460 = "fragmentInstanceId 无法 be null";
  public static final String EXCEPTION_PLANNODEID_CANNOT_BE_NULL_4533C72B = "planNodeId 无法 be null";
  public static final String EXCEPTION_ID_CAN_NOT_BE_NULL_DOT_BDD2AD7D = "id can not be null.";
  public static final String EXCEPTION_QUERYID_CAN_NOT_BE_NULL_DOT_16639DBE = "queryId can not be null.";
  public static final String EXCEPTION_FRAGMENTINSTANCEID_CAN_NOT_BE_NULL_DOT_E88CF18B = "fragmentInstanceId can not be null.";
  public static final String EXCEPTION_PLANNODEID_CAN_NOT_BE_NULL_DOT_44027620 = "planNodeId can not be null.";
  public static final String EXCEPTION_THE_FUTURE_TO_BE_CANCELLED_CAN_NOT_BE_NULL_DOT_73CE402A = "The future 到 be cancelled can not be null.";
  public static final String EXCEPTION_LOCK_IS_NOT_REENTRANT_7967C13E = "Lock is not reentrant";
  public static final String EXCEPTION_CURRENT_THREAD_DOES_NOT_HOLD_LOCK_68FFB1D9 = "Current thread does not hold lock";
  public static final String EXCEPTION_ROOT_OPERATOR_SHOULD_NOT_BE_NULL_F4890A7A = "root Operator 不能为空";
  public static final String EXCEPTION_SINK_SHOULD_NOT_BE_NULL_3CC4F006 = "Sink 不能为空";
  public static final String EXCEPTION_INITIALCOUNT_SHOULDN_QUOTE_T_BE_NULL_HERE_8B333953 = "initialCount shouldn't be null here";
  public static final String EXCEPTION_COUNT_SHOULDN_QUOTE_T_BE_NULL_HERE_1EBA9339 = "count shouldn't be null here";
  public static final String EXCEPTION_TASKID_IS_NULL_E1221EB2 = "taskId 不能为空";
  public static final String EXCEPTION_INSTANCEID_IS_NULL_343234DC = "instanceId 不能为空";
  public static final String EXCEPTION_FRAGMENTINSTANCEID_IS_NULL_4D371DB4 = "fragmentInstanceId 不能为空";
  public static final String EXCEPTION_CURRENT_STATE_IS_ALREADY_DONE_19FC56DC = "Current state is already done";
  public static final String EXCEPTION_SUPPRESSED_IS_NULL_F4CD280B = "suppressed 不能为空";
  public static final String EXCEPTION_STACK_IS_NULL_6844D421 = "stack 不能为空";
  public static final String EXCEPTION_ARG_IS_A_NON_MINUS_DONE_FAILURE_STATE_B167E915 = "%s is a non-done failure state";
  public static final String EXCEPTION_WARNINGCODE_IS_NULL_3CCAE5B7 = "warningCode 不能为空";
  public static final String EXCEPTION_MESSAGE_IS_NULL_D2D078AA = "message 不能为空";
  public static final String EXCEPTION_WARNING_IS_NULL_E5A7C3C1 = "warning 不能为空";
  public static final String EXCEPTION_NO_FIRST_FILE_F5F2E276 = "no first file";
  public static final String EXCEPTION_NO_FIRST_CHUNK_7DCEB14C = "no first chunk";
  public static final String EXCEPTION_CAN_QUOTE_T_INIT_NULL_CHUNKMETA_15C12BEE = "Can't init null chunkMeta";
  public static final String EXCEPTION_OPERATORCONTEXT_IS_NULL_D15B1EDB = "operatorContext 不能为空";
  public static final String EXCEPTION_CHILD_OPERATOR_IS_NULL_8860113C = "child operator 不能为空";
  public static final String EXCEPTION_GROUPBYTIMEPARAMETER_CANNOT_BE_NULL_IN_SLIDINGWINDOWAGGREGATIONOPERATOR_BA42E30D = "GroupByTimeParameter 无法 be null in SlidingWindowAggregationOperator";
  public static final String EXCEPTION_REMAININGINPUTLOCATIONS_IS_NULL_CEBBA2C1 = "remainingInputLocations 不能为空";
  public static final String EXCEPTION_LAST_QUERY_RESULT_SHOULD_ONLY_HAVE_ONE_RECORD_EDFEE635 = "last query result should only have one record";
  public static final String EXCEPTION_OUTPUTPATHS_SHOULDN_QUOTE_T_BE_NULL_BF3F5FB4 = "outputPaths shouldn't be null";
  public static final String EXCEPTION_CHILD_SIZE_OF_INNERTIMEJOINOPERATOR_SHOULD_BE_LARGER_THAN_1_37EB7D74 = "child size of InnerTimeJoinOperator should be larger than 1";
  public static final String EXCEPTION_CHILD_SIZE_OF_VERTICALLYCONCATOPERATOR_SHOULD_BE_LARGER_THAN_0_14A2513A = "child size of VerticallyConcatOperator should be larger than 0";
  public static final String EXCEPTION_CHILD_SIZE_OF_TIMEJOINOPERATOR_SHOULD_BE_LARGER_THAN_0_EDED9CB8 = "child size of TimeJoinOperator should be larger than 0";
  public static final String EXCEPTION_DATASTORE_IS_NULL_D9972B2E = "dataStore 不能为空";
  public static final String EXCEPTION_LASTVALUESCACHERESULTS_SHOULDN_QUOTE_T_BE_NULL_HERE_0DCD5841 = "lastValuesCacheResults shouldn't be null here";
  public static final String EXCEPTION_ACCUMULATOR_SHOULD_BE_LASTDESCACCUMULATOR_WHEN_REACH_HERE_CE38F96A = "Accumulator should be LastDescAccumulator when reach here";
  public static final String EXCEPTION_DRIVERTASK_TO_BE_PUSHED_IS_NULL_7581A0E3 = "DriverTask 到 be pushed 不能为空";
  public static final String EXCEPTION_DRIVERTASK_IS_NULL_A13D4AF9 = "driverTask 不能为空";
  public static final String EXCEPTION_SELECTED_LEVEL_CAN_NOT_EQUAL_TO_MINUS_1_1DA93B14 = "selected level can not equal 到 -1";
  public static final String EXCEPTION_RESULT_DRIVERTASK_CANNOT_BE_NULL_30A06DB1 = "result driverTask 无法 be null";
  public static final String EXCEPTION_DRIVERTASKQUEUE_IS_NULL_7C3C8B7B = "driverTaskQueue 不能为空";
  public static final String EXCEPTION_MAXDRIVERSPERTASK_IS_NULL_8408F9B7 = "maxDriversPerTask 不能为空";
  public static final String EXCEPTION_QUEUE_CAN_NOT_BE_NULL_DOT_9BB286B1 = "queue can not be null.";
  public static final String EXCEPTION_SOURCEHANDLELISTENER_CAN_NOT_BE_NULL_DOT_01817F52 = "sourceHandleListener can not be null.";
  public static final String EXCEPTION_LOCALFRAGMENTINSTANCEID_CAN_NOT_BE_NULL_DOT_37F5917D = "localFragmentInstanceId can not be null.";
  public static final String EXCEPTION_LOCALPLANNODEID_CAN_NOT_BE_NULL_DOT_44A34A33 = "localPlanNodeId can not be null.";
  public static final String EXCEPTION_START_SEQUENCE_ID_SHOULD_BE_GREATER_THAN_OR_EQUAL_TO_ZERO_DOT_START_SEQUENCE_ID__D3C0AAB7 = "Start sequence ID should be greater than 或 equal 到 zero. Start sequence ID: ";
  public static final String EXCEPTION_COMMA_END_SEQUENCE_ID_COLON_DB1AF173 = ", end sequence ID: ";
  public static final String EXCEPTION_END_SEQUENCE_ID_SHOULD_BE_GREATER_THAN_THE_START_SEQUENCE_ID_DOT_START_SEQUENCE__DF1AA2A1 = "End sequence ID should be greater than the start sequence ID. Start sequence ID: ";
  public static final String EXCEPTION_RESERVED_BYTES_SHOULD_BE_GREATER_THAN_ZERO_DOT_64086BE5 = "Reserved bytes should be greater than zero.";
  public static final String EXCEPTION_REMOTEENDPOINT_CAN_NOT_BE_NULL_DOT_DE2B5885 = "remoteEndpoint can not be null.";
  public static final String EXCEPTION_REMOTEFRAGMENTINSTANCEID_CAN_NOT_BE_NULL_DOT_C2449A29 = "remoteFragmentInstanceId can not be null.";
  public static final String EXCEPTION_LOCALMEMORYMANAGER_CAN_NOT_BE_NULL_DOT_7A46C6CE = "localMemoryManager can not be null.";
  public static final String EXCEPTION_EXECUTORSERVICE_CAN_NOT_BE_NULL_DOT_BC459BD4 = "executorService can not be null.";
  public static final String EXCEPTION_SERDE_CAN_NOT_BE_NULL_DOT_D46F66E7 = "serde can not be null.";
  public static final String EXCEPTION_DOT_9D9B854A = ".";
  public static final String EXCEPTION_START_SEQUENCE_ID_SHOULD_BE_GREATER_THAN_OR_EQUAL_TO_ZERO_COMMA_BUT_WAS_COLON_4D2D708E = "Start sequence ID should be greater than 或 equal 到 zero, but was: ";
  public static final String EXCEPTION_REMOTEENDPOINT_CAN_NOT_BE_NULL_DOT_83488ACF = "remoteEndPoint can not be null.";
  public static final String EXCEPTION_REMOTEPLANNODEID_CAN_NOT_BE_NULL_DOT_03956DE2 = "remotePlanNodeId can not be null.";
  public static final String EXCEPTION_SINKLISTENER_CAN_NOT_BE_NULL_DOT_32C9E7C0 = "sinkListener can not be null.";
  public static final String EXCEPTION_TSBLOCKS_IS_NULL_02287FD8 = "tsBlocks 不能为空";
  public static final String EXCEPTION_DOWNSTREAMCHANNELLIST_CAN_NOT_BE_NULL_DOT_417AD5A3 = "downStreamChannelList can not be null.";
  public static final String EXCEPTION_DOWNSTREAMCHANNELINDEX_CAN_NOT_BE_NULL_DOT_A1D5A266 = "downStreamChannelIndex can not be null.";
  public static final String EXCEPTION_STEP_IN_SLIDINGWINDOWAGGREGATIONOPERATOR_CAN_ONLY_PROCESS_PARTIAL_RESULT_E221A2C5 = "Step in SlidingWindowAggregationOperator can only process partial result";
  public static final String EXCEPTION_SLIDINGWINDOWAGGREGATIONOPERATOR_CAN_ONLY_PROCESS_ONE_TSBLOCK_INPUT_DOT_7B9FCAB7 = "SlidingWindowAggregationOperator can only process one tsBlock input.";
  public static final String EXCEPTION_IS_NULL_97AAF381 = " 不能为空";
  public static final String EXCEPTION_MPP_DATA_EXCHANGE_MANAGER_SHOULD_NOT_BE_NULL_44D7141E = "MPP_DATA_EXCHANGE_MANAGER 不能为空";
  public static final String EXCEPTION_ROOTOPERATOR_IS_NULL_050A1E79 = "rootOperator 不能为空";
  public static final String EXCEPTION_DRIVERCONTEXT_IS_NULL_4FEBE55F = "driverContext 不能为空";
  public static final String EXCEPTION_QUERY_CONTEXT_CANNOT_BE_NULL_C4809234 = "Query context 无法 be null";
  public static final String EXCEPTION_DESCRIPTOR_QUOTE_S_INPUT_EXPRESSION_SIZE_IS_NOT_1_DA4BED50 = "descriptor's input expression size is not 1";
  public static final String EXCEPTION_GROUPBYLEVEL_DESCRIPTORLIST_CANNOT_BE_EMPTY_34604314 = "GroupByLevel descriptorList 无法 be empty";
  public static final String EXCEPTION_GROUPBYTAG_TAG_KEYS_CANNOT_BE_EMPTY_5D649624 = "GroupByTag tag keys 无法 be empty";
  public static final String EXCEPTION_GROUPBYTAG_AGGREGATION_DESCRIPTORS_CANNOT_BE_EMPTY_82EC14EB = "GroupByTag aggregation descriptors 无法 be empty";
  public static final String EXCEPTION_AGGREGATION_DESCRIPTORLIST_CANNOT_BE_EMPTY_490C1740 = "Aggregation descriptorList 无法 be empty";
  public static final String EXCEPTION_PUSH_DOWN_PREDICATE_IS_NOT_SUPPORTED_YET_178F04A1 = "Push down predicate is not supported yet";
  public static final String EXCEPTION_SINK_IS_NULL_E33854B4 = "sink 不能为空";
  public static final String EXCEPTION_THERE_MUST_BE_AT_MOST_ONE_SINKNODE_A965AFE7 = "There 必须为 at most one SinkNode";
  public static final String EXCEPTION_QUERY_CONTEXT_CANNOT_BE_NULL_DOT_2D3369FE = "Query context 无法 be null.";
  public static final String EXCEPTION_UNEXPECTED_NODE_TYPE_COLON_41FBCBF3 = "Un期望 node type: ";
  public static final String EXCEPTION_TEMPLATEDINFO_SHOULD_NOT_BE_NULL_B5898568 = "TemplatedInfo 不能为空";
  public static final String EXCEPTION_RESULTHANDLE_IN_COORDINATOR_SHOULD_BE_INIT_FIRSTLY_DOT_0F44159B = "ResultHandle in Coordinator should be init firstly.";
  public static final String EXCEPTION_EXPRESSION_IS_NOT_ANALYZED_COLON_ARG_7D34C49A = "Expression is not analyzed: %s";
  public static final String EXCEPTION_PATH_QUOTE_ARG_QUOTE_IS_NOT_ANALYZED_IN_GROUPBYLEVELHELPER_DOT_BCEE9D39 = "path '%s' is not analyzed in GroupByLevelHelper.";
  public static final String EXCEPTION_SYMBOL_IS_NULL_AE539B31 = "symbol 不能为空";
  public static final String EXCEPTION_NO_TYPE_FOUND_FOR_SYMBOL_QUOTE_ARG_QUOTE_IN_TYPEPROVIDER_F4DD9DF7 = "no type found for symbol '%s' in TypeProvider";
  public static final String EXCEPTION_OUTPUT_COLUMN_QUOTE_ARG_QUOTE_IS_NOT_STORED_IN_ARG_2DE3176D = "output column '%s' is not stored in %s";
  public static final String EXCEPTION_PATTERNSTRING_CANNOT_BE_NULL_8A2903F8 = "patternString 无法 be null";
  public static final String EXCEPTION_THE_LENGTH_OF_CASEWHENTHENEXPRESSION_QUOTE_S_WHENTHENLIST_MUST_GREATER_THAN_0_069775ED = "the length of CaseWhenThenExpression's whenThenList must greater than 0";
  public static final String EXCEPTION_QUERYCONTEXT_IS_NULL_C2344379 = "QueryContext 不能为空";
  public static final String EXCEPTION_PREDICATE_LEFT_BRACKET_ARG_RIGHT_BRACKET_SHOULD_BE_SIMPLIFIED_IN_PREVIOUS_STEP_9262C154 = "Predicate [%s] should be simplified in previous step";
  public static final String EMPTY_MESSAGE = "";
  public static final String EXCEPTION_SEMICOLON_0FAEF84A = "; ";
  public static final String EXCEPTION_THE_TSBLOCK_SHOULD_NOT_BE_NULL_WHEN_CONSTRUCTING_MEMORYSOURCEHANDLE_8D205293 = "the TsBlock 不能为空 when constructing MemorySourceHandle";
  public static final String EXCEPTION_THE_TIME_ORDER_IS_NOT_SPECIFIED_DOT_624A7526 = "The time order is not specified.";
  public static final String EXCEPTION_THE_TIMESERIES_ORDER_IS_NOT_SPECIFIED_DOT_68EE3875 = "The timeseries order is not specified.";
  public static final String EXCEPTION_THE_DEVICE_ORDER_IS_NOT_SPECIFIED_DOT_D3FB9559 = "The device order is not specified.";
  public static final String EXCEPTION_SLASH_BC35AB27 = "/";
  public static final String EXCEPTION_INFORMATIONSCHEMATABLESCANNODE_MUST_HAVE_REGIONREPLICASET_0411DBCB = "InformationSchemaTableScanNode must have regionReplicaSet";
  public static final String EXCEPTION_EACH_INFORMATIONSCHEMATABLESCANNODE_HAVE_ONLY_ONE_DATANODELOCATION_FA3E82C4 = "each InformationSchemaTableScanNode have only one DataNodeLocation";
  public static final String EXCEPTION_CHILD_OF_EXCHANGENODE_MUST_BE_MULTICHILDRENSINKNODE_1BF715FD = "child of ExchangeNode 必须为 MultiChildrenSinkNode";
  public static final String EXCEPTION_SIZE_OF_SOURCELOCATIONS_SHOULD_BE_LARGER_THAN_0_2EC41A23 = "size of sourceLocations should be larger than 0";
  public static final String EXCEPTION_INDEX_IS_NOT_VALID_2AB4FB3A = "index is not valid";
  public static final String EXCEPTION_LINES_OF_BOX_STRING_SHOULD_BE_GREATER_THAN_0_5DB8C047 = "Lines of box string should be greater than 0";
  public static final String EXCEPTION_WRONG_NUMBER_OF_NEW_CHILDREN_817AF800 = "wrong number of new children";
  public static final String EXCEPTION_TRANSLATIONS_IS_NULL_37D62ADC = "translations 不能为空";
  public static final String EXCEPTION_ROOT_IS_NULL_ECC8987D = "root 不能为空";
  public static final String EXCEPTION_SYMBOLHINT_IS_NULL_CE874C40 = "symbolHint 不能为空";
  public static final String EXCEPTION_TYPE_IS_NULL_16A3D3EB = "type 不能为空";
  public static final String EXCEPTION_SYMBOLHINT_NOT_IN_SYMBOLS_MAP_B0D67E43 = "symbolHint not in symbols map";
  public static final String EXCEPTION_PLANNERCONTEXT_IS_NULL_B7C7DE50 = "plannerContext 不能为空";
  public static final String EXCEPTION_OBJECTS_IS_NULL_819EE879 = "objects 不能为空";
  public static final String EXCEPTION_TYPES_IS_NULL_E4B2309D = "types 不能为空";
  public static final String EXCEPTION_OBJECTS_AND_TYPES_DO_NOT_HAVE_THE_SAME_SIZE_8B51E17B = "objects 和 types do not have the same size";
  public static final String EXCEPTION_OUTERCONTEXT_IS_NULL_031CD366 = "outerContext 不能为空";
  public static final String EXCEPTION_SCOPE_IS_NULL_4F364BA2 = "scope 不能为空";
  public static final String EXCEPTION_ANALYSIS_IS_NULL_66666A58 = "analysis 不能为空";
  public static final String EXCEPTION_FIELDSYMBOLS_IS_NULL_5130E49C = "fieldSymbols 不能为空";
  public static final String EXCEPTION_ASTTOSYMBOLS_IS_NULL_80B3970F = "astToSymbols 不能为空";
  public static final String EXCEPTION_TOO_FEW_PARAMETER_VALUES_2F7358C6 = "Too few parameter values";
  public static final String EXCEPTION_SYMBOLALLOCATOR_IS_NULL_E2BE1908 = "symbolAllocator 不能为空";
  public static final String EXCEPTION_QUERYCONTEXT_IS_NULL_761DB539 = "queryContext 不能为空";
  public static final String EXCEPTION_SESSION_IS_NULL_6CF0F47D = "session 不能为空";
  public static final String EXCEPTION_RECURSIVESUBQUERIES_IS_NULL_6AD8A180 = "recursiveSubqueries 不能为空";
  public static final String EXCEPTION_PREDICATEWITHUNCORRELATEDSCALARSUBQUERYRECONSTRUCTOR_IS_NULL_B264FEBC = "predicateWithUncorrelatedScalarSubqueryReconstructor 不能为空";
  public static final String EXCEPTION_ONLY_SUPPORT_ONE_GROUPINGSET_NOW_A1277FA4 = "Only support one groupingSet now";
  public static final String EXCEPTION_EXPRESSION_IS_NULL_16C079B5 = "expression 不能为空";
  public static final String EXCEPTION_EXPRESSIONTYPES_IS_NULL_4107A4A2 = "expressionTypes 不能为空";
  public static final String EXCEPTION_TYPE_NOT_FOUND_FOR_EXPRESSION_COLON_ARG_C26C9237 = "Type not found for expression: %s";
  public static final String EXCEPTION_VALUE_REACH_HERE_MUST_BE_EXPRESSION_C7CA1971 = "Value reach here 必须为 Expression";
  public static final String EXCEPTION_PLAN_IS_NULL_717C9DF7 = "plan 不能为空";
  public static final String EXCEPTION_LOOKUP_IS_NULL_B8FD7E65 = "lookup 不能为空";
  public static final String EXCEPTION_CONSUMER_IS_NULL_B6207072 = "consumer 不能为空";
  public static final String EXCEPTION_FIELDMAPPINGS_IS_NULL_C3681969 = "fieldMappings 不能为空";
  public static final String EXCEPTION_NO_FIELD_MINUS_GREATER_THAN_SYMBOL_MAPPING_FOR_FIELD_ARG_698FDF06 = "No field->symbol mapping for field %s";
  public static final String EXCEPTION_FOR_NOW_COMMA_ONLY_SINGLE_COLUMN_SUBQUERIES_ARE_SUPPORTED_AD9593BE = "For now, only single column subqueries are supported";
  public static final String EXCEPTION_FOR_NOW_COMMA_ONLY_SINGLE_COLUMN_SUBQUERIES_ARE_SUPPORTED_DOT_068B1A66 = "For now, only single column subqueries are supported.";
  public static final String EXCEPTION_CLUSTER_IS_EMPTY_22299EED = "Cluster is empty";
  public static final String EXCEPTION_CLUSTER_CONTAINS_EXPRESSIONS_THAT_ARE_NOT_EQUIVALENT_TO_EACH_OTHER_7AD9A0E3 = "Cluster contains expressions that are not equivalent 到 each other";
  public static final String EXCEPTION_WARNINGCOLLECTOR_IS_NULL_7A524A68 = "warningCollector 不能为空";
  public static final String EXCEPTION_MEASURES_IS_NULL_EC9D2431 = "measures 不能为空";
  public static final String EXCEPTION_MEASUREOUTPUTS_IS_NULL_923F7C4B = "measureOutputs 不能为空";
  public static final String EXCEPTION_SKIPTOPOSITION_IS_NULL_EFBA10CA = "skipToPosition 不能为空";
  public static final String EXCEPTION_PATTERN_IS_NULL_AC4E239A = "pattern 不能为空";
  public static final String EXCEPTION_VARIABLEDEFINITIONS_IS_NULL_5F7B8ED4 = "variableDefinitions 不能为空";
  public static final String EXCEPTION_NO_RELATIONS_SPECIFIED_FOR_UNION_70CE42C4 = "No relations specified for UNION";
  public static final String EXCEPTION_NO_RELATIONS_SPECIFIED_FOR_INTERSECT_76B0ED3B = "No relations specified for intersect";
  public static final String EXCEPTION_NO_RELATIONS_SPECIFIED_FOR_EXCEPT_C8E4B4AA = "No relations specified for except";
  public static final String EXCEPTION_NODE_IS_NULL_C1479F4A = "node 不能为空";
  public static final String EXCEPTION_EXPRESSIONS_MUST_BE_IN_THE_SAME_LOCAL_SCOPE_CCAD793E = "Expressions 必须为 in the same local scope";
  public static final String EXCEPTION_WHERE_IS_NULL_A1A3FCBC = "where 不能为空";
  public static final String EXCEPTION_SKIPONLY_IS_NULL_80DB0703 = "skipOnly 不能为空";
  public static final String EXCEPTION_UNABLE_TO_REMOVE_PLAN_NODE_AS_IT_CONTAINS_0_OR_MORE_THAN_1_CHILDREN_6F26E194 = "Unable 到 remove plan node as it contains 0 或 more than 1 children";
  public static final String EXCEPTION_FIELDS_IS_NULL_DE209DBF = "fields 不能为空";
  public static final String EXCEPTION_CANNOT_RESOLVE_SYMBOL_ARG_79F76FA6 = "无法 resolve symbol %s";
  public static final String EXCEPTION_COLUMNREFERENCES_IS_NULL_124955C5 = "columnReferences 不能为空";
  public static final String EXCEPTION_SCOPEEQUALITIES_IS_NULL_22388B2C = "scopeEqualities 不能为空";
  public static final String EXCEPTION_SCOPECOMPLEMENTEQUALITIES_IS_NULL_B9080FC7 = "scopeComplementEqualities 不能为空";
  public static final String EXCEPTION_SCOPESTRADDLINGEQUALITIES_IS_NULL_F6B979AE = "scopeStraddlingEqualities 不能为空";
  public static final String EXCEPTION_SYMBOLTYPES_IS_NULL_DD16EA83 = "symbolTypes 不能为空";
  public static final String EXCEPTION_EXPRESSION_CANNOT_BE_NULL_EFF1A99C = "expression 无法 be null";
  public static final String EXCEPTION_TYPE_CANNOT_BE_NULL_97A0A8D3 = "type 无法 be null";
  public static final String EXCEPTION_NO_TYPE_FOR_COLON_ARG_9E34AD76 = "No type for: %s";
  public static final String EXCEPTION_CONDITION_MUST_BE_BOOLEAN_COLON_ARG_806C2960 = "Condition 必须为 boolean: %s";
  public static final String EXCEPTION_TYPES_MUST_BE_EQUAL_COLON_ARG_VS_ARG_098424AD = "Types 必须为 equal: %s vs %s";
  public static final String EXCEPTION_ELEMENTS_IS_NULL_3451C1DA = "elements 不能为空";
  public static final String EXCEPTION_PREDICATE_IS_NULL_22E687A9 = "predicate 不能为空";
  public static final String EXCEPTION_MAPPER_IS_NULL_1D7789D1 = "mapper 不能为空";
  public static final String EXCEPTION_FUNCTION_IS_NULL_E0FA4B62 = "function 不能为空";
  public static final String EXCEPTION_N_MUST_BE_GREATER_THAN_OR_EQUAL_TO_ZERO_C4CE8BF0 = "n 必须为 greater than 或 equal 到 zero";
  public static final String EXCEPTION_LOOKUPTYPE_IS_NULL_190054FA = "lookupType 不能为空";
  public static final String EXCEPTION_OPERATORTYPE_IS_NULL_CEA6E3D3 = "operatorType 不能为空";
  public static final String EXCEPTION_ARGUMENTTYPES_IS_NULL_1E377BFD = "argumentTypes 不能为空";
  public static final String EXCEPTION_RETURNTYPE_IS_NULL_07C7C6A5 = "returnType 不能为空";
  public static final String EXCEPTION_TABLE_IS_NULL_8DDD9098 = "table 不能为空";
  public static final String EXCEPTION_COLUMNS_IS_NULL_6C8F32B3 = "columns 不能为空";
  public static final String EXCEPTION_COMMENT_IS_NULL_0AD46118 = "comment 不能为空";
  public static final String EXCEPTION_STATEMENTANALYZERFACTORY_IS_NULL_D309BAB5 = "statementAnalyzerFactory 不能为空";
  public static final String EXCEPTION_FIELD_IS_NULL_80E8CE23 = "field 不能为空";
  public static final String EXCEPTION_PARAMETERS_IS_NULL_418C7892 = "parameters 不能为空";
  public static final String EXCEPTION_TABLEREFERENCE_IS_NULL_C02D3A8F = "tableReference 不能为空";
  public static final String EXCEPTION_QUERY_IS_NULL_689B7978 = "query 不能为空";
  public static final String EXCEPTION_RECURSIVEREFERENCE_IS_NULL_24B9D5DC = "recursiveReference 不能为空";
  public static final String EXCEPTION_ROOT_STATEMENT_IS_ANALYSIS_IS_NULL_36BCD4D1 = "root statement is analysis 不能为空";
  public static final String EXCEPTION_ACCESSCONTROL_IS_NULL_F534EBDD = "accessControl 不能为空";
  public static final String EXCEPTION_IDENTITY_IS_NULL_846265BA = "identity 不能为空";
  public static final String EXCEPTION_HANDLE_IS_NULL_E82FA480 = "handle 不能为空";
  public static final String EXCEPTION_TABLENAME_IS_NULL_20708596 = "tableName 不能为空";
  public static final String EXCEPTION_COLUMNNAME_IS_NULL_81635BA6 = "columnName 不能为空";
  public static final String EXCEPTION_AUTHORIZATION_IS_NULL_7CCA692F = "authorization 不能为空";
  public static final String EXCEPTION_VALUETYPE_IS_NULL_A8582B5F = "valueType 不能为空";
  public static final String EXCEPTION_VALUECOERCION_IS_NULL_E1A004BF = "valueCoercion 不能为空";
  public static final String EXCEPTION_SUBQUERYCOERCION_IS_NULL_33646290 = "subqueryCoercion 不能为空";
  public static final String EXCEPTION_PARTITIONBY_IS_NULL_84791B6B = "partitionBy 不能为空";
  public static final String EXCEPTION_ORDERBY_IS_NULL_AA2494DE = "orderBy 不能为空";
  public static final String EXCEPTION_FRAME_IS_NULL_5A92D609 = "frame 不能为空";
  public static final String EXCEPTION_ATLEAST_IS_NULL_2FE8D701 = "atLeast 不能为空";
  public static final String EXCEPTION_ATMOST_IS_NULL_778B3B3A = "atMost 不能为空";
  public static final String EXCEPTION_FILLEDVALUE_IS_NULL_1FA907D6 = "filledValue 不能为空";
  public static final String EXCEPTION_FIELDREFERENCE_IS_NULL_0B07EA50 = "fieldReference 不能为空";
  public static final String EXCEPTION_QUERY_IS_NOT_REGISTERED_AS_EXPANDABLE_FAAD8FC9 = "query is not registered as expandable";
  public static final String EXCEPTION_EXPRESSION_NOT_ANALYZED_COLON_ARG_D397B665 = "Expression not analyzed: %s";
  public static final String EXCEPTION_EXPRESSION_IS_NOT_A_COLUMN_REFERENCE_COLON_ARG_7957E705 = "Expression is not a column reference: %s";
  public static final String EXCEPTION_EXPECTED_JOIN_FIELDS_FOR_LEFT_AND_RIGHT_TO_HAVE_THE_SAME_SIZE_21BFD449 = "期望 join fields for left 和 right 到 have the same size";
  public static final String EXCEPTION_NO_COLUMNS_GIVEN_TO_INSERT_52C42E47 = "No columns given 到 insert";
  public static final String EXCEPTION_MISSING_FILLANALYSIS_FOR_NODE_ARG_7E5B19A4 = "missing FillAnalysis for node %s";
  public static final String EXCEPTION_MISSING_OFFSET_VALUE_FOR_NODE_ARG_4B107520 = "missing OFFSET value for node %s";
  public static final String EXCEPTION_MISSING_LIMIT_VALUE_FOR_NODE_ARG_DD5FD777 = "missing LIMIT value for node %s";
  public static final String EXCEPTION_MISSING_RANGE_FOR_QUANTIFIER_ARG_03461228 = "missing range for quantifier %s";
  public static final String EXCEPTION_MISSING_UNDEFINED_LABELS_FOR_ARG_CA615EC2 = "missing undefined labels for %s";
  public static final String EXCEPTION_NO_FIELD_FOR_97419CB1 = "No Field for ";
  public static final String EXCEPTION_RELATIONID_IS_NULL_C4683108 = "relationId 不能为空";
  public static final String EXCEPTION_FIELDINDEX_MUST_BE_NON_MINUS_NEGATIVE_COMMA_GOT_COLON_ARG_09C2C06D = "fieldIndex 必须为 non-negative, got: %s";
  public static final String EXCEPTION_TYPEMANAGER_IS_NULL_12A72016 = "typeManager 不能为空";
  public static final String EXCEPTION_NODES_IS_NULL_7AB3C1D7 = "nodes 不能为空";
  public static final String EXCEPTION_CLAZZ_IS_NULL_7F710E3E = "clazz 不能为空";
  public static final String EXCEPTION_GROUPBYEXPRESSIONS_IS_NULL_BFDC07D2 = "groupByExpressions 不能为空";
  public static final String EXCEPTION_SOURCESCOPE_IS_NULL_4B3626A7 = "sourceScope 不能为空";
  public static final String EXCEPTION_ORDERBYSCOPE_IS_NULL_A6017E73 = "orderByScope 不能为空";
  public static final String EXCEPTION_NO_FIELD_FOR_E99DCE9A = "No field for ";
  public static final String EXCEPTION_INVALID_PARAMETER_NUMBER_ARG_COMMA_MAX_VALUES_IS_ARG_B3F5C4E8 = "无效的 parameter number %s, max values is %s";
  public static final String EXCEPTION_GROUPING_FIELD_ARG_SHOULD_ORIGINATE_FROM_ARG_6DBBCE6B = "Grouping field %s should originate 来自 %s";
  public static final String EXCEPTION_ALLLABELS_IS_NULL_9F240FB5 = "allLabels 不能为空";
  public static final String EXCEPTION_DESCRIPTOR_IS_NULL_E6EC1F14 = "descriptor 不能为空";
  public static final String EXCEPTION_ARGUMENTS_IS_NULL_B1F6D4F2 = "arguments 不能为空";
  public static final String EXCEPTION_MODE_IS_NULL_54A948DB = "mode 不能为空";
  public static final String EXCEPTION_LABELS_IS_NULL_F4FBBECE = "labels 不能为空";
  public static final String EXCEPTION_MATCHNUMBERCALLS_IS_NULL_EC08D0D0 = "matchNumberCalls 不能为空";
  public static final String EXCEPTION_CLASSIFIERCALLS_IS_NULL_92AA8B77 = "classifierCalls 不能为空";
  public static final String EXCEPTION_LABEL_IS_NULL_B21FE26B = "label 不能为空";
  public static final String EXCEPTION_NAVIGATION_IS_NULL_3D0CBEE1 = "navigation 不能为空";
  public static final String EXCEPTION_ANCHOR_IS_NULL_4AF93E60 = "anchor 不能为空";
  public static final String EXCEPTION_FIELD_CANNOT_BE_NULL_09155004 = "field 无法 be null";
  public static final String EXCEPTION_FIELD_QUOTE_ARG_QUOTE_NOT_FOUND_1BC2FDED = "Field '%s' not found";
  public static final String EXCEPTION_PARENT_IS_NULL_ED81BAD8 = "parent 不能为空";
  public static final String EXCEPTION_RELATION_IS_NULL_890596ED = "relation 不能为空";
  public static final String EXCEPTION_NAMEDQUERIES_IS_NULL_AFDE9A4A = "namedQueries 不能为空";
  public static final String EXCEPTION_TABLES_IS_NULL_2012309E = "tables 不能为空";
  public static final String EXCEPTION_RELATIONTYPE_IS_NULL_62DDF9C1 = "relationType 不能为空";
  public static final String EXCEPTION_BASISTYPE_IS_NULL_33E4F842 = "basisType 不能为空";
  public static final String EXCEPTION_PARENT_IS_ALREADY_SET_835DE0A5 = "parent is already set";
  public static final String EXCEPTION_QUERY_QUOTE_ARG_QUOTE_IS_ALREADY_ADDED_F3D47DBD = "Query '%s' is already added";
  public static final String EXCEPTION_MISSING_SCOPE_D573869F = "missing scope";
  public static final String EXCEPTION_MISSING_RELATIONTYPE_679D3CFA = "missing relationType";
  public static final String EXCEPTION_SUBQUERYINPREDICATES_IS_NULL_5A37F1C8 = "subqueryInPredicates 不能为空";
  public static final String EXCEPTION_SUBQUERIES_IS_NULL_0D5EA053 = "subqueries 不能为空";
  public static final String EXCEPTION_EXISTSSUBQUERIES_IS_NULL_5EA140F5 = "existsSubqueries 不能为空";
  public static final String EXCEPTION_QUANTIFIEDCOMPARISONS_IS_NULL_A30F5121 = "quantifiedComparisons 不能为空";
  public static final String EXCEPTION_WINDOWFUNCTIONS_IS_NULL_D77C3CD5 = "windowFunctions 不能为空";
  public static final String EXCEPTION_ORIGINTABLE_IS_NULL_18AC52C3 = "originTable 不能为空";
  public static final String EXCEPTION_RELATIONALIAS_IS_NULL_C363AD25 = "relationAlias 不能为空";
  public static final String EXCEPTION_ORIGINCOLUMNNAME_IS_NULL_98607162 = "originColumnName 不能为空";
  public static final String EXCEPTION_METADATA_IS_NULL_6F8F9BA0 = "metadata 不能为空";
  public static final String EXCEPTION_CONTEXT_IS_NULL_E329B664 = "context 不能为空";
  public static final String EXCEPTION_GETRESOLVEDWINDOW_IS_NULL_2438758C = "getResolvedWindow 不能为空";
  public static final String EXCEPTION_GETPREANALYZEDTYPE_IS_NULL_FBB2EC7D = "getPreanalyzedType 不能为空";
  public static final String EXCEPTION_BASESCOPE_IS_NULL_ABE8F618 = "baseScope 不能为空";
  public static final String EXCEPTION_PATTERNRECOGNITIONCONTEXT_IS_NULL_59C665F1 = "patternRecognitionContext 不能为空";
  public static final String EXCEPTION_CORRELATIONSUPPORT_IS_NULL_E0D669BF = "correlationSupport 不能为空";
  public static final String EXCEPTION_FUNCTIONINPUTTYPES_IS_NULL_3030658F = "functionInputTypes 不能为空";
  public static final String EXCEPTION_COLUMN_IS_NULL_0C404041 = "column 不能为空";
  public static final String EXCEPTION_EXPRESSION_NOT_YET_ANALYZED_COLON_ARG_0F4F19B7 = "Expression not yet analyzed: %s";
  public static final String EXCEPTION_ARG_ALREADY_KNOWN_TO_REFER_TO_ARG_8C8B4F24 = "%s already known 到 refer 到 %s";
  public static final String EXCEPTION_NO_RESOLVED_WINDOW_FOR_COLON_AED19667 = "no resolved window for: ";
  public static final String EXCEPTION_NO_LABEL_AVAILABLE_8508CE32 = "no label available";
  public static final String EXCEPTION_REWRITES_IS_NULL_4E5AD77A = "rewrites 不能为空";
  public static final String EXCEPTION_STATEMENT_REWRITE_RETURNED_NULL_AB1E89EA = "Statement rewrite returned null";
  public static final String EXCEPTION_ATN_IS_NULL_48BE0D3E = "atn 不能为空";
  public static final String EXCEPTION_LEXER_IS_NULL_88834E18 = "lexer 不能为空";
  public static final String EXCEPTION_PARSER_IS_NULL_AE8E5D6F = "parser 不能为空";
  public static final String EXCEPTION_LEXER_ATN_MISMATCH_COLON_EXPECTED_ARG_COMMA_FOUND_ARG_8ED22CF1 = "Lexer ATN mismatch: 期望 %s, found %s";
  public static final String EXCEPTION_PARSER_ATN_MISMATCH_COLON_EXPECTED_ARG_COMMA_FOUND_ARG_FF75D61B = "Parser ATN mismatch: 期望 %s, found %s";
  public static final String EXCEPTION_INITIALIZER_IS_NULL_2EEC3764 = "initializer 不能为空";
  public static final String EXCEPTION_TERMINALNODE_IS_NULL_578F27FD = "terminalNode 不能为空";
  public static final String EXCEPTION_PARSERRULECONTEXT_IS_NULL_9E0DD3B5 = "parserRuleContext 不能为空";
  public static final String EXCEPTION_TOKEN_IS_NULL_43094C56 = "token 不能为空";
  public static final String EXCEPTION_VALUE_IS_NULL_192F6BFF = "value 不能为空";
  public static final String EXCEPTION_LOCATION_IS_NULL_F134D388 = "location 不能为空";
  public static final String EXCEPTION_TOPIC_NAME_CAN_NOT_BE_NULL_EA4ED0BF = "topic name can not be null";
  public static final String EXCEPTION_SOURCE_NAME_IS_NULL_287E475D = "source name 不能为空";
  public static final String EXCEPTION_TARGET_NAME_IS_NULL_A5F701C6 = "target name 不能为空";
  public static final String EXCEPTION_DBNAME_IS_NULL_4521C4EE = "dbName 不能为空";
  public static final String EXCEPTION_PROPERTIES_IS_NULL_57B88B49 = "properties 不能为空";
  public static final String EXCEPTION_TARGET_IS_NULL_240F0372 = "target 不能为空";
  public static final String EXCEPTION_INDEXNAME_IS_NULL_2525299C = "indexName 不能为空";
  public static final String EXCEPTION_COLUMNLIST_IS_NULL_DADE6825 = "columnList 不能为空";
  public static final String EXCEPTION_SIZE_OF_COLUMNLIST_SHOULD_BE_LARGER_THAN_1_7EB80E55 = "size of columnList should be larger than 1";
  public static final String EXCEPTION_ASSIGNMENTS_IS_NULL_1FD6142D = "assignments 不能为空";
  public static final String EXCEPTION_PIPE_NAME_CAN_NOT_BE_NULL_14570979 = "pipe name can not be null";
  public static final String EXCEPTION_SQL_IS_NULL_BEDB2B7A = "sql 不能为空";
  public static final String EXCEPTION_SERVICENAME_IS_NULL_1009BA39 = "serviceName 不能为空";
  public static final String EXCEPTION_CANNOT_GET_NON_MINUS_DEFAULT_VALUE_OF_PROPERTY_ARG_SINCE_ITS_VALUE_IS_SET_TO_DEF_E7D3185F = "无法 get non-default value of property %s since its value is set 到 DEFAULT";
  public static final String EXCEPTION_STATEMENTNAME_IS_NULL_C03BB8D4 = "statementName 不能为空";
  public static final String EXCEPTION_CATALOGNAME_IS_NULL_2E3C3C6B = "catalogName 不能为空";
  public static final String EXCEPTION_SOURCE_IS_NULL_45946547 = "source 不能为空";
  public static final String EXCEPTION_SUBSCRIPTION_ID_CAN_NOT_BE_NULL_0CDFFD7D = "subscription id can not be null";
  public static final String EXCEPTION_DB_IS_NULL_E1AD1B58 = "db 不能为空";
  public static final String EXCEPTION_TOPIC_ATTRIBUTES_CAN_NOT_BE_NULL_791A8FED = "topic attributes can not be null";
  public static final String EXCEPTION_EXTRACTOR_SLASH_SOURCE_ATTRIBUTES_CAN_NOT_BE_NULL_2B3A656B = "extractor/source attributes can not be null";
  public static final String EXCEPTION_PROCESSOR_ATTRIBUTES_CAN_NOT_BE_NULL_FFF91008 = "processor attributes can not be null";
  public static final String EXCEPTION_CONNECTOR_ATTRIBUTES_CAN_NOT_BE_NULL_7AF2F613 = "connector attributes can not be null";
  public static final String EXCEPTION_CLASSNAME_IS_NULL_3902B37C = "className 不能为空";
  public static final String EXCEPTION_PLUGIN_NAME_CAN_NOT_BE_NULL_92F0F4D6 = "plugin name can not be null";
  public static final String EXCEPTION_CLASS_NAME_CAN_NOT_BE_NULL_1D276677 = "class name can not be null";
  public static final String EXCEPTION_URI_CAN_NOT_BE_NULL_B3535EDC = "uri can not be null";
  public static final String EXCEPTION_STATEMENT_IS_NULL_693A0622 = "statement 不能为空";
  public static final String EXCEPTION_UDFNAME_IS_NULL_83E9039B = "udfName 不能为空";
  public static final String EXCEPTION_URISTRING_IS_NULL_E7458C6A = "uriString 不能为空";
  public static final String EXCEPTION_FILEPATH_IS_NULL_84CE8A66 = "filePath 不能为空";
  public static final String EXCEPTION_DETAILS_IS_NULL_8EDEEA03 = "details 不能为空";
  public static final String EXCEPTION_COLUMNCATEGORY_IS_NULL_0075924B = "columnCategory 不能为空";
  public static final String EXCEPTION_ARGUMENTNAME_IS_NULL_7F8F665F = "argumentName 不能为空";
  public static final String EXCEPTION_PASSEDARGUMENTS_IS_NULL_98D8CB1F = "passedArguments 不能为空";
  public static final String EXCEPTION_TABLEARGUMENTANALYSES_IS_NULL_C8724E40 = "tableArgumentAnalyses 不能为空";
  public static final String EXCEPTION_ARGUMENT_IS_NULL_0CBBD22B = "argument 不能为空";
  public static final String EXCEPTION_TABLEARGUMENTANALYSIS_IS_NULL_CF9F0E25 = "tableArgumentAnalysis 不能为空";
  public static final String EXCEPTION_RULE_IS_NULL_5387C8CC = "rule 不能为空";
  public static final String EXCEPTION_CANNOT_MERGE_STATS_FOR_DIFFERENT_RULES_COLON_ARG_AND_ARG_F0A5D5E6 = "无法 merge stats for different rules: %s 和 %s";
  public static final String EXCEPTION_SCALAR_SUBQUERY_RESULT_SHOULD_ONLY_HAVE_ONE_COLUMN_DOT_893F76CB = "Scalar Subquery result should only have one column.";
  public static final String EXCEPTION_SCALAR_SUBQUERY_RESULT_SHOULD_ONLY_HAVE_ONE_ROW_DOT_F9007BBC = "Scalar Subquery result should only have one row.";
  public static final String EXCEPTION_SCALAR_SUBQUERY_RESULT_SHOULD_NOT_GET_NULL_DATATYPE_OR_NULL_COLUMN_DOT_616056F4 = "Scalar Subquery result should not get null dataType 或 null column.";
  public static final String EXCEPTION_OPERATOR_IS_NULL_F5BB9F59 = "operator 不能为空";
  public static final String EXCEPTION_EXPRESSIONS_IS_NULL_C44D9384 = "expressions 不能为空";
  public static final String EXCEPTION_CORRELATION_IS_NULL_F8327EAD = "correlation 不能为空";
  public static final String EXCEPTION_CORRELATEDPREDICATES_IS_NULL_5FCB8011 = "correlatedPredicates 不能为空";
  public static final String EXCEPTION_GROUPING_KEYS_WERE_CORRELATED_EE1C8406 = "grouping keys were correlated";
  public static final String EXCEPTION_EXPECTED_CONSTANT_SYMBOLS_TO_CONTAIN_ALL_CORRELATED_SYMBOLS_LOCAL_EQUIVALENTS_E20CB055 = "期望 constant symbols 到 contain all correlated symbols local equivalents";
  public static final String EXCEPTION_EXPECTED_SYMBOLS_TO_PROPAGATE_TO_CONTAIN_ALL_CONSTANT_SYMBOLS_C9D876E4 = "期望 symbols 到 propagate 到 contain all constant symbols";
  public static final String EXCEPTION_CARDINALITYRANGE_IS_NULL_8FDEE0B4 = "cardinalityRange 不能为空";
  public static final String EXCEPTION_METADATAEXPRESSIONS_IS_NULL_3752914C = "metadataExpressions 不能为空";
  public static final String EXCEPTION_EXPRESSIONSCANPUSHDOWN_IS_NULL_DC8DFEB3 = "expressionsCanPushDown 不能为空";
  public static final String EXCEPTION_EXPRESSIONSCANNOTPUSHDOWN_IS_NULL_63BC9AF9 = "expressions无法PushDown 不能为空";
  public static final String EXCEPTION_FILTER_PREDICATE_OF_FILTERNODE_IS_NULL_C3964179 = "Filter predicate of FilterNode 不能为空";
  public static final String EXCEPTION_UNSUPPORTED_JOIN_TYPE_COLON_ARG_9FB6751B = "不支持的 join type: %s";
  public static final String EXCEPTION_UNIQUEID_IN_PREDICATE_IS_NOT_YET_SUPPORTED_7B5D2EAF = "UniqueId in predicate is not yet supported";
  public static final String EXCEPTION_MAPPERPROVIDER_IS_NULL_472725D5 = "mapperProvider 不能为空";
  public static final String EXCEPTION_CORRELATIONMAPPING_IS_NULL_9D595C82 = "correlationMapping 不能为空";
  public static final String EXCEPTION_MAPPINGS_IS_NULL_23BD9025 = "mappings 不能为空";
  public static final String EXCEPTION_AGGREGATE_WITH_ORDER_BY_DOES_NOT_SUPPORT_PARTIAL_AGGREGATION_D5BDD21F = "Aggregate with ORDER BY does not support partial aggregation";
  public static final String EXCEPTION_LEFTEFFECTIVEPREDICATE_MUST_ONLY_CONTAIN_SYMBOLS_FROM_LEFTSYMBOLS_DB3259B8 = "leftEffectivePredicate must only contain symbols 来自 leftSymbols";
  public static final String EXCEPTION_RIGHTEFFECTIVEPREDICATE_MUST_ONLY_CONTAIN_SYMBOLS_FROM_RIGHTSYMBOLS_4B97238D = "rightEffectivePredicate must only contain symbols 来自 rightSymbols";
  public static final String EXCEPTION_OUTEREFFECTIVEPREDICATE_MUST_ONLY_CONTAIN_SYMBOLS_FROM_OUTERSYMBOLS_99FC2AA9 = "outerEffectivePredicate must only contain symbols 来自 outerSymbols";
  public static final String EXCEPTION_INNEREFFECTIVEPREDICATE_MUST_ONLY_CONTAIN_SYMBOLS_FROM_INNERSYMBOLS_ECB7C6A2 = "innerEffectivePredicate must only contain symbols 来自 innerSymbols";
  public static final String EXCEPTION_IDALLOCATOR_IS_NULL_752B308D = "idAllocator 不能为空";
  public static final String EXCEPTION_PLANOPTIMIZERSSTATSCOLLECTOR_IS_NULL_9DA4B0CC = "planOptimizersStatsCollector 不能为空";
  public static final String EXCEPTION_SUBQUERY_RESULT_TYPE_MUST_BE_ORDERABLE_82AF0EFA = "Subquery result type 必须为 orderable";
  public static final String EXCEPTION_ALL_THE_NON_CORRELATED_SUBQUERIES_SHOULD_BE_REWRITTEN_AT_THIS_POINT_B4614541 = "All the non correlated subqueries should be rewritten at this point";
  public static final String EXCEPTION_CHANGEDPLANNODES_IS_NULL_5ECBDE28 = "changedPlanNodes 不能为空";
  public static final String EXCEPTION_MAPPINGFUNCTION_IS_NULL_212D6109 = "mappingFunction 不能为空";
  public static final String EXCEPTION_ROOT_NODE_MUST_RETURN_ONLY_ONE_FF42061C = "Root node must return only one";
  public static final String EXCEPTION_SIZE_OF_TOPKNODE_CAN_ONLY_BE_1_IN_LOGICAL_PLAN_DOT_DB32E3C5 = "Size of TopKNode can only be 1 in logical plan.";
  public static final String EXCEPTION_THE_SIZE_OF_LEFT_CHILDREN_NODE_OF_JOINNODE_SHOULD_BE_1_F3437368 = "The size of left children node of JoinNode should be 1";
  public static final String EXCEPTION_THE_SIZE_OF_RIGHT_CHILDREN_NODE_OF_JOINNODE_SHOULD_BE_1_6BA167CF = "The size of right children node of JoinNode should be 1";
  public static final String EXCEPTION_THE_SIZE_OF_LEFT_CHILDREN_NODE_OF_SEMIJOINNODE_SHOULD_BE_1_FFEE3F41 = "The size of left children node of SemiJoinNode should be 1";
  public static final String EXCEPTION_THE_SIZE_OF_RIGHT_CHILDREN_NODE_OF_SEMIJOINNODE_SHOULD_BE_1_AE90C4B8 = "The size of right children node of SemiJoinNode should be 1";
  public static final String EXCEPTION_CHILDRENNODES_SHOULD_NOT_BE_NULL_DOT_0C93B063 = "childrenNodes 不能为空.";
  public static final String EXCEPTION_CHILDRENNODES_SHOULD_NOT_BE_EMPTY_DOT_E5555FD9 = "childrenNodes should not be empty.";
  public static final String EXCEPTION_SIZE_OF_TOPKRANKINGNODE_CAN_ONLY_BE_1_IN_LOGICAL_PLAN_DOT_20D6A513 = "Size of TopKRankingNode can only be 1 in logical plan.";
  public static final String EXCEPTION_STATS_IS_NULL_D3627E6A = "stats 不能为空";
  public static final String EXCEPTION_USELEGACYRULES_IS_NULL_0AD13CAB = "useLegacyRules 不能为空";
  public static final String EXCEPTION_RULES_IS_NULL_DF243716 = "rules 不能为空";
  public static final String EXCEPTION_EXPECTED_CHILD_TO_BE_A_GROUP_REFERENCE_DOT_FOUND_COLON_EC01971C = "期望 child 到 be a group reference. Found: ";
  public static final String EXCEPTION_TIMEOUT_HAS_TO_BE_A_NON_MINUS_NEGATIVE_NUMBER_LEFT_BRACKET_MILLISECONDS_RIGHT_BR_5201D8B3 = "Timeout has 到 be a non-negative number [milliseconds]";
  public static final String EXCEPTION_NODE_QUOTE_ARG_QUOTE_IS_NOT_A_GROUPREFERENCE_73C8C127 = "Node '%s' is not a GroupReference";
  public static final String EXCEPTION_MEMBER_IS_NULL_466D8670 = "member 不能为空";
  public static final String EXCEPTION_INVALID_GROUP_COLON_ARG_C0BAD253 = "无效的 group: %s";
  public static final String EXCEPTION_ARG_COLON_TRANSFORMED_EXPRESSION_DOESN_QUOTE_T_PRODUCE_SAME_OUTPUTS_COLON_ARG_VS_F9BAF138 = "%s: transformed expression doesn't produce same outputs: %s vs %s";
  public static final String EXCEPTION_CANNOT_DELETE_GROUP_THAT_HAS_INCOMING_REFERENCES_83C9D700 = "无法 delete group that has incoming references";
  public static final String EXCEPTION_REFERENCE_TO_REMOVE_NOT_FOUND_2EB93289 = "Reference 到 remove not found";
  public static final String EXCEPTION_TRANSFORMEDPLAN_IS_NULL_83B2099A = "transformedPlan 不能为空";
  public static final String EXCEPTION_NEWCHILDREN_IS_NOT_EMPTY_170FCE18 = "newChildren is not empty";
  public static final String EXCEPTION_GROUPINGSETS_IS_NULL_8EE6D9BF = "groupingSets 不能为空";
  public static final String EXCEPTION_PREGROUPEDSYMBOLS_IS_NULL_DC24FF7B = "preGroupedSymbols 不能为空";
  public static final String EXCEPTION_GROUPING_COLUMNS_DOES_NOT_CONTAIN_GROUPID_COLUMN_83976C83 = "Grouping columns does not contain groupId column";
  public static final String EXCEPTION_ORDER_BY_DOES_NOT_SUPPORT_DISTRIBUTED_AGGREGATION_05109B26 = "ORDER BY does not support distributed aggregation";
  public static final String EXCEPTION_DATE_BIN_FUNCTION_MUST_BE_THE_LAST_GROUPINGKEY_EE955FF5 = "date_bin function 必须为 the last GroupingKey";
  public static final String EXCEPTION_PRE_MINUS_GROUPED_SYMBOLS_MUST_BE_A_SUBSET_OF_THE_GROUPING_KEYS_AFC6C33D = "Pre-grouped symbols 必须为 a subset of the grouping keys";
  public static final String EXCEPTION_EXPECTED_AGGREGATION_TO_HAVE_DISTINCT_INPUT_EC6AF059 = "期望 aggregation 到 have DISTINCT input";
  public static final String EXCEPTION_MISMATCHED_CHILD_LEFT_PAREN_ARG_RIGHT_PAREN_AND_PERMITTED_OUTPUTS_LEFT_PAREN_ARG_57801144 = "Mismatched child (%s) 和 permitted outputs (%s) sizes";
  public static final String EXCEPTION_MISSING_TYPE_FOR_EXPRESSION_3D66D302 = "missing type for expression";
  public static final String EXCEPTION_EXCEPTNODE_TRANSLATION_RESULT_HAS_NO_COUNT_SYMBOLS_8653930E = "ExceptNode translation result has no count symbols";
  public static final String EXCEPTION_UNEXPECTED_CORRELATED_JOIN_TYPE_COLON_ARG_27E8EC42 = "un期望 correlated join type: %s";
  public static final String EXCEPTION_CORRELATION_IN_ARG_JOIN_2F78ACC3 = "correlation in %s JOIN";
  public static final String EXCEPTION_PLANNODE_IS_NULL_49FBBFCF = "planNode 不能为空";
  public static final String EXCEPTION_COUNTSYMBOLS_IS_NULL_416D96FB = "countSymbols 不能为空";
  public static final String EXCEPTION_ROWNUMBERSYMBOL_IS_NULL_BA30E0AA = "rowNumberSymbol 不能为空";
  public static final String EXCEPTION_CANNOT_SIMPLIFY_A_UNIONNODE_9D5B09A7 = "无法 simplify a UnionNode";
  public static final String EXCEPTION_THE_SIZE_OF_MARKERS_SHOULD_BE_SAME_AS_THE_SIZE_OF_COUNT_OUTPUT_SYMBOLS_6DBDD287 = "the size of markers should be same as the size of count output symbols";
  public static final String EXCEPTION_ROWNUMBERSYMBOL_IS_EMPTY_34FE9565 = "rowNumberSymbol is empty";
  public static final String EXCEPTION_EXPECTED_SUBQUERY_OUTPUT_SYMBOLS_TO_BE_PRUNED_13B84182 = "期望 subquery output symbols 到 be pruned";
  public static final String EXCEPTION_TYPEANALYZER_IS_NULL_3106B188 = "typeAnalyzer 不能为空";
  public static final String EXCEPTION_UNEXPECTED_CORRELATEDJOIN_TYPE_COLON_47A368C1 = "un期望 CorrelatedJoin type: ";
  public static final String EXCEPTION_UNEXPECTED_NULL_LITERAL_WITHOUT_A_CAST_TO_BOOLEAN_D399CFCB = "Un期望 null literal without a cast 到 boolean";
  public static final String EXCEPTION_REWRITER_IS_NULL_B0D8CC88 = "rewriter 不能为空";
  public static final String EXCEPTION_UNEXPECTED_NODE_TYPE_COLON_ARG_B1C0328F = "un期望 node type: %s";
  public static final String EXCEPTION_RESULT_IS_NULL_031E2F89 = "result 不能为空";
  public static final String EXCEPTION_DISTINCT_NOT_SUPPORTED_0E97D0BB = "不支持 distinct";
  public static final String EXCEPTION_EXPRESSION_IS_NOT_ANALYZED_LEFT_PAREN_ARG_RIGHT_PAREN_COLON_ARG_DAE760B6 = "Expression 尚未分析（%s）：%s";
  public static final String EXCEPTION_SYMBOL_REFERENCES_ARE_NOT_ALLOWED_93779D6C = "不允许使用 symbol references";
  public static final String EXCEPTION_EXPRESSION_INTERPRETER_RETURNED_AN_UNRESOLVED_EXPRESSION_5BCE9A51 = "表达式解释器返回了未解析的表达式";
  public static final String EXCEPTION_NULL_OPERAND_SHOULD_HAVE_BEEN_REMOVED_BY_RECURSIVE_COALESCE_PROCESSING_B6D4D443 = "Null operand 应已在递归 coalesce 处理中移除";
  public static final String EXCEPTION_NULL_VALUE_IS_EXPECTED_TO_BE_REPRESENTED_AS_NULL_COMMA_NOT_NULLLITERAL_9B96D25A = "空值应表示为 null，而不是 NullLiteral";

}
