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
  public static final String CURRENT_DEVICE_ENTRY_IN_TABLESCANOPERATOR_IS_EMPTY =
      "TableScanOperator 中当前设备条目为空";
  public static final String UNEXPECTED_END_OF_EXTERNAL_TSFILE_DEVICE_TASK_READER_AT_DEVICE_INDEX =
      "外部 TsFile 设备任务读取器在设备索引处意外结束：";
  public static final String
      EXTERNAL_TSFILE_DEVICE_TASK_READER_IS_NOT_ALIGNED_WITH_DEVICE_ENTRIES =
          "外部 TsFile 设备任务读取器与设备条目不匹配，索引 %d：期望 %s，实际 %s";
  public static final String FAILED_TO_UPDATE_EXTERNAL_TSFILE_DEVICE_RESOURCES =
      "更新外部 TsFile 设备资源失败";
  public static final String SCHEMA_FILTER_TYPE_IS_NOT_SUPPORTED =
      "不支持 SchemaFilter 类型 %s";
  public static final String
      ATTRIBUTE_FILTER_IS_NOT_SUPPORTED_FOR_EXTERNAL_TSFILE_DEVICE_FILTERING =
          "外部 TsFile 设备过滤暂不支持属性过滤";

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
  public static final String UNKNOWN_DATABASE = "未知数据库 %s";
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
  public static final String LOAD_TSFILE_DEVICE_SCHEMA_MISSING_AUTO_CREATE_DISABLED =
      "设备 %s 在 IoTDB 中不存在且无法被创建。请检查是否启用了 auto-create-schema。";
  public static final String LOAD_TSFILE_MEASUREMENT_SCHEMA_MISSING_AUTO_CREATE_DISABLED =
      "时间序列 %s 在 IoTDB 中不存在且无法被创建。请检查是否启用了 auto-create-schema。";
  public static final String PIPE_GENERATED_LOAD_TSFILE_WAITING_FOR_SCHEMA_METADATA =
      "Pipe 生成的 LoadTsFile 正在等待 schema 元数据传输完成。详情：%s";
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
  public static final String NOT_SUPPORT_FOR_THIS_SQL_IN_ALTER_TOPIC =
      "ALTER TOPIC 不支持此 SQL，请输入 topic 名。";
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
  public static final String UNSUPPORTED_EXTERNAL_TSFILE_DEVICE_FILTER =
      "不支持的外部 TsFile 设备过滤器：";

  // --- Plan / Relational / Table Function ---

  public static final String NO_TABLE_SCHEMA_FOUND_IN_TSFILES =
      "TsFile 中未找到表结构";
  public static final String NO_TABLE_SCHEMA_FOUND_FOR_TABLE_IN_TSFILES =
      "TsFile 中未找到表 %s 的表结构";
  public static final String READ_TSFILE_MUST_BE_PLANNED_AS_EXTERNAL_TSFILE_SCAN_NODE =
      "readTsFile 必须规划为 ExternalTsFileScanNode";
  public static final String MISSING_SCALAR_ARGUMENT =
      "缺少标量参数：";
  public static final String ARGUMENT_SHOULD_NOT_BE_EMPTY =
      "参数 %s 不应为空";
  public static final String INVALID_SCALAR_ARGUMENT =
      "无效的标量参数：";
  public static final String ARGUMENT_SHOULD_BE_A_STRING =
      "参数 %s 应为字符串";
  public static final String ARGUMENT_SHOULD_CONTAIN_AT_LEAST_ONE_PATH =
      "参数 %s 应至少包含一个路径";
  public static final String READ_TSFILE_PATH_IS_NOT_ALLOWED =
      "不允许 readTsFile 路径 %s，因为它可能访问 IoTDB 数据目录 %s";
  public static final String OUTPUT_COLUMN_NAMES_AND_TYPES_SIZE_MISMATCH =
      "输出列名和类型数量不匹配";
  public static final String OUTPUT_COLUMN_NAMES_AND_CATEGORIES_SIZE_MISMATCH =
      "输出列名和类别数量不匹配";
  public static final String READ_TSFILE_TABLE_FUNCTION_HANDLE_DOES_NOT_SUPPORT_SERIALIZATION =
      "ReadTsFileTableFunctionHandle 不支持序列化";
  public static final String READ_TSFILE_TABLE_FUNCTION_HANDLE_DOES_NOT_SUPPORT_DESERIALIZATION =
      "ReadTsFileTableFunctionHandle 不支持反序列化";
  public static final String TSFILE_PATH_DOES_NOT_EXIST =
      "TsFile 路径不存在：";
  public static final String TSFILE_PATH_IS_NEITHER_A_FILE_NOR_A_DIRECTORY =
      "TsFile 路径既不是文件也不是目录：";
  public static final String NO_VALID_TSFILES_FOUND =
      "未找到有效的 TsFile";
  public static final String FAILED_TO_SCAN_TSFILE_PATH =
      "扫描 TsFile 路径失败：";
  public static final String CANNOT_INFER_TABLE_NAME_FROM_TSFILES_MULTIPLE_TABLES =
      "无法从 TsFile 推断表名，因为发现了多个表：%s 和 %s";
  public static final String CANNOT_INFER_TABLE_NAME_FROM_TSFILE_NO_TABLE_SCHEMA =
      "无法从 TsFile 推断表名，因为未找到表结构，文件：";
  public static final String CANNOT_INFER_TABLE_NAME_FROM_TSFILE_MULTIPLE_TABLES =
      "无法从 TsFile 推断表名，因为发现了多个表，文件：";
  public static final String FILE_IS_NOT_A_VALID_TSFILE =
      "文件不是有效的 TsFile：";
  public static final String FAILED_TO_READ_TABLE_SCHEMA_FROM_TSFILE =
      "从 TsFile 读取表结构失败：";
  public static final String MULTIPLE_TIME_COLUMNS_FOUND_WHEN_MERGING_TABLE_SCHEMA =
      "合并表结构时发现多个时间列，表：";
  public static final String TIME_COLUMN_CONFLICTS_WHEN_MERGING_TABLE_SCHEMA =
      "合并表结构时时间列冲突，表：";
  public static final String TAG_COLUMNS_CONFLICT_WHEN_MERGING_TABLE_SCHEMA =
      "合并表结构时标签列冲突，表：";
  public static final String FIELD_COLUMN_HAS_CONFLICTING_DATA_TYPES_WHEN_MERGING_TABLE_SCHEMA =
      "字段列 %s 在合并表 %s 的结构时存在冲突的数据类型";
  public static final String COLUMN_HAS_CONFLICTING_CATEGORIES_WHEN_MERGING_TABLE_SCHEMA =
      "列 %s 在合并表 %s 的结构时存在冲突的类别";
  public static final String FAILED_TO_CREATE_EXTERNAL_TSFILE_DEVICE_TASK_RUN_READER =
      "创建外部 TsFile 设备任务运行读取器失败";
  public static final String UNKNOWN_EXTERNAL_TSFILE_DEVICE_TASK_PARTITION =
      "未知的外部 TsFile 设备任务分区：";
  public static final String EXTERNAL_TSFILE_QUERY_RESOURCE_HAS_BEEN_CLOSED =
      "外部 TsFile 查询资源已关闭：";
  public static final String EXTERNAL_TSFILE_QUERY_RESOURCE_HAS_ALREADY_BEEN_SET =
      "当前 FragmentInstance 中已设置外部 TsFile 查询资源";
  public static final String EXTERNAL_TSFILE_FRAGMENT_INSTANCE_USAGE_COUNT_CANNOT_BE_NEGATIVE =
      "外部 TsFile FragmentInstance 使用计数不能为负数";
  public static final String FAILED_TO_DESERIALIZE_EXTERNAL_TSFILE_RESOURCE =
      "反序列化外部 TsFile 资源失败：%s，%s";
  public static final String FAILED_TO_FLUSH_EXTERNAL_TSFILE_DEVICE_TASK_PARTITION =
      "刷写外部 TsFile 设备任务分区失败";
  public static final String EXTERNAL_TSFILE_DEVICE_TASK_PARTITION_COUNT_MUST_BE_POSITIVE =
      "外部 TsFile 设备任务分区数量必须为正数";
  public static final String FAILED_TO_CREATE_EXTERNAL_TSFILE_DEVICE_COLLECTOR =
      "创建外部 TsFile 设备收集器失败";

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
  public static final String READ_TSFILE_TABLE_FUNCTION_HANDLE_IS_INVALID =
      "readTsFile 表函数句柄无效";

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
  public static final String
      EXTERNAL_TSFILE_AGGREGATION_SCAN_NODE_DEVICE_ENTRIES_MUST_BE_SET_BY_DEVICE_ENTRY_INDEXES =
          "ExternalTsFileAggregationScanNode 的设备条目必须通过设备条目索引设置";
  public static final String EXTERNAL_TSFILE_AGGREGATION_SCAN_NODE_CANNOT_BE_SERIALIZED =
      "ExternalTsFileAggregationScanNode 读取本地外部 TsFile，因此不能被序列化";
  public static final String
      EXTERNAL_TSFILE_SCAN_NODE_DEVICE_ENTRIES_MUST_BE_SET_BY_DEVICE_ENTRY_INDEXES =
          "ExternalTsFileScanNode 的设备条目必须通过设备条目索引设置";
  public static final String EXTERNAL_TSFILE_SCAN_NODE_CANNOT_BE_SERIALIZED =
      "ExternalTsFileScanNode 读取本地外部 TsFile，因此不能被序列化";

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


  private DataNodeQueryMessages() {}
}
