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
      "[拓扑] ConfigNode 返回的本节点（{}）最新视图：{}";
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
      "InsertStatement 应至少包含一个测点";
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
      "插入操作包含重复的测点：";
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
      "[发送关闭 SinkChannel 事件] 到 [ShuffleSinkHandle: {}, 索引: {}] 失败。";
  public static final String LOCAL_SINK_CHANNEL_STATE_IS =
      "LocalSinkChannel 状态为 .";
  public static final String SCH_LISTENER_ON_FINISH =
      "[ScH 监听器完成]";
  public static final String SCH_LISTENER_ALREADY_RELEASED =
      "[ScH 监听器已释放]";
  public static final String SCH_LISTENER_ON_ABORT =
      "[ScH 监听器中止]";
  public static final String SHUFFLE_SINK_HANDLE_LISTENER_ON_FINISH =
      "[ShuffleSinkHandle 监听器完成]";
  public static final String SHUFFLE_SINK_HANDLE_LISTENER_ON_END_OF_TSBLOCKS =
      "[ShuffleSinkHandle 监听器 TsBlock 结束]";
  public static final String SHUFFLE_SINK_HANDLE_LISTENER_ON_ABORT =
      "[ShuffleSinkHandle 监听器中止]";
  public static final String SKH_LISTENER_ON_FINISH =
      "[SkH 监听器完成]";
  public static final String SKH_LISTENER_ON_END_OF_TSBLOCKS =
      "[SkH 监听器 TsBlock 结束]";
  public static final String SKH_LISTENER_ON_ABORT =
      "[SkH 监听器中止]";
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
      "[开始强制释放 FI 数据交换资源]";
  public static final String CLOSE_SOURCE_HANDLE =
      "[关闭 SourceHandle] {}";
  public static final String END_FORCE_RELEASE_FI_DATA_EXCHANGE_RESOURCE =
      "[结束强制释放 FI 数据交换资源]";
  public static final String CREATE_LOCAL_SINK_HANDLE_TO_PLAN_NODE =
      "为计划节点 {} 的 {} 创建本地 Sink 句柄，目标 {}";
  public static final String CREATE_SINK_HANDLE_TO_PLAN_NODE =
      "为计划节点 {} 的 {} 创建 Sink 句柄，目标 {}";
  public static final String CREATE_LOCAL_SOURCE_HANDLE_FROM =
      "从 {} 为计划节点 {} 的 {} 创建本地 Source 句柄";
  public static final String GET_SERIALIZED_TSBLOCK =
      "[获取序列化 TsBlock] TsBlock:{}";
  public static final String START_ABORT_LOCAL_SOURCE_HANDLE =
      "[开始中止 LocalSourceHandle]";
  public static final String END_ABORT_LOCAL_SOURCE_HANDLE =
      "[结束中止 LocalSourceHandle]";
  public static final String START_CLOSE_LOCAL_SOURCE_HANDLE =
      "[开始关闭 LocalSourceHandle]";
  public static final String END_CLOSE_LOCAL_SOURCE_HANDLE =
      "[结束关闭 LocalSourceHandle]";
  public static final String START_SET_NO_MORE_TSBLOCKS =
      "[开始设置无更多 TsBlock]";
  public static final String START_ABORT_SINK_CHANNEL =
      "[开始中止 SinkChannel]";
  public static final String END_ABORT_SINK_CHANNEL =
      "[结束中止 SinkChannel]";
  public static final String START_CLOSE_SINK_CHANNEL =
      "[开始关闭 SinkChannel]";
  public static final String END_CLOSE_SINK_CHANNEL =
      "[结束关闭 SinkChannel]";
  public static final String ACK_TSBLOCK =
      "[确认 TsBlock] {}.";
  public static final String NOTIFY_NO_MORE_TSBLOCK =
      "[通知无更多 TsBlock]";
  public static final String START_SEND_TSBLOCK_ON_LOCAL =
      "[开始在本地发送 TsBlock]";
  public static final String START_SET_NO_MORE_TSBLOCKS_ON_LOCAL =
      "[开始在本地设置无更多 TsBlock]";
  public static final String END_SET_NO_MORE_TSBLOCKS_ON_LOCAL =
      "[结束在本地设置无更多 TsBlock]";
  public static final String START_ABORT_LOCAL_SINK_CHANNEL =
      "[开始中止 LocalSinkChannel]";
  public static final String END_ABORT_LOCAL_SINK_CHANNEL =
      "[结束中止 LocalSinkChannel]";
  public static final String START_CLOSE_LOCAL_SINK_CHANNEL =
      "[开始关闭 LocalSinkChannel]";
  public static final String END_CLOSE_LOCAL_SINK_CHANNEL =
      "[结束关闭 LocalSinkChannel]";
  public static final String GET_TSBLOCK_FROM_BUFFER =
      "[从缓冲区获取 TsBlock] sequenceId:{}, size:{}";
  public static final String WAIT_FOR_MORE_TSBLOCK =
      "[等待更多 TsBlock]";
  public static final String RECEIVE_NO_MORE_TSBLOCK_EVENT =
      "[收到无更多 TsBlock 事件]";
  public static final String END_PULL_TSBLOCKS_FROM_REMOTE =
      "[结束从远端拉取 TsBlock] 数量:{}";
  public static final String PUT_TSBLOCKS_INTO_BUFFER =
      "[将 TsBlock 放入缓冲区]";
  public static final String SEND_ACK_TSBLOCK =
      "[发送确认 TsBlock] [{}, {}).";
  public static final String START_ABORT_SHUFFLE_SINK_HANDLE =
      "[开始中止 ShuffleSinkHandle]";
  public static final String END_ABORT_SHUFFLE_SINK_HANDLE =
      "[结束中止 ShuffleSinkHandle]";
  public static final String START_CLOSE_SHUFFLE_SINK_HANDLE =
      "[开始关闭 ShuffleSinkHandle]";
  public static final String END_CLOSE_SHUFFLE_SINK_HANDLE =
      "[结束关闭 ShuffleSinkHandle]";
  public static final String SIGNAL_NO_MORE_TSBLOCK_ON_QUEUE =
      "[队列信号无更多 TsBlock]";
  public static final String QUEUE_DESTROYED_WHEN_SET_NO_MORE_TSBLOCKS =
      "调用 setNoMoreTsBlocks 时队列已被销毁。";
  public static final String ADD_TSBLOCK =
      "[添加 TsBlock] TsBlock:{}";

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

      "CALL INFERENCE 函数不能包含超过一列输入，实际发现 [%d] 列输入。";
  public static final String DATA_TYPE_OF_TAG_COLUMN =
      "tag 列的数据类型 ";
  public static final String IS_NOT_STRING =
      " 不是 STRING";
  public static final String THE_SOURCE_PATHS_S_OF_VIEW_S_ARE_MULTIPLE =


      "源路径 [%s] 对应的视图 [%s] 有多个。";
  public static final String ERROR_OCCURRED_DURING_INFERRING_UDF_DATA_TYPE_S =

      "推断 UDF 数据类型时发生错误：%s";
  public static final String ERROR_OCCURRED_DURING_GETTING_UDF_ACCESS_STRATEGY_S =

      "获取 UDF 访问策略时发生错误：%s";
  public static final String UNSUPPORTED_COMPRESSION_S =

      "不支持压缩方式：%s";
  public static final String TIMESERIES_CONDITION_AND_TIME_CONDITION_CANNOT_BE_USED_AT_THE_SAME_TIME =
      "TIMESERIES 条件和 TIME 条件不能同时使用。";
  public static final String LATEST_AND_ORDER_BY_TIMESERIES_CANNOT_BE_USED_AT_THE_SAME_TIME =


      "LATEST 和 ORDER BY TIMESERIES 不能同时使用。";
  public static final String DEVICE_CONDITION_AND_TIME_CONDITION_CANNOT_BE_USED_AT_THE_SAME_TIME =
      "DEVICE 条件和 TIME 条件不能同时使用。";
  public static final String TIME_CONDITION_AND_GROUP_BY_LEVEL_CANNOT_BE_USED_AT_THE_SAME_TIME =


      "TIME 条件和 GROUP BY LEVEL 不能同时使用。";
  public static final String CQ_AT_LEAST_ONE_OF_THE_PARAMETERS_EVERY_INTERVAL_AND_GROUP_BY_INTERVAL_NEEDS_TO_BE =

      "CQ: 参数 `every_interval` 和 `group_by_interval` 至少需要指定一个。";
  public static final String CAN_NOT_USE_CHAR_DOLLAR_OR_INTO_ITEM_IN_ALTER_VIEW_STATEMENT =


      "不能在 ALTER VIEW 语句中使用字符 '$' 或 INTO 项。";
  public static final String TIME_COLUMN_IS_NO_NEED_TO_APPEAR_IN_SELECT_CLAUSE_EXPLICITLY_IT_WILL_ALWAYS_BE_RETURNED =


      "时间列无需在 SELECT 子句中显式出现；只要可能，它总会被返回。";
  public static final String THE_SECOND_PARAMETER_TIME_INTERVAL_SHOULD_BE_A_POSITIVE_INTEGER =


      "第二个参数时间间隔应为正整数。";
  public static final String THE_THIRD_PARAMETER_TIME_SLIDINGSTEP_SHOULD_BE_A_POSITIVE_INTEGER =


      "第三个参数时间 slidingStep 应为正整数。";
  public static final String CONSTANT_OPERAND_S_IS_NOT_ALLOWED_IN_GROUP_BY_VARIATION_THERE_SHOULD_BE_AN_EXPRESSION =

      "GROUP BY VARIATION 中不允许使用常量操作数 [%s]，应使用表达式";
  public static final String CONSTANT_OPERAND_S_IS_NOT_ALLOWED_IN_GROUP_BY_COUNT_THERE_SHOULD_BE_AN_EXPRESSION =

      "GROUP BY COUNT 中不允许使用常量操作数 [%s]，应使用表达式";
  public static final String ORDER_BY_SORT_KEY_S_IS_NOT_CONTAINED_IN_S =


      "ORDER BY：排序键[%s] 不包含在 '%s' 中";
  public static final String ORDER_BY_EXPRESSION_IS_NOT_SUPPORTED_FOR_CURRENT_STATEMENT_SUPPORTED_SORT_KEY =


      "当前语句不支持 ORDER BY 表达式，支持的排序键：";
  public static final String ONLY_FILL_PREVIOUS_SUPPORT_SPECIFYING_THE_TIME_DURATION_THRESHOLD =
      "只有 FILL(PREVIOUS) 支持指定时间持续阈值。";
  public static final String OUT_OF_RANGE_OFFSET_LT_OFFSETVALUE_GT_OFFSETVALUE_SHOULD_BE_INT64 =

      "超出范围。OFFSET <OFFSETValue>：OFFSETValue 应为 Int64。";
  public static final String OUT_OF_RANGE_SOFFSET_LT_SOFFSETVALUE_GT_SOFFSETVALUE_SHOULD_BE_INT32 =

      "超出范围。SOFFSET <SOFFSETValue>：SOFFSETValue 应为 Int32。";
  public static final String FAILED_TO_PARSE_THE_TIMESTAMP =

      "解析时间戳失败：";
  public static final String CURRENT_SYSTEM_TIMESTAMP_PRECISION_IS_S =


      "当前系统时间戳精度为 %s，";
  public static final String PLEASE_CHECK_WHETHER_THE_TIMESTAMP_S_IS_CORRECT =

      "请检查时间戳 %s 是否正确。";
  public static final String LOAD_TSFILE_FORMAT_S_ERROR_PLEASE_INPUT_AUTOREGISTER_SGLEVEL_VERIFY =


      "加载 TsFile 的格式 %s 错误，请输入 AUTOREGISTER | SGLEVEL | VERIFY。";
  public static final String S_IS_ILLEGAL_UNQUOTED_NODE_NAME_CAN_ONLY_CONSIST_OF_DIGITS_CHARACTERS_AND_UNDERSCORE_OR =

      "%s 非法，未引用的节点名只能由数字、字符和下划线组成，或以通配符开头或结尾";
  public static final String S_IS_ILLEGAL_UNQUOTED_NODE_NAME_IN_SELECT_INTO_CLAUSE_CAN_ONLY_CONSIST_OF_DIGITS =


      "%s 非法，SELECT INTO 子句中的未引用节点名只能由数字、字符、$、{ 和 } 组成";
  public static final String S_IS_ILLEGAL_IDENTIFIER_NOT_ENCLOSED_WITH_BACKTICKS_CAN_ONLY_CONSIST_OF_DIGITS =

      "%s 非法，未用反引号括起的标识符只能由数字、字符和下划线组成。";
  public static final String INPUT_TIME_FORMAT_S_ERROR =

      "输入时间格式 %s 错误。 ";
  public static final String INPUT_LIKE_YYYY_MM_DD_HH_MM_SS_YYYY_MM_DDTHH_MM_SS_OR =

      "请输入类似 yyyy-MM-dd HH:mm:ss、yyyy-MM-ddTHH:mm:ss 或 ";
  public static final String REFER_TO_USER_DOCUMENT_FOR_MORE_INFO =

      "更多信息请参阅用户文档。";
  public static final String GRANT_OPTION_IS_DISABLED_PLEASE_CHECK_THE_PARAMETER_ENABLE_GRANT_OPTION =
      "Grant Option 已禁用，请检查参数 enable_grant_option。";
  public static final String S_CAN_ONLY_BE_SET_ON_PATH_ROOT_STAR_STAR =

      "[%s] 只能设置在路径 root.** 上";
  public static final String PRIVILEGE_TYPE =

      "权限类型 ";
  public static final String IS_DEPRECATED_USE =

      " 已弃用，请使用 ";
  public static final String TO_INSTEAD_IT =

      " 替代它";
  public static final String INVALID_FUNCTION_EXPRESSION_ALL_THE_ARGUMENTS_ARE_CONSTANT_OPERANDS =

      "无效的函数表达式，所有参数都是常量操作数：";
  public static final String ERROR_SIZE_OF_INPUT_EXPRESSIONS_EXPRESSION_S_ACTUAL_SIZE_S_EXPECTED_SIZE_S =

      "输入表达式数量错误。表达式：%s，实际数量：%s，期望数量：%s。";
  public static final String CAN_NOT_PARSE_S_TO_LONG_VALUE =

      "无法将 %s 解析为 long 值";
  public static final String THERE_S_DUPLICATE_S_IN_TAG_OR_ATTRIBUTE_CLAUSE =

      "标签或属性子句中存在重复项 [%s]。";
  public static final String NOT_SUPPORT_FOR_THIS_SQL_IN_CREATE_PIPE_PLEASE_ENTER_PIPE_NAME =

      "CREATE PIPE 中不支持该 SQL，请输入 pipe 名称。";
  public static final String NOT_SUPPORT_FOR_THIS_SQL_IN_ALTER_PIPE_PLEASE_ENTER_PIPE_NAME =

      "ALTER PIPE 中不支持该 SQL，请输入 pipe 名称。";
  public static final String NOT_SUPPORT_FOR_THIS_SQL_IN_CREATE_TOPIC_PLEASE_ENTER_TOPICNAME =

      "CREATE TOPIC 中不支持该 SQL，请输入 topicName。";
  public static final String NOT_SUPPORT_FOR_THIS_SQL_IN_DROP_TOPIC_PLEASE_ENTER_TOPICNAME =

      "DROP TOPIC 中不支持该 SQL，请输入 topicName。";
  public static final String NOT_SUPPORT_FOR_THIS_SQL_IN_DROP_SUBSCRIPTION_PLEASE_ENTER_SUBSCRIPTIONID =

      "DROP SUBSCRIPTION 中不支持该 SQL，请输入 subscriptionId。";
  public static final String PLEASE_SET_THE_CORRECT_REQUEST_TYPE =

      "请设置正确的请求类型：";
  public static final String WHEN_SETTING_THE_REQUEST_THE_UNIT_IS_INCORRECT_PLEASE_USE_SEC_MIN_HOUR_DAY_AS_THE_UNIT =

      "设置请求时单位不正确。请使用 'sec'、'min'、'hour'、'day' 作为单位";
  public static final String WHEN_SETTING_THE_SIZE_TIME_THE_UNIT_IS_INCORRECT_PLEASE_USE_B_K_M_G_P_T_AS_THE_UNIT =


      "设置大小/时间时单位不正确。请使用 'B'、'K'、'M'、'G'、'P'、'T' 作为单位";
  public static final String WHEN_SETTING_THE_DISK_SIZE_THE_UNIT_IS_INCORRECT_PLEASE_USE_M_G_P_T_AS_THE_UNIT =

      "设置磁盘大小时单位不正确。请使用 'M'、'G'、'P'、'T' 作为单位";
  public static final String WINDOW_FUNCTION_E_G_HEAD_TAIL_COUNT_SHOULD_BE_SET_IN_VALUE_WHEN_KEY_IS_WINDOW_IN_CALL =


      "CALL INFERENCE 中 key 为 'WINDOW' 时，应在值中设置窗口函数（例如 HEAD、TAIL、COUNT）";
  public static final String THE_OUTPUT_TYPE_OF_THE_EXPRESSION_IN_HAVING_CLAUSE_SHOULD_BE_BOOLEAN_ACTUAL_DATA_TYPE_S =

      "HAVING 子句中表达式的输出类型应为 BOOLEAN，实际数据类型：%s。";
  public static final String IN =
      " 在 ";
  public static final String START_TIME_D_IS_GREATER_THAN_END_TIME_D =

      "开始时间 %d 大于结束时间 %d";
  public static final String THE_COLUMN =
      "列 '";
  public static final String DOES_NOT_EXIST_OR_IS_NOT_A_TAG_COLUMN =

      "' 不存在或不是标签列";
  public static final String THE_RIGHT_HAND_VALUE_OF_TIME_PREDICATE_MUST_BE_A_LONG =

      "时间谓词的右值必须为 long：";
  public static final String THE_OPERATOR_OF_TIME_PREDICATE_MUST_BE_LT_LT_EQ_GT_OR_GT_EQ =

      "时间谓词的操作符必须为 <、<=、> 或 >=：";
  public static final String THE_RIGHT_HAND_VALUE_OF_TAG_PREDICATE_CANNOT_BE_NULL_WITH_EQ_OPERATOR_PLEASE_USE_IS_NULL =


      "使用 '=' 操作符时，标签谓词的右值不能为 null，请改用 'IS NULL'";
  public static final String THE_RIGHT_HAND_VALUE_OF_TAG_PREDICATE_MUST_BE_A_STRING =

      "标签谓词的右值必须为字符串：";
  public static final String SELECT_INTO_PLACEHOLDER_CAN_ONLY_BE_USED_AT_THE_END_OF_THE_PATH =


      "SELECT INTO：占位符 `::` 只能用于路径末尾。";
  public static final String SELECT_INTO_THE_I_OF_DOLLAR_I_SHOULD_BE_GREATER_THAN_0_AND_EQUAL_TO_OR_LESS_THAN_THE =


      "SELECT INTO：${i} 中的 i 必须大于 0，且不超过查询路径前缀长度。";
  public static final String ALIAS_S_CAN_ONLY_BE_MATCHED_WITH_ONE_RESULT_COLUMN =

      "别名 '%s' 只能匹配一个结果列";
  public static final String RESULT_COLUMN_S_WITH_MORE_THAN_ONE_ALIAS_S_S =

      "结果列 %s 有多个别名[%s, %s]";
  public static final String THERE_ARE_TOO_MANY_CONJUNCTS_MORE_THAN_1000_IN_PREDICATE_AFTER_REWRITING_THIS_MAY_BE =


      "谓词重写后的合取项过多（超过 1000 个），可能是查询中的设备过多导致，请尝试使用 ALIGN BY DEVICE";
  public static final String CASE_EXPRESSION_TEXT_AND_OTHER_TYPES_CANNOT_EXIST_AT_THE_SAME_TIME =

      "CASE 表达式：TEXT 不能与其他类型同时存在。";
  public static final String CASE_EXPRESSION_BOOLEAN_AND_OTHER_TYPES_CANNOT_EXIST_AT_THE_SAME_TIME =

      "CASE 表达式：BOOLEAN 不能与其他类型同时存在。";
  public static final String THE_EXPRESSION_IN_THE_WHEN_CLAUSE_MUST_RETURN_BOOLEAN_EXPRESSION_S_ACTUAL_DATA_TYPE_S =

      "WHEN子句中的表达式必须返回 BOOLEAN。表达式：%s，实际数据类型：%s。";
  public static final String INVALID_INPUT_EXPRESSION_DATA_TYPE_EXPRESSION_S_ACTUAL_DATA_TYPE_S_EXPECTED_DATA_TYPE_S =

      "无效的输入表达式数据类型。表达式：%s，实际数据类型：%s，期望数据类型：%s。";
  public static final String S_IN_ORDER_BY_CLAUSE_DOESN_T_EXIST =

      "%s 在 ORDER BY 子句中不存在。";
  public static final String S_IN_ORDER_BY_CLAUSE_SHOULDN_T_REFER_TO_MORE_THAN_ONE_TIMESERIES =

      "%s 在 ORDER BY 子句中不应引用多个时间序列。";
  public static final String THE_DATA_TYPE_OF_S_IS_NOT_COMPARABLE =

      "%s 的数据类型不可比较";
  public static final String GROUP_BY_LEVEL_THE_DATA_TYPES_OF_THE_SAME_OUTPUT_COLUMN_S_SHOULD_BE_THE_SAME =

      "GROUP_BY_LEVEL: 同一输出列[%s]的数据类型必须一致。";
  public static final String CROSS_DEVICE_QUERIES_ARE_NOT_SUPPORTED_IN_ALIGN_BY_DEVICE_QUERIES =

      "ALIGN_BY_DEVICE 查询不支持跨设备查询。";
  public static final String VIEWS_OR_MEASUREMENT_ALIASES_REPRESENTING_THE_SAME_DATA_SOURCE =

      "表示同一数据源的视图或测点别名 ";
  public static final String CANNOT_BE_QUERIED_CONCURRENTLY_IN_ALIGN_BY_DEVICE_QUERIES =


      "不能在 ALIGN BY DEVICE 查询中同时查询。";
  public static final String THE_TYPE_OF_SQL_RESULT_COLUMN_S_IN_D_SHOULD_BE_NUMERIC_WHEN_INFERENCE =

      "SQL 结果列 [%s 位于 %d] 的类型在 inference 时必须为数值类型。";
  public static final String S_IN_ORDER_BY_CLAUSE_DOESN_T_EXIST_IN_THE_RESULT_OF_LAST_QUERY =

      "%s 在 LAST 查询结果的 ORDER BY 子句中不存在。";
  public static final String S_IN_GROUP_BY_CLAUSE_DOESN_T_EXIST =

      "%s 在 GROUP BY 子句中不存在。";
  public static final String S_IN_GROUP_BY_CLAUSE_SHOULDN_T_REFER_TO_MORE_THAN_ONE_TIMESERIES =

      "%s 在 GROUP BY 子句中不应引用多个时间序列。";
  public static final String PLEASE_CHECK_THE_KEEP_CONDITION_S =

      "请检查 keep 条件（[%s]），";
  public static final String IT_NEED_TO_BE_A_CONSTANT_OR_A_COMPARE_EXPRESSION_CONSTRUCTED_BY_KEEP_AND_A_LONG_NUMBER =

      "它必须是常量，或由 'keep' 和 long 数字构造的比较表达式。";
  public static final String THE_QUERY_TIME_RANGE_SHOULD_BE_SPECIFIED_IN_THE_GROUP_BY_TIME_CLAUSE =

      "查询时间范围必须在 GROUP_BY_TIME 子句中指定。";
  public static final String VIEW_PATH_S_OF_SOURCE_COLUMN_S_IS_ILLEGAL_PATH =

      "源列 %s 的视图路径 %s 是非法路径";
  public static final String ALIGN_BY_DEVICE_THE_DATA_TYPES_OF_THE_SAME_MEASUREMENT_COLUMN_SHOULD_BE_THE_SAME_ACROSS =


      "ALIGN BY DEVICE：不同设备中同名测点列的数据类型必须一致。";
  public static final String ALIAS_S_CAN_ONLY_BE_MATCHED_WITH_ONE_TIME_SERIES =

      "别名 '%s' 只能匹配一个时间序列";
  public static final String TAG_AND_ATTRIBUTE_SHOULDN_T_HAVE_THE_SAME_PROPERTY_KEY_S =

      "标签和属性不应拥有相同的属性键 [%s]";
  public static final String S_IS_NOT_A_LEGAL_PROP =

      "%s 不是合法属性。";
  public static final String MEASUREMENT_UNDER_AN_ALIGNED_DEVICE_IS_NOT_ALLOWED_TO_HAVE_THE_SAME_MEASUREMENT_NAME =

      "对齐设备下的测点不允许使用相同的测点名称";
  public static final String VALUE_FILTER_CAN_T_EXIST_IN_THE_CONDITION_OF_SHOW_COUNT_CLAUSE_ONLY_TIME_CONDITION =

      "SHOW/COUNT 子句条件中不能存在值过滤器，仅支持时间条件。";
  public static final String TIME_CONDITION_CAN_T_BE_EMPTY_IN_THE_CONDITION_OF_SHOW_COUNT_CLAUSE =

      "SHOW/COUNT 子句条件中的时间条件不能为空。";
  public static final String MEASUREMENT_UNDER_TEMPLATE_IS_NOT_ALLOWED_TO_HAVE_THE_SAME_MEASUREMENT_NAME =

      "模板下的测点不允许使用相同的测点名称";
  public static final String THE_SUFFIX_PATHS_CAN_ONLY_BE_MEASUREMENT_OR_ONE_LEVEL_WILDCARD =


      "后缀路径只能是测点或一级通配符。";
  public static final String AGGREGATION_RESULTS_CANNOT_BE_AS_INPUT_OF_THE_AGGREGATION_FUNCTION =

      "聚合结果不能作为聚合函数的输入。";
  public static final String INPUT_OF_S_IS_ILLEGAL =

      "'%s' 的输入非法。";
  public static final String RAW_DATA_AND_AGGREGATION_RESULT_HYBRID_INPUT_OF_S_IS_NOT_SUPPORTED =

      "不支持 '%s' 的原始数据和聚合结果混合输入。";
  public static final String ONLY_WRITABLE_VIEW_TIMESERIES_ARE_SUPPORTED_IN_ALIGN_BY_DEVICE_QUERIES =

      "ALIGN_BY_DEVICE 查询仅支持可写视图时间序列。";
  public static final String INPUT_SERIES_OF_SCALAR_FUNCTION_DIFF_ONLY_SUPPORTS_NUMERIC_DATA_TYPES_INT32_INT64_FLOAT =

      "标量函数 [DIFF] 的输入序列仅支持数值数据类型 [INT32, INT64, FLOAT, DOUBLE]";
  public static final String ARGUMENT_EXCEPTION_THE_SCALAR_FUNCTION_SUBSTRING_NEEDS_AT_LEAST_ONE_ARGUMENT_IT_MUST_BE =

      "参数异常，标量函数 [SUBSTRING] 至少需要一个参数，且必须为有符号整数。";
  public static final String SYNTAX_ERROR_PLEASE_CHECK_THAT_THE_PARAMETERS_OF_THE_FUNCTION_ARE_CORRECT =

      "语法错误，请检查函数参数是否正确。";
  public static final String UNSUPPORTED_DATA_TYPE_S_FOR_FUNCTION_SUBSTRING =

      "函数 SUBSTRING 不支持数据类型 %s。";
  public static final String ARGUMENT_EXCEPTION_THE_SCALAR_FUNCTION_SUBSTRING_BEGINPOSITION_AND_LENGTH_MUST_BE =

      "参数异常，标量函数 [SUBSTRING] 的 beginPosition 和长度必须大于 0。";
  public static final String INPUT_SERIES_OF_SCALAR_FUNCTION_ROUND_ONLY_SUPPORTS_NUMERIC_DATA_TYPES_INT32_INT64_FLOAT =

      "标量函数 [ROUND] 的输入序列仅支持数值数据类型 [INT32, INT64, FLOAT, DOUBLE]";
  public static final String UNSUPPORTED_DATA_TYPE_S_FOR_FUNCTION_REPLACE =

      "函数 REPLACE 不支持数据类型 %s。";
  public static final String TIMESERIES_UNDER_THIS_DEVICE_ISS_ALIGNED_PLEASE_USE_CREATESTIMESERIES_OR_CHANGE_DEVICE =

      "该设备下的时间序列%s对齐，请使用 create%sTimeSeries 或更改设备。（路径：%s）";
  public static final String NOT =

      "未";
  public static final String ALIGNED =
      "Aligned";
  public static final String AUTO_CREATE_OR_VERIFY_SCHEMA_ERROR_DETAIL_S =

      "自动创建或校验 schema 失败。详情：%s。";
  public static final String THE_FILE_S_IS_NOT_A_VALID_TSFILE_PLEASE_CHECK_THE_INPUT_FILE =

      "文件 %s 不是有效的 TsFile。请检查输入文件。";
  public static final String AUTO_CREATE_OR_VERIFY_SCHEMA_ERROR_WHEN_EXECUTING_STATEMENT_S_DETAIL_S =

      "执行语句 %s 时自动创建或校验 schema 失败。详情：%s。";
  public static final String TTL_VALUE_MUST_BE_INF_OR_A_LONG_LITERAL_BUT_NOW_IS =

      "ttl 值必须为 'INF' 或 long 字面量，当前为：";
  public static final String COLUMNS_IN_TABLE_SHALL_NOT_SHARE_THE_SAME_NAME_S =

      "表中的列不应重名：'%s'。";
  public static final String THE_DUPLICATED_SOURCE_MEASUREMENT_S_IS_UNSUPPORTED_YET =

      "暂不支持重复的源测点 %s。";
  public static final String THE_LENGTH_OF_DATABASE_NAME_SHALL_NOT_EXCEED =

      "数据库名称长度不得超过 ";
  public static final String THE_DATABASE_NAME_CAN_ONLY_CONTAIN_ENGLISH_OR_CHINESE_CHARACTERS_NUMBERS_BACKTICKS_AND =

      "数据库名称只能包含英文或中文字符、数字、反引号和下划线。";
  public static final String IS_CURRENTLY_NOT_ALLOWED =

      "' 当前不允许。";
  public static final String VALUE_MUST_BE_A_LONGLITERAL_BUT_NOW_IS =

      " 值必须为 LongLiteral，当前为 ";
  public static final String VALUE =

      "，值：";
  public static final String VALUE_MUST_BE_EQUAL_TO_OR_GREATER_THAN_0_BUT_NOW_IS =

      " 值必须大于等于 0，当前为：";
  public static final String VALUE_MUST_BE_LOWER_THAN =

      " 值必须小于 ";
  public static final String BUT_NOW_IS =

      "，当前为：";
  public static final String FAILED_TO_CREATE_PIPE_S_SETTING_S_IS_NOT_ALLOWED =

      "创建 pipe %s 失败：不允许设置 %s。";
  public static final String FAILED_TO_S_PIPE_S_IN_IOTDB_SOURCE_PASSWORD_MUST_BE_SET_WHEN_THE_USERNAME_IS_SPECIFIED =

      "执行 %s pipe %s 失败：iotdb-source 中指定 username 时必须设置 password。";
  public static final String ALTER =
      "修改";
  public static final String CREATE =
      "创建";
  public static final String FAILED_TO_S_PIPE_S_IN_WRITE_BACK_SINK_PASSWORD_MUST_BE_SET_WHEN_THE_USERNAME_IS =

      "执行 %s pipe %s 失败：write-back-sink 中指定 username 时必须设置 password。";
  public static final String FAILED_TO_ALTER_PIPE_S_MODIFYING_S_IS_NOT_ALLOWED =

      "修改 pipe %s 失败：不允许修改 %s。";
  public static final String FAILED_TO_ALTER_PIPE_THE_SOURCE_PLUGIN_OF_THE_PIPE_CANNOT_BE_CHANGED_FROM_S_TO_S =


      "修改 pipe 失败，pipe 的源插件不能从 %s 改为 %s";
  public static final String FAILED_TO_ALTER_PIPE_S_IN_IOTDB_SOURCE_PASSWORD_MUST_BE_SET_WHEN_THE_USERNAME_IS =

      "alter pipe %s 失败：iotdb-source 中指定 username 时必须设置 password。";
  public static final String FAILED_TO_ALTER_PIPE_S_IN_WRITE_BACK_SINK_PASSWORD_MUST_BE_SET_WHEN_THE_USERNAME_IS =

      "alter pipe %s 失败：write-back-sink 中指定 username 时必须设置 password。";
  public static final String PREPARED_STATEMENT_S_ALREADY_EXISTS =

      "预编译语句 '%s' 已存在。";
  public static final String INSUFFICIENT_MEMORY_FOR_PREPAREDSTATEMENT_S =
      "PreparedStatement '%s' 内存不足。";
  public static final String PLEASE_DEALLOCATE_SOME_PREPAREDSTATEMENTS_AND_TRY_AGAIN =
      "请释放部分 PreparedStatement 后重试。";
  public static final String THE_TABLE =
      "表 ";
  public static final String IS_A_BASE_TABLE_DOES_NOT_SUPPORT_SHOW_CREATE_VIEW =

      " 是基础表，不支持 SHOW CREATE VIEW。";
  public static final String THE_PARAMETERS =
      "参数 '";
  public static final String MUST_BE_CONSISTENT_ACROSS_THE_ENTIRE_CLUSTER_AND_ONLY_ONE_CAN_BE_SET_AT_A_TIME =

      "' 必须在整个集群中保持一致，且同一时间只能设置一个。";
  public static final String CANNOT_INSERT_INTO_MULTIPLE_DATABASES_WITHIN_ONE_STATEMENT_PLEASE_SPLIT_THEM_MANUALLY =
      "不能在一条语句中写入多个数据库，请手动拆分。";
  public static final String THE_MEASUREMENTLIST_S_SIZE_D_IS_NOT_CONSISTENT_WITH_THE_VALUELIST_S_SIZE_D =
      "measurementList 的大小 %d 与 valueList 的大小 %d 不一致";
  public static final String THE_MEASUREMENTLIST_S_SIZE_D_IS_NOT_CONSISTENT_WITH_THE_COLUMNLIST_S_SIZE_D =
      "measurementList 的大小 %d 与 columnList 的大小 %d 不一致";
  public static final String MEASUREMENT_CONTAINS_NULL_OR_EMPTY_STRING =

      "测点包含 null 或空字符串：";
  public static final String CAN_T_BE_USED_IN_GROUP_BY_TAG_IT_WILL_BE_SUPPORTED_IN_THE_FUTURE =

      " 不能用于按 tag 分组，未来版本将支持。";
  public static final String COMMON_QUERIES_AND_AGGREGATED_QUERIES_ARE_NOT_ALLOWED_TO_APPEAR_AT_THE_SAME_TIME =

      "普通查询和聚合查询不允许同时出现";
  public static final String EXPRESSION_OF_HAVING_CLAUSE_CAN_NOT_BE_USED_IN_NONAGGREGATIONQUERY =
      "HAVING 子句中的表达式不能用于 NonAggregationQuery";
  public static final String SORTING_BY_DEVICE_IS_ONLY_SUPPORTED_IN_ALIGN_BY_DEVICE_QUERIES =

      "仅 ALIGN_BY_DEVICE 查询支持按设备排序。";
  public static final String CQ_EVERY_INTERVAL_D_SHOULD_NOT_BE_LOWER_THAN_THE_CONTINUOUS_QUERY_MINIMUM_EVERY_INTERVAL =

      "CQ：every interval [%d] 不应小于配置项 `continuous_query_minimum_every_interval` [%d]。";
  public static final String CQ_THE_START_TIME_OFFSET_SHOULD_BE_GREATER_THAN_END_TIME_OFFSET =

      "CQ：开始时间偏移量应大于结束时间偏移量。";
  public static final String CQ_THE_START_TIME_OFFSET_SHOULD_BE_GREATER_THAN_OR_EQUAL_TO_EVERY_INTERVAL =

      "CQ：开始时间偏移量应大于或等于 every interval。";
  public static final String CQ_SPECIFYING_TIME_RANGE_IN_GROUP_BY_TIME_CLAUSE_IS_PROHIBITED =

      "CQ: 禁止在 GROUP_BY_TIME 子句中指定时间范围。";
  public static final String CANNOT_CREATE_VIEWS_USING_DATA_SOURCES_WITH_CALCULATED_EXPRESSIONS_WHILE_USING_INTO_ITEM =


      "使用 INTO 项时，不能通过带计算表达式的数据源创建视图。";
  public static final String TREE_DEVICE_VIEW_WITH_MULTIPLE_DATABASES =

      "包含多个数据库的树设备视图（";
  public static final String IS_UNSUPPORTED_YET =

      "）暂不支持。";
  public static final String COMPLEX_ASOF_MAIN_JOIN_EXPRESSION_S_IS_NOT_SUPPORTED =

      "不支持复杂的 ASOF 主 JOIN 表达式 [%s]";
  public static final String UNEXPECTED_DESCRIPTOR_TYPE =

      "意外的描述符类型：";
  public static final String WHEN_CLAUSE_OPERAND_TYPE_MUST_MATCH_CASE_OPERAND_TYPE_S_VS_S =

      "WHEN子句操作数类型必须匹配 CASE 操作数类型：%s vs %s";
  public static final String ALL_RESULT_TYPES_MUST_BE_THE_SAME_S =

      "所有结果类型必须一致：%s";
  public static final String DEFAULT_RESULT_TYPE_MUST_BE_THE_SAME_AS_WHEN_RESULT_TYPES_S_VS_S =

      "默认结果类型必须与 WHEN结果类型一致：%s vs %s";
  public static final String ALL_OPERANDS_MUST_HAVE_THE_SAME_TYPE_S =

      "所有操作数必须具有相同类型：%s";
  public static final String TO =

      " 到 ";
  public static final String SCALAR_FUNCTION =
      "标量函数 ";
  public static final String ONLY_SUPPORTS_ONE_NUMERIC_DATA_TYPES_INT32_INT64_FLOAT_DOUBLE_AND_ONE_BOOLEAN =

      " 仅支持一个数值数据类型 [INT32, INT64, FLOAT, DOUBLE] 和一个 boolean";
  public static final String ONLY_SUPPORTS_TWO_NUMERIC_DATA_TYPES_INT32_INT64_FLOAT_DOUBLE =

      " 仅支持两个数值数据类型 [INT32, INT64, FLOAT, DOUBLE]";
  public static final String ONLY_ACCEPTS_TWO_OR_THREE_ARGUMENTS_AND_THEY_MUST_BE_TEXT_OR_STRING_DATA_TYPE =

      " 仅接受两个或三个参数，且参数必须为 TEXT 或 STRING 数据类型。";
  public static final String ONLY_ACCEPTS_TWO_OR_THREE_ARGUMENTS_AND_FIRST_MUST_BE_TEXT_OR_STRING_DATA_TYPE_SECOND =

      " 仅接受两个或三个参数，第一个参数必须为 TEXT 或 STRING 数据类型，第二和第三个参数必须为数值数据类型 [INT32, INT64]";
  public static final String ONLY_ACCEPTS_ONE_ARGUMENT_AND_IT_MUST_BE_TEXT_OR_STRING_OR_BLOB_OR_OBJECT_DATA_TYPE =

      " 仅接受一个参数，且参数必须为 TEXT、STRING、BLOB 或 OBJECT 数据类型。";
  public static final String ONLY_ACCEPTS_ONE_ARGUMENT_AND_IT_MUST_BE_TEXT_OR_STRING_DATA_TYPE =
      " 仅接受一个参数，且参数必须为 text 或 string 数据类型。";
  public static final String ONLY_ACCEPTS_ONE_OR_TWO_ARGUMENTS_AND_THEY_MUST_BE_TEXT_OR_STRING_DATA_TYPE =
      " 仅接受一个或两个参数，且参数必须为 text 或 string 数据类型。";
  public static final String ONLY_ACCEPTS_TWO_ARGUMENTS_AND_THEY_MUST_BE_TEXT_OR_STRING_DATA_TYPE =
      " 仅接受两个参数，且参数必须为 text 或 string 数据类型。";
  public static final String ONLY_ACCEPTS_TWO_OR_MORE_ARGUMENTS_AND_THEY_MUST_BE_TEXT_OR_STRING_DATA_TYPE =

      " 仅接受两个或更多参数，且参数必须为 TEXT 或 STRING 数据类型。";
  public static final String ONLY_ACCEPTS_ONE_ARGUMENT_AND_IT_MUST_BE_DOUBLE_FLOAT_INT32_OR_INT64_DATA_TYPE =
      " 仅接受一个参数，且参数必须为 Double、Float、Int32 或 Int64 数据类型。";
  public static final String ACCEPTS_NO_ARGUMENT =
      " 不接受参数。";
  public static final String ONLY_ACCEPTS_TWO_OR_THREE_ARGUMENTS_AND_THE_SECOND_AND_THIRD_MUST_BE_TIMESTAMP_DATA_TYPE =
      " 仅接受两个或三个参数，且第二、第三个参数必须为 TIMESTAMP 数据类型。";
  public static final String MUST_HAVE_AT_LEAST_TWO_ARGUMENTS_AND_FIRST_ARGUMENT_PATTERN_MUST_BE_TEXT_OR_STRING_TYPE =


      " 必须至少有两个参数，且第一个模式参数必须为 TEXT 或 STRING 类型。";
  public static final String MUST_HAVE_AT_LEAST_TWO_ARGUMENTS_AND_ALL_TYPE_MUST_BE_THE_SAME =

      " 必须至少有两个参数，且所有类型必须一致。";
  public static final String SCALAR_FUNCTION_S_ONLY_ACCEPTS_TWO_ARGUMENTS_AND_THEY_MUST_BE_INT32_OR_INT64_DATA_TYPE =

      "标量函数 %s 仅接受两个参数，且参数必须为 Int32 或 Int64 数据类型。";
  public static final String SCALAR_FUNCTION_S_ONLY_ACCEPTS_ONE_ARGUMENT_AND_IT_MUST_BE_INT32_OR_INT64_DATA_TYPE =

      "标量函数 %s 仅接受一个参数，且参数必须为 Int32 或 Int64 数据类型。";
  public static final String SCALAR_FUNCTION_S_ONLY_ACCEPTS_ONE_ARGUMENT_AND_IT_MUST_BE_TEXT_STRING_OR_BLOB_DATA_TYPE =

      "标量函数 %s 仅接受一个参数，且参数必须为 TEXT、STRING 或 BLOB 数据类型。";
  public static final String SCALAR_FUNCTION_S_ONLY_ACCEPTS_ONE_ARGUMENT_AND_IT_MUST_BE_TEXT_OR_STRING_DATA_TYPE =

      "标量函数 %s 仅接受一个参数，且参数必须为 TEXT 或 STRING 数据类型。";
  public static final String SCALAR_FUNCTION_S_ONLY_ACCEPTS_ONE_ARGUMENT_AND_IT_MUST_BE_TEXT_STRING_OR_BLOB_DATA_TYPE_2 =

      "标量函数 %s 仅接受一个参数，且参数必须为 TEXT、STRING 或 BLOB 数据类型。";
  public static final String SCALAR_FUNCTION_S_ONLY_ACCEPTS_TWO_ARGUMENTS_FIRST_ARGUMENT_MUST_BE_TEXT_STRING_OR_BLOB =

      "标量函数 %s 仅接受两个参数，第一个参数必须为 TEXT、STRING 或 BLOB 类型，第二个参数必须为 STRING 或 TEXT 类型。";
  public static final String SCALAR_FUNCTION_S_ONLY_ACCEPTS_ONE_ARGUMENT_AND_IT_MUST_BE_INT32_DATA_TYPE =

      "标量函数 %s 仅接受一个参数，且参数必须为 Int32 数据类型。";
  public static final String SCALAR_FUNCTION_S_ONLY_ACCEPTS_ONE_ARGUMENT_AND_IT_MUST_BE_BLOB_DATA_TYPE =

      "标量函数 %s 仅接受一个参数，且参数必须为 BLOB 数据类型。";
  public static final String SCALAR_FUNCTION_S_ONLY_ACCEPTS_ONE_ARGUMENT_AND_IT_MUST_BE_INT64_DATA_TYPE =

      "标量函数 %s 仅接受一个参数，且参数必须为 Int64 数据类型。";
  public static final String SCALAR_FUNCTION_S_ONLY_ACCEPTS_ONE_ARGUMENT_AND_IT_MUST_BE_FLOAT_DATA_TYPE =

      "标量函数 %s 仅接受一个参数，且参数必须为 Float 数据类型。";
  public static final String SCALAR_FUNCTION_S_ONLY_ACCEPTS_ONE_ARGUMENT_AND_IT_MUST_BE_DOUBLE_DATA_TYPE =

      "标量函数 %s 仅接受一个参数，且参数必须为 Double 数据类型。";
  public static final String SCALAR_FUNCTION_S_ONLY_ACCEPTS_THREE_ARGUMENTS_FIRST_ARGUMENT_MUST_BE_BLOB_TYPE =

      "标量函数 %s 仅接受三个参数，第一个参数必须为 BLOB 类型，";
  public static final String SECOND_ARGUMENT_MUST_BE_INT32_OR_INT64_TYPE_THIRD_ARGUMENT_MUST_BE_BLOB_TYPE =

      "第二个参数必须为 int32 或 int64 类型，第三个参数必须为 BLOB 类型。";
  public static final String AGGREGATE_FUNCTIONS_S_SHOULD_ONLY_HAVE_ONE_ARGUMENT =

      "聚合函数 [%s] 只能有一个参数";
  public static final String AGGREGATE_FUNCTIONS_S_ONLY_SUPPORT_NUMERIC_DATA_TYPES_INT32_INT64_FLOAT_DOUBLE =

      "聚合函数 [%s] 仅支持数值数据类型 [INT32, INT64, FLOAT, DOUBLE]";
  public static final String ERROR_SIZE_OF_INPUT_EXPRESSIONS_EXPRESSION_S_ACTUAL_SIZE_S_EXPECTED_SIZE_2 =

      "输入表达式数量错误。表达式：%s，实际数量：%s，期望数量：[2]。";
  public static final String AGGREGATE_FUNCTIONS_S_ONLY_SUPPORT_NUMERIC_DATA_TYPES_INT32_INT64_FLOAT_DOUBLE_TIMESTAMP =

      "聚合函数 [%s] 仅支持数值数据类型 [INT32, INT64, FLOAT, DOUBLE, TIMESTAMP]";
  public static final String ERROR_SIZE_OF_INPUT_EXPRESSIONS_EXPRESSION_S_ACTUAL_SIZE_S_EXPECTED_SIZE_1 =

      "输入表达式数量错误。表达式：%s，实际数量：%s，期望数量：[1]。";
  public static final String AGGREGATE_FUNCTIONS_S_SHOULD_ONLY_HAVE_ONE_BOOLEAN_EXPRESSION_AS_ARGUMENT =

      "聚合函数 [%s] 只能有一个 boolean 表达式作为参数";
  public static final String AGGREGATE_FUNCTIONS_S_SHOULD_ONLY_HAVE_ONE_OR_TWO_ARGUMENTS =

      "聚合函数 [%s] 只能有一个或两个参数";
  public static final String SECOND_ARGUMENT_OF_AGGREGATE_FUNCTIONS_S_SHOULD_BE_ORDERABLE =

      "聚合函数 [%s] 的第二个参数应可排序";
  public static final String AGGREGATE_FUNCTIONS_S_SHOULD_ONLY_HAVE_TWO_OR_THREE_ARGUMENTS =

      "聚合函数 [%s] 只能有两个或三个参数";
  public static final String AGGREGATE_FUNCTIONS_S_SHOULD_ONLY_HAVE_TWO_ARGUMENTS =

      "聚合函数 [%s] 只能有两个参数";
  public static final String SECOND_ARGUMENT_OF_AGGREGATE_FUNCTIONS_S_SHOULD_BE_NUMBERIC_TYPE_AND_DO_NOT_USE =

      "聚合函数 [%s] 的第二个参数应为数值类型，且不能使用表达式";
  public static final String AGGREGATION_FUNCTIONS_S_SHOULD_ONLY_HAVE_THREE_ARGUMENTS =

      "聚合函数 [%s] 只能有三个参数";
  public static final String AGGREGATION_FUNCTIONS_S_SHOULD_ONLY_HAVE_TWO_OR_THREE_ARGUMENTS =

      "聚合函数 [%s] 只能有两个或三个参数";
  public static final String AGGREGATION_FUNCTIONS_S_SHOULD_HAVE_VALUE_COLUMN_AS_NUMERIC_TYPE_INT32_INT64_FLOAT =

      "聚合函数 [%s] 的值列应为数值类型 [INT32, INT64, FLOAT, DOUBLE, TIMESTAMP]";
  public static final String AGGREGATION_FUNCTIONS_S_SHOULD_HAVE_PERCENTAGE_AS_DECIMAL_TYPE =


      "聚合函数 [%s] 的百分比应为小数类型";
  public static final String AGGREGATION_FUNCTIONS_S_DO_NOT_SUPPORT_WEIGHT_AS_S_TYPE =


      "聚合函数 [%s] 不支持 %s 类型的权重";
  public static final String WINDOW_FUNCTION_S_SHOULD_ONLY_HAVE_ONE_ARGUMENT =

      "窗口函数 [%s] 只能有一个参数";
  public static final String WINDOW_FUNCTION_NTH_VALUE_SHOULD_ONLY_HAVE_TWO_ARGUMENT_AND_SECOND_ARGUMENT_MUST_BE =

      "窗口函数 [nth_value] 只能有两个参数，且第二个参数必须为 integer 类型";
  public static final String WINDOW_FUNCTION_S_SHOULD_ONLY_HAVE_ONE_TO_THREE_ARGUMENT =

      "窗口函数 [%s] 只能有一到三个参数";
  public static final String WINDOW_FUNCTION_S_S_SECOND_ARGUMENT_MUST_BE_INTEGER_TYPE =

      "窗口函数 [%s] 的第二个参数必须为 integer 类型";
  public static final String UPDATE_ATTRIBUTE_SHALL_SPECIFY_A_ATTRIBUTE_ONLY_ONCE =
      "UPDATE 属性只能指定一次。";
  public static final String CANNOT_BE_RESOLVED =
      "无法解析";
  public static final String IS_NOT_AN_ATTRIBUTE_OR_TAG_COLUMN =
      "不是属性列或标签列";
  public static final String UPDATE_S_ATTRIBUTE_VALUE_MUST_BE_STRING_TEXT_OR_NULL =
      "UPDATE 的属性值必须为 STRING、TEXT 或 null。";
  public static final String MULTIPLE_COLUMNS_FOUND_WITH_TIME_CATEGORY_IN_TABLE_SCHEMA =

      "表 schema 中发现多个 TIME 类别列。";
  public static final String INSERT_COLUMN_NAME_DOES_NOT_EXIST_IN_TARGET_TABLE_S =

      "插入列名在目标表中不存在：%s";
  public static final String INSERT_COLUMN_NAME_IS_SPECIFIED_MORE_THAN_ONCE_S =

      "插入列名被指定多次：%s";
  public static final String INSERT_QUERY_HAS_MISMATCHED_COLUMN_TYPES_TABLE_S_QUERY_S =

      "插入查询的列类型不匹配：表：[%s]，查询：[%s]";
  public static final String WITH_QUERY_NAME_S_SPECIFIED_MORE_THAN_ONCE =

      "WITH查询名称 '%s' 被指定多次。";
  public static final String WITH_TABLE_NAME_IS_REFERENCED_IN_THE_BASE_RELATION_OF_RECURSION =


      "递归的基础关系中引用了 WITH 表名。";
  public static final String MULTIPLE_RECURSIVE_REFERENCES_IN_THE_STEP_RELATION_OF_RECURSION =

      "递归的步骤关系中存在多个递归引用";
  public static final String FETCH_FIRST_LIMIT_CLAUSE_IN_THE_STEP_RELATION_OF_RECURSION =

      "递归的步骤关系中存在 FETCH FIRST / LIMIT 子句";
  public static final String RECURSIVE_REFERENCE_OUTSIDE_OF_FROM_CLAUSE_OF_THE_STEP_RELATION_OF_RECURSION =


      "递归的步骤关系中，FROM 子句外存在递归引用。";
  public static final String IMMEDIATE_WITH_CLAUSE_IN_RECURSIVE_QUERY_IS_NOT_SUPPORTED =

      "递归查询不支持直接使用 WITH子句。";
  public static final String IMMEDIATE_FILL_CLAUSE_IN_RECURSIVE_QUERY_IS_NOT_SUPPORTED =

      "递归查询不支持直接使用 FILL 子句";
  public static final String IMMEDIATE_ORDER_BY_CLAUSE_IN_RECURSIVE_QUERY_IS_NOT_SUPPORTED =

      "递归查询不支持直接使用 ORDER_BY 子句。";
  public static final String IMMEDIATE_OFFSET_CLAUSE_IN_RECURSIVE_QUERY_IS_NOT_SUPPORTED =

      "递归查询不支持直接使用 OFFSET 子句";
  public static final String IMMEDIATE_FETCH_FIRST_LIMIT_CLAUSE_IN_RECURSIVE_QUERY_IS_NOT_SUPPORT =

      "递归查询不支持直接使用 FETCH FIRST / LIMIT 子句";
  public static final String BASE_AND_STEP_RELATIONS_OF_RECURSION_HAVE_DIFFERENT_NUMBER_OF_FIELDS_S_S =

      "递归的基础关系和步骤关系字段数量不同：%s，%s";
  public static final String RECURSION_STEP_RELATION_OUTPUT_TYPE_S_IS_NOT_COERCIBLE_TO_RECURSION_BASE_RELATION_OUTPUT =

      "递归步骤关系的输出类型（%s）无法强制转换为递归基础关系在列 %s 上的输出类型（%s）";
  public static final String CANNOT_NEST_WINDOW_FUNCTIONS_OR_ROW_PATTERN_MEASURES_INSIDE_WINDOW_FUNCTION_ARGUMENTS =

      "不能在窗口函数参数中嵌套窗口函数或行模式度量";
  public static final String DISTINCT_IN_WINDOW_FUNCTION_PARAMETERS_NOT_YET_SUPPORTED_S =

      "暂不支持窗口函数参数中的 DISTINCT：%s";
  public static final String S_FUNCTION_REQUIRES_AN_ORDER_BY_WINDOW_CLAUSE =

      "%s 函数需要 ORDER_BY 窗口子句。";
  public static final String CANNOT_SPECIFY_WINDOW_FRAME_FOR_S_FUNCTION =

      "不能为 %s 函数指定窗口框架";
  public static final String WINDOW_NAME_S_SPECIFIED_MORE_THAN_ONCE =

      "WINDOW 名称 '%s' 被指定多次";
  public static final String CANNOT_RESOLVE_WINDOW_NAME_S =

      "无法解析 WINDOW 名称 %s";
  public static final String WINDOW_SPECIFICATION_WITH_NAMED_WINDOW_REFERENCE_CANNOT_SPECIFY_PARTITION_BY =

      "带命名 WINDOW 引用的 WINDOW 规范不能指定 PARTITION_BY。";
  public static final String CANNOT_SPECIFY_ORDER_BY_IF_REFERENCED_NAMED_WINDOW_SPECIFIES_ORDER_BY =

      "引用的命名 WINDOW 已指定 ORDER_BY 时，不能再指定 ORDER_BY。";
  public static final String CANNOT_REFERENCE_NAMED_WINDOW_CONTAINING_FRAME_SPECIFICATION =

      "不能引用包含框架规范的命名 WINDOW";
  public static final String WHERE_CLAUSE_MUST_EVALUATE_TO_A_BOOLEAN_ACTUAL_TYPE_S =

      "WHERE 子句必须计算为 boolean：实际类型 %s";
  public static final String MULTIPLE_DIFFERENT_COLUMNS_IN_THE_SAME_EXPRESSION_ARE_NOT_SUPPORTED =

      "不支持同一表达式中存在多个不同的 COLUMNS";
  public static final String NO_MATCHING_COLUMNS_FOUND_THAT_MATCH_REGEX_S =

      "没有匹配 regex '%s' 的列。";
  public static final String S_ARE_NOT_SUPPORTED_NOW =

      "目前不支持 %s";
  public static final String UNABLE_TO_RESOLVE_REFERENCE_S =

      "无法解析引用 %s。";
  public static final String IDENTIFIERCHAINBASIS_GET_GETBASISTYPE_EQ_EQ_FIELD_OR_TARGET_EXPRESSION_ISN_T_A =

      "identifierChainBasis.get().getBasisType == FIELD 或目标表达式不是 QualifiedName";
  public static final String SELECT_STAR_FROM_OUTER_SCOPE_TABLE_NOT_SUPPORTED_WITH_ANONYMOUS_COLUMNS =


      "存在匿名列时，不支持从外层作用域表执行 SELECT *";
  public static final String DISTINCT_CAN_ONLY_BE_APPLIED_TO_COMPARABLE_TYPES_ACTUAL_S =

      "DISTINCT 只能应用于可比较类型（实际：%s）";
  public static final String DISTINCT_CAN_ONLY_BE_APPLIED_TO_COMPARABLE_TYPES_ACTUAL_S_S =

      "DISTINCT 只能应用于可比较类型（实际：%s）：%s";
  public static final String GROUP_BY_POSITION_S_IS_NOT_IN_SELECT_LIST =

      "GROUP_BY 位置 %s 不在 select 列表中。";
  public static final String GROUP_BY_EXPRESSION_MUST_BE_A_COLUMN_REFERENCE_S =

      "GROUP_BY 表达式必须为列引用：%s";
  public static final String S_IS_NOT_COMPARABLE_AND_THEREFORE_CANNOT_BE_USED_IN_GROUP_BY =

      "%s 不可比较，因此不能用于 GROUP_BY。";
  public static final String GROUP_BY_HAS_MORE_THAN_S_GROUPING_SETS =

      "GROUP BY 包含超过 %s 个 grouping sets";
  public static final String HAVING_CLAUSE_MUST_EVALUATE_TO_A_BOOLEAN_ACTUAL_TYPE_S =

      "HAVING 子句必须计算为 boolean：实际类型 %s";
  public static final String S_QUERY_HAS_DIFFERENT_NUMBER_OF_FIELDS_D_D =

      "%s 查询的字段数量不同：%d，%d";
  public static final String COLUMN_D_IN_S_QUERY_HAS_INCOMPATIBLE_TYPES_S_S =


      "列 %d 在 %s 查询中类型不兼容：%s，%s";
  public static final String TYPE_S_IS_NOT_COMPARABLE_AND_THEREFORE_CANNOT_BE_USED_IN_SS =

      "类型 %s 不可比较，因此不能用于 %s%s。";
  public static final String DISTINCT =
      " DISTINCT";
  public static final String AMBIGUOUS_COLUMN_S_IN_ROW_PATTERN_INPUT_RELATION =

      "行模式输入关系中的列存在歧义：%s";
  public static final String S_IS_NOT_COMPARABLE_AND_THEREFORE_CANNOT_BE_USED_IN_PARTITION_BY =

      "%s 不可比较，因此不能用于 PARTITION_BY。";
  public static final String S_IS_NOT_ORDERABLE_AND_THEREFORE_CANNOT_BE_USED_IN_ORDER_BY =

      "%s 不可排序，因此不能用于 ORDER_BY。";
  public static final String EXPRESSION_DEFINING_A_LABEL_MUST_BE_BOOLEAN_ACTUAL_TYPE_S =

      "定义标签的表达式必须为 boolean（实际类型：%s）";
  public static final String VALUES_ROWS_HAVE_MISMATCHED_SIZES_S_VS_S =

      "VALUES 行大小不匹配：%s vs %s";
  public static final String TYPE_OF_ROW_D_COLUMN_D_IS_MISMATCHED_EXPECTED_S_ACTUAL_S =

      "第 %d 行第 %d 列的类型不匹配，期望：%s，实际：%s";
  public static final String TYPE_OF_ROW_D_IS_MISMATCHED_EXPECTED_S_ACTUAL_S =

      "第 %d 行的类型不匹配，期望：%s，实际：%s";
  public static final String COLUMN_ALIAS_LIST_HAS_S_ENTRIES_BUT_S_HAS_S_COLUMNS_AVAILABLE =

      "列别名列表有 %s 个条目，但 '%s' 有 %s 个可用列";
  public static final String JOIN_ON_CLAUSE_MUST_EVALUATE_TO_A_BOOLEAN_ACTUAL_TYPE_S =

      "JOIN_ON 子句必须计算为 boolean：实际类型 %s";
  public static final String ASOF_MAIN_JOIN_EXPRESSION_MUST_EVALUATE_TO_A_BOOLEAN_ACTUAL_TYPE_S =

      "ASOF 主 JOIN 表达式必须计算为 boolean：实际类型 %s";
  public static final String LEFT_CHILD_TYPE_OF_ASOF_MAIN_JOIN_EXPRESSION_MUST_BE_TIMESTAMP_ACTUAL_TYPE_S =

      "ASOF 主 JOIN 表达式的左子节点类型必须为 TIMESTAMP：实际类型 %s";
  public static final String RIGHT_CHILD_TYPE_OF_ASOF_MAIN_JOIN_EXPRESSION_MUST_BE_TIMESTAMP_ACTUAL_TYPE_S =

      "ASOF 主 JOIN 表达式的右子节点类型必须为 TIMESTAMP：实际类型 %s";
  public static final String COLUMN_S_APPEARS_MULTIPLE_TIMES_IN_USING_CLAUSE =

      "USING子句中列 '%s' 出现多次。";
  public static final String COLUMN_S_IS_MISSING_FROM_LEFT_SIDE_OF_JOIN =

      "JOIN 左侧缺少列 '%s'";
  public static final String COLUMN_S_IS_MISSING_FROM_RIGHT_SIDE_OF_JOIN =

      "JOIN 右侧缺少列 '%s'";
  public static final String COLUMN_TYPES_OF_LEFT_AND_RIGHT_SIDE_ARE_DIFFERENT_LEFT_IS_S_RIGHT_IS_S =

      "左右两侧的列类型不同：左侧为 %s，右侧为 %s";
  public static final String CANNOT_INFER_TIME_COLUMN_FOR_S_FILL_THERE_EXISTS_NO_COLUMN_WHOSE_TYPE_IS_TIMESTAMP =

      "无法为 %s FILL 推断 TIME_COLUMN，未找到 TIMESTAMP 类型的列。";
  public static final String S_FILL_TIME_COLUMN_POSITION_S_IS_NOT_IN_SELECT_LIST =

      "%s FILL TIME_COLUMN 位置 %s 不在 select 列表中";
  public static final String TYPE_OF_TIME_COLUMN_FOR_S_FILL_SHOULD_ONLY_BE_TIMESTAMP_BUT_TYPE_OF_THE_COLUMN_YOU =

      "用于 %s FILL 的 TIME_COLUMN 类型只能是 TIMESTAMP，但你指定的列类型为 %s";
  public static final String S_FILL_FILL_GROUP_POSITION_S_IS_NOT_IN_SELECT_LIST =

      "%s FILL FILL_GROUP 位置 %s 不在 select 列表中";
  public static final String TYPE_S_IS_NOT_ORDERABLE_AND_THEREFORE_CANNOT_BE_USED_IN_FILL_GROUP_S =

      "类型 %s 不可排序，因此不能用于 FILL_GROUP：%s";
  public static final String ORDER_BY_POSITION_S_IS_NOT_IN_SELECT_LIST =

      "ORDER_BY 位置 %s 不在 select 列表中。";
  public static final String TYPE_S_IS_NOT_ORDERABLE_AND_THEREFORE_CANNOT_BE_USED_IN_ORDER_BY_S =

      "类型 %s 不可排序，因此不能用于 ORDER_BY：%s";
  public static final String OFFSET_ROW_COUNT_MUST_BE_GREATER_OR_EQUAL_TO_0_ACTUAL_VALUE_S =

      "OFFSET 行数必须大于或等于 0（实际值：%s）";
  public static final String LIMIT_ROW_COUNT_MUST_BE_GREATER_OR_EQUAL_TO_0_ACTUAL_VALUE_S =

      "LIMIT 行数必须大于或等于 0（实际值：%s）";
  public static final String NON_CONSTANT_PARAMETER_VALUE_FOR_S_S =

      "%s 的参数值不是常量：%s";
  public static final String PARAMETER_VALUE_PROVIDED_FOR_S_IS_NULL_S =

      "为 %s 提供的参数值为 NULL：%s";
  public static final String RECURSIVE_REFERENCE_IN_LEFT_SOURCE_OF_S_JOIN =


      "%s join 的左侧源中存在递归引用";
  public static final String RECURSIVE_REFERENCE_IN_RIGHT_SOURCE_OF_S_JOIN =


      "%s join 的右侧源中存在递归引用";
  public static final String RECURSIVE_REFERENCE_IN_RIGHT_RELATION_OF_EXCEPT_S =

      "EXCEPT %s 的右侧关系中存在递归引用";
  public static final String DISTINCT_2 =
      "DISTINCT";
  public static final String ALL =
      "ALL";
  public static final String RECURSIVE_REFERENCE_IN_LEFT_RELATION_OF_EXCEPT_ALL =

      "EXCEPT ALL 的左侧关系中存在递归引用";
  public static final String FOR_SELECT_DISTINCT_ORDER_BY_EXPRESSIONS_MUST_APPEAR_IN_SELECT_LIST =

      "对于 SELECT DISTINCT，排序表达式必须出现在 select 列表中。";
  public static final String IS_CURRENTLY_NOT_ALLOWED_2 =

      " 当前不允许。";
  public static final String DUPLICATE_PROPERTY_S =

      "重复属性：%s";
  public static final String TTL_VALUE_MUST_BE_A_INF_OR_A_LONGLITERAL_BUT_NOW_IS =

      "TTL' 值必须为 'INF' 或 LongLiteral，当前为：";
  public static final String COLUMN_NAME_NOT_SPECIFIED_AT_POSITION_S =

      "位置 %s 未指定列名";
  public static final String COLUMN_NAME_S_SPECIFIED_MORE_THAN_ONCE =

      "列名 '%s' 被指定多次";
  public static final String COLUMN_ALIAS_LIST_HAS_S_ENTRIES_BUT_RELATION_HAS_S_COLUMNS =

      "列别名列表有 %s 个条目，但关系有 %s 个列";
  public static final String TABLE_FUNCTION_S_SPECIFIES_REQUIRED_COLUMNS_FROM_TABLE_ARGUMENT_S_WHICH_CANNOT_BE_FOUND =

      "表函数 %s 指定了来自表参数 %s 的必需列，但无法找到。";
  public static final String TABLE_FUNCTION_S_SPECIFIES_EMPTY_LIST_OF_REQUIRED_COLUMNS_FROM_TABLE_ARGUMENT_S =

      "表函数 %s 指定了来自表参数 %s 的必需列空列表。";
  public static final String TABLE_FUNCTION_S_SPECIFIES_NEGATIVE_INDEX_OF_REQUIRED_COLUMN_FROM_TABLE_ARGUMENT_S =

      "表函数 %s 指定了来自表参数 %s 的必需列负索引。";
  public static final String INDEX_S_OF_REQUIRED_COLUMN_FROM_TABLE_ARGUMENT_S_IS_OUT_OF_BOUNDS_FOR_TABLE_WITH_S =

      "必需列索引 %s 越界，来自表参数 %s，该表有 %s 列。";
  public static final String TABLE_FUNCTION_S_DOES_NOT_SPECIFY_REQUIRED_INPUT_COLUMNS_FROM_TABLE_ARGUMENT_S =

      "表函数 %s 未指定来自表参数 %s 的必需输入列。";
  public static final String TOO_MANY_ARGUMENTS_EXPECTED_AT_MOST_S_ARGUMENTS_GOT_S_ARGUMENTS =

      "参数过多。最多期望 %s 个参数，实际得到 %s 个参数";
  public static final String ALL_ARGUMENTS_MUST_BE_PASSED_BY_NAME_OR_ALL_MUST_BE_PASSED_POSITIONALLY =

      "所有参数必须全部按名称传递，或全部按位置传递";
  public static final String UNEXPECTED_ARGUMENT_NAME_S =

      "意外的参数名：%s";
  public static final String UNEXPECTED_TABLE_FUNCTION_ARGUMENT_TYPE_S =

      "意外的表函数参数类型：%s";
  public static final String INVALID_ARGUMENT_S_EXPECTED_TABLE_ARGUMENT_GOT_S =

      "无效参数 %s。期望表参数，实际为 %s";
  public static final String INVALID_ARGUMENT_S_EXPECTED_SCALAR_ARGUMENT_GOT_S =

      "无效参数 %s。期望标量参数，实际为 %s";
  public static final String INVALID_ARGUMENT_S_PARTITIONING_CAN_NOT_BE_SPECIFIED_FOR_TABLE_ARGUMENT_WITH_ROW =

      "无效参数 %s。不能为 row 语义的表参数指定 Partitioning";
  public static final String INVALID_ARGUMENT_S_ORDERING_CAN_NOT_BE_SPECIFIED_FOR_TABLE_ARGUMENT_WITH_ROW_SEMANTICS =

      "无效参数 %s。不能为 row 语义的表参数指定 Ordering";
  public static final String INVALID_SCALAR_ARGUMENT_S_EXPECTED_TYPE_S_GOT_S =

      "无效标量参数 '%s'。期望类型 %s，实际为 %s";
  public static final String INVALID_SCALAR_ARGUMENT_S_S =

      "无效标量参数 %s，%s";
  public static final String MISSING_REQUIRED_ARGUMENT_S =

      "缺少必需参数：%s";
  public static final String EXPECTED_COLUMN_REFERENCE_ACTUAL_S =

      "期望列引用。实际：%s";
  public static final String COLUMN_S_IS_NOT_PRESENT_IN_THE_INPUT_RELATION =

      "输入关系中不存在列 %s";
  public static final String S_CANNOT_CONTAIN_AGGREGATIONS_WINDOW_FUNCTIONS_OR_GROUPING_OPERATIONS_S =

      "%s 不能包含聚合、窗口函数或分组操作：%s";
  public static final String S_MUST_BE_AN_AGGREGATE_EXPRESSION_OR_APPEAR_IN_GROUP_BY_CLAUSE =

      "'%s' 必须是聚合表达式，或出现在 GROUP_BY 子句中。";
  public static final String SUBQUERY_USES_S_WHICH_MUST_APPEAR_IN_GROUP_BY_CLAUSE =

      "子查询使用了 '%s'，该列必须出现在 GROUP_BY 子句中。";
  public static final String CANNOT_NEST_AGGREGATIONS_INSIDE_AGGREGATION_S_S =

      "不能在聚合 '%s' 中嵌套聚合：%s";
  public static final String UNION_PATTERN_VARIABLE_NAME_S_IS_A_DUPLICATE_OF_PRIMARY_PATTERN_VARIABLE_NAME =

      "union 模式变量名 %s 与主模式变量名重复";
  public static final String UNION_PATTERN_VARIABLE_NAME_S_IS_DECLARED_TWICE =

      "union 模式变量名 %s 被声明了两次";
  public static final String SUBSET_ELEMENT_S_IS_NOT_A_PRIMARY_PATTERN_VARIABLE =

      "subset 元素 %s 不是主模式变量";
  public static final String DEFINED_VARIABLE_S_IS_NOT_A_PRIMARY_PATTERN_VARIABLE =

      "定义的变量 %s 不是主模式变量";
  public static final String PATTERN_VARIABLE_WITH_NAME_S_IS_DEFINED_TWICE =

      "名称为 %s 的模式变量被定义了两次";
  public static final String FINAL_SEMANTICS_IS_NOT_SUPPORTED_IN_DEFINE_CLAUSE =

      "DEFINE 子句中不支持 FINAL 语义";
  public static final String PATTERN_QUANTIFIER_LOWER_BOUND_MUST_BE_GREATER_THAN_OR_EQUAL_TO_0 =

      "模式量词下界必须大于或等于 0";
  public static final String PATTERN_QUANTIFIER_LOWER_BOUND_MUST_NOT_EXCEED =

      "模式量词下界不得超过 ";
  public static final String PATTERN_QUANTIFIER_UPPER_BOUND_MUST_BE_GREATER_THAN_OR_EQUAL_TO_1 =

      "模式量词上界必须大于或等于 1";
  public static final String PATTERN_QUANTIFIER_UPPER_BOUND_MUST_NOT_EXCEED =

      "模式量词上界不得超过 ";
  public static final String PATTERN_QUANTIFIER_LOWER_BOUND_MUST_NOT_EXCEED_UPPER_BOUND =

      "模式量词下界不得超过上界";
  public static final String S_IS_NOT_A_PRIMARY_OR_UNION_PATTERN_VARIABLE =

      "%s 不是主模式变量或 union 模式变量";
  public static final String NESTED_ROW_PATTERN_RECOGNITION_IN_ROW_PATTERN_RECOGNITION =

      "行模式识别中嵌套了行模式识别";
  public static final String PATTERN_EXCLUSION_SYNTAX_IS_NOT_ALLOWED_WHEN_ALL_ROWS_PER_MATCH_WITH_UNMATCHED_ROWS_IS =

      "指定 ALL_ROWS_PER_MATCH_WITH_UNMATCHED_ROWS 时，不允许使用模式排除语法。";
  public static final String COLUMN_S_CANNOT_BE_RESOLVED =

      "无法解析列 '%s'";
  public static final String REFERENCE_TO_COLUMN_S_FROM_OUTER_SCOPE_NOT_ALLOWED_IN_THIS_CONTEXT =

      "不允许在当前上下文中引用外层作用域的列 '%s'。";
  public static final String COLUMN_S_PREFIXED_WITH_LABEL_S_CANNOT_BE_RESOLVED =

      "无法解析带标签前缀 %s 的列 %s";
  public static final String EXPRESSION_S_IS_NOT_OF_TYPE_ROW =

      "表达式 %s 不是 ROW 类型";
  public static final String AMBIGUOUS_ROW_FIELD_REFERENCE_S =

      "行字段引用存在歧义：%s";
  public static final String TYPES_ARE_NOT_COMPARABLE_WITH_NULLIF_S_VS_S =

      "NULLIF 中的类型不可比较：%s vs %s";
  public static final String CASE_OPERAND_TYPE_DOES_NOT_MATCH_WHEN_CLAUSE_OPERAND_TYPE_S_VS_S =

      "CASE 操作数类型与 WHEN子句操作数类型不匹配：%s vs %s";
  public static final String UNARY_OPERATOR_CANNOT_BY_APPLIED_TO_S_TYPE =

      "一元 '+' 操作符不能应用于 %s 类型";
  public static final String LEFT_SIDE_OF_LIKE_EXPRESSION_MUST_EVALUATE_TO_TEXT_OR_STRING_TYPE_ACTUAL_S =

      "LIKE 表达式左侧必须计算为 TEXT 或 STRING 类型（实际：%s）";
  public static final String PATTERN_FOR_LIKE_EXPRESSION_MUST_EVALUATE_TO_TEXT_OR_STRING_TYPE_ACTUAL_S =


      "LIKE 表达式的模式必须计算为 TEXT 或 STRING 类型（实际：%s）";
  public static final String ESCAPE_FOR_LIKE_EXPRESSION_MUST_EVALUATE_TO_TEXT_OR_STRING_TYPE_ACTUAL_S =

      "LIKE 表达式的 escape 必须计算为 TEXT 或 STRING 类型（实际：%s）";
  public static final String LABEL_STAR_SYNTAX_IS_ONLY_SUPPORTED_AS_THE_ONLY_ARGUMENT_OF_ROW_PATTERN_COUNT_FUNCTION =

      "label.* 语法仅支持作为行模式 count 函数的唯一参数";
  public static final String CANNOT_USE_DISTINCT_WITH_AGGREGATE_FUNCTION_IN_PATTERN_RECOGNITION_CONTEXT =

      "不能在模式识别上下文中将 DISTINCT 与聚合函数一起使用";
  public static final String S_SEMANTICS_IS_NOT_SUPPORTED_OUT_OF_PATTERN_RECOGNITION_CONTEXT =

      "模式识别上下文外不支持 %s 语义";
  public static final String S_SEMANTICS_IS_SUPPORTED_ONLY_FOR_FIRST_LAST_AND_AGGREGATION_FUNCTIONS_ACTUAL_S =

      "%s 语义仅支持 FIRST()、LAST() 和聚合函数。实际值：%s";
  public static final String THE_SECOND_ARGUMENT_OF_S_FUNCTION_MUST_BE_ACTUAL_TIME_NAME =

      "%s 函数的第二个参数必须为实际时间名称";
  public static final String THE_THIRD_ARGUMENT_OF_S_FUNCTION_MUST_BE_ACTUAL_TIME_NAME =

      "%s 函数的第三个参数必须为实际时间名称";
  public static final String TOO_MANY_ARGUMENTS_FOR_FUNCTION_CALL_S =

      "函数调用 %s() 的参数过多";
  public static final String S_IS_NOT_A_PRIMARY_PATTERN_VARIABLE_OR_SUBSET_NAME =
      "%s 不是主模式变量或子集名称";
  public static final String MISSING_VALID_TIME_COLUMN_THE_TABLE_MUST_CONTAIN_EITHER_A_COLUMN_WITH_THE_TIME_CATEGORY =

      "缺少有效时间列。表必须包含一个 TIME 类别列，或至少一个 TIMESTAMP 列。";
  public static final String CLASSIFIER_PATTERN_RECOGNITION_FUNCTION_TAKES_NO_ARGUMENTS_OR_1_ARGUMENT =

      "CLASSIFIER 模式识别函数不接受参数或只接受 1 个参数";
  public static final String CLASSIFIER_FUNCTION_ARGUMENT_SHOULD_BE_PRIMARY_PATTERN_VARIABLE_OR_SUBSET_NAME_ACTUAL_S =

      "CLASSIFIER 函数参数应为主模式变量或子集名称。实际：%s";
  public static final String CANNOT_USE_DISTINCT_WITH_S_PATTERN_RECOGNITION_FUNCTION =

      "不能将 DISTINCT 与 %s 模式识别函数一起使用";
  public static final String S_SEMANTICS_IS_NOT_SUPPORTED_WITH_S_PATTERN_RECOGNITION_FUNCTION =

      "%s 语义不支持与 %s 模式识别函数一起使用";
  public static final String S_PATTERN_RECOGNITION_FUNCTION_REQUIRES_1_OR_2_ARGUMENTS =

      "%s 模式识别函数需要 1 或 2 个参数。";
  public static final String S_PATTERN_RECOGNITION_NAVIGATION_FUNCTION_REQUIRES_A_NUMBER_AS_THE_SECOND_ARGUMENT =

      "%s 模式识别导航函数需要第二个参数为数字。";
  public static final String S_PATTERN_RECOGNITION_NAVIGATION_FUNCTION_REQUIRES_A_NON_NEGATIVE_NUMBER_AS_THE_SECOND =

      "%s 模式识别导航函数需要第二个参数为非负数（实际：%s）。";
  public static final String THE_SECOND_ARGUMENT_OF_S_PATTERN_RECOGNITION_NAVIGATION_FUNCTION_MUST_NOT_EXCEED_S =

      "%s 模式识别导航函数的第二个参数不得超过 %s（实际：%s）";
  public static final String CANNOT_NEST_S_PATTERN_NAVIGATION_FUNCTION_INSIDE_S_PATTERN_NAVIGATION_FUNCTION =

      "不能在 %s 模式导航函数中嵌套 %s 模式导航函数";
  public static final String CANNOT_NEST_MULTIPLE_PATTERN_NAVIGATION_FUNCTIONS_INSIDE_S_PATTERN_NAVIGATION_FUNCTION =

      "不能在 %s 模式导航函数中嵌套多个模式导航函数";
  public static final String IMMEDIATE_NESTING_IS_REQUIRED_FOR_PATTERN_NAVIGATION_FUNCTIONS =

      "模式导航函数需要直接嵌套。";
  public static final String ALL_LABELS_AND_CLASSIFIERS_INSIDE_THE_CALL_TO_S_MUST_MATCH =

      "调用 '%s' 内的所有标签和 classifier 必须匹配";
  public static final String ALL_AGGREGATE_FUNCTION_ARGUMENTS_MUST_APPLY_TO_ROWS_MATCHED_WITH_THE_SAME_LABEL =

      "所有聚合函数参数都必须应用于匹配相同标签的行";
  public static final String CANNOT_NEST_S_AGGREGATE_FUNCTION_INSIDE_S_FUNCTION =

      "不能在 %s 函数中嵌套 %s 聚合函数";
  public static final String CANNOT_NEST_S_PATTERN_NAVIGATION_FUNCTION_INSIDE_S_FUNCTION =

      "不能在 %s 函数中嵌套 %s 模式导航函数";
  public static final String INVALID_PARAMETER_INDEX_S_MAX_VALUE_IS_S =

      "无效的参数索引 %s，最大值为 %s";
  public static final String CANNOT_CHECK_IF_S_IS_BETWEEN_S_AND_S =

      "无法判断 %s 是否位于 %s 和 %s 之间。";
  public static final String S_IS_NOT_COMPARABLE_AND_THEREFORE_CANNOT_BE_USED_IN_WINDOW_FUNCTION_PARTITION_BY =

      "%s 不可比较，因此不能用于窗口函数 PARTITION_BY。";
  public static final String S_IS_NOT_ORDERABLE_AND_THEREFORE_CANNOT_BE_USED_IN_WINDOW_FUNCTION_ORDER_BY =

      "%s 不可排序，因此不能用于窗口函数 ORDER_BY。";
  public static final String WINDOW_FRAME_STARTING_FROM_CURRENT_ROW_CANNOT_END_WITH_PRECEDING =

      "从 CURRENT_ROW 开始的窗口框架不能以 PRECEDING 结束。";
  public static final String WINDOW_FRAME_STARTING_FROM_FOLLOWING_CANNOT_END_WITH_PRECEDING =

      "从 FOLLOWING 开始的窗口框架不能以 PRECEDING 结束";
  public static final String WINDOW_FRAME_STARTING_FROM_FOLLOWING_CANNOT_END_WITH_CURRENT_ROW =

      "从 FOLLOWING 开始的窗口框架不能以 CURRENT_ROW 结束。";
  public static final String WINDOW_FRAME_ROWS_START_VALUE_TYPE_MUST_BE_EXACT_NUMERIC_TYPE_WITH_SCALE_0_ACTUAL_S =

      "窗口框架 ROWS 起始值类型必须是 scale 为 0 的精确数值类型（实际 %s）";
  public static final String WINDOW_FRAME_ROWS_END_VALUE_TYPE_MUST_BE_EXACT_NUMERIC_TYPE_WITH_SCALE_0_ACTUAL_S =

      "窗口框架 ROWS 结束值类型必须是 scale 为 0 的精确数值类型（实际 %s）";
  public static final String WINDOW_FRAME_OF_TYPE_GROUPS_PRECEDING_OR_FOLLOWING_REQUIRES_ORDER_BY =

      "GROUPS PRECEDING 或 FOLLOWING 类型的窗口框架需要 ORDER_BY。";
  public static final String WINDOW_FRAME_GROUPS_START_VALUE_TYPE_MUST_BE_EXACT_NUMERIC_TYPE_WITH_SCALE_0_ACTUAL_S =

      "窗口框架 GROUPS 起始值类型必须是 scale 为 0 的精确数值类型（实际 %s）";
  public static final String WINDOW_FRAME_OF_TYPE_RANGE_PRECEDING_OR_FOLLOWING_REQUIRES_ORDER_BY =

      "RANGE PRECEDING 或 FOLLOWING 类型的窗口框架需要 ORDER_BY。";
  public static final String WINDOW_FRAME_OF_TYPE_RANGE_PRECEDING_OR_FOLLOWING_REQUIRES_SINGLE_SORT_ITEM_IN_ORDER_BY =

      "RANGE PRECEDING 或 FOLLOWING 类型的窗口框架需要 ORDER_BY 中只有一个排序项（实际：%s）。";
  public static final String WINDOW_FRAME_OF_TYPE_RANGE_PRECEDING_OR_FOLLOWING_REQUIRES_THAT_SORT_ITEM_TYPE_BE =

      "RANGE PRECEDING 或 FOLLOWING 类型的窗口框架要求排序项类型为 numeric、datetime 或 interval（实际：%s）";
  public static final String WINDOW_FRAME_RANGE_VALUE_TYPE_S_NOT_COMPATIBLE_WITH_SORT_ITEM_TYPE_S =

      "窗口框架 RANGE 值类型（%s）与排序项类型（%s）不兼容";
  public static final String TYPE_S_MUST_BE_ORDERABLE_IN_ORDER_TO_BE_USED_IN_QUANTIFIED_COMPARISON =

      "类型 [%s] 必须可排序，才能用于量化比较。";
  public static final String TYPE_S_MUST_BE_COMPARABLE_IN_ORDER_TO_BE_USED_IN_QUANTIFIED_COMPARISON =

      "类型 [%s] 必须可比较，才能用于量化比较。";
  public static final String NOT_YET_IMPLEMENTED_S =

      "尚未实现：%s";
  public static final String S_MUST_EVALUATE_TO_A_S_ACTUAL_S =

      "%s 必须计算为 %s（实际：%s）";
  public static final String S_MUST_BE_THE_SAME_TYPE_OR_COERCIBLE_TO_A_COMMON_TYPE_CANNOT_FIND_COMMON_TYPE_BETWEEN_S =
      "%s 必须为相同类型，或可强制转换为公共类型。无法在 %s 和 %s 之间找到公共类型，所有类型（去重后）：%s";
  public static final String PATTERN_RECOGNITION_FUNCTION_NAME_MUST_NOT_BE_QUALIFIED =

      "模式识别函数名不得有限定符：";
  public static final String PATTERN_RECOGNITION_FUNCTION_NAME_MUST_NOT_BE_DELIMITED =

      "模式识别函数名不得被分隔：";
  public static final String PATTERN_RECOGNITION_FUNCTION_NAMES_CANNOT_BE_LAST_OR_FIRST_USE_RPR_LAST_OR_RPR_FIRST =

      "模式识别函数名不能为 LAST 或 FIRST，请改用 RPR_LAST 或 RPR_FIRST。";
  public static final String PARAMETER_NODE_MUST_HAVE_A_LOCATION =
      "参数节点必须包含位置信息";
  public static final String INVALID_NUMBER_OF_PARAMETERS_EXPECTED_D_GOT_D =

      "参数数量无效：期望 %d，实际得到 %d";
  public static final String CANNOT_INSERT_IDENTIFIER_S_PLEASE_USE_STRING_LITERAL =

      "不能插入标识符 %s，请使用字符串字面量";
  public static final String EXPRESSIONS_AND_COLUMNS_DO_NOT_MATCH_EXPRESSIONS_SIZE =

      "表达式和列不匹配，表达式数量：";
  public static final String COLUMNS_SIZE =

      "，列数量：";
  public static final String TIMECOLUMNINDEX_OUT_OF_BOUND_D_D =
      "timeColumnIndex 越界：%d-%d";
  public static final String INCONSISTENT_NUMBERS_OF_NON_TIME_COLUMN_NAMES_AND_VALUES_D_D =

      "非时间列名称和值的数量不一致：%d-%d";
  public static final String IS_NOT_SUPPORTED_FOR_PROPERTY_VALUE_OF_SET_CONFIGURATION =

      " 不支持作为 'set configuration' 的属性值。";
  public static final String NOTE_THAT_THE_SYNTAX_FOR_SET_CONFIGURATION_IN_THE_TREE_MODEL_IS_NOT_EXACTLY_THE_SAME_AS =

      "注意：树模型中的 'set configuration' 语法与表模型中的语法并不完全相同。";
  public static final String UNSUPPORTED_COPY_TO_FORMAT_S_SUPPORTED_FORMATS_S =

      "不支持 COPY_TO 格式 '%s'。支持的格式：%s";
  public static final String SIMULTANEOUS_SETTING_OF_MONTHLY_AND_NON_MONTHLY_INTERVALS_IS_NOT_SUPPORTED =
      "不支持同时设置月级和非月级时间间隔。";
  public static final String DON_T_NEED_TO_SPECIFY_TIME_COLUMN_WHILE_EITHER_TIME_BOUND_OR_FILL_GROUP_PARAMETER_IS_NOT =
      "未指定 TIME_BOUND 或 FILL_GROUP 参数时，无需指定 TIME_COLUMN";
  public static final String MONTH_OR_YEAR_INTERVAL_IN_TOLERANCE_IS_NOT_SUPPORTED_NOW =
      "目前不支持在 tolerance 中使用月或年时间间隔。";
  public static final String ASOF_JOIN_DOES_NOT_SUPPORT_S_TYPE_NOW =

      "ASOF JOIN 目前不支持 %s 类型";
  public static final String THE_SECOND_ARGUMENT_OF_APPROX_COUNT_DISTINCT_FUNCTION_MUST_BE_A_LITERAL =

      "'approx_count_distinct' 函数的第二个参数必须为字面量";
  public static final String THE_SECOND_AND_THIRD_ARGUMENT_OF_APPROX_MOST_FREQUENT_FUNCTION_MUST_BE_POSITIVE_INTEGER =

      "'approx_most_frequent' 函数的第二和第三个参数必须为正整数字面量";
  public static final String THE_SECOND_ARGUMENT_OF_APPROX_PERCENTILE_FUNCTION_PERCENTAGE_MUST_BE_A_DOUBLE_LITERAL =

      "'approx_percentile' 函数 percentage 的第二个参数必须为 double 字面量";
  public static final String THE_THIRD_ARGUMENT_OF_APPROX_PERCENTILE_FUNCTION_PERCENTAGE_MUST_BE_A_DOUBLE_LITERAL =

      "'approx_percentile' 函数 percentage 的第三个参数必须为 double 字面量";
  public static final String INCONSISTENT_COLUMN_CATEGORY_OF_COLUMN_S_S_S =

      "列 %s 的类别不一致：%s/%s";
  public static final String COLUMN =
      "列 ";
  public static final String DOES_NOT_EXISTS_OR_FAILS_TO_BE =
      " 不存在或无法被 ";
  public static final String CREATED =

      "已创建";
  public static final String INCOMPATIBLE_DATA_TYPE_OF_COLUMN_S_S_S =

      "列 %s 的数据类型不兼容：%s/%s";
  public static final String INLIST_LITERAL_FOR_TIMESTAMP_CAN_ONLY_BE_LONGLITERAL_DOUBLELITERAL_AND_GENERICLITERAL =
      "TIMESTAMP 的 InList literal 只能是 LongLiteral、DoubleLiteral 或 GenericLiteral，当前为 ";
  public static final String THE_TIME_FIELD_COLUMNS_ARE_CURRENTLY_NOT_ALLOWED_IN_DEVICES_RELATED_OPERATIONS =

      "设备相关操作中当前不允许使用 TIME/FIELD 列";
  public static final String UNKNOWN_COLUMN_CATEGORY_FOR_S_CANNOT_AUTO_CREATE_COLUMN =
      "列 %s 的类别未知，无法自动创建列。";
  public static final String UNKNOWN_COLUMN_DATA_TYPE_FOR_S_CANNOT_AUTO_CREATE_COLUMN =

      "%s 的列数据类型未知，无法自动创建列。";
  public static final String WRONG_CATEGORY_AT_COLUMN_S =
      "列 %s 的类别错误。";
  public static final String MISSING_COLUMNS_S =
      "缺少列 %s。";
  public static final String DATATYPE_OF_TAG_COLUMN_SHOULD_ONLY_BE_STRING_CURRENT_IS =
      "TAG 列的数据类型只能是 STRING，当前为 ";
  public static final String DATATYPE_OF_ATTRIBUTE_COLUMN_SHOULD_ONLY_BE_STRING_CURRENT_IS =
      "ATTRIBUTE 列的数据类型只能是 STRING，当前为 ";
  public static final String DECORRELATION_FOR_LIMIT_WITH_ROW_COUNT_GREATER_THAN_1_IS_NOT_SUPPORTED_YET =

      "暂不支持行数大于 1 的 LIMIT 去关联";
  public static final String GIVEN_CORRELATED_SUBQUERY_IS_NOT_SUPPORTED =

      "不支持给定的关联子查询";
  public static final String GIVEN_QUERIED_DATABASE_S_IS_NOT_EXIST =
      "查询的数据库 %s 不存在！";

  // --- QueryEngine log messages (additional) ---
  public static final String INITIALIZED_SHARED_MEMORYBLOCK_COORDINATOR_WITH_ALL_AVAILABLE_MEMORY_ARG_BYTES =
      "已使用全部可用内存初始化共享 MemoryBlock 'Coordinator'：{} 字节";
  public static final String ERROR_OCCURRED_DURING_EXECUTING_UDAF_PLEASE_CHECK_WHETHER_THE_IMPLEMENTATION_OF_UDF_IS =
      "执行 UDAF 时发生错误，请根据 udf-api 描述检查 UDF 实现是否正确。";
  public static final String PROCESSGETTSBLOCKREQUEST_SEQUENCE_ID_IN_ARG_ARG =

      "[ProcessGetTsBlockRequest] sequence ID 位于 [{}, {})";
  public static final String RECEIVED_ACKNOWLEDGEDATABLOCKEVENT_FOR_TSBLOCKS_WHOSE_SEQUENCE_ID_ARE_IN_ARG_ARG_FROM =

      "已从 {} 收到 TsBlock 的 AcknowledgeDataBlockEvent，其 sequence ID 位于 [{}, {})。";
  public static final String RECEIVED_ACK_EVENT_BUT_TARGET_FRAGMENTINSTANCE_ARG_IS_NOT_FOUND =

      "已收到 ACK event，但目标 FragmentInstance[{}] 未找到。";
  public static final String CLOSED_SOURCE_HANDLE_OF_SHUFFLESINKHANDLE_ARG_CHANNEL_INDEX_ARG =

      "已关闭 ShuffleSinkHandle {} 的 source handle，channel index：{}。";
  public static final String RECEIVED_CLOSESINKCHANNELEVENT_BUT_TARGET_FRAGMENTINSTANCE_ARG_IS_NOT_FOUND =

      "已收到 CloseSinkChannelEvent，但目标 FragmentInstance[{}] 未找到。";
  public static final String NEW_DATA_BLOCK_EVENT_RECEIVED_FOR_PLAN_NODE_ARG_OF_ARG_FROM_ARG =

      "已收到新的 data block event，对应 plan node {}、{}，来自 {}。";
  public static final String RECEIVED_NEWDATABLOCKEVENT_BUT_THE_DOWNSTREAM_FRAGMENTINSTANCE_ARG_IS_NOT_FOUND =

      "已收到 NewDataBlockEvent，但下游 FragmentInstance[{}] 未找到。";
  public static final String END_OF_DATA_BLOCK_EVENT_RECEIVED_FOR_PLAN_NODE_ARG_OF_ARG_FROM_ARG =

      "已收到数据块结束事件，对应计划节点 {}、{}，来自 {}。";
  public static final String RECEIVED_ONENDOFDATABLOCKEVENT_BUT_THE_DOWNSTREAM_FRAGMENTINSTANCE_ARG_IS_NOT_FOUND =

      "已收到 onEndOfDataBlockEvent，但下游 FragmentInstance[{}] 未找到。";
  public static final String CREATE_SOURCE_HANDLE_FROM_ARG_FOR_PLAN_NODE_ARG_OF_ARG =

      "从 {} 为 plan node {}（{}）创建 source handle";
  public static final String THE_TASK_ARG_IS_ABORTED_ALL_OTHER_TASKS_IN_THE_SAME_QUERY_WILL_BE_CANCELLED =
      "任务 {} 已中止，同一查询中的其他任务也将被取消";
  public static final String DRIVERTASKTIMEOUT_CURRENT_TIME_IS_ARG_DDL_OF_TASK_IS_ARG =
      "[DriverTaskTimeout] 当前时间为 {}，任务 ddl 为 {}";
  public static final String EXECUTOR_ARG_EXITS_BECAUSE_IT_S_INTERRUPTED_WE_WILL_PRODUCE_ANOTHER_THREAD_TO_REPLACE =

      "Executor {} 因中断退出。将创建另一个线程进行替换。";
  public static final String CANNOT_RESERVE_ARG_MAX_ARG_BYTES_MEMORY_FROM_MEMORYPOOL_FOR_PLANNODEIDARG =
      "无法从 MemoryPool 预留 {} 字节内存（最大：{}），planNodeId{}";
  public static final String BLOCKED_RESERVE_REQUEST_ARG_BYTES_MEMORY_FOR_PLANNODEIDARG =

      "预留请求被阻塞：为 planNodeId{} 预留 {} 字节内存";
  public static final String FAILED_TO_DO_THE_INITIALIZATION_FOR_DRIVER_ARG =

      "driver {} 初始化失败 ";
  public static final String FAILED_TO_ACQUIRE_THE_READ_LOCK_OF_DATAREGION_ARG_FOR_ARG_TIMES =

      "获取 DataRegion-{} 的读锁失败，尝试次数 {}。";
  public static final String INTERRUPTED_WHEN_AWAIT_ON_ALLDRIVERSCLOSED_FRAGMENTINSTANCE_ID_IS_ARG =
      "等待 allDriversClosed 时被中断，FragmentInstance Id 为 {}";
  public static final String RELEASE_TVLIST_OWNED_BY_QUERY_ALLOCATE_SIZE_ARG_RELEASE_SIZE_ARG =

      "释放查询拥有的 TVList：分配大小 {}，释放大小 {}。";
  public static final String TVLIST_ARG_IS_RELEASED_BY_THE_QUERY_FRAGMENTINSTANCE_ID_IS_ARG =

      "查询已释放 TVList {}，FragmentInstance Id 为 {}。";
  public static final String MEMORYNOTENOUGHEXCEPTION_WHEN_TRANSFERRING_TVLIST_OWNERSHIP_FROM_QUERY_ARG_TO_ANOTHER =
      "将 TVList ownership 从查询 {} 转移到另一个查询 {} 时发生 MemoryNotEnoughException。";
  public static final String UNEXPECTED_EXCEPTION_WHEN_TRANSFERRING_TVLIST_OWNERSHIP_FROM_QUERY_ARG_TO_ANOTHER_QUERY =

      "将 TVList ownership 从查询 {} 转移到另一个查询 {} 时发生意外异常。";
  public static final String TVLIST_ARG_IS_NOW_OWNED_BY_ANOTHER_QUERY_FRAGMENTINSTANCE_ID_IS_ARG =

      "TVList {} 现在由另一个查询持有，FragmentInstance Id 为 {}。";
  public static final String GETTSBLOCKFROMQUEUE_TSBLOCK_ARG_SIZE_ARG =

      "[GetTsBlockFromQueue] TsBlock:{} 大小:{}";
  public static final String RECEIVENEWTSBLOCKNOTIFICATION_ARG_ARG_EACH_SIZE_IS_ARG =

      "[ReceiveNewTsBlockNotification] [{}, {})，每个大小为：{}";
  public static final String STARTPULLTSBLOCKSFROMREMOTE_ARG_ARG_ARG_ARG =
      "[StartPullTsBlocksFromRemote] {}-{} [{}, {}) ";
  public static final String SENDCLOSESINKCHANNELEVENT_TO_SHUFFLESINKHANDLE_ARG_INDEX_ARG =
      "[SendCloseSinkChannelEvent] 到 [ShuffleSinkHandle: {}, index: {}]).";
  public static final String SINKCHANNEL_STILL_RECEIVE_GETTING_TSBLOCK_REQUEST_AFTER_BEING_ABORTED_ARG_OR_CLOSED_ARG =

      "SinkChannel 在 aborted={} 或 closed={} 后仍收到获取 TsBlock 请求。";
  public static final String NOTIFYNEWTSBLOCK_ARG_ARG_TO_ARG_ARG =
      "[NotifyNewTsBlock] [{}, {}) 到 {}.{}";
  public static final String PLAINSHUFFLESTRATEGY_NEEDS_TO_DO_NOTHING_CURRENT_CHANNEL_INDEX_IS_ARG =

      "PlainShuffleStrategy 无需操作，当前 channel index 为 {}。";
  public static final String LAYERROWWINDOWREADER_INDEX_OVERFLOW_BEGININDEX_ARG_ENDINDEX_ARG_WINDOWSIZE_ARG =

      "LayerRowWindowReader 索引溢出。beginIndex：{}，endIndex：{}，windowSize：{}。";
  public static final String CONSUMEMEMORY_CONSUME_ARG_CURRENT_REMAINING_MEMORY_ARG =

      "[ConsumeMemory] 消耗：{}，当前剩余内存：{}";
  public static final String RELEASEMEMORY_RELEASE_ARG_CURRENT_REMAINING_MEMORY_ARG =

      "[ReleaseMemory] 释放：{}，当前剩余内存：{}";
  public static final String MAXBYTESONEHANDLECANRESERVE_FOR_EXCHANGEOPERATOR_IS_ARG_EXCHANGESUMNUM_IS_ARG =

      "ExchangeOperator 的 MaxBytesOneHandleCanReserve 为 {}，exchangeSumNum 为 {}。";
  public static final String STATE_TRACKER_STARTS =
      "状态跟踪器启动";
  public static final String DISPATCH_WRITE_FAILED_STATUS_ARG_CODE_ARG_MESSAGE_ARG_NODE_ARG =

      "分发写入失败。状态：{}，code：{}，消息：{}，节点 {}。";
  public static final String CAN_T_EXECUTE_REQUEST_ON_NODE_ARG_IN_SECOND_TRY_ERROR_MSG_IS_ARG =

      "第二次尝试在节点 {} 执行请求失败，错误消息为 {}。";
  public static final String CAN_T_EXECUTE_REQUEST_ON_NODE_ARG_ERROR_MSG_IS_ARG_AND_WE_TRY_TO_RECONNECT_THIS_NODE =

      "在节点 {} 执行请求失败，错误消息为 {}，将尝试重连该节点。";
  public static final String WRITE_LOCALLY_FAILED_TSSTATUS_ARG_MESSAGE_ARG =

      "本地写入失败。TSStatus：{}，消息：{}。";
  public static final String DISPATCH_WRITE_FAILED_MESSAGE_ARG_NODE_ARG =

      "分发写入失败。消息：{}，节点 {}。";
  public static final String DISPATCH_WRITE_FAILED_STATUS_ARG_CODE_ARG_MESSAGE_ARG_NODE_ARG_2 =

      "分发写入失败。状态：{}，code：{}，消息：{}，节点 {}。";
  public static final String LOGICAL_PLAN_IS_ARG =

      "逻辑计划为：\n {}";
  public static final String DISTRIBUTION_PLAN_DONE_FRAGMENT_INSTANCE_COUNT_IS_ARG_DETAILS_IS_ARG =

      "分布式计划完成。FragmentInstance 数量为 {}，详情：\n {}";
  public static final String FAILED_TO_CHECK_TABLE_SCHEMA_WILL_SKIP_BECAUSE_SKIPFAILEDTABLESCHEMACHECK_IS_SET_TO_TRUE =
      "检查表元数据失败，但由于 skipFailedTableSchemaCheck 设置为 true，将跳过该检查，消息：{}";
  public static final String FAILED_TO_CHECK_IF_DEVICE_ARG_IS_DELETED_BY_MODS_WILL_SEE_IT_AS_NOT_DELETED =

      "检查设备 {} 是否被 mods 删除失败，将视为未删除。";
  public static final String COLUMN_ARG_IN_TABLE_ARG_IS_NOT_FOUND_IN_IOTDB_WHILE_LOADING_TSFILE =

      "加载 TsFile 时，表 {} 中的列 {} 在 IoTDB 中未找到。";
  public static final String DEVICE_ARG_IS_NOT_IN_THE_TSFILEDEVICE2ISALIGNED_CACHE_ARG =

      "设备 {} 不在 tsFileDevice2IsAligned 缓存 {} 中。";
  public static final String LOADTSFILEANALYZER_CURRENT_DATANODE_IS_READ_ONLY_WILL_TRY_TO_CONVERT_TO_TABLETS_AND =

      "LoadTsFileAnalyzer: 当前 DataNode 为只读，将尝试转换为 tablet 后再插入。";
  public static final String LOAD_ANALYSIS_STAGE_ARG_ARG_TSFILES_HAVE_BEEN_ANALYZED_PROGRESS_ARG_PERCENT =

      "加载 - 分析阶段：已分析 {}/{} 个 TsFile，进度：{}%";
  public static final String THE_FILE_ARG_IS_NOT_A_VALID_TSFILE_PLEASE_CHECK_THE_INPUT_FILE =

      "文件 {} 不是有效的 TsFile，请检查输入文件。";
  public static final String TSFILE_ARG_IS_A_ARG_MODEL_FILE =

      "TsFile {} 是 {}-model 文件。";
  public static final String LOAD_FAILED_TO_CONVERT_MINI_TSFILE_ARG_TO_TABLETS_FROM_STATEMENT_ARG_STATUS_ARG =

      "加载：无法从 statement {} 将 mini TsFile {} 转换为 tablet。状态：{}。";
  public static final String LOAD_FAILED_TO_CONVERT_TO_TABLETS_FROM_STATEMENT_ARG_BECAUSE_FAILED_TO_READ_MODEL_INFO =

      "加载：无法从 statement {} 转换为 tablet，原因是从文件读取 model info 失败，消息：{}。";
  public static final String LOAD_FAILED_TO_CONVERT_TO_TABLETS_FROM_STATEMENT_ARG_STATUS_IS_NULL =

      "加载：无法从 statement {} 转换为 tablet。状态为 null。";
  public static final String LOAD_FAILED_TO_CONVERT_TO_TABLETS_FROM_STATEMENT_ARG_STATUS_ARG =

      "加载：无法从 statement {} 转换为 tablet。状态：{}";
  public static final String LOAD_FAILED_TO_CONVERT_TO_TABLETS_FROM_STATEMENT_ARG_BECAUSE_EXCEPTION_ARG =

      "加载：无法从 statement {} 转换为 tablet，原因是异常：{}";
  public static final String FAILED_TO_CHECK_IF_DEVICE_ARG_TIMESERIES_ARG_IS_DELETED_BY_MODS_WILL_SEE_IT_AS_NOT =

      "检查设备 {}、timeseries {} 是否被 mods 删除失败，将视为未删除。";
  public static final String CREATE_DATABASE_ERROR_STATEMENT_ARG_RESULT_STATUS_IS_ARG =


      "创建数据库错误，statement：{}，结果状态为：{}。";
  public static final String MEASUREMENT_ARGARGARG_DATATYPE_NOT_MATCH_TSFILE_ARG_IOTDB_ARG =

      "测点 {}{}{} 的数据类型不匹配，TsFile：{}，IoTDB：{}";
  public static final String ENCODING_TYPE_NOT_MATCH_MEASUREMENT_ARGARGARG =

      "编码类型不匹配，测点：{}{}{}，";
  public static final String TSFILE_ENCODING_ARG_IOTDB_ENCODING_ARG =

      "TsFile 编码：{}，IoTDB 编码：{}";
  public static final String COMPRESSOR_NOT_MATCH_MEASUREMENT_ARGARGARG =

      "压缩器不匹配，测点：{}{}{}，";
  public static final String TSFILE_COMPRESSOR_ARG_IOTDB_COMPRESSOR_ARG =

      "TsFile 压缩器：{}，IoTDB 压缩器：{}";
  public static final String ARG_CACHE_FAILED_TO_CREATE_DATABASE_ARG =

      "[{} Cache] 创建数据库 {} 失败";
  public static final String ARG_CACHE_MISS_WHEN_SEARCH_DEVICE_ARG =

      "[{} Cache] 搜索设备 {} 时未命中";
  public static final String ARG_CACHE_HIT_WHEN_SEARCH_DEVICE_ARG =

      "[{} Cache] 搜索设备 {} 时命中";
  public static final String UNEXPECTED_ERROR_WHEN_GETREGIONREPLICASET_STATUS_ARG_REGIONMAP_ARG =

      "执行 getRegionReplicaSet 时发生意外错误：状态 {}，regionMap：{}";
  public static final String ARG_CACHE_MISS_WHEN_SEARCH_DATABASE_ARG =

      "[{} Cache] 搜索数据库 {} 时未命中";
  public static final String ARG_CACHE_MISS_WHEN_SEARCH_TIME_PARTITION_ARG =

      "[{} Cache] 搜索时间分区 {} 时未命中";
  public static final String FAILURES_HAPPENED_DURING_RUNNING_CONFIGEXECUTION_WHEN_EXECUTING_ARG_MESSAGE_ARG_STATUS =

      "执行 {} 的 ConfigExecution 期间发生失败，消息：{}，状态：{}";
  public static final String FAILURES_HAPPENED_DURING_RUNNING_CONFIGEXECUTION_WHEN_EXECUTING_ARG =

      "执行 {} 的 ConfigExecution 期间发生失败。";
  public static final String FAILED_TO_EXECUTE_CREATE_DATABASE_ARG_IN_CONFIG_NODE_STATUS_IS_ARG =

      "在 config node 中执行 create database {} 失败，状态为 {}。";
  public static final String FAILED_TO_EXECUTE_ALTER_DATABASE_ARG_IN_CONFIG_NODE_STATUS_IS_ARG =

      "在 config node 中执行 alter database {} 失败，状态为 {}。";
  public static final String FAILED_TO_EXECUTE_DELETE_DATABASE_ARG_IN_CONFIG_NODE_STATUS_IS_ARG =

      "在 config node 中执行 delete database {} 失败，状态为 {}。";
  public static final String FAILED_TO_CREATE_FUNCTION_WHEN_TRY_TO_CREATE_ARG_ARG_INSTANCE_FIRST =

      "尝试先创建 {}({}) 实例时，创建函数失败。";
  public static final String FAILED_TO_GET_EXECUTABLE_FOR_TRIGGER_ARG_USING_URI_ARG =
      "无法获取 Trigger({}) 的可执行文件，URI：{}。";
  public static final String FAILED_TO_CREATE_TRIGGER_WHEN_TRY_TO_CREATE_TRIGGER_ARG_INSTANCE_FIRST =

      "尝试先创建 trigger({}) 实例时，创建 trigger 失败。";
  public static final String ARG_FAILED_TO_CREATE_TRIGGER_ARG_TSSTATUS_IS_ARG =

      "[{}] 创建 trigger {} 失败。TSStatus 为 {}";
  public static final String FAILED_TO_GET_EXECUTABLE_FOR_PIPEPLUGIN_ARG_USING_URI_ARG =
      "无法获取 PipePlugin({}) 的可执行文件，URI：{}。";
  public static final String FAILED_TO_CREATE_PIPEPLUGIN_ARG_BECAUSE_THIS_PLUGIN_IS_NOT_DESIGNED_FOR_ARG_MODEL =


      "创建 PipePlugin({}) 失败：该插件不是为 {} model 设计的。";
  public static final String FAILED_TO_CREATE_FUNCTION_WHEN_TRY_TO_CREATE_PIPEPLUGIN_ARG_INSTANCE_FIRST =

      "尝试先创建 PipePlugin({}) 实例时，创建函数失败。";
  public static final String FAILED_TO_CREATE_PIPEPLUGIN_ARG_ARG_BECAUSE_ARG =

      "创建 PipePlugin {}({}) 失败，原因：{}";
  public static final String ARG_FAILED_TO_DROP_PIPE_PLUGIN_ARG =

      "[{}] 删除 pipe plugin {} 失败。";
  public static final String FAILED_TO_EXECUTE_ARG_ARG_IN_CONFIG_NODE_STATUS_IS_ARG =

      "在 config node 中执行 {} {} 失败，状态为 {}。";
  public static final String FAILED_TO_EXECUTE_ALTER_VIEW_ARG_BY_PIPE_STATUS_IS_ARG =

      "通过 pipe 执行 alter view {} 失败，状态为 {}。";
  public static final String THE_DATANODE_TO_BE_REMOVED_IS_NOT_IN_THE_CLUSTER_OR_THE_INPUT_FORMAT_IS_INCORRECT =

      "要移除的 DataNode 不在集群中，或输入格式不正确。";
  public static final String SUBMIT_REMOVE_DATANODE_REQUEST_SUCCESSFULLY_BUT_THE_PROCESS_MAY_FAIL =

      "remove-DataNode 请求已提交成功，但流程可能失败。 ";
  public static final String MORE_DETAILS_ARE_SHOWN_IN_THE_LOGS_OF_CONFIGNODE_LEADER_AND_REMOVED_DATANODE =

      "更多详情见 ConfigNode-leader 和 removed-DataNode 的日志， ";
  public static final String AND_AFTER_THE_PROCESS_OF_REMOVING_DATANODE_ENDS_SUCCESSFULLY =

      "remove DataNode 流程成功结束后， ";
  public static final String YOU_ARE_SUPPOSED_TO_DELETE_DIRECTORY_AND_DATA_OF_THE_REMOVED_DATANODE_MANUALLY =

      "需要手动删除已移除 DataNode 的目录和数据";
  public static final String THE_CONFIGNODE_TO_BE_REMOVED_IS_NOT_IN_THE_CLUSTER_OR_THE_INPUT_FORMAT_IS_INCORRECT =

      "要移除的 ConfigNode 不在集群中，或输入格式不正确。";
  public static final String FAILED_TO_DROP_DATABASE_ARG_BECAUSE_IT_DOESN_T_EXIST =

      "DROP DATABASE {} 失败：数据库不存在。";
  public static final String FAILED_TO_ALLOCATE_ARG_BYTES_FROM_SHARED_MEMORYBLOCK_ARG_FOR_PREPAREDSTATEMENT_ARG =

      "无法从共享 MemoryBlock '{}' 为 PreparedStatement '{}' 分配 {} 字节";
  public static final String ALLOCATED_ARG_BYTES_FOR_PREPAREDSTATEMENT_ARG_FROM_SHARED_MEMORYBLOCK_ARG =

      "已从共享 MemoryBlock '{}' 为 PreparedStatement '{}' 分配 {} 字节。 ";
  public static final String RELEASED_ARG_BYTES_FROM_SHARED_MEMORYBLOCK_ARG_FOR_PREPAREDSTATEMENT =

      "已从共享 MemoryBlock '{}' 为 PreparedStatement 释放 {} 字节。 ";
  public static final String ATTEMPTED_TO_RELEASE_MEMORY_FROM_SHARED_MEMORYBLOCK_ARG_BUT_IT_IS_RELEASED =

      "尝试从共享 MemoryBlock '{}' 释放内存，但它已释放";
  public static final String RELEASED_ARG_PREPAREDSTATEMENT_S_ARG_BYTES_TOTAL_FOR_SESSION_ARG =

      "已为 session {} 释放 {} 个 PreparedStatement（共 {} 字节）";
  public static final String THE_PREFIX_OF_SOURCEKEY_IS_NOT_SOURCE_PLEASE_CHECK_THE_PARAMETERS_PASSED_IN_ARG =

      "sourceKey 前缀不是 'source.'，请检查传入参数：{}";
  public static final String LOADTSFILESCHEDULER_REGION_MIGRATION_WAS_DETECTED_DURING_LOADING_TSFILE_ARG_WILL_CONVERT =


      "LoadTsFileScheduler：加载 TsFile {} 期间检测到 Region 迁移，将转换为 insertion 以避免数据丢失。";
  public static final String LOAD_TSFILE_ARG_SUCCESSFULLY_LOAD_PROCESS_ARG_ARG =
      "成功加载 TsFile {}，加载进度 [{}/{}]";
  public static final String CAN_NOT_LOAD_TSFILE_ARG_LOAD_PROCESS_ARG_ARG =

      "无法加载 TsFile {}，加载进度 [{}/{}]。";
  public static final String LOAD_TSFILE_S_FAILED_WILL_TRY_TO_CONVERT_TO_TABLETS_AND_INSERT_FAILED_TSFILES_ARG =

      "加载 TsFile 失败，将尝试转换为 tablet 并写入。失败的 TsFile：{}";
  public static final String DISPATCH_TSFILEDATA_ERROR_WHEN_PARSING_TSFILE_S =

      "解析 TsFile %s 时分发 TsFileData 出错。";
  public static final String PARSE_OR_SEND_TSFILE_S_ERROR =

      "解析或发送 TsFile %s 出错。";
  public static final String DISPATCH_ONE_PIECE_TO_REPLICASET_ARG_ERROR_RESULT_STATUS_CODE_ARG =

      "分发 TsFile 片段到 ReplicaSet {} 出错。结果状态码 {}。 ";
  public static final String RESULT_STATUS_MESSAGE_ARG_DISPATCH_PIECE_NODE_ERROR_PERCENT_NARG =

      "结果状态消息 {}。分发片段节点出错：%n{}";
  public static final String SUB_STATUS_CODE_ARG_SUB_STATUS_MESSAGE_ARG =

      "子状态码 {}。子状态消息 {}。";
  public static final String WAIT_FOR_LOADING_S_TIME_OUT =

      "等待加载 %s 超时。";
  public static final String DISPATCH_LOAD_COMMAND_ARG_OF_TSFILE_ARG_ERROR_TO_REPLICASETS_ARG_ERROR =

      "分发加载命令 {}（TsFile {}）到 replicaSets {} 时出错。 ";
  public static final String RESULT_STATUS_CODE_ARG_RESULT_STATUS_MESSAGE_ARG =

      "结果状态码 {}。结果状态消息 {}。";
  public static final String DISPATCH_TSFILE_S_ERROR_TO_LOCAL_ERROR_RESULT_STATUS_CODE_S =

      "向本地分发 tsFile %s 时出错。结果状态码 %s。 ";
  public static final String RESULT_STATUS_MESSAGE_S =

      "结果状态消息 %s。";
  public static final String LOAD_SUCCESSFULLY_CONVERTED_TSFILE_ARG_INTO_TABLETS_AND_INSERTED =

      "加载：已成功将 TsFile {} 转换为 tablet 并插入。";
  public static final String LOAD_FAILED_TO_CONVERT_TO_TABLETS_FROM_TSFILE_ARG_STATUS_ARG =

      "加载：无法从 TsFile {} 转换为 tablet。状态：{}";
  public static final String LOAD_FAILED_TO_CONVERT_TO_TABLETS_FROM_TSFILE_ARG_EXCEPTION_ARG =

      "加载：无法从 TsFile {} 转换为 tablet。异常：{}";
  public static final String DISPATCH_PIECE_NODE_ARG_OF_TSFILE_ARG_ERROR =

      "分发片段节点 {}（TsFile {}）出错。";
  public static final String CANNOT_DISPATCH_LOADCOMMAND_FOR_LOAD_OPERATION_ARG =

      "无法为加载操作 {} 分发 LoadCommand";
  public static final String LOAD_REMOTE_PROCEDURE_CALL_CONNECTION_TIMEOUT_IS_ADJUSTED_TO_ARG_MS_ARG_MINS =

      "加载远程过程调用连接超时已调整为 {} ms（{} mins）";
  public static final String DATA_TYPE_OF_ARG_ARG_IS_NOT_CONSISTENT =

      "{}.{} 的数据类型不一致， ";
  public static final String REGISTERED_TYPE_ARG_INSERTING_TIMESTAMP_ARG_VALUE_ARG =

      "已注册类型 {}，正在插入时间戳 {}，值 {}";
  public static final String TIMES_ARRAY_IS_NULL_OR_TOO_SMALL_TIMES_LENGTH_ARG_ROWSIZE_ARG_DEVICEID_ARG =

      "Times 数组为 null 或过小。times.length={}，rowSize={}，deviceId={}";
  public static final String SERIALIZE_DATA_OF_TSFILE_S_ERROR_SKIP_TSFILEDATA_S =

      "序列化 TsFile %s 的数据出错，跳过 TsFileData %s。";
  public static final String FAIL_TO_MATERIALIZE_CTE_BECAUSE_THE_DATA_SIZE_EXCEEDED_MEMORY_OR_THE_ROW_COUNT_THRESHOLD =

      "物化 CTE 失败：数据大小超过内存限制或行数阈值。";
  public static final String UNEXPECTED_FAILURE_WHEN_HANDLING_PARSING_ERROR_THIS_IS_LIKELY_A_BUG_IN_THE =

      "处理解析错误时出现意外失败，这很可能是实现缺陷。";
  public static final String AND_EXPRESSION_ENCOUNTERED_DURING_TAG_DETERMINED_CHECKING_WILL_BE_CLASSIFIED_INTO_FUZZY =

      "tag-determined 检查期间遇到 AND表达式，将归类为模糊表达式。Sql：{}";
  public static final String LOGICAL_EXPRESSION_TYPE_ENCOUNTERED_IN_NOT_EXPRESSION_CHILD_DURING_TAG_DETERMINED =

      "tag-determined 检查期间在 NOT表达式子节点中遇到逻辑表达式类型，将归类为模糊表达式。Sql：{}";
  public static final String VALIDATING_DEVICE_SCHEMA_ARG_ARG_AND_OTHER_ARG_DEVICES =

      "正在校验设备元数据 {}.{} 和其他 {} 个设备";
  public static final String ILLEGAL_TABLEID_ARG_FOUND_IN_CACHE_WHEN_INVALIDATING_BY_PATH_ARG_INVALIDATE_IT_ANYWAY =
      "发现非法 tableID {}，按路径 {} 失效 cache 时仍将使其失效";
  public static final String ILLEGAL_DEVICEID_ARG_FOUND_IN_CACHE_WHEN_INVALIDATING_BY_PATH_ARG_INVALIDATE_IT_ANYWAY =
      "发现非法 deviceID {}，按路径 {} 失效 cache 时仍将使其失效";
  public static final String RULE_S_BEFORE_S_AFTER_S =

      "规则：{}\n之前：\n{}\n之后：\n{}";

  private DataNodeQueryMessages() {}
  // ---------------------------------------------------------------------------
  // 补充异常消息
  // ---------------------------------------------------------------------------
  public static final String QUERY_EXCEPTION_FAILED_TO_SERIALIZE_INTERMEDIATE_RESULT_FOR_MAXBYACCUMULATOR_2F18B6E7 =

      "无法序列化 MaxByAccumulator 的中间结果。";
  public static final String QUERY_EXCEPTION_UNSUPPORTED_DATA_TYPE_IN_AGGREGATION_AVG_S_D1DAD6A6 =


      "聚合 AVG 不支持数据类型：%s";
  public static final String QUERY_EXCEPTION_UNSUPPORTED_DATA_TYPE_IN_MINVALUE_S_BC092694 =

      "MinValue 不支持数据类型：%s";
  public static final String QUERY_EXCEPTION_UNSUPPORTED_DATA_TYPE_IN_LASTVALUE_S_02ECF8E4 =

      "LastValue 不支持数据类型：%s";
  public static final String QUERY_EXCEPTION_UNSUPPORTED_DATA_TYPE_IN_EXTREME_S_84B651D3 =

      "Extreme 不支持数据类型：%s";
  public static final String QUERY_EXCEPTION_UNSUPPORTED_DATA_TYPE_IN_FIRSTVALUE_S_97025F25 =

      "FirstValue 不支持数据类型：%s";
  public static final String QUERY_EXCEPTION_ERROR_OCCURRED_DURING_EXECUTING_UDAF_S_S_PLEASE_CHECK_WHETHER_9E9D20C6 =

      "执行 UDAF#%s 时出错：%s，请根据 udf-api 描述检查 UDF 实现是否正确。";
  public static final String QUERY_EXCEPTION_UNSUPPORTED_EXPRESSION_TYPE_S_FD6F5B7C =

      "不支持的表达式类型：%s";
  public static final String QUERY_EXCEPTION_UNSUPPORTED_DATA_TYPE_IN_MAXVALUE_S_521AC345 =

      "MaxValue 不支持数据类型：%s";
  public static final String QUERY_EXCEPTION_THERE_IS_NOT_ENOUGH_CPU_TO_EXECUTE_CURRENT_FRAGMENT_INSTANCE_E7719FB8 =

      "没有足够 CPU 执行当前 fragment instance。";
  public static final String QUERY_EXCEPTION_THERE_IS_NO_ENOUGH_MEMORY_TO_EXECUTE_CURRENT_FRAGMENT_INSTANCE_CB632843 =

      "没有足够内存执行当前 fragment instance。";
  public static final String QUERY_EXCEPTION_PLANNODE_RELATED_MEMORY_IS_NOT_ZERO_WHEN_TRYING_TO_DEREGISTER_E01109C5 =


      "尝试从查询内存池注销 FI 时，PlanNode 相关内存不为 0。QueryId 为：%s，FragmentInstanceId 为：%s，非零 PlanNode 相关内存为：%s。";
  public static final String QUERY_EXCEPTION_QUERY_IS_ABORTED_SINCE_IT_REQUESTS_MORE_MEMORY_THAN_CAN_D77C2921 =


      "查询已中止，因为其请求的内存超过可分配上限，bytesToReserve：%sB，maxBytesCanReserve：%sB";
  public static final String QUERY_EXCEPTION_RELATEDMEMORYRESERVED_CAN_T_BE_NULL_WHEN_FREEING_MEMORY_C80009F2 =
      "释放内存时 RelatedMemoryReserved 不能为 null";
  public static final String QUERY_EXCEPTION_INTERRUPTED_BY_92FAED2D =
      "被中断，原因：";
  public static final String QUERY_EXCEPTION_DRIVER_WAS_INTERRUPTED_737358E4 =

      "Driver 已中断";
  public static final String QUERY_EXCEPTION_UNSUPPORTED_QUERY_DATA_SOURCE_TYPE_S_7424E63F =

      "不支持的查询数据源类型：%s";
  public static final String QUERY_EXCEPTION_REPEATED_RPC_CALL_DETECTED_FOR_FRAGMENTINSTANCE_S_REJECT_BF609A26 =

      "检测到 FragmentInstance %s 的重复 RPC 调用，拒绝重复分发。";
  public static final String QUERY_EXCEPTION_QUERY_HAS_EXECUTED_MORE_THAN_SMS_AND_NOW_IS_IN_FLUSHING_4BF7535B =

      "查询已执行超过 %sms，目前处于 flushing 状态";
  public static final String QUERY_EXCEPTION_THE_QUERYCONTEXT_DOES_NOT_SUPPORT_ROW_LEVEL_FILTERING_D4CD0678 =

      "QueryContext 不支持行级过滤";
  public static final String QUERY_EXCEPTION_S_IS_NOT_VIEW_B5840A3C =
      "%s 不是视图。";
  public static final String QUERY_EXCEPTION_THE_TIMESERIES_S_USED_NEW_TYPE_S_IS_NOT_COMPATIBLE_WITH_455D4D4A =

      "时间序列 %s 使用的新类型 %s 与现有类型 %s 不兼容。";
  public static final String QUERY_EXCEPTION_ALL_CACHED_PAGES_SHOULD_BE_CONSUMED_FIRST_UNSEQPAGEREADERS_55898EFB =

      "应先消费所有缓存页，unSeqPageReaders.isEmpty() 为 %s，firstPageReader != null 为 %s，mergeReader.hasNextTimeValuePair() = %s";
  public static final String QUERY_EXCEPTION_NOT_SUPPORT_THIS_TYPE_OF_AGGREGATION_WINDOW_S_604F93D0 =

      "不支持此类聚合窗口：%s";
  public static final String QUERY_EXCEPTION_UNSUPPORTED_DATA_TYPE_IN_EQUAL_EVENT_AGGREGATION_S_5076ACFE =


      "equal event aggregation 不支持数据类型：%s";
  public static final String QUERY_EXCEPTION_UNSUPPORTED_DATA_TYPE_IN_VARIATION_EVENT_AGGREGATION_S_47341532 =


      "variation event aggregation 不支持数据类型：%s";
  public static final String QUERY_EXCEPTION_THE_OPERATOR_CANNOT_CONTINUE_UNTIL_THE_LAST_WRITE_OPERATION_1F241343 =

      "上一次写操作完成前，operator 不能继续。";
  public static final String QUERY_EXCEPTION_DATA_TYPE_S_IS_NOT_SUPPORTED_5D5C02E4 =

      "不支持数据类型 %s。";
  public static final String QUERY_EXCEPTION_DATA_TYPE_S_IS_NOT_SUPPORTED_WHEN_CONVERT_DATA_AT_CLIENT_405429CC =

      "客户端转换数据时不支持数据类型 %s";
  public static final String QUERY_EXCEPTION_CHILD_SIZE_OF_INNERTIMEJOINOPERATOR_SHOULD_BE_LARGER_THAN_4E7CF105 =

      "InnerTimeJoinOperator 的子节点数量应大于 1。";
  public static final String QUERY_EXCEPTION_THE_OPERATOR_CANNOT_CONTINUE_UNTIL_THE_FORECAST_EXECUTION_AF8A3145 =

      "forecast 执行完成前，operator 不能继续。";
  public static final String QUERY_EXCEPTION_RESULT_TYPE_MISMATCH_FOR_ATTRIBUTE_S_EXPECTED_S_ACTUAL_S_E5637B91 =

      "属性 '%s' 的结果类型不匹配，期望 %s，实际 %s";
  public static final String QUERY_EXCEPTION_DEVICE_ENTRIES_OF_INDEX_S_IS_EMPTY_BCFB0644 =

      "索引 %s 的设备条目为空。";
  public static final String QUERY_EXCEPTION_SHOULD_NOT_CALL_GETRESULTDATATYPES_METHOD_IN_DEVICEITERATORSCANOPERATOR_E915A153 =

      "不应在 DeviceIteratorScanOperator 中调用 getResultDataTypes() 方法";
  public static final String QUERY_EXCEPTION_UNEXPECTED_COLUMN_CATEGORY_S_6E60A44E =

      "意外的列类别：%s";
  public static final String QUERY_EXCEPTION_DEVICE_ENTRIES_OF_INDEX_S_IN_TABLESCANOPERATOR_IS_EMPTY_FDEB574F =

      "TableScanOperator 中索引 %s 的设备条目为空。";
  public static final String QUERY_EXCEPTION_MULTILEVELPRIORITYQUEUE_DOES_NOT_SUPPORT_ACCESS_ELEMENT_02FE5AC9 =

      "MultilevelPriorityQueue 不支持随机访问元素。";
  public static final String QUERY_EXCEPTION_ASCENDING_IS_NOT_SUPPORTED_WHEN_SLIDING_STEP_CONTAINS_MONTH_3446C0DC =

      "sliding step 包含月份时不支持升序。";
  public static final String QUERY_EXCEPTION_THIS_OPERATION_IS_NOT_SUPPORTED_IN_SCHEMAMEASUREMENTNODE_93A81AE3 =

      "SchemaMeasurementNode 不支持此操作。";
  public static final String QUERY_EXCEPTION_REMOVE_CHILD_OPERATION_IS_NOT_SUPPORTED_IN_SCHEMAMEASUREMENTNODE_940D080F =
      "SchemaMeasurementNode 不支持 removeChild 操作。";
  public static final String QUERY_EXCEPTION_DO_NOT_SUPPORT_CREATE_COLUMNBUILDER_WITH_DATA_TYPE_S_1672578A =
      "不支持使用数据类型 %s 创建 ColumnBuilder";
  public static final String QUERY_EXCEPTION_INVALID_CONSTANT_OPERAND_S_939F3B8D =
      "无效的 constant operand：%s";
  public static final String QUERY_EXCEPTION_THE_DATA_TYPE_OF_THE_STATE_WINDOW_STRATEGY_IS_NOT_VALID_DFFBF210 =

      "state window strategy 的数据类型无效。";
  public static final String QUERY_EXCEPTION_STATEWINDOWACCESSSTRATEGY_DOES_NOT_SUPPORT_PURE_CONSTANT_B09D811B =

      "StateWindowAccessStrategy 不支持纯常量输入。";
  public static final String QUERY_EXCEPTION_UNEXPECTED_ACCESS_STRATEGY_S_92EA9D64 =
      "意外的访问策略：%s";
  public static final String QUERY_EXCEPTION_STATEWINDOWACCESSSTRATEGY_ONLY_SUPPORT_ONE_INPUT_SERIES_6856E52C =

      "StateWindowAccessStrategy 目前仅支持一个输入序列。";
  public static final String QUERY_EXCEPTION_UNSUPPORTED_SOURCE_DATATYPE_S_EA03E121 =

      "不支持的源 dataType：%s";
  public static final String QUERY_EXCEPTION_UNSUPPORTED_TARGET_DATATYPE_S_8DEFDAE6 =

      "不支持的目标 dataType：%s";
  public static final String QUERY_EXCEPTION_UNSUPPORTED_STATEMENT_TYPE_S_FBCA7305 =

      "不支持的语句类型：%s";
  public static final String QUERY_EXCEPTION_IDENTITYSINKNODE_SHOULD_ONLY_HAVE_ONE_CHILD_IN_TABLE_MODEL_5E995EB3 =

      "表模型中 IdentitySinkNode 只能有一个子节点。";
  public static final String QUERY_EXCEPTION_TREE_DB_NAME_SHOULD_AT_LEAST_BE_TWO_LEVEL_S_772B6832 =

      "树模型数据库名称至少应有两级：%s";
  public static final String QUERY_EXCEPTION_PUSHDOWNOFFSET_SHOULD_NOT_BE_SET_WHEN_ISPUSHLIMITTOEACHDEVICE_9B6D5144 =

      "isPushLimitToEachDevice 为 true 时不应设置 PushDownOffset。";
  public static final String QUERY_EXCEPTION_DEVICE_ENTRIES_OF_INDEX_S_IN_S_IS_EMPTY_68D1DB60 =

      "索引 %s 在 %s 中的设备条目为空。";
  public static final String QUERY_EXCEPTION_THE_AGGREGATIONTREEDEVICEVIEWSCANNODE_SHOULD_HAS_BEEN_TRANSFERRED_76A35037 =

      "AggregationTreeDeviceViewScanNode 应已转换为其子类节点";
  public static final String QUERY_EXCEPTION_GROUPING_KEY_MUST_BE_ID_OR_ATTRIBUTE_IN_AGGREGATIONTABLESCAN_7B592AE6 =

      "AggregationTableScan 中的 grouping key 必须为 ID 或 Attribute";
  public static final String QUERY_EXCEPTION_CANNOT_FIND_COLUMN_S_IN_CHILD_S_OUTPUT_10FBE4C8 =

      "无法在子节点输出中找到列 [%s]。";
  public static final String QUERY_EXCEPTION_DESCRIPTOR_S_INPUT_EXPRESSION_MUST_BE_TIMESERIESOPERAND_F4F66475 =

      "descriptor 的输入表达式必须是 TimeSeriesOperand/TimestampOperand，当前为 %s。";
  public static final String QUERY_EXCEPTION_AGGREGATIONMERGESORTNODE_WITHOUT_ORDER_BY_DEVICE_SHOULD_7AED85D1 =

      "不应在此处出现没有 ORDER BY DEVICE 的 AggregationMergeSortNode";
  public static final String QUERY_EXCEPTION_UNEXPECTED_PLANNODE_IN_GETOUTPUTCOLUMNTYPESOFTIMEJOINNODE_00FAAEED =

      "getOutputColumnTypesOfTimeJoinNode 中出现意外 PlanNode，类型：%s";
  public static final String QUERY_EXCEPTION_THE_SIZE_OF_MEASUREMENTLIST_AND_TIMESERIESSCHEMAINFOLIST_A6649661 =

      "对齐路径中 measurementList 和 timeseriesSchemaInfoList 的大小应相等。";
  public static final String QUERY_EXCEPTION_THE_PLANNODE_IS_NULL_DURING_LOCAL_EXECUTION_MAYBE_CAUSED_C5B942CA =
      "local execution 期间 planNode 为 null，可能是当前 DataNode 关闭导致";
  public static final String QUERY_EXCEPTION_THERE_IS_NOT_ENOUGH_MEMORY_TO_EXECUTE_CURRENT_FRAGMENT_INSTANCE_6071A581 =

      "没有足够内存执行当前 fragment instance，当前剩余空闲内存为 %dB，当前 fragment instance 预计内存使用量为 %dB。";
  public static final String QUERY_EXCEPTION_BYTES_TO_RESERVE_FROM_FREE_MEMORY_FOR_OPERATORS_SHOULD_BE_4DC404D5 =

      "为 operator 从空闲内存预留的字节数必须大于 0。";
  public static final String QUERY_EXCEPTION_THERE_IS_NOT_ENOUGH_MEMORY_FOR_QUERY_S_THE_CONTEXTHOLDER_546CDD02 =

      "Query %s 内存不足，contextHolder 为 %s，当前剩余空闲内存为 %dB，该 context 已预留总内存为 %dB，本次请求内存为 %dB。";
  public static final String QUERY_EXCEPTION_BYTES_TO_RELEASE_TO_FREE_MEMORY_FOR_OPERATORS_SHOULD_BE_3E5B0CB1 =

      "为 operator 释放到空闲内存的字节数必须大于 0。";
  public static final String QUERY_EXCEPTION_INVALID_AGGREGATION_EXPRESSION_S_B28EB91B =

      "无效的聚合表达式：%s";
  public static final String QUERY_EXCEPTION_ILLEGAL_DEVICE_PATH_S_IN_AGGREGATIONPUSHDOWN_RULE_60D5F633 =

      "AggregationPushDown rule 中存在非法设备路径：%s。";
  public static final String QUERY_EXCEPTION_AGGREGATION_DESCRIPTORS_WITH_NON_ALIGNED_TEMPLATE_ARE_NOT_6D3C7C0F =

      "不支持带有非对齐模板的聚合描述符";
  public static final String QUERY_EXCEPTION_FRAGMENTINSTANCE_S_IS_FAILED_S_MAY_BE_CAUSED_BY_DN_RESTARTING_45D7D52A =

      "FragmentInstance[%s] 失败。%s，可能由 DN 重启导致。";
  public static final String QUERY_EXCEPTION_FRAGMENTINSTANCE_S_IS_FAILED_S_566B0005 =

      "FragmentInstance[%s] 失败。%s";
  public static final String QUERY_EXCEPTION_LINE_S_S_S_7CA5F0E1 =
      "行 %s:%s %s";
  public static final String QUERY_EXCEPTION_MISSING_OR_INVALID_COLUMN_CATEGORIES_FOR_TABLE_INSERTION_5DF990B9 =

      "表插入缺少列类别或列类别无效";
  public static final String QUERY_EXCEPTION_THE_NAME_OF_A_MEASUREMENT_IN_SCHEMA_TEMPLATE_SHALL_NOT_BE_937264BD =

      "schema template 中的测点名称不得为 null。";
  public static final String QUERY_EXCEPTION_UNSUPPORTED_COLUMNCATEGORY_S_1260CFFD =
      "不支持的 ColumnCategory：%s";
  public static final String QUERY_EXCEPTION_UNSUPPORTED_RAWEXPRESSION_TYPE_S_CDBBD685 =

      "不支持的 rawExpression 类型：%s";
  public static final String QUERY_EXCEPTION_AN_ERROR_OCCURRED_WHEN_EXECUTING_GETSCHEMAPARTITION_S_A0156043 =

      "执行 getSchemaPartition() 时发生错误：%s";
  public static final String QUERY_EXCEPTION_AN_ERROR_OCCURRED_WHEN_EXECUTING_GETORCREATESCHEMAPARTITION_4D22BE9B =

      "执行 getOrCreateSchemaPartition() 时发生错误：%s";
  public static final String QUERY_EXCEPTION_AN_ERROR_OCCURRED_WHEN_EXECUTING_GETSCHEMANODEMANAGEMENTPARTITION_84AC8509 =

      "执行 getSchemaNodeManagementPartition() 时发生错误：%s";
  public static final String QUERY_EXCEPTION_AN_ERROR_OCCURRED_WHEN_EXECUTING_GETDATAPARTITION_S_D21A0011 =

      "执行 getDataPartition() 时发生错误：%s";
  public static final String QUERY_EXCEPTION_AN_ERROR_OCCURRED_WHEN_EXECUTING_GETORCREATEDATAPARTITION_2EB2EBBE =

      "执行 getOrCreateDataPartition() 时发生错误：%s";
  public static final String QUERY_EXCEPTION_THE_TYPE_OF_INPUT_EXPRESSION_S_IS_UNKNOWN_841AC714 =

      "输入表达式 %s 的类型未知";
  public static final String QUERY_EXCEPTION_MEET_ERROR_WHEN_ANALYZING_THE_QUERY_STATEMENT_S_AD732908 =

      "分析查询语句时遇到错误：%s";
  public static final String QUERY_EXCEPTION_UNSUPPORTED_EXPRESSION_TYPE_FOR_SOURCE_EXPRESSION_S_FB5583E7 =

      "源表达式不支持的表达式类型：%s";
  public static final String QUERY_EXCEPTION_UNSUPPORTED_EXPRESSION_TYPE_S_737846D6 =
      "不支持的 Expression Type：%s";
  public static final String QUERY_EXCEPTION_UNKNOWN_EXPRESSION_TYPE_S_PERHAPS_IT_HAS_NON_EXISTENT_MEASUREMENT_B6705F86 =

      "未知表达式类型：%s，可能存在不存在的测点。";
  public static final String QUERY_EXCEPTION_UNSUPPORTED_EXPRESSION_TYPE_IN_TRANSFORMTOVIEWEXPRESSIONVISITOR_0871FB56 =

      "TransformToViewExpressionVisitor 中不支持的表达式类型：%s";
  public static final String QUERY_EXCEPTION_CAN_NOT_CONSTRUCT_EXPRESSION_USING_NON_VIEW_PATH_IN_TRANSFORMVIEWPATH_A9CCB5B1 =


      "无法在 transformViewPath 中使用非视图路径构造表达式！";
  public static final String QUERY_EXCEPTION_UNSUPPORTED_EXPRESSION_TYPE_S_7C6F99A9 =

      "不支持的表达式类型 %s";
  public static final String QUERY_EXCEPTION_S_CANNOT_BE_CAST_TO_S_DABC2DA0 =

      "\"%s\" 无法转换为 [%s]";
  public static final String QUERY_EXCEPTION_UNSUPPORTED_DATA_TYPE_S_4CB21D47 = "不支持的数据类型 %s";
  public static final String QUERY_EXCEPTION_MEASUREMENT_S_DOES_NOT_EXIST_23D2B5BE =
      "Measurement %s 不存在";
  public static final String QUERY_EXCEPTION_INVALID_SCALAR_FUNCTION_S_4DC1ED95 =

      "无效的标量函数 [%s]。";
  public static final String QUERY_EXCEPTION_FETCH_SCHEMA_FAILED_BECAUSE_S_BE584DCE =

      "获取 Schema 失败，原因：%s";
  public static final String QUERY_EXCEPTION_FETCH_SCHEMA_FAILED_S_1C7B0050 =

      "获取 Schema 失败：%s";
  public static final String QUERY_EXCEPTION_FAILED_TO_VALIDATE_SCHEMA_FOR_TABLE_S_S_D7031B7B =
      "无法校验表 {%s, %s} 的 schema";
  public static final String QUERY_EXCEPTION_THE_DATABASE_S_DOES_NOT_EXIST_PLEASE_ENABLE_ENABLE_AUTO_B6683D0E =

      "数据库 %s 不存在，请启用 'enable_auto_create_schema' 以允许自动创建。";
  public static final String QUERY_EXCEPTION_AUTO_CREATE_DATABASE_FAILED_S_STATUS_CODE_S_D8EB60FA =

      "自动创建数据库失败：%s，状态码：%s";
  public static final String QUERY_EXCEPTION_TAG_COLUMN_S_IN_TSFILE_IS_NOT_FOUND_IN_IOTDB_TABLE_S_12E8C1EF =

      "TsFile 中的 tag 列 %s 在 IoTDB 表 %s 中未找到。";
  public static final String QUERY_EXCEPTION_DUPLICATED_MEASUREMENTS_S_IN_DEVICE_S_438713CD =

      "设备 %s 中存在重复测点 %s。";
  public static final String QUERY_EXCEPTION_DATABASE_LEVEL_D_IS_LONGER_THAN_DEVICE_S_9B34DD2F =


      "数据库级别 %d 长于设备 %s。";
  public static final String QUERY_EXCEPTION_CREATE_DATABASE_ERROR_STATEMENT_S_RESULT_STATUS_IS_S_5C4AFD58 =


      "创建数据库错误，statement：%s，结果状态为：%s";
  public static final String QUERY_EXCEPTION_DEVICE_S_DOES_NOT_EXIST_IN_IOTDB_AND_CAN_NOT_BE_CREATED_5171DE45 =


      "设备 %s 在 IoTDB 中不存在且无法创建。请检查是否已启用 auto-create-schema。";
  public static final String QUERY_EXCEPTION_DEVICE_S_IN_TSFILE_IS_S_BUT_IN_IOTDB_IS_S_350D5903 =

      "TsFile 中设备 %s 为 %s，但 IoTDB 中为 %s。";
  public static final String QUERY_EXCEPTION_MEASUREMENT_S_DOES_NOT_EXIST_IN_IOTDB_AND_CAN_NOT_BE_CREATED_B1F446A5 =


      "测点 %s 在 IoTDB 中不存在且无法创建。请检查是否已启用 auto-create-schema。";
  public static final String QUERY_EXCEPTION_AN_ERROR_OCCURRED_WHEN_EXECUTING_GETDEVICETODATABASE_S_CCA611CC =
      "执行 getDeviceToDatabase() 时发生错误：%s";
  public static final String QUERY_EXCEPTION_FAILED_TO_GET_REPLICASET_OF_CONSENSUS_GROUPS_IDS_S_CC30C7A6 =

      "无法获取 consensus group 的 replicaSet[ids= %s]。";
  public static final String QUERY_EXCEPTION_AN_ERROR_OCCURRED_WHEN_EXECUTING_GETREGIONREPLICASET_S_370D5526 =

      "执行 getRegionReplicaSet() 时发生错误：%s";
  public static final String QUERY_EXCEPTION_SUBSCRIPTION_IS_NOT_ENABLED_7F43DCBB =

      "Subscription 未启用。";
  public static final String QUERY_EXCEPTION_FAILED_TO_CREATE_UDF_S_THE_GIVEN_FUNCTION_NAME_CONFLICTS_6FBB1136 =
      "无法创建 UDF [%s]，给定函数名与内置函数名冲突。";
  public static final String QUERY_EXCEPTION_THE_SCHEME_OF_URI_IS_NOT_SET_PLEASE_SPECIFY_THE_SCHEME_OF_225DFB9E =

      "URI scheme 未设置，请指定 URI scheme。";
  public static final String QUERY_EXCEPTION_FAILED_TO_GET_EXECUTABLE_FOR_UDF_S_PLEASE_CHECK_THE_URI_F4D87A1E =

      "无法获取 UDF '%s' 的可执行文件，请检查 URI。";
  public static final String QUERY_EXCEPTION_FAILED_TO_CREATE_FUNCTION_S_BECAUSE_THERE_IS_DUPLICATE_ARGUMENT_7905BC09 =

      "无法创建函数 '%s'：存在重复的参数名 '%s'。";
  public static final String QUERY_EXCEPTION_FAILED_TO_CREATE_FUNCTION_S_BECAUSE_THERE_IS_AN_ARGUMENT_E7A0B1D6 =

      "无法创建函数 '%s'：存在 OBJECT 类型参数 '%s'。";
  public static final String QUERY_EXCEPTION_FAILED_TO_LOAD_CLASS_S_BECAUSE_IT_S_NOT_FOUND_IN_JAR_FILE_E467D08D =
      "无法加载 class '%s'，原因：jar 文件中未找到该 class 或其无效：%s";
  public static final String QUERY_EXCEPTION_BUILT_IN_FUNCTION_S_CAN_NOT_BE_DEREGISTERED_1CC7D3C3 =

      "内置函数 %s 不能注销。";
  public static final String QUERY_EXCEPTION_FAILED_TO_GET_EXECUTABLE_FOR_TRIGGER_S_PLEASE_CHECK_THE_DA49134A =

      "无法获取 Trigger '%s' 的可执行文件，请检查 URI。";
  public static final String QUERY_EXCEPTION_FAILED_TO_CREATE_PIPE_PLUGIN_BECAUSE_THE_URI_IS_EMPTY_7FCB6EF4 =

      "无法创建 pipe plugin：URI 为空。";
  public static final String QUERY_EXCEPTION_FAILED_TO_GET_EXECUTABLE_FOR_PIPEPLUGIN_S_PLEASE_CHECK_THE_FAC5DCB7 =

      "无法获取 PipePlugin %s 的可执行文件，请检查 URI。";
  public static final String QUERY_EXCEPTION_FAILED_TO_CREATE_PIPEPLUGIN_S_BECAUSE_THIS_PLUGIN_IS_NOT_F5A284B4 =
      "无法创建 PipePlugin '%s'，原因：该插件不是为 %s 模型设计的。";
  public static final String QUERY_EXCEPTION_NOT_ALL_SG_IS_READY_9F51CF3E =
      "并非所有 sg 都已就绪";
  public static final String QUERY_EXCEPTION_CANNOT_START_REPAIR_TASK_BECAUSE_COMPACTION_IS_NOT_ENABLED_975C8DCD =

      "无法启动 repair task：compaction 未启用。";
  public static final String QUERY_EXCEPTION_PLEASE_ENSURE_YOUR_INPUT_QUERYID_IS_CORRECT_D86C841E =

      "请确保输入的 <queryId> 正确";
  public static final String QUERY_EXCEPTION_DUPLICATED_MEASUREMENT_S_IN_DEVICE_TEMPLATE_ALTER_REQUEST_963FE4A6 =

      "device template alter 请求中 measurement [%s] 重复。";
  public static final String QUERY_EXCEPTION_FAILED_TO_CREATE_PIPE_S_BECAUSE_TSFILE_IS_CONFIGURED_WITH_2F8CD704 =
      "无法创建 Pipe %s，原因：TsFile 配置了加密，禁止使用 Pipe";
  public static final String QUERY_EXCEPTION_FAILED_TO_CREATE_PIPE_S_PIPE_NAME_STARTING_WITH_S_ARE_NOT_201FE8C3 =
      "无法创建 pipe %s，不允许创建以 \"%s\" 开头的 pipe。";
  public static final String QUERY_EXCEPTION_FAILED_TO_ALTER_PIPE_S_PIPE_NAME_STARTING_WITH_S_ARE_NOT_03D99ECF =
      "无法修改 pipe %s，不允许修改以 \"%s\" 开头的 pipe。";
  public static final String QUERY_EXCEPTION_FAILED_TO_GET_PIPE_INFO_FROM_CONFIG_NODE_STATUS_IS_S_FE797D7B =

      "无法从 config node 获取 pipe info，状态为 %s。";
  public static final String QUERY_EXCEPTION_FAILED_TO_ALTER_PIPE_S_PIPE_NOT_FOUND_IN_SYSTEM_63B5D3CC =

      "修改 pipe %s 失败：系统中未找到该 pipe。";
  public static final String QUERY_EXCEPTION_FAILED_TO_ALTER_PIPE_S_BECAUSE_S_A1823289 =
      "无法修改 pipe %s, 原因：%s";
  public static final String QUERY_EXCEPTION_FAILED_TO_START_PIPE_S_PIPE_NAME_STARTING_WITH_S_ARE_NOT_F16D488F =
      "无法启动 pipe %s，不允许启动名称以 \"%s\" 开头的 pipe。";
  public static final String QUERY_EXCEPTION_FAILED_TO_DROP_PIPE_S_PIPE_NAME_STARTING_WITH_S_ARE_NOT_840E238B =
      "无法删除 pipe %s，不允许删除以 \"%s\" 开头的 pipe。";
  public static final String QUERY_EXCEPTION_FAILED_TO_STOP_PIPE_S_PIPE_NAME_STARTING_WITH_S_ARE_NOT_C78DFC3D =
      "无法停止 pipe %s，不允许停止名称以 \"%s\" 开头的 pipe。";
  public static final String QUERY_EXCEPTION_UNKNOWN_DATABASE_S_5AB61128 = "未知的数据库 %s";
  public static final String QUERY_EXCEPTION_DATABASE_S_DOESN_T_EXIST_5A8EE8CA =

      "数据库 %s 不存在。";
  public static final String QUERY_EXCEPTION_DATABASE_S_ALREADY_EXISTS_D8BE5332 = "数据库 %s 已存在";
  public static final String QUERY_EXCEPTION_DATA_TYPE_CANNOT_BE_NULL_EXECUTING_THE_STATEMENT_THAT_ALTER_4C959B2F =

      "执行修改时间序列 %s 数据类型的语句时，数据类型不能为 null";
  public static final String QUERY_EXCEPTION_NO_CURRENT_SESSION_AVAILABLE_FOR_PREPARE_STATEMENT_36717E9B =

      "没有可用于 PREPARE 语句的当前 session。";
  public static final String QUERY_EXCEPTION_NO_CURRENT_SESSION_AVAILABLE_FOR_DEALLOCATE_STATEMENT_1EA5DE79 =

      "没有可用于 DEALLOCATE 语句的当前 session。";
  public static final String QUERY_EXCEPTION_TSFILE_S_IS_LOADING_BY_ANOTHER_SCHEDULER_55077B82 =

      "TsFile %s 正在由另一个 scheduler 加载。";
  public static final String QUERY_EXCEPTION_SERIALIZE_PROGRESS_INDEX_ERROR_ISFIRSTPHASESUCCESS_S_UUID_690F0419 =
      "序列化 Progress Index 出错，isFirstPhaseSuccess：%s，uuid：%s，TsFile：%s";
  public static final String QUERY_EXCEPTION_UNSUPPORTED_TSFILEDATATYPE_S_374475FA =
      "不支持的 TsFileDataType %s。";
  public static final String QUERY_EXCEPTION_UNKNOWN_SORT_KEY_S_37965711 = "未知 sort key %s";
  public static final String QUERY_EXCEPTION_CAN_NOT_FIND_S_ON_THIS_MACHINE_NOTICE_THAT_LOAD_CAN_ONLY_B7886C0E =
      "无法在本机找到 %s。注意，load 只能处理本机文件。";
  public static final String QUERY_EXCEPTION_LOAD_TSFILE_SOURCE_PATH_S_IS_OUTSIDE_ALLOWED_DIRECTORIES_85A6019F =

      "加载 TsFile 的源路径 %s 位于允许目录 %s 之外。";
  public static final String QUERY_EXCEPTION_FAILED_TO_RESOLVE_CANONICAL_PATH_FOR_LOAD_TSFILE_SOURCE_09CC9AC6 =
      "无法解析 load TsFile source %s 的 canonical path：%s";
  public static final String QUERY_EXCEPTION_DATA_TYPE_IS_NOT_CONSISTENT_INPUT_S_REGISTERED_S_AE9DBDC0 =

      "数据类型不一致，输入 %s，已注册 %s";
  public static final String QUERY_EXCEPTION_REGIONREPLICASET_IS_INVALID_S_1C2671AD =

      "regionReplicaSet 无效：%s";
  public static final String QUERY_EXCEPTION_PLANNODE_SHOULD_BE_IWRITEPLANNODE_IN_WRITE_OPERATION_S_36501D8A =

      "写操作中的 PlanNode 应为 IWritePlanNode：%s";
  public static final String QUERY_EXCEPTION_SIZE_OF_DEVICES_AND_ITS_CHILDREN_IN_DEVICEVIEWNODE_SHOULD_10709A84 =

      "DeviceViewNode 中 devices 及其 children 的大小应一致";
  public static final String QUERY_EXCEPTION_IN_NON_CROSS_DATA_REGION_DEVICE_VIEW_SITUATION_EACH_DEVICE_3A76445B =

      "在非跨 data region 的 device-view 场景中，每个设备应只有一个 data partition。";
  public static final String QUERY_EXCEPTION_IN_NON_CROSS_DATA_REGION_AGGREGATION_DEVICE_VIEW_SITUATION_557AE5D2 =

      "在非跨 data region 的 aggregation device-view 场景下，DeviceView 的每个重写子节点都只能有一个。";
  public static final String QUERY_EXCEPTION_ALL_CHILD_NODES_OF_INNERTIMEJOINNODE_SHOULD_BE_SERIESSOURCENODE_B92B181D =

      "InnerTimeJoinNode 的所有子节点都应为 SeriesSourceNode";
  public static final String QUERY_EXCEPTION_YOU_SHOULD_NEVER_SEE_CONTINUOUSSAMESEARCHINDEXSEPARATORNODE_F380A4B6 =

      "不应在该函数中看到 ContinuousSameSearchIndexSeparatorNode，因为 ContinuousSameSearchIndexSeparatorNode 不应用于网络传输。";
  public static final String QUERY_EXCEPTION_AGGREGATIONTREEDEVICEVIEWSCANNODE_SHOULD_NOT_BE_DESERIALIZED_11788F1B =

      "AggregationTreeDeviceViewScanNode 不应被反序列化";
  public static final String QUERY_EXCEPTION_CONTINUOUSSAMESEARCHINDEXSEPARATORNODE_NOT_SUPPORT_MERGE_FD194976 =

      "ContinuousSameSearchIndexSeparatorNode 不支持 merge";
  public static final String QUERY_EXCEPTION_DELETEDATANODES_WHICH_START_TIME_OR_END_TIME_ARE_NOT_SAME_F396951C =

      "开始时间或结束时间不同的 DeleteDataNodes 不能合并";
  public static final String QUERY_EXCEPTION_NO_CHILD_IS_ALLOWED_FOR_ALIGNEDSERIESAGGREGATIONSCANNODE_41654FE2 =

      "AlignedSeriesAggregationScanNode 不允许有子节点";
  public static final String QUERY_EXCEPTION_DEVICES_IN_TSFILE_S_IS_EMPTY_THIS_SHOULD_NOT_HAPPEN_HERE_BC1BE63C =

      "TsFile %s 中的设备为空，这里不应发生该情况。";
  public static final String QUERY_EXCEPTION_DEVICEMERGENODE_SHOULD_HAVE_ONLY_ONE_LOCAL_CHILD_IN_SINGLE_D1A2E6CF =

      "单 data region 中 DeviceMergeNode 只能有一个 local child。";
  public static final String QUERY_EXCEPTION_GETOUTPUTCOLUMNNAMES_OF_CREATEMULTITIMESERIESNODE_IS_NOT_9D02257A =

      "CreateMultiTimeSeriesNode 的 getOutputColumnNames 尚未实现";
  public static final String QUERY_EXCEPTION_GETOUTPUTCOLUMNNAMES_OF_ALTERLOGICALVIEWNODE_IS_NOT_IMPLEMENTED_D2294789 =

      "AlterLogicalViewNode 的 getOutputColumnNames 尚未实现";
  public static final String QUERY_EXCEPTION_THE_DATABASE_S_IS_READ_ONLY_CB6732CE =

      "数据库 '%s' 为只读。";
  public static final String QUERY_EXCEPTION_THE_DATABASE_S_CAN_ONLY_BE_QUERIED_BY_AUDIT_ADMIN_4A510F66 =

      "数据库 '%s' 仅允许 AUDIT admin 查询。";
  public static final String QUERY_EXCEPTION_UNEXPECTED_WINDOW_FRAME_TYPE_S_F06F81B8 =

      "意外的窗口框架类型：%s";
  public static final String QUERY_EXCEPTION_SIZE_OF_COLUMN_NAMES_AND_COLUMN_DATA_TYPES_DO_NOT_MATCH_9333D273 =

      "列名和列数据类型的数量不匹配";
  public static final String QUERY_EXCEPTION_FAILED_TO_FETCH_FRAGMENT_INSTANCE_STATISTICS_45176795 =
      "无法获取 fragment instance statistics";
  public static final String QUERY_EXCEPTION_UNABLE_TO_REMOVE_FIRST_NODE_WHEN_A_NODE_HAS_MULTIPLE_CHILDREN_FB6E81C5 =
      "节点有多个 child 时无法移除 first node，请改用 removeAll";
  public static final String QUERY_EXCEPTION_UNABLE_TO_REPLACE_FIRST_NODE_WHEN_A_NODE_HAS_MULTIPLE_CHILDREN_2C3D0E9E =
      "节点有多个 child 时无法替换 first node，请改用 replaceAll";
  public static final String QUERY_EXCEPTION_UNSUPPORTED_EXPRESSION_IN_EXTRACTGLOBALTIMEPREDICATE_S_083B1BFA =

      "extractGlobalTimePredicate 中不支持的表达式：%s";
  public static final String QUERY_EXCEPTION_NOT_A_VALID_IR_EXPRESSION_S_03C41ADD =

      "不是有效的 IR 表达式：%s";
  public static final String QUERY_EXCEPTION_UNKNOWN_ALIGNEDDEVICEENTRY_TYPE_S_370A0039 =
      "未知的 AlignedDeviceEntry Type：%s";
  public static final String QUERY_EXCEPTION_UNSUPPORTED_SELECTITEM_TYPE_S_4B0B155A =
      "不支持的 SelectItem type：%s";
  public static final String QUERY_EXCEPTION_THIS_VISITOR_ONLY_SUPPORTED_PROCESS_OF_EXPRESSION_7CEB79CB =

      "该 Visitor 仅支持处理 Expression";
  public static final String QUERY_EXCEPTION_UNSUPPORTED_GROUPING_ELEMENT_TYPE_S_B3C526E6 =
      "不支持的 grouping element type：%s";
  public static final String QUERY_EXCEPTION_UNSUPPORTED_JOIN_CRITERIA_S_311289C1 =
      "不支持的 join criteria：%s";
  public static final String QUERY_EXCEPTION_DUPLICATE_ARGUMENT_SPECIFICATION_FOR_NAME_S_F804F3DC =

      "名称存在重复的参数规范：%s";
  public static final String QUERY_EXCEPTION_FORECASTTABLEFUNCTION_MUST_CONTAIN_FORECASTTABLEFUNCTION_DA5828E9 =
      "ForecastTableFunction 必须包含 ForecastTableFunction.TIMECOL_PARAMETER_NAME";
  public static final String QUERY_EXCEPTION_UNEXPECTED_ARGUMENT_SPECIFICATION_S_830154B1 =

      "意外的参数规范：%s";
  public static final String QUERY_EXCEPTION_UNEXPECTED_PARTITIONBY_EXPRESSION_S_8D74EB2D =
      "意外的 partitionBy 表达式：%s";
  public static final String QUERY_EXCEPTION_UNEXPECTED_ORDERBY_EXPRESSION_S_9301B69E =
      "意外的 orderBy 表达式：%s";
  public static final String QUERY_EXCEPTION_AGGREGATION_ANALYSIS_NOT_YET_IMPLEMENTED_FOR_S_38B64170 =

      "尚未实现 %s 的聚合分析";
  public static final String QUERY_EXCEPTION_UNEXPECTED_VALUE_LIST_TYPE_FOR_INPREDICATE_S_3D50B78B =
      "InPredicate 的值列表类型意外：%s";
  public static final String QUERY_EXCEPTION_UNEXPECTED_COMPARISON_TYPE_S_5D101FCB =
      "意外的比较类型：%s";
  public static final String QUERY_EXCEPTION_ZERO_LENGTH_DELIMITED_IDENTIFIER_NOT_ALLOWED_00C9ADEC =
      "不允许使用零长度分隔标识符";
  public static final String QUERY_EXCEPTION_BACKQUOTED_IDENTIFIERS_ARE_NOT_SUPPORTED_USE_DOUBLE_QUOTES_78BC7EE3 =
      "不支持反引号标识符；请使用双引号引用标识符";
  public static final String QUERY_EXCEPTION_UNSUPPORTED_UNBOUNDED_TYPE_S_2D943211 =
      "不支持的 unbounded type：%s";
  public static final String QUERY_EXCEPTION_ONLY_SUPPORT_MEASUREMENT_COLUMN_IN_FILTER_S_140800D9 =
      "过滤条件中仅支持测点列：%s";
  public static final String QUERY_EXCEPTION_UNSUPPORTED_COMPARISON_OPERATOR_S_8357E642 =
      "不支持的 comparison operator %s";
  public static final String QUERY_EXCEPTION_UNSUPPORTED_EXTRACT_COMPARISON_OPERATOR_S_38A9CDFA =
      "不支持的 extract comparison operator %s";
  public static final String QUERY_EXCEPTION_UNSUPPORTED_LOGICAL_OPERATOR_S_FDC60986 =
      "不支持的 logical operator %s";
  public static final String QUERY_EXCEPTION_S_IS_NOT_SUPPORTED_IN_VALUE_PUSH_DOWN_DD54E38A =
      "值下推中不支持 %s";
  public static final String QUERY_EXCEPTION_SHOULD_NOT_REACH_HERE_BEFORE_PREDICATECOMBINEINTOTABLESCANCHECKER_C591ED7D =
      "PredicateCombineIntoTableScanChecker 支持第三个子节点的 Extract push-down 前，不应执行到这里";
  public static final String QUERY_EXCEPTION_COLUMNSCHEMA_OF_SYMBOL_S_ISN_T_SAVED_IN_SCHEMAMAP_3A172EBC =
      "Symbol %s 的 ColumnSchema 未保存在 schemaMap 中";
  public static final String QUERY_EXCEPTION_SHOULD_NEVER_RETURN_NULL_IN_PREDICATECOMBINEINTOTABLESCANCHECKER_2A687052 =
      "PredicateCombineIntoTableScanChecker 不应返回 null。";
  public static final String QUERY_EXCEPTION_EITHER_LEFT_OR_RIGHT_OPERAND_OF_COMPARISONEXPRESSION_SHOULD_FC89CE57 =
      "ComparisonExpression 的左操作数或右操作数应包含时间列。";
  public static final String QUERY_EXCEPTION_SHOULD_NOT_REACH_HERE_BEFORE_GLOBALTIMEPREDICATEEXTRACTVISITOR_3ECC819B =
      "GlobalTimePredicateExtractVisitor 支持第二个子节点的 Extract push-down 前，不应执行到这里";
  public static final String QUERY_EXCEPTION_SHOULD_NOT_REACH_HERE_BEFORE_GLOBALTIMEPREDICATEEXTRACTVISITOR_FB8489E8 =
      "GlobalTimePredicateExtractVisitor 支持第三个子节点的 Extract push-down 前，不应执行到这里";
  public static final String QUERY_EXCEPTION_THREE_OPERAND_OF_BETWEEN_EXPRESSION_SHOULD_HAVE_TIME_COLUMN_25ED881D =
      "范围表达式的三个操作数都应包含时间列。";
  public static final String QUERY_EXCEPTION_FETCH_TABLE_DEVICE_SCHEMA_FAILED_BECAUSE_S_20B7D6C2 =

      "获取 Table Device Schema 失败，原因：%s";
  public static final String QUERY_EXCEPTION_THE_SCHEMA_FILTER_TYPE_S_IS_NOT_SUPPORTED_200D1E0B =
      "不支持 schema filter 类型 %s";
  public static final String QUERY_EXCEPTION_AUTO_CREATE_TABLE_SUCCEED_BUT_CANNOT_GET_TABLE_SCHEMA_IN_74985A8E =
      "auto create table 成功，但无法从当前节点的 DataNodeTableCache 获取 table schema，可能是并发 "
          + "auto creating table 导致";
  public static final String QUERY_EXCEPTION_CAN_NOT_CREATE_TABLE_BECAUSE_INCOMING_TABLE_HAS_NO_LESS_D3D33555 =

      "无法创建表：传入表的 tag 列不少于现有表，且现有 tag 列不是传入 tag 列的前缀。现有 tag 列：%s，现有表索引：%s，传入表索引：%s";
  public static final String QUERY_EXCEPTION_CAN_NOT_CREATE_TABLE_BECAUSE_EXISTING_TABLE_HAS_MORE_TAG_8364B675 =

      "无法创建表：现有表的 tag 列多于传入表，且传入 tag 列不是现有 tag 列的前缀。传入 tag 列：%s，现有表索引：%s，传入表索引：%s";
  public static final String QUERY_EXCEPTION_AUTO_ADD_TABLE_COLUMN_FAILED_S_S_02F3DD19 =

      "自动添加表列失败：%s.%s";
  public static final String QUERY_EXCEPTION_CANNOT_CREATE_COLUMN_S_CATEGORY_IS_NOT_PROVIDED_E5410BD3 =

      "无法创建列 %s：未提供 category。";
  public static final String QUERY_EXCEPTION_CANNOT_CREATE_COLUMN_S_DATATYPE_IS_NOT_PROVIDED_2A7D27FA =

      "无法创建列 %s：未提供 datatype。";
  public static final String QUERY_EXCEPTION_UNKNOWN_COLUMNCATEGORY_FOR_ADDING_COLUMN_S_ED1BF7FA =
      "添加 column 时遇到未知 ColumnCategory：%s";
  public static final String QUERY_EXCEPTION_VISIT_NOT_IMPLEMENTED_FOR_S_1A798A4D =

      "visit() 尚未为 %s 实现";
  public static final String QUERY_EXCEPTION_UNSUPPORTED_DATA_TYPE_FOR_SCALAR_SUBQUERY_RESULT_S_4381E489 =

      "不支持标量子查询结果的数据类型：%s";
  public static final String QUERY_EXCEPTION_TOPKNODE_MUST_BE_APPEARED_AFTER_PUSHLIMITOFFSETINTOTABLESCAN_844A065D =

      "TopKNode 必须出现在 PushLimitOffsetIntoTableScan 之后";
  public static final String QUERY_EXCEPTION_TABLE_MODEL_CAN_ONLY_PROCESS_DATA_ONLY_IN_ONE_DATABASE_YET_AB8C1EF5 =

      "表模型目前只能处理单个数据库中的数据！";
  public static final String QUERY_EXCEPTION_UNSUPPORTED_JOIN_TYPE_IN_PREDICATE_PUSH_DOWN_S_4493D86C =

      "predicate push down 中不支持的 join 类型：%s";
  public static final String QUERY_EXCEPTION_UNSUPPORTED_PLAN_NODE_S_72DD2270 = "不支持的 plan node %s";
  public static final String QUERY_EXCEPTION_AN_ERROR_OCCURRED_WHEN_EXECUTING_GETREADABLEDATAREGIONS_A884A6BB =
      "执行 getReadableDataRegions() 时发生错误：%s";
  public static final String QUERY_EXCEPTION_AN_ERROR_OCCURRED_WHEN_EXECUTING_GETREADABLEDATANODELOCATIONS_54CBD60D =
      "执行 getReadableDataNodeLocations() 时发生错误：%s";
  public static final String QUERY_EXCEPTION_UNSUPPORTED_QUANTIFIED_COMPARISON_S_C3700430 =
      "不支持的 quantified comparison：%s";
  public static final String QUERY_EXCEPTION_UNEXPECTED_QUANTIFIEDCOMPARISON_S_F13E4EB2 =
      "意外的 quantifiedComparison：%s";
  public static final String QUERY_EXCEPTION_UNEXPECTED_QUANTIFIER_S_62214B74 =
      "意外的 Quantifier：%s";
  public static final String QUERY_EXCEPTION_THIS_OPTIMIZER_SHOULD_BE_USED_BEFORE_OPTIMIZER_OF_PUSHAGGREGATIONINTOTABLESCAN_9F6016E3 =
      "该优化器应在 PushAggregationIntoTableScan 优化器之前使用";
  public static final String QUERY_EXCEPTION_UNSUPPORTED_VALUEPOINTER_TYPE_S_4147FFFB =
      "不支持的 ValuePointer type：%s";
  public static final String QUERY_EXCEPTION_SYMBOL_S_IS_NOT_EXIST_IN_FETYPEPROVIDER_WITH_S_5CBBFB8B =

      "Symbol：%s 不存在于 feTypeProvider，附带信息：%s";
  public static final String QUERY_EXCEPTION_RIGHT_JOIN_SHOULD_BE_TRANSFORMED_TO_LEFT_JOIN_IN_PREVIOUS_D6B56B1F =

      "RIGHT Join 应在前一处理阶段转换为 LEFT Join";
  public static final String QUERY_EXCEPTION_NO_AVAILABLE_DATANODES_MAY_BE_THE_CLUSTER_IS_CLOSING_E13B8C50 =

      "没有可用 DataNode，集群可能正在关闭";
  public static final String QUERY_EXCEPTION_SHOULD_NEVER_REACH_HERE_CHILD_ORDERING_S_PREGROUPEDSYMBOLS_79A94AB5 =

      "不应执行到这里。Child ordering：%s。PreGroupedSymbols：%s";
  public static final String QUERY_EXCEPTION_UNSUPPORTED_TO_SERIALIZE_S_484DAAAF =
      "不支持 serialize：%s";
  public static final String QUERY_EXCEPTION_UNSUPPORTED_TO_DESERIALIZE_S_4641CD63 =
      "不支持 deserialize：%s";
  public static final String QUERY_EXCEPTION_INSERT_INTO_TABLE_COLUMNS_S_SIZE_SHOULD_BE_SAME_AS_QUERY_E7437397 =

      "INSERT INTO 表列的数量应与查询结果一致";
  public static final String QUERY_EXCEPTION_THE_TABLEDEVICEFETCHNODE_S_CLONE_METHOD_SHALL_NOT_BE_CALLED_977C41FD =

      "不应调用 TableDeviceFetchNode 的 clone() 方法。";
  public static final String QUERY_EXCEPTION_UNKNOWN_TABLESCANNODE_TYPE_S_6246EF1E =

      "未知的 TableScanNode 类型：%s";
  public static final String QUERY_EXCEPTION_UNEXPECTED_SETOPERATION_NODE_TYPE_S_3AE3EECA =

      "意外的 setOperation 节点类型：%s";
  public static final String QUERY_EXCEPTION_UNSUPPORTED_PATTERN_QUANTIFIER_TYPE_S_4ED427E9 =

      "不支持的模式量词类型：%s";
  public static final String FAILED_TO_CREATE_PIPE_PLUGIN_PREFIX_FMT =

      "创建 pipe plugin %s 失败。 ";
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
  public static final String MESSAGE_FAILED_FETCH_STATE_HAS_RETRIED_ARG_TIMES_E7572C66 =
      "获取 state 失败，已重试 %s 次";
  public static final String MESSAGE_IGNORED_CONFIG_ITEMS_FE28ADBC =
      "已忽略配置项：";
  public static final String MESSAGE_BECAUSE_THEY_IMMUTABLE_UNDEFINED_07C04F65 =

      "，因为它们不可变或未定义。";
  public static final String MESSAGE_LOAD_ARG_PIECE_ERROR_1ST_PHASE_BECAUSE_F3D9672C =

      "第一阶段加载 %s 片段出错。原因：";
  public static final String MESSAGE_LOAD_ARG_ERROR_SECOND_PHASE_BECAUSE_ARG_FIRST_PHASE_ARG_CBA980FC =

      "第二阶段加载 %s 出错。原因：%s，第一阶段为 %s";
  public static final String MESSAGE_SUCCESS_260CA9DD =
      "成功";
  public static final String MESSAGE_FAILED_26934EB3 =
      "失败";
  public static final String MESSAGE_AUDIT_PERMISSION_NEEDED_ALTER_ENCODING_COMPRESSOR_DATABASE_ARG_CC06994D =

      "修改数据库 %s 的 encoding 和 compressor 需要 AUDIT 权限";
  public static final String EXCEPTION_QUERYID_IS_NULL_056E92E4 = "queryId 不能为空";
  public static final String EXCEPTION_EXPECTED_TWO_IDS_BUT_GOT_COLON_ARG_020F9D13 =
      "期望两个 id，实际得到：%s";
  public static final String EXCEPTION_ID_IS_NULL_9D5D27B1 = "id 不能为空";
  public static final String EXCEPTION_NAME_IS_NULL_C8B35959 = "name 不能为空";
  public static final String EXCEPTION_EXPECTEDPARTS_MUST_BE_AT_LEAST_1_B867DB08 = "expectedParts 必须至少为 1";
  public static final String EXCEPTION_INVALID_ARG_ARG_2946DBE5 = "无效的 %s %s";
  public static final String EXCEPTION_ID_IS_EMPTY_28C94FC0 =
      "id 为空";
  public static final String EXCEPTION_EXECUTOR_IS_NULL_7FBE03A4 = "executor 不能为空";
  public static final String EXCEPTION_INITIALSTATE_IS_NULL_8992A39F = "initialState 不能为空";
  public static final String EXCEPTION_TERMINALSTATES_IS_NULL_E0FC2A93 = "terminalStates 不能为空";
  public static final String EXCEPTION_EXPECTEDSTATE_IS_NULL_5E8C2F32 = "expectedState 不能为空";
  public static final String EXCEPTION_CURRENTSTATE_IS_NULL_AEDB20DB = "currentState 不能为空";
  public static final String EXCEPTION_STATECHANGELISTENER_IS_NULL_635AE7D2 = "stateChangeListener 不能为空";
  public static final String EXCEPTION_ARG_CANNOT_TRANSITION_FROM_ARG_TO_ARG_8C680D30 = "%s 无法从 %s 转换到 %s";
  public static final String EXCEPTION_CANNOT_FIRE_STATE_CHANGE_EVENT_WHILE_HOLDING_THE_LOCK_35243BC4 =
      "持有锁时无法触发状态变更事件。";
  public static final String EXCEPTION_CANNOT_NOTIFY_WHILE_HOLDING_THE_LOCK_15625D48 =
      "持有锁时无法通知。";
  public static final String EXCEPTION_CANNOT_WAIT_FOR_STATE_CHANGE_WHILE_HOLDING_THE_LOCK_CBD9F784 =
      "持有锁时无法等待状态变更。";
  public static final String EXCEPTION_DONESTATE_IS_NULL_D88F77E5 = "doneState 不能为空";
  public static final String EXCEPTION_DONESTATE_ARG_IS_NOT_A_DONE_STATE_8724C618 =
      "doneState %s 不是完成状态";
  public static final String EXCEPTION_DATANODEID_SHOULD_BE_INIT_FIRST_13B19A85 =
      "DataNodeId 应先初始化！";
  public static final String EXCEPTION_LENGTH_OF_INPUT_COLUMN_LEFT_BRACKET_RIGHT_BRACKET_FOR_MAXBY_SLASH_MINBY_SHOULD_B_1F3F2F1C =
      "MaxBy/MinBy 的输入 Column[] 长度应为 3";
  public static final String EXCEPTION_PARTIALRESULT_OF_MAXBY_SLASH_MINBY_SHOULD_BE_1_BF0078F4 = "MaxBy/MinBy 的 partialResult 应为 1";
  public static final String EXCEPTION_PARTIALRESULT_OF_MAXVALUE_SHOULD_BE_1_659B6D42 =
      "MaxValue 的 partialResult 应为 1";
  public static final String EXCEPTION_PARTIALRESULT_OF_AVG_SHOULD_BE_2_7A8C375E =
      "Avg 的 partialResult 应为 2";
  public static final String EXCEPTION_PARTIALRESULT_OF_MINVALUE_SHOULD_BE_1_C9DAF94D =
      "MinValue 的 partialResult 应为 1";
  public static final String EXCEPTION_PARTIALRESULT_OF_LASTVALUE_SHOULD_BE_2_68963ECE =
      "LastValue 的 partialResult 应为 2";
  public static final String EXCEPTION_PARTIALRESULT_OF_MINTIME_SHOULD_BE_1_B7EFE10B =
      "MinTime 的 partialResult 应为 1";
  public static final String EXCEPTION_PARTIALRESULT_OF_COUNT_SHOULD_BE_1_972B9219 =
      "Count 的 partialResult 应为 1";
  public static final String EXCEPTION_PARTIALRESULT_OF_FIRSTVALUE_SHOULD_BE_2_3FB20C54 =
      "FirstValue 的 partialResult 应为 2";
  public static final String EXCEPTION_PARTIALRESULT_OF_COUNT_IF_SHOULD_BE_1_70D01652 =
      "count_if 的 partialResult 应为 1";
  public static final String EXCEPTION_STEP_IN_SERIESAGGREGATESCANOPERATOR_AND_RAWDATAAGGREGATEOPERATOR_CAN_ONLY_PROCES_5575BD95 =
      "SeriesAggregateScanOperator 和 RawDataAggregateOperator 中的 Step 只能处理原始输入。";
  public static final String EXCEPTION_RAWDATAAGGREGATEOPERATOR_CAN_ONLY_PROCESS_ONE_TSBLOCK_INPUT_DOT_5ABCB8C0 =
      "RawDataAggregateOperator 只能处理一个 TsBlock 输入。";
  public static final String EXCEPTION_STEP_IN_AGGREGATEOPERATOR_CANNOT_PROCESS_RAW_INPUT_22620F61 =
      "AggregateOperator 中的 Step 不能处理原始输入。";
  public static final String EXCEPTION_FINAL_OUTPUT_CAN_ONLY_BE_SINGLE_COLUMN_6D82F9E0 = "最终输出只能是单列";
  public static final String EXCEPTION_PARTIALRESULT_OF_SUM_SHOULD_BE_1_40E85216 =
      "Sum 的 partialResult 应为 1";
  public static final String EXCEPTION_INPUT_OF_SUM_SHOULD_BE_1_D5C11EC8 =
      "Sum 的输入应为 1";
  public static final String EXCEPTION_PARTIALRESULT_OF_UDAF_SHOULD_BE_1_E094029D =
      "UDAF 的 partialResult 应为 1";
  public static final String EXCEPTION_PARTIALRESULT_OF_EXTREMEVALUE_SHOULD_BE_1_A7713D8A =
      "ExtremeValue 的 partialResult 应为 1";
  public static final String EXCEPTION_WRONG_INPUTDATATYPES_SIZE_DOT_675FF289 = "inputDataTypes 大小错误。";
  public static final String EXCEPTION_INPUT_OF_COUNT_SHOULD_BE_1_C7EEEC46 =
      "Count 的输入应为 1";
  public static final String EXCEPTION_PARTIALRESULT_OF_MAXTIME_SHOULD_BE_1_877F7EAC =
      "MaxTime 的 partialResult 应为 1";
  public static final String EXCEPTION_LOCALMEMORYMANAGER_IS_NULL_DOT_69FE497A = "localMemoryManager 不能为空。";
  public static final String EXCEPTION_TSBLOCKSERDEFACTORY_IS_NULL_DOT_32EB5BD2 = "tsBlockSerdeFactory 不能为空。";
  public static final String EXCEPTION_EXECUTORSERVICE_IS_NULL_DOT_7B057909 = "executorService 不能为空。";
  public static final String EXCEPTION_MPPDATAEXCHANGESERVICECLIENTMANAGER_IS_NULL_DOT_F31E746C = "mppDataExchangeServiceClientManager 不能为空。";
  public static final String EXCEPTION_SHAREDTSBLOCKQUEUE_IS_FULL_87493E26 = "SharedTsBlockQueue 已满";
  public static final String EXCEPTION_FRAGMENT_INSTANCE_ID_CANNOT_BE_NULL_4BE84F40 = "FragmentInstance ID 不能为空";
  public static final String EXCEPTION_PLANNODE_ID_CANNOT_BE_NULL_F91303CD = "PlanNode ID 不能为空";
  public static final String EXCEPTION_LOCAL_MEMORY_MANAGER_CANNOT_BE_NULL_54701481 = "本地内存管理器不能为空";
  public static final String EXCEPTION_EXECUTORSERVICE_CAN_NOT_BE_NULL_DOT_220C966B =
      "ExecutorService 不能为空";
  public static final String EXCEPTION_TSBLOCK_CANNOT_BE_NULL_E7EA3BDA = "TsBlock 不能为空";
  public static final String EXCEPTION_BYTESTORESERVE_SHOULD_BE_GREATER_THAN_ZERO_DOT_56D15DE0 =
      "bytesToReserve 应大于 0";
  public static final String EXCEPTION_MAXBYTESCANRESERVE_SHOULD_BE_GREATER_THAN_ZERO_DOT_E9F7D365 =
      "maxBytesCanReserve 应大于 0";
  public static final String EXCEPTION_MAX_BYTES_SHOULD_BE_GREATER_THAN_ZERO_COLON_ARG_EA1FB495 = "max bytes 应大于 0：%d";
  public static final String EXCEPTION_MAX_BYTES_PER_FI_SHOULD_BE_IN_LEFT_PAREN_0_COMMA_MAXBYTES_RIGHT_BRACKET_DOT_MAXB_4D37C457 = "max bytes per FI 应在 (0,maxBytes] 范围内。maxBytesPerFI：%d，maxBytes：%d";
  public static final String EXCEPTION_BYTESTORESERVE_SHOULD_BE_IN_LEFT_PAREN_0_COMMA_MAXBYTESPERFI_RIGHT_BRACKET_DOT_M_0753BB69 = "bytesToReserve 应在 (0,maxBytesPerFI] 范围内。maxBytesPerFI：%d，bytesToReserve：%d";
  public static final String EXCEPTION_INVALID_FUTURE_TYPE_14507EF5 = "无效的 future type ";
  public static final String EXCEPTION_QUERYID_CANNOT_BE_NULL_861D7663 = "queryId 不能为空";
  public static final String EXCEPTION_FRAGMENTINSTANCEID_CANNOT_BE_NULL_C722F460 = "fragmentInstanceId 不能为空";
  public static final String EXCEPTION_PLANNODEID_CANNOT_BE_NULL_4533C72B = "planNodeId 不能为空";
  public static final String EXCEPTION_ID_CAN_NOT_BE_NULL_DOT_BDD2AD7D =
      "id 不能为空";
  public static final String EXCEPTION_QUERYID_CAN_NOT_BE_NULL_DOT_16639DBE =
      "queryId 不能为空";
  public static final String EXCEPTION_FRAGMENTINSTANCEID_CAN_NOT_BE_NULL_DOT_E88CF18B =
      "fragmentInstanceId 不能为空";
  public static final String EXCEPTION_PLANNODEID_CAN_NOT_BE_NULL_DOT_44027620 =
      "planNodeId 不能为空";
  public static final String EXCEPTION_THE_FUTURE_TO_BE_CANCELLED_CAN_NOT_BE_NULL_DOT_73CE402A =
      "待取消的 future 不能为空。";
  public static final String EXCEPTION_LOCK_IS_NOT_REENTRANT_7967C13E = "锁不可重入";
  public static final String EXCEPTION_CURRENT_THREAD_DOES_NOT_HOLD_LOCK_68FFB1D9 = "当前线程未持有锁";
  public static final String EXCEPTION_ROOT_OPERATOR_SHOULD_NOT_BE_NULL_F4890A7A = "root Operator 不能为空";
  public static final String EXCEPTION_SINK_SHOULD_NOT_BE_NULL_3CC4F006 = "Sink 不能为空";
  public static final String EXCEPTION_INITIALCOUNT_SHOULDN_QUOTE_T_BE_NULL_HERE_8B333953 =
      "initialCount 在此处不能为空";
  public static final String EXCEPTION_COUNT_SHOULDN_QUOTE_T_BE_NULL_HERE_1EBA9339 =
      "count 在此处不能为空";
  public static final String EXCEPTION_TASKID_IS_NULL_E1221EB2 = "taskId 不能为空";
  public static final String EXCEPTION_INSTANCEID_IS_NULL_343234DC = "instanceId 不能为空";
  public static final String EXCEPTION_FRAGMENTINSTANCEID_IS_NULL_4D371DB4 = "fragmentInstanceId 不能为空";
  public static final String EXCEPTION_CURRENT_STATE_IS_ALREADY_DONE_19FC56DC = "当前状态已完成";
  public static final String EXCEPTION_SUPPRESSED_IS_NULL_F4CD280B = "suppressed 不能为空";
  public static final String EXCEPTION_STACK_IS_NULL_6844D421 = "stack 不能为空";
  public static final String EXCEPTION_ARG_IS_A_NON_MINUS_DONE_FAILURE_STATE_B167E915 = "%s 是未完成的失败状态";
  public static final String EXCEPTION_WARNINGCODE_IS_NULL_3CCAE5B7 = "warningCode 不能为空";
  public static final String EXCEPTION_MESSAGE_IS_NULL_D2D078AA = "message 不能为空";
  public static final String EXCEPTION_WARNING_IS_NULL_E5A7C3C1 = "warning 不能为空";
  public static final String EXCEPTION_NO_FIRST_FILE_F5F2E276 = "没有第一个文件";
  public static final String EXCEPTION_NO_FIRST_CHUNK_7DCEB14C = "没有第一个 chunk";
  public static final String EXCEPTION_CAN_QUOTE_T_INIT_NULL_CHUNKMETA_15C12BEE = "不能用 null chunkMeta 初始化";
  public static final String EXCEPTION_OPERATORCONTEXT_IS_NULL_D15B1EDB = "operatorContext 不能为空";
  public static final String EXCEPTION_CHILD_OPERATOR_IS_NULL_8860113C = "child operator 不能为空";
  public static final String EXCEPTION_GROUPBYTIMEPARAMETER_CANNOT_BE_NULL_IN_SLIDINGWINDOWAGGREGATIONOPERATOR_BA42E30D =
      "SlidingWindowAggregationOperator 中的 GroupByTimeParameter 不能为空。";
  public static final String EXCEPTION_REMAININGINPUTLOCATIONS_IS_NULL_CEBBA2C1 = "remainingInputLocations 不能为空";
  public static final String EXCEPTION_LAST_QUERY_RESULT_SHOULD_ONLY_HAVE_ONE_RECORD_EDFEE635 = "last 查询结果只能有一条记录";
  public static final String EXCEPTION_OUTPUTPATHS_SHOULDN_QUOTE_T_BE_NULL_BF3F5FB4 = "outputPaths 不能为空";
  public static final String EXCEPTION_CHILD_SIZE_OF_INNERTIMEJOINOPERATOR_SHOULD_BE_LARGER_THAN_1_37EB7D74 =
      "InnerTimeJoinOperator 的子节点数量应大于 1";
  public static final String EXCEPTION_CHILD_SIZE_OF_VERTICALLYCONCATOPERATOR_SHOULD_BE_LARGER_THAN_0_14A2513A =
      "VerticallyConcatOperator 的子节点数量应大于 0";
  public static final String EXCEPTION_CHILD_SIZE_OF_TIMEJOINOPERATOR_SHOULD_BE_LARGER_THAN_0_EDED9CB8 =
      "TimeJoinOperator 的子节点数量应大于 0";
  public static final String EXCEPTION_DATASTORE_IS_NULL_D9972B2E = "dataStore 不能为空";
  public static final String EXCEPTION_LASTVALUESCACHERESULTS_SHOULDN_QUOTE_T_BE_NULL_HERE_0DCD5841 =
      "lastValuesCacheResults 在此处不能为空";
  public static final String EXCEPTION_ACCUMULATOR_SHOULD_BE_LASTDESCACCUMULATOR_WHEN_REACH_HERE_CE38F96A = "执行到这里时 Accumulator 应为 LastDescAccumulator";
  public static final String EXCEPTION_DRIVERTASK_TO_BE_PUSHED_IS_NULL_7581A0E3 = "待推送的 DriverTask 不能为空";
  public static final String EXCEPTION_DRIVERTASK_IS_NULL_A13D4AF9 = "driverTask 不能为空";
  public static final String EXCEPTION_SELECTED_LEVEL_CAN_NOT_EQUAL_TO_MINUS_1_1DA93B14 =
      "selected level 不能等于 -1。";
  public static final String EXCEPTION_RESULT_DRIVERTASK_CANNOT_BE_NULL_30A06DB1 = "结果 DriverTask 不能为空";
  public static final String EXCEPTION_DRIVERTASKQUEUE_IS_NULL_7C3C8B7B = "driverTaskQueue 不能为空";
  public static final String EXCEPTION_MAXDRIVERSPERTASK_IS_NULL_8408F9B7 = "maxDriversPerTask 不能为空";
  public static final String EXCEPTION_QUEUE_CAN_NOT_BE_NULL_DOT_9BB286B1 =
      "queue 不能为空";
  public static final String EXCEPTION_SOURCEHANDLELISTENER_CAN_NOT_BE_NULL_DOT_01817F52 =
      "sourceHandleListener 不能为空";
  public static final String EXCEPTION_LOCALFRAGMENTINSTANCEID_CAN_NOT_BE_NULL_DOT_37F5917D =
      "localFragmentInstanceId 不能为空";
  public static final String EXCEPTION_LOCALPLANNODEID_CAN_NOT_BE_NULL_DOT_44A34A33 =
      "localPlanNodeId 不能为空";
  public static final String EXCEPTION_START_SEQUENCE_ID_SHOULD_BE_GREATER_THAN_OR_EQUAL_TO_ZERO_DOT_START_SEQUENCE_ID__D3C0AAB7 =
      "开始序列 ID 必须大于等于 0。开始序列 ID：";
  public static final String EXCEPTION_COMMA_END_SEQUENCE_ID_COLON_DB1AF173 = "，结束序列 ID：";
  public static final String EXCEPTION_END_SEQUENCE_ID_SHOULD_BE_GREATER_THAN_THE_START_SEQUENCE_ID_DOT_START_SEQUENCE__DF1AA2A1 = "结束序列 ID 应大于开始序列 ID。开始序列 ID：";
  public static final String EXCEPTION_RESERVED_BYTES_SHOULD_BE_GREATER_THAN_ZERO_DOT_64086BE5 =
      "Reserved bytes 应大于 0";
  public static final String EXCEPTION_REMOTEENDPOINT_CAN_NOT_BE_NULL_DOT_DE2B5885 =
      "remoteEndpoint 不能为空";
  public static final String EXCEPTION_REMOTEFRAGMENTINSTANCEID_CAN_NOT_BE_NULL_DOT_C2449A29 =
      "remoteFragmentInstanceId 不能为空";
  public static final String EXCEPTION_LOCALMEMORYMANAGER_CAN_NOT_BE_NULL_DOT_7A46C6CE =
      "localMemoryManager 不能为空";
  public static final String EXCEPTION_EXECUTORSERVICE_CAN_NOT_BE_NULL_DOT_BC459BD4 =
      "executorService 不能为空";
  public static final String EXCEPTION_SERDE_CAN_NOT_BE_NULL_DOT_D46F66E7 =
      "serde 不能为空";
  public static final String EXCEPTION_DOT_9D9B854A = ".";
  public static final String EXCEPTION_START_SEQUENCE_ID_SHOULD_BE_GREATER_THAN_OR_EQUAL_TO_ZERO_COMMA_BUT_WAS_COLON_4D2D708E =
      "开始序列 ID 必须大于等于 0，实际值：";
  public static final String EXCEPTION_REMOTEENDPOINT_CAN_NOT_BE_NULL_DOT_83488ACF =
      "remoteEndPoint 不能为空";
  public static final String EXCEPTION_REMOTEPLANNODEID_CAN_NOT_BE_NULL_DOT_03956DE2 =
      "remotePlanNodeId 不能为空";
  public static final String EXCEPTION_SINKLISTENER_CAN_NOT_BE_NULL_DOT_32C9E7C0 =
      "sinkListener 不能为空";
  public static final String EXCEPTION_TSBLOCKS_IS_NULL_02287FD8 = "tsBlocks 不能为空";
  public static final String EXCEPTION_DOWNSTREAMCHANNELLIST_CAN_NOT_BE_NULL_DOT_417AD5A3 =
      "downStreamChannelList 不能为空";
  public static final String EXCEPTION_DOWNSTREAMCHANNELINDEX_CAN_NOT_BE_NULL_DOT_A1D5A266 =
      "downStreamChannelIndex 不能为空";
  public static final String EXCEPTION_STEP_IN_SLIDINGWINDOWAGGREGATIONOPERATOR_CAN_ONLY_PROCESS_PARTIAL_RESULT_E221A2C5 = "SlidingWindowAggregationOperator 中的 Step 只能处理部分结果";
  public static final String EXCEPTION_SLIDINGWINDOWAGGREGATIONOPERATOR_CAN_ONLY_PROCESS_ONE_TSBLOCK_INPUT_DOT_7B9FCAB7 = "SlidingWindowAggregationOperator 只能处理一个 TsBlock 输入。";
  public static final String EXCEPTION_IS_NULL_97AAF381 = " 不能为空";
  public static final String EXCEPTION_MPP_DATA_EXCHANGE_MANAGER_SHOULD_NOT_BE_NULL_44D7141E = "MPP_DATA_EXCHANGE_MANAGER 不能为空";
  public static final String EXCEPTION_ROOTOPERATOR_IS_NULL_050A1E79 = "rootOperator 不能为空";
  public static final String EXCEPTION_DRIVERCONTEXT_IS_NULL_4FEBE55F = "driverContext 不能为空";
  public static final String EXCEPTION_QUERY_CONTEXT_CANNOT_BE_NULL_C4809234 = "查询上下文不能为空";
  public static final String EXCEPTION_DESCRIPTOR_QUOTE_S_INPUT_EXPRESSION_SIZE_IS_NOT_1_DA4BED50 = "descriptor 的输入表达式大小不是 1";
  public static final String EXCEPTION_GROUPBYLEVEL_DESCRIPTORLIST_CANNOT_BE_EMPTY_34604314 = "GroupByLevel descriptorList 不能为空";
  public static final String EXCEPTION_GROUPBYTAG_TAG_KEYS_CANNOT_BE_EMPTY_5D649624 = "GroupByTag tag keys 不能为空";
  public static final String EXCEPTION_GROUPBYTAG_AGGREGATION_DESCRIPTORS_CANNOT_BE_EMPTY_82EC14EB = "GroupByTag aggregation descriptors 不能为空";
  public static final String EXCEPTION_AGGREGATION_DESCRIPTORLIST_CANNOT_BE_EMPTY_490C1740 = "Aggregation descriptorList 不能为空";
  public static final String EXCEPTION_PUSH_DOWN_PREDICATE_IS_NOT_SUPPORTED_YET_178F04A1 = "暂不支持 push down predicate";
  public static final String EXCEPTION_SINK_IS_NULL_E33854B4 = "sink 不能为空";
  public static final String EXCEPTION_THERE_MUST_BE_AT_MOST_ONE_SINKNODE_A965AFE7 = "最多只能有一个 SinkNode";
  public static final String EXCEPTION_QUERY_CONTEXT_CANNOT_BE_NULL_DOT_2D3369FE = "查询上下文不能为空。";
  public static final String EXCEPTION_UNEXPECTED_NODE_TYPE_COLON_41FBCBF3 = "意外的节点类型：";
  public static final String EXCEPTION_TEMPLATEDINFO_SHOULD_NOT_BE_NULL_B5898568 = "TemplatedInfo 不能为空";
  public static final String EXCEPTION_RESULTHANDLE_IN_COORDINATOR_SHOULD_BE_INIT_FIRSTLY_DOT_0F44159B = "Coordinator 中的 ResultHandle 应先初始化。";
  public static final String EXCEPTION_EXPRESSION_IS_NOT_ANALYZED_COLON_ARG_7D34C49A = "表达式尚未分析：%s";
  public static final String EXCEPTION_PATH_QUOTE_ARG_QUOTE_IS_NOT_ANALYZED_IN_GROUPBYLEVELHELPER_DOT_BCEE9D39 = "路径 '%s' 未在 GroupByLevelHelper 中分析。";
  public static final String EXCEPTION_SYMBOL_IS_NULL_AE539B31 = "symbol 不能为空";
  public static final String EXCEPTION_NO_TYPE_FOUND_FOR_SYMBOL_QUOTE_ARG_QUOTE_IN_TYPEPROVIDER_F4DD9DF7 = "TypeProvider 中未找到 symbol '%s' 的类型";
  public static final String EXCEPTION_OUTPUT_COLUMN_QUOTE_ARG_QUOTE_IS_NOT_STORED_IN_ARG_2DE3176D = "输出列 '%s' 未存储在 %s 中";
  public static final String EXCEPTION_PATTERNSTRING_CANNOT_BE_NULL_8A2903F8 = "patternString 不能为空";
  public static final String EXCEPTION_THE_LENGTH_OF_CASEWHENTHENEXPRESSION_QUOTE_S_WHENTHENLIST_MUST_GREATER_THAN_0_069775ED = "CaseWhenThenExpression 的 whenThenList 长度必须大于 0";
  public static final String EXCEPTION_QUERYCONTEXT_IS_NULL_C2344379 = "QueryContext 不能为空";
  public static final String EXCEPTION_PREDICATE_LEFT_BRACKET_ARG_RIGHT_BRACKET_SHOULD_BE_SIMPLIFIED_IN_PREVIOUS_STEP_9262C154 = "谓词 [%s] 应在前一步中简化";
  public static final String EMPTY_MESSAGE = "";
  public static final String EXCEPTION_SEMICOLON_0FAEF84A = "; ";
  public static final String EXCEPTION_THE_TSBLOCK_SHOULD_NOT_BE_NULL_WHEN_CONSTRUCTING_MEMORYSOURCEHANDLE_8D205293 =
      "构造 MemorySourceHandle 时 TsBlock 不能为空。";
  public static final String EXCEPTION_THE_TIME_ORDER_IS_NOT_SPECIFIED_DOT_624A7526 = "未指定时间顺序。";
  public static final String EXCEPTION_THE_TIMESERIES_ORDER_IS_NOT_SPECIFIED_DOT_68EE3875 = "未指定时间序列顺序。";
  public static final String EXCEPTION_THE_DEVICE_ORDER_IS_NOT_SPECIFIED_DOT_D3FB9559 = "未指定设备顺序。";
  public static final String EXCEPTION_SLASH_BC35AB27 = "/";
  public static final String EXCEPTION_INFORMATIONSCHEMATABLESCANNODE_MUST_HAVE_REGIONREPLICASET_0411DBCB = "InformationSchemaTableScanNode 必须包含 regionReplicaSet";
  public static final String EXCEPTION_EACH_INFORMATIONSCHEMATABLESCANNODE_HAVE_ONLY_ONE_DATANODELOCATION_FA3E82C4 = "每个 InformationSchemaTableScanNode 只能有一个 DataNodeLocation";
  public static final String EXCEPTION_CHILD_OF_EXCHANGENODE_MUST_BE_MULTICHILDRENSINKNODE_1BF715FD =
      "ExchangeNode 的子节点必须是 MultiChildrenSinkNode。";
  public static final String EXCEPTION_SIZE_OF_SOURCELOCATIONS_SHOULD_BE_LARGER_THAN_0_2EC41A23 = "sourceLocations 的大小应大于 0";
  public static final String EXCEPTION_INDEX_IS_NOT_VALID_2AB4FB3A = "索引无效";
  public static final String EXCEPTION_LINES_OF_BOX_STRING_SHOULD_BE_GREATER_THAN_0_5DB8C047 = "box string 的行数应大于 0";
  public static final String EXCEPTION_WRONG_NUMBER_OF_NEW_CHILDREN_817AF800 = "新子节点数量错误";
  public static final String EXCEPTION_TRANSLATIONS_IS_NULL_37D62ADC = "translations 不能为空";
  public static final String EXCEPTION_ROOT_IS_NULL_ECC8987D = "root 不能为空";
  public static final String EXCEPTION_SYMBOLHINT_IS_NULL_CE874C40 = "symbolHint 不能为空";
  public static final String EXCEPTION_TYPE_IS_NULL_16A3D3EB = "type 不能为空";
  public static final String EXCEPTION_SYMBOLHINT_NOT_IN_SYMBOLS_MAP_B0D67E43 = "symbolHint 不在 symbols map 中";
  public static final String EXCEPTION_PLANNERCONTEXT_IS_NULL_B7C7DE50 = "plannerContext 不能为空";
  public static final String EXCEPTION_OBJECTS_IS_NULL_819EE879 = "objects 不能为空";
  public static final String EXCEPTION_TYPES_IS_NULL_E4B2309D = "types 不能为空";
  public static final String EXCEPTION_OBJECTS_AND_TYPES_DO_NOT_HAVE_THE_SAME_SIZE_8B51E17B =
      "objects 与 types 的大小不一致。";
  public static final String EXCEPTION_OUTERCONTEXT_IS_NULL_031CD366 = "outerContext 不能为空";
  public static final String EXCEPTION_SCOPE_IS_NULL_4F364BA2 = "scope 不能为空";
  public static final String EXCEPTION_ANALYSIS_IS_NULL_66666A58 = "analysis 不能为空";
  public static final String EXCEPTION_FIELDSYMBOLS_IS_NULL_5130E49C = "fieldSymbols 不能为空";
  public static final String EXCEPTION_ASTTOSYMBOLS_IS_NULL_80B3970F = "astToSymbols 不能为空";
  public static final String EXCEPTION_TOO_FEW_PARAMETER_VALUES_2F7358C6 = "参数值过少";
  public static final String EXCEPTION_SYMBOLALLOCATOR_IS_NULL_E2BE1908 = "symbolAllocator 不能为空";
  public static final String EXCEPTION_QUERYCONTEXT_IS_NULL_761DB539 = "queryContext 不能为空";
  public static final String EXCEPTION_SESSION_IS_NULL_6CF0F47D = "session 不能为空";
  public static final String EXCEPTION_RECURSIVESUBQUERIES_IS_NULL_6AD8A180 = "recursiveSubqueries 不能为空";
  public static final String EXCEPTION_PREDICATEWITHUNCORRELATEDSCALARSUBQUERYRECONSTRUCTOR_IS_NULL_B264FEBC = "predicateWithUncorrelatedScalarSubqueryReconstructor 不能为空";
  public static final String EXCEPTION_ONLY_SUPPORT_ONE_GROUPINGSET_NOW_A1277FA4 = "目前仅支持一个 groupingSet";
  public static final String EXCEPTION_EXPRESSION_IS_NULL_16C079B5 = "expression 不能为空";
  public static final String EXCEPTION_EXPRESSIONTYPES_IS_NULL_4107A4A2 = "expressionTypes 不能为空";
  public static final String EXCEPTION_TYPE_NOT_FOUND_FOR_EXPRESSION_COLON_ARG_C26C9237 = "未找到表达式的类型：%s";
  public static final String EXCEPTION_VALUE_REACH_HERE_MUST_BE_EXPRESSION_C7CA1971 = "执行到这里时值必须为表达式";
  public static final String EXCEPTION_PLAN_IS_NULL_717C9DF7 = "plan 不能为空";
  public static final String EXCEPTION_LOOKUP_IS_NULL_B8FD7E65 = "lookup 不能为空";
  public static final String EXCEPTION_CONSUMER_IS_NULL_B6207072 = "consumer 不能为空";
  public static final String EXCEPTION_FIELDMAPPINGS_IS_NULL_C3681969 = "fieldMappings 不能为空";
  public static final String EXCEPTION_NO_FIELD_MINUS_GREATER_THAN_SYMBOL_MAPPING_FOR_FIELD_ARG_698FDF06 = "字段 %s 没有 field->symbol 映射";
  public static final String EXCEPTION_FOR_NOW_COMMA_ONLY_SINGLE_COLUMN_SUBQUERIES_ARE_SUPPORTED_AD9593BE = "目前仅支持单列子查询";
  public static final String EXCEPTION_FOR_NOW_COMMA_ONLY_SINGLE_COLUMN_SUBQUERIES_ARE_SUPPORTED_DOT_068B1A66 = "目前仅支持单列子查询。";
  public static final String EXCEPTION_CLUSTER_IS_EMPTY_22299EED =
      "Cluster 为空";
  public static final String EXCEPTION_CLUSTER_CONTAINS_EXPRESSIONS_THAT_ARE_NOT_EQUIVALENT_TO_EACH_OTHER_7AD9A0E3 =
      "Cluster 包含彼此不等价的表达式。";
  public static final String EXCEPTION_WARNINGCOLLECTOR_IS_NULL_7A524A68 = "warningCollector 不能为空";
  public static final String EXCEPTION_MEASURES_IS_NULL_EC9D2431 = "measures 不能为空";
  public static final String EXCEPTION_MEASUREOUTPUTS_IS_NULL_923F7C4B = "measureOutputs 不能为空";
  public static final String EXCEPTION_SKIPTOPOSITION_IS_NULL_EFBA10CA = "skipToPosition 不能为空";
  public static final String EXCEPTION_PATTERN_IS_NULL_AC4E239A = "pattern 不能为空";
  public static final String EXCEPTION_VARIABLEDEFINITIONS_IS_NULL_5F7B8ED4 = "variableDefinitions 不能为空";
  public static final String EXCEPTION_NO_RELATIONS_SPECIFIED_FOR_UNION_70CE42C4 = "未为 UNION 指定关系";
  public static final String EXCEPTION_NO_RELATIONS_SPECIFIED_FOR_INTERSECT_76B0ED3B = "未为 INTERSECT 指定关系";
  public static final String EXCEPTION_NO_RELATIONS_SPECIFIED_FOR_EXCEPT_C8E4B4AA = "未为 EXCEPT 指定关系";
  public static final String EXCEPTION_NODE_IS_NULL_C1479F4A = "node 不能为空";
  public static final String EXCEPTION_EXPRESSIONS_MUST_BE_IN_THE_SAME_LOCAL_SCOPE_CCAD793E =
      "表达式必须位于同一局部作用域。";
  public static final String EXCEPTION_WHERE_IS_NULL_A1A3FCBC = "WHERE 不能为空";
  public static final String EXCEPTION_SKIPONLY_IS_NULL_80DB0703 = "skipOnly 不能为空";
  public static final String EXCEPTION_UNABLE_TO_REMOVE_PLAN_NODE_AS_IT_CONTAINS_0_OR_MORE_THAN_1_CHILDREN_6F26E194 =
      "无法移除 plan node：其子节点数量为 0 或大于 1。";
  public static final String EXCEPTION_FIELDS_IS_NULL_DE209DBF = "fields 不能为空";
  public static final String EXCEPTION_CANNOT_RESOLVE_SYMBOL_ARG_79F76FA6 = "无法解析 symbol %s";
  public static final String EXCEPTION_COLUMNREFERENCES_IS_NULL_124955C5 = "columnReferences 不能为空";
  public static final String EXCEPTION_SCOPEEQUALITIES_IS_NULL_22388B2C = "scopeEqualities 不能为空";
  public static final String EXCEPTION_SCOPECOMPLEMENTEQUALITIES_IS_NULL_B9080FC7 = "scopeComplementEqualities 不能为空";
  public static final String EXCEPTION_SCOPESTRADDLINGEQUALITIES_IS_NULL_F6B979AE = "scopeStraddlingEqualities 不能为空";
  public static final String EXCEPTION_SYMBOLTYPES_IS_NULL_DD16EA83 = "symbolTypes 不能为空";
  public static final String EXCEPTION_EXPRESSION_CANNOT_BE_NULL_EFF1A99C = "表达式不能为空";
  public static final String EXCEPTION_TYPE_CANNOT_BE_NULL_97A0A8D3 = "类型不能为空";
  public static final String EXCEPTION_NO_TYPE_FOR_COLON_ARG_9E34AD76 = "没有对应类型：%s";
  public static final String EXCEPTION_CONDITION_MUST_BE_BOOLEAN_COLON_ARG_806C2960 = "条件必须为 boolean：%s";
  public static final String EXCEPTION_TYPES_MUST_BE_EQUAL_COLON_ARG_VS_ARG_098424AD = "类型必须相同：%s vs %s";
  public static final String EXCEPTION_ELEMENTS_IS_NULL_3451C1DA = "elements 不能为空";
  public static final String EXCEPTION_PREDICATE_IS_NULL_22E687A9 = "predicate 不能为空";
  public static final String EXCEPTION_MAPPER_IS_NULL_1D7789D1 = "mapper 不能为空";
  public static final String EXCEPTION_FUNCTION_IS_NULL_E0FA4B62 = "function 不能为空";
  public static final String EXCEPTION_N_MUST_BE_GREATER_THAN_OR_EQUAL_TO_ZERO_C4CE8BF0 = "n 必须大于或等于 0";
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
  public static final String EXCEPTION_ROOT_STATEMENT_IS_ANALYSIS_IS_NULL_36BCD4D1 =
      "根语句的分析结果不能为空。";
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
  public static final String EXCEPTION_QUERY_IS_NOT_REGISTERED_AS_EXPANDABLE_FAAD8FC9 = "查询未注册为可扩展";
  public static final String EXCEPTION_EXPRESSION_NOT_ANALYZED_COLON_ARG_D397B665 = "表达式尚未分析：%s";
  public static final String EXCEPTION_EXPRESSION_IS_NOT_A_COLUMN_REFERENCE_COLON_ARG_7957E705 = "表达式不是列引用：%s";
  public static final String EXCEPTION_EXPECTED_JOIN_FIELDS_FOR_LEFT_AND_RIGHT_TO_HAVE_THE_SAME_SIZE_21BFD449 =
      "期望左右 join fields 大小一致。";
  public static final String EXCEPTION_NO_COLUMNS_GIVEN_TO_INSERT_52C42E47 = "未给出要插入的列";
  public static final String EXCEPTION_MISSING_FILLANALYSIS_FOR_NODE_ARG_7E5B19A4 = "节点 %s 缺少 FillAnalysis";
  public static final String EXCEPTION_MISSING_OFFSET_VALUE_FOR_NODE_ARG_4B107520 = "节点 %s 缺少 OFFSET 值";
  public static final String EXCEPTION_MISSING_LIMIT_VALUE_FOR_NODE_ARG_DD5FD777 = "节点 %s 缺少 LIMIT 值";
  public static final String EXCEPTION_MISSING_RANGE_FOR_QUANTIFIER_ARG_03461228 = "量词 %s 缺少范围";
  public static final String EXCEPTION_MISSING_UNDEFINED_LABELS_FOR_ARG_CA615EC2 = "%s 缺少未定义标签";
  public static final String EXCEPTION_NO_FIELD_FOR_97419CB1 = "没有对应的 Field：";
  public static final String EXCEPTION_RELATIONID_IS_NULL_C4683108 = "relationId 不能为空";
  public static final String EXCEPTION_FIELDINDEX_MUST_BE_NON_MINUS_NEGATIVE_COMMA_GOT_COLON_ARG_09C2C06D = "fieldIndex 必须为非负数，实际为：%s";
  public static final String EXCEPTION_TYPEMANAGER_IS_NULL_12A72016 = "typeManager 不能为空";
  public static final String EXCEPTION_NODES_IS_NULL_7AB3C1D7 = "nodes 不能为空";
  public static final String EXCEPTION_CLAZZ_IS_NULL_7F710E3E = "clazz 不能为空";
  public static final String EXCEPTION_GROUPBYEXPRESSIONS_IS_NULL_BFDC07D2 = "groupByExpressions 不能为空";
  public static final String EXCEPTION_SOURCESCOPE_IS_NULL_4B3626A7 = "sourceScope 不能为空";
  public static final String EXCEPTION_ORDERBYSCOPE_IS_NULL_A6017E73 = "orderByScope 不能为空";
  public static final String EXCEPTION_NO_FIELD_FOR_E99DCE9A = "没有对应的 field：";
  public static final String EXCEPTION_INVALID_PARAMETER_NUMBER_ARG_COMMA_MAX_VALUES_IS_ARG_B3F5C4E8 =
      "无效的参数数量 %s，最大值为 %s。";
  public static final String EXCEPTION_GROUPING_FIELD_ARG_SHOULD_ORIGINATE_FROM_ARG_6DBBCE6B =
      "Grouping field %s 应来源于 %s。";
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
  public static final String EXCEPTION_FIELD_CANNOT_BE_NULL_09155004 = "字段不能为空";
  public static final String EXCEPTION_FIELD_QUOTE_ARG_QUOTE_NOT_FOUND_1BC2FDED = "未找到 Field '%s'";
  public static final String EXCEPTION_PARENT_IS_NULL_ED81BAD8 = "parent 不能为空";
  public static final String EXCEPTION_RELATION_IS_NULL_890596ED = "relation 不能为空";
  public static final String EXCEPTION_NAMEDQUERIES_IS_NULL_AFDE9A4A = "namedQueries 不能为空";
  public static final String EXCEPTION_TABLES_IS_NULL_2012309E = "tables 不能为空";
  public static final String EXCEPTION_RELATIONTYPE_IS_NULL_62DDF9C1 = "relationType 不能为空";
  public static final String EXCEPTION_BASISTYPE_IS_NULL_33E4F842 = "basisType 不能为空";
  public static final String EXCEPTION_PARENT_IS_ALREADY_SET_835DE0A5 = "parent 已设置";
  public static final String EXCEPTION_QUERY_QUOTE_ARG_QUOTE_IS_ALREADY_ADDED_F3D47DBD = "Query '%s' 已添加";
  public static final String EXCEPTION_MISSING_SCOPE_D573869F = "缺少 scope";
  public static final String EXCEPTION_MISSING_RELATIONTYPE_679D3CFA = "缺少 relationType";
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
  public static final String EXCEPTION_EXPRESSION_NOT_YET_ANALYZED_COLON_ARG_0F4F19B7 = "表达式尚未分析：%s";
  public static final String EXCEPTION_ARG_ALREADY_KNOWN_TO_REFER_TO_ARG_8C8B4F24 =
      "%s 已知引用 %s。";
  public static final String EXCEPTION_NO_RESOLVED_WINDOW_FOR_COLON_AED19667 = "没有 resolved window：";
  public static final String EXCEPTION_NO_LABEL_AVAILABLE_8508CE32 = "没有可用 label";
  public static final String EXCEPTION_REWRITES_IS_NULL_4E5AD77A = "rewrites 不能为空";
  public static final String EXCEPTION_STATEMENT_REWRITE_RETURNED_NULL_AB1E89EA = "Statement rewrite 返回了 null";
  public static final String EXCEPTION_ATN_IS_NULL_48BE0D3E = "atn 不能为空";
  public static final String EXCEPTION_LEXER_IS_NULL_88834E18 = "lexer 不能为空";
  public static final String EXCEPTION_PARSER_IS_NULL_AE8E5D6F = "parser 不能为空";
  public static final String EXCEPTION_LEXER_ATN_MISMATCH_COLON_EXPECTED_ARG_COMMA_FOUND_ARG_8ED22CF1 =
      "Lexer ATN 不匹配：期望 %s，实际 %s。";
  public static final String EXCEPTION_PARSER_ATN_MISMATCH_COLON_EXPECTED_ARG_COMMA_FOUND_ARG_FF75D61B =
      "Parser ATN 不匹配：期望 %s，实际 %s。";
  public static final String EXCEPTION_INITIALIZER_IS_NULL_2EEC3764 = "initializer 不能为空";
  public static final String EXCEPTION_TERMINALNODE_IS_NULL_578F27FD = "terminalNode 不能为空";
  public static final String EXCEPTION_PARSERRULECONTEXT_IS_NULL_9E0DD3B5 = "parserRuleContext 不能为空";
  public static final String EXCEPTION_TOKEN_IS_NULL_43094C56 = "token 不能为空";
  public static final String EXCEPTION_VALUE_IS_NULL_192F6BFF = "值不能为空";
  public static final String EXCEPTION_LOCATION_IS_NULL_F134D388 = "location 不能为空";
  public static final String EXCEPTION_TOPIC_NAME_CAN_NOT_BE_NULL_EA4ED0BF =
      "topic name 不能为空";
  public static final String EXCEPTION_SOURCE_NAME_IS_NULL_287E475D = "source name 不能为空";
  public static final String EXCEPTION_TARGET_NAME_IS_NULL_A5F701C6 = "target name 不能为空";
  public static final String EXCEPTION_DBNAME_IS_NULL_4521C4EE = "dbName 不能为空";
  public static final String EXCEPTION_PROPERTIES_IS_NULL_57B88B49 = "properties 不能为空";
  public static final String EXCEPTION_TARGET_IS_NULL_240F0372 = "target 不能为空";
  public static final String EXCEPTION_INDEXNAME_IS_NULL_2525299C = "indexName 不能为空";
  public static final String EXCEPTION_COLUMNLIST_IS_NULL_DADE6825 = "columnList 不能为空";
  public static final String EXCEPTION_SIZE_OF_COLUMNLIST_SHOULD_BE_LARGER_THAN_1_7EB80E55 = "columnList 的大小应大于 1";
  public static final String EXCEPTION_ASSIGNMENTS_IS_NULL_1FD6142D = "assignments 不能为空";
  public static final String EXCEPTION_PIPE_NAME_CAN_NOT_BE_NULL_14570979 =
      "pipe name 不能为空";
  public static final String EXCEPTION_SQL_IS_NULL_BEDB2B7A = "sql 不能为空";
  public static final String EXCEPTION_SERVICENAME_IS_NULL_1009BA39 = "serviceName 不能为空";
  public static final String EXCEPTION_CANNOT_GET_NON_MINUS_DEFAULT_VALUE_OF_PROPERTY_ARG_SINCE_ITS_VALUE_IS_SET_TO_DEF_E7D3185F =
      "无法获取属性 %s 的非默认值，因为其值已设置为 DEFAULT。";
  public static final String EXCEPTION_STATEMENTNAME_IS_NULL_C03BB8D4 = "statementName 不能为空";
  public static final String EXCEPTION_CATALOGNAME_IS_NULL_2E3C3C6B = "catalogName 不能为空";
  public static final String EXCEPTION_SOURCE_IS_NULL_45946547 = "source 不能为空";
  public static final String EXCEPTION_SUBSCRIPTION_ID_CAN_NOT_BE_NULL_0CDFFD7D =
      "subscription id 不能为空";
  public static final String EXCEPTION_DB_IS_NULL_E1AD1B58 = "db 不能为空";
  public static final String EXCEPTION_TOPIC_ATTRIBUTES_CAN_NOT_BE_NULL_791A8FED =
      "topic attributes 不能为空";
  public static final String EXCEPTION_EXTRACTOR_SLASH_SOURCE_ATTRIBUTES_CAN_NOT_BE_NULL_2B3A656B =
      "extractor/source attributes 不能为空";
  public static final String EXCEPTION_PROCESSOR_ATTRIBUTES_CAN_NOT_BE_NULL_FFF91008 =
      "processor attributes 不能为空";
  public static final String EXCEPTION_CONNECTOR_ATTRIBUTES_CAN_NOT_BE_NULL_7AF2F613 =
      "connector attributes 不能为空";
  public static final String EXCEPTION_CLASSNAME_IS_NULL_3902B37C = "className 不能为空";
  public static final String EXCEPTION_PLUGIN_NAME_CAN_NOT_BE_NULL_92F0F4D6 =
      "plugin name 不能为空";
  public static final String EXCEPTION_CLASS_NAME_CAN_NOT_BE_NULL_1D276677 =
      "class name 不能为空";
  public static final String EXCEPTION_URI_CAN_NOT_BE_NULL_B3535EDC =
      "uri 不能为空";
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
  public static final String EXCEPTION_CANNOT_MERGE_STATS_FOR_DIFFERENT_RULES_COLON_ARG_AND_ARG_F0A5D5E6 =
      "无法合并不同规则的统计信息：%s 和 %s。";
  public static final String EXCEPTION_SCALAR_SUBQUERY_RESULT_SHOULD_ONLY_HAVE_ONE_COLUMN_DOT_893F76CB = "Scalar Subquery 结果只能有一列。";
  public static final String EXCEPTION_SCALAR_SUBQUERY_RESULT_SHOULD_ONLY_HAVE_ONE_ROW_DOT_F9007BBC = "Scalar Subquery 结果只能有一行。";
  public static final String EXCEPTION_SCALAR_SUBQUERY_RESULT_SHOULD_NOT_GET_NULL_DATATYPE_OR_NULL_COLUMN_DOT_616056F4 =
      "Scalar Subquery 结果不应得到 null dataType 或 null 列。";
  public static final String EXCEPTION_OPERATOR_IS_NULL_F5BB9F59 = "operator 不能为空";
  public static final String EXCEPTION_EXPRESSIONS_IS_NULL_C44D9384 = "expressions 不能为空";
  public static final String EXCEPTION_CORRELATION_IS_NULL_F8327EAD = "correlation 不能为空";
  public static final String EXCEPTION_CORRELATEDPREDICATES_IS_NULL_5FCB8011 = "correlatedPredicates 不能为空";
  public static final String EXCEPTION_GROUPING_KEYS_WERE_CORRELATED_EE1C8406 = "分组键存在关联";
  public static final String EXCEPTION_EXPECTED_CONSTANT_SYMBOLS_TO_CONTAIN_ALL_CORRELATED_SYMBOLS_LOCAL_EQUIVALENTS_E20CB055 = "期望常量符号包含所有关联符号的本地等价符号";
  public static final String EXCEPTION_EXPECTED_SYMBOLS_TO_PROPAGATE_TO_CONTAIN_ALL_CONSTANT_SYMBOLS_C9D876E4 = "期望要传播的符号包含所有常量符号";
  public static final String EXCEPTION_CARDINALITYRANGE_IS_NULL_8FDEE0B4 = "cardinalityRange 不能为空";
  public static final String EXCEPTION_METADATAEXPRESSIONS_IS_NULL_3752914C = "metadataExpressions 不能为空";
  public static final String EXCEPTION_EXPRESSIONSCANPUSHDOWN_IS_NULL_DC8DFEB3 = "expressionsCanPushDown 不能为空";
  public static final String EXCEPTION_EXPRESSIONSCANNOTPUSHDOWN_IS_NULL_63BC9AF9 = "expressionsCannotPushDown 不能为空";
  public static final String EXCEPTION_FILTER_PREDICATE_OF_FILTERNODE_IS_NULL_C3964179 =
      "FilterNode 的过滤谓词为空。";
  public static final String EXCEPTION_UNSUPPORTED_JOIN_TYPE_COLON_ARG_9FB6751B = "不支持的 join type: %s";
  public static final String EXCEPTION_UNIQUEID_IN_PREDICATE_IS_NOT_YET_SUPPORTED_7B5D2EAF = "predicate 中暂不支持 UniqueId";
  public static final String EXCEPTION_MAPPERPROVIDER_IS_NULL_472725D5 = "mapperProvider 不能为空";
  public static final String EXCEPTION_CORRELATIONMAPPING_IS_NULL_9D595C82 = "correlationMapping 不能为空";
  public static final String EXCEPTION_MAPPINGS_IS_NULL_23BD9025 = "mappings 不能为空";
  public static final String EXCEPTION_AGGREGATE_WITH_ORDER_BY_DOES_NOT_SUPPORT_PARTIAL_AGGREGATION_D5BDD21F = "带 ORDER BY 的聚合不支持部分聚合";
  public static final String EXCEPTION_LEFTEFFECTIVEPREDICATE_MUST_ONLY_CONTAIN_SYMBOLS_FROM_LEFTSYMBOLS_DB3259B8 =
      "leftEffectivePredicate 只能包含 leftSymbols 中的 symbols。";
  public static final String EXCEPTION_RIGHTEFFECTIVEPREDICATE_MUST_ONLY_CONTAIN_SYMBOLS_FROM_RIGHTSYMBOLS_4B97238D =
      "rightEffectivePredicate 只能包含 rightSymbols 中的 symbols。";
  public static final String EXCEPTION_OUTEREFFECTIVEPREDICATE_MUST_ONLY_CONTAIN_SYMBOLS_FROM_OUTERSYMBOLS_99FC2AA9 =
      "outerEffectivePredicate 只能包含 outerSymbols 中的 symbols。";
  public static final String EXCEPTION_INNEREFFECTIVEPREDICATE_MUST_ONLY_CONTAIN_SYMBOLS_FROM_INNERSYMBOLS_ECB7C6A2 =
      "innerEffectivePredicate 只能包含 innerSymbols 中的 symbols。";
  public static final String EXCEPTION_IDALLOCATOR_IS_NULL_752B308D = "idAllocator 不能为空";
  public static final String EXCEPTION_PLANOPTIMIZERSSTATSCOLLECTOR_IS_NULL_9DA4B0CC = "planOptimizersStatsCollector 不能为空";
  public static final String EXCEPTION_SUBQUERY_RESULT_TYPE_MUST_BE_ORDERABLE_82AF0EFA = "子查询结果类型必须可排序";
  public static final String EXCEPTION_ALL_THE_NON_CORRELATED_SUBQUERIES_SHOULD_BE_REWRITTEN_AT_THIS_POINT_B4614541 = "执行到这里时，所有非关联子查询都应已被重写";
  public static final String EXCEPTION_CHANGEDPLANNODES_IS_NULL_5ECBDE28 = "changedPlanNodes 不能为空";
  public static final String EXCEPTION_MAPPINGFUNCTION_IS_NULL_212D6109 = "mappingFunction 不能为空";
  public static final String EXCEPTION_ROOT_NODE_MUST_RETURN_ONLY_ONE_FF42061C = "根节点只能返回一个结果";
  public static final String EXCEPTION_SIZE_OF_TOPKNODE_CAN_ONLY_BE_1_IN_LOGICAL_PLAN_DOT_DB32E3C5 = "逻辑计划中的 TopKNode 大小只能为 1。";
  public static final String EXCEPTION_THE_SIZE_OF_LEFT_CHILDREN_NODE_OF_JOINNODE_SHOULD_BE_1_F3437368 = "JoinNode 的左子节点数量应为 1";
  public static final String EXCEPTION_THE_SIZE_OF_RIGHT_CHILDREN_NODE_OF_JOINNODE_SHOULD_BE_1_6BA167CF = "JoinNode 的右子节点数量应为 1";
  public static final String EXCEPTION_THE_SIZE_OF_LEFT_CHILDREN_NODE_OF_SEMIJOINNODE_SHOULD_BE_1_FFEE3F41 = "SemiJoinNode 的左子节点数量应为 1";
  public static final String EXCEPTION_THE_SIZE_OF_RIGHT_CHILDREN_NODE_OF_SEMIJOINNODE_SHOULD_BE_1_AE90C4B8 = "SemiJoinNode 的右子节点数量应为 1";
  public static final String EXCEPTION_CHILDRENNODES_SHOULD_NOT_BE_NULL_DOT_0C93B063 = "childrenNodes 不能为空。";
  public static final String EXCEPTION_CHILDRENNODES_SHOULD_NOT_BE_EMPTY_DOT_E5555FD9 = "childrenNodes 不能为空。";
  public static final String EXCEPTION_SIZE_OF_TOPKRANKINGNODE_CAN_ONLY_BE_1_IN_LOGICAL_PLAN_DOT_20D6A513 = "逻辑计划中的 TopKRankingNode 大小只能为 1。";
  public static final String EXCEPTION_STATS_IS_NULL_D3627E6A = "stats 不能为空";
  public static final String EXCEPTION_USELEGACYRULES_IS_NULL_0AD13CAB = "useLegacyRules 不能为空";
  public static final String EXCEPTION_RULES_IS_NULL_DF243716 = "rules 不能为空";
  public static final String EXCEPTION_EXPECTED_CHILD_TO_BE_A_GROUP_REFERENCE_DOT_FOUND_COLON_EC01971C =
      "期望子节点为 group reference。实际：";
  public static final String EXCEPTION_TIMEOUT_HAS_TO_BE_A_NON_MINUS_NEGATIVE_NUMBER_LEFT_BRACKET_MILLISECONDS_RIGHT_BR_5201D8B3 = "Timeout 必须为非负数 [milliseconds]";
  public static final String EXCEPTION_NODE_QUOTE_ARG_QUOTE_IS_NOT_A_GROUPREFERENCE_73C8C127 = "节点 '%s' 不是 GroupReference";
  public static final String EXCEPTION_MEMBER_IS_NULL_466D8670 = "member 不能为空";
  public static final String EXCEPTION_INVALID_GROUP_COLON_ARG_C0BAD253 = "无效的 group: %s";
  public static final String EXCEPTION_ARG_COLON_TRANSFORMED_EXPRESSION_DOESN_QUOTE_T_PRODUCE_SAME_OUTPUTS_COLON_ARG_VS_F9BAF138 = "%s：转换后的表达式产生的输出不同：%s vs %s";
  public static final String EXCEPTION_CANNOT_DELETE_GROUP_THAT_HAS_INCOMING_REFERENCES_83C9D700 =
      "无法删除存在传入引用的 group。";
  public static final String EXCEPTION_REFERENCE_TO_REMOVE_NOT_FOUND_2EB93289 =
      "未找到待移除的引用。";
  public static final String EXCEPTION_TRANSFORMEDPLAN_IS_NULL_83B2099A = "transformedPlan 不能为空";
  public static final String EXCEPTION_NEWCHILDREN_IS_NOT_EMPTY_170FCE18 = "newChildren 非空";
  public static final String EXCEPTION_GROUPINGSETS_IS_NULL_8EE6D9BF = "groupingSets 不能为空";
  public static final String EXCEPTION_PREGROUPEDSYMBOLS_IS_NULL_DC24FF7B = "preGroupedSymbols 不能为空";
  public static final String EXCEPTION_GROUPING_COLUMNS_DOES_NOT_CONTAIN_GROUPID_COLUMN_83976C83 = "分组列不包含 groupId 列";
  public static final String EXCEPTION_ORDER_BY_DOES_NOT_SUPPORT_DISTRIBUTED_AGGREGATION_05109B26 = "ORDER BY 不支持分布式聚合";
  public static final String EXCEPTION_DATE_BIN_FUNCTION_MUST_BE_THE_LAST_GROUPINGKEY_EE955FF5 =
      "date_bin 函数必须是最后一个 GroupingKey。";
  public static final String EXCEPTION_PRE_MINUS_GROUPED_SYMBOLS_MUST_BE_A_SUBSET_OF_THE_GROUPING_KEYS_AFC6C33D =
      "预分组符号必须是分组键的子集。";
  public static final String EXCEPTION_EXPECTED_AGGREGATION_TO_HAVE_DISTINCT_INPUT_EC6AF059 = "期望聚合包含 DISTINCT 输入";
  public static final String EXCEPTION_MISMATCHED_CHILD_LEFT_PAREN_ARG_RIGHT_PAREN_AND_PERMITTED_OUTPUTS_LEFT_PAREN_ARG_57801144 = "子节点 (%s) 与允许输出 (%s) 的大小不匹配";
  public static final String EXCEPTION_MISSING_TYPE_FOR_EXPRESSION_3D66D302 = "缺少表达式类型";
  public static final String EXCEPTION_EXCEPTNODE_TRANSLATION_RESULT_HAS_NO_COUNT_SYMBOLS_8653930E = "ExceptNode 翻译结果中没有计数符号";
  public static final String EXCEPTION_UNEXPECTED_CORRELATED_JOIN_TYPE_COLON_ARG_27E8EC42 = "意外的 correlated join 类型：%s";
  public static final String EXCEPTION_CORRELATION_IN_ARG_JOIN_2F78ACC3 = "%s JOIN 中的关联";
  public static final String EXCEPTION_PLANNODE_IS_NULL_49FBBFCF = "planNode 不能为空";
  public static final String EXCEPTION_COUNTSYMBOLS_IS_NULL_416D96FB = "countSymbols 不能为空";
  public static final String EXCEPTION_ROWNUMBERSYMBOL_IS_NULL_BA30E0AA = "rowNumberSymbol 不能为空";
  public static final String EXCEPTION_CANNOT_SIMPLIFY_A_UNIONNODE_9D5B09A7 = "无法简化 UnionNode";
  public static final String EXCEPTION_THE_SIZE_OF_MARKERS_SHOULD_BE_SAME_AS_THE_SIZE_OF_COUNT_OUTPUT_SYMBOLS_6DBDD287 = "markers 的数量应与计数输出符号的数量相同";
  public static final String EXCEPTION_ROWNUMBERSYMBOL_IS_EMPTY_34FE9565 =
      "rowNumberSymbol 为空";
  public static final String EXCEPTION_EXPECTED_SUBQUERY_OUTPUT_SYMBOLS_TO_BE_PRUNED_13B84182 = "期望子查询输出符号已被剪枝";
  public static final String EXCEPTION_TYPEANALYZER_IS_NULL_3106B188 = "typeAnalyzer 不能为空";
  public static final String EXCEPTION_UNEXPECTED_CORRELATEDJOIN_TYPE_COLON_47A368C1 = "意外的 CorrelatedJoin 类型：";
  public static final String EXCEPTION_UNEXPECTED_NULL_LITERAL_WITHOUT_A_CAST_TO_BOOLEAN_D399CFCB = "意外的 null 字面量，未 cast 为 boolean";
  public static final String EXCEPTION_REWRITER_IS_NULL_B0D8CC88 = "rewriter 不能为空";
  public static final String EXCEPTION_UNEXPECTED_NODE_TYPE_COLON_ARG_B1C0328F = "意外的节点类型：%s";
  public static final String EXCEPTION_RESULT_IS_NULL_031E2F89 = "结果不能为空";
  public static final String EXCEPTION_DISTINCT_NOT_SUPPORTED_0E97D0BB = "不支持 distinct";
  public static final String EXCEPTION_EXPRESSION_IS_NOT_ANALYZED_LEFT_PAREN_ARG_RIGHT_PAREN_COLON_ARG_DAE760B6 = "Expression 尚未分析（%s）：%s";
  public static final String EXCEPTION_SYMBOL_REFERENCES_ARE_NOT_ALLOWED_93779D6C = "不允许使用 symbol references";
  public static final String EXCEPTION_EXPRESSION_INTERPRETER_RETURNED_AN_UNRESOLVED_EXPRESSION_5BCE9A51 = "表达式解释器返回了未解析的表达式";
  public static final String EXCEPTION_NULL_OPERAND_SHOULD_HAVE_BEEN_REMOVED_BY_RECURSIVE_COALESCE_PROCESSING_B6D4D443 = "空操作数应已在递归 coalesce 处理中移除";
  public static final String EXCEPTION_NULL_VALUE_IS_EXPECTED_TO_BE_REPRESENTED_AS_NULL_COMMA_NOT_NULLLITERAL_9B96D25A = "空值应表示为 null，而不是 NullLiteral";
  public static final String EXCEPTION_AGGREGATION_FUNCTIONS_ARG_SHOULD_ONLY_HAVE_TWO_ARGUMENTS_3D12DCFD = "聚合函数 [%s] 应当只有两个参数";
  public static final String EXCEPTION_AGGREGATION_FUNCTIONS_ARG_SHOULD_HAVE_VALUE_COLUMN_AS_NUMERIC_TYPE_INT32_INT64_FLOAT_DOUBLE_TIMESTAMP_97A6CA87 = "聚合函数 [%s] 的值列应为数值类型 [INT32, INT64, FLOAT, DOUBLE, TIMESTAMP]";
  public static final String EXCEPTION_AGGREGATION_FUNCTIONS_ARG_SHOULD_HAVE_PERCENTAGE_AS_DECIMAL_TYPE_57033ADF = "聚合函数 [%s] 的百分比参数应为 decimal 类型";
  public static final String EXCEPTION_CANNOT_NEST_AGGREGATIONS_INSIDE_AGGREGATION_ARG_ARG_6E5073A4 = "不能在聚合 '%s' 中嵌套聚合：%s";
  public static final String EXCEPTION_CANNOT_NEST_WINDOW_FUNCTIONS_INSIDE_AGGREGATION_ARG_ARG_8F94A897 = "不能在聚合 '%s' 中嵌套窗口函数：%s";
  public static final String EXCEPTION_PARTITION_BY_EXPRESSION_ARG_MUST_BE_AN_AGGREGATE_EXPRESSION_OR_APPEAR_IN_GROUP_BY_CLAUSE_E3C696D6 = "PARTITION BY 表达式 '%s' 必须是聚合表达式或出现在 GROUP BY 子句中";
  public static final String EXCEPTION_ORDER_BY_EXPRESSION_ARG_MUST_BE_AN_AGGREGATE_EXPRESSION_OR_APPEAR_IN_GROUP_BY_CLAUSE_7FF8267B = "ORDER BY 表达式 '%s' 必须是聚合表达式或出现在 GROUP BY 子句中";
  public static final String EXCEPTION_WINDOW_FRAME_START_MUST_BE_AN_AGGREGATE_EXPRESSION_OR_APPEAR_IN_GROUP_BY_CLAUSE_74E30A63 = "窗口帧起始位置必须是聚合表达式或出现在 GROUP BY 子句中";
  public static final String EXCEPTION_WINDOW_FRAME_END_MUST_BE_AN_AGGREGATE_EXPRESSION_OR_APPEAR_IN_GROUP_BY_CLAUSE_31784B55 = "窗口帧结束位置必须是聚合表达式或出现在 GROUP BY 子句中";
  public static final String EXCEPTION_INVALID_EXPLAIN_FORMAT_3FD91910 = "无效的 EXPLAIN 格式：";
  public static final String EXCEPTION_INVALID_EXPLAIN_ANALYZE_FORMAT_0879A025 = "无效的 EXPLAIN ANALYZE 格式：";
  public static final String EXCEPTION_INVALID_EXPLAIN_FORMAT_ARG_SUPPORTED_FORMATS_GRAPHVIZ_JSON_441A1D48 =
      "无效的 EXPLAIN 格式：%s。支持的格式：GRAPHVIZ、JSON";
  public static final String EXCEPTION_INVALID_EXPLAIN_ANALYZE_FORMAT_ARG_SUPPORTED_FORMATS_TEXT_JSON_77FA30EB =
      "无效的 EXPLAIN ANALYZE 格式：%s。支持的格式：TEXT、JSON";
  public static final String EXCEPTION_GROUP_BY_ALL_CANNOT_BE_COMBINED_WITH_EXPLICIT_GROUPING_ELEMENTS_4CFE411D = "GROUP BY ALL 不能与显式分组元素组合使用";
  public static final String EXCEPTION_THE_SECOND_ARGUMENT_OF_PERCENTILE_FUNCTION_PERCENTAGE_MUST_BE_A_DOUBLE_LITERAL_D9464B46 = "'percentile' 函数的第二个参数 percentage 必须是 double 字面量";
  public static final String EXCEPTION_DATA_TYPE_MISMATCH_FOR_MEASUREMENT_ARGARGARG_TYPE_IN_TSFILE_ARG_TYPE_IN_IOTDB_ARG_C5BA7DBD = "测点 %s%s%s 的数据类型不匹配，TsFile 中类型：%s，IoTDB 中类型：%s";
  public static final String MESSAGE_FAILED_TO_RELEASE_EXTERNAL_TSFILE_QUERY_RESOURCE_712EE978 = "释放外部 TsFile 查询资源失败";
  public static final String EXCEPTION_OUTER_QUERY_TIMEOUT_EXCEEDED_BEFORE_IOTDBLOCAL_QUERY_STARTS_800BFA63 = "在 IoTDBLocal 查询开始前，外层查询已超时";
  public static final String MESSAGE_FAILED_TO_CLOSE_UDF_RESULT_SET_AT_INDEX_ARG_A293B7EC = "关闭索引 {} 处的 UDF 结果集失败";
  public static final String EXCEPTION_INTERNAL_QUERY_EXECUTION_NOT_FOUND_62642542 = "未找到内部查询执行";
  public static final String EXCEPTION_ONLY_QUERY_IS_SUPPORTED_FOR_IOTDBLOCAL_QUERY_INTERFACE_09195570 = "IoTDBLocal 查询接口仅支持 query 操作";
  public static final String EXCEPTION_FAILED_TO_CLOSE_INTERNAL_QUERY_RESULT_ED86BF5E = "关闭内部查询结果失败";
  public static final String EXCEPTION_UDFRESULTSET_IS_ALREADY_CLOSED_E1ACABE3 = "UDFResultSet 已经关闭";

  public static final String EXCEPTION_LATERALCOLUMNALIASREFERENCES_IS_NULL_0CC58C78 = "lateralColumnAliasReferences 不能为空";

  public static final String EXCEPTION_OUTPUTEXPRESSIONS_IS_NULL_7BD39A8F = "outputExpressions 不能为空";
  public static final String EXCEPTION_ALIASES_IS_NULL_6D98FA6C = "aliases 不能为空";
  public static final String EXCEPTION_SINGLECOLUMNEXPRESSIONS_IS_NULL_EFB296FD = "singleColumnExpressions 不能为空";
  public static final String EXCEPTION_CANONICALNAME_IS_NULL_DDFE6DB1 = "canonicalName 不能为空";
  public static final String EXCEPTION_REWRITTENEXPRESSION_IS_NULL_6EE23088 = "rewrittenExpression 不能为空";
  public static final String EXCEPTION_ALIASESBYCANONICALNAME_IS_NULL_635C7ED3 = "aliasesByCanonicalName 不能为空";
  public static final String EXCEPTION_ALIAS_IS_NULL_862D23B1 = "alias 不能为空";
  public static final String EXCEPTION_REFERENCES_IS_NULL_25A7E3AF = "references 不能为空";
  public static final String EXCEPTION_VISIBLEALIASES_IS_NULL_630B27F1 = "visibleAliases 不能为空";

}
