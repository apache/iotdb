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

public final class DataNodePipeMessages {

  // ===================== CONSENSUS =====================

  public static final String CLOSING_DELETION_RESOURCE_MANAGER_FOR = "正在关闭 {} 的删除资源管理器...";
  public static final String DAL_THREAD_STILL_DOESN_T_EXIT_AFTER = "DAL 线程 {} 在 30 秒后仍未退出";
  public static final String DELETIONMANAGER_CURRENT_DAL_DIR_IS_DELETED_SUCCESSFULLY =
      "DeletionManager-{}：当前 DAL 目录 {} 已成功删除";
  public static final String DELETIONMANAGER_CURRENT_DAL_DIR_IS_NOT_INITIALIZED =
      "DeletionManager-{}：当前 DAL 目录 {} 未初始化，无需删除。";
  public static final String DELETIONMANAGER_CURRENT_WAITING_IS_INTERRUPTED_MAY_BECAUSE =
      "DeletionManager-{}：当前等待被中断，可能是因为当前应用已停止。";
  public static final String DELETIONMANAGER_DELETE_DELETION_FILE_IN_DIR =
      "DeletionManager-{}：正在删除目录 {} 中的删除文件...";
  public static final String DELETIONMANAGER_FAILED_TO_DELETE_FILE_IN_DIR =
      "DeletionManager-{}：删除目录 {} 中的文件失败，请手动检查！";
  public static final String DELETIONRESOURCE_HAS_BEEN_RELEASED_TRIGGER_A_REMOVE =
      "DeletionResource {} 已释放，触发移除 DAL...";
  public static final String DELETION_PERSIST_CANNOT_CREATE_FILE_PLEASE_CHECK =
      "Deletion persist：无法创建文件 {}，请手动检查文件系统。";
  public static final String DELETION_PERSIST_CANNOT_WRITE_TO_MAY_CAUSE =
      "Deletion persist：无法写入 {}，可能导致数据不一致。";
  public static final String DELETION_PERSIST_CURRENT_BATCH_FSYNC_DUE_TO =
      "Deletion persist-{}：当前批次因超时执行 fsync";
  public static final String DELETION_PERSIST_CURRENT_FILE_HAS_BEEN_CLOSED =
      "Deletion persist-{}：当前文件已关闭";
  public static final String DELETION_PERSIST_SERIALIZE_DELETION_RESOURCE =
      "Deletion persist-{}：序列化删除资源 {}";
  public static final String DELETION_PERSIST_STARTING_TO_PERSIST_CURRENT_WRITING =
      "Deletion persist-{}：开始持久化，当前写入文件：{}";
  public static final String DELETION_PERSIST_SWITCHING_TO_A_NEW_FILE =
      "Deletion persist-{}：切换到新文件，当前写入文件：{}";
  public static final String DELETION_RESOURCE_MANAGER_FOR_HAS_BEEN_SUCCESSFULLY =
      "{} 的删除资源管理器已成功关闭！";
  public static final String DETECT_FILE_CORRUPTED_WHEN_RECOVER_DAL_DISCARD =
      "恢复 DAL-{} 时检测到文件损坏，将丢弃后续所有 DAL...";
  public static final String FAILED_TO_INITIALIZE_DELETIONRESOURCEMANAGER =
      "初始化 DeletionResourceManager 失败";
  public static final String FAILED_TO_READ_DELETION_FILE_MAY_BECAUSE =
      "读取删除文件 {} 失败，原因可能是该文件写入时已损坏。";
  public static final String FAILED_TO_RECOVER_DELETIONRESOURCEMANAGER =
      "恢复 DeletionResourceManager 失败";
  public static final String FAIL_TO_ALLOCATE_DELETIONBUFFER_GROUP_S_BUFFER =
      "分配 deletionBuffer-group-{} 的缓冲区失败，原因：内存不足。";
  public static final String FAIL_TO_CLOSE_CURRENT_LOGGING_FILE_WHEN = "关闭时无法关闭当前日志文件";
  public static final String FAIL_TO_REGISTER_DELETIONRESOURCE_INTO_DELETIONBUFFER_BECAUSE =
      "向 deletionBuffer-{} 注册 DeletionResource 失败，原因：该缓冲区已关闭。";
  public static final String INTERRUPTED_WHEN_WAITING_FOR_ALL_DELETIONS_FLUSHED = "等待所有删除操作刷盘时被中断。";
  public static final String INTERRUPTED_WHEN_WAITING_FOR_RESULT = "等待结果时被中断。";
  public static final String INTERRUPTED_WHEN_WAITING_FOR_TAKING_DELETIONRESOURCE_FROM =
      "等待从阻塞队列中取出 DeletionResource 进行序列化时被中断。";
  public static final String INTERRUPTED_WHEN_WAITING_FOR_TAKING_WALENTRY_FROM =
      "等待从阻塞队列中取出 WALEntry 进行序列化时被中断。";
  public static final String INVALID_DELETION_PROGRESS_INDEX = "无效的删除进度索引：";
  public static final String PERSISTTHREAD_DID_NOT_TERMINATE_WITHIN_S = "persistThread 在 {} 秒内未终止";
  public static final String READ_DELETION_FILE_MAGIC_VERSION =
      "读取删除文件-{} 的 magic version：{}";
  public static final String READ_DELETION_FROM_FILE = "从文件 {} 读取删除操作：{}";
  public static final String UNABLE_TO_CREATE_IOTCONSENSUSV2_DELETION_DIR_AT =
      "无法在 {} 创建 IoTConsensusV2 删除目录";

  // ===================== AGENT =====================

  public static final String ATTEMPT_TO_REPORT_PIPE_EXCEPTION_TO_A =
      "尝试向空 PipeTaskMeta 上报 pipe 异常。";
  public static final String CANNOT_PARSE_REBOOT_TIMES_FROM_FILE_SET =
      "无法从文件 {} 解析重启次数，将当前秒级时间戳（{}）设为重启次数";
  public static final String CANNOT_RECORD_REBOOT_TIMES_TO_FILE_THE =
      "无法将重启次数 {} 记录到文件 {}，重启次数将不会更新";
  public static final String CANNOT_START_SIMPLEPROGRESSINDEXASSIGNER_BECAUSE_OF =
      "无法启动 SimpleProgressIndexAssigner，原因：{}";
  public static final String CREATE_PIPE_DN_TASK_SUCCESSFULLY_WITHIN_MS =
      "成功创建 pipe DataNode 任务 {}，耗时 {} ms";
  public static final String DEREGISTER_SUBTASK_RUNNINGTASKCOUNT_REGISTEREDTASKCOUNT =
      "注销子任务 {}。runningTaskCount：{}，registeredTaskCount：{}";
  public static final String DROP_PIPE_DN_TASK_SUCCESSFULLY_WITHIN_MS =
      "成功删除 pipe DataNode 任务 {}，耗时 {} ms";
  public static final String ERROR_OCCURRED_WHEN_COLLECTING_EVENTS_FROM_PROCESSOR =
      "从 processor 收集事件时发生错误";
  public static final String EXCEPTION_IN_PIPE_EVENT_PROCESSING_IGNORED_BECAUSE =
      "pipe 事件处理时发生异常，因 pipe 已被删除而忽略。{}";
  public static final String EXCEPTION_OCCURRED_WHEN_CLOSING_PIPE_CONNECTOR_SUBTASK =
      "关闭 pipe connector 子任务 {} 时发生异常，根因：{}";
  public static final String EXCEPTION_OCCURRED_WHEN_CLOSING_PIPE_PROCESSOR_SUBTASK =
      "关闭 pipe processor 子任务 {} 时发生异常，根因：{}";
  public static final String EXCEPTION_OCCURS_WHEN_EXECUTING_PIPE_TASK =
      "执行 pipe 任务时发生异常：";
  public static final String FAILED_TO_CHECK_IF_PIPE_HAS_RELEASE =
      "检查 pipe 是否已释放共识组 {} 的 region 相关资源失败。";
  public static final String FAILED_TO_CLEAR_CLOSE_THE_SCHEMA_REGION =
      "清理/关闭 schema region 监听队列失败，原因：{}。将等待直到成功或该 region 的状态机停止。";
  public static final String FAILED_TO_CLOSE_CONNECTOR_AFTER_FAILED_TO =
      "初始化 connector 失败后关闭 connector 失败。忽略此异常。";
  public static final String FAILED_TO_CLOSE_LISTENING_QUEUE_FOR_SCHEMAREGION =
      "关闭 SchemaRegion 的监听队列失败";
  public static final String FAILED_TO_CLOSE_SOURCE_AFTER_FAILED_TO =
      "初始化 source 失败后关闭 source 失败。忽略此异常。";
  public static final String FAILED_TO_CONSTRUCT_PIPECONNECTOR_BECAUSE_OF =
      "构造 PipeConnector 失败，原因：";
  public static final String FAILED_TO_DECREASE_REFERENCE_COUNT_FOR_EVENT =
      "减少 PipeRealtimePriorityBlockingQueue 中事件 {} 的引用计数失败";
  public static final String FAILED_TO_GET_PENDINGQUEUE_NO_SUCH_SUBTASK =
      "获取 PendingQueue 失败，不存在该子任务：";
  public static final String FAILED_TO_GET_PIPE_METAS_WILL_BE =
      "获取 pipe 元数据失败，稍后将由 ConfigNode 同步。";
  public static final String FAILED_TO_GET_PIPE_PLUGIN_JAR_FROM =
      "从 ConfigNode 获取 pipe 插件 jar 失败。";
  public static final String FAILED_TO_GET_PIPE_TASK_META_FROM =
      "从 ConfigNode 获取 pipe 任务元数据失败。忽略此异常，原因：ConfigNode 可能尚未就绪，元数据稍后将由 ConfigNode 推送。";
  public static final String FAILED_TO_PERSIST_PROGRESS_INDEX_TO_CONFIGNODE =
      "向 ConfigNode 持久化进度索引失败，状态：{}";
  public static final String FAILURE_WHEN_REGISTER_PIPE_PLUGIN_SKIP_THIS =
      "注册 pipe 插件 {} 失败。跳过该插件并继续启动。";
  public static final String PIPECONNECTOR = "PipeConnector: ";
  public static final String PIPEDATANODETASKBUILDER_FAILED_TO_PARSE_INCLUSION_AND_EXCLUSION =
      "PipeDataNodeTaskBuilder 解析 'inclusion' 和 'exclusion' 参数失败：{}";
  public static final String PIPEDATANODETASKBUILDER_WHEN_INCLUSION_CONTAINS_DATA_DELETE_REALTIME =
      "PipeDataNodeTaskBuilder：当 'inclusion' 包含 'data.delete' 时，'realtime-first' 默认设为 "
          + "'false'，以避免删除后的同步问题。";
  public static final String PIPEDATANODETASKBUILDER_WHEN_INCLUSION_INCLUDES_DATA_DELETE_REALTIME =
      "PipeDataNodeTaskBuilder：当 'inclusion' 包含 'data.delete' 时，将 'realtime-first' 设为 "
          + "'true' 可能导致删除后的数据同步问题。";
  public static final String PIPEDATANODETASKBUILDER_WHEN_SOURCE_USES_SNAPSHOT_MODEL_REALTIME =
      "PipeDataNodeTaskBuilder：当 source 使用快照模式时，'realtime-first' 默认设为 'false'，"
          + "以避免传输完成前过早停止。";
  public static final String PIPEDATANODETASKBUILDER_WHEN_SOURCE_USES_SNAPSHOT_MODEL_REALTIME_1 =
      "PipeDataNodeTaskBuilder：当 source 使用快照模式时，将 'realtime-first' 设为 'true' "
          + "可能导致传输完成前过早停止。";
  public static final String PIPEDATANODETASKBUILDER_WHEN_THE_REALTIME_SYNC_IS_ENABLED =
      "PipeDataNodeTaskBuilder：启用实时同步时，如果发送 TsFile 时不启用限流，可能会导致实时发送延迟。";
  public static final String PIPEDATANODETASKBUILDER_WHEN_THE_REALTIME_SYNC_IS_ENABLED_1 =
      "PipeDataNodeTaskBuilder：启用实时同步时，默认在发送 TsFile 时启用限流，为实时发送预留磁盘和网络 IO。";
  public static final String PIPEEVENTCOLLECTOR_THE_EVENT_IS_ALREADY_RELEASED_SKIPPING =
      "PipeEventCollector：事件 {} 已释放，跳过该事件。";
  public static final String PIPE_CONNECTOR_SUBTASK_WAS_CLOSED_WITHIN_MS =
      "Pipe：connector 子任务 {}（{}）已在 {} ms 内关闭";
  public static final String PIPE_META_NOT_FOUND = "未找到 Pipe 元数据：";
  public static final String PIPE_SINK_SUBTASKS_WITH_ATTRIBUTES_IS_BOUNDED =
      "属性为 {} 的 Pipe sink 子任务已绑定到 sinkExecutor {} 和 callbackExecutor {}。";
  public static final String PIPE_SKIPPING_TEMPORARY_TSFILE_WHICH_SHOULDN_T =
      "Pipe 跳过不应传输的临时 TsFile：{}";
  public static final String PULLED_PIPE_META_FROM_CONFIG_NODE_RECOVERING =
      "已从 ConfigNode 拉取 pipe 元数据：{}，正在恢复...";
  public static final String RECEIVED_PIPE_HEARTBEAT_REQUEST_FROM_CONFIG_NODE =
      "收到来自 ConfigNode 的 pipe 心跳请求 {}。";
  public static final String REGION_NO_TSFILEINSERTIONEVENTS_TO_REPLACE_FOR_SOURCE =
      "Region {}：源文件 {} 没有需要替换的 TsFileInsertionEvent";
  public static final String REGION_REPLACED_TSFILEINSERTIONEVENTS_WITH =
      "Region {}：已将 TsFileInsertionEvent {} 替换为 {}";
  public static final String REGISTEREDTASKCOUNT_0 = "registeredTaskCount < 0";
  public static final String REGISTEREDTASKCOUNT_0_1 = "registeredTaskCount <= 0";
  public static final String REGISTER_SUBTASK_RUNNINGTASKCOUNT_REGISTEREDTASKCOUNT =
      "注册子任务 {}。runningTaskCount：{}，registeredTaskCount：{}";
  public static final String REPORT_PIPERUNTIMEEXCEPTION_TO_LOCAL_PIPETASKMETA_EXCEPTION_MESSAGE =
      "向本地 PipeTaskMeta({}) 上报 PipeRuntimeException，异常消息：{}";
  public static final String RUNNINGTASKCOUNT_0 = "runningTaskCount < 0";
  public static final String RUNNINGTASKCOUNT_0_1 = "runningTaskCount <= 0";
  public static final String SIMPLEPROGRESSINDEXASSIGNER_STARTED_SUCCESSFULLY_ISSIMPLECONSENSUSENABLE_R =
      "SimpleProgressIndexAssigner 启动成功。isSimpleConsensusEnable：{}，rebootTimes：{}";
  public static final String STARTING_SIMPLEPROGRESSINDEXASSIGNER =
      "正在启动 SimpleProgressIndexAssigner ...";
  public static final String START_PIPE_DN_TASK_SUCCESSFULLY_WITHIN_MS =
      "成功启动 pipe DataNode 任务 {}，耗时 {} ms";
  public static final String START_SUBTASK_RUNNINGTASKCOUNT_REGISTEREDTASKCOUNT =
      "启动子任务 {}。runningTaskCount：{}，registeredTaskCount：{}";
  public static final String STOP_PIPE_DN_TASK_SUCCESSFULLY_WITHIN_MS =
      "成功停止 pipe DataNode 任务 {}，耗时 {} ms";
  public static final String STOP_SUBTASK_RUNNINGTASKCOUNT_REGISTEREDTASKCOUNT =
      "停止子任务 {}。runningTaskCount：{}，registeredTaskCount：{}";
  public static final String SUBTASK_IS_CLOSED_IGNORE_EXCEPTION =
      "子任务 {} 已关闭，忽略异常";
  public static final String SUBTASK_WORKER_IS_INTERRUPTED = "子任务 worker 被中断";
  public static final String SUCCESSFULLY_PERSISTED_ALL_PIPE_S_INFO_TO =
      "已成功将所有 pipe 信息持久化到 ConfigNode。";
  public static final String THE_EXECUTOR_AND_HAS_BEEN_SUCCESSFULLY_SHUTDOWN =
      "执行器 {} 和 {} 已成功关闭。";

  // ===================== EVENT =====================

  public static final String DATABASENAMEFROMDATAREGION_IS_NULL = "databaseNameFromDataRegion 为空";
  public static final String DECREASE_REFERENCE_COUNT_ERROR = "减少引用计数出错。";
  public static final String DECREASE_REFERENCE_COUNT_FOR_MTREE_SNAPSHOT_OR =
      "减少 mTree 快照 {}、tLog {} 或属性快照 {} 的引用计数出错。";
  public static final String DECREASE_REFERENCE_COUNT_FOR_TSFILE_ERROR =
      "减少 TsFile {} 的引用计数出错。";
  public static final String DO_NOT_HAS_A_COMPLETE_PAGE_BODY =
      "没有完整的 page body。期望：";
  public static final String ERROR_WHILE_PARSING_TSFILE_INSERTION_EVENT =
      "解析 TsFile 插入事件时出错";
  public static final String EXCEPTION_OCCURRED_WHEN_DETERMINING_THE_EVENT_TIME =
      "判断 PipeInsertNodeTabletInsertionEvent({}) 的事件时间是否与时间范围 [{}, {}] 重叠时发生异常。"
          + "为保证数据完整性，将返回 true";
  public static final String FAILED_TO_ALLOCATE_MEMORY_FOR_PARSING_TSFILE =
      "{}：为解析 TsFile {} 分配内存失败，tablet event 序号 {}，重试次数 {}，将继续重试。";
  public static final String FAILED_TO_BUILD_TABLET = "构建 tablet 失败";
  public static final String FAILED_TO_CHECK_NEXT = "检查 next 失败";
  public static final String FAILED_TO_CLOSE_TSFILEREADER = "关闭 TsFileReader 失败";
  public static final String FAILED_TO_CLOSE_TSFILESEQUENCEREADER = "关闭 TsFileSequenceReader 失败";
  public static final String FAILED_TO_CREATE_TSFILEINSERTIONDATATABLETITERATOR =
      "创建 TsFileInsertionDataTabletIterator 失败";
  public static final String FAILED_TO_GET_NEXT_TABLET_INSERTION_EVENT =
      "获取下一个 tablet 插入事件失败。";
  public static final String FAILED_TO_LOAD_MODIFICATIONS_FROM_TSFILE =
      "从 TsFile 加载 modifications 失败：";
  public static final String FAILED_TO_READ_METADATA_FOR_DEVICEID_MEASUREMENT =
      "读取 deviceId：{}、measurement：{} 的元数据失败，正在移除";
  public static final String FAILED_TO_RECORD_PARSE_END_TIME_FOR =
      "记录 pipe {} 的解析结束时间失败";
  public static final String FAILED_TO_RECORD_TABLET_METRICS_FOR_PIPE =
      "记录 pipe {} 的 tablet 指标失败";
  public static final String FOUND_NULL_DEVICEID_REMOVING_ENTRY = "发现空 deviceId，正在移除条目";
  public static final String INITIALIZE_DATA_CONTAINER_ERROR = "初始化数据容器出错。";
  public static final String INSERTNODE_HAS_BEEN_RELEASED = "InsertNode 已被释放";
  public static final String INSERTROWNODE_IS_PARSED_TO_ZERO_ROWS_ACCORDING =
      "InsertRowNode({}) 根据 pattern({}) 和时间范围 [{}, {}] 被解析为 0 行，对应的源事件 ({}) 将被忽略。";
  public static final String INSERTTABLETNODE_IS_PARSED_TO_ZERO_ROWS_ACCORDING =
      "InsertTabletNode({}) 根据 pattern({}) 和时间范围 [{}, {}] 被解析为 0 行，对应的源事件 ({}) 将被忽略。";
  public static final String INVALID_EVENT_TYPE = "无效的事件类型：";
  public static final String INVALID_INPUT = "无效的输入：";
  public static final String ISGENERATEDBYPIPE_IS_NOT_SUPPORTED =
      "不支持 isGeneratedByPipe()！";
  public static final String MAYEVENTPATHSOVERLAPPEDWITHPATTERN_IS_NOT_SUPPORTED =
      "不支持 mayEventPathsOverlappedWithPattern()！";
  public static final String MAYEVENTTIMEOVERLAPPEDWITHTIMERANGE_IS_NOT_SUPPORTED =
      "不支持 mayEventTimeOverlappedWithTimeRange()！";
  public static final String NO_COMMIT_IDS_FOUND_IN_PIPECOMPACTEDTSFILEINSERTIONEVENT =
      "PipeCompactedTsFileInsertionEvent 中未找到 commit ID。";
  public static final String PIPECOMPACTEDTSFILEINSERTIONEVENT_DOES_NOT_SUPPORT_EQUALSINIOTCONSENSUSV2 =
      "PipeCompactedTsFileInsertionEvent 不支持 equalsInIoTConsensusV2.";
  public static final String PIPECOMPACTEDTSFILEINSERTIONEVENT_DOES_NOT_SUPPORT_GETREBOOTTIMES =
      "PipeCompactedTsFileInsertionEvent 不支持 getRebootTimes.";
  public static final String PIPE_FAILED_TO_GET_DEVICES_FROM_TSFILE =
      "Pipe {}：从 TsFile {} 获取 device 失败，仍将继续提取";
  public static final String PIPE_SKIPPING_TEMPORARY_TSFILE_S_PARSING_WHICH =
      "Pipe 跳过不应传输的临时 TsFile 解析：{}";
  public static final String ROW_CAN_NOT_BE_CUSTOMIZED = "Row 不能被自定义";
  public static final String SHALLOWCOPYSELFANDBINDPIPETASKMETAFORPROGRESSREPORT_IS_NOT_SUPPORTED =
      "不支持 shallowCopySelfAndBindPipeTaskMetaForProgressReport()！";
  public static final String SKIPPING_TEMPORARY_TSFILE_S_PROGRESSINDEX_WILL_REPORT =
      "跳过临时 TsFile {} 的 progressIndex，将上报 MinimumProgressIndex";
  public static final String TABLEPATTERNPARSER_DOES_NOT_SUPPORT_ROW_BY_ROW =
      "TablePatternParser 不支持逐行处理";
  public static final String TABLEPATTERNPARSER_DOES_NOT_SUPPORT_TABLET_PROCESSING =
      "TablePatternParser 不支持 tablet processing";
  public static final String TABLEPATTERNPARSER_DOES_NOT_SUPPORT_TABLET_PROCESSING_WITH =
      "TablePatternParser 不支持带 collect 的 tablet processing";
  public static final String TABLET_IS_PARSED_TO_ZERO_ROWS_ACCORDING =
      "Tablet({}) 根据 pattern({}) 和时间范围 [{}, {}] 被解析为 0 行，对应的源事件 ({}) 将被忽略。";
  public static final String TABLE_MODEL_TSFILE_PARSING_DOES_NOT_SUPPORT =
      "表模型 TsFile 解析不支持此类 ChunkMeta";
  public static final String TEMPORARY_TSFILE_DETECTED_WILL_SKIP_ITS_TRANSFER =
      "检测到临时 TsFile {}，将跳过传输。";
  public static final String TSFILE_HAS_INITIALIZED_PIPENAME_CREATION_TIME_PATTERN =
      "TsFile {} 已初始化 {}，pipeName：{}，创建时间：{}，pattern：{}，startTime：{}，endTime：{}，withMod：{}";
  public static final String UNCOMPRESS_ERROR_UNCOMPRESS_SIZE =
      "解压出错！解压大小：";
  public static final String UNSUPPORTED = "不支持";
  public static final String UNSUPPORTED_NODE_TYPE = "不支持的节点类型 ";
  public static final String WAIT_FOR_MEMORY_ENOUGH_FOR_PARSING_FOR =
      "等待内存充足以解析 {}，已等待 {} 秒。";

  // ===================== PROCESSOR =====================

  public static final String ABSTRACTSAMETYPENUMERICOPERATOR_DOES_NOT_SUPPORT_BINARY_INPUT =
      "AbstractSameTypeNumericOperator 不支持 binary 输入";
  public static final String ABSTRACTSAMETYPENUMERICOPERATOR_DOES_NOT_SUPPORT_BOOLEAN_INPUT =
      "AbstractSameTypeNumericOperator 不支持 boolean 输入";
  public static final String ABSTRACTSAMETYPENUMERICOPERATOR_DOES_NOT_SUPPORT_DATE_INPUT =
      "AbstractSameTypeNumericOperator 不支持 date 输入";
  public static final String ABSTRACTSAMETYPENUMERICOPERATOR_DOES_NOT_SUPPORT_STRING_INPUT =
      "AbstractSameTypeNumericOperator 不支持 string 输入";
  public static final String CHANGINGVALUESAMPLINGPROCESSOR_IN_IS_INITIALIZED_WITH =
      "{} 中的 ChangingValueSamplingProcessor 已初始化，参数为 {}：{}，{}：{}，{}：{}。";
  public static final String CLEAN_OUTDATED_INCOMPLETE_COMBINER_PIPENAME_CREATIONTIME_COMBINEID =
      "清理过期且未完成的 combiner：pipeName={}，creationTime={}，combineId={}";
  public static final String COMBINEHANDLER_NOT_FOUND_FOR_PIPEID =
      "未找到 CombineHandler，pipeId = ";
  public static final String COMBINER_COMBINE_COMPLETED_REGIONID_STATE_RECEIVEDREGIONIDSET_EX =
      "Combiner combine 已完成：regionId：{}，state：{}，receivedRegionIdSet：{}，expectedRegionIdSet：{}";
  public static final String COMBINER_COMBINE_REGIONID_STATE_RECEIVEDREGIONIDSET_EXPECTEDREGI =
      "Combiner combine：regionId：{}，state：{}，receivedRegionIdSet：{}，expectedRegionIdSet：{}";
  public static final String DATA_NODES_ENDPOINTS_FOR_TWO_STAGE_AGGREGATION =
      "两阶段聚合的 DataNode 端点：{}";
  public static final String DIFFERENT_DATA_TYPE_ENCOUNTERED_IN_ONE_WINDOW =
      "一个窗口中遇到不同数据类型，将清除该窗口。之前类型：{}，当前类型：{}";
  public static final String ENCOUNTERED_EXCEPTION_WHEN_DESERIALIZING_FROM_PIPETASKMETA =
      "从 PipeTaskMeta 反序列化时遇到异常";
  public static final String END_POINTS_FOR_TWO_STAGE_AGGREGATION_PIPE =
      "两阶段聚合 pipe（pipeName={}，creationTime={}）的端点已更新为 {}";
  public static final String ERROR_OCCURRED_WHEN_CLOSING_COMBINEHANDLER_ID =
      "closing CombineHandler(id = {}) 时发生错误";
  public static final String ERROR_OCCURS_WHEN_RECEIVING_REQUEST = "接收请求 {} 时发生错误";
  public static final String FAILED_TO_CLOSE_IOTDBSYNCCLIENT = "关闭 IoTDBSyncClient 失败";
  public static final String FAILED_TO_CLOSE_OLD_IOTDBSYNCCLIENT = "关闭旧 IoTDBSyncClient 失败";
  public static final String FAILED_TO_COMBINE_COUNT = "合并 count 失败：";
  public static final String FAILED_TO_CONSTRUCT_IOTDBSYNCCLIENT = "构造 IoTDBSyncClient 失败";
  public static final String FAILED_TO_FETCH_COMBINE_RESULT = "获取 combine 结果失败：";
  public static final String FAILED_TO_FETCH_DATA_NODES = "获取 DataNode 失败";
  public static final String FAILED_TO_FETCH_DATA_REGION_IDS = "获取 data region ID 失败";
  public static final String FAILED_TO_RECONSTRUCT_IOTDBSYNCCLIENT_AFTER_FAILURE_TO =
      "重建 IoTDBSyncClient {} 失败，之前发送请求 {}（watermark = {}）失败";
  public static final String FAILED_TO_SEND_REQUEST_WATERMARK_TO =
      "发送请求 {}（watermark = {}）到 {} 失败";
  public static final String FAILED_TO_TRIGGER_COMBINE_WATERMARK_COUNT_PROGRESSINDEX =
      "触发 combine 失败。watermark={}，count={}，progressIndex={}";
  public static final String FAILURE_OCCURRED_WHEN_TRYING_TO_COMMIT_PROGRESS =
      "提交进度索引时失败。timestamp={}，count={}，progressIndex={}";
  public static final String FETCHED_DATA_REGION_IDS_AT = "已获取 data region ID {}，时间：{}";
  public static final String FRACTIONPOWEREDSUMOPERATOR_DOES_NOT_SUPPORT_BINARY_INPUT =
      "FractionPoweredSumOperator 不支持 binary 输入";
  public static final String FRACTIONPOWEREDSUMOPERATOR_DOES_NOT_SUPPORT_BOOLEAN_INPUT =
      "FractionPoweredSumOperator 不支持 boolean 输入";
  public static final String FRACTIONPOWEREDSUMOPERATOR_DOES_NOT_SUPPORT_DATE_INPUT =
      "FractionPoweredSumOperator 不支持 date 输入";
  public static final String FRACTIONPOWEREDSUMOPERATOR_DOES_NOT_SUPPORT_STRING_INPUT =
      "FractionPoweredSumOperator 不支持 string 输入";
  public static final String GLOBAL_COUNT_IS_LESS_THAN_THE_LAST =
      "全局 count 小于上次收集的 count：timestamp={}，count={}";
  public static final String IGNORED_TABLETINSERTIONEVENT_IS_NOT_AN_INSTANCE_OF =
      "已忽略 TabletInsertionEvent，因为它不是 PipeInsertNodeTabletInsertionEvent 或 "
          + "PipeRawTabletInsertionEvent 的实例：{}";
  public static final String IGNORED_TSFILEINSERTIONEVENT_IS_EMPTY =
      "已忽略空 TsFileInsertionEvent：{}";
  public static final String IGNORED_TSFILEINSERTIONEVENT_IS_NOT_AN_INSTANCE_OF =
      "已忽略 TsFileInsertionEvent，因为它不是 PipeTsFileInsertionEvent 的实例：{}";
  public static final String ILLEGAL_OUTPUT_SERIES_PATH = "非法的输出序列路径：";
  public static final String NO_DATA_NODES_ENDPOINTS_FETCHED = "未获取到 DataNode 端点";
  public static final String NO_EXPECTED_REGION_ID_SET_FETCHED =
      "未获取到期望的 region ID 集合";
  public static final String PARTIALPATHLASTOBJECTCACHE_ALLOCATEDMEMORYBLOCK_HAS_EXPANDED_FROM_TO =
      "PartialPathLastObjectCache.allocatedMemoryBlock 已从 {} 扩展到 {}。";
  public static final String PARTIALPATHLASTOBJECTCACHE_ALLOCATEDMEMORYBLOCK_HAS_SHRUNK_FROM_TO =
      "PartialPathLastObjectCache.allocatedMemoryBlock 已从 {} 收缩到 {}。";
  public static final String SENDING_REQUEST_WATERMARK_TO = "正在发送请求 {}（watermark = {}）到 {}";
  public static final String SWINGINGDOORTRENDINGSAMPLINGPROCESSOR_IN_IS_INITIALIZED_WITH =
      "{} 中的 SwingingDoorTrendingSamplingProcessor 已初始化，参数为 {}：{}，{}：{}，{}：{}。";
  public static final String THE_ABSTRACT_FORMAL_PROCESSOR_DOES_NOT_SUPPORT = "抽象形式处理器不支持处理事件";
  public static final String TUMBLINGTIMESAMPLINGPROCESSOR_IN_IS_INITIALIZED_WITH_S =
      "{} 中的 TumblingTimeSamplingProcessor 已初始化，参数为 {}：{}s，{}：{}，{}：{}。";
  public static final String TWOSTAGECOUNTPROCESSOR_CUSTOMIZED_BY_THREAD_PIPENAME_CREATIONTIME_RE =
      "TwoStageCountProcessor 已由线程 {} 自定义：pipeName={}，creationTime={}，"
          + "regionId={}，outputSeries={}，localCommitProgressIndex={}，localCount={}";
  public static final String TWO_STAGE_AGGREGATE_PIPE_PIPENAME_CREATIONTIME_RELATED =
      "两阶段聚合 pipe（pipeName={}，creationTime={}）相关的 region ID：{}";
  public static final String TWO_STAGE_AGGREGATE_RECEIVER_IS_EXITING =
      "两阶段聚合接收器正在退出。";
  public static final String TWO_STAGE_COMBINE_REGION_ID_COMBINE_ID =
      "两阶段 combine（region id = {}，combine id = {}）未完成：timestamp={}，count={}，progressIndex={}";
  public static final String TWO_STAGE_COMBINE_REGION_ID_COMBINE_ID_1 =
      "两阶段 combine（region id = {}，combine id = {}）已过期：timestamp={}，count={}，progressIndex={}";
  public static final String TWO_STAGE_COMBINE_REGION_ID_COMBINE_ID_2 =
      "两阶段 combine（region id = {}，combine id = {}）成功：timestamp={}，count={}，"
          + "progressIndex={}，已提交 progressIndex={}";
  public static final String UNEXPECTED_STATE_CLASS = "意外的状态类：";
  public static final String UNKNOWN_COMBINE_RESULT_TYPE = "未知的 combine 结果类型：";
  public static final String UNKNOWN_REQUEST_TYPE = "未知的请求类型 {}：{}。";

  // ===================== SOURCE =====================

  public static final String ALL_DATA_IN_TSFILEEPOCH_WAS_EXTRACTED =
      "TsFileEpoch {} 中的所有数据均已提取";
  public static final String BUFFERSIZE_MUST_BE_A_POWER_OF_2 = "bufferSize 必须是 2 的幂";
  public static final String BUFFERSIZE_MUST_NOT_BE_LESS_THAN_1 =
      "bufferSize 不能小于 1";
  public static final String CAPTURE_TREE_AND_CAPTURE_TABLE_CAN_NOT =
      "capture.tree 和 capture.table 不能同时设为 false";
  public static final String DATABASE_NAME_IS_NULL_WHEN_MATCHING_SOURCES =
      "匹配表模型事件的 source 时数据库名为空。";
  public static final String DATA_REGION_INJECTED_WATERMARK_EVENT_WITH_TIMESTAMP =
      "Data region {}：已注入时间戳为 {} 的水印事件";
  public static final String DISCARD_TABLET_EVENT_BECAUSE_IT_IS_NOT =
      "丢弃 tablet 事件 {}，因为该事件已不再可靠。将 TsFileEpoch 的状态改为 USING_BOTH。";
  public static final String DISRUPTOR_ALREADY_STARTED = "Disruptor 已启动";
  public static final String DISRUPTOR_SHUTDOWN_COMPLETED = "Disruptor 关闭完成";
  public static final String DISRUPTOR_STARTED_WITH_BUFFER_SIZE = "Disruptor 已启动，缓冲区大小：{}";
  public static final String EXCEPTION_DURING_ONSHUTDOWN = "onShutdown() 期间发生异常";
  public static final String EXCEPTION_DURING_ONSTART = "onStart() 期间发生异常";
  public static final String EXCEPTION_ENCOUNTERED_WHEN_TRIGGERING_SCHEMA_REGION_SNAPSHOT =
      "触发 schema region 快照时遇到异常。";
  public static final String EXCEPTION_PROCESSING = "处理时发生异常：{} {}";
  public static final String FAILED_TO_LOAD_SNAPSHOT = "加载快照 {} 失败";
  public static final String FAILED_TO_LOAD_SNAPSHOT_FROM_BYTEBUFFER =
      "从 ByteBuffer {} 加载快照失败。";
  public static final String FAILED_TO_START_SOURCES = "启动 source 失败。";
  public static final String HEARTBEAT_EVENT_CAN_NOT_BE_SUPPLIED_BECAUSE =
      "无法提供心跳事件 {}，因为无法增加引用计数";
  public static final String INTERRUPTED_WAITING_FOR_PROCESSOR_TO_STOP =
      "等待 processor 停止时被中断";
  public static final String IOTDBSCHEMAREGIONSOURCE_DOES_NOT_SUPPORT_TRANSFERRING_EVENTS_UNDER =
      "IoTDBSchemaRegionSource 不支持在 simple consensus 下传输事件";
  public static final String NOT_HAS_PRIVILEGE_TO_TRANSFER_EVENT = "没有权限传输事件：";
  public static final String NOT_HAS_PRIVILEGE_TO_TRANSFER_PLAN = "没有权限传输计划：";
  public static final String NO_EVENT_HANDLER_CONFIGURED = "未配置事件处理器";
  public static final String N_MUST_BE_0 = "n 必须大于 0";
  public static final String PIPEREALTIMEDATAREGIONEXTRACTOR_OBSERVED_DATA_REGION_TIME_PARTITION_GROWT =
      "PipeRealtimeDataRegionExtractor({}) 观察到 data region {} 的时间分区增长，记录时间分区 ID 边界：{}。";
  public static final String PIPE_AND_IS_NOT_SET_USE_HYBRID =
      "Pipe：未设置 '{}'（'{}'）和 '{}'（'{}'），默认使用 hybrid 模式。";
  public static final String PIPE_ASSIGNER_ON_DATA_REGION_SHUTDOWN_INTERNAL =
      "Pipe：data region {} 上的 assigner 已在 {} ms 内关闭内部 disruptor";
  public static final String PIPE_FAILED_TO_GET_DEVICES_FROM_TSFILE_1 =
      "Pipe {}@{}：从 TsFile {} 获取 device 失败，仍将继续提取";
  public static final String PIPE_FAILED_TO_INCREASE_REFERENCE_COUNT_FOR =
      "Pipe {}@{}：增加历史删除事件 {} 的引用计数失败，将丢弃该事件";
  public static final String PIPE_FAILED_TO_INCREASE_REFERENCE_COUNT_FOR_1 =
      "Pipe {}@{}：增加历史 TsFile 事件 {} 的引用计数失败，将丢弃该事件";
  public static final String PIPE_FAILED_TO_INCREASE_REFERENCE_COUNT_FOR_2 =
      "Pipe {}@{}：增加终止事件的引用计数失败，将重新发送该事件";
  public static final String PIPE_FAILED_TO_PIN_TSFILERESOURCE = "Pipe：固定 TsFileResource {} 失败";
  public static final String PIPE_FAILED_TO_START_TO_EXTRACT_HISTORICAL =
      "Pipe {}@{}：启动历史 TsFile 提取失败，存储引擎尚未就绪。稍后将重试。";
  public static final String PIPE_FAILED_TO_UNPIN_SKIPPED_HISTORICAL_TSFILERESOURCE =
      "Pipe {}@{}：取消固定已跳过的历史 TsFileResource 失败，原始路径：{}";
  public static final String PIPE_FAILED_TO_UNPIN_TSFILERESOURCE_AFTER_CREATING =
      "Pipe {}@{}：创建事件后取消固定 TsFileResource 失败，原始路径：{}";
  public static final String PIPE_FAILED_TO_UNPIN_TSFILERESOURCE_AFTER_DROPPING =
      "Pipe {}@{}：删除 pipe 后取消固定 TsFileResource 失败，原始路径：{}";
  public static final String PIPE_FINISH_TO_EXTRACT_DELETIONS_EXTRACT_DELETIONS =
      "Pipe {}@{}：删除操作提取完成，已提取删除操作数 {}/{}，耗时 {} ms";
  public static final String PIPE_FINISH_TO_EXTRACT_HISTORICAL_TSFILE_EXTRACTED =
      "Pipe {}@{}：历史 TsFile 提取完成，已提取顺序文件数 {}/{}，已提取乱序文件数 {}/{}，"
          + "已提取文件总数 {}/{}，耗时 {} ms";
  public static final String PIPE_FINISH_TO_SORT_ALL_EXTRACTED_RESOURCES =
      "Pipe {}@{}：所有已提取资源排序完成，耗时 {} ms";
  public static final String PIPE_HISTORICAL_DATA_EXTRACTION_TIME_RANGE_START =
      "Pipe {}@{}：历史数据提取时间范围，开始时间 {}({})，结束时间 {}({})，sloppy pattern {}，"
          + "sloppy time range {}，是否传输 mod 文件 {}，用户名：{}，无权限时是否跳过：{}，"
          + "是否转发 pipe 请求：{}";
  public static final String PIPE_IS_SET_TO_FALSE_USE_HEARTBEAT =
      "Pipe：'{}'（'{}'）设为 false，使用 heartbeat realtime source。";
  public static final String PIPE_ON_DATA_REGION_SKIP_COMMIT_OF =
      "data region {} 上的 Pipe {} 跳过事件 {} 的提交，因为该事件过早刷盘。";
  public static final String PIPE_REALTIME_DATA_REGION_SOURCE_IS_INITIALIZED =
      "Pipe {}@{}：实时 data region source 已初始化，参数：{}。";
  public static final String PIPE_RESOURCE_MEETS_MAYTSFILECONTAINUNPROCESSEDDATA_CONDITION_EXTRACT =
      "Pipe {}@{}：资源 {} 满足 mayTsFileContainUnprocessedData 条件，extractor progressIndex：{}，"
          + "resource ProgressIndex：{}";
  public static final String PIPE_SET_WATERMARK_INJECTOR_WITH_INTERVAL_MS =
      "Pipe {}@{}：设置水印注入器，间隔 {} ms。";
  public static final String PIPE_SKIP_HISTORICAL_TSFILE_BECAUSE_REALTIME_SOURCE =
      "Pipe {}@{}：跳过历史 TsFile {}，因为当前任务 {} 中的 realtime source 已捕获该文件。";
  public static final String PIPE_SNAPSHOT_MODE_IS_ENABLED_USE_HEARTBEAT =
      "Pipe：快照模式已启用，使用 heartbeat realtime source。";
  public static final String PIPE_STARTED_HISTORICAL_SOURCE_AND_REALTIME_SOURCE =
      "Pipe {}@{}：已在 {} ms 内成功启动 historical source {} 和 realtime source {}。";
  public static final String PIPE_STARTING_HISTORICAL_SOURCE_AND_REALTIME_SOURCE =
      "Pipe {}@{}：正在启动 historical source {} 和 realtime source {}。";
  public static final String PIPE_START_HISTORICAL_SOURCE_AND_REALTIME_SOURCE =
      "Pipe {}@{}：启动 historical source {} 和 realtime source {} 出错。";
  public static final String PIPE_START_TO_EXTRACT_DELETIONS = "Pipe {}@{}：开始提取删除操作";
  public static final String PIPE_START_TO_EXTRACT_HISTORICAL_TSFILE_ORIGINAL =
      "Pipe {}@{}：开始提取历史 TsFile，原始顺序文件数 {}，原始乱序文件数 {}，起始进度索引 {}";
  public static final String PIPE_START_TO_FLUSH_DATA_REGION = "Pipe {}@{}：开始刷新 data region";
  public static final String PIPE_START_TO_SORT_ALL_EXTRACTED_RESOURCES =
      "Pipe {}@{}：开始排序所有已提取资源";
  public static final String PIPE_TASK_CANNOTUSETABLETANYMORE_FOR_TSFILE_THE_MEMORY =
      "Pipe 任务 {}@{} 对 TsFile {} 执行 canNotUseTabletAnyMore：InsertNode {} 的内存使用量已达到"
          + "单个 pipe 的危险阈值 {}，事件数：{}";
  public static final String PIPE_UNEXPECTED_PROGRESSINDEX_TYPE_FALLBACK_TO_ORIGIN =
      "Pipe {}@{}：意外的 ProgressIndex 类型 {}，回退到原始值 {}。";
  public static final String PIPE_UNSUPPORTED_SOURCE_REALTIME_MODE_CREATE_A =
      "Pipe：不支持的 source realtime mode：{}，将创建 hybrid source。";
  public static final String PROCESSOR_INTERRUPTED = "处理器被中断";
  public static final String PROCESSOR_STOPPED = "处理器已停止";
  public static final String SET_FOR_HISTORICAL_DELETION_EVENT =
      "[{}] 将 {} 设置到历史删除事件 {}";
  public static final String SET_FOR_HISTORICAL_EVENT = "[{}] 将 {} 设置到历史事件 {}";
  public static final String SET_FOR_REALTIME_EVENT = "[{}] 将 {} 设置到实时事件 {}";
  public static final String SOURCES_FILTERED_BY_DATABASE_AND_TABLE_IS =
      "匹配表模型事件的 source 时，按数据库和表过滤后的 source 为空。";
  public static final String SOURCES_FILTERED_BY_DEVICE_IS_NULL_WHEN =
      "匹配树模型事件的 source 时，按 device 过滤后的 source 为空。";
  public static final String TAKE_SNAPSHOT_ERROR = "创建快照出错：{}";
  public static final String THE_ASSIGNER_QUEUE_CONTENT_HAS_EXCEEDED_HALF =
      "assigner 队列内容已超过一半，可能发生阻塞并阻碍写入。regionId：{}，capacity：{}，bufferSize：{}";
  public static final String THE_PIPE_CANNOT_EXTRACT_TABLE_MODEL_DATA =
      "当 SQL 方言设为 tree 时，pipe 无法提取表模型数据。";
  public static final String THE_PIPE_CANNOT_EXTRACT_TREE_MODEL_DATA =
      "当 SQL 方言设为 table 时，pipe 无法提取树模型数据。";
  public static final String THE_PIPE_CANNOT_TRANSFER_DATA_WHEN_DATA =
      "当 data region 使用 Ratis 共识时，pipe 无法传输数据。";
  public static final String THE_REFERENCE_COUNT_OF_THE_EVENT_CANNOT =
      "无法增加事件 {} 的引用计数，跳过该事件。";
  public static final String THE_REFERENCE_COUNT_OF_THE_REALTIME_EVENT =
      "无法增加实时事件 {} 的引用计数，跳过该事件。";
  public static final String TIMED_OUT_WAITING_FOR_PROCESSOR_TO_STOP =
      "等待 processor 停止超时";
  public static final String TSFILEEPOCH_NOT_FOUND_FOR_TSFILE_CREATING_A =
      "未找到 TsFile {} 对应的 TsFileEpoch，正在创建新的 TsFileEpoch";
  public static final String WHEN_IS_SET_TO_FALSE_SPECIFYING_AND =
      "当 '{}'（'{}'）设为 false 时，指定 {} 和 {} 是无效的。";
  public static final String WHEN_IS_SET_TO_TRUE_SPECIFYING_AND =
      "当 '{}'（'{}'、'{}'、'{}'）设为 true 时，指定 {} 和 {} 是无效的。";
  public static final String WHEN_OR_IS_SPECIFIED_SPECIFYING_AND_IS =
      "当指定 {}、{}、{} 或 {} 时，再指定 {}、{}、{}、{}、{} 和 {} 是无效的。";

  // ===================== SINK =====================

  public static final String ACQUIRE_IOPCITEMMGT_SUCCESSFULLY_INTERFACE_ADDRESS =
      "成功获取 IOPCItemMgt！接口地址：{}";
  public static final String ACQUIRE_IOPCSYNCIO_SUCCESSFULLY_INTERFACE_ADDRESS =
      "成功获取 IOPCSyncIO！接口地址：{}";
  public static final String ADDED_EVENT_TO_RETRY_QUEUE = "已将事件 {} 添加到重试队列";
  public static final String BATCH_ID_CREATE_BATCH_DIR_SUCCESSFULLY_BATCH =
      "批次 id = {}：创建批次目录成功，批次文件目录 = {}。";
  public static final String BATCH_ID_DELETE_THE_TSFILE_AFTER_FAILED =
      "批次 id = {}：{} 删除 TsFile {}，此前向 {} 写入 tablet 失败。{}";
  public static final String BATCH_ID_FAILED_TO_BUILD_THE_TABLE =
      "批次 id = {}：构建表模型 TsFile 失败。请检查写入的 Tablet 是否存在时间重叠，以及表 Schema 是否正确。";
  public static final String BATCH_ID_FAILED_TO_CLOSE_THE_TSFILE =
      "批次 id = {}：写入 tablet 失败后关闭 TsFile {} 失败，原因：{}";
  public static final String BATCH_ID_FAILED_TO_CLOSE_THE_TSFILE_1 =
      "批次 id = {}：尝试关闭批次时关闭 TsFile {} 失败，原因：{}";
  public static final String BATCH_ID_FAILED_TO_CREATE_BATCH_FILE =
      "批次 id = {}：创建批次文件目录 {} 失败。";
  public static final String BATCH_ID_FAILED_TO_DELETE_THE_TSFILE =
      "批次 id = {}：尝试关闭批次时删除 TsFile {} 失败，原因：{}";
  public static final String BATCH_ID_FAILED_TO_WRITE_TABLETS_INTO =
      "批次 id = {}：向 TsFile 写入 tablet 失败，原因：{}";
  public static final String BATCH_ID_SEAL_TSFILE_SUCCESSFULLY = "批次 id = {}：成功封存 TsFile {}。";
  public static final String BATCH_ID_UNSUPPORTED_EVENT_TYPE_WHEN_CONSTRUCTING =
      "批次 id = {}：构造 TsFile 批次时不支持事件 {} 的类型 {}";
  public static final String CANNOT_INCREASE_REFERENCE_COUNT_FOR_EVENT_IGNORE =
      "无法增加事件 {} 的引用计数，将在批次中忽略该事件";
  public static final String CANNOT_SERIALIZE_BOTH_TABLET_AND_STATEMENT_ARE =
      "无法序列化：tablet 和 statement 均为空";
  public static final String CERTIFICATE_DIRECTORY_IS_PLEASE_MOVE_CERTIFICATES_FROM =
      "证书目录为：{}，请将证书从 reject 目录移动到 trusted 目录以允许加密访问";
  public static final String CLIENT_HAS_BEEN_RETURNED_TO_THE_POOL =
      "客户端已归还到池中。当前 handler 状态为 {}，不会传输 {}。";
  public static final String CLOSED_ASYNCPIPEDATATRANSFERSERVICECLIENTMANAGER_FOR_RECEIVER_ATTRIBUTES =
      "已关闭 receiver attributes 为 {} 的 AsyncPipeDataTransferServiceClientManager";
  public static final String CREATE_GROUP_SUCCESSFULLY_SERVER_HANDLE_UPDATE_RATE =
      "创建 group 成功！Server handle：{}，更新频率：{} ms";
  public static final String DELETENODETRANSFER_NO_EVENT_SUCCESSFULLY_PROCESSED =
      "DeleteNodeTransfer：第 {} 个事件处理成功！";
  public static final String DESERIALIZE_PIPEDATA_ERROR_BECAUSE_UNKNOWN_TYPE =
      "反序列化 PipeData 出错，原因：未知类型 ";
  public static final String DESERIALIZE_PIPEDATA_ERROR_BECAUSE_UNKNOWN_TYPE_1 =
      "反序列化 PipeData 出错，原因：未知类型 {}。";
  public static final String ERROR_GETTING_OPC_CLIENT = "获取 OPC 客户端出错：";
  public static final String ERROR_PROGID_IS_INVALID_OR_UNREGISTERED_HRESULT =
      "错误：ProgID 无效或未注册，（HRESULT=0x";
  public static final String ERROR_RUNNING_OPC_CLIENT = "运行 OPC 客户端出错：";
  public static final String EXCEPTION_OCCURRED_WHEN_PIPETABLEMODELTSFILEBUILDERV2_WRITING_TABLETS_TO =
      "PipeTableModelTsFileBuilderV2 向 TsFile 写入 tablet 时发生异常，将使用 fallback TsFile builder：{}";
  public static final String EXCEPTION_OCCURRED_WHEN_PIPETREEMODELTSFILEBUILDERV2_WRITING_TABLETS_TO =
      "PipeTreeModelTsFileBuilderV2 向 TsFile 写入 tablet 时发生异常，将使用 fallback TsFile builder：{}";
  public static final String EXECUTE_STATEMENT_TO_DATABASE_SKIP_BECAUSE_NO =
      "执行语句 {} 到数据库 {} 时因无权限而跳过。";
  public static final String FAILED_TO_ACQUIRE_IOPCITEMMGT_ERROR_CODE_0X =
      "获取 IOPCItemMgt 失败，错误码：0x";
  public static final String FAILED_TO_ACQUIRE_IOPCSYNCIO_ERROR_CODE_0X =
      "获取 IOPCSyncIO 失败，错误码：0x";
  public static final String FAILED_TO_ADD_ITEM = "添加 item 失败";
  public static final String FAILED_TO_ADD_ITEM_WIN_ERROR_CODE = "添加 item 失败，Windows 错误码：0x";
  public static final String FAILED_TO_ADJUST_TIMEOUT_WHEN_FAILED_TO =
      "传输文件失败后调整超时时间失败。";
  public static final String FAILED_TO_BORROW_CLIENT_FOR_CACHED_LEADER =
      "为缓存的 leader 借用客户端 {}:{} 失败。";
  public static final String FAILED_TO_BUILD_AND_STARTUP_OPCUASERVER =
      "构建并启动 OpcUaServer 失败";
  public static final String FAILED_TO_CLOSE_ASYNCPIPEDATATRANSFERSERVICECLIENTMANAGER_FOR_RECEIVER_ATTRIBUTE =
      "关闭 receiver attributes 为 {} 的 AsyncPipeDataTransferServiceClientManager 失败";
  public static final String FAILED_TO_CLOSE_CLIENT_AFTER_HANDSHAKE_FAILURE =
      "manager 已关闭且握手失败后，关闭客户端 {}:{} 失败。";
  public static final String FAILED_TO_CLOSE_CLIENT_MANAGER = "关闭客户端管理器失败。";
  public static final String FAILED_TO_CLOSE_FILE_READER_OR_DELETE =
      "传输文件失败后关闭文件读取器或删除 TsFile 失败。";
  public static final String FAILED_TO_CLOSE_FILE_READER_OR_DELETE_1 =
      "成功传输文件后关闭文件读取器或删除 TsFile 失败。";
  public static final String FAILED_TO_CLOSE_FILE_READER_WHEN_SUCCESSFULLY =
      "成功传输 mod 文件后关闭文件读取器失败。";
  public static final String FAILED_TO_CLOSE_OR_INVALIDATE_CLIENT_WHEN =
      "connector 关闭时关闭或失效客户端失败。客户端：{}，异常：{}";
  public static final String FAILED_TO_CLOSE_TRUSTLISTMANAGER_BECAUSE =
      "关闭 trustListManager 失败，原因：{}。";
  public static final String FAILED_TO_CONNECT_TO_SERVER_ERROR_CODE =
      "连接服务器失败，错误码：0x";
  public static final String FAILED_TO_CONVERT_STATEMENT_TO_TABLET = "将 statement 转换为 tablet 失败。";
  public static final String FAILED_TO_CONVERT_STATEMENT_TO_TABLET_FOR =
      "为序列化将 statement 转换为 tablet 失败";
  public static final String FAILED_TO_CREATE_GROUP_ERROR_CODE_0X = "创建 group 失败，错误码：0x";
  public static final String FAILED_TO_CREATE_NODES_AFTER_TRANSFER_DATA =
      "传输 data value 后创建 node 失败，创建状态：";
  public static final String FAILED_TO_DELETE_BATCH_FILE_THIS_FILE =
      "删除批次文件 {} 失败，该文件稍后需要手动删除";
  public static final String FAILED_TO_GET_THE_SIZE_OF_PIPETRANSFERBATCHREQBUILDER =
      "获取 PipeTransferBatchReqBuilder 大小失败，返回 0。异常：{}";
  public static final String FAILED_TO_HANDSHAKE = "握手失败。";
  public static final String FAILED_TO_LOG_ERROR_WHEN_FAILED_TO =
      "传输文件失败后记录错误日志失败。";
  public static final String FAILED_TO_PUSH_VALUE_CHANGE_TO_CLIENT =
      "向客户端推送值变更失败，nodeId={}";
  public static final String FAILED_TO_SEND_INITIAL_VALUE_TO_NEW =
      "向新订阅发送初始值失败，nodeId={}";
  public static final String FAILED_TO_SERIALIZE_PROGRESS_INDEX = "序列化进度索引 {} 失败";
  public static final String FAILED_TO_SHUTDOWN_EXECUTOR = "关闭执行器 {} 失败。";
  public static final String FAILED_TO_TRANSFER_DATAVALUE = "传输 dataValue 失败";
  public static final String FAILED_TO_TRANSFER_DATAVALUE_AFTER_SUCCESSFULLY_CREATED =
      "成功创建 node 后传输 dataValue 失败";
  public static final String FAILED_TO_TRANSFER_PIPEDELETENODEEVENT_COMMITTER_KEY_REPLICATE =
      "传输 PipeDeleteNodeEvent {}（committer key={}，replicate index={}）失败。";
  public static final String FAILED_TO_TRANSFER_TABLETINSERTIONEVENT_COMMITTER_KEY_REPLICATE =
      "传输 TabletInsertionEvent {}（committer key={}，replicate index={}）失败。";
  public static final String FAILED_TO_TRANSFER_TSFILE_BATCH = "传输 TsFile 批次（{}）失败。";
  public static final String FAILED_TO_TRANSFER_TSFILE_EVENT_ASYNCHRONOUSLY =
      "异步传输 TsFile 事件 {} 失败。";
  public static final String FAILED_TO_UPDATE_LEADER_CACHE_FOR_DEVICE =
      "更新 device {} 的 leader cache 到端点 {}:{} 失败。";
  public static final String FAILED_TO_WRITE = "写入失败 ";
  public static final String FAILED_TO_WRITE_WIN_ERROR_CODE_0X =
      "写入失败，Windows 错误码：0x";
  public static final String GENERATE_STATEMENT_FROM_TABLET_ERROR = "从 tablet {} 生成 Statement 出错。";
  public static final String GOT_AN_ERROR_FROM = "收到错误 \\\"{}\\\"，来源 {}:{}。";
  public static final String GOT_AN_ERROR_FROM_AN_UNKNOWN_CLIENT =
      "从未知客户端收到错误 \\\"{}\\\"。";
  public static final String HANDSHAKE_SUCCESSFULLY_WITH_RECEIVER =
      "与接收端 {}:{} 握手成功。";
  public static final String ILLEGAL_STATE_WHEN_RETURN_THE_CLIENT_TO =
      "将客户端归还对象池时状态非法，可能对象池已清空。将忽略。";
  public static final String INSERTNODETRANSFER_NO_EVENT_SUCCESSFULLY_PROCESSED =
      "InsertNodeTransfer：第 {} 个事件处理成功！";
  public static final String INTERRUPTED_WHILE_WAITING_FOR_HANDSHAKE_RESPONSE =
      "等待握手响应时被中断。";
  public static final String IOTCONSENSUSV2ASYNCCONNECTOR_DOES_NOT_SUPPORT_TRANSFERRING_GENERIC_EVENT =
      "IoTConsensusV2AsyncConnector 不支持传输通用事件：{}。";
  public static final String IOTCONSENSUSV2ASYNCCONNECTOR_DOES_NOT_SUPPORT_TRANSFER_GENERIC_EVENT =
      "IoTConsensusV2AsyncConnector 不支持传输通用事件：{}。";
  public static final String IOTCONSENSUSV2ASYNCCONNECTOR_ONLY_SUPPORT_PIPETSFILEINSERTIONEVENT_CURRENT_EVEN =
      "IoTConsensusV2AsyncConnector 仅支持 PipeTsFileInsertionEvent。当前事件：{}。";
  public static final String IOTCONSENSUSV2CONNECTOR_TRANSFERBUFFER_QUEUE_OFFER_IS_INTERRUPTED =
      "IoTConsensusV2Connector 写入 transferBuffer 队列时被中断。";
  public static final String IOTCONSENSUSV2TRANSFERBATCHREQBUILDER_THE_MAX_BATCH_SIZE_IS_ADJUSTED =
      "IoTConsensusV2TransferBatchReqBuilder：最大批次大小因内存限制从 {} 调整为 {}";
  public static final String IOTCONSENSUSV2_CONSENSUSGROUP_EVENT_NOT_FOUND_IN_TRANSFERBUFFER =
      "IoTConsensusV2-ConsensusGroup-{}：transferBuffer 中未找到事件-{}，跳过移除。队列大小 = {}";
  public static final String IOTCONSENSUSV2_CONSENSUSGROUP_EVENT_REPLICATE_INDEX_TRANSFER_FAILED =
      "IoTConsensusV2-ConsensusGroup-{}：事件 {}（复制索引 {}）传输失败，且加入重试队列失败，"
          + "该事件将被忽略。";
  public static final String IOTCONSENSUSV2_CONSENSUSGROUP_EVENT_REPLICATE_INDEX_TRANSFER_FAILED_1 =
      "IoTConsensusV2-ConsensusGroup-{}：事件 {}（复制索引 {}）传输失败，将加入重试队列。";
  public static final String IOTCONSENSUSV2_CONSENSUSGROUP_NO_EVENT_ADDED_TO_CONNECTOR =
      "IoTConsensusV2-ConsensusGroup-{}：第 {} 个事件-{} 已添加到连接器缓冲区";
  public static final String IOTCONSENSUSV2_CONSENSUSGROUP_ONE_EVENT_SUCCESSFULLY_RECEIVED_BY =
      "IoTConsensusV2-ConsensusGroup-{}：事件-{} 已被从节点成功接收，将从队列移除，"
          + "队列大小 = {}，限制大小 = {}";
  public static final String IOTCONSENSUSV2_CONSENSUSGROUP_RETRYEVENTQUEUE_IS_NOT_EMPTY_AFTER =
      "IoTConsensusV2-ConsensusGroup-{}：20 秒后重试事件队列仍不为空。重试队列大小：{}";
  public static final String IOTCONSENSUSV2_CONSENSUSGROUP_RETRY_WITH_INTERVAL_FOR_INDEX =
      "IoTConsensusV2-ConsensusGroup-{}：以间隔 {} 重试索引 {} {}";
  public static final String IOTCONSENSUSV2_CONSENSUSGROUP_TRY_TO_REMOVE_EVENT_AFTER =
      "IoTConsensusV2-ConsensusGroup-{}：连接器已关闭后尝试移除事件-{}，将忽略。";
  public static final String IOTCONSENSUSV2_FAILED_TO_CLOSE_FILE_READER_WHEN =
      "IoTConsensusV2-{}：传输文件失败后关闭文件读取器失败。";
  public static final String IOTCONSENSUSV2_FAILED_TO_CLOSE_FILE_READER_WHEN_1 =
      "IoTConsensusV2-{}：成功传输文件后关闭文件读取器失败。";
  public static final String IOTCONSENSUSV2_FAILED_TO_CLOSE_FILE_READER_WHEN_2 =
      "IoTConsensusV2-{}：成功传输 mod 文件后关闭文件读取器失败。";
  public static final String IOTCONSENSUSV2_FAILED_TO_TRANSFER_TABLETINSERTIONEVENT_BATCH_TOTAL =
      "IoTConsensusV2：传输 TabletInsertionEvent 批次失败。失败事件总数：{}，相关 Pipe 名称：{}";
  public static final String IOTCONSENSUSV2_FAILED_TO_TRANSFER_TSFILEINSERTIONEVENT_COMMITTER_KEY =
      "IoTConsensusV2-{}：传输 TsFileInsertionEvent {}（提交器键 {}，复制索引 {}）失败。";
  public static final String IOTCONSENSUSV2_REDIRECT_FILE_POSITION_TO =
      "IoTConsensusV2-{}：将文件位置重定向到 {}。";
  public static final String IOTCONSENSUSV2_SUCCESSFULLY_TRANSFERRED_FILE_COMMITTER_KEY_REPLICATE =
      "IoTConsensusV2-{}：成功传输文件 {}（提交器键={}，复制索引={}）。";
  public static final String IOTDBCDCCONNECTOR_ONLY_SUPPORT_PIPEINSERTNODETABLETINSERTIONEVENT_AND_PIPERAWTAB =
      "IoTDBCDCConnector 仅支持 PipeInsertNodeTabletInsertionEvent 和 PipeRawTabletInsertionEvent。";
  public static final String IOTDBDATAREGIONAIRGAPCONNECTOR_DOES_NOT_SUPPORT_TRANSFERRING_GENERIC_EVENT =
      "IoTDBDataRegionAirGapConnector 不支持传输通用事件：{}。";
  public static final String IOTDBDATAREGIONAIRGAPCONNECTOR_ONLY_SUPPORT_PIPEINSERTNODETABLETINSERTIONEVENT_A =
      "IoTDBDataRegionAirGapConnector 仅支持 PipeInsertNodeTabletInsertionEvent 和 "
          + "PipeRawTabletInsertionEvent。忽略 {}。";
  public static final String IOTDBDATAREGIONAIRGAPCONNECTOR_ONLY_SUPPORT_PIPETSFILEINSERTIONEVENT_IGNORE =
      "IoTDBDataRegionAirGapConnector 仅支持 PipeTsFileInsertionEvent。忽略 {}。";
  public static final String IOTDBLEGACYPIPECONNECTOR_DOES_NOT_SUPPORT_TRANSFERRING_GENERIC_EVENT =
      "IoTDBLegacyPipeConnector 不支持传输通用事件：{}。";
  public static final String IOTDBLEGACYPIPECONNECTOR_ONLY_SUPPORT_PIPEINSERTNODEINSERTIONEVENT_AND_PIPETABLE =
      "IoTDBLegacyPipeConnector 仅支持 PipeInsertNodeInsertionEvent 和 PipeTabletInsertionEvent。";
  public static final String IOTDBLEGACYPIPECONNECTOR_ONLY_SUPPORT_PIPETSFILEINSERTIONEVENT =
      "IoTDBLegacyPipeConnector 仅支持 PipeTsFileInsertionEvent。";
  public static final String IOTDBSCHEMAREGIONAIRGAPSINK_CAN_T_TRANSFER_TABLETINSERTIONEVENT =
      "IoTDBSchemaRegionAirGapSink 无法传输 TabletInsertionEvent。";
  public static final String IOTDBSCHEMAREGIONAIRGAPSINK_CAN_T_TRANSFER_TSFILEINSERTIONEVENT =
      "IoTDBSchemaRegionAirGapSink 无法传输 TsFileInsertionEvent。";
  public static final String IOTDBSCHEMAREGIONAIRGAPSINK_DOES_NOT_SUPPORT_TRANSFERRING_GENERIC_EVENT =
      "IoTDBSchemaRegionAirGapSink 不支持传输通用事件：{}。";
  public static final String IOTDBSCHEMAREGIONCONNECTOR_CAN_T_TRANSFER_TABLETINSERTIONEVENT =
      "IoTDBSchemaRegionConnector 无法传输 TabletInsertionEvent。";
  public static final String IOTDBSCHEMAREGIONCONNECTOR_CAN_T_TRANSFER_TSFILEINSERTIONEVENT =
      "IoTDBSchemaRegionConnector 无法传输 TsFileInsertionEvent。";
  public static final String IOTDBSCHEMAREGIONCONNECTOR_DOES_NOT_SUPPORT_TRANSFERRING_GENERIC_EVENT =
      "IoTDBSchemaRegionConnector 不支持传输通用事件：{}。";
  public static final String IOTDBTHRIFTASYNCCONNECTOR_DOES_NOT_SUPPORT_TRANSFERRING_GENERIC_EVENT =
      "IoTDBThriftAsyncConnector 不支持传输通用事件：{}。";
  public static final String IOTDBTHRIFTASYNCCONNECTOR_DOES_NOT_SUPPORT_TRANSFER_GENERIC_EVENT =
      "IoTDBThriftAsyncConnector 不支持传输通用事件：{}。";
  public static final String IOTDBTHRIFTASYNCCONNECTOR_ONLY_SUPPORT_PIPEINSERTNODETABLETINSERTIONEVENT_AND_PI =
      "IoTDBThriftAsyncConnector 仅支持 PipeInsertNodeTabletInsertionEvent 和 "
          + "PipeRawTabletInsertionEvent。当前事件：{}。";
  public static final String IOTDBTHRIFTASYNCCONNECTOR_ONLY_SUPPORT_PIPETSFILEINSERTIONEVENT_CURRENT_EVENT =
      "IoTDBThriftAsyncConnector 仅支持 PipeTsFileInsertionEvent。当前事件：{}。";
  public static final String IOTDBTHRIFTSYNCCONNECTOR_DOES_NOT_SUPPORT_TRANSFERRING_GENERIC_EVENT =
      "IoTDBThriftSyncConnector 不支持传输通用事件：{}。";
  public static final String IOTDBTHRIFTSYNCCONNECTOR_ONLY_SUPPORT_PIPEINSERTNODETABLETINSERTIONEVENT_AND_PIP =
      "IoTDBThriftSyncConnector 仅支持 PipeInsertNodeTabletInsertionEvent 和 "
          + "PipeRawTabletInsertionEvent。忽略 {}。";
  public static final String IOTDBTHRIFTSYNCCONNECTOR_ONLY_SUPPORT_PIPETSFILEINSERTIONEVENT_IGNORE =
      "IoTDBThriftSyncConnector 仅支持 PipeTsFileInsertionEvent。忽略 {}。";
  public static final String LEADERCACHEMANAGER_ALLOCATEDMEMORYBLOCK_HAS_EXPANDED_FROM_TO =
      "LeaderCacheManager.allocatedMemoryBlock 已从 {} 扩展到 {}。";
  public static final String LEADERCACHEMANAGER_ALLOCATEDMEMORYBLOCK_HAS_SHRUNK_FROM_TO =
      "LeaderCacheManager.allocatedMemoryBlock 已从 {} 收缩到 {}。";
  public static final String LOADING_KEYSTORE_AT = "正在从 {} 加载 KeyStore";
  public static final String LOADING_KEYSTORE_AT_1 = "正在从 {} 加载 KeyStore";
  public static final String LOAD_KEYSTORE_FAILED_THE_EXISTING_KEYSTORE_MAY =
      "加载 KeyStore 失败，现有 KeyStore 可能已过期，正在重新构建...";
  public static final String NO_OPC_CLIENT_OR_SERVER_IS_SPECIFIED =
      "传输 tablet 时未指定 OPC 客户端或服务器";
  public static final String OPC_DA_SINK_MUST_RUN_ON_WINDOWS = "opc-da-sink 必须在 Windows 系统上运行。";
  public static final String PIPETABLEMODETSFILEBUILDERV2_DOES_NOT_SUPPORT_TREE_MODEL_TABLET =
      "PipeTableModeTsFileBuilderV2 不支持使用树模型 tablet 构建 TSFile";
  public static final String PIPETABLEMODETSFILEBUILDER_DOES_NOT_SUPPORT_TREE_MODEL_TABLET =
      "PipeTableModeTsFileBuilder 不支持使用树模型 tablet 构建 TSFile";
  public static final String PIPETREEMODELTSFILEBUILDERV2_DOES_NOT_SUPPORT_TABLE_MODEL_TABLET =
      "PipeTreeModelTsFileBuilderV2 不支持使用表模型 tablet 构建 TSFile";
  public static final String PIPETREEMODELTSFILEBUILDER_DOES_NOT_SUPPORT_TABLE_MODEL_TABLET =
      "PipeTreeModelTsFileBuilder 不支持使用表模型 tablet 构建 TSFile";
  public static final String POLLED_EVENT_FROM_RETRY_QUEUE = "已从重试队列取出事件 {}。";
  public static final String RECEIVED_AN_ERROR_MESSAGE_FROM =
      "收到错误消息 {}，来源 {}:{}";
  public static final String RECEIVED_AN_UNKNOWN_MESSAGE_FROM =
      "收到未知消息 {}，来源 {}:{}";
  public static final String RECEIVED_A_ACK_MESSAGE_FROM = "从 {}:{} 收到 ack 消息";
  public static final String RECEIVED_A_BIND_MESSAGE_FROM = "从 {}:{} 收到 bind 消息";
  public static final String REDIRECT_FILE_POSITION_TO = "将文件位置重定向到 {}。";
  public static final String REDIRECT_TO_POSITION_IN_TRANSFERRING_TSFILE =
      "重定向到位置 {}，正在传输的 TsFile：{}。";
  public static final String SECURITY_DIR = "安全目录：{}";
  public static final String SECURITY_PKI_DIR = "安全 PKI 目录：{}";
  public static final String SUCCESSFULLY_ADDED_ITEM = "成功添加 item {}。";
  public static final String SUCCESSFULLY_CONVERTED_PROGID_TO_CLSID =
      "成功将 progID {} 转换为 CLSID：{{}}";
  public static final String SUCCESSFULLY_SHUTDOWN_EXECUTOR = "成功关闭执行器 {}。";
  public static final String SUCCESSFULLY_TRANSFERRED_DELETION_EVENT =
      "成功传输删除事件 {}。";
  public static final String SUCCESSFULLY_TRANSFERRED_FILE = "成功传输文件 {}。";
  public static final String SUCCESSFULLY_TRANSFERRED_FILE_AND =
      "成功传输文件 {}、{} 和 {}。";
  public static final String SUCCESSFULLY_TRANSFERRED_FILE_BATCHED_TABLEINSERTIONEVENTS_REFERENCE_COUNT =
      "成功传输文件 {}（批量 TableInsertionEvent，引用计数={}）。";
  public static final String SUCCESSFULLY_TRANSFERRED_FILE_COMMITTER_KEY_COMMIT_ID =
      "成功传输文件 {}（committer key={}，commit id={}，引用计数={}）。";
  public static final String SUCCESSFULLY_TRANSFERRED_SCHEMA_EVENT =
      "成功传输 schema 事件 {}。";
  public static final String SUCCESSFULLY_TRANSFERRED_SCHEMA_REGION_SNAPSHOT_AND =
      "成功传输 schema region 快照 {}、{} 和 {}。";
  public static final String THE_BATCH_SIZE_LIMIT_HAS_EXPANDED_FROM =
      "批次大小限制已从 {} 扩展到 {}。";
  public static final String THE_BATCH_SIZE_LIMIT_HAS_SHRUNK_FROM =
      "批次大小限制已从 {} 收缩到 {}。";
  public static final String THE_DEFAULT_QUALITY_CAN_ONLY_BE_GOOD =
      "默认质量只能是 'GOOD'、'BAD' 或 'UNCERTAIN'。";
  public static final String THE_EVENT_ACK_IS_NOT_FOUND = "未找到事件 ack {}。";
  public static final String THE_EVENT_CAN_T_BE_TRANSFERRED_TO =
      "事件 {} 无法传输到客户端，稍后将重试。";
  public static final String THE_EVENT_IN_ERROR_IS_NOT_FOUND =
      "未找到错误中的事件 {}。";
  public static final String THE_EVENT_POLLED_FROM_THE_QUEUE_IS =
      "从队列 poll 出的事件与 peek 到的事件不一致。peek 事件：{}，poll 事件：{}。";
  public static final String THE_FILE_IS_NOT_FOUND_MAY_ALREADY =
      "未找到文件 {}，可能已被删除。";
  public static final String THE_PIPE_WAS_DROPPED_SO_THE_EVENT =
      "pipe {} 已被删除，事件 ack {} 将被忽略。";
  public static final String THE_PIPE_WAS_DROPPED_SO_THE_EVENT_1 =
      "pipe {} 已被删除，错误中的事件 {} 将被忽略。";
  public static final String THE_PIPE_WAS_DROPPED_SO_THE_EVENT_2 =
      "pipe {} 已被删除，事件 {} 将被丢弃。";
  public static final String THE_QUALITY_VALUE_ONLY_SUPPORTS_BOOLEAN_TYPE =
      "quality 值仅支持 boolean 类型，其中 true == GOOD，false == BAD。";
  public static final String THE_SCHEMA_REGION_AIR_GAP_CONNECTOR_DOES =
      "schema region air gap connector 不支持传输单个文件片段字节。";
  public static final String THE_SCHEMA_REGION_CONNECTOR_DOES_NOT_SUPPORT =
      "schema region connector 不支持传输单个文件片段请求。";
  public static final String THE_SECURITY_POLICY_CANNOT_BE_EMPTY =
      "安全策略不能为空。";
  public static final String THE_SECURITY_POLICY_CAN_ONLY_BE_NONE =
      "安全策略只能是 'None'、'Basic128Rsa15'、'Basic256'、'Basic256Sha256'、"
          + "'Aes128_Sha256_RsaOaep' 或 'Aes256_Sha256_RsaPss'。";
  public static final String THE_SEGMENTS_OF_TABLETS_MUST_EXIST =
      "tablet 的 segment 必须存在";
  public static final String THE_TABLET_OF_COMMITID_CAN_T_BE =
      "commitId 为 {} 的 tablet 无法被客户端解析，稍后将重试。";
  public static final String THE_TRANSFER_THREAD_IS_INTERRUPTED = "传输线程被中断。";
  public static final String THE_WEBSOCKET_CONNECTION_FROM_CLIENT_HAS_BEEN =
      "来自客户端的 WebSocket 连接已关闭！code 为 {}，原因：{}，是否由远端关闭：{}";
  public static final String THE_WEBSOCKET_CONNECTION_FROM_CLIENT_HAS_BEEN_1 =
      "来自客户端 {}:{} 的 WebSocket 连接已关闭！code 为 {}，原因：{}，是否由远端关闭：{}";
  public static final String THE_WEBSOCKET_CONNECTION_FROM_CLIENT_HAS_BEEN_2 =
      "来自客户端 {}:{} 的 WebSocket 连接已打开！";
  public static final String THE_WEBSOCKET_CONNECTION_FROM_HAS_BEEN_CLOSED =
      "来自 {}:{} 的 WebSocket 连接已关闭，但收到了 commitId 为 {} 的 ack 消息。";
  public static final String THE_WEBSOCKET_CONNECTION_FROM_HAS_BEEN_CLOSED_1 =
      "来自 {}:{} 的 WebSocket 连接已关闭，但收到了 commitId 为 {} 的错误消息。";
  public static final String THE_WEBSOCKET_SERVER_HAS_BEEN_STARTED =
      "WebSocket 服务器 {}:{} 已启动！";
  public static final String THE_WRITTEN_TABLET_TIME_MAY_OVERLAP_OR =
      "写入的 Tablet 时间可能重叠，或 Schema 可能不正确";
  public static final String THIS_CONNECTOR_ONLY_SUPPORT_PIPEINSERTNODETABLETINSERTIONEVENT_AND_PIPERAWTABLET =
      "该 Connector 仅支持 PipeInsertNodeTabletInsertionEvent 和 PipeRawTabletInsertionEvent。忽略 {}。";
  public static final String TIMED_OUT_WHEN_WAITING_FOR_CLIENT_HANDSHAKE =
      "等待客户端握手完成超时。";
  public static final String TIOTCONSENSUSV2BATCHTRANSFERRESP_IS_NULL =
      "TIoTConsensusV2BatchTransferResp 为空";
  public static final String TIOTCONSENSUSV2TRANSFERRESP_IS_NULL = "TIoTConsensusV2TransferResp 为空";
  public static final String TPIPETRANSFERRESP_IS_NULL = "TPipeTransferResp 为空";
  public static final String TRANSFER_TSFILE_EVENT_ASYNCHRONOUSLY_WAS_INTERRUPTED =
      "异步传输 TsFile 事件 {} 被中断。";
  public static final String UNABLE_TO_CREATE_SECURITY_DIR = "无法创建安全目录：";
  public static final String UNKNOWN_LOAD_BALANCE_STRATEGY_USE_ROUND_ROBIN =
      "未知的负载均衡策略：{}，将使用轮询策略替代。";
  public static final String UNSUPPORTED_BATCH_TYPE = "不支持的批次类型 {}。";
  public static final String UNSUPPORTED_BATCH_TYPE_WHEN_TRANSFERRING_TABLET_INSERTION =
      "传输 tablet 插入事件时不支持批次类型 {}。";
  public static final String UNSUPPORTED_DATATYPE = "不支持的数据类型 ";
  public static final String UNSUPPORTED_EVENT_TYPE_WHEN_BUILDING_TRANSFER_REQUEST =
      "构建传输请求时不支持事件 {} 的类型 {}";
  public static final String WAIT_FOR_RESOURCE_ENOUGH_FOR_SLICING_TSFILE =
      "等待资源充足以切片 TsFile {}，已等待 {} 秒。";
  public static final String WEBSOCKETCONNECTOR_FAILED_TO_INCREASE_THE_REFERENCE_COUNT =
      "WebSocketConnector 增加事件引用计数失败，将忽略该事件。当前事件：{}。";
  public static final String WEBSOCKETCONNECTOR_ONLY_SUPPORT_PIPEINSERTNODETABLETINSERTIONEVENT_AND_PIPERAWTA =
      "WebSocketConnector 仅支持 PipeInsertNodeTabletInsertionEvent 和 "
          + "PipeRawTabletInsertionEvent。当前事件：{}。";
  public static final String WEBSOCKETCONNECTOR_ONLY_SUPPORT_PIPETSFILEINSERTIONEVENT_CURRENT_EVENT =
      "WebSocketConnector 仅支持 PipeTsFileInsertionEvent。当前事件：{}。";
  public static final String WHEN_THE_OPC_UA_SINK_POINTS_TO =
      "当 OPC UA sink 指向外部服务器时，不支持表模型数据。";
  public static final String WHEN_THE_OPC_UA_SINK_SETS_WITH =
      "当 OPC UA sink 将 'with-quality' 设为 true 时，不支持表模型数据。";
  public static final String WRITEBACKSINK_ONLY_SUPPORT_PIPEINSERTNODETABLETINSERTIONEVENT_AND_PIPERAWTABLETI =
      "WriteBackSink 仅支持 PipeInsertNodeTabletInsertionEvent 和 PipeRawTabletInsertionEvent。忽略 {}。";

  // ===================== RECEIVER =====================

  public static final String ALL_RECEIVERS_RELATED_TO_ARE_RELEASED =
      "与 {} 相关的所有接收器均已释放。";
  public static final String AUTO_CREATE_DATABASE_FAILED_BECAUSE = "自动创建数据库失败，原因：";
  public static final String CREATE_DATABASE_ERROR_STATEMENT_RESULT_STATUS =
      "创建数据库出错，语句：{}，结果状态：{}。";
  public static final String DATABASE_NAME_IS_UNEXPECTEDLY_NULL_FOR_LOADTSFILESTATEMENT =
      "LoadTsFileStatement 的数据库名意外为空：{}。跳过数据类型转换。";
  public static final String DATABASE_NAME_IS_UNEXPECTEDLY_NULL_FOR_STATEMENT =
      "statement 的数据库名意外为空：{}。跳过数据类型转换。";
  public static final String DATA_TYPE_CONVERSION_FOR_LOADTSFILESTATEMENT_IS_SUCCESSFUL =
      "LoadTsFileStatement {} 的数据类型转换成功。";
  public static final String DATA_TYPE_MISMATCH_DETECTED_TSSTATUS_FOR_LOADTSFILESTATEMENT =
      "检测到 LoadTsFileStatement 的数据类型不匹配（TSStatus：{}）：{}。开始数据类型转换。";
  public static final String DELETE_ERROR_STATEMENT = "删除 {} 出错，语句：{}。";
  public static final String DELETE_RESULT_STATUS = "删除结果状态：{}。";
  public static final String FAILED_TO_CLOSE_IOTDBAIRGAPRECEIVERAGENT_S_SERVER_SOCKET =
      "关闭 IoTDBAirGapReceiverAgent 的 server socket 失败";
  public static final String FAILED_TO_CONVERT_DATA_TYPE_FOR_LOADTSFILESTATEMENT =
      "转换 LoadTsFileStatement {} 的数据类型失败。";
  public static final String FAILED_TO_EXECUTE_STATEMENT_AFTER_DATA_TYPE =
      "数据类型转换后执行语句失败。";
  public static final String FAILED_TO_HANDLE_CONFIG_CLIENT_ID_EXIT =
      "处理 config client（id = {}）退出失败";
  public static final String FAIL_TO_CREATE_IOTCONSENSUSV2_RECEIVER_FILE_FOLDERS =
      "创建 IoTConsensusV2 接收文件目录分配策略失败，原因：所有目录所在磁盘均已满。";
  public static final String FAIL_TO_CREATE_PIPE_RECEIVER_FILE_FOLDERS =
      "创建 pipe 接收文件目录分配策略失败，原因：所有目录所在磁盘均已满。";
  public static final String FAIL_TO_INITIATE_FILE_BUFFER_FOLDER_ERROR =
      "初始化文件缓冲目录失败，错误消息：{}";
  public static final String FAIL_TO_LOAD_PIPEDATA_BECAUSE = "加载 pipeData 失败，原因：{}.";
  public static final String FAIL_TO_RENAME_FILE_TO = "将文件 {} 重命名为 {} 失败";
  public static final String INVOKE_HANDSHAKE_METHOD_FROM_CLIENT_IP =
      "调用来自客户端 ip = {} 的 handshake 方法";
  public static final String INVOKE_TRANSPORTDATA_METHOD_FROM_CLIENT_IP =
      "调用来自客户端 ip = {} 的 transportData 方法";
  public static final String INVOKE_TRANSPORTPIPEDATA_METHOD_FROM_CLIENT_IP =
      "调用来自客户端 ip = {} 的 transportPipeData 方法";
  public static final String IOTCONSENSUSV2RECEIVER_THREAD_IS_INTERRUPTED_WHEN_WAITING_FOR =
      "IoTConsensusV2Receiver 线程在等待接收器初始化时被中断，可能是系统正在退出。";
  public static final String IOTCONSENSUSV2_PIPENAME = "IoTConsensusV2-PipeName-{}：{}";
  public static final String IOTCONSENSUSV2_PIPENAME_CURRENT_WAITING_IS_INTERRUPTED_ONSYNCEDCOMMITINDEX =
      "IoTConsensusV2-PipeName-{}：当前等待被中断。onSyncedCommitIndex：{}。异常：";
  public static final String IOTCONSENSUSV2_PIPENAME_CURRENT_WRITING_FILE_WRITER_IS =
      "IoTConsensusV2-PipeName-{}：当前写入文件的 writer 为空，无需关闭。";
  public static final String IOTCONSENSUSV2_PIPENAME_CURRENT_WRITING_FILE_WRITER_WAS =
      "IoTConsensusV2-PipeName-{}：当前写入文件的 writer {} 已关闭。";
  public static final String IOTCONSENSUSV2_PIPENAME_FAILED_TO_CLOSE_CURRENT_WRITING =
      "IoTConsensusV2-PipeName-{}：关闭当前写入文件的 writer {} 失败，原因：{}。";
  public static final String IOTCONSENSUSV2_PIPENAME_FAILED_TO_CREATE_RECEIVER_FILE =
      "IoTConsensusV2-PipeName-{}：创建接收文件目录 {} 失败。";
  public static final String IOTCONSENSUSV2_PIPENAME_FAILED_TO_CREATE_RECEIVER_FILE_1 =
      "IoTConsensusV2-PipeName-{}：创建接收文件目录 {} 失败，原因：系统并发退出导致父系统目录已被删除。";
  public static final String IOTCONSENSUSV2_PIPENAME_FAILED_TO_CREATE_RECEIVER_FILE_2 =
      "IoTConsensusV2-PipeName-{}：创建接收文件目录 {} 失败，可能原因：权限不足或目录已存在等。";
  public static final String IOTCONSENSUSV2_PIPENAME_FAILED_TO_CREATE_RECEIVER_TSFILEWRITER =
      "IoTConsensusV2-PipeName-{}：创建接收端 TsFileWriter-{} 的文件目录 {} 失败";
  public static final String IOTCONSENSUSV2_PIPENAME_FAILED_TO_DELETE_BECAUSE =
      "IoTConsensusV2-PipeName-{}：{} 删除 {} 失败，原因：{}。";
  public static final String IOTCONSENSUSV2_PIPENAME_FAILED_TO_GET_BASE_DIRECTORY =
      "IoTConsensusV2-PipeName-{}：获取基础目录失败";
  public static final String IOTCONSENSUSV2_PIPENAME_FAILED_TO_LOAD_FILE_FROM =
      "IoTConsensusV2-PipeName-{}：加载文件 {}（来自请求 {}）失败。";
  public static final String IOTCONSENSUSV2_PIPENAME_FAILED_TO_READ_TSFILE_WHEN =
      "IoTConsensusV2-PipeName-{}：统计点数时读取 TsFile 失败：{}。";
  public static final String IOTCONSENSUSV2_PIPENAME_FAILED_TO_RETURN_TSFILEWRITER =
      "IoTConsensusV2-PipeName-{}：归还 TsFileWriter {} 失败。";
  public static final String IOTCONSENSUSV2_PIPENAME_FAILED_TO_SEAL_FILE_BECAUSE =
      "IoTConsensusV2-PipeName-{}：封存文件 {} 失败，原因：文件不存在。";
  public static final String IOTCONSENSUSV2_PIPENAME_FAILED_TO_SEAL_FILE_BECAUSE_1 =
      "IoTConsensusV2-PipeName-{}：封存文件 {} 失败，原因：当前写入文件为 {}。";
  public static final String IOTCONSENSUSV2_PIPENAME_FAILED_TO_SEAL_FILE_BECAUSE_2 =
      "IoTConsensusV2-PipeName-{}：封存文件 {} 失败，原因：{}。";
  public static final String IOTCONSENSUSV2_PIPENAME_FAILED_TO_SEAL_FILE_FROM =
      "IoTConsensusV2-PipeName-{}：封存文件 {}（来自请求 {}）失败。";
  public static final String IOTCONSENSUSV2_PIPENAME_FAILED_TO_SEAL_FILE_STATUS =
      "IoTConsensusV2-PipeName-{}：封存文件 {} 失败，状态为 {}。";
  public static final String IOTCONSENSUSV2_PIPENAME_FAILED_TO_SEAL_FILE_WHEN =
      "IoTConsensusV2-PipeName-{}：检查最终封存文件时封存文件 {} 失败，原因：文件长度不正确。"
          + "原始文件长度为 {}，接收文件长度为 {}。";
  public static final String IOTCONSENSUSV2_PIPENAME_FAILED_TO_SEAL_FILE_WHEN_1 =
      "IoTConsensusV2-PipeName-{}：检查非最终封存时封存文件 {} 失败，原因：文件长度不正确。"
          + "原始文件长度为 {}，接收文件长度为 {}。";
  public static final String IOTCONSENSUSV2_PIPENAME_FAILED_TO_WRITE_FILE_PIECE =
      "IoTConsensusV2-PipeName-{}：从请求 {} 写入文件片段失败。";
  public static final String IOTCONSENSUSV2_PIPENAME_FILE_OFFSET_RESET_REQUESTED_BY =
      "IoTConsensusV2-PipeName-{}：接收端请求重置文件偏移量，响应状态 = {}。";
  public static final String IOTCONSENSUSV2_PIPENAME_ILLEGAL_FILE_NAME_WHEN_CHECKING =
      "IoTConsensusV2-PipeName-{}：检查写入文件时发现非法文件名 {}。";
  public static final String IOTCONSENSUSV2_PIPENAME_IS_NOT_EXISTED_NO_NEED =
      "IoTConsensusV2-PipeName-{}：{} {} 不存在，无需删除。";
  public static final String IOTCONSENSUSV2_PIPENAME_NO_EVENT_GET_EXECUTED_AFTER =
      "IoTConsensusV2-PipeName-{}：等待超时后执行第 {} 个事件，当前接收端同步索引：{}";
  public static final String IOTCONSENSUSV2_PIPENAME_NO_EVENT_GET_EXECUTED_BECAUSE =
      "IoTConsensusV2-PipeName-{}：执行第 {} 个事件，原因：接收端缓冲区长度已达到 pipeline 阈值，"
          + "当前接收端同步索引 {}，当前缓冲区长度 {}";
  public static final String IOTCONSENSUSV2_PIPENAME_PATH_TRAVERSAL_ATTEMPT_DETECTED_FILENAME =
      "IoTConsensusV2-PipeName-{}：检测到路径遍历尝试！文件名：{}";
  public static final String IOTCONSENSUSV2_PIPENAME_PROCESS_NO_EVENT_SUCCESSFULLY =
      "IoTConsensusV2-PipeName-{}：处理第 {} 个事件成功。";
  public static final String IOTCONSENSUSV2_PIPENAME_RECEIVED_A_DEPRECATED_REQUEST_WHICH =
      "IoTConsensusV2-PipeName-{}：收到已弃用的请求-{}，可能原因：{}。";
  public static final String IOTCONSENSUSV2_PIPENAME_RECEIVER_DETECTED_AN_NEWER_PIPETASKRESTARTTIMES =
      "IoTConsensusV2-PipeName-{}：接收端检测到更新的 pipeTaskRestartTimes，表示 pipe 任务已重启。"
          + "接收端将重置其所有数据。";
  public static final String IOTCONSENSUSV2_PIPENAME_RECEIVER_DETECTED_AN_NEWER_REBOOTTIMES =
      "IoTConsensusV2-PipeName-{}：接收端检测到更新的 rebootTimes，表示主节点已重启。接收端将重置其所有数据。";
  public static final String IOTCONSENSUSV2_PIPENAME_RECEIVER_FILE_DIR_WAS_CREATED =
      "IoTConsensusV2-PipeName-{}：接收文件目录 {} 已创建。";
  public static final String IOTCONSENSUSV2_PIPENAME_RECEIVER_THREAD_GET_INTERRUPTED_WHEN =
      "IoTConsensusV2-PipeName-{}：接收线程退出时被中断。";
  public static final String IOTCONSENSUSV2_PIPENAME_SEAL_FILE_SUCCESSFULLY =
      "IoTConsensusV2-PipeName-{}：成功封存文件 {}。";
  public static final String IOTCONSENSUSV2_PIPENAME_SEAL_FILE_WITH_MODS_SUCCESSFULLY =
      "IoTConsensusV2-PipeName-{}：成功封存包含 mods 的文件 {}。";
  public static final String IOTCONSENSUSV2_PIPENAME_SKIP_LOAD_TSFILE_WHEN_SEALING =
      "IoTConsensusV2-PipeName-{}：封存时跳过加载 TsFile-{}，因为该 Region 已被移除或迁移。";
  public static final String IOTCONSENSUSV2_PIPENAME_STARTING_TO_RECEIVE_TSFILE_PIECES =
      "IoTConsensusV2-PipeName-{}：开始接收 TsFile 片段";
  public static final String IOTCONSENSUSV2_PIPENAME_STARTING_TO_RECEIVE_TSFILE_SEAL =
      "IoTConsensusV2-PipeName-{}：开始接收 TsFile 封存请求";
  public static final String IOTCONSENSUSV2_PIPENAME_STARTING_TO_RECEIVE_TSFILE_SEAL_1 =
      "IoTConsensusV2-PipeName-{}：开始接收带 mods 的 TsFile 封存请求";
  public static final String IOTCONSENSUSV2_PIPENAME_START_TO_RECEIVE_NO_EVENT =
      "IoTConsensusV2-PipeName-{}：开始接收第 {} 个事件";
  public static final String IOTCONSENSUSV2_PIPENAME_THE_POINT_COUNT_OF_TSFILE =
      "IoTConsensusV2-PipeName-{}：发送端未给出 TsFile {} 的点数，将从 TsFile 读取实际点数。";
  public static final String IOTCONSENSUSV2_PIPENAME_TSFILEWRITER_RETURNED_SELF =
      "IoTConsensusV2-PipeName-{}：TsFileWriter-{} 已归还自身";
  public static final String IOTCONSENSUSV2_PIPENAME_TSFILEWRITER_ROLL_TO_WRITING_PATH =
      "IoTConsensusV2-PipeName-{}：TsFileWriter-{} 已切换至写入路径 {}";
  public static final String IOTCONSENSUSV2_PIPENAME_TSFILE_WRITER_IS_CLEANED_UP =
      "IoTConsensusV2-PipeName-{}：TsFileWriter-{} 已被清理，原因：长时间未收到新请求。";
  public static final String IOTCONSENSUSV2_PIPENAME_UNKNOWN_PIPEREQUESTTYPE_RESPONSE_STATUS =
      "IoTConsensusV2-PipeName-{}：未知的 PipeRequestType，响应状态 = {}。";
  public static final String IOTCONSENSUSV2_PIPENAME_WAS_DELETED =
      "IoTConsensusV2-PipeName-{}：{} {} 已删除。";
  public static final String IOTCONSENSUSV2_PIPENAME_WRITING_FILE_IS_NOT_AVAILABLE =
      "IoTConsensusV2-PipeName-{}：写入文件 {} 不可用。写入文件为空：{}，写入文件存在：{}，"
          + "写入文件的 writer 为空：{}。";
  public static final String IOTCONSENSUSV2_PIPENAME_WRITING_FILE_IS_NOT_EXISTED =
      "IoTConsensusV2-PipeName-{}：写入文件 {} 不存在或名称不正确，尝试创建。当前写入文件为 {}。";
  public static final String IOTCONSENSUSV2_PIPENAME_WRITING_FILE_S_OFFSET_IS =
      "IoTConsensusV2-PipeName-{}：写入文件 {} 的偏移量为 {}，但请求发送端的偏移量为 {}。";
  public static final String IOTCONSENSUSV2_PIPENAME_WRITING_FILE_WAS_CREATED_READY =
      "IoTConsensusV2-PipeName-{}：写入文件 {} 已创建。准备写入文件片段。";
  public static final String IOTCONSENSUSV2_RECEIVE_ON_THE_FLY_NO_EVENT =
      "IoTConsensusV2-{}：DataRegion 已删除后收到实时传输的第 {} 个事件，将丢弃该事件";
  public static final String IOTCONSENSUSV2_TRANSFER_BATCH_HASN_T_BEEN_IMPLEMENTED =
      "IoTConsensusV2 批量传输尚未实现。";
  public static final String IOTCONSENSUSV2_TSFILEWRITER_SET_NULL_WRITING_FILE =
      "IoTConsensusV2-{}：TsFileWriter-{} 将写入文件设为空";
  public static final String IOTCONSENSUSV2_TSFILEWRITER_SET_NULL_WRITING_FILE_WRITER =
      "IoTConsensusV2-{}：TsFileWriter-{} 将写入文件的 writer 设为空";
  public static final String IOTCONSENSUSV2_UNKNOWN_IOTCONSENSUSV2REQUESTVERSION_RESPONSE_STATUS =
      "IoTConsensusV2：未知的 IoTConsensusV2RequestVersion，响应状态 = {}。";
  public static final String IOTCONSENSUSV2_UNKNOWN_PIPEREQUESTTYPE_RESPONSE_STATUS =
      "IoTConsensusV2：未知的 PipeRequestType，响应状态 = {}。";
  public static final String IOTCONSENSUSV2_WAITING_FOR_THE_PREVIOUS_EVENT_TIMES =
      "IoTConsensusV2-{}：等待上一个事件超时，当前队首元素：{}，当前 ID：{}";
  public static final String IOTDBAIRGAPRECEIVERAGENT_STARTED =
      "IoTDBAirGapReceiverAgent {} 已启动。";
  public static final String IOTDBAIRGAPRECEIVERAGENT_STOPPED =
      "IoTDBAirGapReceiverAgent {} 已停止。";
  public static final String LOAD_ACTIVE_LISTENING_PIPE_DIR_IS_NOT =
      "未设置加载 active listening pipe 目录。";
  public static final String LOAD_PIPEDATA_WITH_SERIALIZE_NUMBER_SUCCESSFULLY =
      "成功加载序列号为 {} 的 pipeData。";
  public static final String LOAD_TSFILE_ERROR_STATEMENT = "加载 TsFile {} 出错，语句：{}。";
  public static final String LOAD_TSFILE_RESULT_STATUS = "加载 TsFile 结果状态：{}。";
  public static final String PARSE_DATABASE_PARTIALPATH_ERROR = "解析数据库 PartialPath {} 出错。";
  public static final String PIPE_AIR_GAP_RECEIVER_CHECKSUM_FAILED_EXPECTED =
      "Pipe air gap receiver {}：校验和校验失败，期望值：{}，实际值：{}";
  public static final String PIPE_AIR_GAP_RECEIVER_CLOSED_BECAUSE_OF =
      "Pipe air gap receiver {} 因校验和校验失败而关闭。Socket：{}";
  public static final String PIPE_AIR_GAP_RECEIVER_CLOSED_BECAUSE_OF_1 =
      "Pipe air gap receiver {} 因异常而关闭。Socket：{}";
  public static final String PIPE_AIR_GAP_RECEIVER_CLOSED_BECAUSE_SOCKET =
      "Pipe air gap receiver {} 因 socket 已关闭而关闭。Socket：{}";
  public static final String PIPE_AIR_GAP_RECEIVER_EXCEPTION_DURING_HANDLING =
      "Pipe air gap receiver {}：处理接收数据时发生异常。Socket：{}";
  public static final String PIPE_AIR_GAP_RECEIVER_HANDLE_DATA_FAILED =
      "Pipe air gap receiver {}：处理数据失败，状态：{}，请求：{}";
  public static final String PIPE_AIR_GAP_RECEIVER_SOCKET_CLOSED_WHEN =
      "Pipe air gap receiver {}：监听数据时 Socket {} 关闭。原因：{}";
  public static final String PIPE_AIR_GAP_RECEIVER_STARTED_SOCKET =
      "Pipe air gap receiver {} 已启动。Socket：{}";
  public static final String PIPE_AIR_GAP_RECEIVER_TEMPORARY_UNAVAILABLE_RETRY =
      "Pipe air gap receiver {}：临时不可用重试超时，向发送端返回 FAIL。";
  public static final String PIPE_AIR_GAP_RECEIVER_TSSTATUS_IS_ENCOUNTERED =
      "Pipe air gap receiver {}：air gap receiver 遇到 TSStatus {}，将忽略。";
  public static final String PIPE_DATA_TRANSPORT_ERROR = "Pipe 数据传输错误，{}";
  public static final String PIPE_INSERTING_TABLET_TO_CASTING_TYPE_FROM =
      "Pipe：正在向 {}.{} 插入 tablet。将类型从 {} 转换为 {}。";
  public static final String RECEIVERS_EXECUTOR_IS_CLOSED = "Receivers-{} 的执行器已关闭。";
  public static final String RECEIVER_EXIT_SUCCESSFULLY = "Receiver-{} 退出成功。";
  public static final String RECEIVER_ID = "接收器 id = {}：{}";
  public static final String RECEIVER_ID_THE_NUMBER_OF_DEVICE_PATHS =
      "接收器 id = {}：device path 数量与语句 {} 中的 sub-status 数量不相等：{}。";
  public static final String RECEIVER_ID_UNKNOWN_PIPEREQUESTTYPE_RESPONSE_STATUS =
      "接收器 id = {}：未知的 PipeRequestType，响应状态 = {}。";
  public static final String RECEIVER_ID_UNSUPPORTED_STATEMENT_TYPE_FOR_REDIRECTION =
      "接收器 id = {}：不支持用于重定向的 statement 类型 {}。";
  public static final String RECEIVER_IS_READY = "Receiver-{} 已就绪";
  public static final String REGISTER_WITH_INTERVAL_IN_SECONDS_SUCCESSFULLY =
      "成功注册 {}，间隔为 {} 秒。";
  public static final String SOCKET_CLOSED_WHEN_EXECUTING_READTILLFULL =
      "执行 readTillFull 时 Socket 已关闭。";
  public static final String SOCKET_CLOSED_WHEN_EXECUTING_SKIPTILLENOUGH =
      "执行 skipTillEnough 时 Socket 已关闭。";
  public static final String START_LOAD_PIPEDATA_WITH_SERIALIZE_NUMBER_AND =
      "开始加载序列号为 {}、类型为 {}、值为 {} 的 pipeData";
  public static final String STORAGE_ENGINE_READONLY = "存储引擎只读";
  public static final String SYNC_START_AT_TO_IS_DONE = "同步 {} 从 {} 到 {} 已完成。";
  public static final String TEMPORARY_UNAVAILABLE_EXCEPTION_ENCOUNTERED_AT_AIR_GAP =
      "air gap receiver 遇到临时不可用异常，将在本地重试。";
  public static final String THE_IOTCONSENSUSV2_REQUEST_VERSION_IS_DIFFERENT_FROM =
      "IoTConsensusV2 请求版本 {} 与发送端请求版本 {} 不同，接收端将重置为发送端请求版本。";
  public static final String THE_START_INDEX_OF_DATA_SYNC_IS =
      "数据同步起始索引 {} 无效。文件不存在，起始索引应等于 0。";
  public static final String THE_START_INDEX_OF_DATA_SYNC_IS_1 =
      "数据同步起始索引 {} 无效。文件的起始索引应等于 {}。";
  public static final String THRIFT_CONNECTION_IS_NOT_ALIVE = "Thrift 连接已断开。";
  public static final String TSFILECHECKER_DID_NOT_TERMINATE_WITHIN_S =
      "TsFileChecker 未在 {} 秒内终止";
  public static final String TSFILECHECKER_THREAD_STILL_DOESN_T_EXIT_AFTER =
      "TsFileChecker 线程 {} 在 30 秒后仍未退出";
  public static final String UNHANDLED_EXCEPTION_DURING_PIPE_AIR_GAP_RECEIVER =
      "pipe air gap receiver 监听期间发生未处理异常";
  public static final String UNSUPPORTED_DATA_TYPE = "不支持的数据类型：";

  // ===================== RESOURCE =====================

  public static final String CANNOT_GET_DATA_REGION_IDS_USE_DEFAULT =
      "无法获取 data region ID，使用默认锁分段大小：{}";
  public static final String EXPAND_CALLBACK_IS_NOT_SUPPORTED_IN_PIPEFIXEDMEMORYBLOCK =
      "PipeFixedMemoryBlock 不支持扩展回调";
  public static final String EXPAND_METHOD_IS_NOT_SUPPORTED_IN_PIPEFIXEDMEMORYBLOCK =
      "PipeFixedMemoryBlock 不支持扩展方法";
  public static final String FAILED_TO_CACHEDEVICEISALIGNEDMAPIFABSENT_FOR_TSFILE_BECAUSE_MEMORY =
      "对 TsFile {} 执行 cacheDeviceIsAlignedMapIfAbsent 失败，原因：内存使用率过高";
  public static final String FAILED_TO_CACHEOBJECTSIFABSENT_FOR_TSFILE_BECAUSE_MEMORY =
      "对 TsFile {} 执行 cacheObjectsIfAbsent 失败，原因：内存使用率过高";
  public static final String FAILED_TO_ESTIMATE_SIZE_FOR_INSERTNODE =
      "估算 InsertNode 大小失败：{}";
  public static final String FAILED_TO_EXECUTE_THE_EXPAND_CALLBACK =
      "执行扩展回调失败。";
  public static final String FAILED_TO_EXECUTE_THE_SHRINK_CALLBACK =
      "执行收缩回调失败。";
  public static final String FAILED_TO_GET_FILE_SIZE_OF_LINKED =
      "获取链接 TsFile {} 的文件大小失败：";
  public static final String FORCEALLOCATEWITHRETRY_INTERRUPTED_WHILE_WAITING_FOR_AVAILABLE_MEMORY =
      "forceAllocateWithRetry：等待可用内存时被中断";
  public static final String FORCEALLOCATE_INTERRUPTED_WHILE_WAITING_FOR_AVAILABLE_MEMORY =
      "forceAllocate：等待可用内存时被中断";
  public static final String FORCERESIZE_CANNOT_RESIZE_A_NULL_OR_RELEASED =
      "forceResize：不能 resize 空内存块或已释放内存块";
  public static final String FORCERESIZE_INTERRUPTED_WHILE_WAITING_FOR_AVAILABLE_MEMORY =
      "forceResize：等待可用内存时被中断";
  public static final String INTERRUPTED_WHILE_WAITING_FOR_THE_LOCK = "等待锁时被中断。";
  public static final String IS_RELEASED_AFTER_THREAD_INTERRUPTION =
      "{} 在线程中断后被释放。";
  public static final String PIPEPERIODICALLOGREDUCER_IS_ALLOCATED_TO_BYTES =
      "PipePeriodicalLogReducer 已分配 {} 字节。";
  public static final String PIPETSFILERESOURCE_CACHED_DEVICEISALIGNEDMAP_FOR_TSFILE =
      "PipeTsFileResource：已为 TsFile {} 缓存 deviceIsAlignedMap。";
  public static final String PIPETSFILERESOURCE_CACHED_OBJECTS_FOR_TSFILE =
      "PipeTsFileResource：已为 TsFile {} 缓存对象。";
  public static final String PIPETSFILERESOURCE_CLOSED_TSFILE_AND_CLEANED_UP =
      "PipeTsFileResource：已关闭 TsFile {} 并完成清理。";
  public static final String PIPETSFILERESOURCE_FAILED_TO_CACHE_OBJECTS_FOR_TSFILE =
      "PipeTsFileResource：将 TsFile {} 的对象写入缓存失败，原因：内存使用率过高";
  public static final String PIPETSFILERESOURCE_FAILED_TO_DELETE_TSFILE_WHEN_CLOSING =
      "PipeTsFileResource：关闭时删除 TsFile {} 失败，原因：{}。请手动删除该文件。";
  public static final String PIPETSFILERESOURCE_S_REFERENCE_COUNT_IS_DECREASED_TO =
      "PipeTsFileResource 的引用计数已减少到 0 以下。";
  public static final String PIPE_HARDLINK_DIR_FOUND_DELETING_IT_RESULT =
      "发现 Pipe 硬链接目录，正在删除：{}，结果：{}";
  public static final String PIPE_SNAPSHOT_DIR_FOUND_DELETING_IT =
      "发现 Pipe 快照目录，正在删除：{}，";
  public static final String SHRINK_CALLBACK_IS_NOT_SUPPORTED_IN_PIPEFIXEDMEMORYBLOCK =
      "PipeFixedMemoryBlock 不支持收缩回调";
  public static final String SHRINK_METHOD_IS_NOT_SUPPORTED_IN_PIPEFIXEDMEMORYBLOCK =
      "PipeFixedMemoryBlock 不支持收缩方法";
  public static final String THE_MEMORY_BLOCK_HAS_BEEN_RELEASED = "内存块已被释放";
  public static final String THE_MULTIPLE_N_MUST_BE_GREATER_THAN =
      "倍数 n 必须大于 0";
  public static final String TRYALLOCATE_ALLOCATED_MEMORY_TOTAL_MEMORY_SIZE_BYTES =
      "tryAllocate：已分配内存，总内存大小 {} 字节，已用内存大小 {} 字节，"
          + "原始请求内存大小 {} 字节，实际请求内存大小 {} 字节";
  public static final String TRYALLOCATE_FAILED_TO_ALLOCATE_MEMORY_TOTAL_MEMORY =
      "tryAllocate：分配内存失败，总内存大小 {} 字节，已用内存大小 {} 字节，请求内存大小 {} 字节";
  public static final String TRYEXPANDALLANDCHECKCONSISTENCY_MEMORY_USAGE_IS_NOT_CONSISTENT_WITH =
      "tryExpandAllAndCheckConsistency：内存使用量与已分配块不一致，usedMemorySizeInBytes 为 {}，"
          + "但所有块的总和为 {}";
  public static final String TRYEXPANDALLANDCHECKCONSISTENCY_MEMORY_USAGE_OF_TABLETS_IS_NOT =
      "tryExpandAllAndCheckConsistency：tablet 内存使用量与已分配块不一致，"
          + "usedMemorySizeInBytesOfTablets 为 {}，但所有 tablet 块的总和为 {}";
  public static final String TRYEXPANDALLANDCHECKCONSISTENCY_MEMORY_USAGE_OF_TSFILES_IS_NOT =
      "tryExpandAllAndCheckConsistency：TsFile 内存使用量与已分配块不一致，"
          + "usedMemorySizeInBytesOfTsFiles 为 {}，但所有 TsFile 块的总和为 {}";

  // ===================== METRIC =====================

  public static final String FAILED_TO_DEREGISTER_PIPE_ASSIGNER_METRICS_PIPEDATAREGIONASSIGNER =
      "注销 pipe assigner 指标失败，PipeDataRegionAssigner({}) 不存在";
  public static final String FAILED_TO_DEREGISTER_PIPE_DATA_REGION_EXTRACTOR =
      "注销 pipe data region extractor 指标失败，IoTDBDataRegionExtractor({}) 不存在";
  public static final String FAILED_TO_DEREGISTER_PIPE_DATA_REGION_SINK =
      "注销 pipe data region sink 指标失败，PipeSinkSubtask({}) 不存在";
  public static final String FAILED_TO_DEREGISTER_PIPE_REMAINING_EVENT_AND =
      "注销 pipe remaining event and time 指标失败，RemainingEventAndTimeOperator({}) 不存在";
  public static final String FAILED_TO_DEREGISTER_PIPE_SCHEMA_REGION_CONNECTOR =
      "注销 pipe schema region connector 指标失败，PipeConnectorSubtask({}) 不存在";
  public static final String FAILED_TO_DEREGISTER_PIPE_SCHEMA_REGION_SOURCE =
      "注销 pipe schema region source 指标失败，IoTDBSchemaRegionSource({}) 不存在";
  public static final String SKIP_DEREGISTER_PIPE_TSFILE_TO_TABLETS =
      "跳过注销 pipe tsfile to tablets 指标，因为 pipeID({}) 未注册";
  public static final String FAILED_TO_DEREGISTER_SCHEMA_REGION_LISTENER_METRICS =
      "注销 schema region listener 指标失败，SchemaRegionListeningQueue({}) 不存在";
  public static final String FAILED_TO_MARK_PIPE_DATA_REGION_EXTRACTOR =
      "标记 pipe data region extractor 心跳事件失败，IoTDBDataRegionExtractor({}) 不存在";
  public static final String FAILED_TO_MARK_PIPE_DATA_REGION_EXTRACTOR_1 =
      "标记 pipe data region extractor tablet 事件失败，IoTDBDataRegionExtractor({}) 不存在";
  public static final String FAILED_TO_MARK_PIPE_DATA_REGION_EXTRACTOR_2 =
      "标记 pipe data region extractor TsFile 事件失败，IoTDBDataRegionExtractor({}) 不存在";
  public static final String FAILED_TO_MARK_PIPE_DATA_REGION_SINK =
      "标记 pipe data region sink tablet 事件失败，PipeSinkSubtask({}) 不存在";
  public static final String FAILED_TO_MARK_PIPE_DATA_REGION_SINK_1 =
      "标记 pipe data region sink TsFile 事件失败，PipeSinkSubtask({}) 不存在";
  public static final String FAILED_TO_MARK_PIPE_PROCESSOR_HEARTBEAT_EVENT =
      "标记 pipe processor 心跳事件失败，PipeProcessorSubtask({}) 不存在";
  public static final String FAILED_TO_MARK_PIPE_PROCESSOR_TABLET_EVENT =
      "标记 pipe processor tablet 事件失败，PipeProcessorSubtask({}) 不存在";
  public static final String FAILED_TO_MARK_PIPE_PROCESSOR_TSFILE_EVENT =
      "标记 pipe processor TsFile 事件失败，PipeProcessorSubtask({}) 不存在";
  public static final String FAILED_TO_MARK_PIPE_REGION_COMMIT_REMAININGEVENTANDTIMEOPERATOR =
      "标记 pipe region commit 失败，RemainingEventAndTimeOperator({}) 不存在";
  public static final String FAILED_TO_MARK_PIPE_SCHEMA_REGION_WRITE =
      "标记 pipe schema region write plan 事件失败，PipeConnectorSubtask({}) 不存在";
  public static final String FAILED_TO_MARK_PIPE_TSFILE_TO_TABLETS =
      "标记 pipe tsfile to tablets 调用失败，pipeID({}) 不存在";
  public static final String FAILED_TO_RECORD_PIPE_TSFILE_TO_TABLETS =
      "记录 pipe tsfile to tablets 耗时失败，pipeID({}) 不存在";
  public static final String FAILED_TO_RECORD_TABLET_GENERATED_PIPEID_DOES =
      "记录 tablet generated 失败，pipeID({}) 不存在";
  public static final String FAILED_TO_SET_RECENT_PROCESSED_TSFILE_EPOCH =
      "设置最近处理的 TsFile epoch 状态失败，PipeRealtimeDataRegionExtractor({}) 不存在";
  public static final String FAILED_TO_UNBIND_FROM_PIPE_ASSIGNER_METRICS =
      "从 pipe assigner 指标解绑失败，assigner map 非空";
  public static final String FAILED_TO_UNBIND_FROM_PIPE_DATA_REGION =
      "从 pipe data region sink 指标解绑失败，sink map 非空";
  public static final String FAILED_TO_UNBIND_FROM_PIPE_EXTRACTOR_METRICS =
      "从 pipe extractor 指标解绑失败，extractor map 非空";
  public static final String FAILED_TO_UNBIND_FROM_PIPE_PROCESSOR_METRICS =
      "从 pipe processor 指标解绑失败，processor map 非空";
  public static final String FAILED_TO_UNBIND_FROM_PIPE_REMAINING_EVENT =
      "从 pipe remaining event and time 指标解绑失败，RemainingEventAndTimeOperator map 非空";
  public static final String FAILED_TO_UNBIND_FROM_PIPE_SCHEMA_REGION =
      "从 pipe schema region connector 指标解绑失败，connector map 非空";
  public static final String FAILED_TO_UNBIND_FROM_PIPE_SCHEMA_REGION_1 =
      "从 pipe schema region extractor 指标解绑失败，extractor map 非空";
  public static final String FAILED_TO_UNBIND_FROM_PIPE_SCHEMA_REGION_2 =
      "从 pipe schema region listener 指标解绑失败，listening queue map 非空";
  public static final String FAILED_TO_UNBIND_FROM_PIPE_TSFILE_TO =
      "从 pipe tsfile to tablets 指标解绑失败，pipe map 非空，pipe：{}";

  // ---------------------------------------------------------------------------
  // pipe – AbstractSameTypeNumericOperator
  // ---------------------------------------------------------------------------
  public static final String UNSUPPORTED_OUTPUT_DATATYPE_FMT = "不支持的输出数据类型 %s";

  // ---------------------------------------------------------------------------
  // pipe – IoTDBDataRegionSource
  // ---------------------------------------------------------------------------
  public static final String ILLEGAL_TREE_PATTERN_FMT = "Pattern \"%s\" 非法。";

  // ---------------------------------------------------------------------------
  // pipe – OpcUaServerBuilder
  // ---------------------------------------------------------------------------
  public static final String UNABLE_CREATE_SECURITY_DIR = "无法创建安全目录：";

  // ---------------------------------------------------------------------------
  // pipe – PipeDataNodePluginAgent
  // ---------------------------------------------------------------------------
  public static final String PLUGIN_NOT_REGISTERED_FMT = "插件 %s 未注册。";

  // ---------------------------------------------------------------------------
  // pipe – PipeTransferTrackableHandler
  // ---------------------------------------------------------------------------
  public static final String TPIPE_TRANSFER_RESP_IS_NULL_WHEN_TRANSFERRING_SLICE =
      "传输分片时 TPipeTransferResp 为空。";

  private DataNodePipeMessages() {}
}
