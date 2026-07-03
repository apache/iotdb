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
      "DeletionManager-{}：current DAL dir {} 已成功删除";
  public static final String DELETIONMANAGER_CURRENT_DAL_DIR_IS_NOT_INITIALIZED =
      "DeletionManager-{}：current DAL dir {} 未初始化，无需删除。";
  public static final String DELETIONMANAGER_CURRENT_WAITING_IS_INTERRUPTED_MAY_BECAUSE =
      "DeletionManager-{}：current waiting is interrupted. May because current application is "
          + "down. ";
  public static final String DELETIONMANAGER_DELETE_DELETION_FILE_IN_DIR =
      "DeletionManager-{} delete deletion file in {} dir...";
  public static final String DELETIONMANAGER_FAILED_TO_DELETE_FILE_IN_DIR =
      "DeletionManager-{} 删除 file in {} dir, please manually check! 失败";
  public static final String DELETIONRESOURCE_HAS_BEEN_RELEASED_TRIGGER_A_REMOVE =
      "DeletionResource {} 已释放，触发移除 DAL...";
  public static final String DELETION_PERSIST_CANNOT_CREATE_FILE_PLEASE_CHECK =
      "Deletion persist: Cannot create file {}, please check your file system manually.";
  public static final String DELETION_PERSIST_CANNOT_WRITE_TO_MAY_CAUSE =
      "Deletion persist: Cannot write to {}, may cause data inconsistency.";
  public static final String DELETION_PERSIST_CURRENT_BATCH_FSYNC_DUE_TO =
      "Deletion persist-{}：current batch fsync due to timeout";
  public static final String DELETION_PERSIST_CURRENT_FILE_HAS_BEEN_CLOSED =
      "Deletion persist-{}：current file 已关闭";
  public static final String DELETION_PERSIST_SERIALIZE_DELETION_RESOURCE =
      "Deletion persist-{}：serialize deletion resource {}";
  public static final String DELETION_PERSIST_STARTING_TO_PERSIST_CURRENT_WRITING =
      "Deletion persist-{}：starting to persist, current writing: {}";
  public static final String DELETION_PERSIST_SWITCHING_TO_A_NEW_FILE =
      "Deletion persist-{}：switching to a new file, current writing: {}";
  public static final String DELETION_RESOURCE_MANAGER_FOR_HAS_BEEN_SUCCESSFULLY =
      "{} 的删除资源管理器已成功关闭！";
  public static final String DETECT_FILE_CORRUPTED_WHEN_RECOVER_DAL_DISCARD =
      "recover DAL-{}, discard all subsequent DALs... 时检测到 file corrupted";
  public static final String FAILED_TO_INITIALIZE_DELETIONRESOURCEMANAGER =
      "初始化 DeletionResourceManager 失败";
  public static final String FAILED_TO_READ_DELETION_FILE_MAY_BECAUSE =
      "读取 deletion file {}, may 失败，原因：this file corrupted when writing it.";
  public static final String FAILED_TO_RECOVER_DELETIONRESOURCEMANAGER =
      "恢复 DeletionResourceManager 失败";
  public static final String FAIL_TO_ALLOCATE_DELETIONBUFFER_GROUP_S_BUFFER =
      "分配 deletionBuffer-group-{}'s buffer 失败，原因：out of memory.";
  public static final String FAIL_TO_CLOSE_CURRENT_LOGGING_FILE_WHEN = "关闭时无法关闭当前日志文件";
  public static final String FAIL_TO_REGISTER_DELETIONRESOURCE_INTO_DELETIONBUFFER_BECAUSE =
      "注册 DeletionResource into deletionBuffer-{} 失败，原因：this buffer is closed.";
  public static final String INTERRUPTED_WHEN_WAITING_FOR_ALL_DELETIONS_FLUSHED = "等待所有删除操作刷盘时被中断。";
  public static final String INTERRUPTED_WHEN_WAITING_FOR_RESULT = "等待结果时被中断。";
  public static final String INTERRUPTED_WHEN_WAITING_FOR_TAKING_DELETIONRESOURCE_FROM =
      "等待从阻塞队列中取出 DeletionResource 进行序列化时被中断。";
  public static final String INTERRUPTED_WHEN_WAITING_FOR_TAKING_WALENTRY_FROM =
      "等待从阻塞队列中取出 WALEntry 进行序列化时被中断。";
  public static final String INVALID_DELETION_PROGRESS_INDEX = "无效的删除进度索引：";
  public static final String PERSISTTHREAD_DID_NOT_TERMINATE_WITHIN_S = "persistThread 在 {} 秒内未终止";
  public static final String READ_DELETION_FILE_MAGIC_VERSION =
      "读取 deletion file-{} magic version: {}";
  public static final String READ_DELETION_FROM_FILE = "从 file {} 读取 deletion: {}";
  public static final String UNABLE_TO_CREATE_IOTCONSENSUSV2_DELETION_DIR_AT =
      "无法在 {} 创建 iotConsensusV2 删除目录";

  // ===================== AGENT =====================

  public static final String ATTEMPT_TO_REPORT_PIPE_EXCEPTION_TO_A =
      "尝试向空的 PipeTaskMeta 上报 pipe 异常。";
  public static final String CANNOT_PARSE_REBOOT_TIMES_FROM_FILE_SET =
      "无法解析 reboot times from file {}, set the current time in seconds ({}) as the reboot times";
  public static final String CANNOT_RECORD_REBOOT_TIMES_TO_FILE_THE =
      "无法记录 reboot times {} to file {}, the reboot times will not be updated";
  public static final String CANNOT_START_SIMPLEPROGRESSINDEXASSIGNER_BECAUSE_OF =
      "无法启动 SimpleProgressIndexAssigner because of {}";
  public static final String CREATE_PIPE_DN_TASK_SUCCESSFULLY_WITHIN_MS =
      "创建 pipe DN task {} 成功，耗时 {} ms";
  public static final String DEREGISTER_SUBTASK_RUNNINGTASKCOUNT_REGISTEREDTASKCOUNT =
      "注销子任务 {}。runningTaskCount: {}, registeredTaskCount: {}";
  public static final String DROP_PIPE_DN_TASK_SUCCESSFULLY_WITHIN_MS =
      "删除 pipe DN task {} 成功，耗时 {} ms";
  public static final String ERROR_OCCURRED_WHEN_COLLECTING_EVENTS_FROM_PROCESSOR =
      "collecting events from processor 时发生错误";
  public static final String EXCEPTION_IN_PIPE_EVENT_PROCESSING_IGNORED_BECAUSE =
      "pipe event processing, ignored because pipe is dropped.{} 中发生异常";
  public static final String EXCEPTION_OCCURRED_WHEN_CLOSING_PIPE_CONNECTOR_SUBTASK =
      "closing pipe connector subtask {}, root cause: {} 时发生异常";
  public static final String EXCEPTION_OCCURRED_WHEN_CLOSING_PIPE_PROCESSOR_SUBTASK =
      "closing pipe processor subtask {}, root cause: {} 时发生异常";
  public static final String EXCEPTION_OCCURS_WHEN_EXECUTING_PIPE_TASK =
      "executing pipe task:  时发生异常";
  public static final String FAILED_TO_CHECK_IF_PIPE_HAS_RELEASE =
      "check if pipe has release region related resource with consensus group id: {} 失败。";
  public static final String FAILED_TO_CLEAR_CLOSE_THE_SCHEMA_REGION =
      "Failed to clear/close the schema region listening queue, because {}. Will wait until "
          + "success or the region's state machine is stopped.";
  public static final String FAILED_TO_CLOSE_CONNECTOR_AFTER_FAILED_TO =
      "关闭 connector after failed to initialize connector. Ignore this exception 失败。";
  public static final String FAILED_TO_CLOSE_LISTENING_QUEUE_FOR_SCHEMAREGION =
      "关闭 listening queue for SchemaRegion  失败";
  public static final String FAILED_TO_CLOSE_SOURCE_AFTER_FAILED_TO =
      "关闭 source after failed to initialize source. Ignore this exception 失败。";
  public static final String FAILED_TO_CONSTRUCT_PIPECONNECTOR_BECAUSE_OF =
      "构造 PipeConnector 失败，原因：of ";
  public static final String FAILED_TO_DECREASE_REFERENCE_COUNT_FOR_EVENT =
      "减少 reference count for event {} in PipeRealtimePriorityBlockingQueue 失败";
  public static final String FAILED_TO_GET_PENDINGQUEUE_NO_SUCH_SUBTASK =
      "获取 PendingQueue. No such subtask:  失败";
  public static final String FAILED_TO_GET_PIPE_METAS_WILL_BE =
      "获取 pipe metas, will be synced by configNode later 失败。";
  public static final String FAILED_TO_GET_PIPE_PLUGIN_JAR_FROM =
      "获取 pipe plugin jar from config node 失败。";
  public static final String FAILED_TO_GET_PIPE_TASK_META_FROM =
      "获取 pipe task meta from config node. Ignore the exception 失败，原因：config node may not be "
          + "ready yet, and meta will be pushed by config node later.";
  public static final String FAILED_TO_PERSIST_PROGRESS_INDEX_TO_CONFIGNODE =
      "持久化进度索引到 ConfigNode 失败，状态：{}";
  public static final String SHUTDOWN_PROGRESS_NOT_CONFIRMED =
      "本次关闭流程中的进度未确认已持久化到 ConfigNode。";
  public static final String START_TO_PERSIST_ALL_PIPE_PROGRESS_INDEXES_DURING_SHUTDOWN =
      "开始在关闭期间持久化所有 Pipe 进度索引，Pipe 数量 {}，超时时间 {} ms";
  public static final String
      INTERRUPTED_WHILE_PERSISTING_ALL_PIPE_PROGRESS_INDEXES_DURING_SHUTDOWN =
          "在关闭期间持久化所有 Pipe 进度索引时被中断。"
              + SHUTDOWN_PROGRESS_NOT_CONFIRMED;
  public static final String
      TIMED_OUT_WHILE_PERSISTING_ALL_PIPE_PROGRESS_INDEXES_DURING_SHUTDOWN =
          "在关闭期间持久化所有 Pipe 进度索引超时，耗时 {} ms。"
              + SHUTDOWN_PROGRESS_NOT_CONFIRMED;
  public static final String FAILED_TO_PERSIST_ALL_PIPE_PROGRESS_INDEXES_DURING_SHUTDOWN =
      "在关闭期间持久化所有 Pipe 进度索引失败，耗时 {} ms。"
          + SHUTDOWN_PROGRESS_NOT_CONFIRMED;
  public static final String COLLECTED_PIPE_METAS_FOR_SHUTDOWN_PROGRESS_PERSIST =
      "已收集关闭期间进度持久化所需的 Pipe 元数据，Pipe 数量 {}，Pipe 元数据数量 {}，"
          + "Pipe 元数据大小 {} 字节，耗时 {} ms";
  public static final String COLLECTED_EMPTY_PIPE_METAS_DURING_SHUTDOWN =
      "关闭期间为 {} 个 Pipe 收集到空 Pipe 元数据。";
  public static final String START_TO_PUSH_HEARTBEAT_SHUTDOWN_PIPE_META_TO_CONFIGNODE =
      "开始向 ConfigNode 推送关闭期间的 Pipe 元数据心跳，DataNode ID {}，Pipe 数量 {}，"
          + "Pipe 元数据数量 {}，Pipe 元数据大小 {} 字节";
  public static final String FAILED_TO_PUSH_HEARTBEAT_SHUTDOWN_PIPE_META_TO_CONFIGNODE =
      "向 ConfigNode 推送关闭期间的 Pipe 元数据心跳失败，状态 {}，耗时 {} ms。"
          + SHUTDOWN_PROGRESS_NOT_CONFIRMED;
  public static final String
      SUCCESSFULLY_FINISHED_PUSH_HEARTBEAT_SHUTDOWN_PIPE_META_TO_CONFIGNODE =
          "成功向 ConfigNode 推送关闭期间的 Pipe 元数据心跳，Pipe 数量 {}，Pipe 元数据数量 {}，"
              + "Pipe 元数据大小 {} 字节，耗时 {} ms";
  public static final String
      EXCEPTION_OCCURRED_WHILE_PERSISTING_ALL_PIPE_PROGRESS_INDEXES_DURING_SHUTDOWN =
          "在关闭期间持久化所有 Pipe 进度索引时发生异常。"
              + SHUTDOWN_PROGRESS_NOT_CONFIRMED;
  public static final String PERSISTING_PIPE_PROGRESS_INDEXES_BEFORE_SHUTDOWN =
      "关闭前正在持久化 Pipe 进度索引，超时时间 {} ms。";
  public static final String PIPE_PROGRESS_INDEXES_WERE_NOT_CONFIRMED_DURING_SHUTDOWN =
      "关闭期间 Pipe 进度索引未被 ConfigNode 确认。"
          + SHUTDOWN_PROGRESS_NOT_CONFIRMED;
  public static final String FAILURE_WHEN_REGISTER_PIPE_PLUGIN_SKIP_THIS =
      "注册 pipe plugin {} 失败。将跳过该插件并继续启动。";
  public static final String
      FAILED_TO_REGISTER_PIPE_PLUGIN_BECAUSE_NAME_CONFLICTS_WITH_BUILTIN =
          "注册 PipePlugin %s 失败，因为给定的 PipePlugin 名称与内置 PipePlugin 名称重复。";
  public static final String
      FAILED_TO_REGISTER_PIPE_PLUGIN_BECAUSE_INSTANCE_CONSTRUCTION_FAILED =
          "注册 PipePlugin %s(%s) 失败，因为其实例无法成功构造。异常：%s";
  public static final String FAILED_TO_REGISTER_PIPE_PLUGIN_BECAUSE_JAR_MD5_MISMATCH =
      "注册 PipePlugin %s 失败，因为 pipe plugin %s 已存在的 jar 文件 MD5 与新的 jar 文件不同。";
  public static final String FAILED_TO_DEREGISTER_BUILTIN_PIPE_PLUGIN =
      "注销内置 PipePlugin %s 失败。";
  public static final String PIPECONNECTOR = "PipeConnector: ";
  public static final String PIPEDATANODETASKBUILDER_FAILED_TO_PARSE_INCLUSION_AND_EXCLUSION =
      "PipeDataNodeTaskBuilder failed to parse 'inclusion' and 'exclusion' parameters: {}";
  public static final String PIPEDATANODETASKBUILDER_WHEN_INCLUSION_CONTAINS_DATA_DELETE_REALTIME =
      "PipeDataNodeTaskBuilder: When 'inclusion' contains 'data.delete', 'realtime-first' is "
          + "defaulted to 'false' to prevent sync issues after deletion.";
  public static final String PIPEDATANODETASKBUILDER_WHEN_INCLUSION_INCLUDES_DATA_DELETE_REALTIME =
      "PipeDataNodeTaskBuilder: When 'inclusion' includes 'data.delete', 'realtime-first' set "
          + "to 'true' may result in data synchronization issues after deletion.";
  public static final String PIPEDATANODETASKBUILDER_WHEN_SOURCE_USES_SNAPSHOT_MODEL_REALTIME =
      "PipeDataNodeTaskBuilder: When source uses snapshot model, 'realtime-first' is defaulted "
          + "to 'false' to prevent premature halt before transfer completion.";
  public static final String PIPEDATANODETASKBUILDER_WHEN_SOURCE_USES_SNAPSHOT_MODEL_REALTIME_1 =
      "PipeDataNodeTaskBuilder: When source uses snapshot model, 'realtime-first' set to "
          + "'true' may cause prevent premature halt before transfer completion.";
  public static final String PIPEDATANODETASKBUILDER_WHEN_THE_REALTIME_SYNC_IS_ENABLED =
      "PipeDataNodeTaskBuilder: When the realtime sync is enabled, not enabling the rate "
          + "limiter in sending tsfile may introduce delay for realtime sending.";
  public static final String PIPEDATANODETASKBUILDER_WHEN_THE_REALTIME_SYNC_IS_ENABLED_1 =
      "PipeDataNodeTaskBuilder: When the realtime sync is enabled, we enable rate limiter in "
          + "sending tsfile by default to reserve disk and network IO for realtime sending.";
  public static final String PIPEEVENTCOLLECTOR_THE_EVENT_IS_ALREADY_RELEASED_SKIPPING =
      "PipeEventCollector：事件 {} 已被释放，跳过处理。";
  public static final String PIPE_CONNECTOR_SUBTASK_WAS_CLOSED_WITHIN_MS =
      "Pipe：connector subtask {} ({}) 已关闭 within {} ms";
  public static final String PIPE_META_NOT_FOUND = "未找到 pipe 元数据：";
  public static final String PIPE_SINK_SUBTASKS_WITH_ATTRIBUTES_IS_BOUNDED =
      "Pipe sink subtasks with attributes {} is bounded with sinkExecutor {} and "
          + "callbackExecutor {}.";
  public static final String PIPE_SINK_SUBTASK_DELAYED_TO_AVOID_FREQUENT_HANDSHAKES =
      "Pipe sink 子任务 {} 在拉取事件前延迟 {} ms，以避免客户端借用失败后频繁握手。";
  public static final String PIPE_SKIPPING_TEMPORARY_TSFILE_WHICH_SHOULDN_T =
      "Pipe 跳过不应传输的临时 TsFile：{}";
  public static final String PULLED_PIPE_META_FROM_CONFIG_NODE_RECOVERING =
      "已从 config node 拉取 pipe 元数据：{}，正在恢复 ...";
  public static final String FAILED_TO_SHOW_CREATE_PIPE_NOT_EXIST =
      "show create pipe %s 失败，该 pipe 不存在。";
  public static final String FAILED_TO_SHOW_CREATE_TOPIC_NOT_EXIST =
      "show create topic %s 失败，该 topic 不存在。";
  public static final String RECEIVED_PIPE_HEARTBEAT_REQUEST_FROM_CONFIG_NODE =
      "收到来自 config node 的 pipe 心跳请求 {}。";
  public static final String REGION_NO_TSFILEINSERTIONEVENTS_TO_REPLACE_FOR_SOURCE =
      "Region {}: No TsFileInsertionEvents to replace for source files {}";
  public static final String REGION_REPLACED_TSFILEINSERTIONEVENTS_WITH =
      "Region {}: Replaced TsFileInsertionEvents {} with {}";
  public static final String REGISTEREDTASKCOUNT_0 = "registeredTaskCount 小于 0";
  public static final String REGISTEREDTASKCOUNT_0_1 = "registeredTaskCount 小于等于 0";
  public static final String REGISTER_SUBTASK_RUNNINGTASKCOUNT_REGISTEREDTASKCOUNT =
      "注册子任务 {}。runningTaskCount: {}, registeredTaskCount: {}";
  public static final String REPORT_PIPERUNTIMEEXCEPTION_TO_LOCAL_PIPETASKMETA_EXCEPTION_MESSAGE =
      "向本地 PipeTaskMeta({}) 上报 PipeRuntimeException，异常信息：{}";
  public static final String RUNNINGTASKCOUNT_0 = "runningTaskCount 小于 0";
  public static final String RUNNINGTASKCOUNT_0_1 = "runningTaskCount 小于等于 0";
  public static final String SIMPLEPROGRESSINDEXASSIGNER_STARTED_SUCCESSFULLY_ISSIMPLECONSENSUSENABLE_R =
      "SimpleProgressIndexAssigner 启动成功。isSimpleConsensusEnable: {}, "
          + "rebootTimes: {}";
  public static final String STARTING_SIMPLEPROGRESSINDEXASSIGNER =
      "正在启动 SimpleProgressIndexAssigner ...";
  public static final String START_PIPE_DN_TASK_SUCCESSFULLY_WITHIN_MS =
      "启动 pipe DN task {} 成功，耗时 {} ms";
  public static final String START_SUBTASK_RUNNINGTASKCOUNT_REGISTEREDTASKCOUNT =
      "启动子任务 {}。runningTaskCount: {}, registeredTaskCount: {}";
  public static final String STOP_PIPE_DN_TASK_SUCCESSFULLY_WITHIN_MS =
      "停止 pipe DN task {} 成功，耗时 {} ms";
  public static final String STOP_SUBTASK_RUNNINGTASKCOUNT_REGISTEREDTASKCOUNT =
      "停止子任务 {}。runningTaskCount: {}, registeredTaskCount: {}";
  public static final String SUBTASK_IS_CLOSED_IGNORE_EXCEPTION =
      "subtask {} 已关闭, ignore exception";
  public static final String SUBTASK_WORKER_IS_INTERRUPTED = "子任务工作线程被中断";
  public static final String SUCCESSFULLY_PERSISTED_ALL_PIPE_S_INFO_TO =
      "成功将所有 Pipe 信息持久化到 ConfigNode。";
  public static final String THE_EXECUTOR_AND_HAS_BEEN_SUCCESSFULLY_SHUTDOWN =
      "执行器 {} 和 {} 已成功关闭。";

  // ===================== EVENT =====================

  public static final String DATABASENAMEFROMDATAREGION_IS_NULL = "databaseNameFromDataRegion 为空";
  public static final String DECREASE_REFERENCE_COUNT_ERROR = "减少引用计数出错。";
  public static final String DECREASE_REFERENCE_COUNT_FOR_MTREE_SNAPSHOT_OR =
      "Decrease reference count for mTree snapshot {} or tLog {} or attribute snapshot {} 出错。";
  public static final String DECREASE_REFERENCE_COUNT_FOR_TSFILE_ERROR =
      "Decrease reference count for TsFile {} 出错。";
  public static final String DO_NOT_HAS_A_COMPLETE_PAGE_BODY =
      "do not has a complete page body. Expected:";
  public static final String ERROR_WHILE_PARSING_TSFILE_INSERTION_EVENT =
      "Error while parsing tsfile insertion event";
  public static final String EXCEPTION_OCCURRED_WHEN_DETERMINING_THE_EVENT_TIME =
      "determining the event time of PipeInsertNodeTabletInsertionEvent({}) overlaps with the "
          + "time range: [{}, {}]. Returning true to ensure data integrity 时发生异常";
  public static final String FAILED_TO_ALLOCATE_MEMORY_FOR_PARSING_TSFILE =
      "{}: failed to allocate memory for parsing TsFile {}, tablet event no. {}, retry count "
          + "is {}, will keep retrying.";
  public static final String FAILED_TO_BUILD_TABLET = "构建 tablet 失败";
  public static final String FAILED_TO_CHECK_NEXT = "check next 失败";
  public static final String FAILED_TO_CLOSE_TSFILEREADER = "关闭 TsFileReader 失败";
  public static final String FAILED_TO_CLOSE_TSFILESEQUENCEREADER = "关闭 TsFileSequenceReader 失败";
  public static final String FAILED_TO_CREATE_TSFILEINSERTIONDATATABLETITERATOR =
      "创建 TsFileInsertionDataTabletIterator 失败";
  public static final String FAILED_TO_GET_NEXT_TABLET_INSERTION_EVENT =
      "获取 next tablet insertion event 失败。";
  public static final String FAILED_TO_LOAD_MODIFICATIONS_FROM_TSFILE =
      "加载 modifications from TsFile:  失败";
  public static final String FAILED_TO_READ_METADATA_FOR_DEVICEID_MEASUREMENT =
      "读取 metadata for deviceId: {}, measurement: {}, removing 失败";
  public static final String FAILED_TO_RECORD_PARSE_END_TIME_FOR =
      "记录 parse end time for pipe {} 失败";
  public static final String FAILED_TO_RECORD_TABLET_METRICS_FOR_PIPE =
      "记录 tablet metrics for pipe {} 失败";
  public static final String FOUND_NULL_DEVICEID_REMOVING_ENTRY =
      "Found null deviceId, removing entry";
  public static final String INITIALIZE_DATA_CONTAINER_ERROR = "Initialize data container 出错。";
  public static final String INSERTNODE_HAS_BEEN_RELEASED = "InsertNode 已被释放";
  public static final String INSERTROWNODE_IS_PARSED_TO_ZERO_ROWS_ACCORDING =
      "InsertRowNode({}) is parsed to zero rows according to the pattern({}) and time range "
          + "[{}, {}], the corresponding source event({}) will be ignored.";
  public static final String INSERTTABLETNODE_IS_PARSED_TO_ZERO_ROWS_ACCORDING =
      "InsertTabletNode({}) is parsed to zero rows according to the pattern({}) and time range "
          + "[{}, {}], the corresponding source event({}) will be ignored.";
  public static final String INVALID_EVENT_TYPE = "无效的 event type: ";
  public static final String INVALID_INPUT = "无效的 input: ";
  public static final String ISGENERATEDBYPIPE_IS_NOT_SUPPORTED =
      "isGeneratedByPipe() is not supported!";
  public static final String MAYEVENTPATHSOVERLAPPEDWITHPATTERN_IS_NOT_SUPPORTED =
      "mayEventPathsOverlappedWithPattern() is not supported!";
  public static final String MAYEVENTTIMEOVERLAPPEDWITHTIMERANGE_IS_NOT_SUPPORTED =
      "mayEventTimeOverlappedWithTimeRange() is not supported!";
  public static final String NO_COMMIT_IDS_FOUND_IN_PIPECOMPACTEDTSFILEINSERTIONEVENT =
      "No commit IDs found in PipeCompactedTsFileInsertionEvent.";
  public static final String PIPECOMPACTEDTSFILEINSERTIONEVENT_DOES_NOT_SUPPORT_EQUALSINIOTCONSENSUSV2 =
      "PipeCompactedTsFileInsertionEvent 不支持 equalsInIoTConsensusV2.";
  public static final String PIPECOMPACTEDTSFILEINSERTIONEVENT_DOES_NOT_SUPPORT_GETREBOOTTIMES =
      "PipeCompactedTsFileInsertionEvent 不支持 getRebootTimes.";
  public static final String PIPE_FAILED_TO_GET_DEVICES_FROM_TSFILE =
      "Pipe {}：获取 devices from TsFile {}, extract it anyway 失败";
  public static final String PIPE_SKIPPING_TEMPORARY_TSFILE_S_PARSING_WHICH =
      "Pipe skipping temporary TsFile's parsing which shouldn't be transferred: {}";
  public static final String ROW_CAN_NOT_BE_CUSTOMIZED = "Row can not be customized";
  public static final String SHALLOWCOPYSELFANDBINDPIPETASKMETAFORPROGRESSREPORT_IS_NOT_SUPPORTED =
      "shallowCopySelfAndBindPipeTaskMetaForProgressReport() is not supported!";
  public static final String SKIPPING_TEMPORARY_TSFILE_S_PROGRESSINDEX_WILL_REPORT =
      "跳过 temporary TsFile {}'s progressIndex, will report MinimumProgressIndex";
  public static final String TABLEPATTERNPARSER_DOES_NOT_SUPPORT_ROW_BY_ROW =
      "TablePatternParser 不支持 row by row processing";
  public static final String TABLEPATTERNPARSER_DOES_NOT_SUPPORT_TABLET_PROCESSING =
      "TablePatternParser 不支持 tablet processing";
  public static final String TABLEPATTERNPARSER_DOES_NOT_SUPPORT_TABLET_PROCESSING_WITH =
      "TablePatternParser 不支持 tablet processing with collect";
  public static final String TABLET_IS_PARSED_TO_ZERO_ROWS_ACCORDING =
      "Tablet({}) is parsed to zero rows according to the pattern({}) and time range [{}, {}], "
          + "the corresponding source event({}) will be ignored.";
  public static final String TABLE_MODEL_TSFILE_PARSING_DOES_NOT_SUPPORT =
      "Table model tsfile parsing 不支持 this type of ChunkMeta";
  public static final String TEMPORARY_TSFILE_DETECTED_WILL_SKIP_ITS_TRANSFER =
      "Temporary tsFile {} detected, will skip its transfer.";
  public static final String TSFILE_HAS_INITIALIZED_PIPENAME_CREATION_TIME_PATTERN =
      "TsFile {} has initialized {}, pipeName: {}, creation time: {}, pattern: {}, startTime: "
          + "{}, endTime: {}, withMod: {}";
  public static final String UNCOMPRESS_ERROR_UNCOMPRESS_SIZE =
      "Uncompress error! uncompress size: ";
  public static final String UNSUPPORTED = "不支持";
  public static final String UNSUPPORTED_NODE_TYPE = "不支持的 node type ";
  public static final String WAIT_FOR_MEMORY_ENOUGH_FOR_PARSING_FOR =
      "等待 memory enough，已等待 parsing {} for {} 秒。";

  // ===================== PROCESSOR =====================

  public static final String ABSTRACTSAMETYPENUMERICOPERATOR_DOES_NOT_SUPPORT_BINARY_INPUT =
      "AbstractSameTypeNumericOperator 不支持 binary input";
  public static final String ABSTRACTSAMETYPENUMERICOPERATOR_DOES_NOT_SUPPORT_BOOLEAN_INPUT =
      "AbstractSameTypeNumericOperator 不支持 boolean input";
  public static final String ABSTRACTSAMETYPENUMERICOPERATOR_DOES_NOT_SUPPORT_DATE_INPUT =
      "AbstractSameTypeNumericOperator 不支持 date input";
  public static final String ABSTRACTSAMETYPENUMERICOPERATOR_DOES_NOT_SUPPORT_STRING_INPUT =
      "AbstractSameTypeNumericOperator 不支持 string input";
  public static final String CHANGINGVALUESAMPLINGPROCESSOR_IN_IS_INITIALIZED_WITH =
      "ChangingValueSamplingProcessor in {} is initialized with {}: {}, {}: {}, {}: {}.";
  public static final String CLEAN_OUTDATED_INCOMPLETE_COMBINER_PIPENAME_CREATIONTIME_COMBINEID =
      "清理 outdated incomplete combiner: pipeName={}, creationTime={}, combineId={}";
  public static final String COMBINEHANDLER_NOT_FOUND_FOR_PIPEID =
      "CombineHandler not found for pipeId = ";
  public static final String COMBINER_COMBINE_COMPLETED_REGIONID_STATE_RECEIVEDREGIONIDSET_EX =
      "Combiner combine completed: regionId: {}, state: {}, receivedRegionIdSet: {}, "
          + "expectedRegionIdSet: {}";
  public static final String COMBINER_COMBINE_REGIONID_STATE_RECEIVEDREGIONIDSET_EXPECTEDREGI =
      "Combiner combine: regionId: {}, state: {}, receivedRegionIdSet: {}, expectedRegionIdSet: {}";
  public static final String DATA_NODES_ENDPOINTS_FOR_TWO_STAGE_AGGREGATION =
      "Data nodes' endpoints for two-stage aggregation: {}";
  public static final String DIFFERENT_DATA_TYPE_ENCOUNTERED_IN_ONE_WINDOW =
      "Different data type encountered in one window, will purge. Previous type: {}, now type: {}";
  public static final String ENCOUNTERED_EXCEPTION_WHEN_DESERIALIZING_FROM_PIPETASKMETA =
      "Encountered exception when deserializing from PipeTaskMeta";
  public static final String END_POINTS_FOR_TWO_STAGE_AGGREGATION_PIPE =
      "End points for two-stage aggregation pipe (pipeName={}, creationTime={}) were updated to {}";
  public static final String ERROR_OCCURRED_WHEN_CLOSING_COMBINEHANDLER_ID =
      "closing CombineHandler(id = {}) 时发生错误";
  public static final String ERROR_OCCURS_WHEN_RECEIVING_REQUEST = "receiving request: {} 时发生错误";
  public static final String FAILED_TO_CLOSE_IOTDBSYNCCLIENT = "关闭 IoTDBSyncClient 失败";
  public static final String FAILED_TO_CLOSE_OLD_IOTDBSYNCCLIENT = "关闭 old IoTDBSyncClient 失败";
  public static final String FAILED_TO_COMBINE_COUNT = "combine count:  失败";
  public static final String FAILED_TO_CONSTRUCT_IOTDBSYNCCLIENT = "构造 IoTDBSyncClient 失败";
  public static final String FAILED_TO_FETCH_COMBINE_RESULT = "获取 combine result:  失败";
  public static final String FAILED_TO_FETCH_DATA_NODES = "获取 data nodes 失败";
  public static final String FAILED_TO_FETCH_DATA_REGION_IDS = "获取 data region ids 失败";
  public static final String FAILED_TO_RECONSTRUCT_IOTDBSYNCCLIENT_AFTER_FAILURE_TO =
      "reconstruct IoTDBSyncClient {} after failure to send request {} (watermark = {}) 失败";
  public static final String FAILED_TO_SEND_REQUEST_WATERMARK_TO =
      "发送 request {} (watermark = {}) to {} 失败";
  public static final String FAILED_TO_TRIGGER_COMBINE_WATERMARK_COUNT_PROGRESSINDEX =
      "trigger combine. watermark={}, count={}, progressIndex={} 失败";
  public static final String FAILURE_OCCURRED_WHEN_TRYING_TO_COMMIT_PROGRESS =
      "Failure occurred when trying to commit progress index. timestamp={}, count={}, "
          + "progressIndex={}";
  public static final String FETCHED_DATA_REGION_IDS_AT = "Fetched data region ids {} at {}";
  public static final String FRACTIONPOWEREDSUMOPERATOR_DOES_NOT_SUPPORT_BINARY_INPUT =
      "FractionPoweredSumOperator 不支持 binary input";
  public static final String FRACTIONPOWEREDSUMOPERATOR_DOES_NOT_SUPPORT_BOOLEAN_INPUT =
      "FractionPoweredSumOperator 不支持 boolean input";
  public static final String FRACTIONPOWEREDSUMOPERATOR_DOES_NOT_SUPPORT_DATE_INPUT =
      "FractionPoweredSumOperator 不支持 date input";
  public static final String FRACTIONPOWEREDSUMOPERATOR_DOES_NOT_SUPPORT_STRING_INPUT =
      "FractionPoweredSumOperator 不支持 string input";
  public static final String GLOBAL_COUNT_IS_LESS_THAN_THE_LAST =
      "Global count is less than the last collected count: timestamp={}, count={}";
  public static final String IGNORED_TABLETINSERTIONEVENT_IS_NOT_AN_INSTANCE_OF =
      "已忽略 TabletInsertionEvent is not an instance of PipeInsertNodeTabletInsertionEvent or "
          + "PipeRawTabletInsertionEvent: {}";
  public static final String IGNORED_TSFILEINSERTIONEVENT_IS_EMPTY =
      "Ignored TsFileInsertionEvent 为空: {}";
  public static final String IGNORED_TSFILEINSERTIONEVENT_IS_NOT_AN_INSTANCE_OF =
      "已忽略 TsFileInsertionEvent is not an instance of PipeTsFileInsertionEvent: {}";
  public static final String ILLEGAL_OUTPUT_SERIES_PATH = "非法的 output series path: ";
  public static final String NO_DATA_NODES_ENDPOINTS_FETCHED = "No data nodes' endpoints fetched";
  public static final String NO_EXPECTED_REGION_ID_SET_FETCHED =
      "No expected region id set fetched";
  public static final String PARTIALPATHLASTOBJECTCACHE_ALLOCATEDMEMORYBLOCK_HAS_EXPANDED_FROM_TO =
      "PartialPathLastObjectCache.allocatedMemoryBlock has expanded from {} to {}.";
  public static final String PARTIALPATHLASTOBJECTCACHE_ALLOCATEDMEMORYBLOCK_HAS_SHRUNK_FROM_TO =
      "PartialPathLastObjectCache.allocatedMemoryBlock has shrunk from {} to {}.";
  public static final String SENDING_REQUEST_WATERMARK_TO = "正在发送 request {} (watermark = {}) 到 {}";
  public static final String SWINGINGDOORTRENDINGSAMPLINGPROCESSOR_IN_IS_INITIALIZED_WITH =
      "SwingingDoorTrendingSamplingProcessor in {} is initialized with {}: {}, {}: {}, {}: {}.";
  public static final String THE_ABSTRACT_FORMAL_PROCESSOR_DOES_NOT_SUPPORT = "抽象形式处理器不支持处理事件";
  public static final String TUMBLINGTIMESAMPLINGPROCESSOR_IN_IS_INITIALIZED_WITH_S =
      "TumblingTimeSamplingProcessor in {} is initialized with {}: {}s, {}: {}, {}: {}.";
  public static final String TWOSTAGECOUNTPROCESSOR_CUSTOMIZED_BY_THREAD_PIPENAME_CREATIONTIME_RE =
      "TwoStageCountProcessor customized by thread {}: pipeName={}, creationTime={}, "
          + "regionId={}, outputSeries={}, localCommitProgressIndex={}, localCount={}";
  public static final String TWO_STAGE_AGGREGATE_PIPE_PIPENAME_CREATIONTIME_RELATED =
      "Two stage aggregate pipe (pipeName={}, creationTime={}) related region ids {}";
  public static final String TWO_STAGE_AGGREGATE_RECEIVER_IS_EXITING =
      "Two stage aggregate receiver is exiting.";
  public static final String TWO_STAGE_COMBINE_REGION_ID_COMBINE_ID =
      "Two stage combine (region id = {}, combine id = {}) incomplete: timestamp={}, count={}, "
          + "progressIndex={}";
  public static final String TWO_STAGE_COMBINE_REGION_ID_COMBINE_ID_1 =
      "Two stage combine (region id = {}, combine id = {}) outdated: timestamp={}, count={}, "
          + "progressIndex={}";
  public static final String TWO_STAGE_COMBINE_REGION_ID_COMBINE_ID_2 =
      "Two stage combine (region id = {}, combine id = {}) success: timestamp={}, count={}, "
          + "progressIndex={}, committed progressIndex={}";
  public static final String UNEXPECTED_STATE_CLASS = "Unexpected state class: ";
  public static final String UNKNOWN_COMBINE_RESULT_TYPE = "未知的 combine result type: ";
  public static final String UNKNOWN_REQUEST_TYPE = "未知的 request type {}: {}。";

  // ===================== SOURCE =====================

  public static final String ALL_DATA_IN_TSFILEEPOCH_WAS_EXTRACTED =
      "All data in TsFileEpoch {} 已提取";
  public static final String BUFFERSIZE_MUST_BE_A_POWER_OF_2 = "bufferSize must be a power of 2";
  public static final String BUFFERSIZE_MUST_NOT_BE_LESS_THAN_1 =
      "bufferSize must not be less than 1";
  public static final String CAPTURE_TREE_AND_CAPTURE_TABLE_CAN_NOT =
      "capture.tree 和 capture.table 不能同时设为 false";
  public static final String DATABASE_NAME_IS_NULL_WHEN_MATCHING_SOURCES =
      "匹配表模型事件的 source 时数据库名称为空。";
  public static final String DATA_REGION_INJECTED_WATERMARK_EVENT_WITH_TIMESTAMP =
      "Data region {}: Injected watermark event with timestamp: {}";
  public static final String DISCARD_TABLET_EVENT_BECAUSE_IT_IS_NOT =
      "Discard tablet event {} because it is not reliable anymore. Change the state of "
          + "TsFileEpoch to USING_BOTH.";
  public static final String DISRUPTOR_ALREADY_STARTED = "Disruptor already started";
  public static final String DISRUPTOR_SHUTDOWN_COMPLETED = "Disruptor 关闭完成";
  public static final String DISRUPTOR_STARTED_WITH_BUFFER_SIZE = "Disruptor 已启动，缓冲区大小：{}";
  public static final String EXCEPTION_DURING_ONSHUTDOWN = "onShutdown() 期间发生异常";
  public static final String EXCEPTION_DURING_ONSTART = "onStart() 期间发生异常";
  public static final String EXCEPTION_ENCOUNTERED_WHEN_TRIGGERING_SCHEMA_REGION_SNAPSHOT =
      "Exception encountered when triggering schema region snapshot.";
  public static final String EXCEPTION_PROCESSING = "处理时发生异常：{} {}";
  public static final String FAILED_TO_LOAD_SNAPSHOT = "加载 snapshot {} 失败";
  public static final String FAILED_TO_LOAD_SNAPSHOT_FROM_BYTEBUFFER =
      "加载 snapshot from byteBuffer {} 失败。";
  public static final String FAILED_TO_START_SOURCES = "启动 sources 失败。";
  public static final String HEARTBEAT_EVENT_CAN_NOT_BE_SUPPLIED_BECAUSE =
      "Heartbeat Event {} 无法被提供，因为其引用计数无法增加";
  public static final String EVENT_CAN_NOT_BE_SUPPLIED_BECAUSE_DATA_IS_LOST =
      "Event %s 无法被提供，因为其引用计数无法增加，事件代表的数据已经丢失";
  public static final String INTERRUPTED_WAITING_FOR_PROCESSOR_TO_STOP =
      "等待 processor 停止时被中断";
  public static final String INTERRUPTED_WHEN_WAITING_FOR_PARSING_PRIVILEGE_FOR_TSFILE =
      "等待解析 TsFile %s 的权限信息时被中断。";
  public static final String PARSE_TSFILE_WHEN_CHECKING_PRIVILEGE_ERROR =
      "检查权限时解析 TsFile %s 失败。原因：%s";
  public static final String READ_TSFILE_ERROR = "读取 TsFile %s 失败。";
  public static final String IOTDBSCHEMAREGIONSOURCE_DOES_NOT_SUPPORT_TRANSFERRING_EVENTS_UNDER =
      "IoTDBSchemaRegionSource 不支持 transferring events under simple consensus";
  public static final String NOT_HAS_PRIVILEGE_TO_TRANSFER_EVENT = "没有权限 transfer event: ";
  public static final String NOT_HAS_PRIVILEGE_TO_TRANSFER_PLAN = "没有权限传输计划：";
  public static final String NO_EVENT_HANDLER_CONFIGURED = "No event handler configured";
  public static final String N_MUST_BE_0 = "n must be > 0";
  public static final String PIPEREALTIMEDATAREGIONEXTRACTOR_OBSERVED_DATA_REGION_TIME_PARTITION_GROWT =
      "PipeRealtimeDataRegionExtractor({}) observed data region {} time partition growth, "
          + "recording time partition id bound: {}.";
  public static final String PIPE_AND_IS_NOT_SET_USE_HYBRID =
      "Pipe：'{}' ('{}') and '{}' ('{}') is not set, use hybrid mode by default.";
  public static final String PIPE_ASSIGNER_ON_DATA_REGION_SHUTDOWN_INTERNAL =
      "Pipe：Assigner on data region {} shutdown internal disruptor within {} ms";
  public static final String PIPE_FAILED_TO_GET_DEVICES_FROM_TSFILE_1 =
      "Pipe {}@{}：获取 devices from TsFile {}, extract it anyway 失败";
  public static final String PIPE_FAILED_TO_INCREASE_REFERENCE_COUNT_FOR =
      "Pipe {}@{}：增加 reference count for historical deletion event {}, will discard it 失败";
  public static final String PIPE_FAILED_TO_INCREASE_REFERENCE_COUNT_FOR_1 =
      "Pipe {}@{}：增加 reference count for historical tsfile event {}, will discard it 失败";
  public static final String PIPE_FAILED_TO_INCREASE_REFERENCE_COUNT_FOR_2 =
      "Pipe {}@{}：增加 reference count for terminate event, will resend it 失败";
  public static final String PIPE_FAILED_TO_PIN_TSFILERESOURCE = "Pipe：固定 TsFileResource {} 失败";
  public static final String PIPE_FAILED_TO_START_TO_EXTRACT_HISTORICAL =
      "Pipe {}@{}：启动 to extract historical TsFile, storage engine is not ready. Will retry "
          + "later 失败。";
  public static final String PIPE_FAILED_TO_UNPIN_SKIPPED_HISTORICAL_TSFILERESOURCE =
      "Pipe {}@{}：unpin skipped historical TsFileResource, original path: {} 失败";
  public static final String PIPE_FAILED_TO_UNPIN_TSFILERESOURCE_AFTER_CREATING =
      "Pipe {}@{}：unpin TsFileResource after creating event, original path: {} 失败";
  public static final String PIPE_FAILED_TO_UNPIN_TSFILERESOURCE_AFTER_DROPPING =
      "Pipe {}@{}：unpin TsFileResource after dropping pipe, original path: {} 失败";
  public static final String PIPE_FINISH_TO_EXTRACT_DELETIONS_EXTRACT_DELETIONS =
      "Pipe {}@{}：finish to extract deletions, extract deletions count {}/{}, took {} ms";
  public static final String PIPE_FINISH_TO_EXTRACT_HISTORICAL_TSFILE_EXTRACTED =
      "Pipe {}@{}：finish to extract historical TsFile, extracted sequence file count {}/{}, "
          + "extracted unsequence file count {}/{}, extracted file count {}/{}, took {} ms";
  public static final String PIPE_FINISH_TO_SORT_ALL_EXTRACTED_RESOURCES =
      "Pipe {}@{}：finish to sort all extracted resources, took {} ms";
  public static final String PIPE_HISTORICAL_DATA_EXTRACTION_TIME_RANGE_START =
      "Pipe {}@{}：historical data extraction time range, start time {}({}), end time {}({}), "
          + "sloppy pattern {}, sloppy time range {}, should transfer mod file {}, username: {}, "
          + "skip if no privileges: {}, is forwarding pipe requests: {}";
  public static final String PIPE_IS_SET_TO_FALSE_USE_HEARTBEAT =
      "Pipe：'{}' ('{}') is set to false, use heartbeat realtime source.";
  public static final String PIPE_ON_DATA_REGION_SKIP_COMMIT_OF =
      "Pipe {} on data region {} skip commit of event {} because it was flushed prematurely.";
  public static final String PIPE_REALTIME_DATA_REGION_SOURCE_IS_INITIALIZED =
      "Pipe {}@{}：realtime data region source is initialized with parameters: {}.";
  public static final String PIPE_RESOURCE_MEETS_MAYTSFILECONTAINUNPROCESSEDDATA_CONDITION_EXTRACT =
      "Pipe {}@{}：resource {} meets mayTsFileContainUnprocessedData condition, extractor "
          + "progressIndex: {}, resource ProgressIndex: {}";
  public static final String PIPE_SET_WATERMARK_INJECTOR_WITH_INTERVAL_MS =
      "Pipe {}@{}：Set watermark injector with interval {} ms.";
  public static final String PIPE_SKIP_HISTORICAL_TSFILE_BECAUSE_REALTIME_SOURCE =
      "Pipe {}@{}：skip historical tsfile {} because realtime source in current task {} has "
          + "already captured it.";
  public static final String PIPE_SNAPSHOT_MODE_IS_ENABLED_USE_HEARTBEAT =
      "Pipe：快照模式已启用，使用 heartbeat 实时 source。";
  public static final String PIPE_STARTED_HISTORICAL_SOURCE_AND_REALTIME_SOURCE =
      "Pipe {}@{}：在 {} ms 内成功启动 historical source {} and realtime source {}。";
  public static final String PIPE_STARTING_HISTORICAL_SOURCE_AND_REALTIME_SOURCE =
      "Pipe {}@{}：Starting historical source {} and realtime source {}.";
  public static final String PIPE_START_HISTORICAL_SOURCE_AND_REALTIME_SOURCE =
      "Pipe {}@{}：Start historical source {} and realtime source {} 出错。";
  public static final String PIPE_START_TO_EXTRACT_DELETIONS = "Pipe {}@{}：开始提取 deletions";
  public static final String PIPE_START_TO_EXTRACT_HISTORICAL_TSFILE_ORIGINAL =
      "Pipe {}@{}：开始提取 historical TsFile, original sequence file count {}, original unSequence "
          + "file count {}, start progress index {}";
  public static final String PIPE_START_TO_FLUSH_DATA_REGION = "Pipe {}@{}：开始刷新 data region";
  public static final String PIPE_START_TO_SORT_ALL_EXTRACTED_RESOURCES =
      "Pipe {}@{}：开始排序 all extracted resources";
  public static final String PIPE_TASK_CANNOTUSETABLETANYMORE_FOR_TSFILE_THE_MEMORY =
      "Pipe task {}@{} canNotUseTabletAnyMore for tsFile {}: The memory usage of the insert "
          + "node {} has reached the dangerous threshold of single pipe {}, event count: {}";
  public static final String PIPE_UNEXPECTED_PROGRESSINDEX_TYPE_FALLBACK_TO_ORIGIN =
      "Pipe {}@{}：unexpected ProgressIndex type {}, fallback to origin {}.";
  public static final String PIPE_UNSUPPORTED_SOURCE_REALTIME_MODE_CREATE_A =
      "Pipe：不支持的 source realtime mode: {}, create a hybrid source。";
  public static final String PROCESSOR_INTERRUPTED = "处理器被中断";
  public static final String PROCESSOR_INTERRUPTED_UNEXPECTEDLY = "处理器意外中断，继续运行";
  public static final String PROCESSOR_STOPPED = "处理器已停止";
  public static final String SET_FOR_HISTORICAL_DELETION_EVENT =
      "[{}]Set {} for historical deletion event {}";
  public static final String SET_FOR_HISTORICAL_EVENT = "[{}]Set {} for historical event {}";
  public static final String SET_FOR_REALTIME_EVENT = "[{}]Set {} for realtime event {}";
  public static final String SOURCES_FILTERED_BY_DATABASE_AND_TABLE_IS =
      "Sources filtered by database and table 为空 when matching sources for table model event.";
  public static final String SOURCES_FILTERED_BY_DEVICE_IS_NULL_WHEN =
      "Sources filtered by device 为空 when matching sources for tree model event.";
  public static final String TAKE_SNAPSHOT_ERROR = "Take snapshot error: {}";
  public static final String THE_ASSIGNER_QUEUE_CONTENT_HAS_EXCEEDED_HALF =
      "The assigner queue content has exceeded half, it may be stuck and may block insertion. "
          + "regionId: {}, capacity: {}, bufferSize: {}";
  public static final String THE_PIPE_CANNOT_EXTRACT_TABLE_MODEL_DATA =
      "The pipe cannot extract table model data when sql dialect is set to tree.";
  public static final String THE_PIPE_CANNOT_EXTRACT_TREE_MODEL_DATA =
      "The pipe cannot extract tree model data when sql dialect is set to table.";
  public static final String THE_REFERENCE_COUNT_OF_THE_EVENT_CANNOT =
      "The reference count of the event {} cannot be increased, skipping it.";
  public static final String THE_REFERENCE_COUNT_OF_THE_REALTIME_EVENT =
      "The reference count of the realtime event {} cannot be increased, skipping it.";
  public static final String TIMED_OUT_WAITING_FOR_PROCESSOR_TO_STOP =
      "Timed out waiting for processor to stop";
  public static final String TSFILEEPOCH_NOT_FOUND_FOR_TSFILE_CREATING_A =
      "TsFileEpoch not found for TsFile {}, creating a new one";
  public static final String WHEN_IS_SET_TO_FALSE_SPECIFYING_AND =
      "When '{}' ('{}') is set to false, specifying {} and {} is invalid.";
  public static final String WHEN_IS_SET_TO_TRUE_SPECIFYING_AND =
      "When '{}' ('{}', '{}', '{}') is set to true, specifying {} and {} is invalid.";
  public static final String WHEN_OR_IS_SPECIFIED_SPECIFYING_AND_IS =
      "When {}, {}, {} or {} is specified, specifying {}, {}, {}, {}, {} and {} is invalid.";

  // ===================== SINK =====================

  public static final String ACQUIRE_IOPCITEMMGT_SUCCESSFULLY_INTERFACE_ADDRESS =
      "成功获取 IOPCItemMgt! Interface address: {}";
  public static final String ACQUIRE_IOPCSYNCIO_SUCCESSFULLY_INTERFACE_ADDRESS =
      "成功获取 IOPCSyncIO! Interface address: {}";
  public static final String ADDED_EVENT_TO_RETRY_QUEUE = "已将 event {} 添加到 retry queue";
  public static final String BATCH_ID_CREATE_BATCH_DIR_SUCCESSFULLY_BATCH =
      "批次 id = {}：创建 batch dir successfully, batch file dir = {}.";
  public static final String BATCH_ID_DELETE_THE_TSFILE_AFTER_FAILED =
      "批次 id = {}：{} delete the tsfile {} after failed to write tablets into {}. {}";
  public static final String BATCH_ID_FAILED_TO_BUILD_THE_TABLE =
      "批次 id = {}：构建 the table model TSFile. Please check whether the written Tablet has time "
          + "overlap and whether the Table Schema is correct 失败。";
  public static final String BATCH_ID_FAILED_TO_CLOSE_THE_TSFILE =
      "批次 id = {}：关闭 the tsfile {} after failed to write tablets into 失败，原因：{}";
  public static final String BATCH_ID_FAILED_TO_CLOSE_THE_TSFILE_1 =
      "批次 id = {}：关闭 the tsfile {} when trying to close batch 失败，原因：{}";
  public static final String BATCH_ID_FAILED_TO_CREATE_BATCH_FILE =
      "批次 id = {}：创建 batch file dir {} 失败。";
  public static final String BATCH_ID_FAILED_TO_DELETE_THE_TSFILE =
      "批次 id = {}：删除 the tsfile {} when trying to close batch 失败，原因：{}";
  public static final String BATCH_ID_FAILED_TO_WRITE_TABLETS_INTO =
      "批次 id = {}：写入 tablets into tsfile 失败，原因：{}";
  public static final String BATCH_ID_SEAL_TSFILE_SUCCESSFULLY = "批次 id = {}：成功封存 tsfile {}。";
  public static final String BATCH_ID_UNSUPPORTED_EVENT_TYPE_WHEN_CONSTRUCTING =
      "批次 id = {}：不支持的 event {} type {} when constructing tsfile batch";
  public static final String CANNOT_INCREASE_REFERENCE_COUNT_FOR_EVENT_IGNORE =
      "无法增加 reference count for event: {}, ignore it in batch";
  public static final String CANNOT_SERIALIZE_BOTH_TABLET_AND_STATEMENT_ARE =
      "Cannot serialize: both tablet and statement are null";
  public static final String CERTIFICATE_DIRECTORY_IS_PLEASE_MOVE_CERTIFICATES_FROM =
      "Certificate directory is: {}, Please move certificates from the reject dir to the "
          + "trusted directory to allow encrypted access";
  public static final String CLIENT_HAS_BEEN_RETURNED_TO_THE_POOL =
      "Client has been returned to the pool. Current handler status is {}. Will not transfer {}.";
  public static final String CLOSED_ASYNCPIPEDATATRANSFERSERVICECLIENTMANAGER_FOR_RECEIVER_ATTRIBUTES =
      "已关闭 AsyncPipeDataTransferServiceClientManager for receiver attributes: {}";
  public static final String CREATE_GROUP_SUCCESSFULLY_SERVER_HANDLE_UPDATE_RATE =
      "创建 group successfully! Server handle: {}, update rate: {} ms";
  public static final String DELETENODETRANSFER_NO_EVENT_SUCCESSFULLY_PROCESSED =
      "DeleteNodeTransfer: no.{} event successfully processed!";
  public static final String DESERIALIZE_PIPEDATA_ERROR_BECAUSE_UNKNOWN_TYPE =
      "Deserialize PipeData error because Unknown type ";
  public static final String DESERIALIZE_PIPEDATA_ERROR_BECAUSE_UNKNOWN_TYPE_1 =
      "Deserialize PipeData error because Unknown type {}.";
  public static final String ERROR_GETTING_OPC_CLIENT = "Error getting opc client: ";
  public static final String ERROR_PROGID_IS_INVALID_OR_UNREGISTERED_HRESULT =
      "Error: ProgID is invalid or unregistered, (HRESULT=0x";
  public static final String ERROR_RUNNING_OPC_CLIENT = "Error running opc client: ";
  public static final String EXCEPTION_OCCURRED_WHEN_PIPETABLEMODELTSFILEBUILDERV2_WRITING_TABLETS_TO =
      "PipeTableModelTsFileBuilderV2 writing tablets to tsfile, use fallback tsfile builder: "
          + "{} 时发生异常";
  public static final String EXCEPTION_OCCURRED_WHEN_PIPETREEMODELTSFILEBUILDERV2_WRITING_TABLETS_TO =
      "PipeTreeModelTsFileBuilderV2 writing tablets to tsfile, use fallback tsfile builder: {} "
          + "时发生异常";
  public static final String EXECUTE_STATEMENT_TO_DATABASE_SKIP_BECAUSE_NO =
      "Execute statement {} to database {}, skip because no permission.";
  public static final String FAILED_TO_ACQUIRE_IOPCITEMMGT_ERROR_CODE_0X =
      "获取 IOPCItemMgt, error code: 0x 失败";
  public static final String FAILED_TO_ACQUIRE_IOPCSYNCIO_ERROR_CODE_0X =
      "获取 IOPCSyncIO, error code: 0x 失败";
  public static final String FAILED_TO_ADD_ITEM = "add item  失败";
  public static final String FAILED_TO_ADD_ITEM_WIN_ERROR_CODE = "add item, win error code: 0x 失败";
  public static final String FAILED_TO_ADJUST_TIMEOUT_WHEN_FAILED_TO =
      "adjust timeout when failed to transfer file 失败。";
  public static final String FAILED_TO_BORROW_CLIENT_FOR_CACHED_LEADER =
      "borrow client {}:{} for cached leader 失败。";
  public static final String FAILED_TO_BUILD_AND_STARTUP_OPCUASERVER =
      "构建 and startup OpcUaServer 失败";
  public static final String FAILED_TO_CLOSE_ASYNCPIPEDATATRANSFERSERVICECLIENTMANAGER_FOR_RECEIVER_ATTRIBUTE =
      "关闭 AsyncPipeDataTransferServiceClientManager for receiver attributes: {} 失败";
  public static final String FAILED_TO_CLOSE_CLIENT_AFTER_HANDSHAKE_FAILURE =
      "关闭 client {}:{} after handshake failure when the manager is closed 失败。";
  public static final String FAILED_TO_CLOSE_CLIENT_MANAGER = "关闭 client manager 失败。";
  public static final String FAILED_TO_CLOSE_FILE_READER_OR_DELETE =
      "关闭 file reader or delete tsFile when failed to transfer file 失败。";
  public static final String FAILED_TO_CLOSE_FILE_READER_OR_DELETE_1 =
      "关闭 file reader or delete tsFile when successfully transferred file 失败。";
  public static final String FAILED_TO_CLOSE_FILE_READER_WHEN_SUCCESSFULLY =
      "关闭 file reader when successfully transferred mod file 失败。";
  public static final String FAILED_TO_CLOSE_OR_INVALIDATE_CLIENT_WHEN =
      "关闭 or invalidate client when connector is closed. Client: {}, Exception: {} 失败";
  public static final String FAILED_TO_CLOSE_TRUSTLISTMANAGER_BECAUSE =
      "关闭 trustListManager 失败，原因：{}.";
  public static final String FAILED_TO_CONNECT_TO_SERVER_ERROR_CODE =
      "连接 to server, error code: 0x 失败";
  public static final String FAILED_TO_CONVERT_STATEMENT_TO_TABLET = "转换 statement to tablet 失败。";
  public static final String FAILED_TO_CONVERT_STATEMENT_TO_TABLET_FOR =
      "转换 statement to tablet for serialization 失败";
  public static final String FAILED_TO_CREATE_GROUP_ERROR_CODE_0X = "创建 group，error code: 0x 失败";
  public static final String FAILED_TO_CREATE_NODES_AFTER_TRANSFER_DATA =
      "创建 nodes after transfer data value, creation status:  失败";
  public static final String FAILED_TO_DELETE_BATCH_FILE_THIS_FILE =
      "删除 batch file {}, this file should be deleted manually later 失败";
  public static final String FAILED_TO_GET_THE_SIZE_OF_PIPETRANSFERBATCHREQBUILDER =
      "获取 the size of PipeTransferBatchReqBuilder, return 0. Exception: {} 失败";
  public static final String FAILED_TO_HANDSHAKE = "Failed to handshake.";
  public static final String FAILED_TO_LOG_ERROR_WHEN_FAILED_TO =
      "log error when failed to transfer file 失败。";
  public static final String FAILED_TO_PUSH_VALUE_CHANGE_TO_CLIENT =
      "push value change to client, nodeId={} 失败";
  public static final String FAILED_TO_SEND_INITIAL_VALUE_TO_NEW =
      "发送 initial value to new subscription, nodeId={} 失败";
  public static final String FAILED_TO_SERIALIZE_PROGRESS_INDEX = "序列化 progress index {} 失败";
  public static final String FAILED_TO_SHUTDOWN_EXECUTOR = "关闭 executor {} 失败。";
  public static final String FAILED_TO_TRANSFER_DATAVALUE = "传输 dataValue 失败";
  public static final String FAILED_TO_TRANSFER_DATAVALUE_AFTER_SUCCESSFULLY_CREATED =
      "传输 dataValue after successfully created nodes 失败";
  public static final String FAILED_TO_TRANSFER_PIPEDELETENODEEVENT_COMMITTER_KEY_REPLICATE =
      "传输 PipeDeleteNodeEvent {} (committer key={}, replicate index={}) 失败。";
  public static final String FAILED_TO_TRANSFER_TABLETINSERTIONEVENT_COMMITTER_KEY_REPLICATE =
      "传输 TabletInsertionEvent {} (committer key={}, replicate index={}) 失败。";
  public static final String FAILED_TO_TRANSFER_TSFILE_BATCH = "传输 tsfile batch ({}) 失败。";
  public static final String FAILED_TO_TRANSFER_TSFILE_EVENT_ASYNCHRONOUSLY =
      "传输 tsfile event {} asynchronously 失败。";
  public static final String FAILED_TO_UPDATE_LEADER_CACHE_FOR_DEVICE =
      "更新 leader cache for device {} with endpoint {}:{} 失败。";
  public static final String FAILED_TO_WRITE = "Failed to write ";
  public static final String FAILED_TO_WRITE_WIN_ERROR_CODE_0X =
      "Failed to write, win error code: 0x";
  public static final String GENERATE_STATEMENT_FROM_TABLET_ERROR = "从 tablet {} 生成 Statement 出错。";
  public static final String GOT_AN_ERROR_FROM = "Got an error \\\"{}\\\" from {}:{}.";
  public static final String GOT_AN_ERROR_FROM_AN_UNKNOWN_CLIENT =
      "Got an error \\\"{}\\\" from an unknown client.";
  public static final String HANDSHAKE_SUCCESSFULLY_WITH_RECEIVER =
      "握手 successfully with receiver {}:{}.";
  public static final String ILLEGAL_STATE_WHEN_RETURN_THE_CLIENT_TO =
      "非法的 state when return the client to object pool, maybe the pool is already cleared. "
          + "Will ignore。";
  public static final String INSERTNODETRANSFER_NO_EVENT_SUCCESSFULLY_PROCESSED =
      "InsertNodeTransfer: no.{} event successfully processed!";
  public static final String INTERRUPTED_WHILE_WAITING_FOR_HANDSHAKE_RESPONSE =
      "waiting for handshake response 时被中断。";
  public static final String IOTCONSENSUSV2ASYNCCONNECTOR_DOES_NOT_SUPPORT_TRANSFERRING_GENERIC_EVENT =
      "IoTConsensusV2AsyncConnector 不支持 transferring generic event: {}.";
  public static final String IOTCONSENSUSV2ASYNCCONNECTOR_DOES_NOT_SUPPORT_TRANSFER_GENERIC_EVENT =
      "IoTConsensusV2AsyncConnector 不支持 transfer generic event: {}.";
  public static final String IOTCONSENSUSV2ASYNCCONNECTOR_ONLY_SUPPORT_PIPETSFILEINSERTIONEVENT_CURRENT_EVEN =
      "IoTConsensusV2AsyncConnector only support PipeTsFileInsertionEvent. Current event: {}.";
  public static final String IOTCONSENSUSV2CONNECTOR_TRANSFERBUFFER_QUEUE_OFFER_IS_INTERRUPTED =
      "IoTConsensusV2Connector transferBuffer queue offer is interrupted.";
  public static final String IOTCONSENSUSV2TRANSFERBATCHREQBUILDER_THE_MAX_BATCH_SIZE_IS_ADJUSTED =
      "IoTConsensusV2TransferBatchReqBuilder: the max batch size is adjusted from {} to {} due "
          + "to the memory restriction";
  public static final String IOTCONSENSUSV2_CONSENSUSGROUP_EVENT_NOT_FOUND_IN_TRANSFERBUFFER =
      "IoTConsensusV2-ConsensusGroup-{}: event-{} not found in transferBuffer, skip removing. "
          + "queue size = {}";
  public static final String IOTCONSENSUSV2_CONSENSUSGROUP_EVENT_REPLICATE_INDEX_TRANSFER_FAILED =
      "IoTConsensusV2-ConsensusGroup-{}: Event {} replicate index {} transfer failed, added to "
          + "retry queue failed, this event will be ignored.";
  public static final String IOTCONSENSUSV2_CONSENSUSGROUP_EVENT_REPLICATE_INDEX_TRANSFER_FAILED_1 =
      "IoTConsensusV2-ConsensusGroup-{}: Event {} replicate index {} transfer failed, will be "
          + "added to retry queue.";
  public static final String IOTCONSENSUSV2_CONSENSUSGROUP_NO_EVENT_ADDED_TO_CONNECTOR =
      "IoTConsensusV2-ConsensusGroup-{}: no.{} event-{} added to connector buffer";
  public static final String IOTCONSENSUSV2_CONSENSUSGROUP_ONE_EVENT_SUCCESSFULLY_RECEIVED_BY =
      "IoTConsensusV2-ConsensusGroup-{}: one event-{} successfully received by the follower, "
          + "will be removed from queue, queue size = {}, limit size = {}";
  public static final String IOTCONSENSUSV2_CONSENSUSGROUP_RETRYEVENTQUEUE_IS_NOT_EMPTY_AFTER =
      "IoTConsensusV2-ConsensusGroup-{}: retryEventQueue is not empty after 20 seconds. "
          + "retryQueue size: {}";
  public static final String IOTCONSENSUSV2_CONSENSUSGROUP_RETRY_WITH_INTERVAL_FOR_INDEX =
      "IoTConsensusV2-ConsensusGroup-{}: retry with interval {} for index {} {}";
  public static final String IOTCONSENSUSV2_CONSENSUSGROUP_TRY_TO_REMOVE_EVENT_AFTER =
      "IoTConsensusV2-ConsensusGroup-{}: try to remove event-{} after "
          + "iotConsensusV2AsyncConnector being closed. Ignore it.";
  public static final String IOTCONSENSUSV2_FAILED_TO_CLOSE_FILE_READER_WHEN =
      "IoTConsensusV2-{}：关闭 file reader when failed to transfer file 失败。";
  public static final String IOTCONSENSUSV2_FAILED_TO_CLOSE_FILE_READER_WHEN_1 =
      "IoTConsensusV2-{}：关闭 file reader when successfully transferred file 失败。";
  public static final String IOTCONSENSUSV2_FAILED_TO_CLOSE_FILE_READER_WHEN_2 =
      "IoTConsensusV2-{}：关闭 file reader when successfully transferred mod file 失败。";
  public static final String IOTCONSENSUSV2_FAILED_TO_TRANSFER_TABLETINSERTIONEVENT_BATCH_TOTAL =
      "IoTConsensusV2：传输 TabletInsertionEvent batch. Total failed events: {}, related pipe "
          + "names: {} 失败";
  public static final String IOTCONSENSUSV2_FAILED_TO_TRANSFER_TSFILEINSERTIONEVENT_COMMITTER_KEY =
      "IoTConsensusV2-{}：传输 TsFileInsertionEvent {} (committer key {}, replicate index {}) 失败。";
  public static final String IOTCONSENSUSV2_REDIRECT_FILE_POSITION_TO =
      "IoTConsensusV2-{}：Redirect file position to {}.";
  public static final String IOTCONSENSUSV2_SUCCESSFULLY_TRANSFERRED_FILE_COMMITTER_KEY_REPLICATE =
      "IoTConsensusV2-{}：成功 transferred file {} (committer key={}, replicate index={})。";
  public static final String IOTDBCDCCONNECTOR_ONLY_SUPPORT_PIPEINSERTNODETABLETINSERTIONEVENT_AND_PIPERAWTAB =
      "IoTDBCDCConnector only support PipeInsertNodeTabletInsertionEvent and "
          + "PipeRawTabletInsertionEvent.";
  public static final String IOTDBDATAREGIONAIRGAPCONNECTOR_DOES_NOT_SUPPORT_TRANSFERRING_GENERIC_EVENT =
      "IoTDBDataRegionAirGapConnector 不支持 transferring generic event: {}.";
  public static final String IOTDBDATAREGIONAIRGAPCONNECTOR_ONLY_SUPPORT_PIPEINSERTNODETABLETINSERTIONEVENT_A =
      "IoTDBDataRegionAirGapConnector only support PipeInsertNodeTabletInsertionEvent and "
          + "PipeRawTabletInsertionEvent. Ignore {}.";
  public static final String IOTDBDATAREGIONAIRGAPCONNECTOR_ONLY_SUPPORT_PIPETSFILEINSERTIONEVENT_IGNORE =
      "IoTDBDataRegionAirGapConnector only support PipeTsFileInsertionEvent. Ignore {}.";
  public static final String FAILED_TO_LOGIN_TO_RECEIVER_FOR_LEGACY_PIPE_TRANSFER =
      "登录 receiver %s:%s for legacy pipe transfer 失败，原因：code: %d, message: %s";
  public static final String IOTDBLEGACYPIPECONNECTOR_DOES_NOT_SUPPORT_TRANSFERRING_GENERIC_EVENT =
      "IoTDBLegacyPipeConnector 不支持 transferring generic event: {}.";
  public static final String IOTDBLEGACYPIPECONNECTOR_ONLY_SUPPORT_PIPEINSERTNODEINSERTIONEVENT_AND_PIPETABLE =
      "IoTDBLegacyPipeConnector only support PipeInsertNodeInsertionEvent and "
          + "PipeTabletInsertionEvent.";
  public static final String IOTDBLEGACYPIPECONNECTOR_ONLY_SUPPORT_PIPETSFILEINSERTIONEVENT =
      "IoTDBLegacyPipeConnector only support PipeTsFileInsertionEvent.";
  public static final String IOTDBSCHEMAREGIONAIRGAPSINK_CAN_T_TRANSFER_TABLETINSERTIONEVENT =
      "IoTDBSchemaRegionAirGapSink can't transfer TabletInsertionEvent.";
  public static final String IOTDBSCHEMAREGIONAIRGAPSINK_CAN_T_TRANSFER_TSFILEINSERTIONEVENT =
      "IoTDBSchemaRegionAirGapSink can't transfer TsFileInsertionEvent.";
  public static final String IOTDBSCHEMAREGIONAIRGAPSINK_DOES_NOT_SUPPORT_TRANSFERRING_GENERIC_EVENT =
      "IoTDBSchemaRegionAirGapSink 不支持 transferring generic event: {}.";
  public static final String IOTDBSCHEMAREGIONCONNECTOR_CAN_T_TRANSFER_TABLETINSERTIONEVENT =
      "IoTDBSchemaRegionConnector can't transfer TabletInsertionEvent.";
  public static final String IOTDBSCHEMAREGIONCONNECTOR_CAN_T_TRANSFER_TSFILEINSERTIONEVENT =
      "IoTDBSchemaRegionConnector can't transfer TsFileInsertionEvent.";
  public static final String IOTDBSCHEMAREGIONCONNECTOR_DOES_NOT_SUPPORT_TRANSFERRING_GENERIC_EVENT =
      "IoTDBSchemaRegionConnector 不支持 transferring generic event: {}.";
  public static final String IOTDBTHRIFTASYNCCONNECTOR_DOES_NOT_SUPPORT_TRANSFERRING_GENERIC_EVENT =
      "IoTDBThriftAsyncConnector 不支持 transferring generic event: {}.";
  public static final String IOTDBTHRIFTASYNCCONNECTOR_DOES_NOT_SUPPORT_TRANSFER_GENERIC_EVENT =
      "IoTDBThriftAsyncConnector 不支持 transfer generic event: {}.";
  public static final String IOTDBTHRIFTASYNCCONNECTOR_ONLY_SUPPORT_PIPEINSERTNODETABLETINSERTIONEVENT_AND_PI =
      "IoTDBThriftAsyncConnector only support PipeInsertNodeTabletInsertionEvent and "
          + "PipeRawTabletInsertionEvent. Current event: {}.";
  public static final String IOTDBTHRIFTASYNCCONNECTOR_ONLY_SUPPORT_PIPETSFILEINSERTIONEVENT_CURRENT_EVENT =
      "IoTDBThriftAsyncConnector only support PipeTsFileInsertionEvent. Current event: {}.";
  public static final String IOTDBTHRIFTSYNCCONNECTOR_DOES_NOT_SUPPORT_TRANSFERRING_GENERIC_EVENT =
      "IoTDBThriftSyncConnector 不支持 transferring generic event: {}.";
  public static final String IOTDBTHRIFTSYNCCONNECTOR_ONLY_SUPPORT_PIPEINSERTNODETABLETINSERTIONEVENT_AND_PIP =
      "IoTDBThriftSyncConnector only support PipeInsertNodeTabletInsertionEvent and "
          + "PipeRawTabletInsertionEvent. Ignore {}.";
  public static final String IOTDBTHRIFTSYNCCONNECTOR_ONLY_SUPPORT_PIPETSFILEINSERTIONEVENT_IGNORE =
      "IoTDBThriftSyncConnector only support PipeTsFileInsertionEvent. Ignore {}.";
  public static final String LEADERCACHEMANAGER_ALLOCATEDMEMORYBLOCK_HAS_EXPANDED_FROM_TO =
      "LeaderCacheManager.allocatedMemoryBlock has expanded from {} to {}.";
  public static final String LEADERCACHEMANAGER_ALLOCATEDMEMORYBLOCK_HAS_SHRUNK_FROM_TO =
      "LeaderCacheManager.allocatedMemoryBlock has shrunk from {} to {}.";
  public static final String LOADING_KEYSTORE_AT = "正在从 {} 加载 KeyStore";
  public static final String LOADING_KEYSTORE_AT_1 = "正在从 {}. 加载 KeyStore";
  public static final String LOAD_KEYSTORE_FAILED_THE_EXISTING_KEYSTORE_MAY =
      "Load keyStore failed, the existing keyStore may be stale, re-constructing...";
  public static final String NO_OPC_CLIENT_OR_SERVER_IS_SPECIFIED =
      "No OPC client or server is specified when transferring tablet";
  public static final String OPC_DA_SINK_MUST_RUN_ON_WINDOWS = "opc-da-sink 必须在 Windows 系统上运行。";
  public static final String PIPETABLEMODETSFILEBUILDERV2_DOES_NOT_SUPPORT_TREE_MODEL_TABLET =
      "PipeTableModeTsFileBuilderV2 不支持 tree model tablet to build TSFile";
  public static final String PIPETABLEMODETSFILEBUILDER_DOES_NOT_SUPPORT_TREE_MODEL_TABLET =
      "PipeTableModeTsFileBuilder 不支持 tree model tablet to build TSFile";
  public static final String PIPETREEMODELTSFILEBUILDERV2_DOES_NOT_SUPPORT_TABLE_MODEL_TABLET =
      "PipeTreeModelTsFileBuilderV2 不支持 table model tablet to build TSFile";
  public static final String PIPETREEMODELTSFILEBUILDER_DOES_NOT_SUPPORT_TABLE_MODEL_TABLET =
      "PipeTreeModelTsFileBuilder 不支持 table model tablet to build TSFile";
  public static final String POLLED_EVENT_FROM_RETRY_QUEUE = "Polled event {} from retry queue.";
  public static final String RECEIVED_AN_ERROR_MESSAGE_FROM =
      "Received an error message {} from {}:{}";
  public static final String RECEIVED_AN_UNKNOWN_MESSAGE_FROM =
      "Received an unknown message {} from {}:{}";
  public static final String RECEIVED_A_ACK_MESSAGE_FROM = "Received a ack message from {}:{}";
  public static final String RECEIVED_A_BIND_MESSAGE_FROM = "Received a bind message from {}:{}";
  public static final String REDIRECT_FILE_POSITION_TO = "Redirect file position to {}.";
  public static final String REDIRECT_TO_POSITION_IN_TRANSFERRING_TSFILE =
      "Redirect to position {} in transferring tsFile {}.";
  public static final String SECURITY_DIR = "security dir: {}";
  public static final String SECURITY_PKI_DIR = "security pki dir: {}";
  public static final String SUCCESSFULLY_ADDED_ITEM = "成功 added item {}。";
  public static final String SUCCESSFULLY_CONVERTED_PROGID_TO_CLSID =
      "成功 converted progID {} to CLSID: {{}}";
  public static final String SUCCESSFULLY_SHUTDOWN_EXECUTOR = "成功 shutdown executor {}。";
  public static final String SUCCESSFULLY_TRANSFERRED_DELETION_EVENT =
      "成功 transferred deletion event {}。";
  public static final String SUCCESSFULLY_TRANSFERRED_FILE = "成功 transferred file {}。";
  public static final String SUCCESSFULLY_TRANSFERRED_FILE_AND =
      "成功 transferred file {}, {} and {}。";
  public static final String SUCCESSFULLY_TRANSFERRED_FILE_BATCHED_TABLEINSERTIONEVENTS_REFERENCE_COUNT =
      "成功 transferred file {} (batched TableInsertionEvents, reference count={})。";
  public static final String SUCCESSFULLY_TRANSFERRED_FILE_COMMITTER_KEY_COMMIT_ID =
      "成功 transferred file {} (committer key={}, commit id={}, reference count={})。";
  public static final String SUCCESSFULLY_TRANSFERRED_SCHEMA_EVENT =
      "成功 transferred schema event {}。";
  public static final String SUCCESSFULLY_TRANSFERRED_SCHEMA_REGION_SNAPSHOT_AND =
      "成功 transferred schema region snapshot {}, {} and {}。";
  public static final String THE_BATCH_SIZE_LIMIT_HAS_EXPANDED_FROM =
      "The batch size limit has expanded from {} to {}.";
  public static final String THE_BATCH_SIZE_LIMIT_HAS_SHRUNK_FROM =
      "The batch size limit has shrunk from {} to {}.";
  public static final String THE_DEFAULT_QUALITY_CAN_ONLY_BE_GOOD =
      "The default quality can only be 'GOOD', 'BAD' or 'UNCERTAIN'.";
  public static final String THE_EVENT_ACK_IS_NOT_FOUND = "The event ack {} is not found.";
  public static final String THE_EVENT_CAN_T_BE_TRANSFERRED_TO =
      "The event {} can't be transferred to client, it will be retried later.";
  public static final String THE_EVENT_IN_ERROR_IS_NOT_FOUND =
      "The event in error {} is not found.";
  public static final String THE_EVENT_POLLED_FROM_THE_QUEUE_IS =
      "The event polled from the queue is not the same as the event peeked from the queue. "
          + "Peeked event: {}, polled event: {}.";
  public static final String THE_FILE_IS_NOT_FOUND_MAY_ALREADY =
      "The file {} is not found, may already be deleted.";
  public static final String NETWORK_FAILED_TO_RECEIVE_TSFILE_STATUS =
      "网络接收 TsFile %s 失败，状态：%s";
  public static final String THE_PIPE_WAS_DROPPED_SO_THE_EVENT =
      "The pipe {} was dropped so the event ack {} will be ignored.";
  public static final String THE_PIPE_WAS_DROPPED_SO_THE_EVENT_1 =
      "The pipe {} was dropped so the event in error {} will be ignored.";
  public static final String THE_PIPE_WAS_DROPPED_SO_THE_EVENT_2 =
      "The pipe {} was dropped so the event {} will be dropped.";
  public static final String THE_QUALITY_VALUE_ONLY_SUPPORTS_BOOLEAN_TYPE =
      "The quality value only supports boolean type, while true == GOOD and false == BAD.";
  public static final String THE_SCHEMA_REGION_AIR_GAP_CONNECTOR_DOES =
      "The schema region air gap connector 不支持 transferring single file piece bytes.";
  public static final String THE_SCHEMA_REGION_CONNECTOR_DOES_NOT_SUPPORT =
      "The schema region connector 不支持 transferring single file piece req.";
  public static final String THE_SECURITY_POLICY_CANNOT_BE_EMPTY =
      "The security policy cannot be empty.";
  public static final String THE_SECURITY_POLICY_CAN_ONLY_BE_NONE =
      "The security policy can only be 'None', 'Basic128Rsa15', 'Basic256', 'Basic256Sha256', "
          + "'Aes128_Sha256_RsaOaep' or 'Aes256_Sha256_RsaPss'.";
  public static final String THE_SEGMENTS_OF_TABLETS_MUST_EXIST =
      "The segments of tablets must exist";
  public static final String THE_TABLET_OF_COMMITID_CAN_T_BE =
      "The tablet of commitId: {} can't be parsed by client, it will be retried later.";
  public static final String THE_TRANSFER_THREAD_IS_INTERRUPTED = "传输线程被中断。";
  public static final String THE_WEBSOCKET_CONNECTION_FROM_CLIENT_HAS_BEEN =
      "The websocket connection from client 已关闭!The code is {}. The reason is {}. Is it closed "
          + "by remote? {}";
  public static final String THE_WEBSOCKET_CONNECTION_FROM_CLIENT_HAS_BEEN_1 =
      "The websocket connection from client {}:{} 已关闭! The code is {}. The reason is {}. Is it "
          + "closed by remote? {}";
  public static final String THE_WEBSOCKET_CONNECTION_FROM_CLIENT_HAS_BEEN_2 =
      "The websocket connection from client {}:{} has been opened!";
  public static final String THE_WEBSOCKET_CONNECTION_FROM_HAS_BEEN_CLOSED =
      "The websocket connection from {}:{} 已关闭, but the ack message of commitId: {} is received.";
  public static final String THE_WEBSOCKET_CONNECTION_FROM_HAS_BEEN_CLOSED_1 =
      "The websocket connection from {}:{} 已关闭, but the error message of commitId: {} is received.";
  public static final String THE_WEBSOCKET_SERVER_HAS_BEEN_STARTED =
      "The websocket server {}:{} 已启动!";
  public static final String THE_WRITTEN_TABLET_TIME_MAY_OVERLAP_OR =
      "The written Tablet time may overlap or the Schema may be incorrect";
  public static final String THIS_CONNECTOR_ONLY_SUPPORT_PIPEINSERTNODETABLETINSERTIONEVENT_AND_PIPERAWTABLET =
      "This Connector only support PipeInsertNodeTabletInsertionEvent and "
          + "PipeRawTabletInsertionEvent. Ignore {}.";
  public static final String TIMED_OUT_WHEN_WAITING_FOR_CLIENT_HANDSHAKE =
      "Timed out when waiting for client handshake finish.";
  public static final String TIOTCONSENSUSV2BATCHTRANSFERRESP_IS_NULL =
      "TIoTConsensusV2BatchTransferResp 为空";
  public static final String TIOTCONSENSUSV2TRANSFERRESP_IS_NULL = "TIoTConsensusV2TransferResp 为空";
  public static final String TPIPETRANSFERRESP_IS_NULL = "TPipeTransferResp 为空";
  public static final String TRANSFER_TSFILE_EVENT_ASYNCHRONOUSLY_WAS_INTERRUPTED =
      "Transfer tsfile event {} asynchronously was interrupted.";
  public static final String UNABLE_TO_CREATE_SECURITY_DIR = "无法创建 security dir: ";
  public static final String UNKNOWN_LOAD_BALANCE_STRATEGY_USE_ROUND_ROBIN =
      "未知的 load balance strategy: {}, use round-robin strategy instead。";
  public static final String UNSUPPORTED_BATCH_TYPE = "不支持的 batch type {}。";
  public static final String UNSUPPORTED_BATCH_TYPE_WHEN_TRANSFERRING_TABLET_INSERTION =
      "不支持的 batch type {} when transferring tablet insertion event。";
  public static final String UNSUPPORTED_DATATYPE = "不支持的 dataType ";
  public static final String UNSUPPORTED_EVENT_TYPE_WHEN_BUILDING_TRANSFER_REQUEST =
      "不支持的 event {} type {} when building transfer request";
  public static final String WAIT_FOR_RESOURCE_ENOUGH_FOR_SLICING_TSFILE =
      "等待 resource enough，已等待 slicing tsfile {} for {} 秒。";
  public static final String WEBSOCKETCONNECTOR_FAILED_TO_INCREASE_THE_REFERENCE_COUNT =
      "WebsocketConnector failed to increase the reference count of the event. Ignore it. "
          + "Current event: {}.";
  public static final String WEBSOCKETCONNECTOR_ONLY_SUPPORT_PIPEINSERTNODETABLETINSERTIONEVENT_AND_PIPERAWTA =
      "WebsocketConnector only support PipeInsertNodeTabletInsertionEvent and "
          + "PipeRawTabletInsertionEvent. Current event: {}.";
  public static final String WEBSOCKETCONNECTOR_ONLY_SUPPORT_PIPETSFILEINSERTIONEVENT_CURRENT_EVENT =
      "WebsocketConnector only support PipeTsFileInsertionEvent. Current event: {}.";
  public static final String WHEN_THE_OPC_UA_SINK_POINTS_TO =
      "When the OPC UA sink points to an outer server, the table model data is not supported.";
  public static final String WHEN_THE_OPC_UA_SINK_SETS_WITH =
      "When the OPC UA sink sets 'with-quality' to true, the table model data is not supported.";
  public static final String WRITEBACKSINK_ONLY_SUPPORT_PIPEINSERTNODETABLETINSERTIONEVENT_AND_PIPERAWTABLETI =
      "WriteBackSink only support PipeInsertNodeTabletInsertionEvent and "
          + "PipeRawTabletInsertionEvent. Ignore {}.";

  // ===================== RECEIVER =====================

  public static final String ALL_RECEIVERS_RELATED_TO_ARE_RELEASED =
      "All Receivers related to {} are released.";
  public static final String AUTO_CREATE_DATABASE_FAILED_BECAUSE = "自动创建 database failed because: ";
  public static final String CREATE_DATABASE_ERROR_STATEMENT_RESULT_STATUS =
      "创建 Database error, statement: {}, result status : {}.";
  public static final String DATABASE_NAME_IS_UNEXPECTEDLY_NULL_FOR_LOADTSFILESTATEMENT =
      "Database name is unexpectedly null for LoadTsFileStatement: {}. Skip data type conversion.";
  public static final String DATABASE_NAME_IS_UNEXPECTEDLY_NULL_FOR_STATEMENT =
      "Database name is unexpectedly null for statement: {}. Skip data type conversion.";
  public static final String DATA_TYPE_CONVERSION_FOR_LOADTSFILESTATEMENT_IS_SUCCESSFUL =
      "Data type conversion for LoadTsFileStatement {} is successful.";
  public static final String DATA_TYPE_MISMATCH_DETECTED_TSSTATUS_FOR_LOADTSFILESTATEMENT =
      "Data type mismatch detected (TSStatus: {}) for LoadTsFileStatement: {}. Start data type "
          + "conversion.";
  public static final String DELETE_ERROR_STATEMENT = "Delete {} error, statement: {}.";
  public static final String DELETE_RESULT_STATUS = "Delete result status : {}.";
  public static final String FAILED_TO_CLOSE_IOTDBAIRGAPRECEIVERAGENT_S_SERVER_SOCKET =
      "关闭 IoTDBAirGapReceiverAgent's server socket 失败";
  public static final String FAILED_TO_CONVERT_DATA_TYPE_FOR_LOADTSFILESTATEMENT =
      "转换 data type for LoadTsFileStatement: {} 失败。";
  public static final String FAILED_TO_EXECUTE_STATEMENT_AFTER_DATA_TYPE =
      "execute statement after data type conversion 失败。";
  public static final String FAILED_TO_HANDLE_CONFIG_CLIENT_ID_EXIT =
      "处理 config client (id = {}) exit 失败";
  public static final String FAIL_TO_CREATE_IOTCONSENSUSV2_RECEIVER_FILE_FOLDERS =
      "创建 iotConsensusV2 receiver file folders allocation strategy 失败，原因：all disks of folders "
          + "are full.";
  public static final String FAIL_TO_CREATE_PIPE_RECEIVER_FILE_FOLDERS =
      "创建 pipe receiver file folders allocation strategy 失败，原因：all disks of folders are full.";
  public static final String FAIL_TO_INITIATE_FILE_BUFFER_FOLDER_ERROR =
      "初始化 file buffer folder, Error msg: {} 失败";
  public static final String FAIL_TO_LOAD_PIPEDATA_BECAUSE = "加载 pipeData 失败，原因：{}.";
  public static final String FAIL_TO_RENAME_FILE_TO = "rename file {} to {} 失败";
  public static final String INVOKE_HANDSHAKE_METHOD_FROM_CLIENT_IP =
      "Invoke handshake method from client ip = {}";
  public static final String INVOKE_TRANSPORTDATA_METHOD_FROM_CLIENT_IP =
      "Invoke transportData method from client ip = {}";
  public static final String INVOKE_TRANSPORTPIPEDATA_METHOD_FROM_CLIENT_IP =
      "Invoke transportPipeData method from client ip = {}";
  public static final String IOTCONSENSUSV2RECEIVER_THREAD_IS_INTERRUPTED_WHEN_WAITING_FOR =
      "IoTConsensusV2Receiver thread is interrupted when waiting for receiver get initiated, "
          + "may because system exit.";
  public static final String IOTCONSENSUSV2_PIPENAME = "IoTConsensusV2-PipeName-{}：{}";
  public static final String IOTCONSENSUSV2_PIPENAME_CURRENT_WAITING_IS_INTERRUPTED_ONSYNCEDCOMMITINDEX =
      "IoTConsensusV2-PipeName-{}：current waiting is interrupted. onSyncedCommitIndex: {}. "
          + "Exception: ";
  public static final String IOTCONSENSUSV2_PIPENAME_CURRENT_WRITING_FILE_WRITER_IS =
      "IoTConsensusV2-PipeName-{}：Current writing file writer 为空，无需关闭。";
  public static final String IOTCONSENSUSV2_PIPENAME_CURRENT_WRITING_FILE_WRITER_WAS =
      "IoTConsensusV2-PipeName-{}：Current writing file writer {} 已关闭.";
  public static final String IOTCONSENSUSV2_PIPENAME_FAILED_TO_CLOSE_CURRENT_WRITING =
      "IoTConsensusV2-PipeName-{}：关闭 current writing file writer {} 失败，原因：{}.";
  public static final String IOTCONSENSUSV2_PIPENAME_FAILED_TO_CREATE_RECEIVER_FILE =
      "IoTConsensusV2-PipeName-{}：创建 receiver file dir {} 失败。";
  public static final String IOTCONSENSUSV2_PIPENAME_FAILED_TO_CREATE_RECEIVER_FILE_1 =
      "IoTConsensusV2-PipeName-{}：创建 receiver file dir {}. Because parent system dir have been "
          + "deleted due to system concurrently exit 失败。";
  public static final String IOTCONSENSUSV2_PIPENAME_FAILED_TO_CREATE_RECEIVER_FILE_2 =
      "IoTConsensusV2-PipeName-{}：创建 receiver file dir {}. May 失败，原因：authority or dir already "
          + "exists etc.";
  public static final String IOTCONSENSUSV2_PIPENAME_FAILED_TO_CREATE_RECEIVER_TSFILEWRITER =
      "IoTConsensusV2-PipeName-{}：创建 receiver tsFileWriter-{} file dir {} 失败";
  public static final String IOTCONSENSUSV2_PIPENAME_FAILED_TO_DELETE_BECAUSE =
      "IoTConsensusV2-PipeName-{}：{} Failed to delete {}, because {}.";
  public static final String IOTCONSENSUSV2_PIPENAME_FAILED_TO_GET_BASE_DIRECTORY =
      "IoTConsensusV2-PipeName-{}：获取 base directory 失败";
  public static final String IOTCONSENSUSV2_PIPENAME_FAILED_TO_LOAD_FILE_FROM =
      "IoTConsensusV2-PipeName-{}：加载 file {} from req {} 失败。";
  public static final String IOTCONSENSUSV2_PIPENAME_FAILED_TO_READ_TSFILE_WHEN =
      "IoTConsensusV2-PipeName-{}：读取 TsFile when counting points: {} 失败。";
  public static final String IOTCONSENSUSV2_PIPENAME_FAILED_TO_RETURN_TSFILEWRITER =
      "IoTConsensusV2-PipeName-{}：return tsFileWriter {} 失败。";
  public static final String IOTCONSENSUSV2_PIPENAME_FAILED_TO_SEAL_FILE_BECAUSE =
      "IoTConsensusV2-PipeName-{}：封存 file {} 失败，原因：the file does not exist.";
  public static final String IOTCONSENSUSV2_PIPENAME_FAILED_TO_SEAL_FILE_BECAUSE_1 =
      "IoTConsensusV2-PipeName-{}：封存 file {} 失败，原因：writing file is {}.";
  public static final String IOTCONSENSUSV2_PIPENAME_FAILED_TO_SEAL_FILE_BECAUSE_2 =
      "IoTConsensusV2-PipeName-{}：封存 file {} 失败，原因：{}.";
  public static final String IOTCONSENSUSV2_PIPENAME_FAILED_TO_SEAL_FILE_FROM =
      "IoTConsensusV2-PipeName-{}：封存 file {} from req {} 失败。";
  public static final String IOTCONSENSUSV2_PIPENAME_FAILED_TO_SEAL_FILE_STATUS =
      "IoTConsensusV2-PipeName-{}：封存 file {}, status is {} 失败。";
  public static final String IOTCONSENSUSV2_PIPENAME_FAILED_TO_SEAL_FILE_WHEN =
      "IoTConsensusV2-PipeName-{}：封存 file {} when check final seal file 失败，原因：the length of "
          + "file is not correct. The original file has length {}, but receiver file has length {}.";
  public static final String IOTCONSENSUSV2_PIPENAME_FAILED_TO_SEAL_FILE_WHEN_1 =
      "IoTConsensusV2-PipeName-{}：封存 file {} when check non final seal 失败，原因：the length of "
          + "file is not correct. The original file has length {}, but receiver file has length {}.";
  public static final String IOTCONSENSUSV2_PIPENAME_FAILED_TO_WRITE_FILE_PIECE =
      "IoTConsensusV2-PipeName-{}：写入 file piece from req {} 失败。";
  public static final String IOTCONSENSUSV2_PIPENAME_FILE_OFFSET_RESET_REQUESTED_BY =
      "IoTConsensusV2-PipeName-{}：File offset reset requested by receiver, response status = {}.";
  public static final String IOTCONSENSUSV2_PIPENAME_ILLEGAL_FILE_NAME_WHEN_CHECKING =
      "IoTConsensusV2-PipeName-{}：非法的 file name {} when checking writing file。";
  public static final String IOTCONSENSUSV2_PIPENAME_IS_NOT_EXISTED_NO_NEED =
      "IoTConsensusV2-PipeName-{}：{} {} 不存在，无需删除。";
  public static final String IOTCONSENSUSV2_PIPENAME_NO_EVENT_GET_EXECUTED_AFTER =
      "IoTConsensusV2-PipeName-{}：no.{} event get executed after awaiting timeout, current "
          + "receiver syncIndex: {}";
  public static final String IOTCONSENSUSV2_PIPENAME_NO_EVENT_GET_EXECUTED_BECAUSE =
      "IoTConsensusV2-PipeName-{}：no.{} event get executed because receiver buffer's len >= "
          + "pipeline, current receiver syncIndex {}, current buffer len {}";
  public static final String IOTCONSENSUSV2_PIPENAME_PATH_TRAVERSAL_ATTEMPT_DETECTED_FILENAME =
      "IoTConsensusV2-PipeName-{}：Path traversal attempt detected! Filename: {}";
  public static final String IOTCONSENSUSV2_PIPENAME_PROCESS_NO_EVENT_SUCCESSFULLY =
      "IoTConsensusV2-PipeName-{}：process no.{} event successfully!";
  public static final String IOTCONSENSUSV2_PIPENAME_RECEIVED_A_DEPRECATED_REQUEST_WHICH =
      "IoTConsensusV2-PipeName-{}：received a deprecated request-{}, which may because {}. ";
  public static final String IOTCONSENSUSV2_PIPENAME_RECEIVER_DETECTED_AN_NEWER_PIPETASKRESTARTTIMES =
      "IoTConsensusV2-PipeName-{}：receiver detected an newer pipeTaskRestartTimes, which "
          + "indicates the pipe task has restarted. receiver will reset all its data.";
  public static final String IOTCONSENSUSV2_PIPENAME_RECEIVER_DETECTED_AN_NEWER_REBOOTTIMES =
      "IoTConsensusV2-PipeName-{}：receiver detected an newer rebootTimes, which indicates the "
          + "leader has rebooted. receiver will reset all its data.";
  public static final String IOTCONSENSUSV2_PIPENAME_RECEIVER_FILE_DIR_WAS_CREATED =
      "IoTConsensusV2-PipeName-{}：Receiver file dir {} 已创建.";
  public static final String IOTCONSENSUSV2_PIPENAME_RECEIVER_THREAD_GET_INTERRUPTED_WHEN =
      "IoTConsensusV2-PipeName-{}：receiver thread get interrupted when exiting.";
  public static final String IOTCONSENSUSV2_PIPENAME_SEAL_FILE_SUCCESSFULLY =
      "IoTConsensusV2-PipeName-{}：成功封存 file {}。";
  public static final String IOTCONSENSUSV2_PIPENAME_SEAL_FILE_WITH_MODS_SUCCESSFULLY =
      "IoTConsensusV2-PipeName-{}：成功封存 file with mods {}。";
  public static final String IOTCONSENSUSV2_PIPENAME_SKIP_LOAD_TSFILE_WHEN_SEALING =
      "IoTConsensusV2-PipeName-{}：skip load tsfile-{} when sealing, because this region has "
          + "been removed or migrated.";
  public static final String IOTCONSENSUSV2_PIPENAME_STARTING_TO_RECEIVE_TSFILE_PIECES =
      "IoTConsensusV2-PipeName-{}：开始接收 tsFile pieces";
  public static final String IOTCONSENSUSV2_PIPENAME_STARTING_TO_RECEIVE_TSFILE_SEAL =
      "IoTConsensusV2-PipeName-{}：开始接收 tsFile seal";
  public static final String IOTCONSENSUSV2_PIPENAME_STARTING_TO_RECEIVE_TSFILE_SEAL_1 =
      "IoTConsensusV2-PipeName-{}：开始接收 tsFile seal with mods";
  public static final String IOTCONSENSUSV2_PIPENAME_START_TO_RECEIVE_NO_EVENT =
      "IoTConsensusV2-PipeName-{}：开始接收 no.{} event";
  public static final String IOTCONSENSUSV2_PIPENAME_THE_POINT_COUNT_OF_TSFILE =
      "IoTConsensusV2-PipeName-{}：The point count of TsFile {} is not given by sender, will "
          + "read actual point count from TsFile.";
  public static final String IOTCONSENSUSV2_PIPENAME_TSFILEWRITER_RETURNED_SELF =
      "IoTConsensusV2-PipeName-{}：tsFileWriter-{} returned self";
  public static final String IOTCONSENSUSV2_PIPENAME_TSFILEWRITER_ROLL_TO_WRITING_PATH =
      "IoTConsensusV2-PipeName-{}：tsfileWriter-{} roll to writing path {}";
  public static final String IOTCONSENSUSV2_PIPENAME_TSFILE_WRITER_IS_CLEANED_UP =
      "IoTConsensusV2-PipeName-{}：tsfile writer-{} is cleaned up because no new requests were "
          + "received for too long.";
  public static final String IOTCONSENSUSV2_PIPENAME_UNKNOWN_PIPEREQUESTTYPE_RESPONSE_STATUS =
      "IoTConsensusV2-PipeName-{}：未知的 PipeRequestType, response status = {}。";
  public static final String IOTCONSENSUSV2_PIPENAME_WAS_DELETED =
      "IoTConsensusV2-PipeName-{}：{} {} 已删除.";
  public static final String IOTCONSENSUSV2_PIPENAME_WRITING_FILE_IS_NOT_AVAILABLE =
      "IoTConsensusV2-PipeName-{}：Writing file {} 不可用. Writing file is null: {}, writing file "
          + "exists: {}, writing file writer is null: {}.";
  public static final String IOTCONSENSUSV2_PIPENAME_WRITING_FILE_IS_NOT_EXISTED =
      "IoTConsensusV2-PipeName-{}：Writing file {} 不存在或名称不正确，尝试创建。Current writing file is {}.";
  public static final String IOTCONSENSUSV2_PIPENAME_WRITING_FILE_S_OFFSET_IS =
      "IoTConsensusV2-PipeName-{}：Writing file {}'s offset is {}, but request sender's offset "
          + "is {}.";
  public static final String IOTCONSENSUSV2_PIPENAME_WRITING_FILE_WAS_CREATED_READY =
      "IoTConsensusV2-PipeName-{}：Writing file {} 已创建. Ready to write file pieces.";
  public static final String IOTCONSENSUSV2_RECEIVE_ON_THE_FLY_NO_EVENT =
      "IoTConsensusV2-{}：receive on-the-fly no.{} event after data region 已删除, discard it";
  public static final String IOTCONSENSUSV2_TRANSFER_BATCH_HASN_T_BEEN_IMPLEMENTED =
      "IoTConsensusV2 transfer batch hasn't been implemented yet.";
  public static final String IOTCONSENSUSV2_TSFILEWRITER_SET_NULL_WRITING_FILE =
      "IoTConsensusV2-{}：TsFileWriter-{} set null writing file";
  public static final String IOTCONSENSUSV2_TSFILEWRITER_SET_NULL_WRITING_FILE_WRITER =
      "IoTConsensusV2-{}：TsFileWriter-{} set null writing file writer";
  public static final String IOTCONSENSUSV2_UNKNOWN_IOTCONSENSUSV2REQUESTVERSION_RESPONSE_STATUS =
      "IoTConsensusV2：未知的 IoTConsensusV2RequestVersion, response status = {}。";
  public static final String IOTCONSENSUSV2_UNKNOWN_PIPEREQUESTTYPE_RESPONSE_STATUS =
      "IoTConsensusV2 Unknown PipeRequestType, response status = {}.";
  public static final String IOTCONSENSUSV2_WAITING_FOR_THE_PREVIOUS_EVENT_TIMES =
      "IoTConsensusV2-{}：等待 the previous event times out, current peek {}, current id {}";
  public static final String IOTDBAIRGAPRECEIVERAGENT_STARTED =
      "IoTDBAirGapReceiverAgent {} started.";
  public static final String IOTDBAIRGAPRECEIVERAGENT_STOPPED =
      "IoTDBAirGapReceiverAgent {} stopped.";
  public static final String LOAD_ACTIVE_LISTENING_PIPE_DIR_IS_NOT =
      "Load active listening pipe dir is not set.";
  public static final String LOAD_PIPEDATA_WITH_SERIALIZE_NUMBER_SUCCESSFULLY =
      "Load pipeData with serialize number {} successfully.";
  public static final String LOAD_TSFILE_ERROR_STATEMENT = "Load TsFile {} error, statement: {}.";
  public static final String LOAD_TSFILE_RESULT_STATUS = "Load TsFile result status : {}.";
  public static final String PARSE_DATABASE_PARTIALPATH_ERROR = "Parse database PartialPath {} 出错。";
  public static final String PIPE_AIR_GAP_RECEIVER_CHECKSUM_FAILED_EXPECTED =
      "Pipe air gap receiver {}: checksum failed, expected: {}, actual: {}";
  public static final String PIPE_AIR_GAP_RECEIVER_CLOSED_BECAUSE_OF =
      "Pipe air gap receiver {} closed because of checksum failed. Socket: {}";
  public static final String PIPE_AIR_GAP_RECEIVER_CLOSED_BECAUSE_OF_1 =
      "Pipe air gap receiver {} closed because of exception. Socket: {}";
  public static final String PIPE_AIR_GAP_RECEIVER_CLOSED_BECAUSE_SOCKET =
      "Pipe air gap receiver {} closed because socket 已关闭. Socket: {}";
  public static final String PIPE_AIR_GAP_RECEIVER_EXCEPTION_DURING_HANDLING =
      "Pipe air gap receiver {}: Exception during handling receiving. Socket: {}";
  public static final String PIPE_AIR_GAP_RECEIVER_HANDLE_DATA_FAILED =
      "Pipe air gap receiver {}: Handle data failed, status: {}, req: {}";
  public static final String PIPE_AIR_GAP_RECEIVER_SOCKET_CLOSED_WHEN =
      "Pipe air gap receiver {}: Socket {} closed when listening to data. Because: {}";
  public static final String PIPE_AIR_GAP_RECEIVER_STARTED_SOCKET =
      "Pipe air gap receiver {} started. Socket: {}";
  public static final String PIPE_AIR_GAP_RECEIVER_TEMPORARY_UNAVAILABLE_RETRY =
      "Pipe air gap receiver {}: Temporary unavailable retry timed out, returning FAIL to sender.";
  public static final String PIPE_AIR_GAP_RECEIVER_TSSTATUS_IS_ENCOUNTERED =
      "Pipe air gap receiver {}: TSStatus {} is encountered at the air gap receiver, will ignore.";
  public static final String PIPE_DATA_TRANSPORT_ERROR = "Pipe data transport error, {}";
  public static final String PIPE_INSERTING_TABLET_TO_CASTING_TYPE_FROM =
      "Pipe：Inserting tablet to {}.{}. Casting type from {} to {}.";
  public static final String RECEIVERS_EXECUTOR_IS_CLOSED = "Receivers-{}' executor 已关闭.";
  public static final String RECEIVER_EXIT_SUCCESSFULLY = "Receiver-{} exit successfully.";
  public static final String RECEIVER_ID = "接收器 id = {}：{}";
  public static final String RECEIVER_ID_THE_NUMBER_OF_DEVICE_PATHS =
      "接收器 id = {}：The number of device paths is not equal to sub-status in statement {}: {}.";
  public static final String RECEIVER_ID_UNKNOWN_PIPEREQUESTTYPE_RESPONSE_STATUS =
      "接收器 id = {}：未知的 PipeRequestType, response status = {}。";
  public static final String RECEIVER_ID_UNSUPPORTED_STATEMENT_TYPE_FOR_REDIRECTION =
      "接收器 id = {}：不支持的 statement type {} for redirection。";
  public static final String RECEIVER_IS_READY = "Receiver-{} is ready";
  public static final String REGISTER_WITH_INTERVAL_IN_SECONDS_SUCCESSFULLY =
      "Register {} with interval in seconds {} successfully.";
  public static final String SOCKET_CLOSED_WHEN_EXECUTING_READTILLFULL =
      "Socket closed when executing readTillFull.";
  public static final String SOCKET_CLOSED_WHEN_EXECUTING_SKIPTILLENOUGH =
      "Socket closed when executing skipTillEnough.";
  public static final String START_LOAD_PIPEDATA_WITH_SERIALIZE_NUMBER_AND =
      "Start load pipeData with serialize number {} and type {},value={}";
  public static final String STORAGE_ENGINE_READONLY = "storage engine readonly";
  public static final String SYNC_START_AT_TO_IS_DONE = "Sync {} start at {} to {} is done.";
  public static final String TEMPORARY_UNAVAILABLE_EXCEPTION_ENCOUNTERED_AT_AIR_GAP =
      "Temporary unavailable exception encountered at air gap receiver, will retry locally.";
  public static final String THE_IOTCONSENSUSV2_REQUEST_VERSION_IS_DIFFERENT_FROM =
      "The iotConsensusV2 request version {} is different from the sender request version {}, "
          + "the receiver will be reset to the sender request version.";
  public static final String THE_START_INDEX_OF_DATA_SYNC_IS =
      "The start index {} of data sync is not valid. The file is not exist and start index "
          + "should equal to 0).";
  public static final String THE_START_INDEX_OF_DATA_SYNC_IS_1 =
      "The start index {} of data sync is not valid. The start index of the file should equal "
          + "to {}.";
  public static final String THRIFT_CONNECTION_IS_NOT_ALIVE = "Thrift 连接已断开。";
  public static final String TSFILECHECKER_DID_NOT_TERMINATE_WITHIN_S =
      "TsFileChecker did not terminate within {}s";
  public static final String TSFILECHECKER_THREAD_STILL_DOESN_T_EXIT_AFTER =
      "TsFileChecker Thread {} still doesn't exit after 30s";
  public static final String UNHANDLED_EXCEPTION_DURING_PIPE_AIR_GAP_RECEIVER =
      "Unhandled exception during pipe air gap receiver listening";
  public static final String UNSUPPORTED_DATA_TYPE = "不支持的 data type: ";

  // ===================== RESOURCE =====================

  public static final String CANNOT_GET_DATA_REGION_IDS_USE_DEFAULT =
      "无法获取 data region ids, use default lock segment size: {}";
  public static final String EXPAND_CALLBACK_IS_NOT_SUPPORTED_IN_PIPEFIXEDMEMORYBLOCK =
      "Expand callback is not supported in PipeFixedMemoryBlock";
  public static final String EXPAND_METHOD_IS_NOT_SUPPORTED_IN_PIPEFIXEDMEMORYBLOCK =
      "Expand method is not supported in PipeFixedMemoryBlock";
  public static final String FAILED_TO_CACHEDEVICEISALIGNEDMAPIFABSENT_FOR_TSFILE_BECAUSE_MEMORY =
      "cacheDeviceIsAlignedMapIfAbsent for tsfile {} 失败，原因：memory usage is high";
  public static final String FAILED_TO_CACHEOBJECTSIFABSENT_FOR_TSFILE_BECAUSE_MEMORY =
      "cacheObjectsIfAbsent for tsfile {} 失败，原因：memory usage is high";
  public static final String FAILED_TO_ESTIMATE_SIZE_FOR_INSERTNODE =
      "estimate size for InsertNode: {} 失败";
  public static final String FAILED_TO_EXECUTE_THE_EXPAND_CALLBACK =
      "execute the expand callback 失败。";
  public static final String FAILED_TO_EXECUTE_THE_SHRINK_CALLBACK =
      "execute the shrink callback 失败。";
  public static final String FAILED_TO_GET_FILE_SIZE_OF_LINKED =
      "获取 file size of linked TsFile {}:  失败";
  public static final String FORCEALLOCATEWITHRETRY_INTERRUPTED_WHILE_WAITING_FOR_AVAILABLE_MEMORY =
      "forceAllocateWithRetry：等待可用内存时被中断";
  public static final String FORCEALLOCATE_INTERRUPTED_WHILE_WAITING_FOR_AVAILABLE_MEMORY =
      "forceAllocate: interrupted while waiting for available memory";
  public static final String FORCERESIZE_CANNOT_RESIZE_A_NULL_OR_RELEASED =
      "forceResize: cannot resize a null or released memory block";
  public static final String FORCERESIZE_INTERRUPTED_WHILE_WAITING_FOR_AVAILABLE_MEMORY =
      "forceResize: interrupted while waiting for available memory";
  public static final String INTERRUPTED_WHILE_WAITING_FOR_THE_LOCK = "waiting for the lock 时被中断。";
  public static final String IS_RELEASED_AFTER_THREAD_INTERRUPTION =
      "{} is released after thread interruption.";
  public static final String PIPEPERIODICALLOGREDUCER_IS_ALLOCATED_TO_BYTES =
      "PipePeriodicalLogReducer is allocated to {} bytes.";
  public static final String PIPETSFILERESOURCE_CACHED_DEVICEISALIGNEDMAP_FOR_TSFILE =
      "PipeTsFileResource: Cached deviceIsAlignedMap for tsfile {}.";
  public static final String PIPETSFILERESOURCE_CACHED_OBJECTS_FOR_TSFILE =
      "PipeTsFileResource: Cached objects for tsfile {}.";
  public static final String PIPETSFILERESOURCE_CLOSED_TSFILE_AND_CLEANED_UP =
      "PipeTsFileResource: Closed tsfile {} and cleaned up.";
  public static final String PIPETSFILERESOURCE_FAILED_TO_CACHE_OBJECTS_FOR_TSFILE =
      "PipeTsFileResource: Failed to cache objects for tsfile {} in cache, because memory "
          + "usage is high";
  public static final String PIPETSFILERESOURCE_FAILED_TO_DELETE_TSFILE_WHEN_CLOSING =
      "PipeTsFileResource: Failed to delete tsfile {} when closing, because {}. Please "
          + "MANUALLY delete it.";
  public static final String PIPETSFILERESOURCE_S_REFERENCE_COUNT_IS_DECREASED_TO =
      "PipeTsFileResource's reference count is decreased to below 0.";
  public static final String PIPE_HARDLINK_DIR_FOUND_DELETING_IT_RESULT =
      "Pipe hardlink dir found, deleting it: {}, result: {}";
  public static final String PIPE_HARDLINK_DIR_FOUND_MOVED_TO_PERIODICAL_DELETE =
      "Pipe hardlink dir found, moved it from {} to {} for throttled periodical deletion.";
  public static final String PIPE_STALE_HARDLINK_DIR_FOUND_REGISTERING_PERIODICAL_DELETE =
      "Stale pipe hardlink dir found, registering it for throttled periodical deletion: {}";
  public static final String PIPE_HARDLINK_DIR_PERIODICAL_DELETE_FINISHED =
      "Finished deleting stale pipe hardlink dir {} by periodical job, result: {}";
  public static final String PIPE_HARDLINK_DIR_PERIODICAL_DELETE_PROGRESS =
      "Periodically deleted {} paths from stale pipe hardlink dirs, current dir: {}, current round result: {}";
  public static final String PIPE_HARDLINK_DIR_PERIODICAL_DELETE_ALL_FINISHED =
      "Finished deleting all stale pipe hardlink dirs by periodical job.";
  public static final String PIPE_HARDLINK_DIR_MOVE_FAILED_DELETING_SYNC =
      "Failed to move pipe hardlink dir {} for periodical deletion, deleting it synchronously.";
  public static final String PIPE_SNAPSHOT_DIR_FOUND_DELETING_IT =
      "Pipe snapshot dir found, deleting it: {},";
  public static final String SHRINK_CALLBACK_IS_NOT_SUPPORTED_IN_PIPEFIXEDMEMORYBLOCK =
      "Shrink callback is not supported in PipeFixedMemoryBlock";
  public static final String SHRINK_METHOD_IS_NOT_SUPPORTED_IN_PIPEFIXEDMEMORYBLOCK =
      "Shrink method is not supported in PipeFixedMemoryBlock";
  public static final String THE_MEMORY_BLOCK_HAS_BEEN_RELEASED = "内存块已被释放";
  public static final String THE_MULTIPLE_N_MUST_BE_GREATER_THAN =
      "The multiple n must be greater than 0";
  public static final String TRYALLOCATE_ALLOCATED_MEMORY_TOTAL_MEMORY_SIZE_BYTES =
      "tryAllocate: allocated memory, total memory size {} bytes, used memory size {} bytes, "
          + "original requested memory size {} bytes, actual requested memory size {} bytes";
  public static final String TRYALLOCATE_FAILED_TO_ALLOCATE_MEMORY_TOTAL_MEMORY =
      "tryAllocate: failed to allocate memory, total memory size {} bytes, used memory size {} "
          + "bytes, requested memory size {} bytes";
  public static final String TRYEXPANDALLANDCHECKCONSISTENCY_MEMORY_USAGE_IS_NOT_CONSISTENT_WITH =
      "tryExpandAllAndCheckConsistency: memory usage is not consistent with allocated blocks, "
          + "usedMemorySizeInBytes is {} but sum of all blocks is {}";
  public static final String TRYEXPANDALLANDCHECKCONSISTENCY_MEMORY_USAGE_OF_TABLETS_IS_NOT =
      "tryExpandAllAndCheckConsistency: memory usage of tablets is not consistent with "
          + "allocated blocks, usedMemorySizeInBytesOfTablets is {} but sum of all tablet blocks is "
          + "{}";
  public static final String TRYEXPANDALLANDCHECKCONSISTENCY_MEMORY_USAGE_OF_TSFILES_IS_NOT =
      "tryExpandAllAndCheckConsistency: memory usage of tsfiles is not consistent with "
          + "allocated blocks, usedMemorySizeInBytesOfTsFiles is {} but sum of all tsfile blocks is "
          + "{}";

  // ===================== METRIC =====================

  public static final String FAILED_TO_DEREGISTER_PIPE_ASSIGNER_METRICS_PIPEDATAREGIONASSIGNER =
      "注销 pipe assigner metrics, PipeDataRegionAssigner({}) does not exist 失败";
  public static final String FAILED_TO_DEREGISTER_PIPE_DATA_REGION_EXTRACTOR =
      "注销 pipe data region extractor metrics, IoTDBDataRegionExtractor({}) does not exist 失败";
  public static final String FAILED_TO_DEREGISTER_PIPE_DATA_REGION_SINK =
      "注销 pipe data region sink metrics, PipeSinkSubtask({}) does not exist 失败";
  public static final String FAILED_TO_DEREGISTER_PIPE_REMAINING_EVENT_AND =
      "注销 pipe remaining event and time metrics, RemainingEventAndTimeOperator({}) does not "
          + "exist 失败";
  public static final String FAILED_TO_DEREGISTER_PIPE_SCHEMA_REGION_CONNECTOR =
      "注销 pipe schema region connector metrics, PipeConnectorSubtask({}) does not exist 失败";
  public static final String FAILED_TO_DEREGISTER_PIPE_SCHEMA_REGION_SOURCE =
      "注销 pipe schema region source metrics, IoTDBSchemaRegionSource({}) does not exist 失败";
  public static final String SKIP_DEREGISTER_PIPE_TSFILE_TO_TABLETS =
      "跳过注销 pipe tsfile to tablets metrics，因为 pipeID({}) 未注册";
  public static final String FAILED_TO_DEREGISTER_SCHEMA_REGION_LISTENER_METRICS =
      "注销 schema region listener metrics, SchemaRegionListeningQueue({}) does not exist 失败";
  public static final String FAILED_TO_MARK_PIPE_DATA_REGION_EXTRACTOR =
      "mark pipe data region extractor heartbeat event, IoTDBDataRegionExtractor({}) does not "
          + "exist 失败";
  public static final String FAILED_TO_MARK_PIPE_DATA_REGION_EXTRACTOR_1 =
      "mark pipe data region extractor tablet event, IoTDBDataRegionExtractor({}) does not "
          + "exist 失败";
  public static final String FAILED_TO_MARK_PIPE_DATA_REGION_EXTRACTOR_2 =
      "mark pipe data region extractor tsfile event, IoTDBDataRegionExtractor({}) does not "
          + "exist 失败";
  public static final String FAILED_TO_MARK_PIPE_DATA_REGION_SINK =
      "mark pipe data region sink tablet event, PipeSinkSubtask({}) does not exist 失败";
  public static final String FAILED_TO_MARK_PIPE_DATA_REGION_SINK_1 =
      "mark pipe data region sink tsfile event, PipeSinkSubtask({}) does not exist 失败";
  public static final String FAILED_TO_MARK_PIPE_PROCESSOR_HEARTBEAT_EVENT =
      "mark pipe processor heartbeat event, PipeProcessorSubtask({}) does not exist 失败";
  public static final String FAILED_TO_MARK_PIPE_PROCESSOR_TABLET_EVENT =
      "mark pipe processor tablet event, PipeProcessorSubtask({}) does not exist 失败";
  public static final String FAILED_TO_MARK_PIPE_PROCESSOR_TSFILE_EVENT =
      "mark pipe processor tsfile event, PipeProcessorSubtask({}) does not exist 失败";
  public static final String FAILED_TO_MARK_PIPE_REGION_COMMIT_REMAININGEVENTANDTIMEOPERATOR =
      "mark pipe region commit, RemainingEventAndTimeOperator({}) does not exist 失败";
  public static final String FAILED_TO_MARK_PIPE_SCHEMA_REGION_WRITE =
      "mark pipe schema region write plan event, PipeConnectorSubtask({}) does not exist 失败";
  public static final String FAILED_TO_MARK_PIPE_TSFILE_TO_TABLETS =
      "mark pipe tsfile to tablets invocation, pipeID({}) does not exist 失败";
  public static final String FAILED_TO_RECORD_PIPE_TSFILE_TO_TABLETS =
      "记录 pipe tsfile to tablets time, pipeID({}) does not exist 失败";
  public static final String FAILED_TO_RECORD_TABLET_GENERATED_PIPEID_DOES =
      "记录 tablet generated, pipeID({}) does not exist 失败";
  public static final String FAILED_TO_SET_RECENT_PROCESSED_TSFILE_EPOCH =
      "设置 recent processed tsfile epoch state, PipeRealtimeDataRegionExtractor({}) does not "
          + "exist 失败";
  public static final String FAILED_TO_UNBIND_FROM_PIPE_ASSIGNER_METRICS =
      "解绑 from pipe assigner metrics, assigner map not empty 失败";
  public static final String FAILED_TO_UNBIND_FROM_PIPE_DATA_REGION =
      "解绑 from pipe data region sink metrics, sink map not empty 失败";
  public static final String FAILED_TO_UNBIND_FROM_PIPE_EXTRACTOR_METRICS =
      "解绑 from pipe extractor metrics, extractor map not empty 失败";
  public static final String FAILED_TO_UNBIND_FROM_PIPE_PROCESSOR_METRICS =
      "解绑 from pipe processor metrics, processor map not empty 失败";
  public static final String FAILED_TO_UNBIND_FROM_PIPE_REMAINING_EVENT =
      "解绑 from pipe remaining event and time metrics, RemainingEventAndTimeOperator map not "
          + "empty 失败";
  public static final String FAILED_TO_UNBIND_FROM_PIPE_SCHEMA_REGION =
      "解绑 from pipe schema region connector metrics, connector map not empty 失败";
  public static final String FAILED_TO_UNBIND_FROM_PIPE_SCHEMA_REGION_1 =
      "解绑 from pipe schema region extractor metrics, extractor map not empty 失败";
  public static final String FAILED_TO_UNBIND_FROM_PIPE_SCHEMA_REGION_2 =
      "解绑 from pipe schema region listener metrics, listening queue map not empty 失败";
  public static final String FAILED_TO_UNBIND_FROM_PIPE_TSFILE_TO =
      "解绑 from pipe tsfile to tablets metrics, pipe map is not empty, pipe: {} 失败";

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
  // pipe - WriteBackSink
  // ---------------------------------------------------------------------------
  public static final String TABLE_MODEL_DATABASE_INVALID_FMT =
      "表模型数据库 %s 非法：不应包含 '%s'，应匹配 %s，且长度不应超过 %d";
  public static final String TREE_MODEL_DATABASE_INVALID_FMT =
      "树模型数据库 %s 非法：应为合法的树模型数据库路径，应匹配 %s，且长度不应超过 %d";
  public static final String TARGET_TREE_MODEL_DATABASE_CANNOT_BE_USED_FOR_TABLE_MODEL_EVENTS_FMT =
      "目标树模型数据库 %s 不能用于表模型事件，因为对应的表模型数据库 %s 非法。";
  public static final String FAILED_TO_REWRITE_TREE_MODEL_DATABASE_FMT =
      "将树模型数据库从 %s 重写为 %s 失败，设备为 %s。";

  // ---------------------------------------------------------------------------
  // pipe – PipeTransferTrackableHandler
  // ---------------------------------------------------------------------------
  public static final String TPIPE_TRANSFER_RESP_IS_NULL_WHEN_TRANSFERRING_SLICE =
      "传输分片时 TPipeTransferResp 为空。";

  private DataNodePipeMessages() {}
}
