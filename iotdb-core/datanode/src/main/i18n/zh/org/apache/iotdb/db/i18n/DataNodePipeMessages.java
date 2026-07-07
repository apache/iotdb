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
      "DeletionManager-{}：当前等待被中断，可能因为当前应用已停止。";
  public static final String DELETIONMANAGER_DELETE_DELETION_FILE_IN_DIR =
      "DeletionManager-{}：删除 {} 目录中的 deletion file...";
  public static final String DELETIONMANAGER_FAILED_TO_DELETE_FILE_IN_DIR =
      "DeletionManager-{} 删除 file in {} dir, please manually check! 失败";
  public static final String DELETIONRESOURCE_HAS_BEEN_RELEASED_TRIGGER_A_REMOVE =
      "DeletionResource {} 已释放，触发移除 DAL...";
  public static final String DELETION_PERSIST_CANNOT_CREATE_FILE_PLEASE_CHECK =
      "Deletion persist：无法创建文件 {}，请手动检查文件系统。";
  public static final String DELETION_PERSIST_CANNOT_WRITE_TO_MAY_CAUSE =
      "Deletion persist：无法写入 {}，可能导致数据不一致。";
  public static final String DELETION_PERSIST_CURRENT_BATCH_FSYNC_DUE_TO =
      "Deletion persist-{}：当前批次因超时执行 fsync";
  public static final String DELETION_PERSIST_CURRENT_FILE_HAS_BEEN_CLOSED =
      "Deletion persist-{}：current file 已关闭";
  public static final String DELETION_PERSIST_SERIALIZE_DELETION_RESOURCE =
      "Deletion persist-{}：序列化 deletion resource {}";
  public static final String DELETION_PERSIST_STARTING_TO_PERSIST_CURRENT_WRITING =
      "Deletion persist-{}：开始持久化，当前写入：{}";
  public static final String DELETION_PERSIST_SWITCHING_TO_A_NEW_FILE =
      "Deletion persist-{}：切换到新文件，当前写入：{}";
  public static final String DELETION_RESOURCE_MANAGER_FOR_HAS_BEEN_SUCCESSFULLY =
      "{} 的删除资源管理器已成功关闭！";
  public static final String DETECT_FILE_CORRUPTED_WHEN_RECOVER_DAL_DISCARD =
      "恢复 DAL-{} 时检测到文件损坏，丢弃后续所有 DAL...";
  public static final String FAILED_TO_INITIALIZE_DELETIONRESOURCEMANAGER =
      "初始化 DeletionResourceManager 失败";
  public static final String FAILED_TO_READ_DELETION_FILE_MAY_BECAUSE =
      "读取 deletion file {} 失败，可能因为写入时文件已损坏。";
  public static final String FAILED_TO_RECOVER_DELETIONRESOURCEMANAGER =
      "恢复 DeletionResourceManager 失败";
  public static final String FAIL_TO_ALLOCATE_DELETIONBUFFER_GROUP_S_BUFFER =
      "分配 deletionBuffer-group-{} 的 buffer 失败，原因：内存不足。";
  public static final String FAIL_TO_CLOSE_CURRENT_LOGGING_FILE_WHEN = "关闭时无法关闭当前日志文件";
  public static final String FAIL_TO_REGISTER_DELETIONRESOURCE_INTO_DELETIONBUFFER_BECAUSE =
      "将 DeletionResource 注册到 deletionBuffer-{} 失败，原因：该 buffer 已关闭。";
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
      "无法从文件 {} 解析 reboot times，将当前时间（秒，{}）作为 reboot times";
  public static final String CANNOT_RECORD_REBOOT_TIMES_TO_FILE_THE =
      "无法将 reboot times {} 记录到文件 {}，reboot times 将不会被更新";
  public static final String CANNOT_START_SIMPLEPROGRESSINDEXASSIGNER_BECAUSE_OF =
      "无法启动 SimpleProgressIndexAssigner，原因：{}";
  public static final String CREATE_PIPE_DN_TASK_SUCCESSFULLY_WITHIN_MS =
      "创建 pipe DN task {} 成功，耗时 {} ms";
  public static final String DEREGISTER_SUBTASK_RUNNINGTASKCOUNT_REGISTEREDTASKCOUNT =
      "注销子任务 {}。runningTaskCount: {}, registeredTaskCount: {}";
  public static final String DROP_PIPE_DN_TASK_SUCCESSFULLY_WITHIN_MS =
      "删除 pipe DN task {} 成功，耗时 {} ms";
  public static final String ERROR_OCCURRED_WHEN_COLLECTING_EVENTS_FROM_PROCESSOR =
      "从 processor 收集事件时发生错误";
  public static final String EXCEPTION_IN_PIPE_EVENT_PROCESSING_IGNORED_BECAUSE =
      "pipe event processing 中发生异常，因为 pipe 已被删除，忽略该异常。{}";
  public static final String EXCEPTION_OCCURRED_WHEN_CLOSING_PIPE_CONNECTOR_SUBTASK =
      "关闭 pipe connector 子任务 {} 时发生异常，根因：{}";
  public static final String EXCEPTION_OCCURRED_WHEN_CLOSING_PIPE_PROCESSOR_SUBTASK =
      "关闭 pipe processor 子任务 {} 时发生异常，根因：{}";
  public static final String EXCEPTION_OCCURS_WHEN_EXECUTING_PIPE_TASK =
      "执行 pipe task 时发生异常：";
  public static final String FAILED_TO_CHECK_IF_PIPE_HAS_RELEASE =
      "check if pipe has release region related resource with consensus group id: {} 失败。";
  public static final String FAILED_TO_CLEAR_CLOSE_THE_SCHEMA_REGION =
      "清理/关闭 schema region 监听队列失败，原因：{}。将一直等待直到成功或该 region 的状态机停止。";
  public static final String FAILED_TO_CLOSE_CONNECTOR_AFTER_FAILED_TO =
      "初始化 connector 失败后关闭 connector 失败。忽略该异常。";
  public static final String FAILED_TO_CLOSE_LISTENING_QUEUE_FOR_SCHEMAREGION =
      "关闭 SchemaRegion 的监听队列失败";
  public static final String FAILED_TO_CLOSE_SOURCE_AFTER_FAILED_TO =
      "初始化 source 失败后关闭 source 失败。忽略该异常。";
  public static final String FAILED_TO_CONSTRUCT_PIPECONNECTOR_BECAUSE_OF =
      "构造 PipeConnector 失败，原因：";
  public static final String FAILED_TO_DECREASE_REFERENCE_COUNT_FOR_EVENT =
      "减少 reference count for event {} in PipeRealtimePriorityBlockingQueue 失败";
  public static final String FAILED_TO_GET_PENDINGQUEUE_NO_SUCH_SUBTASK =
      "获取 PendingQueue 失败，没有该子任务:  ";
  public static final String FAILED_TO_GET_PIPE_INFO_FROM_CONFIG_NODE_STATUS =
      "从 CN 处获取 pipe 信息失败，状态是 %s。";
  public static final String FAILED_TO_GET_PIPE_METAS_WILL_BE =
      "获取 pipe metas 失败，稍后会从 CN 处再次同步。";
  public static final String FAILED_TO_GET_PIPE_PLUGIN_JAR_FROM =
      "从 CN 处获取 pipe 插件 jar 包失败。";
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
  public static final String EXCEPTION_PIPECONNECTOR_ARG_ID_ARG_HEARTBEAT_FAILED_OR_ENCOUNTERED_FAILURE_WHEN_TRANSFERRING_GENERIC_EVENT_FAILURE_ARG_679A4A49 =
      "PipeConnector：%s(id: %s) 心跳失败，或在传输 generic event 时遇到失败。失败原因：%s";
  public static final String EXCEPTION_THE_DATABASE_NAME_IN_TREE_MODEL_MUST_START_WITH_ROOT_7BFA4609 =
      "tree 模型中的数据库名称必须以 'root.' 开头。";
  public static final String EXCEPTION_THE_LENGTH_OF_DATABASE_NAME_SHALL_NOT_EXCEED_82C7199C =
      "数据库名称的长度不得超过 ";
  public static final String PIPEDATANODETASKBUILDER_FAILED_TO_PARSE_INCLUSION_AND_EXCLUSION =
      "PipeDataNodeTaskBuilder 解析 'inclusion' 和 'exclusion' 参数失败：{}";
  public static final String PIPEDATANODETASKBUILDER_WHEN_INCLUSION_CONTAINS_DATA_DELETE_REALTIME =
      "PipeDataNodeTaskBuilder：当 'inclusion' 包含 'data.delete' 时，'realtime-first' 默认为 "
          + "'false'，以避免删除后出现同步问题。";
  public static final String PIPEDATANODETASKBUILDER_WHEN_INCLUSION_INCLUDES_DATA_DELETE_REALTIME =
      "PipeDataNodeTaskBuilder：当 'inclusion' 包含 'data.delete' 时，将 'realtime-first' 设置为 "
          + "'true' 可能导致删除后出现数据同步问题。";
  public static final String PIPEDATANODETASKBUILDER_WHEN_SOURCE_USES_SNAPSHOT_MODEL_REALTIME =
      "PipeDataNodeTaskBuilder：当 source 使用快照模式时，'realtime-first' 默认为 'false'，"
          + "以避免在传输完成前过早停止。";
  public static final String PIPEDATANODETASKBUILDER_WHEN_SOURCE_USES_SNAPSHOT_MODEL_REALTIME_1 =
      "PipeDataNodeTaskBuilder：当 source 使用快照模式时，将 'realtime-first' 设置为 'true' "
          + "可能导致在传输完成前过早停止。";
  public static final String PIPEDATANODETASKBUILDER_WHEN_THE_REALTIME_SYNC_IS_ENABLED =
      "PipeDataNodeTaskBuilder：当启用实时同步时，不在发送 tsfile 时启用 rate limiter 可能"
          + "会给实时发送带来延迟。";
  public static final String PIPEDATANODETASKBUILDER_WHEN_THE_REALTIME_SYNC_IS_ENABLED_1 =
      "PipeDataNodeTaskBuilder：当启用实时同步时，默认在发送 tsfile 时启用 rate limiter，"
          + "以为实时发送保留磁盘和网络 IO。";
  public static final String PIPEEVENTCOLLECTOR_THE_EVENT_IS_ALREADY_RELEASED_SKIPPING =
      "PipeEventCollector：事件 {} 已被释放，跳过处理。";
  public static final String PIPE_CONNECTOR_SUBTASK_WAS_CLOSED_WITHIN_MS =
      "Pipe：connector 子任务 {} ({}) 在 {} ms 内已关闭";
  public static final String PIPE_META_NOT_FOUND = "未找到 pipe 元数据：";
  public static final String PIPE_SINK_SUBTASKS_WITH_ATTRIBUTES_IS_BOUNDED =
      "带属性 {} 的 Pipe sink 子任务绑定到 sinkExecutor {} 和 callbackExecutor {}。";
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
      "Region {}：没有可替换 source 文件 {} 的 TsFileInsertionEvent";
  public static final String REGION_REPLACED_TSFILEINSERTIONEVENTS_WITH =
      "Region {}：将 TsFileInsertionEvent {} 替换为 {}";
  public static final String REGISTEREDTASKCOUNT_0 = "registeredTaskCount 小于 0";
  public static final String REGISTEREDTASKCOUNT_0_1 = "registeredTaskCount 小于等于 0";
  public static final String REGISTER_SUBTASK_RUNNINGTASKCOUNT_REGISTEREDTASKCOUNT =
      "注册子任务 {}。runningTaskCount: {}, registeredTaskCount: {}";
  public static final String REPORT_PIPERUNTIMEEXCEPTION_TO_LOCAL_PIPETASKMETA_EXCEPTION_MESSAGE =
      "向本地 PipeTaskMeta({}) 上报 PipeRuntimeException，异常信息：{}";
  public static final String RUNNINGTASKCOUNT_0 = "runningTaskCount 小于 0";
  public static final String RUNNINGTASKCOUNT_0_1 = "runningTaskCount 小于等于 0";
  public static final String SIMPLEPROGRESSINDEXASSIGNER_STARTED_SUCCESSFULLY_ISSIMPLECONSENSUSENABLE_R =
      "SimpleProgressIndexAssigner 启动成功。isSimpleConsensusEnable：{}，"
          + "rebootTimes：{}";
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
      "subtask {} 已关闭，忽略异常";
  public static final String SUBTASK_WORKER_IS_INTERRUPTED = "子任务工作线程被中断";
  public static final String SUCCESSFULLY_PERSISTED_ALL_PIPE_S_INFO_TO =
      "成功将所有 Pipe 信息持久化到 ConfigNode。";
  public static final String THE_EXECUTOR_AND_HAS_BEEN_SUCCESSFULLY_SHUTDOWN =
      "执行器 {} 和 {} 已成功关闭。";

  // ===================== EVENT =====================

  public static final String DATABASENAMEFROMDATAREGION_IS_NULL = "databaseNameFromDataRegion 为空";
  public static final String DECREASE_REFERENCE_COUNT_ERROR = "减少引用计数出错。";
  public static final String DECREASE_REFERENCE_COUNT_FOR_MTREE_SNAPSHOT_OR =
      "减少 mTree snapshot {} 或 tLog {} 或 attribute snapshot {} 的引用计数出错。";
  public static final String DECREASE_REFERENCE_COUNT_FOR_TSFILE_ERROR =
      "Decrease reference count for TsFile {} 出错。";
  public static final String DO_NOT_HAS_A_COMPLETE_PAGE_BODY =
      "没有完整的 page body。期望：";
  public static final String ERROR_WHILE_PARSING_TSFILE_INSERTION_EVENT =
      "解析 tsfile insertion event 时出错";
  public static final String EXCEPTION_OCCURRED_WHEN_DETERMINING_THE_EVENT_TIME =
      "判断 PipeInsertNodeTabletInsertionEvent({}) 的事件时间是否与时间范围 [{}, {}] 重叠时"
          + "发生异常。为保证数据完整性返回 true。";
  public static final String FAILED_TO_ALLOCATE_MEMORY_FOR_PARSING_TSFILE =
      "{}：为解析 TsFile {} 分配内存失败，tablet 事件编号 {}，"
          + "将释放解析器内存并稍后重试该 TsFile 事件。";
  public static final String FAILED_TO_CONSUME_PARSED_TABLET_FROM_TSFILE_KEEP_PARSER =
      "{}：消费 TsFile {} 解析出的 tablet 失败，tablet 事件编号 {}，重试次数 {}，"
          + "将暂时保留解析器并在本地短暂重试。";
  public static final String FAILED_TO_BUILD_TABLET = "构建 tablet 失败";
  public static final String FAILED_TO_CHECK_NEXT = "check next 失败";
  public static final String FAILED_TO_CLOSE_TSFILEREADER = "关闭 TsFileReader 失败";
  public static final String FAILED_TO_CLOSE_TSFILESEQUENCEREADER = "关闭 TsFileSequenceReader 失败";
  public static final String FAILED_TO_CREATE_TSFILEINSERTIONDATATABLETITERATOR =
      "创建 TsFileInsertionDataTabletIterator 失败";
  public static final String FAILED_TO_GET_NEXT_TABLET_INSERTION_EVENT =
      "获取 next tablet insertion event 失败。";
  public static final String FAILED_TO_LOAD_MODIFICATIONS_FROM_TSFILE =
      "从 TsFile 加载 modifications 失败：";
  public static final String FAILED_TO_READ_METADATA_FOR_DEVICEID_MEASUREMENT =
      "读取 deviceId：{}、measurement：{} 的 metadata 失败，移除该条目";
  public static final String FAILED_TO_RECORD_PARSE_END_TIME_FOR =
      "记录 parse end time for pipe {} 失败";
  public static final String FAILED_TO_RECORD_TABLET_METRICS_FOR_PIPE =
      "记录 tablet metrics for pipe {} 失败";
  public static final String FOUND_NULL_DEVICEID_REMOVING_ENTRY =
      "发现 deviceId 为 null，移除该条目";
  public static final String INITIALIZE_DATA_CONTAINER_ERROR = "初始化数据容器出错。";
  public static final String INSERTNODE_HAS_BEEN_RELEASED = "InsertNode 已被释放";
  public static final String INSERTROWNODE_IS_PARSED_TO_ZERO_ROWS_ACCORDING =
      "InsertRowNode({}) 根据 pattern({}) 和时间范围 [{}, {}] 解析后行数为 0，"
          + "对应的 source event({}) 将被忽略。";
  public static final String INSERTTABLETNODE_IS_PARSED_TO_ZERO_ROWS_ACCORDING =
      "InsertTabletNode({}) 根据 pattern({}) 和时间范围 [{}, {}] 解析后行数为 0，"
          + "对应的 source event({}) 将被忽略。";
  public static final String INVALID_EVENT_TYPE = "无效的 event type: ";
  public static final String INVALID_INPUT = "无效的 input: ";
  public static final String ISGENERATEDBYPIPE_IS_NOT_SUPPORTED =
      "isGeneratedByPipe() 不支持！";
  public static final String MAYEVENTPATHSOVERLAPPEDWITHPATTERN_IS_NOT_SUPPORTED =
      "mayEventPathsOverlappedWithPattern() 不支持！";
  public static final String MAYEVENTTIMEOVERLAPPEDWITHTIMERANGE_IS_NOT_SUPPORTED =
      "mayEventTimeOverlappedWithTimeRange() 不支持！";
  public static final String NO_COMMIT_IDS_FOUND_IN_PIPECOMPACTEDTSFILEINSERTIONEVENT =
      "在 PipeCompactedTsFileInsertionEvent 中未找到 commit ID。";
  public static final String PIPECOMPACTEDTSFILEINSERTIONEVENT_DOES_NOT_SUPPORT_EQUALSINIOTCONSENSUSV2 =
      "PipeCompactedTsFileInsertionEvent 不支持 equalsInIoTConsensusV2.";
  public static final String PIPECOMPACTEDTSFILEINSERTIONEVENT_DOES_NOT_SUPPORT_GETREBOOTTIMES =
      "PipeCompactedTsFileInsertionEvent 不支持 getRebootTimes.";
  public static final String PIPE_FAILED_TO_GET_DEVICES_FROM_TSFILE =
      "Pipe {}：获取 devices from TsFile {}, extract it anyway 失败";
  public static final String PIPE_SKIPPING_TEMPORARY_TSFILE_S_PARSING_WHICH =
      "Pipe 跳过不应传输的临时 TsFile 的解析：{}";
  public static final String ROW_CAN_NOT_BE_CUSTOMIZED = "Row 无法自定义";
  public static final String SHALLOWCOPYSELFANDBINDPIPETASKMETAFORPROGRESSREPORT_IS_NOT_SUPPORTED =
      "shallowCopySelfAndBindPipeTaskMetaForProgressReport() 不支持！";
  public static final String SKIPPING_TEMPORARY_TSFILE_S_PROGRESSINDEX_WILL_REPORT =
      "跳过 temporary TsFile {}'s progressIndex, will report MinimumProgressIndex";
  public static final String TABLEPATTERNPARSER_DOES_NOT_SUPPORT_ROW_BY_ROW =
      "TablePatternParser 不支持 row by row processing";
  public static final String TABLEPATTERNPARSER_DOES_NOT_SUPPORT_TABLET_PROCESSING =
      "TablePatternParser 不支持 tablet processing";
  public static final String TABLEPATTERNPARSER_DOES_NOT_SUPPORT_TABLET_PROCESSING_WITH =
      "TablePatternParser 不支持 tablet processing with collect";
  public static final String TABLET_IS_PARSED_TO_ZERO_ROWS_ACCORDING =
      "Tablet({}) 根据 pattern({}) 和时间范围 [{}, {}] 解析后行数为 0，"
          + "对应的 source event({}) 将被忽略。";
  public static final String TABLE_MODEL_TSFILE_PARSING_DOES_NOT_SUPPORT =
      "Table model tsfile 解析不支持该类型的 ChunkMeta";
  public static final String TEMPORARY_TSFILE_DETECTED_WILL_SKIP_ITS_TRANSFER =
      "检测到临时 tsFile {}，将跳过其传输。";
  public static final String TSFILE_HAS_INITIALIZED_PIPENAME_CREATION_TIME_PATTERN =
      "TsFile {} 已初始化 {}，pipeName：{}，创建时间：{}，pattern：{}，startTime："
          + "{}, endTime：{}, withMod：{}";
  public static final String UNCOMPRESS_ERROR_UNCOMPRESS_SIZE =
      "解压出错！解压后大小：";
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
      "ChangingValueSamplingProcessor 在 {} 中初始化，{}：{}, {}：{}, {}：{}。";
  public static final String CLEAN_OUTDATED_INCOMPLETE_COMBINER_PIPENAME_CREATIONTIME_COMBINEID =
      "清理过期的未完成 combiner：pipeName={}, creationTime={}, combineId={}";
  public static final String COMBINEHANDLER_NOT_FOUND_FOR_PIPEID =
      "未找到 pipeId = 对应的 CombineHandler";
  public static final String COMBINER_COMBINE_COMPLETED_REGIONID_STATE_RECEIVEDREGIONIDSET_EX =
      "Combiner 合并完成：regionId：{}, state：{}, receivedRegionIdSet：{}, "
          + "expectedRegionIdSet：{}";
  public static final String COMBINER_COMBINE_REGIONID_STATE_RECEIVEDREGIONIDSET_EXPECTEDREGI =
      "Combiner 合并：regionId：{}, state：{}, receivedRegionIdSet：{}, expectedRegionIdSet：{}";
  public static final String DATA_NODES_ENDPOINTS_FOR_TWO_STAGE_AGGREGATION =
      "用于两阶段聚合的 DataNode endpoints：{}";
  public static final String DIFFERENT_DATA_TYPE_ENCOUNTERED_IN_ONE_WINDOW =
      "一个窗口内出现不同的数据类型，将清理。前一个类型：{}，当前类型：{}";
  public static final String ENCOUNTERED_EXCEPTION_WHEN_DESERIALIZING_FROM_PIPETASKMETA =
      "从 PipeTaskMeta 反序列化时遇到异常";
  public static final String END_POINTS_FOR_TWO_STAGE_AGGREGATION_PIPE =
      "两阶段聚合 pipe（pipeName={}, creationTime={}）的 endpoints 已更新为 {}";
  public static final String ERROR_OCCURRED_WHEN_CLOSING_COMBINEHANDLER_ID =
      "关闭 CombineHandler（id = {}）时发生错误";
  public static final String ERROR_OCCURS_WHEN_RECEIVING_REQUEST = "接收请求时发生错误：{}。";
  public static final String LOGIN_FAILED_OR_SESSION_TIMED_OUT = "登录失败。您未被授权或会话已超时。";
  public static final String FAILED_TO_CLOSE_IOTDBSYNCCLIENT = "关闭 IoTDBSyncClient 失败";
  public static final String FAILED_TO_CLOSE_OLD_IOTDBSYNCCLIENT = "关闭 old IoTDBSyncClient 失败";
  public static final String FAILED_TO_COMBINE_COUNT = "合并 count 失败：";
  public static final String FAILED_TO_CONSTRUCT_IOTDBSYNCCLIENT = "构造 IoTDBSyncClient 失败";
  public static final String FAILED_TO_FETCH_COMBINE_RESULT = "获取合并结果失败：";
  public static final String FAILED_TO_FETCH_DATA_NODES = "获取 data node 失败";
  public static final String FAILED_TO_FETCH_DATA_REGION_IDS = "获取 data region id 失败";
  public static final String FAILED_TO_RECONSTRUCT_IOTDBSYNCCLIENT_AFTER_FAILURE_TO =
      "发送 request {}（watermark = {}）失败后重构 IoTDBSyncClient {} 失败";
  public static final String FAILED_TO_SEND_REQUEST_WATERMARK_TO =
      "发送 request {}（watermark = {}）到 {} 失败";
  public static final String FAILED_TO_TRIGGER_COMBINE_WATERMARK_COUNT_PROGRESSINDEX =
      "触发合并失败。watermark={}, count={}, progressIndex={}";
  public static final String FAILURE_OCCURRED_WHEN_TRYING_TO_COMMIT_PROGRESS =
      "尝试提交进度索引时发生失败。timestamp={}, count={}, "
          + "progressIndex={}";
  public static final String FETCHED_DATA_REGION_IDS_AT = "在 {} 获取到 data region id {}";
  public static final String FRACTIONPOWEREDSUMOPERATOR_DOES_NOT_SUPPORT_BINARY_INPUT =
      "FractionPoweredSumOperator 不支持 binary input";
  public static final String FRACTIONPOWEREDSUMOPERATOR_DOES_NOT_SUPPORT_BOOLEAN_INPUT =
      "FractionPoweredSumOperator 不支持 boolean input";
  public static final String FRACTIONPOWEREDSUMOPERATOR_DOES_NOT_SUPPORT_DATE_INPUT =
      "FractionPoweredSumOperator 不支持 date input";
  public static final String FRACTIONPOWEREDSUMOPERATOR_DOES_NOT_SUPPORT_STRING_INPUT =
      "FractionPoweredSumOperator 不支持 string input";
  public static final String GLOBAL_COUNT_IS_LESS_THAN_THE_LAST =
      "Global count 小于上次收集的 count：timestamp={}, count={}";
  public static final String IGNORED_TABLETINSERTIONEVENT_IS_NOT_AN_INSTANCE_OF =
      "已忽略 TabletInsertionEvent is not an instance of PipeInsertNodeTabletInsertionEvent or "
          + "PipeRawTabletInsertionEvent: {}";
  public static final String IGNORED_TSFILEINSERTIONEVENT_IS_EMPTY =
      "已忽略空的 TsFileInsertionEvent：{}";
  public static final String IGNORED_TSFILEINSERTIONEVENT_IS_NOT_AN_INSTANCE_OF =
      "已忽略 TsFileInsertionEvent is not an instance of PipeTsFileInsertionEvent: {}";
  public static final String ILLEGAL_OUTPUT_SERIES_PATH = "非法的 output series path: ";
  public static final String NO_DATA_NODES_ENDPOINTS_FETCHED = "未获取到任何 data node 的 endpoint";
  public static final String NO_EXPECTED_REGION_ID_SET_FETCHED =
      "未获取到预期的 region id 集合";
  public static final String PARTIALPATHLASTOBJECTCACHE_ALLOCATEDMEMORYBLOCK_HAS_EXPANDED_FROM_TO =
      "PartialPathLastObjectCache.allocatedMemoryBlock 已从 {} 扩展到 {}。";
  public static final String PARTIALPATHLASTOBJECTCACHE_ALLOCATEDMEMORYBLOCK_HAS_SHRUNK_FROM_TO =
      "PartialPathLastObjectCache.allocatedMemoryBlock 已从 {} 缩小到 {}。";
  public static final String SENDING_REQUEST_WATERMARK_TO = "正在发送 request {}（watermark = {}）到 {}";
  public static final String SWINGINGDOORTRENDINGSAMPLINGPROCESSOR_IN_IS_INITIALIZED_WITH =
      "SwingingDoorTrendingSamplingProcessor 在 {} 中初始化，{}：{}, {}：{}, {}：{}。";
  public static final String THE_ABSTRACT_FORMAL_PROCESSOR_DOES_NOT_SUPPORT = "抽象形式处理器不支持处理事件";
  public static final String TUMBLINGTIMESAMPLINGPROCESSOR_IN_IS_INITIALIZED_WITH_S =
      "TumblingTimeSamplingProcessor 在 {} 中初始化，{}：{}s, {}：{}, {}：{}。";
  public static final String TWOSTAGECOUNTPROCESSOR_CUSTOMIZED_BY_THREAD_PIPENAME_CREATIONTIME_RE =
      "由线程 {} 自定义的 TwoStageCountProcessor：pipeName={}, creationTime={}, "
          + "regionId={}, outputSeries={}, localCommitProgressIndex={}, localCount={}";
  public static final String TWO_STAGE_AGGREGATE_PIPE_PIPENAME_CREATIONTIME_RELATED =
      "两阶段聚合 pipe（pipeName={}, creationTime={}）相关的 region id {}";
  public static final String TWO_STAGE_AGGREGATE_RECEIVER_IS_EXITING =
      "两阶段聚合 receiver 正在退出。";
  public static final String TWO_STAGE_COMBINE_REGION_ID_COMBINE_ID =
      "两阶段合并（region id = {}, combine id = {}）未完成：timestamp={}, count={}, "
          + "progressIndex={}";
  public static final String TWO_STAGE_COMBINE_REGION_ID_COMBINE_ID_1 =
      "两阶段合并（region id = {}, combine id = {}）已过期：timestamp={}, count={}, "
          + "progressIndex={}";
  public static final String TWO_STAGE_COMBINE_REGION_ID_COMBINE_ID_2 =
      "两阶段合并（region id = {}, combine id = {}）成功：timestamp={}, count={}, "
          + "progressIndex={}, committed progressIndex={}";
  public static final String UNEXPECTED_STATE_CLASS = "非预期的 state class：";
  public static final String UNKNOWN_COMBINE_RESULT_TYPE = "未知的 combine result type：";
  public static final String UNKNOWN_REQUEST_TYPE = "未知的 request type {}：{}。";

  // ===================== SOURCE =====================

  public static final String ALL_DATA_IN_TSFILEEPOCH_WAS_EXTRACTED =
      "TsFileEpoch {} 中的所有数据已提取";
  public static final String BUFFERSIZE_MUST_BE_A_POWER_OF_2 = "bufferSize 必须是 2 的幂";
  public static final String BUFFERSIZE_MUST_NOT_BE_LESS_THAN_1 =
      "bufferSize 不能小于 1";
  public static final String CAPTURE_TREE_AND_CAPTURE_TABLE_CAN_NOT =
      "capture.tree 和 capture.table 不能同时设为 false";
  public static final String DATABASE_NAME_IS_NULL_WHEN_MATCHING_SOURCES =
      "匹配表模型事件的 source 时数据库名称为空。";
  public static final String DATA_REGION_INJECTED_WATERMARK_EVENT_WITH_TIMESTAMP =
      "Data region {}：注入 watermark 事件，timestamp：{}";
  public static final String DISCARD_TABLET_EVENT_BECAUSE_IT_IS_NOT =
      "丢弃 tablet 事件 {}，因为它不再可靠。将 TsFileEpoch 的状态改为 USING_BOTH。";
  public static final String DISRUPTOR_ALREADY_STARTED = "Disruptor 已启动";
  public static final String DISRUPTOR_SHUTDOWN_COMPLETED = "Disruptor 关闭完成";
  public static final String DISRUPTOR_STARTED_WITH_BUFFER_SIZE = "Disruptor 已启动，缓冲区大小：{}";
  public static final String EXCEPTION_DURING_ONSHUTDOWN = "onShutdown() 期间发生异常";
  public static final String EXCEPTION_DURING_ONSTART = "onStart() 期间发生异常";
  public static final String EXCEPTION_ENCOUNTERED_WHEN_TRIGGERING_SCHEMA_REGION_SNAPSHOT =
      "触发 schema region snapshot 时遇到异常。";
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
      "IoTDBSchemaRegionSource 不支持在 simple consensus 下传输事件";
  public static final String NOT_HAS_PRIVILEGE_TO_TRANSFER_EVENT = "没有权限传输 event：";
  public static final String NOT_HAS_PRIVILEGE_TO_TRANSFER_PLAN = "没有权限传输 plan：";
  public static final String NO_EVENT_HANDLER_CONFIGURED = "未配置 event handler";
  public static final String N_MUST_BE_0 = "n 必须 > 0";
  public static final String PIPEREALTIMEDATAREGIONEXTRACTOR_OBSERVED_DATA_REGION_TIME_PARTITION_GROWT =
      "PipeRealtimeDataRegionExtractor({}) 观察到 data region {} 的 time partition 增长，"
          + "记录 time partition id 边界：{}。";
  public static final String PIPE_AND_IS_NOT_SET_USE_HYBRID =
      "Pipe：'{}'（'{}'）和 '{}'（'{}'）未设置，默认使用 hybrid 模式。";
  public static final String PIPE_ASSIGNER_ON_DATA_REGION_SHUTDOWN_INTERNAL =
      "Pipe：data region {} 上的 Assigner 在 {} ms 内关闭 internal disruptor";
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
      "Pipe：'{}'（'{}'）设置为 false，使用 heartbeat 实时 source。";
  public static final String PIPE_ON_DATA_REGION_SKIP_COMMIT_OF =
      "Pipe {} 在 data region {} 上跳过 event {} 的提交，因为它已被提前 flush。";
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
      "Pipe task {}@{} 对 tsFile {} 触发 canNotUseTabletAnyMore：insert node {} 的内存使用已达单条 pipe {} 的危险阈值，event count：{}";
  public static final String PIPE_UNEXPECTED_PROGRESSINDEX_TYPE_FALLBACK_TO_ORIGIN =
      "Pipe {}@{}：unexpected ProgressIndex type {}, fallback to origin {}.";
  public static final String PIPE_UNSUPPORTED_SOURCE_REALTIME_MODE_CREATE_A =
      "Pipe：不支持的 source realtime mode: {}, create a hybrid source。";
  public static final String PROCESSOR_INTERRUPTED = "处理器被中断";
  public static final String PROCESSOR_INTERRUPTED_UNEXPECTEDLY = "处理器意外中断，继续运行";
  public static final String PROCESSOR_STOPPED = "处理器已停止";
  public static final String SET_FOR_HISTORICAL_DELETION_EVENT =
      "[{}]为 historical deletion event {} 设置 {}";
  public static final String SET_FOR_HISTORICAL_EVENT = "[{}]为 historical event {} 设置 {}";
  public static final String SET_FOR_REALTIME_EVENT = "[{}]为 realtime event {} 设置 {}";
  public static final String SOURCES_FILTERED_BY_DATABASE_AND_TABLE_IS =
      "为 table model 事件匹配 source 时，按 database 和 table 过滤后的结果为空。";
  public static final String SOURCES_FILTERED_BY_DEVICE_IS_NULL_WHEN =
      "为 tree model 事件匹配 source 时，按 device 过滤后的结果为空。";
  public static final String TAKE_SNAPSHOT_ERROR = "获取快照出错：{}";
  public static final String THE_ASSIGNER_QUEUE_CONTENT_HAS_EXCEEDED_HALF =
      "Assigner 队列内容已超过一半，可能已阻塞并可能阻塞写入。"
          + "regionId：{}, capacity：{}, bufferSize：{}";
  public static final String THE_PIPE_CANNOT_EXTRACT_TABLE_MODEL_DATA =
      "sql dialect 设置为 tree 时，pipe 无法抽取 table model 数据。";
  public static final String THE_PIPE_CANNOT_EXTRACT_TREE_MODEL_DATA =
      "sql dialect 设置为 table 时，pipe 无法抽取 tree model 数据。";
  public static final String THE_REFERENCE_COUNT_OF_THE_EVENT_CANNOT =
      "事件 {} 的引用计数无法增加，跳过该事件。";
  public static final String THE_REFERENCE_COUNT_OF_THE_REALTIME_EVENT =
      "实时事件 {} 的引用计数无法增加，跳过该事件。";
  public static final String TIMED_OUT_WAITING_FOR_PROCESSOR_TO_STOP =
      "等待 processor 停止超时";
  public static final String TSFILEEPOCH_NOT_FOUND_FOR_TSFILE_CREATING_A =
      "未找到 TsFile {} 对应的 TsFileEpoch，创建一个新的";
  public static final String WHEN_IS_SET_TO_FALSE_SPECIFYING_AND =
      "当 '{}'（'{}'）设置为 false 时，指定 {} 和 {} 无效。";
  public static final String WHEN_IS_SET_TO_TRUE_SPECIFYING_AND =
      "当 '{}'（'{}'、'{}'、'{}'）设置为 true 时，指定 {} 和 {} 无效。";
  public static final String WHEN_OR_IS_SPECIFIED_SPECIFYING_AND_IS =
      "当指定 {}、{}、{} 或 {} 时，指定 {}、{}、{}、{}、{} 和 {} 无效。";

  // ===================== SINK =====================

  public static final String ACQUIRE_IOPCITEMMGT_SUCCESSFULLY_INTERFACE_ADDRESS =
      "成功获取 IOPCItemMgt! Interface address: {}";
  public static final String ACQUIRE_IOPCSYNCIO_SUCCESSFULLY_INTERFACE_ADDRESS =
      "成功获取 IOPCSyncIO! Interface address: {}";
  public static final String ADDED_EVENT_TO_RETRY_QUEUE = "已将 event {} 添加到 retry queue";
  public static final String BATCH_ID_CREATE_BATCH_DIR_SUCCESSFULLY_BATCH =
      "批次 id = {}：成功创建 batch dir，batch file dir = {}。";
  public static final String BATCH_ID_DELETE_THE_TSFILE_AFTER_FAILED =
      "批次 id = {}：写入 tablet 到 {} 失败后，{} 删除 tsfile {}。{}";
  public static final String MESSAGE_MAYBE_THE_TSFILE_NEEDS_TO_BE_DELETED_MANUALLY_342E28E2 =
      "可能需要手动删除该 tsfile。";
  public static final String BATCH_ID_FAILED_TO_BUILD_THE_TABLE =
      "批次 id = {}：构建 table model TSFile 失败。请检查写入的 Tablet 是否存在时间重叠，"
          + "以及 Table Schema 是否正确。";
  public static final String BATCH_ID_FAILED_TO_CLOSE_THE_TSFILE =
      "批次 id = {}：写入 tablet 失败后关闭 tsfile {} 失败，原因：{}";
  public static final String BATCH_ID_FAILED_TO_CLOSE_THE_TSFILE_1 =
      "批次 id = {}：尝试关闭 batch 时关闭 tsfile {} 失败，原因：{}";
  public static final String BATCH_ID_FAILED_TO_CREATE_BATCH_FILE =
      "批次 id = {}：创建 batch file dir {} 失败。";
  public static final String BATCH_ID_FAILED_TO_DELETE_THE_TSFILE =
      "批次 id = {}：尝试关闭 batch 时删除 tsfile {} 失败，原因：{}";
  public static final String BATCH_ID_FAILED_TO_WRITE_TABLETS_INTO =
      "批次 id = {}：写入 tablet 到 tsfile 失败，原因：{}";
  public static final String BATCH_ID_SEAL_TSFILE_SUCCESSFULLY = "批次 id = {}：成功封存 tsfile {}。";
  public static final String BATCH_ID_UNSUPPORTED_EVENT_TYPE_WHEN_CONSTRUCTING =
      "批次 id = {}：构造 tsfile batch 时不支持的事件 {} 类型 {}";
  public static final String CANNOT_INCREASE_REFERENCE_COUNT_FOR_EVENT_IGNORE =
      "无法增加 event {} 的引用计数，在 batch 中忽略";
  public static final String CANNOT_SERIALIZE_BOTH_TABLET_AND_STATEMENT_ARE =
      "无法序列化：tablet 和 statement 均为空";
  public static final String CERTIFICATE_DIRECTORY_IS_PLEASE_MOVE_CERTIFICATES_FROM =
      "证书目录为：{}，请将证书从 reject 目录移动到 trusted 目录以允许加密访问";
  public static final String CLIENT_HAS_BEEN_RETURNED_TO_THE_POOL =
      "Client 已归还到连接池。当前 handler 状态为 {}。将不会传输 {}。";
  public static final String CLOSED_ASYNCPIPEDATATRANSFERSERVICECLIENTMANAGER_FOR_RECEIVER_ATTRIBUTES =
      "已关闭 AsyncPipeDataTransferServiceClientManager for receiver attributes: {}";
  public static final String CREATE_GROUP_SUCCESSFULLY_SERVER_HANDLE_UPDATE_RATE =
      "创建 group 成功！Server handle：{}, update rate：{} ms";
  public static final String DELETENODETRANSFER_NO_EVENT_SUCCESSFULLY_PROCESSED =
      "DeleteNodeTransfer：第 {} 个 event 处理成功！";
  public static final String DESERIALIZE_PIPEDATA_ERROR_BECAUSE_UNKNOWN_TYPE =
      "反序列化 PipeData 出错，因为未知类型 ";
  public static final String DESERIALIZE_PIPEDATA_ERROR_BECAUSE_UNKNOWN_TYPE_1 =
      "反序列化 PipeData 出错，因为未知类型 {}。";
  public static final String ERROR_GETTING_OPC_CLIENT = "获取 opc client 出错：";
  public static final String ERROR_PROGID_IS_INVALID_OR_UNREGISTERED_HRESULT =
      "错误：ProgID 无效或未注册，(HRESULT=0x";
  public static final String ERROR_RUNNING_OPC_CLIENT = "运行 opc client 出错：";
  public static final String EXCEPTION_OCCURRED_WHEN_PIPETABLEMODELTSFILEBUILDERV2_WRITING_TABLETS_TO =
      "PipeTableModelTsFileBuilderV2 向 tsfile 写入 tablet 时发生异常，"
          + "使用 fallback tsfile builder：{}";
  public static final String EXCEPTION_OCCURRED_WHEN_PIPETREEMODELTSFILEBUILDERV2_WRITING_TABLETS_TO =
      "PipeTreeModelTsFileBuilderV2 向 tsfile 写入 tablet 时发生异常，"
          + "使用 fallback tsfile builder：{}";
  public static final String EXECUTE_STATEMENT_TO_DATABASE_SKIP_BECAUSE_NO =
      "向数据库 {} 执行语句 {} 时因无权限而跳过。";
  public static final String FAILED_TO_ACQUIRE_IOPCITEMMGT_ERROR_CODE_0X =
      "获取 IOPCItemMgt, error code: 0x 失败";
  public static final String FAILED_TO_ACQUIRE_IOPCSYNCIO_ERROR_CODE_0X =
      "获取 IOPCSyncIO, error code: 0x 失败";
  public static final String FAILED_TO_ADD_ITEM = "添加 item 失败 ";
  public static final String FAILED_TO_ADD_ITEM_WIN_ERROR_CODE = "添加 item 失败，win 错误码：0x";
  public static final String FAILED_TO_ADJUST_TIMEOUT_WHEN_FAILED_TO =
      "传输文件失败后调整超时时间失败。";
  public static final String FAILED_TO_BORROW_CLIENT_FOR_CACHED_LEADER =
      "为 cached leader 借用 client {}:{} 失败。";
  public static final String FAILED_TO_BUILD_AND_STARTUP_OPCUASERVER =
      "构建并启动 OpcUaServer 失败";
  public static final String FAILED_TO_CLOSE_ASYNCPIPEDATATRANSFERSERVICECLIENTMANAGER_FOR_RECEIVER_ATTRIBUTE =
      "关闭 AsyncPipeDataTransferServiceClientManager for receiver attributes: {} 失败";
  public static final String FAILED_TO_CLOSE_CLIENT_AFTER_HANDSHAKE_FAILURE =
      "关闭 client {}:{} after handshake failure when the manager is closed 失败。";
  public static final String FAILED_TO_CLOSE_CLIENT_MANAGER = "关闭 client manager 失败。";
  public static final String FAILED_TO_CLOSE_FILE_READER_OR_DELETE =
      "传输文件失败后关闭 file reader 或删除 tsFile 失败。";
  public static final String FAILED_TO_CLOSE_FILE_READER_OR_DELETE_1 =
      "成功传输文件后关闭 file reader 或删除 tsFile 失败。";
  public static final String FAILED_TO_CLOSE_FILE_READER_WHEN_SUCCESSFULLY =
      "成功传输 mod 文件后关闭 file reader 失败。";
  public static final String FAILED_TO_CLOSE_OR_INVALIDATE_CLIENT_WHEN =
      "connector 关闭时关闭或作废 client 失败。Client：{}, Exception：{}";
  public static final String FAILED_TO_CLOSE_TRUSTLISTMANAGER_BECAUSE =
      "关闭 trustListManager 失败，原因：{}。";
  public static final String FAILED_TO_CONNECT_TO_SERVER_ERROR_CODE =
      "连接 server 失败，错误码：0x";
  public static final String FAILED_TO_CONVERT_STATEMENT_TO_TABLET = "将 statement 转换为 tablet 失败。";
  public static final String FAILED_TO_CONVERT_STATEMENT_TO_TABLET_FOR =
      "为序列化将 statement 转换为 tablet 失败";
  public static final String FAILED_TO_CREATE_GROUP_ERROR_CODE_0X = "创建 group 失败，错误码：0x";
  public static final String FAILED_TO_CREATE_NODES_AFTER_TRANSFER_DATA =
      "传输 data value 后创建 node 失败，创建状态：";
  public static final String FAILED_TO_DELETE_BATCH_FILE_THIS_FILE =
      "删除 batch file {} 失败，该文件应稍后手动删除";
  public static final String FAILED_TO_GET_THE_SIZE_OF_PIPETRANSFERBATCHREQBUILDER =
      "获取 PipeTransferBatchReqBuilder 大小失败，返回 0。Exception：{}";
  public static final String FAILED_TO_HANDSHAKE = "握手失败。";
  public static final String FAILED_TO_LOG_ERROR_WHEN_FAILED_TO =
      "传输文件失败后记录错误日志失败。";
  public static final String FAILED_TO_PUSH_VALUE_CHANGE_TO_CLIENT =
      "向 client 推送 value 变更失败，nodeId={}";
  public static final String FAILED_TO_SEND_INITIAL_VALUE_TO_NEW =
      "向新订阅发送初始 value 失败，nodeId={}";
  public static final String FAILED_TO_SERIALIZE_PROGRESS_INDEX = "序列化 progress index {} 失败";
  public static final String FAILED_TO_SHUTDOWN_EXECUTOR = "关闭 executor {} 失败。";
  public static final String FAILED_TO_TRANSFER_DATAVALUE = "传输 dataValue 失败";
  public static final String FAILED_TO_TRANSFER_DATAVALUE_AFTER_SUCCESSFULLY_CREATED =
      "成功创建 node 后传输 dataValue 失败";
  public static final String FAILED_TO_TRANSFER_PIPEDELETENODEEVENT_COMMITTER_KEY_REPLICATE =
      "传输 PipeDeleteNodeEvent {} (committer key={}, replicate index={}) 失败。";
  public static final String FAILED_TO_TRANSFER_TABLETINSERTIONEVENT_COMMITTER_KEY_REPLICATE =
      "传输 TabletInsertionEvent {} (committer key={}, replicate index={}) 失败。";
  public static final String FAILED_TO_TRANSFER_TSFILE_BATCH = "传输 tsfile batch ({}) 失败。";
  public static final String FAILED_TO_TRANSFER_TSFILE_EVENT_ASYNCHRONOUSLY =
      "传输 tsfile event {} asynchronously 失败。";
  public static final String FAILED_TO_UPDATE_LEADER_CACHE_FOR_DEVICE =
      "更新 leader cache for device {} with endpoint {}:{} 失败。";
  public static final String FAILED_TO_WRITE = "写入失败 ";
  public static final String FAILED_TO_WRITE_WIN_ERROR_CODE_0X =
      "写入失败，win 错误码：0x";
  public static final String GENERATE_STATEMENT_FROM_TABLET_ERROR = "从 tablet {} 生成 Statement 出错。";
  public static final String GOT_AN_ERROR_FROM = "从 {}:{} 收到错误 \\\"{}\\\"。";
  public static final String GOT_AN_ERROR_FROM_AN_UNKNOWN_CLIENT =
      "从未知 client 收到错误 \\\"{}\\\"。";
  public static final String HANDSHAKE_SUCCESSFULLY_WITH_RECEIVER =
      "与 receiver {}:{} 握手成功。";
  public static final String ILLEGAL_STATE_WHEN_RETURN_THE_CLIENT_TO =
      "归还 client 到对象池时出现非法状态，连接池可能已被清理。将忽略。";
  public static final String INSERTNODETRANSFER_NO_EVENT_SUCCESSFULLY_PROCESSED =
      "InsertNodeTransfer：第 {} 个 event 处理成功！";
  public static final String INTERRUPTED_WHILE_WAITING_FOR_HANDSHAKE_RESPONSE =
      "waiting for handshake response 时被中断。";
  public static final String IOTCONSENSUSV2ASYNCCONNECTOR_DOES_NOT_SUPPORT_TRANSFERRING_GENERIC_EVENT =
      "IoTConsensusV2AsyncConnector 不支持 transferring generic event: {}.";
  public static final String IOTCONSENSUSV2ASYNCCONNECTOR_DOES_NOT_SUPPORT_TRANSFER_GENERIC_EVENT =
      "IoTConsensusV2AsyncConnector 不支持 transfer generic event: {}.";
  public static final String IOTCONSENSUSV2ASYNCCONNECTOR_ONLY_SUPPORT_PIPETSFILEINSERTIONEVENT_CURRENT_EVEN =
      "IoTConsensusV2AsyncConnector 仅支持 PipeTsFileInsertionEvent。当前事件：{}。";
  public static final String IOTCONSENSUSV2CONNECTOR_TRANSFERBUFFER_QUEUE_OFFER_IS_INTERRUPTED =
      "IoTConsensusV2Connector transferBuffer 队列入队被中断。";
  public static final String IOTCONSENSUSV2TRANSFERBATCHREQBUILDER_THE_MAX_BATCH_SIZE_IS_ADJUSTED =
      "IoTConsensusV2TransferBatchReqBuilder：因内存限制，最大 batch 大小已从 {} 调整为 {}";
  public static final String IOTCONSENSUSV2_CONSENSUSGROUP_EVENT_NOT_FOUND_IN_TRANSFERBUFFER =
      "IoTConsensusV2-ConsensusGroup-{}：在 transferBuffer 中未找到 event-{}，跳过移除。"
          + "queue size = {}";
  public static final String IOTCONSENSUSV2_CONSENSUSGROUP_EVENT_REPLICATE_INDEX_TRANSFER_FAILED =
      "IoTConsensusV2-ConsensusGroup-{}：Event {} replicate index {} 传输失败，"
          + "加入 retry queue 失败，该 event 将被忽略。";
  public static final String IOTCONSENSUSV2_CONSENSUSGROUP_EVENT_REPLICATE_INDEX_TRANSFER_FAILED_1 =
      "IoTConsensusV2-ConsensusGroup-{}：Event {} replicate index {} 传输失败，"
          + "将加入 retry queue。";
  public static final String IOTCONSENSUSV2_CONSENSUSGROUP_NO_EVENT_ADDED_TO_CONNECTOR =
      "IoTConsensusV2-ConsensusGroup-{}：第 {} 个 event-{} 已加入 connector buffer";
  public static final String IOTCONSENSUSV2_CONSENSUSGROUP_ONE_EVENT_SUCCESSFULLY_RECEIVED_BY =
      "IoTConsensusV2-ConsensusGroup-{}：一个 event-{} 已被 follower 成功接收，"
          + "将从队列中移除，queue size = {}, limit size = {}";
  public static final String IOTCONSENSUSV2_CONSENSUSGROUP_RETRYEVENTQUEUE_IS_NOT_EMPTY_AFTER =
      "IoTConsensusV2-ConsensusGroup-{}：20 秒后 retryEventQueue 仍不为空。"
          + "retryQueue size：{}";
  public static final String IOTCONSENSUSV2_CONSENSUSGROUP_RETRY_WITH_INTERVAL_FOR_INDEX =
      "IoTConsensusV2-ConsensusGroup-{}：以间隔 {} 重试 index {} {}";
  public static final String IOTCONSENSUSV2_CONSENSUSGROUP_TRY_TO_REMOVE_EVENT_AFTER =
      "IoTConsensusV2-ConsensusGroup-{}：iotConsensusV2AsyncConnector 关闭后"
          + "尝试移除 event-{}。忽略该操作。";
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
      "IoTConsensusV2-{}：成功传输 file {}（committer key={}, replicate index={}）。";
  public static final String IOTDBCDCCONNECTOR_ONLY_SUPPORT_PIPEINSERTNODETABLETINSERTIONEVENT_AND_PIPERAWTAB =
      "IoTDBCDCConnector 仅支持 PipeInsertNodeTabletInsertionEvent 和 PipeRawTabletInsertionEvent。";
  public static final String IOTDBDATAREGIONAIRGAPCONNECTOR_DOES_NOT_SUPPORT_TRANSFERRING_GENERIC_EVENT =
      "IoTDBDataRegionAirGapConnector 不支持 transferring generic event: {}.";
  public static final String IOTDBDATAREGIONAIRGAPCONNECTOR_ONLY_SUPPORT_PIPEINSERTNODETABLETINSERTIONEVENT_A =
      "IoTDBDataRegionAirGapConnector 仅支持 PipeInsertNodeTabletInsertionEvent 和 PipeRawTabletInsertionEvent。忽略 {}。";
  public static final String IOTDBDATAREGIONAIRGAPCONNECTOR_ONLY_SUPPORT_PIPETSFILEINSERTIONEVENT_IGNORE =
      "IoTDBDataRegionAirGapConnector 仅支持 PipeTsFileInsertionEvent。忽略 {}。";
  public static final String FAILED_TO_LOGIN_TO_RECEIVER_FOR_LEGACY_PIPE_TRANSFER =
      "登录 receiver %s:%s for legacy pipe transfer 失败，原因：code: %d, message: %s";
  public static final String IOTDBLEGACYPIPECONNECTOR_DOES_NOT_SUPPORT_TRANSFERRING_GENERIC_EVENT =
      "IoTDBLegacyPipeConnector 不支持 transferring generic event: {}.";
  public static final String IOTDBLEGACYPIPECONNECTOR_ONLY_SUPPORT_PIPEINSERTNODEINSERTIONEVENT_AND_PIPETABLE =
      "IoTDBLegacyPipeConnector 仅支持 PipeInsertNodeInsertionEvent 和 PipeTabletInsertionEvent。";
  public static final String IOTDBLEGACYPIPECONNECTOR_ONLY_SUPPORT_PIPETSFILEINSERTIONEVENT =
      "IoTDBLegacyPipeConnector 仅支持 PipeTsFileInsertionEvent。";
  public static final String IOTDBSCHEMAREGIONAIRGAPSINK_CAN_T_TRANSFER_TABLETINSERTIONEVENT =
      "IoTDBSchemaRegionAirGapSink 无法传输 TabletInsertionEvent。";
  public static final String IOTDBSCHEMAREGIONAIRGAPSINK_CAN_T_TRANSFER_TSFILEINSERTIONEVENT =
      "IoTDBSchemaRegionAirGapSink 无法传输 TsFileInsertionEvent。";
  public static final String IOTDBSCHEMAREGIONAIRGAPSINK_DOES_NOT_SUPPORT_TRANSFERRING_GENERIC_EVENT =
      "IoTDBSchemaRegionAirGapSink 不支持 transferring generic event: {}.";
  public static final String IOTDBSCHEMAREGIONCONNECTOR_CAN_T_TRANSFER_TABLETINSERTIONEVENT =
      "IoTDBSchemaRegionConnector 无法传输 TabletInsertionEvent。";
  public static final String IOTDBSCHEMAREGIONCONNECTOR_CAN_T_TRANSFER_TSFILEINSERTIONEVENT =
      "IoTDBSchemaRegionConnector 无法传输 TsFileInsertionEvent。";
  public static final String IOTDBSCHEMAREGIONCONNECTOR_DOES_NOT_SUPPORT_TRANSFERRING_GENERIC_EVENT =
      "IoTDBSchemaRegionConnector 不支持 transferring generic event: {}.";
  public static final String IOTDBTHRIFTASYNCCONNECTOR_DOES_NOT_SUPPORT_TRANSFERRING_GENERIC_EVENT =
      "IoTDBThriftAsyncConnector 不支持 transferring generic event: {}.";
  public static final String IOTDBTHRIFTASYNCCONNECTOR_DOES_NOT_SUPPORT_TRANSFER_GENERIC_EVENT =
      "IoTDBThriftAsyncConnector 不支持 transfer generic event: {}.";
  public static final String IOTDBTHRIFTASYNCCONNECTOR_ONLY_SUPPORT_PIPEINSERTNODETABLETINSERTIONEVENT_AND_PI =
      "IoTDBThriftAsyncConnector 仅支持 PipeInsertNodeTabletInsertionEvent 和 PipeRawTabletInsertionEvent。当前事件：{}。";
  public static final String IOTDBTHRIFTASYNCCONNECTOR_ONLY_SUPPORT_PIPETSFILEINSERTIONEVENT_CURRENT_EVENT =
      "IoTDBThriftAsyncConnector 仅支持 PipeTsFileInsertionEvent。当前事件：{}。";
  public static final String IOTDBTHRIFTSYNCCONNECTOR_DOES_NOT_SUPPORT_TRANSFERRING_GENERIC_EVENT =
      "IoTDBThriftSyncConnector 不支持 transferring generic event: {}.";
  public static final String IOTDBTHRIFTSYNCCONNECTOR_ONLY_SUPPORT_PIPEINSERTNODETABLETINSERTIONEVENT_AND_PIP =
      "IoTDBThriftSyncConnector 仅支持 PipeInsertNodeTabletInsertionEvent 和 PipeRawTabletInsertionEvent。忽略 {}。";
  public static final String IOTDBTHRIFTSYNCCONNECTOR_ONLY_SUPPORT_PIPETSFILEINSERTIONEVENT_IGNORE =
      "IoTDBThriftSyncConnector 仅支持 PipeTsFileInsertionEvent。忽略 {}。";
  public static final String LEADERCACHEMANAGER_ALLOCATEDMEMORYBLOCK_HAS_EXPANDED_FROM_TO =
      "LeaderCacheManager.allocatedMemoryBlock 已从 {} 扩展到 {}。";
  public static final String LEADERCACHEMANAGER_ALLOCATEDMEMORYBLOCK_HAS_SHRUNK_FROM_TO =
      "LeaderCacheManager.allocatedMemoryBlock 已从 {} 缩小到 {}。";
  public static final String LOADING_KEYSTORE_AT = "正在从 {} 加载 KeyStore";
  public static final String LOADING_KEYSTORE_AT_1 = "正在从 {}. 加载 KeyStore";
  public static final String LOAD_KEYSTORE_FAILED_THE_EXISTING_KEYSTORE_MAY =
      "加载 keyStore 失败，现有 keyStore 可能已过期，正在重新构造...";
  public static final String NO_OPC_CLIENT_OR_SERVER_IS_SPECIFIED =
      "传输 tablet 时未指定 OPC client 或 server";
  public static final String OPC_DA_SINK_MUST_RUN_ON_WINDOWS = "opc-da-sink 必须在 Windows 系统上运行。";
  public static final String PIPETABLEMODETSFILEBUILDERV2_DOES_NOT_SUPPORT_TREE_MODEL_TABLET =
      "PipeTableModeTsFileBuilderV2 不支持 tree model tablet to build TSFile";
  public static final String PIPETABLEMODETSFILEBUILDER_DOES_NOT_SUPPORT_TREE_MODEL_TABLET =
      "PipeTableModeTsFileBuilder 不支持 tree model tablet to build TSFile";
  public static final String PIPETREEMODELTSFILEBUILDERV2_DOES_NOT_SUPPORT_TABLE_MODEL_TABLET =
      "PipeTreeModelTsFileBuilderV2 不支持 table model tablet to build TSFile";
  public static final String PIPETREEMODELTSFILEBUILDER_DOES_NOT_SUPPORT_TABLE_MODEL_TABLET =
      "PipeTreeModelTsFileBuilder 不支持 table model tablet to build TSFile";
  public static final String POLLED_EVENT_FROM_RETRY_QUEUE = "从 retry queue 取出 event {}。";
  public static final String RECEIVED_AN_ERROR_MESSAGE_FROM =
      "从 {}:{} 收到错误消息 {}";
  public static final String RECEIVED_AN_UNKNOWN_MESSAGE_FROM =
      "从 {}:{} 收到未知消息 {}";
  public static final String RECEIVED_A_ACK_MESSAGE_FROM = "从 {}:{} 收到 ack 消息";
  public static final String RECEIVED_A_BIND_MESSAGE_FROM = "从 {}:{} 收到 bind 消息";
  public static final String REDIRECT_FILE_POSITION_TO = "重定向 file position 到 {}。";
  public static final String REDIRECT_TO_POSITION_IN_TRANSFERRING_TSFILE =
      "正在传输的 tsFile {} 中重定向到 position {}。";
  public static final String NETWORK_FAILED_TO_RECEIVE_TSFILE_STATUS =
      "网络接收 TsFile %s 失败，状态：%s";
  public static final String SECURITY_DIR = "security 目录：{}";
  public static final String SECURITY_PKI_DIR = "security pki 目录：{}";
  public static final String SUCCESSFULLY_ADDED_ITEM = "成功添加 item {}。";
  public static final String SUCCESSFULLY_CONVERTED_PROGID_TO_CLSID =
      "成功将 progID {} 转换为 CLSID: {{}}";
  public static final String SUCCESSFULLY_SHUTDOWN_EXECUTOR = "成功关闭 executor {}。";
  public static final String SUCCESSFULLY_TRANSFERRED_DELETION_EVENT =
      "成功传输 deletion event {}。";
  public static final String SUCCESSFULLY_TRANSFERRED_FILE = "成功传输 file {}。";
  public static final String SUCCESSFULLY_TRANSFERRED_FILE_AND =
      "成功传输 file {}、{} 和 {}。";
  public static final String SUCCESSFULLY_TRANSFERRED_FILE_BATCHED_TABLEINSERTIONEVENTS_REFERENCE_COUNT =
      "成功传输 file {}（批量 TableInsertionEvents，引用计数={}）。";
  public static final String SUCCESSFULLY_TRANSFERRED_FILE_COMMITTER_KEY_COMMIT_ID =
      "成功传输 file {}（committer key={}, commit id={}, 引用计数={}）。";
  public static final String SUCCESSFULLY_TRANSFERRED_SCHEMA_EVENT =
      "成功传输 schema event {}。";
  public static final String SUCCESSFULLY_TRANSFERRED_SCHEMA_REGION_SNAPSHOT_AND =
      "成功传输 schema region 快照 {}、{} 和 {}。";
  public static final String THE_BATCH_SIZE_LIMIT_HAS_EXPANDED_FROM =
      "batch 大小上限已从 {} 扩展到 {}。";
  public static final String THE_BATCH_SIZE_LIMIT_HAS_SHRUNK_FROM =
      "batch 大小上限已从 {} 缩小到 {}。";
  public static final String THE_DEFAULT_QUALITY_CAN_ONLY_BE_GOOD =
      "默认 quality 只能为 'GOOD'、'BAD' 或 'UNCERTAIN'。";
  public static final String THE_EVENT_ACK_IS_NOT_FOUND = "未找到 event ack {}。";
  public static final String THE_EVENT_CAN_T_BE_TRANSFERRED_TO =
      "事件 {} 无法传输给 client，将稍后重试。";
  public static final String THE_EVENT_IN_ERROR_IS_NOT_FOUND =
      "未找到出错的 event {}。";
  public static final String THE_EVENT_POLLED_FROM_THE_QUEUE_IS =
      "从队列 poll 到的 event 与 peek 到的不一致。Peeked event：{}，polled event：{}。";
  public static final String THE_FILE_IS_NOT_FOUND_MAY_ALREADY =
      "未找到 file {}，可能已被删除。";
  public static final String THE_PIPE_WAS_DROPPED_SO_THE_EVENT =
      "pipe {} 已被 drop，event ack {} 将被忽略。";
  public static final String THE_PIPE_WAS_DROPPED_SO_THE_EVENT_1 =
      "pipe {} 已被 drop，出错的 event {} 将被忽略。";
  public static final String THE_PIPE_WAS_DROPPED_SO_THE_EVENT_2 =
      "pipe {} 已被 drop，event {} 将被丢弃。";
  public static final String THE_QUALITY_VALUE_ONLY_SUPPORTS_BOOLEAN_TYPE =
      "quality 值仅支持 boolean 类型，其中 true == GOOD，false == BAD。";
  public static final String THE_SCHEMA_REGION_AIR_GAP_CONNECTOR_DOES =
      "The schema region air gap connector 不支持 transferring single file piece bytes.";
  public static final String THE_SCHEMA_REGION_CONNECTOR_DOES_NOT_SUPPORT =
      "The schema region connector 不支持 transferring single file piece req.";
  public static final String THE_SECURITY_POLICY_CANNOT_BE_EMPTY =
      "安全策略不能为空。";
  public static final String THE_SECURITY_POLICY_CAN_ONLY_BE_NONE =
      "安全策略只能是 'None'、'Basic128Rsa15'、'Basic256'、'Basic256Sha256'、'Aes128_Sha256_RsaOaep' 或 'Aes256_Sha256_RsaPss'。";
  public static final String THE_SEGMENTS_OF_TABLETS_MUST_EXIST =
      "tablet 的 segment 必须存在";
  public static final String THE_TABLET_OF_COMMITID_CAN_T_BE =
      "commitId 为 {} 的 tablet 无法被 client 解析，将稍后重试。";
  public static final String THE_TRANSFER_THREAD_IS_INTERRUPTED = "传输线程被中断。";
  public static final String THE_WEBSOCKET_CONNECTION_FROM_CLIENT_HAS_BEEN =
      "来自 client 的 websocket 连接已关闭！code 为 {}，原因为 {}。是否由远端关闭？{}";
  public static final String THE_WEBSOCKET_CONNECTION_FROM_CLIENT_HAS_BEEN_1 =
      "来自 client {}:{} 的 websocket 连接已关闭！code 为 {}，原因为 {}。是否由远端关闭？{}";
  public static final String THE_WEBSOCKET_CONNECTION_FROM_CLIENT_HAS_BEEN_2 =
      "来自 client {}:{} 的 websocket 连接已打开！";
  public static final String THE_WEBSOCKET_CONNECTION_FROM_HAS_BEEN_CLOSED =
      "来自 {}:{} 的 websocket 连接已关闭，但收到了 commitId 为 {} 的 ack 消息。";
  public static final String THE_WEBSOCKET_CONNECTION_FROM_HAS_BEEN_CLOSED_1 =
      "来自 {}:{} 的 websocket 连接已关闭，但收到了 commitId 为 {} 的 error 消息。";
  public static final String THE_WEBSOCKET_SERVER_HAS_BEEN_STARTED =
      "websocket server {}:{} 已启动！";
  public static final String THE_WRITTEN_TABLET_TIME_MAY_OVERLAP_OR =
      "写入的 Tablet 时间可能重叠，或 Schema 可能不正确";
  public static final String THIS_CONNECTOR_ONLY_SUPPORT_PIPEINSERTNODETABLETINSERTIONEVENT_AND_PIPERAWTABLET =
      "此 Connector 仅支持 PipeInsertNodeTabletInsertionEvent 和 PipeRawTabletInsertionEvent。忽略 {}。";
  public static final String TIMED_OUT_WHEN_WAITING_FOR_CLIENT_HANDSHAKE =
      "等待 client 握手完成超时。";
  public static final String TIOTCONSENSUSV2BATCHTRANSFERRESP_IS_NULL =
      "TIoTConsensusV2BatchTransferResp 为空";
  public static final String TIOTCONSENSUSV2TRANSFERRESP_IS_NULL = "TIoTConsensusV2TransferResp 为空";
  public static final String TPIPETRANSFERRESP_IS_NULL = "TPipeTransferResp 为空";
  public static final String TRANSFER_TSFILE_EVENT_ASYNCHRONOUSLY_WAS_INTERRUPTED =
      "异步传输 tsfile event {} 被中断。";
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
      "WebsocketConnector 增加事件的引用计数失败。忽略该事件。当前事件：{}。";
  public static final String WEBSOCKETCONNECTOR_ONLY_SUPPORT_PIPEINSERTNODETABLETINSERTIONEVENT_AND_PIPERAWTA =
      "WebsocketConnector 仅支持 PipeInsertNodeTabletInsertionEvent 和 PipeRawTabletInsertionEvent。当前事件：{}。";
  public static final String WEBSOCKETCONNECTOR_ONLY_SUPPORT_PIPETSFILEINSERTIONEVENT_CURRENT_EVENT =
      "WebsocketConnector 仅支持 PipeTsFileInsertionEvent。当前事件：{}。";
  public static final String WHEN_THE_OPC_UA_SINK_POINTS_TO =
      "当 OPC UA sink 指向外部 server 时，不支持 table model 数据。";
  public static final String WHEN_THE_OPC_UA_SINK_SETS_WITH =
      "当 OPC UA sink 将 'with-quality' 设置为 true 时，不支持 table model 数据。";
  public static final String WRITEBACKSINK_ONLY_SUPPORT_PIPEINSERTNODETABLETINSERTIONEVENT_AND_PIPERAWTABLETI =
      "WriteBackSink 仅支持 PipeInsertNodeTabletInsertionEvent 和 PipeRawTabletInsertionEvent。忽略 {}。";

  // ===================== RECEIVER =====================

  public static final String ALL_RECEIVERS_RELATED_TO_ARE_RELEASED =
      "与 {} 相关的所有 Receiver 已释放。";
  public static final String AUTO_CREATE_DATABASE_FAILED_BECAUSE = "自动创建 database failed because: ";
  public static final String CREATE_DATABASE_ERROR_STATEMENT_RESULT_STATUS =
      "创建 Database error, statement: {}, result status : {}.";
  public static final String DATABASE_NAME_IS_UNEXPECTEDLY_NULL_FOR_LOADTSFILESTATEMENT =
      "LoadTsFileStatement：{} 的数据库名称为空，跳过数据类型转换。";
  public static final String DATABASE_NAME_IS_UNEXPECTEDLY_NULL_FOR_STATEMENT =
      "statement：{} 的数据库名称为空，跳过数据类型转换。";
  public static final String DATA_TYPE_CONVERSION_FOR_LOADTSFILESTATEMENT_IS_SUCCESSFUL =
      "LoadTsFileStatement {} 的数据类型转换成功。";
  public static final String DATA_TYPE_MISMATCH_DETECTED_TSSTATUS_FOR_LOADTSFILESTATEMENT =
      "LoadTsFileStatement：{} 检测到数据类型不匹配（TSStatus：{}）。开始数据类型转换。";
  public static final String DELETE_ERROR_STATEMENT = "删除 {} 出错，statement：{}。";
  public static final String DELETE_RESULT_STATUS = "删除结果状态：{}。";
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
      "从 client ip = {} 调用 handshake 方法";
  public static final String INVOKE_TRANSPORTDATA_METHOD_FROM_CLIENT_IP =
      "从 client ip = {} 调用 transportData 方法";
  public static final String INVOKE_TRANSPORTPIPEDATA_METHOD_FROM_CLIENT_IP =
      "从 client ip = {} 调用 transportPipeData 方法";
  public static final String IOTCONSENSUSV2RECEIVER_THREAD_IS_INTERRUPTED_WHEN_WAITING_FOR =
      "IoTConsensusV2Receiver 线程在等待 receiver 初始化时被中断，可能因为系统退出。";
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
      "IoTConsensusV2-PipeName-{}：第 {} 个 event 在等待超时后执行，当前 "
          + "receiver syncIndex：{}";
  public static final String IOTCONSENSUSV2_PIPENAME_NO_EVENT_GET_EXECUTED_BECAUSE =
      "IoTConsensusV2-PipeName-{}：第 {} 个 event 已执行，因为 receiver buffer 的长度 >= "
          + "pipeline，当前 receiver syncIndex {}, 当前 buffer 长度 {}";
  public static final String IOTCONSENSUSV2_PIPENAME_PATH_TRAVERSAL_ATTEMPT_DETECTED_FILENAME =
      "IoTConsensusV2-PipeName-{}：Path traversal attempt detected! Filename: {}";
  public static final String IOTCONSENSUSV2_PIPENAME_PROCESS_NO_EVENT_SUCCESSFULLY =
      "IoTConsensusV2-PipeName-{}：process no.{} event successfully!";
  public static final String IOTCONSENSUSV2_PIPENAME_RECEIVED_A_DEPRECATED_REQUEST_WHICH =
      "IoTConsensusV2-PipeName-{}：收到已弃用的 request-{}，可能因为 {}。 ";
  public static final String IOTCONSENSUSV2_PIPENAME_RECEIVER_DETECTED_AN_NEWER_PIPETASKRESTARTTIMES =
      "IoTConsensusV2-PipeName-{}：receiver 检测到更新的 pipeTaskRestartTimes，"
          + "表明 pipe task 已重启。receiver 将重置其所有数据。";
  public static final String IOTCONSENSUSV2_PIPENAME_RECEIVER_DETECTED_AN_NEWER_REBOOTTIMES =
      "IoTConsensusV2-PipeName-{}：receiver 检测到更新的 rebootTimes，"
          + "表明 leader 已重启。receiver 将重置其所有数据。";
  public static final String IOTCONSENSUSV2_PIPENAME_RECEIVER_FILE_DIR_WAS_CREATED =
      "IoTConsensusV2-PipeName-{}：Receiver file dir {} 已创建。";
  public static final String IOTCONSENSUSV2_PIPENAME_RECEIVER_THREAD_GET_INTERRUPTED_WHEN =
      "IoTConsensusV2-PipeName-{}：receiver 线程在退出时被中断。";
  public static final String IOTCONSENSUSV2_PIPENAME_SEAL_FILE_SUCCESSFULLY =
      "IoTConsensusV2-PipeName-{}：成功封存 file {}。";
  public static final String IOTCONSENSUSV2_PIPENAME_SEAL_FILE_WITH_MODS_SUCCESSFULLY =
      "IoTConsensusV2-PipeName-{}：成功封存 file with mods {}。";
  public static final String IOTCONSENSUSV2_PIPENAME_SKIP_LOAD_TSFILE_WHEN_SEALING =
      "IoTConsensusV2-PipeName-{}：封存时跳过加载 tsfile-{}，因为该 region "
          + "已被移除或迁移。";
  public static final String IOTCONSENSUSV2_PIPENAME_STARTING_TO_RECEIVE_TSFILE_PIECES =
      "IoTConsensusV2-PipeName-{}：开始接收 tsFile pieces";
  public static final String IOTCONSENSUSV2_PIPENAME_STARTING_TO_RECEIVE_TSFILE_SEAL =
      "IoTConsensusV2-PipeName-{}：开始接收 tsFile seal";
  public static final String IOTCONSENSUSV2_PIPENAME_STARTING_TO_RECEIVE_TSFILE_SEAL_1 =
      "IoTConsensusV2-PipeName-{}：开始接收 tsFile seal with mods";
  public static final String IOTCONSENSUSV2_PIPENAME_START_TO_RECEIVE_NO_EVENT =
      "IoTConsensusV2-PipeName-{}：开始接收 no.{} event";
  public static final String IOTCONSENSUSV2_PIPENAME_THE_POINT_COUNT_OF_TSFILE =
      "IoTConsensusV2-PipeName-{}：sender 未给出 TsFile {} 的点数，将从 TsFile 读取实际点数。";
  public static final String IOTCONSENSUSV2_PIPENAME_TSFILEWRITER_RETURNED_SELF =
      "IoTConsensusV2-PipeName-{}：tsFileWriter-{} 返回自身";
  public static final String IOTCONSENSUSV2_PIPENAME_TSFILEWRITER_ROLL_TO_WRITING_PATH =
      "IoTConsensusV2-PipeName-{}：tsfileWriter-{} 滚动到 writing path {}";
  public static final String IOTCONSENSUSV2_PIPENAME_TSFILE_WRITER_IS_CLEANED_UP =
      "IoTConsensusV2-PipeName-{}：tsfile writer-{} 已被清理，因为长时间未收到新请求。";
  public static final String IOTCONSENSUSV2_PIPENAME_UNKNOWN_PIPEREQUESTTYPE_RESPONSE_STATUS =
      "IoTConsensusV2-PipeName-{}：未知的 PipeRequestType, response status = {}。";
  public static final String IOTCONSENSUSV2_PIPENAME_WAS_DELETED =
      "IoTConsensusV2-PipeName-{}：{} {} 已删除.";
  public static final String IOTCONSENSUSV2_PIPENAME_WRITING_FILE_IS_NOT_AVAILABLE =
      "IoTConsensusV2-PipeName-{}：Writing file {} 不可用。Writing file 为 null：{}, writing file "
          + "是否存在：{}, writing file writer 是否为 null：{}。";
  public static final String IOTCONSENSUSV2_PIPENAME_WRITING_FILE_IS_NOT_EXISTED =
      "IoTConsensusV2-PipeName-{}：Writing file {} 不存在或名称不正确，尝试创建。当前 writing file 为 {}。";
  public static final String IOTCONSENSUSV2_PIPENAME_WRITING_FILE_S_OFFSET_IS =
      "IoTConsensusV2-PipeName-{}：Writing file {} 的 offset 为 {}，但请求 sender 的 offset "
          + "为 {}。";
  public static final String IOTCONSENSUSV2_PIPENAME_WRITING_FILE_WAS_CREATED_READY =
      "IoTConsensusV2-PipeName-{}：Writing file {} 已创建。准备写入 file piece。";
  public static final String IOTCONSENSUSV2_RECEIVE_ON_THE_FLY_NO_EVENT =
      "IoTConsensusV2-{}：data region 删除后收到 on-the-fly 的第 {} 个 event，将其丢弃";
  public static final String IOTCONSENSUSV2_TRANSFER_BATCH_HASN_T_BEEN_IMPLEMENTED =
      "IoTConsensusV2 transfer batch 尚未实现。";
  public static final String IOTCONSENSUSV2_TSFILEWRITER_SET_NULL_WRITING_FILE =
      "IoTConsensusV2-{}：TsFileWriter-{} 设置 writing file 为 null";
  public static final String IOTCONSENSUSV2_TSFILEWRITER_SET_NULL_WRITING_FILE_WRITER =
      "IoTConsensusV2-{}：TsFileWriter-{} 设置 writing file writer 为 null";
  public static final String IOTCONSENSUSV2_UNKNOWN_IOTCONSENSUSV2REQUESTVERSION_RESPONSE_STATUS =
      "IoTConsensusV2：未知的 IoTConsensusV2RequestVersion, response status = {}。";
  public static final String IOTCONSENSUSV2_UNKNOWN_PIPEREQUESTTYPE_RESPONSE_STATUS =
      "IoTConsensusV2 未知的 PipeRequestType，response status = {}。";
  public static final String IOTCONSENSUSV2_WAITING_FOR_THE_PREVIOUS_EVENT_TIMES =
      "IoTConsensusV2-{}：等待上一个 event 超时，当前 peek {}, 当前 id {}";
  public static final String IOTDBAIRGAPRECEIVERAGENT_STARTED =
      "IoTDBAirGapReceiverAgent {} 已启动。";
  public static final String IOTDBAIRGAPRECEIVERAGENT_STOPPED =
      "IoTDBAirGapReceiverAgent {} 已停止。";
  public static final String LOAD_ACTIVE_LISTENING_PIPE_DIR_IS_NOT =
      "未设置 load active listening pipe dir。";
  public static final String LOAD_PIPEDATA_WITH_SERIALIZE_NUMBER_SUCCESSFULLY =
      "成功加载 serialize number 为 {} 的 pipeData。";
  public static final String LOAD_TSFILE_ERROR_STATEMENT = "加载 TsFile {} 出错，statement：{}。";
  public static final String LOAD_TSFILE_RESULT_STATUS = "加载 TsFile 结果状态：{}。";
  public static final String PARSE_DATABASE_PARTIALPATH_ERROR = "Parse database PartialPath {} 出错。";
  public static final String PIPE_AIR_GAP_RECEIVER_CHECKSUM_FAILED_EXPECTED =
      "Pipe air gap receiver {}：校验和失败，期望：{}, 实际：{}";
  public static final String PIPE_AIR_GAP_RECEIVER_CLOSED_BECAUSE_OF =
      "Pipe air gap receiver {} 因校验和失败而关闭。Socket：{}";
  public static final String PIPE_AIR_GAP_RECEIVER_CLOSED_BECAUSE_OF_1 =
      "Pipe air gap receiver {} 因异常而关闭。Socket：{}";
  public static final String PIPE_AIR_GAP_RECEIVER_CLOSED_BECAUSE_SOCKET =
      "Pipe air gap receiver {} 因 socket 已关闭而关闭。Socket：{}";
  public static final String PIPE_AIR_GAP_RECEIVER_EXCEPTION_DURING_HANDLING =
      "Pipe air gap receiver {}：处理接收时发生异常。Socket：{}";
  public static final String PIPE_AIR_GAP_RECEIVER_HANDLE_DATA_FAILED =
      "Pipe air gap receiver {}：处理数据失败，状态：{}, req：{}";
  public static final String PIPE_AIR_GAP_RECEIVER_SOCKET_CLOSED_WHEN =
      "Pipe air gap receiver {}：监听数据时 socket {} 关闭。原因：{}";
  public static final String PIPE_AIR_GAP_RECEIVER_STARTED_SOCKET =
      "Pipe air gap receiver {} 已启动。Socket：{}";
  public static final String PIPE_AIR_GAP_RECEIVER_TEMPORARY_UNAVAILABLE_RETRY =
      "Pipe air gap receiver {}：临时不可用重试超时，向 sender 返回 FAIL。";
  public static final String PIPE_AIR_GAP_RECEIVER_TSSTATUS_IS_ENCOUNTERED =
      "Pipe air gap receiver {}：在 air gap receiver 处遇到 TSStatus {}，将忽略。";
  public static final String PIPE_DATA_TRANSPORT_ERROR = "Pipe 数据传输错误，{}";
  public static final String PIPE_INSERTING_TABLET_TO_CASTING_TYPE_FROM =
      "Pipe：向 {}.{} 写入 tablet。将类型从 {} 转换为 {}。";
  public static final String RECEIVERS_EXECUTOR_IS_CLOSED = "Receivers-{} 的 executor 已关闭。";
  public static final String RECEIVER_EXIT_SUCCESSFULLY = "Receiver-{} 成功退出。";
  public static final String RECEIVER_ID = "Receiver id = {}：{}";
  public static final String RECEIVER_ID_THE_NUMBER_OF_DEVICE_PATHS =
      "Receiver id = {}：device path 数量与 statement {} 中的 sub-status 不一致：{}。";
  public static final String RECEIVER_ID_UNKNOWN_PIPEREQUESTTYPE_RESPONSE_STATUS =
      "Receiver id = {}：未知的 PipeRequestType，response status = {}。";
  public static final String RECEIVER_ID_UNSUPPORTED_STATEMENT_TYPE_FOR_REDIRECTION =
      "Receiver id = {}：不支持的 statement type {} 用于 redirection。";
  public static final String RECEIVER_IS_READY = "Receiver-{} 已就绪";
  public static final String RECEIVER_TEMPORARILY_OUT_OF_MEMORY_FORMAT =
      "执行 %s 时暂时内存不足。请求内存：%d bytes，已用内存：%d bytes，可用内存：%d bytes，"
          + "总非浮动内存：%d bytes";
  public static final String REGISTER_WITH_INTERVAL_IN_SECONDS_SUCCESSFULLY =
      "成功注册 {}，间隔时间（秒）{}。";
  public static final String SOCKET_CLOSED_WHEN_EXECUTING_READTILLFULL =
      "执行 readTillFull 时 socket 关闭。";
  public static final String SOCKET_CLOSED_WHEN_EXECUTING_SKIPTILLENOUGH =
      "执行 skipTillEnough 时 socket 关闭。";
  public static final String START_LOAD_PIPEDATA_WITH_SERIALIZE_NUMBER_AND =
      "开始加载 serialize number 为 {}、type 为 {} 的 pipeData，value={}";
  public static final String STORAGE_ENGINE_READONLY = "存储引擎只读";
  public static final String SYNC_START_AT_TO_IS_DONE = "Sync {} 从 {} 开始到 {} 已完成。";
  public static final String TEMPORARY_UNAVAILABLE_EXCEPTION_ENCOUNTERED_AT_AIR_GAP =
      "在 air gap receiver 处遇到临时不可用异常，将在本地重试。";
  public static final String THE_IOTCONSENSUSV2_REQUEST_VERSION_IS_DIFFERENT_FROM =
      "iotConsensusV2 请求版本 {} 与 sender 请求版本 {} 不同，"
          + "receiver 将被重置为 sender 请求版本。";
  public static final String THE_START_INDEX_OF_DATA_SYNC_IS =
      "data sync 的起始索引 {} 无效。文件不存在且起始索引应等于 0。";
  public static final String THE_START_INDEX_OF_DATA_SYNC_IS_1 =
      "data sync 的起始索引 {} 无效。文件的起始索引应等于 {}。";
  public static final String THRIFT_CONNECTION_IS_NOT_ALIVE = "Thrift 连接已断开。";
  public static final String TSFILECHECKER_DID_NOT_TERMINATE_WITHIN_S =
      "TsFileChecker 未在 {} 秒内终止";
  public static final String TSFILECHECKER_THREAD_STILL_DOESN_T_EXIT_AFTER =
      "TsFileChecker 线程 {} 在 30 秒后仍未退出";
  public static final String UNHANDLED_EXCEPTION_DURING_PIPE_AIR_GAP_RECEIVER =
      "pipe air gap receiver 监听期间发生未处理异常";
  public static final String UNSUPPORTED_DATA_TYPE = "不支持的 data type：";

  // ===================== RESOURCE =====================

  public static final String CANNOT_GET_DATA_REGION_IDS_USE_DEFAULT =
      "无法获取 data region id，使用默认的 lock segment 大小：{}";
  public static final String EXPAND_CALLBACK_IS_NOT_SUPPORTED_IN_PIPEFIXEDMEMORYBLOCK =
      "PipeFixedMemoryBlock 不支持 expand callback";
  public static final String EXPAND_METHOD_IS_NOT_SUPPORTED_IN_PIPEFIXEDMEMORYBLOCK =
      "PipeFixedMemoryBlock 不支持 expand method";
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
      "forceAllocate：等待可用内存时被中断";
  public static final String FORCERESIZE_CANNOT_RESIZE_A_NULL_OR_RELEASED =
      "forceResize：无法对 null 或已释放的内存块进行 resize";
  public static final String FORCERESIZE_INTERRUPTED_WHILE_WAITING_FOR_AVAILABLE_MEMORY =
      "forceResize：等待可用内存时被中断";
  public static final String INTERRUPTED_WHILE_WAITING_FOR_THE_LOCK = "等待锁时被中断。";
  public static final String IS_RELEASED_AFTER_THREAD_INTERRUPTION =
      "{} 在线程中断后已被释放。";
  public static final String PIPEPERIODICALLOGREDUCER_IS_ALLOCATED_TO_BYTES =
      "PipePeriodicalLogReducer 已分配 {} 字节。";
  public static final String PIPETSFILERESOURCE_CACHED_DEVICEISALIGNEDMAP_FOR_TSFILE =
      "PipeTsFileResource：已为 tsfile {} 缓存 deviceIsAlignedMap。";
  public static final String PIPETSFILERESOURCE_CACHED_OBJECTS_FOR_TSFILE =
      "PipeTsFileResource：已为 tsfile {} 缓存对象。";
  public static final String PIPETSFILERESOURCE_CLOSED_TSFILE_AND_CLEANED_UP =
      "PipeTsFileResource：已关闭 tsfile {} 并完成清理。";
  public static final String PIPETSFILERESOURCE_FAILED_TO_CACHE_OBJECTS_FOR_TSFILE =
      "PipeTsFileResource：为 tsfile {} 缓存对象失败，因为内存使用过高";
  public static final String PIPETSFILERESOURCE_FAILED_TO_DELETE_TSFILE_WHEN_CLOSING =
      "PipeTsFileResource：关闭时删除 tsfile {} 失败，原因：{}。请手动删除。";
  public static final String PIPETSFILERESOURCE_S_REFERENCE_COUNT_IS_DECREASED_TO =
      "PipeTsFileResource 的引用计数减少到 0 以下。";
  public static final String PIPE_HARDLINK_DIR_FOUND_DELETING_IT_RESULT =
      "发现 Pipe hardlink 目录，正在删除：{}, 结果：{}";
  public static final String PIPE_HARDLINK_DIR_FOUND_MOVED_TO_PERIODICAL_DELETE =
      "发现 Pipe hardlink 目录，已将其从 {} 移动到 {} 以进行限速周期性删除。";
  public static final String PIPE_STALE_HARDLINK_DIR_FOUND_REGISTERING_PERIODICAL_DELETE =
      "发现过期的 Pipe hardlink 目录，已为其注册限速周期性删除：{}";
  public static final String PIPE_HARDLINK_DIR_PERIODICAL_DELETE_FINISHED =
      "已通过周期性任务删除过期的 Pipe hardlink 目录 {}，结果：{}";
  public static final String PIPE_HARDLINK_DIR_PERIODICAL_DELETE_PROGRESS =
      "已从过期的 Pipe hardlink 目录中周期性删除 {} 个路径，当前目录：{}, 当前轮次结果：{}";
  public static final String PIPE_HARDLINK_DIR_PERIODICAL_DELETE_ALL_FINISHED =
      "已通过周期性任务删除全部过期的 Pipe hardlink 目录。";
  public static final String PIPE_HARDLINK_DIR_MOVE_FAILED_DELETING_SYNC =
      "为周期性删除移动 Pipe hardlink 目录 {} 失败，改为同步删除。";
  public static final String PIPE_SNAPSHOT_DIR_FOUND_DELETING_IT =
      "发现 Pipe snapshot 目录，正在删除：{},";
  public static final String SHRINK_CALLBACK_IS_NOT_SUPPORTED_IN_PIPEFIXEDMEMORYBLOCK =
      "PipeFixedMemoryBlock 不支持 shrink callback";
  public static final String SHRINK_METHOD_IS_NOT_SUPPORTED_IN_PIPEFIXEDMEMORYBLOCK =
      "PipeFixedMemoryBlock 不支持 shrink method";
  public static final String THE_MEMORY_BLOCK_HAS_BEEN_RELEASED = "内存块已被释放";
  public static final String THE_MULTIPLE_N_MUST_BE_GREATER_THAN =
      "倍数 n 必须大于 0";
  public static final String TRYALLOCATE_ALLOCATED_MEMORY_TOTAL_MEMORY_SIZE_BYTES =
      "tryAllocate：已分配内存，总内存大小 {} 字节，已用内存大小 {} 字节，原始请求内存大小 {} 字节，实际请求内存大小 {} 字节";
  public static final String TRYALLOCATE_FAILED_TO_ALLOCATE_MEMORY_TOTAL_MEMORY =
      "tryAllocate：分配内存失败，总内存大小 {} 字节，已用内存大小 {} 字节，请求内存大小 {} 字节";
  public static final String TRYEXPANDALLANDCHECKCONSISTENCY_MEMORY_USAGE_IS_NOT_CONSISTENT_WITH =
      "tryExpandAllAndCheckConsistency：内存使用与已分配 block 不一致，usedMemorySizeInBytes 为 {}，但所有 block 的总和为 {}";
  public static final String TRYEXPANDALLANDCHECKCONSISTENCY_MEMORY_USAGE_OF_TABLETS_IS_NOT =
      "tryExpandAllAndCheckConsistency：tablet 的内存使用与已分配 block 不一致，usedMemorySizeInBytesOfTablets 为 {}，但所有 tablet block 的总和为 {}";
  public static final String TRYEXPANDALLANDCHECKCONSISTENCY_MEMORY_USAGE_OF_TSFILES_IS_NOT =
      "tryExpandAllAndCheckConsistency：tsfile 的内存使用与已分配 block 不一致，usedMemorySizeInBytesOfTsFiles 为 {}，但所有 tsfile block 的总和为 {}";

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
  public static final String OPC_UA_SECURITY_DIR =
      "安全目录：{}";
  public static final String OPC_UA_SECURITY_PKI_DIR =
      "安全 PKI 目录：{}";

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
  // ---------------------------------------------------------------------------
  // 补充日志消息
  // ---------------------------------------------------------------------------
  public static final String PIPE_LOG_SUBSCRIPTION_DETECT_DUPLICATED_PIPETSFILEINSERTIONEVENT_23A4740C =
      "Subscription：检测到重复的 PipeTsFileInsertionEvent {}，直接提交";
  public static final String PIPE_LOG_SUBSCRIPTION_PREFETCHING_QUEUE_BOUND_TO_TOPIC_FOR_CONSUMER_ECB64624 =
      "Subscription：绑定到 topic [{}]、consumer group [{}] 的 prefetching queue 已完成，向客户端返回终止响应";
  public static final String PIPE_LOG_SUBSCRIPTION_PREFETCHING_QUEUE_BOUND_TO_TOPIC_FOR_CONSUMER_8F561EB2 =
      "Subscription：绑定到 topic [{}]、consumer group [{}] 的 prefetching queue 已完成，回复客户端心跳请求";
  public static final String PIPE_LOG_SUBSCRIPTION_CREATE_PREFETCHING_QUEUE_BOUND_TO_TOPIC_FOR_E7F21F1E =
      "Subscription：创建绑定到 topic [{}]、consumer group [{}] 的 prefetching queue";
  public static final String PIPE_LOG_SUBSCRIPTION_DROP_PREFETCHING_QUEUE_BOUND_TO_TOPIC_FOR_CONSUMER_21F313CB =
      "Subscription：删除绑定到 topic [{}]、consumer group [{}] 的 prefetching queue";
  public static final String PIPE_LOG_SUBSCRIPTION_PREFETCHING_QUEUE_BOUND_TO_TOPIC_FOR_CONSUMER_03B89C51 =
      "Subscription：绑定到 topic [{}]、consumer group [{}] 的 prefetching queue 仍然存在，请在关闭前解绑";
  public static final String PIPE_LOG_SUBSCRIPTION_PREFETCHING_QUEUE_BOUND_TO_TOPIC_FOR_CONSUMER_EA7D450B =
      "Subscription：绑定到 topic [{}]、consumer group [{}] 的 prefetching queue 已关闭";
  public static final String PIPE_LOG_SUBSCRIPTION_PREFETCHING_QUEUE_BOUND_TO_TOPIC_FOR_CONSUMER_12E69B65 =
      "Subscription：绑定到 topic [{}]、consumer group [{}] 的 prefetching queue 不存在";
  public static final String PIPE_LOG_SUBSCRIPTION_PREFETCHING_QUEUE_BOUND_TO_TOPIC_FOR_CONSUMER_C2735402 =
      "Subscription：绑定到 topic [{}]、consumer group [{}] 的 prefetching queue 已存在";
  public static final String PIPE_LOG_SUBSCRIPTIONPREFETCHINGTABLETQUEUE_DETECTED_OUTDATED_POLL_C0001CCF =
      "SubscriptionPrefetchingTabletQueue {} 检测到过期的 poll 请求，consumer {}，commit context {}，offset {}";
  public static final String PIPE_LOG_CONSENSUSSUBSCRIPTIONBROKER_POLL_CALLED_CONSUMERID_TOPICNAMES_5F1F5175 =
      "ConsensusSubscriptionBroker [{}]：调用 poll，consumerId={}，topicNames={}，queueCount={}，"
          + "maxBytes={}";
  public static final String PIPE_LOG_CONSENSUSSUBSCRIPTIONBROKER_POLL_RESULT_CONSUMERID_EVENTSPOLLED_06412726 =
      "ConsensusSubscriptionBroker [{}]：poll 结果，consumerId={}，eventsPolled={}，eventsNacked={}";
  public static final String PIPE_LOG_CONSENSUSSUBSCRIPTIONBROKER_REFRESHED_OWNERSHIP_FOR_TOPIC_EB11CF64 =
      "ConsensusSubscriptionBroker [{}]：刷新 topic [{}] 的 ownership，consumers={}，regions={}，"
          + "generation={}";
  public static final String PIPE_LOG_CONSENSUSSUBSCRIPTIONBROKER_STABLE_OWNERSHIP_POLL_ORDER_D40BB7D4 =
      "ConsensusSubscriptionBroker [{}]：topic [{}] 的稳定 ownership poll 顺序，assignedQueueCount={}";
  public static final String PIPE_LOG_SUBSCRIPTION_CONSENSUS_PREFETCHING_QUEUE_FOR_TOPIC_REGION_B40792D9 =
      "Subscription：topic [{}]、Region [{}]、consumer group [{}] 的 consensus prefetching queue 已存在，跳过";
  public static final String PIPE_LOG_SUBSCRIPTION_CREATE_CONSENSUS_PREFETCHING_QUEUE_BOUND_TO_0DBFC05E =
      "Subscription：创建绑定到 topic [{}]、consumer group [{}] 的 consensus prefetching queue，"
          + "consensusGroupId={}，fallbackCommittedRegionProgress={}，tailStartSearchIndex={}，"
          + "initialRuntimeVersion={}，initialActive={}，totalRegionQueues={}";
  public static final String PIPE_LOG_SUBSCRIPTION_CLOSED_CONSENSUS_PREFETCHING_QUEUE_FOR_TOPIC_3A9DDEC5 =
      "Subscription：由于 Region 移除，已关闭 topic [{}]、Region [{}]、consumer group [{}] 的 consensus "
          + "prefetching queue";
  public static final String PIPE_LOG_SUBSCRIPTION_CONSENSUS_PREFETCHING_QUEUE_S_BOUND_TO_TOPIC_AB10ED07 =
      "Subscription：绑定到 topic [{}]、consumer group [{}] 的 consensus prefetching queue 仍然存在，请在关闭前解绑";
  public static final String PIPE_LOG_SUBSCRIPTION_DROP_ALL_CONSENSUS_PREFETCHING_QUEUE_S_BOUND_FCC1B2C4 =
      "Subscription：删除全部 {} 个绑定到 topic [{}]、consumer group [{}] 的 consensus prefetching queue";
  public static final String PIPE_LOG_CONSENSUSSUBSCRIPTIONBROKER_NO_QUEUES_FOR_TOPIC_TO_COMMIT_7D8CC39D =
      "ConsensusSubscriptionBroker [{}]：topic [{}] 没有可提交的 queue";
  public static final String PIPE_LOG_CONSENSUSSUBSCRIPTIONBROKER_COMMIT_CONTEXT_NOT_FOUND_IN_46DF62A6 =
      "ConsensusSubscriptionBroker [{}]：未找到 commit context {}，已检查 {} 个 Region queue，topic [{}]";
  public static final String PIPE_LOG_CONSENSUSSUBSCRIPTIONBROKER_NO_QUEUES_FOR_TOPIC_TO_SEEK_6307A90D =
      "ConsensusSubscriptionBroker [{}]：topic [{}] 没有可执行 seek 的 queue";
  public static final String PIPE_LOG_CONSENSUSSUBSCRIPTIONBROKER_UNSUPPORTED_SEEKTYPE_FOR_TOPIC_EDCA2CF2 =
      "ConsensusSubscriptionBroker [{}]：不支持 seekType {}，topic [{}]";
  public static final String PIPE_LOG_CONSENSUSSUBSCRIPTIONBROKER_NO_QUEUES_FOR_TOPIC_TO_SEEK_9AC3890C =
      "ConsensusSubscriptionBroker [{}]：topic [{}] 没有可执行 seek(topicProgress) 的 queue";
  public static final String PIPE_LOG_CONSENSUSSUBSCRIPTIONBROKER_NO_QUEUES_FOR_TOPIC_TO_SEEKAFTER_C6D87BFD =
      "ConsensusSubscriptionBroker [{}]：topic [{}] 没有可执行 seekAfter(topicProgress) 的 queue";
  public static final String PIPE_LOG_SUBSCRIPTION_CONSENSUS_PREFETCHING_QUEUES_BOUND_TO_TOPIC_63B37089 =
      "Subscription：绑定到 topic [{}]、consumer group [{}] 的 consensus prefetching queue 不存在";
  public static final String PIPE_LOG_SUBSCRIPTIONPREFETCHINGTSFILEQUEUE_DETECTED_OUTDATED_POLL_7E0CE108 =
      "SubscriptionPrefetchingTsFileQueue {} 检测到过期的 poll 请求，consumer {}，commit context {}，writing "
          + "offset {}";
  public static final String PIPE_LOG_SUBSCRIPTION_SUBSCRIPTIONPREFETCHINGQUEUE_COMMIT_PIPETERMINATEEVENT_36529DC9 =
      "Subscription：SubscriptionPrefetchingQueue {} 提交 PipeTerminateEvent {}";
  public static final String PIPE_LOG_SUBSCRIPTION_SUBSCRIPTIONPREFETCHINGQUEUE_IGNORE_ENRICHEDEVENT_95C6241C =
      "Subscription：SubscriptionPrefetchingQueue {} 在 prefetch 期间忽略 EnrichedEvent {}。";
  public static final String PIPE_LOG_SUBSCRIPTION_SUBSCRIPTIONPREFETCHINGQUEUE_POLL_COMMITTED_8684FF17 =
      "Subscription：SubscriptionPrefetchingQueue {} 从 prefetching queue poll 到已提交事件 {}（不变量被破坏），"
          + "移除该事件";
  public static final String PIPE_LOG_SUBSCRIPTION_SUBSCRIPTIONPREFETCHINGQUEUE_POLL_NON_POLLABLE_644D5D6B =
      "Subscription：SubscriptionPrefetchingQueue {} 从 prefetching queue poll 到不可 poll 事件 {}（不变量被破坏），"
          + "执行 nack 并移除该事件";
  public static final String PIPE_LOG_SUBSCRIPTION_SUBSCRIPTIONPREFETCHINGQUEUE_INTERRUPTED_WHILE_F8923826 =
      "Subscription：SubscriptionPrefetchingQueue {} 在 poll 事件期间被中断。";
  public static final String PIPE_LOG_SUBSCRIPTION_INCONSISTENT_HEARTBEAT_EVENT_WHEN_PEEKING_BROKEN_BFE1DF6E =
      "Subscription：{} peeking 时 heartbeat event 不一致（不变量被破坏），期望 {}，实际 {}，放回队列";
  public static final String PIPE_LOG_SUBSCRIPTION_SUBSCRIPTIONPREFETCHINGQUEUE_ONLY_SUPPORT_PREFETCH_F3B33B30 =
      "Subscription：SubscriptionPrefetchingQueue {} 仅支持 prefetch EnrichedEvent。忽略 {}。";
  public static final String PIPE_LOG_SUBSCRIPTION_SUBSCRIPTIONPREFETCHINGQUEUE_PREFETCH_TSFILEINSERTIONEVENT_19444D2C =
      "Subscription：SubscriptionPrefetchingQueue {} 在 ToTabletIterator 非 null 时 prefetch "
          + "TsFileInsertionEvent（不变量被破坏）。忽略 {}。";
  public static final String PIPE_LOG_FAILED_TO_INCREASE_REFERENCE_COUNT_FOR_WHEN_ON_RETRYABLE_4E10BE3B =
      "为 {} 增加引用计数失败，发生在可重试 TabletInsertionEvent 上执行 {} 时";
  public static final String PIPE_LOG_EXCEPTION_OCCURRED_WHEN_ON_RETRYABLE_TABLETINSERTIONEVENT_2350D9F7 =
      "执行 {} 时在可重试 TabletInsertionEvent {} 上发生异常";
  public static final String PIPE_LOG_SUBSCRIPTION_SUBSCRIPTION_COMMIT_CONTEXT_DOES_NOT_EXIST_0E4EF990 =
      "Subscription：subscription commit context {} 不存在，可能已提交或发生了意外情况，prefetching queue：{}";
  public static final String PIPE_LOG_SUBSCRIPTION_SUBSCRIPTION_EVENT_IS_COMMITTED_SUBSCRIPTION_BEE17D7F =
      "Subscription：subscription event {} 已提交，subscription commit context {}，prefetching queue：{}";
  public static final String PIPE_LOG_SUBSCRIPTION_SUBSCRIPTION_EVENT_IS_NOT_COMMITTABLE_SUBSCRIPTION_8D03A10C =
      "Subscription：subscription event {} 不可提交，subscription commit context {}，prefetching queue：{}";
  public static final String PIPE_LOG_INCONSISTENT_CONSUMER_GROUP_WHEN_ACKING_EVENT_CURRENT_INCOMING_AEE3E90F =
      "acking event 时 consumer group 不一致，当前：{}，传入：{}，consumer id：{}，event commit context：{}，"
          + "prefetching queue：{}，仍然提交。";
  public static final String PIPE_LOG_SUBSCRIPTION_SUBSCRIPTION_COMMIT_CONTEXT_DOES_NOT_EXIST_DE907E05 =
      "Subscription：subscription commit context [{}] 不存在，可能已提交或发生了意外情况，prefetching queue：{}";
  public static final String PIPE_LOG_INCONSISTENT_CONSUMER_GROUP_WHEN_NACKING_EVENT_CURRENT_INCOMING_B0104C41 =
      "nacking event 时 consumer group 不一致，当前：{}，传入：{}，consumer id：{}，event commit context：{}，"
          + "prefetching queue：{}，仍然提交。";
  public static final String PIPE_LOG_SUBSCRIPTION_SUBSCRIPTIONPREFETCHINGQUEUE_RECYCLE_EVENT_7B120BC3 =
      "Subscription：SubscriptionPrefetchingQueue {} 回收处理中事件 {}，执行 nack 并重新放入 prefetching queue";
  public static final String PIPE_LOG_SUBSCRIPTION_POISON_MESSAGE_DETECTED_NACKCOUNT_FORCE_ACKING_7528DD6B =
      "Subscription：检测到 poison message（nackCount={}），对事件 {} 在 prefetching queue {} 中强制执行 ack";
  public static final String PIPE_LOG_SUBSCRIPTION_POISON_MESSAGE_DETECTED_NACKCOUNT_FORCE_ACKING_D984349C =
      "Subscription：检测到 poison message（nackCount={}），对 eagerly pollable event {} 在 prefetching "
          + "queue {} 中强制执行 ack";
  public static final String PIPE_LOG_SUBSCRIPTION_POISON_MESSAGE_DETECTED_NACKCOUNT_FORCE_ACKING_FEF0F0BF =
      "Subscription：检测到 poison message（nackCount={}），对 pollable event {} 在 prefetching queue {} "
          + "中强制执行 ack";
  public static final String PIPE_LOG_SUBSCRIPTION_UNKNOWN_PIPESUBSCRIBEREQUESTVERSION_RESPONSE_56E5D93F =
      "Subscription：未知的 PipeSubscribeRequestVersion，响应状态 = {}。";
  public static final String PIPE_LOG_THE_SUBSCRIPTION_REQUEST_VERSION_IS_DIFFERENT_FROM_THE_CLIENT_324A125F =
      "subscription 请求版本 {} 与客户端请求版本 {} 不同，receiver 将重置为客户端请求版本。";
  public static final String PIPE_LOG_SUBSCRIPTION_CONSENSUS_IS_A_NO_OP_ON_THIS_DATANODE_BECAUSE_28F7E92B =
      "Subscription：consensus {} 在该 DataNode 上为空操作，因为本地 queue 不存在，consumerGroup={}，topic={}";
  public static final String PIPE_LOG_SUBSCRIPTIONBROKERAGENT_REFRESHING_CONSENSUS_QUEUE_ORDER_1886704D =
      "SubscriptionBrokerAgent：将 topic [{}] 的 consensus queue order-mode 刷新为 [{}]";
  public static final String PIPE_LOG_SUBSCRIPTION_UNBOUND_CONSENSUS_PREFETCHING_QUEUE_S_FOR_REMOVED_AC018742 =
      "Subscription：已解绑 {} 个已移除 Region [{}] 的 consensus prefetching queue";
  public static final String PIPE_LOG_SUBSCRIPTIONBROKERAGENT_SETACTIVEFORREGION_REGIONID_ACTIVE_4AC3A2CB =
      "SubscriptionBrokerAgent：setActiveForRegion regionId={}，active={}";
  public static final String PIPE_LOG_SUBSCRIPTIONBROKERAGENT_SETACTIVEWRITERSFORREGION_REGIONID_48B39B3E =
      "SubscriptionBrokerAgent：setActiveWritersForRegion regionId={}，activeWriterNodeIds={}";
  public static final String PIPE_LOG_SUBSCRIPTIONBROKERAGENT_APPLYRUNTIMESTATEFORREGION_REGIONID_6D8C37A1 =
      "SubscriptionBrokerAgent：applyRuntimeStateForRegion regionId={}，runtimeState={}";
  public static final String PIPE_LOG_SUBSCRIPTION_FAILED_TO_PARSE_CONSENSUS_REGION_ID_FOR_COMMITTED_9F1A50EB =
      "Subscription：解析 committed progress 的 consensus Region id {} 失败，topic={}，consumerGroup={}";
  public static final String PIPE_LOG_SUBSCRIPTION_CONSENSUS_BROKER_BOUND_TO_CONSUMER_GROUP_DOES_E46FCDD9 =
      "Subscription：绑定到 consumer group [{}] 的 consensus broker 不存在";
  public static final String PIPE_LOG_SUBSCRIPTION_PIPE_BROKER_BOUND_TO_CONSUMER_GROUP_DOES_NOT_E9B60B22 =
      "Subscription：绑定到 consumer group [{}] 的 pipe broker 不存在";
  public static final String PIPE_LOG_SUBSCRIPTION_BROKER_BOUND_TO_CONSUMER_GROUP_DOES_NOT_EXIST_74CAD5BE =
      "Subscription：绑定到 consumer group [{}] 的 broker 不存在";
  public static final String PIPE_LOG_SUBSCRIPTION_CONSUMER_GROUP_META_CHANGE_DETECTED_TOPICSUNSUBBYGROUP_F6DAF20A =
      "Subscription：检测到 consumer group [{}] meta 变更，topicsUnsubByGroup={}，newlySubscribedTopics={}";
  public static final String PIPE_LOG_EXCEPTION_OCCURRED_WHEN_HANDLING_SINGLE_CONSUMER_GROUP_META_10E7688C =
      "处理 consumer group {} 的单个 consumer group meta 变更时发生异常";
  public static final String PIPE_LOG_SUBSCRIPTION_BROKER_BOUND_TO_CONSUMER_GROUP_HAS_ALREADY_0F37997F =
      "Subscription：绑定到 consumer group [{}] 的 broker 已存在，本地 agent {} 上 consumer group meta 的创建时间与 "
          + "coordinator {} 的 meta 不一致，删除该 broker";
  public static final String PIPE_LOG_SUBSCRIPTION_BROKER_BOUND_TO_CONSUMER_GROUP_DOES_NOT_EXISTED_9F09E4DE =
      "Subscription：绑定到 consumer group [{}] 的 broker 不存在，但对应 consumer group meta 已存在于本地 agent，忽略该情况";
  public static final String PIPE_LOG_EXCEPTION_OCCURRED_WHEN_HANDLING_SINGLE_TOPIC_META_CHANGES_43434FC4 =
      "处理 topic {} 的单个 topic meta 变更时发生异常";
  public static final String PIPE_LOG_PULLED_TOPIC_META_FROM_CONFIG_NODE_RECOVERING_5C4B1AEE =
      "已从 ConfigNode 拉取 topic meta：{}，正在恢复……";
  public static final String PIPE_LOG_INTERRUPTED_WHILE_SLEEPING_WILL_RETRY_TO_GET_TOPIC_META_976E4BE2 =
      "休眠期间被中断，将重试从 ConfigNode 获取 topic meta。";
  public static final String PIPE_LOG_PULLED_CONSUMER_GROUP_META_FROM_CONFIG_NODE_RECOVERING_A85B948F =
      "已从 ConfigNode 拉取 consumer group meta：{}，正在恢复……";
  public static final String PIPE_LOG_INTERRUPTED_WHILE_SLEEPING_WILL_RETRY_TO_GET_CONSUMER_GROUP_7E161F39 =
      "休眠期间被中断，将重试从 ConfigNode 获取 consumer group meta。";
  public static final String PIPE_LOG_FAILED_TO_GET_TOPIC_META_FROM_CONFIG_NODE_FOR_TIMES_WILL_E8D0B7F8 =
      "从 ConfigNode 获取 topic meta 已失败 {} 次，最多将重试 {} 次。";
  public static final String PIPE_LOG_FAILED_TO_GET_CONSUMER_GROUP_META_FROM_CONFIG_NODE_FOR_TIMES_3E4C727C =
      "从 ConfigNode 获取 consumer group meta 已失败 {} 次，最多将重试 {} 次。";
  public static final String PIPE_LOG_SUBSCRIPTION_CONSUMER_REFRESHED_OF_PROCESSOR_BUFFERED_COMMIT_8C7A352A =
      "Subscription：consumer {} 已刷新 {} 个 processor-buffered commit context lease，共 {} 个";
  public static final String PIPE_LOG_SUBSCRIPTION_CONSUMER_POLL_SUCCESSFULLY_WITH_REQUEST_6BC8BFED =
      "Subscription：consumer {} poll {} 成功，请求：{}";
  public static final String PIPE_LOG_SUBSCRIPTION_CONSUMER_COMMIT_NACK_FULL_COMMIT_CONTEXTS_CFC18359 =
      "Subscription：consumer {} commit（nack：{}）完整 commit context：{}";
  public static final String PIPE_LOG_SUBSCRIPTION_CONSUMER_COMMIT_NACK_FULL_REQUESTED_COMMIT_1E67E8A3 =
      "Subscription：consumer {} commit（nack：{}）完整请求 commit context：{}，完整接受 commit context：{}，"
          + "完整过期已取消订阅 commit context：{}";
  public static final String PIPE_LOG_SUBSCRIPTION_REMOVE_CONSUMER_CONFIG_WHEN_HANDLING_EXIT_3827D0E8 =
      "Subscription：处理退出时移除 consumer config {}";
  public static final String PIPE_LOG_SUBSCRIPTION_CONSUMER_IS_INACTIVE_FOR_MS_EXCEEDING_TIMEOUT_36E06B11 =
      "Subscription：consumer {} 已非活跃 {} ms，超过超时阈值 {} ms，在服务端关闭该 consumer。";
  public static final String PIPE_LOG_SUBSCRIPTION_THE_CONSUMER_HAS_ALREADY_EXISTED_WHEN_HANDSHAKING_3761AD81 =
      "Subscription：握手时 consumer {} 已存在，跳过 consumer 创建。";
  public static final String PIPE_LOG_SUBSCRIPTION_CONSUMER_HANDSHAKE_SUCCESSFULLY_DATA_NODE_ID_58DA6A5F =
      "Subscription：consumer {} 握手成功，data node id：{}";
  public static final String PIPE_LOG_SUBSCRIPTION_CONSUMER_UNSUBSCRIBE_SUCCESSFULLY_AA5E0AA9 =
      "Subscription：consumer {} 取消订阅 {} 成功";
  public static final String PIPE_LOG_SUBSCRIPTION_CONSUMER_COMMIT_NACK_ACCEPTED_SUCCESSFULLY_58D1C111 =
      "Subscription：consumer {} commit（nack：{}）accepted 成功，summary：{}";
  public static final String PIPE_LOG_SUBSCRIPTION_CONSUMER_SEEK_TOPIC_TO_TOPICPROGRESS_REGIONCOUNT_41702313 =
      "Subscription：consumer {} 将 topic {} seek 到 topicProgress（regionCount={}）";
  public static final String PIPE_LOG_SUBSCRIPTION_CONSUMER_SEEKAFTER_TOPIC_TO_TOPICPROGRESS_REGIONCOUNT_838584F8 =
      "Subscription：consumer {} 将 topic {} seekAfter 到 topicProgress（regionCount={}）";
  public static final String PIPE_LOG_SUBSCRIPTION_CONSUMER_SEEK_TOPIC_WITH_SEEKTYPE_799FF449 =
      "Subscription：consumer {} 对 topic {} 使用 seekType={} 执行 seek";
  public static final String PIPE_LOG_SUBSCRIPTION_UNSUBSCRIBE_ALL_SUBSCRIBED_TOPICS_BEFORE_CLOSE_BFB787AE =
      "Subscription：取消订阅全部已订阅 topic {}，然后关闭 consumer {}";
  public static final String PIPE_LOG_SUBSCRIPTION_THE_CONSUMER_DOES_NOT_EXISTED_WHEN_CLOSING_CCB63DCB =
      "Subscription：关闭时 consumer {} 不存在，跳过删除 consumer。";
  public static final String PIPE_LOG_SUBSCRIPTION_CONSUMER_UNSUBSCRIBE_COMPLETED_TOPICS_SUCCESSFULLY_44BAFF55 =
      "Subscription：consumer {} 取消订阅 {}（已完成 topic）成功";
  public static final String PIPE_LOG_SUBSCRIPTION_FAILED_TO_CLOSE_TIMED_OUT_CONSUMER_AFTER_MS_89CC11F1 =
      "Subscription：consumer {} 非活跃 {} ms 后，关闭超时 consumer 失败";
  public static final String PIPE_LOG_SUBSCRIPTION_DETECT_STALE_CONSUMER_CONFIG_WHEN_HANDSHAKING_B0196DB8 =
      "Subscription：握手时检测到过期 consumer config，将清理过期 consumer config {}，并将 consumer config 设置为传入的 "
          + "consumer config {}。";
  public static final String PIPE_LOG_SUBSCRIPTION_MISSING_CONSUMER_CONFIG_WHEN_HANDLING_HEARTBEAT_B9EFB1CC =
      "Subscription：处理心跳请求时缺少 consumer config：{}";
  public static final String PIPE_LOG_EXCEPTION_OCCURRED_WHEN_FETCH_ENDPOINTS_FOR_CONSUMER_IN_325B571A =
      "在 ConfigNode 中获取 consumer {} 的 endpoints 时发生异常";
  public static final String PIPE_LOG_SUBSCRIPTION_MISSING_CONSUMER_CONFIG_WHEN_HANDLING_PIPESUBSCRIBESUBSCRIBEREQ_DF466A30 =
      "Subscription：处理 PipeSubscribeSubscribeReq 时缺少 consumer config：{}";
  public static final String PIPE_LOG_SUBSCRIPTION_MISSING_CONSUMER_CONFIG_WHEN_HANDLING_PIPESUBSCRIBEUNSUBSCRIBEREQ_673CE701 =
      "Subscription：处理 PipeSubscribeUnsubscribeReq 时缺少 consumer config：{}";
  public static final String PIPE_LOG_SUBSCRIPTION_MISSING_CONSUMER_CONFIG_WHEN_HANDLING_PIPESUBSCRIBEPOLLREQ_6BB9292B =
      "Subscription：处理 PipeSubscribePollReq 时缺少 consumer config：{}";
  public static final String PIPE_LOG_SUBSCRIPTION_CONSUMER_POLL_NULL_RESPONSE_FOR_EVENT_OUTDATED_4CF7FAAA =
      "Subscription：consumer {} 针对事件 {} poll 到 null 响应（outdated：{}），请求：{}";
  public static final String PIPE_LOG_SUBSCRIPTION_CONSUMER_POLL_FOR_EVENT_OUTDATED_FAILED_WITH_0BEFF244 =
      "Subscription：consumer {} poll {} 失败，event={}（outdated：{}），请求：{}";
  public static final String PIPE_LOG_SUBSCRIPTION_MISSING_CONSUMER_CONFIG_WHEN_HANDLING_PIPESUBSCRIBECOMMITREQ_76B28EBB =
      "Subscription：处理 PipeSubscribeCommitReq 时缺少 consumer config：{}";
  public static final String PIPE_LOG_SUBSCRIPTION_CONSUMER_COMMIT_NACK_PARTIALLY_ACCEPTED_REQUESTED_87D0C038 =
      "Subscription：consumer {} commit（nack：{}）部分 accepted，请求 summary：{}，accepted summary：{}，"
          + "过期已取消订阅 summary：{}";
  public static final String PIPE_LOG_SUBSCRIPTION_MISSING_CONSUMER_CONFIG_WHEN_HANDLING_PIPESUBSCRIBECLOSEREQ_717660F8 =
      "Subscription：处理 PipeSubscribeCloseReq 时缺少 consumer config：{}";
  public static final String PIPE_LOG_EXCEPTION_OCCURRED_WHEN_SEEKING_WITH_REQUEST_6B581543 =
      "使用请求 {} 执行 seek 时发生异常";
  public static final String PIPE_LOG_SUBSCRIPTION_MISSING_CONSUMER_CONFIG_WHEN_HANDLING_SUBSCRIPTION_B85D47A4 =
      "Subscription：处理 subscription seek 请求时缺少 consumer config：{}";
  public static final String PIPE_LOG_UNEXPECTED_STATUS_CODE_WHEN_CREATING_CONSUMER_IN_CONFIG_5D2E1B97 =
      "收到非预期状态码 {}，在 ConfigNode 中创建 consumer {} 时";
  public static final String PIPE_LOG_UNEXPECTED_STATUS_CODE_WHEN_CLOSING_CONSUMER_IN_CONFIG_NODE_0C2E0CE6 =
      "收到非预期状态码 {}，在 ConfigNode 中关闭 consumer {} 时";
  public static final String PIPE_LOG_UNEXPECTED_STATUS_CODE_WHEN_SUBSCRIBING_TOPICS_FOR_CONSUMER_8676DA8A =
      "收到非预期状态码 {}，在 ConfigNode 中订阅 topic {} 给 consumer {} 时";
  public static final String PIPE_LOG_EXCEPTION_OCCURRED_WHEN_SUBSCRIBING_TOPICS_FOR_CONSUMER_E5D72F10 =
      "在 ConfigNode 中订阅 topic {} 给 consumer {} 时发生异常";
  public static final String PIPE_LOG_UNEXPECTED_STATUS_CODE_WHEN_UNSUBSCRIBING_TOPICS_FOR_CONSUMER_EFC771F0 =
      "收到非预期状态码 {}，在 ConfigNode 中为 topic {} 取消 consumer {} 的订阅时";
  public static final String PIPE_LOG_EXCEPTION_OCCURRED_WHEN_UNSUBSCRIBING_TOPICS_FOR_CONSUMER_FE4B3CEE =
      "在 ConfigNode 中为 topic {} 取消 consumer {} 的订阅时发生异常";
  public static final String PIPE_LOG_SUBSCRIPTION_CONSUMER_POLL_EXCESSIVE_PAYLOAD_FOR_EVENT_OUTDATED_2BFF690B =
      "Subscription：consumer {} poll 到过大的 payload {}，event={}（outdated：{}），请求：{}，参数配置或 payload "
          + "控制可能出现意外情况……";
  public static final String PIPE_LOG_FAILED_TO_UNBIND_FROM_SUBSCRIPTION_PREFETCHING_QUEUE_METRICS_6614388C =
      "解绑 subscription prefetching queue metrics 失败，prefetching queue map 非空";
  public static final String PIPE_LOG_FAILED_TO_DEREGISTER_SUBSCRIPTION_PREFETCHING_QUEUE_METRICS_F08479A7 =
      "注销 subscription prefetching queue metrics 失败，SubscriptionPrefetchingQueue({}) 不存在";
  public static final String PIPE_LOG_FAILED_TO_MARK_TRANSFER_EVENT_RATE_SUBSCRIPTIONPREFETCHINGQUEUE_7DEF95B5 =
      "标记传输事件速率失败，SubscriptionPrefetchingQueue({}) 不存在";
  public static final String PIPE_LOG_FAILED_TO_UNBIND_FROM_CONSENSUS_SUBSCRIPTION_PREFETCHING_A8F920D9 =
      "解绑 consensus subscription prefetching queue metrics 失败，queue map 非空";
  public static final String PIPE_LOG_FAILED_TO_DEREGISTER_CONSENSUS_SUBSCRIPTION_PREFETCHING_8B180091 =
      "注销 consensus subscription prefetching queue metrics 失败，ConsensusPrefetchingQueue({}) 不存在";
  public static final String PIPE_LOG_FAILED_TO_MARK_TRANSFER_EVENT_RATE_CONSENSUSPREFETCHINGQUEUE_FE9B91C3 =
      "标记传输事件速率失败，ConsensusPrefetchingQueue({}) 不存在";
  public static final String PIPE_LOG_SUBSCRIPTIONEVENTTSFILERESPONSE_IS_EMPTY_WHEN_FETCHING_NEXT_DFD60DF1 =
      "获取下一响应时 SubscriptionEventTsFileResponse {} 为空（不变量被破坏）";
  public static final String PIPE_LOG_SUBSCRIPTIONEVENTTSFILERESPONSE_IS_NOT_EMPTY_WHEN_INITIALIZING_C9DE83C9 =
      "初始化时 SubscriptionEventTsFileResponse {} 非空（不变量被破坏）";
  public static final String PIPE_LOG_SUBSCRIPTIONEVENTTSFILERESPONSE_IS_EMPTY_WHEN_GENERATING_B8D03E93 =
      "生成下一响应时 SubscriptionEventTsFileResponse {} 为空（不变量被破坏）";
  public static final String PIPE_LOG_SUBSCRIPTIONEVENTTABLETRESPONSE_WAIT_FOR_RESOURCE_ENOUGH_9926289F =
      "SubscriptionEventTabletResponse {} 等待足够资源以解析 tablets {} 秒。";
  public static final String PIPE_LOG_SUBSCRIPTIONEVENTTABLETRESPONSE_IS_EMPTY_WHEN_FETCHING_NEXT_4464E3F2 =
      "获取下一响应时 SubscriptionEventTabletResponse {} 为空（不变量被破坏）";
  public static final String PIPE_LOG_SUBSCRIPTIONEVENTTABLETRESPONSE_IS_NOT_EMPTY_WHEN_INITIALIZING_88F075C9 =
      "初始化时 SubscriptionEventTabletResponse {} 非空（不变量被破坏）";
  public static final String PIPE_LOG_DETECT_LARGE_TABLETS_WITH_BYTE_S_CURRENT_TABLETS_SIZE_BYTE_4D472E38 =
      "检测到大 tablets，大小 {} byte(s)，当前 tablets 大小 {} byte(s)";
  public static final String PIPE_LOG_SUBSCRIPTIONEVENTBINARYCACHE_ALLOCATEDMEMORYBLOCK_HAS_SHRUNK_08F23ADE =
      "SubscriptionEventBinaryCache.allocatedMemoryBlock 已从 {} 缩小到 {}。";
  public static final String PIPE_LOG_SUBSCRIPTIONEVENTBINARYCACHE_ALLOCATEDMEMORYBLOCK_HAS_EXPANDED_52A971D9 =
      "SubscriptionEventBinaryCache.allocatedMemoryBlock 已从 {} 扩大到 {}。";
  public static final String PIPE_LOG_SUBSCRIPTIONEVENTBINARYCACHE_RAISED_AN_EXCEPTION_WHILE_SERIALIZING_F3B698CB =
      "SubscriptionEventBinaryCache 序列化 CachedSubscriptionPollResponse 时抛出异常：{}";
  public static final String PIPE_LOG_SUBSCRIPTION_SOMETHING_UNEXPECTED_HAPPENED_WHEN_SERIALIZING_5467B7B6 =
      "Subscription：序列化 CachedSubscriptionPollResponse 时发生意外情况：{}";
  public static final String PIPE_LOG_HAS_BEEN_ITERATED_TIMES_CURRENT_TSFILEINSERTIONEVENT_0939C298 =
      "{} 已被迭代 {} 次，当前 TsFileInsertionEvent {}";
  public static final String PIPE_LOG_SUBSCRIPTIONPIPETABLETEVENTBATCH_ONLY_SUPPORT_CONVERT_PIPEINSERTNODETABLETINSERTIONEVENT_B888B8AA =
      "SubscriptionPipeTabletEventBatch {} 仅支持将 PipeInsertNodeTabletInsertionEvent 或 "
          + "PipeRawTabletInsertionEvent 转换为 tablet。忽略 {}。";
  public static final String PIPE_LOG_SUBSCRIPTIONPIPETABLETEVENTBATCH_UNEXPECTED_TABLET_INSERTION_8FB1B507 =
      "SubscriptionPipeTabletEventBatch：非预期 tablet insertion event {}，跳过该事件。";
  public static final String PIPE_LOG_SUBSCRIPTIONPIPETABLETEVENTBATCH_FAILED_TO_INCREASE_THE_595722D8 =
      "SubscriptionPipeTabletEventBatch：增加事件 {} 的引用计数失败，跳过该事件。";
  public static final String PIPE_LOG_SUBSCRIPTIONPIPETABLETEVENTBATCH_OVERRIDE_NON_NULL_CURRENTTABLETINSERTIONEVENTSITERATOR_2633B158 =
      "SubscriptionPipeTabletEventBatch {} 迭代时覆盖非 null 的 "
          + "currentTabletInsertionEventsIterator（不变量被破坏）。";
  public static final String PIPE_LOG_SUBSCRIPTIONPIPETABLETEVENTBATCH_IGNORE_ENRICHEDEVENT_WHEN_E6BAEACE =
      "SubscriptionPipeTabletEventBatch {} 迭代时忽略 EnrichedEvent {}（不变量被破坏）。";
  public static final String PIPE_LOG_SUBSCRIPTIONPIPETSFILEEVENTBATCH_IGNORE_TSFILEINSERTIONEVENT_88189024 =
      "SubscriptionPipeTsFileEventBatch {} 批处理时忽略 TsFileInsertionEvent {}。";
  public static final String PIPE_LOG_SUBSCRIPTIONPIPEEVENTBATCH_IGNORE_ENRICHEDEVENT_WHEN_BATCHING_E69BE90D =
      "SubscriptionPipeEventBatch {} 批处理时忽略 EnrichedEvent {}。";
  public static final String PIPE_LOG_CONSENSUS_PREFETCH_EXECUTOR_IS_SHUTDOWN_SKIP_REGISTERING_83E36171 =
      "Consensus prefetch executor 已关闭，跳过注册 {}";
  public static final String PIPE_LOG_CONSENSUS_PREFETCH_SUBTASK_IS_ALREADY_REGISTERED_419FE7AD =
      "Consensus prefetch subtask {} 已注册";
  public static final String PIPE_LOG_CONSENSUS_PREFETCH_WORKER_LOOP_EXITS_ABNORMALLY_531EE564 =
      "Consensus prefetch worker loop 异常退出";
  public static final String PIPE_LOG_FAILED_TO_CLOSE_SINK_AFTER_FAILED_TO_INITIALIZE_SINK_IGNORE_CF2E3D90 =
      "sink 初始化失败后关闭 sink 失败。忽略该异常。";
  public static final String PIPE_LOG_CONSENSUSPREFETCHSUBTASK_UNEXPECTED_ERROR_WHILE_DRIVING_D361F4C2 =
      "ConsensusPrefetchSubtask {}：驱动 queue {} 时发生非预期错误";
  public static final String PIPE_LOG_SUBSCRIPTIONSINKSUBTASK_FOR_CONSENSUS_TOPIC_FAILED_UNEXPECTEDLY_FC41B565 =
      "consensus topic [{}] 的 SubscriptionSinkSubtask 意外失败，跳过自动重新提交";
  public static final String PIPE_LOG_FAILED_TO_BROADCAST_SUBSCRIPTION_PROGRESS_TO_DATANODE_AT_7024F5B2 =
      "向 DataNode {} 广播 subscription progress 失败，地址 {}：{}";
  public static final String PIPE_LOG_FAILED_TO_BROADCAST_SUBSCRIPTION_PROGRESS_FOR_REGION_DE9074BD =
      "广播 Region {} 的 subscription progress 失败：{}";
  public static final String PIPE_LOG_RECEIVED_SUBSCRIPTION_PROGRESS_BROADCAST_CONSUMERGROUPID_CDAEF839 =
      "收到 subscription progress 广播：consumerGroupId={}，topicName={}，regionId={}，physicalTime={}，"
          + "localSeq={}";
  public static final String PIPE_LOG_CONSENSUSSUBSCRIPTIONCOMMITSTATE_IDEMPOTENT_RE_COMMIT_FOR_30464FC4 =
      "ConsensusSubscriptionCommitState：幂等重新提交 ({},{},{})";
  public static final String PIPE_LOG_CONSENSUSSUBSCRIPTIONCOMMITSTATE_IDEMPOTENT_DIRECT_COMMIT_B093AC01 =
      "ConsensusSubscriptionCommitState：幂等直接提交 ({},{},{})";
  public static final String PIPE_LOG_CONSENSUSSUBSCRIPTIONCOMMITMANAGER_RECOVERED_COMMITTEDREGIONPROGRESS_F6B92C6B =
      "ConsensusSubscriptionCommitManager：已从 ConfigNode 恢复 committedRegionProgress={}，"
          + "consumerGroupId={}，topicName={}，regionId={}";
  public static final String PIPE_LOG_CONSENSUSSUBSCRIPTIONCOMMITMANAGER_CANNOT_COMMIT_FOR_UNKNOWN_751BD2A9 =
      "ConsensusSubscriptionCommitManager：无法提交未知状态，consumerGroupId={}，topicName={}，regionId={}，"
          + "writerId={}，writerProgress={}";
  public static final String PIPE_LOG_CONSENSUSSUBSCRIPTIONCOMMITMANAGER_CANNOT_DIRECT_COMMIT_D6AD7D96 =
      "ConsensusSubscriptionCommitManager：无法直接提交未知状态，consumerGroupId={}，topicName={}，regionId={}，"
          + "writerId={}，writerProgress={}";
  public static final String PIPE_LOG_CONSENSUSSUBSCRIPTIONCOMMITMANAGER_CANNOT_RESET_UNKNOWN_C469052F =
      "ConsensusSubscriptionCommitManager：无法重置未知状态，consumerGroupId={}，topicName={}，regionId={}";
  public static final String PIPE_LOG_CONSENSUSSUBSCRIPTIONCOMMITMANAGER_IGNORE_BROADCAST_WITHOUT_211DE477 =
      "ConsensusSubscriptionCommitManager：忽略缺少 writer 标识的广播，consumerGroupId={}，topicName={}，"
          + "regionId={}，writerId={}，writerProgress={}";
  public static final String PIPE_LOG_SKIP_MALFORMED_CONSENSUS_SUBSCRIPTION_PROGRESS_FILE_NAME_BB4D75F0 =
      "跳过格式错误的 consensus subscription progress 文件名 {}";
  public static final String PIPE_LOG_FAILED_TO_RECOVER_CONSENSUS_SUBSCRIPTION_PROGRESS_FOR_CONSUMERGROUPID_DF30716B =
      "恢复 consensus subscription progress 失败，consumerGroupId={}，topicName={}";
  public static final String PIPE_LOG_FAILED_TO_DELETE_CONSENSUS_SUBSCRIPTION_PROGRESS_FILE_51C57096 =
      "删除 consensus subscription progress 文件 {} 失败";
  public static final String PIPE_LOG_FAILED_TO_PERSIST_CONSENSUS_SUBSCRIPTION_PROGRESS_FOR_CONSUMERGROUPID_4EA71236 =
      "持久化 consensus subscription progress 失败，consumerGroupId={}，topicName={}，regionId={}";
  public static final String PIPE_LOG_FAILED_TO_REWRITE_CONSENSUS_SUBSCRIPTION_PROGRESS_FOR_CONSUMERGROUPID_8B230D50 =
      "重写 consensus subscription progress 失败，consumerGroupId={}，topicName={}";
  public static final String PIPE_LOG_CONSENSUSSUBSCRIPTIONCOMMITMANAGER_FAILED_TO_QUERY_COMMIT_31E47F21 =
      "ConsensusSubscriptionCommitManager：从 ConfigNode 查询提交进度失败，consumerGroupId={}，"
          + "topicName={}，regionId={}，状态={}";
  public static final String PIPE_LOG_CONSENSUSSUBSCRIPTIONCOMMITMANAGER_FAILED_TO_QUERY_COMMIT_16CFDCD9 =
      "ConsensusSubscriptionCommitManager：从 ConfigNode 查询提交进度失败，consumerGroupId={}，"
          + "topicName={}，regionId={}，从 0 开始";
  public static final String PIPE_LOG_FAILED_TO_SERIALIZE_COMMITTED_REGION_PROGRESS_0D8D2129 =
      "序列化 committed region progress {} 失败";
  public static final String PIPE_LOG_CONSENSUSSUBSCRIPTIONCOMMITSTATE_IGNORE_MAPPING_WITHOUT_3E66A74D =
      "ConsensusSubscriptionCommitState：忽略缺少 writer 标识的 mapping，writerId={}，writerProgress={}";
  public static final String PIPE_LOG_CONSENSUSSUBSCRIPTIONCOMMITSTATE_DUPLICATE_OUTSTANDING_MAPPING_B5B34891 =
      "ConsensusSubscriptionCommitState：slot={} 存在重复 outstanding mapping，前值={}，当前值={}";
  public static final String PIPE_LOG_CONSENSUSSUBSCRIPTIONCOMMITSTATE_OUTSTANDING_SIZE_EXCEEDS_1463BF02 =
      "ConsensusSubscriptionCommitState：outstanding size（{}）超过阈值（{}），consumers 可能未提交。"
          + "committed=({},{}), writerNodeId={}";
  public static final String PIPE_LOG_CONSENSUSSUBSCRIPTIONCOMMITSTATE_MISSING_WRITER_IDENTITY_01040357 =
      "ConsensusSubscriptionCommitState：commit 缺少 writer 标识，writerId={}，writerProgress={}";
  public static final String PIPE_LOG_CONSENSUSSUBSCRIPTIONCOMMITSTATE_UNKNOWN_KEY_FOR_COMMIT_5F699CFD =
      "ConsensusSubscriptionCommitState：commit 的 key ({},{},{}) 未知";
  public static final String PIPE_LOG_CONSENSUSSUBSCRIPTIONCOMMITSTATE_MISSING_WRITER_IDENTITY_BB10A3B1 =
      "ConsensusSubscriptionCommitState：direct commit 缺少 writer 标识，writerId={}，writerProgress={}";
  public static final String PIPE_LOG_CONSENSUSSUBSCRIPTIONCOMMITSTATE_REJECT_DIRECT_COMMIT_WITHOUT_5B975E49 =
      "ConsensusSubscriptionCommitState：拒绝 direct commit，({},{},{}) 缺少 outstanding mapping";
  public static final String PIPE_LOG_ISCONSENSUSBASEDTOPIC_CHECK_FOR_TOPIC_MODE_RESULT_19EFA0F9 =
      "isConsensusBasedTopic 检查 topic [{}]：模式={}，结果={}";
  public static final String PIPE_LOG_SET_IOTCONSENSUS_ONNEWPEERCREATED_CALLBACK_FOR_CONSENSUS_0766CE68 =
      "设置 IoTConsensus.onNewPeerCreated 回调，用于 consensus subscription 自动绑定";
  public static final String PIPE_LOG_SET_IOTCONSENSUS_ONPEERREMOVED_CALLBACK_FOR_CONSENSUS_SUBSCRIPTION_21D4D6AC =
      "设置 IoTConsensus.onPeerRemoved 回调，用于 consensus subscription 清理";
  public static final String PIPE_LOG_NEW_DATAREGION_CREATED_CHECKING_CONSUMER_GROUP_S_FOR_AUTO_787C16E9 =
      "新 DataRegion {} 已创建，正在检查 {} 个 consumer group 以执行自动绑定，currentSearchIndex={}";
  public static final String PIPE_LOG_AUTO_BINDING_CONSENSUS_QUEUE_FOR_TOPIC_IN_GROUP_TO_NEW_REGION_86F21649 =
      "为 topic [{}]、group [{}] 自动绑定 consensus queue 到新 Region {}（database={}，"
          + "tailStartSearchIndex={}，hasLocalPersistedState={}，committedRegionProgress={}，"
          + "initialRuntimeVersion={}，initialActive={}）";
  public static final String PIPE_LOG_DATAREGION_BEING_REMOVED_UNBINDING_ALL_CONSENSUS_SUBSCRIPTION_848A29F0 =
      "DataRegion {} 正在被移除，解绑全部 consensus subscription queue";
  public static final String PIPE_LOG_SETTING_UP_CONSENSUS_SUBSCRIPTIONS_FOR_CONSUMER_GROUP_TOPICS_204374A2 =
      "正在为 consumer group [{}] 设置 consensus subscription，topics={}，consensus group 总数={}";
  public static final String PIPE_LOG_SETTING_UP_CONSENSUS_QUEUE_FOR_TOPIC_ISTABLETOPIC_ORDERMODE_4F1CDC66 =
      "正在为 topic [{}] 设置 consensus queue：isTableTopic={}，orderMode={}，config={}";
  public static final String PIPE_LOG_DISCOVERED_CONSENSUS_GROUP_S_FOR_TOPIC_IN_CONSUMER_GROUP_012EE420 =
      "发现 {} 个 consensus group，topic [{}]，consumer group [{}]：{}";
  public static final String PIPE_LOG_SKIPPING_REGION_DATABASE_FOR_TABLE_TOPIC_DATABASE_KEY_2DA27A84 =
      "跳过 Region {}（database={}），table topic [{}]（DATABASE_KEY={}）";
  public static final String PIPE_LOG_BINDING_CONSENSUS_PREFETCHING_QUEUE_FOR_TOPIC_IN_CONSUMER_45239EEA =
      "将 topic [{}]、consumer group [{}] 的 consensus prefetching queue 绑定到 data region consensus "
          + "group [{}]（database={}，tailStartSearchIndex={}，hasLocalPersistedState={}，"
          + "committedRegionProgress={}，initialRuntimeVersion={}，initialActive={}）";
  public static final String PIPE_LOG_TORE_DOWN_CONSENSUS_SUBSCRIPTION_FOR_TOPIC_IN_CONSUMER_GROUP_80B84227 =
      "已拆除 topic [{}]、consumer group [{}] 的 consensus subscription";
  public static final String PIPE_LOG_CHECKING_NEW_SUBSCRIPTIONS_IN_CONSUMER_GROUP_FOR_CONSENSUS_4A56D78A =
      "正在检查 consumer group [{}] 中 consensus-based topic 的新 subscription：{}";
  public static final String PIPE_LOG_CONSENSUSSUBSCRIPTIONSETUPHANDLER_IGNORE_STALE_RUNTIME_STATE_6C36B250 =
      "ConsensusSubscriptionSetupHandler：忽略 Region {} 的过期 runtime state，incomingRuntimeVersion={}，"
          + "currentRuntimeVersion={}，runtimeState={}";
  public static final String PIPE_LOG_CONSENSUSSUBSCRIPTIONSETUPHANDLER_APPLYING_RUNTIME_STATE_1FB8937E =
      "ConsensusSubscriptionSetupHandler：应用 Region {} 的 runtime state，preferred writer {} -> {}，"
          + "runtimeVersion {} -> {}，runtimeState={}";
  public static final String PIPE_LOG_CONSENSUSSUBSCRIPTIONSETUPHANDLER_REGION_PREFERRED_WRITER_46C1A894 =
      "ConsensusSubscriptionSetupHandler：Region {} 的 preferred writer 已变更 {} -> {}，runtimeVersion "
          + "{} -> {}，runtimeState={}（route hint）";
  public static final String PIPE_LOG_FAILED_TO_CHECK_IF_TOPIC_IS_CONSENSUS_BASED_DEFAULTING_TO_ECCE1509 =
      "检查 topic [{}] 是否为 consensus-based 失败，默认设为 false";
  public static final String PIPE_LOG_SKIPPING_SETUP_OF_CONSENSUS_BASED_SUBSCRIPTIONS_FOR_CONSUMER_A7B2C812 =
      "跳过 consumer group [{}] 的 consensus-based subscription 设置，因为 mode=consensus 仅支持 "
          + "data_region_consensus_protocol_class={}，但当前配置值为 {}（运行时 consensus 实现：{}）";
  public static final String PIPE_LOG_TOPIC_CONFIG_NOT_FOUND_FOR_TOPIC_CANNOT_SET_UP_CONSENSUS_A93339CE =
      "未找到 topic [{}] 的配置，无法设置 consensus queue";
  public static final String PIPE_LOG_NO_LOCAL_IOTCONSENSUS_DATA_REGION_FOUND_FOR_TOPIC_IN_CONSUMER_6FD0600E =
      "topic [{}] 在 consumer group [{}] 中没有本地 IoTConsensus data region。匹配的 data region 可用后将设置 "
          + "consensus subscription。";
  public static final String PIPE_LOG_FAILED_TO_TEAR_DOWN_CONSENSUS_SUBSCRIPTION_FOR_TOPIC_IN_F59E8B7C =
      "拆除 topic [{}]、consumer group [{}] 的 consensus subscription 失败";
  public static final String PIPE_LOG_FAILED_TO_AUTO_BIND_TOPIC_IN_GROUP_TO_NEW_REGION_5BFD0E7D =
      "将 topic [{}]、group [{}] 自动绑定到新 Region {} 失败";
  public static final String PIPE_LOG_FAILED_TO_UNBIND_CONSENSUS_SUBSCRIPTION_QUEUES_FOR_REMOVED_7086F70A =
      "解绑已移除 Region {} 的 consensus subscription queue 失败";
  public static final String PIPE_LOG_FAILED_TO_SET_UP_CONSENSUS_SUBSCRIPTION_FOR_TOPIC_IN_CONSUMER_1A30001B =
      "为 topic [{}]、consumer group [{}] 设置 consensus subscription 失败";
  public static final String PIPE_LOG_CONSENSUSLOGTOTABLETCONVERTER_DESERIALIZED_MERGED_INSERTNODE_51FB8295 =
      "ConsensusLogToTabletConverter：已反序列化合并的 InsertNode，searchIndex={}，type={}，deviceId={}，"
          + "searchNodeCount={}";
  public static final String PIPE_LOG_CONSENSUSLOGTOTABLETCONVERTER_SEARCHINDEX_CONTAINS_NON_INSERTNODE_CFA9FA49 =
      "ConsensusLogToTabletConverter：searchIndex={} 包含非 InsertNode PlanNode：{}";
  public static final String PIPE_LOG_CONSENSUSLOGTOTABLETCONVERTER_CONVERTING_INSERTNODE_TYPE_B80428A0 =
      "ConsensusLogToTabletConverter：正在转换 InsertNode，type={}，deviceId={}";
  public static final String PIPE_LOG_UNSUPPORTED_INSERTNODE_TYPE_FOR_SUBSCRIPTION_E488EF74 =
      "不支持用于 subscription 的 InsertNode 类型：{}";
  public static final String PIPE_LOG_CONSENSUSLOGTOTABLETCONVERTER_FAILED_TO_DESERIALIZE_ICONSENSUSREQUEST_EC1F6BAD =
      "ConsensusLogToTabletConverter：反序列化 IConsensusRequest 失败（type={}），searchIndex={}：{}";
  public static final String PIPE_LOG_INSERTNODE_TYPE_IS_NULL_SKIPPING_CONVERSION_A2F1ADF7 =
      "InsertNode 类型为 null，跳过转换";
  public static final String PIPE_LOG_UNSUPPORTED_DATA_TYPE_C8929F11 =
      "不支持的数据类型：{}";
  public static final String PIPE_LOG_UNSUPPORTED_DATA_TYPE_FOR_COPY_8AD25FE7 =
      "copy 不支持的数据类型：{}";
  public static final String PIPE_LOG_CONSENSUSPREFETCHINGQUEUE_PREFETCHING_QUEUE_IS_EMPTY_FOR_22836B5E =
      "ConsensusPrefetchingQueue {}：consumerId={} 的 prefetching queue 为空，pendingEntriesSize={}，"
          + "nextExpected={}，isClosed={}，prefetchInitialized={}，subtaskScheduled={}";
  public static final String PIPE_LOG_CONSENSUSPREFETCHINGQUEUE_POLLING_QUEUE_SIZE_CONSUMERID_FCA0AAD3 =
      "ConsensusPrefetchingQueue {}：正在 poll，queue size={}，consumerId={}";
  public static final String PIPE_LOG_CONSENSUSPREFETCHINGQUEUE_DRAINED_ENTRIES_FROM_PENDINGENTRIES_2D4E0BE7 =
      "ConsensusPrefetchingQueue {}：从 pendingEntries drain 出 {} 个条目，first searchIndex={}，last "
          + "searchIndex={}，nextExpected={}，prefetchingQueueSize={}";
  public static final String PIPE_LOG_CONSENSUSPREFETCHINGQUEUE_TIME_BASED_FLUSH_TABLETS_LINGERED_10A4EBA8 =
      "ConsensusPrefetchingQueue {}：基于时间触发 flush，{} 个 tablet 滞留 {}ms（阈值={}ms）";
  public static final String PIPE_LOG_CONSENSUSPREFETCHINGQUEUE_GAP_DETECTED_EXPECTED_GOT_FILLING_70DD08B3 =
      "ConsensusPrefetchingQueue {}：检测到缺口，期望={}，实际={}。从 WAL 填充 {} 个条目。";
  public static final String PIPE_LOG_CONSENSUSPREFETCHINGQUEUE_ACCUMULATE_COMPLETE_BATCHSIZE_FA3F3B41 =
      "ConsensusPrefetchingQueue {}：累积完成，batchSize={}，processed={}，skipped={}，lingerTablets={}，"
          + "nextExpected={}";
  public static final String PIPE_LOG_CONSENSUSPREFETCHINGQUEUE_SUBSCRIPTION_WAL_READ_ENTRIES_14AA5096 =
      "ConsensusPrefetchingQueue {}：subscription WAL 读取 {} 个条目，nextExpectedSearchIndex={}";
  public static final String PIPE_LOG_CONSENSUSPREFETCHINGQUEUE_SUBSCRIPTION_WAL_EXHAUSTED_AT_E61AF763 =
      "ConsensusPrefetchingQueue {}：subscription WAL 在 {} 耗尽，当前 WAL 为 {}。滚动 WAL 文件以暴露当前文件条目。";
  public static final String PIPE_LOG_CONSENSUSPREFETCHINGQUEUE_SKIP_STALE_EVENT_WITH_SEARCHINDEX_07A09B36 =
      "ConsensusPrefetchingQueue {}：跳过过期事件，searchIndex 范围 [{}, {}]，expectedSeekGeneration={}，"
          + "currentSeekGeneration={}";
  public static final String PIPE_LOG_CONSENSUSPREFETCHINGQUEUE_ENQUEUED_EVENT_WITH_TABLETS_SEARCHINDEX_140FDDCB =
      "ConsensusPrefetchingQueue {}：已入队包含 {} 个 tablet 的事件，searchIndex 范围 [{}, {}]，"
          + "prefetchQueueSize={}";
  public static final String PIPE_LOG_CONSENSUSPREFETCHINGQUEUE_REJECT_WITHOUT_WRITER_PROGRESS_D84AA802 =
      "ConsensusPrefetchingQueue {}：拒绝缺少 writer progress 的 {}，commitContext={}";
  public static final String PIPE_LOG_CONSENSUSPREFETCHINGQUEUE_REJECT_FOR_INACTIVE_QUEUE_COMMITCONTEXT_AE6D382C =
      "ConsensusPrefetchingQueue {}：因 queue 非活跃而拒绝 {}，commitContext={}，runtimeVersion={}";
  public static final String PIPE_LOG_CONSENSUSPREFETCHINGQUEUE_RECYCLED_TIMED_OUT_EVENT_BACK_5E58639C =
      "ConsensusPrefetchingQueue {}：将超时事件 {} 回收到 prefetching queue";
  public static final String PIPE_LOG_CONSENSUSPREFETCHINGQUEUE_INJECTED_WATERMARK_WATERMARKTIMESTAMP_BF373164 =
      "ConsensusPrefetchingQueue {}：已注入 WATERMARK，watermarkTimestamp={}";
  public static final String PIPE_LOG_CONSENSUSPREFETCHINGQUEUE_CREATED_DORMANT_CONSUMERGROUPID_863BC6D6 =
      "ConsensusPrefetchingQueue 已创建（dormant）：consumerGroupId={}，topicName={}，orderMode={}，"
          + "consensusGroupId={}，fallbackCommittedRegionProgress={}，fallbackTailSearchIndex={}，"
          + "initialRuntimeVersion={}，initialActive={}";
  public static final String PIPE_LOG_CONSENSUSPREFETCHINGQUEUE_PREFETCH_INITIALIZED_STARTSEARCHINDEX_69B53EE6 =
      "ConsensusPrefetchingQueue {}：prefetch 已初始化，startSearchIndex={}，progressSource={}，"
          + "recoveryWriterCount={}";
  public static final String PIPE_LOG_CONSENSUSPREFETCHINGQUEUE_PERIODIC_STATS_LAG_PENDINGDELTA_D75375D0 =
      "ConsensusPrefetchingQueue {}：周期统计，lag={}，pendingDelta={}，walDelta={}，pendingTotal={}，"
          + "walTotal={}，pendingQueueSize={}，prefetchingQueueSize={}，inFlightEventsSize={}，"
          + "realtimeWriterCount={}，walHasNext={}，isActive={}，subtaskScheduled={}";
  public static final String PIPE_LOG_CONSENSUSPREFETCHINGQUEUE_WAITING_MS_FOR_WAL_GAP_TO_BECOME_7D91C6C5 =
      "ConsensusPrefetchingQueue {}：等待 {}ms，使 WAL 缺口 [{}, {}) 可见，currentNextExpected={}，"
          + "currentWalIndex={}，seekGeneration={}";
  public static final String PIPE_LOG_CONSENSUSPREFETCHINGQUEUE_SEEKTOREGIONPROGRESS_WRITERCOUNT_3134A29B =
      "ConsensusPrefetchingQueue {}：seekToRegionProgress writerCount={} -> {}，searchIndex={}";
  public static final String PIPE_LOG_CONSENSUSPREFETCHINGQUEUE_SEEKAFTERREGIONPROGRESS_WRITERCOUNT_C6B26D20 =
      "ConsensusPrefetchingQueue {}：seekAfterRegionProgress writerCount={} -> {}，searchIndex={}";
  public static final String PIPE_LOG_CONSENSUSPREFETCHINGQUEUE_ABORTED_PENDING_SEEK_DURING_RUNTIME_F9928604 =
      "ConsensusPrefetchingQueue {}：运行时停止期间中止待处理 seek({})，恢复 prefetchInitialized {} -> "
          + "{}，seekGeneration {} -> {}";
  public static final String PIPE_LOG_CONSENSUSPREFETCHINGQUEUE_FAILED_TO_SCHEDULE_SEEK_BECAUSE_9E407068 =
      "ConsensusPrefetchingQueue {}：调度 seek({}) 失败，原因：{}，恢复 prefetchInitialized {} -> {}，"
          + "seekGeneration {} -> {}";
  public static final String MESSAGE_THE_QUEUE_IS_CLOSING_AC6C2AB4 = "队列正在关闭";
  public static final String MESSAGE_PREFETCH_RUNTIME_IS_UNAVAILABLE_F1721E89 =
      "预取运行时不可用";
  public static final String PIPE_LOG_CONSENSUSPREFETCHINGQUEUE_SEEK_APPLIED_TO_SEARCHINDEX_WRITERCOUNT_FA2C4327 =
      "ConsensusPrefetchingQueue {}：seek({}) 已应用到 searchIndex={}，writerCount={}，seekGeneration={}";
  public static final String PIPE_LOG_CONSENSUSPREFETCHINGQUEUE_FLUSHING_LINGERING_TABLETS_DURING_4C4AF235 =
      "ConsensusPrefetchingQueue {}：关闭期间 flush {} 个滞留 tablet";
  public static final String PIPE_LOG_CONSENSUSPREFETCHINGQUEUE_ISACTIVE_SET_TO_REGION_EC0AD7BA =
      "ConsensusPrefetchingQueue {}：isActive 设置为 {}（region={}）";
  public static final String PIPE_LOG_CONSENSUSPREFETCHINGQUEUE_RUNTIMEACTIVEWRITERNODEIDS_EFFECTIVEACTIVEWRITERNODEIDS_246519D2 =
      "ConsensusPrefetchingQueue {}：runtimeActiveWriterNodeIds={}，"
          + "effectiveActiveWriterNodeIds={}（region={}，orderMode={}，preferredWriterNodeId={}）";
  public static final String PIPE_LOG_CONSENSUSPREFETCHINGQUEUE_PREFERREDWRITERNODEID_SET_TO_EFFECTIVEACTIVEWRITERNODEIDS_B08E8180 =
      "ConsensusPrefetchingQueue {}：preferredWriterNodeId 设置为 {}，"
          + "effectiveActiveWriterNodeIds={}（region={}，orderMode={}）";
  public static final String PIPE_LOG_CONSENSUSPREFETCHINGQUEUE_ORDERMODE_SET_TO_EFFECTIVEACTIVEWRITERNODEIDS_CDD3C86E =
      "ConsensusPrefetchingQueue {}：orderMode 设置为 {}，effectiveActiveWriterNodeIds={}（region={}，"
          + "preferredWriterNodeId={}，runtimeActiveWriterNodeIds={}）";
  public static final String PIPE_LOG_CONSENSUSPREFETCHINGQUEUE_APPLIED_RUNTIMEVERSION_36E05B80 =
      "ConsensusPrefetchingQueue {}：已应用 runtimeVersion {}";
  public static final String PIPE_LOG_CONSENSUSPREFETCHINGQUEUE_APPLIED_RUNTIMESTATE_PREFERREDWRITERNODEID_D845E9D6 =
      "ConsensusPrefetchingQueue {}：已应用 runtimeState={}，preferredWriterNodeId={}";
  public static final String PIPE_LOG_CONSENSUSPREFETCHINGQUEUE_POLL_COMMITTED_EVENT_BROKEN_INVARIANT_E478FA3C =
      "ConsensusPrefetchingQueue {} poll 到已提交事件 {}（不变量被破坏），移除该事件";
  public static final String PIPE_LOG_CONSENSUSPREFETCHINGQUEUE_POLL_NON_POLLABLE_EVENT_BROKEN_E9551325 =
      "ConsensusPrefetchingQueue {} poll 到不可 poll 事件 {}（不变量被破坏），执行 nack";
  public static final String PIPE_LOG_CONSENSUSPREFETCHINGQUEUE_INTERRUPTED_WHILE_POLLING_B7CFF5FD =
      "ConsensusPrefetchingQueue {} 在 polling 期间被中断";
  public static final String PIPE_LOG_CONSENSUSPREFETCHINGQUEUE_ERROR_READING_SUBSCRIPTION_WAL_A3888AC5 =
      "ConsensusPrefetchingQueue {}：读取 subscription WAL 出错";
  public static final String PIPE_LOG_CONSENSUSPREFETCHINGQUEUE_ERROR_CLOSING_SUBSCRIPTION_WAL_19711C01 =
      "ConsensusPrefetchingQueue {}：关闭 subscription WAL iterator 出错";
  public static final String PIPE_LOG_CONSENSUSPREFETCHINGQUEUE_COMMIT_CONTEXT_DOES_NOT_EXIST_99B8A8F3 =
      "ConsensusPrefetchingQueue {}：ack 的 commit context {} 不存在";
  public static final String PIPE_LOG_CONSENSUSPREFETCHINGQUEUE_EVENT_ALREADY_COMMITTED_AC34E829 =
      "ConsensusPrefetchingQueue {}：事件 {} 已提交";
  public static final String PIPE_LOG_CONSENSUSPREFETCHINGQUEUE_FAILED_TO_ADVANCE_COMMIT_FRONTIER_56E606C0 =
      "ConsensusPrefetchingQueue {}：推进 {} 的 commit frontier 失败";
  public static final String PIPE_LOG_CONSENSUSPREFETCHINGQUEUE_COMMIT_CONTEXT_DOES_NOT_EXIST_05F6C6E0 =
      "ConsensusPrefetchingQueue {}：nack 的 commit context {} 不存在";
  public static final String PIPE_LOG_CONSENSUSPREFETCHINGQUEUE_SEEKTOREGIONPROGRESS_NOT_SUPPORTED_85477BAB =
      "ConsensusPrefetchingQueue {}：不支持 seekToRegionProgress（没有 WAL 目录）";
  public static final String PIPE_LOG_CONSENSUSPREFETCHINGQUEUE_SEEKAFTERREGIONPROGRESS_NOT_SUPPORTED_55F36BE8 =
      "ConsensusPrefetchingQueue {}：不支持 seekAfterRegionProgress（没有 WAL 目录）";
  public static final String PIPE_LOG_CONSENSUSPREFETCHINGQUEUE_FAILED_TO_READ_WAL_METADATA_FROM_A2ED50D1 =
      "ConsensusPrefetchingQueue {}：计算 seekToEnd frontier 时，从 {} 读取 WAL metadata 失败";
  public static final String PIPE_LOG_CONSENSUSPREFETCHINGQUEUE_ERROR_DURING_DEREGISTER_34C332E7 =
      "ConsensusPrefetchingQueue {}：注销期间出错";
  public static final String PIPE_LOG_CONSENSUSPREFETCHINGQUEUE_FAILED_TO_FLUSH_LINGERING_BATCH_F97D8AA7 =
      "ConsensusPrefetchingQueue {}：关闭期间 flush 滞留 batch 失败，丢弃该 batch";
  public static final String PIPE_LOG_CONSENSUSPREFETCHINGQUEUE_PREFETCH_ROUND_FAILED_TYPE_MESSAGE_63BC909B =
      "ConsensusPrefetchingQueue {}：prefetch 轮次失败（type={}，message={}）";
  public static final String PIPE_LOG_CONSENSUSPREFETCHINGQUEUE_POISON_MESSAGE_DETECTED_NACKCOUNT_3A9255FB =
      "ConsensusPrefetchingQueue {}：检测到 poison message（nackCount={}），为避免无限重复投递，对事件 {} 强制执行 ack";
  public static final String PIPE_LOG_CONSENSUSPREFETCHINGQUEUE_POISON_MESSAGE_DETECTED_DURING_23159F02 =
      "ConsensusPrefetchingQueue {}：recycle 期间检测到 poison message（nackCount={}），对事件 {} 强制执行 ack";
  public static final String PIPE_LOG_PROGRESSWALITERATOR_FAILED_TO_OPEN_NEAR_LIVE_WAL_FILE_RETRYING_5AEB94AC =
      "ProgressWALIterator：打开 near-live WAL 文件 {} 失败，不加入黑名单并重试";
  public static final String PIPE_LOG_PROGRESSWALITERATOR_ERROR_READING_WAL_2DB46D41 =
      "ProgressWALIterator：读取 WAL 出错";
  public static final String PIPE_LOG_PROGRESSWALITERATOR_FAILED_TO_OPEN_WAL_FILE_SKIPPING_29CA1092 =
      "ProgressWALIterator：打开 WAL 文件 {} 失败，跳过该文件";
  public static final String PIPE_LOG_PIPE_TERMINATE_EVENT_COMMITTED_FOR_HISTORICAL_TRANSFER_CREATIONTIME_9B807B28 =
      "Pipe {}@{}：历史传输的终止事件已提交。creationTime：{}，shouldMark：{}。{}";
  public static final String PIPE_LOG_PIPE_HISTORICAL_SOURCE_HAS_SUPPLIED_ALL_EVENTS_EMITTING_8B58DE19 =
      "Pipe {}@{}：历史 source 已提供全部事件，正在发出终止事件。{}";
  public static final String PIPE_LOG_PIPE_REALTIME_SOURCE_ON_DATA_REGION_LISTENTOTSFILE_LISTENTOINSERTNODE_A02E1552 =
      "Pipe {}@{} {}：DataRegion {} 上的实时 source（listenToTsFile={}，listenToInsertNode={}，"
          + "registeredSourceCount={}，tsFileSourceCount={}，insertNodeSourceCount={}）。";
  public static final String PIPE_LOG_INTERRUPTED_WHILE_WAITING_FOR_IN_FLIGHT_PUBLISHES_TO_FINISH_C8E3757B =
      "关闭 DataRegion {} 上的 assigner 时，等待处理中 publish 完成期间被中断。";
  public static final String PIPE_LOG_SCHEMAREGIONSTATEMACHINE_EXECUTE_READ_PLAN_FRAGMENTINSTANCE_F85A001F =
      "SchemaRegionStateMachine[{}]：执行读 plan：FragmentInstance-{}";
  public static final String PIPE_LOG_CURRENT_NODE_NODEID_IS_NO_LONGER_THE_SCHEMA_REGION_LEADER_FD783B3C =
      "当前节点 [nodeId：{}] 不再是 schema region leader [regionId：{}]，新 leader 为 [nodeId：{}]";
  public static final String PIPE_LOG_CURRENT_NODE_NODEID_IS_NO_LONGER_THE_SCHEMA_REGION_LEADER_12E06F99 =
      "当前节点 [nodeId：{}] 不再是 schema region leader [regionId：{}]，开始清理相关服务。";
  public static final String PIPE_LOG_CURRENT_NODE_NODEID_IS_NO_LONGER_THE_SCHEMA_REGION_LEADER_3092822E =
      "当前节点 [nodeId：{}] 不再是 schema region leader [regionId：{}]，旧 leader 上的全部服务当前不可用。";
  public static final String PIPE_LOG_CURRENT_NODE_NODEID_BECOMES_SCHEMA_REGION_LEADER_REGIONID_46C70A32 =
      "当前节点 [nodeId：{}] 成为 schema region leader [regionId：{}]";
  public static final String PIPE_LOG_CURRENT_NODE_NODEID_AS_SCHEMA_REGION_LEADER_REGIONID_IS_F00BFAC5 =
      "当前节点 [nodeId：{}] 作为 schema region leader [regionId：{}] 已准备好工作";
  public static final String PIPE_LOG_SCHEMA_REGION_LISTENING_QUEUE_LISTEN_TO_SNAPSHOT_FAILED_64845A44 =
      "Schema Region Listening Queue 监听 snapshot 失败，历史数据可能无法传输。snapshotPaths:{}";
  public static final String PIPE_LOG_WRITE_OPERATION_FAILED_BECAUSE_RETRYTIME_34EFBE99 =
      "写入操作失败，原因：{}，重试次数：{}。";
  public static final String PIPE_LOG_EXCEPTION_OCCURS_WHEN_TAKING_SNAPSHOT_FOR_IN_48CBDFCC =
      "为 {}-{} 在 {} 中执行 snapshot 时发生异常";
  public static final String PIPE_LOG_MEETS_ERROR_WHEN_GETTING_SNAPSHOT_FILES_FOR_9BFA76B9 =
      "获取 {}-{} 的 snapshot 文件时出错";
  public static final String PIPE_LOG_WRITE_OPERATION_STILL_FAILED_AFTER_RETRY_TIMES_BECAUSE_15EEA702 =
      "写入操作在重试 {} 次后仍失败，原因：{}。";
  public static final String PIPE_LOG_NOW_TRY_TO_DELETE_DIRECTLY_DATABASEPATH_DELETEPATH_A427CD01 =
      "现在尝试直接删除，databasePath：{}，deletePath：{}";
  public static final String PIPE_LOG_BATCH_FAILURE_IN_EXECUTING_A_INSERTTABLETNODE_DEVICE_STARTTIME_9A5A70F6 =
      "批量执行 InsertTabletNode 失败。device：{}，startTime：{}，measurements：{}，失败状态：{}";
  public static final String PIPE_LOG_INSERT_ROW_FAILED_DEVICE_TIME_MEASUREMENTS_FAILING_STATUS_63054E8B =
      "插入行失败。device：{}，time：{}，measurements：{}，失败状态：{}";
  public static final String PIPE_LOG_INSERT_TABLET_FAILED_DEVICE_STARTTIME_MEASUREMENTS_FAILING_B409B2C4 =
      "插入 tablet 失败。device：{}，startTime：{}，measurements：{}，失败状态：{}";

  // ---------------------------------------------------------------------------
  // 补充异常消息
  // ---------------------------------------------------------------------------
  public static final String PIPE_EXCEPTION_UNSUPPORTED_SUBSCRIPTION_REQUEST_VERSION_D_1E7C211A =
      "不支持的 subscription 请求版本 %d";
  public static final String PIPE_EXCEPTION_PAYLOAD_SIZE_S_BYTE_S_WILL_EXCEED_THE_THRESHOLD_S_BYTE_S_6043B3D8 =
      "payload 大小 %s byte(s) 将超过阈值 %s byte(s)";
  public static final String PIPE_EXCEPTION_INCONSISTENT_READ_LENGTH_BROKEN_INVARIANT_EXPECTED_S_ACTUAL_9203668A =
      "读取长度不一致（不变量被破坏），期望：%s，实际：%s";
  public static final String PIPE_EXCEPTION_TIMEOUTEXCEPTION_WAITED_S_SECONDS_8B31A3A5 =
      "TimeoutException：等待 %s 秒";
  public static final String PIPE_EXCEPTION_THE_SUBSCRIPTIONCONNECTORSUBTASKMANAGER_ONLY_SUPPORTS_SUBSCRIPTION_CEFFAAA9 =
      "SubscriptionConnectorSubtaskManager 仅支持 subscription-sink。";
  public static final String PIPE_EXCEPTION_FAILED_TO_CONSTRUCT_SUBSCRIPTION_SINK_BECAUSE_OF_S_OR_S_DBA27DC2 =
      "构造 subscription sink 失败，原因：pipe connector 参数中不存在 %s 或 %s";
  public static final String PIPE_EXCEPTION_FAILED_TO_GET_PENDINGQUEUE_NO_SUCH_SUBTASK_S_B445404A =
      "获取 PendingQueue 失败。不存在该 subtask：%s";
  public static final String PIPE_EXCEPTION_INVALID_BASE64_URL_COMPONENT_LENGTH_F1F1B6BA =
      "无效的 base64 URL component 长度";
  public static final String PIPE_EXCEPTION_INVALID_CONSENSUS_SUBSCRIPTION_PROGRESS_REGION_COUNT_S_7CE4FD8E =
      "无效的 consensus subscription progress Region 数量 %s";
  public static final String PIPE_EXCEPTION_INVALID_CONSENSUS_SUBSCRIPTION_PROGRESS_PAYLOAD_LENGTH_S_8C145986 =
      "无效的 consensus subscription progress payload 长度 %s";
  public static final String PIPE_EXCEPTION_MALFORMED_CONSENSUS_SUBSCRIPTION_PROGRESS_FILE_S_83042847 =
      "格式错误的 consensus subscription progress 文件 %s";
  public static final String PIPE_EXCEPTION_ILLEGAL_S_S_72D743AA =
      "非法的 %s=%s";
  public static final String PIPE_EXCEPTION_INTERRUPTED_WHILE_WAITING_FOR_SEEK_APPLICATION_7C7ECAF2 =
      "等待 seek 应用时被中断";
  public static final String PIPE_EXCEPTION_CONSENSUSPREFETCHINGQUEUE_S_CANNOT_RECOVER_FROM_NON_EMPTY_C1B367EF =
      "ConsensusPrefetchingQueue %s：无法在没有 WAL 访问权限的情况下，从非空 Region progress 恢复：%s";
  public static final String PIPE_EXCEPTION_CONSENSUSPREFETCHINGQUEUE_S_CANNOT_INITIALIZE_REPLAY_START_E02DE40E =
      "ConsensusPrefetchingQueue %s：无法根据 region progress %s 初始化 replay 起点：%s";
  public static final String PIPE_EXCEPTION_CONSENSUSPREFETCHINGQUEUE_S_CANNOT_SEEKTOREGIONPROGRESS_2746E514 =
      "ConsensusPrefetchingQueue %s：无法执行 seekToRegionProgress %s：%s";
  public static final String PIPE_EXCEPTION_CONSENSUSPREFETCHINGQUEUE_S_CANNOT_SEEKAFTERREGIONPROGRESS_48A500C3 =
      "ConsensusPrefetchingQueue %s：无法执行 seekAfterRegionProgress %s：%s";
  public static final String PIPE_EXCEPTION_CONSENSUSPREFETCHINGQUEUE_S_IS_CLOSING_WHILE_APPLYING_SEEK_2BB2B431 =
      "ConsensusPrefetchingQueue %s 正在应用 seek 时关闭";
  public static final String PIPE_EXCEPTION_CONSENSUSPREFETCHINGQUEUE_S_RUNTIME_STOPPED_BEFORE_SEEK_7BCB4F4B =
      "ConsensusPrefetchingQueue %s 运行时在应用 seek(%s) 前已停止";
  public static final String PIPE_EXCEPTION_CONSENSUSPREFETCHINGQUEUE_S_IS_CLOSING_BEFORE_SEEK_APPLIES_F893BB02 =
      "ConsensusPrefetchingQueue %s 在 seek 应用前正在关闭";
  public static final String PIPE_EXCEPTION_NO_PRIVILEGE_FOR_SELECT_FOR_USER_S_AT_TABLE_S_S_84B0C299 =
      "用户 %s 对表 %s.%s 没有 SELECT 权限";
  public static final String PIPE_EXCEPTION_EXPECTED_BINARY_BYTE_OR_STRING_BUT_WAS_S_7976B10F =
      "期望 Binary、byte[] 或 String，实际为 %s。";
  public static final String PIPE_EXCEPTION_TIMEOUTEXCEPTION_WAITED_S_SECONDS_FOR_MEMORY_TO_PARSE_TSFILE_0E4EF8FD =
      "TimeoutException：等待 %s 秒以获取解析 TsFile 所需内存";
  public static final String PIPE_EXCEPTION_UNSUPPORTED_DATA_TYPE_S_FOR_COLUMN_S_9F870C01 =
      "数据类型 %s 不支持用于列 %s";
  public static final String PIPE_EXCEPTION_COLUMN_S_NOT_FOUND_0FA13581 =
      "未找到列 %s";
  public static final String PIPE_EXCEPTION_INSERTNODE_TYPE_S_IS_NOT_SUPPORTED_7DF82B58 =
      "不支持 InsertNode 类型 %s。";
  public static final String PIPE_EXCEPTION_DATA_TYPE_S_IS_NOT_SUPPORTED_5D5C02E4 =
      "不支持数据类型 %s。";
  public static final String PIPE_EXCEPTION_FORCEALLOCATEFORTABLET_FAILED_TO_ALLOCATE_BECAUSE_THERE_F878474D =
      "forceAllocateForTablet：分配失败，原因：tablet 占用内存过多，总内存大小 %d bytes，tablet 已用内存大小 %d bytes，请求内存大小 %d "
          + "bytes";
  public static final String PIPE_EXCEPTION_FORCEALLOCATEFORTSFILE_FAILED_TO_ALLOCATE_BECAUSE_THERE_6D614467 =
      "forceAllocateForTsFile：分配失败，原因：tsfile 占用内存过多，总内存大小 %d bytes，tsfile 已用内存大小 %d bytes，请求内存大小 %d "
          + "bytes";
  public static final String PIPE_EXCEPTION_FORCEALLOCATE_FAILED_TO_ALLOCATE_MEMORY_AFTER_D_RETRIES_44EF7AE7 =
      "forceAllocate：重试 %d 次后仍无法分配内存，总内存大小 %d bytes，已用内存大小 %d bytes，请求内存大小 %d bytes";
  public static final String PIPE_EXCEPTION_FORCERESIZE_FAILED_TO_ALLOCATE_MEMORY_AFTER_D_RETRIES_TOTAL_8C6948BC =
      "forceResize：重试 %d 次后仍无法分配内存，总内存大小 %d bytes，已用内存大小 %d bytes，请求内存大小 %d bytes";
  public static final String PIPE_EXCEPTION_FAILED_TO_GET_HARDLINK_OR_COPIED_FILE_IN_PIPE_DIR_FOR_FILE_F009D86E =
      "获取 pipe 目录中文件 %s 的 hardlink 或复制文件失败；该文件不是 tsfile、mod 文件或 resource 文件";
  public static final String PIPE_EXCEPTION_PIPEPLANTOSTATEMENTVISITOR_DOES_NOT_SUPPORT_VISITING_GENERAL_452AAA60 =
      "PipePlanToStatementVisitor 不支持访问通用 plan，PlanNode：%s";
  public static final String PIPE_EXCEPTION_AIRGAP_PAYLOAD_LENGTH_D_EXCEEDS_MAXIMUM_ALLOWED_D_CLOSING_D1712B3D =
      "AirGap payload 长度（%d）超过最大允许值（%d）。关闭来自 %s 的连接";
  public static final String PIPE_EXCEPTION_DETECTED_SUSPICIOUS_NESTED_E_LANGUAGE_PREFIX_CLOSING_CONNECTION_69C76172 =
      "检测到可疑的嵌套 E-Language 前缀。关闭来自 %s 的连接";
  public static final String PIPE_EXCEPTION_AUTO_CREATE_DATABASE_FAILED_S_STATUS_CODE_S_D8EB60FA =
      "自动创建数据库失败：%s，状态码：%s";
  public static final String PIPE_EXCEPTION_IOTCONSENSUSV2_PIPENAME_S_FAILED_TO_CREATE_RECEIVER_FILE_DD67E854 =
      "IoTConsensusV2-PipeName-%s：创建 receiver 文件目录 %s 失败。原因：父级系统目录因系统并发退出而被删除。";
  public static final String PIPE_EXCEPTION_IOTCONSENSUSV2_PIPENAME_S_FAILED_TO_CREATE_RECEIVER_FILE_5ADC430A =
      "IoTConsensusV2-PipeName-%s：创建 receiver 文件目录 %s 失败。原因可能是权限不足、目录已存在等。";
  public static final String PIPE_EXCEPTION_IOTCONSENSUSV2_PIPENAME_S_FAILED_TO_CREATE_TSFILEWRITER_85EC8DD2 =
      "IoTConsensusV2-PipeName-%s：创建 tsFileWriter-%d receiver 文件目录失败";
  public static final String PIPE_EXCEPTION_UNSUPPORTED_IOTCONSENSUSV2_REQUEST_VERSION_D_E1D94606 =
      "不支持的 iotConsensusV2 请求版本 %d";
  public static final String PIPE_EXCEPTION_CAN_NOT_EXECUTE_DELETE_STATEMENT_S_3563E8A3 =
      "无法执行删除语句：%s";
  public static final String PIPE_EXCEPTION_CAN_NOT_EXECUTE_LOAD_TSFILE_STATEMENT_S_8CC1A096 =
      "无法执行加载 TsFile 语句：%s";
  public static final String PIPE_EXCEPTION_FAILED_TO_GET_PIPE_TASK_PROGRESS_INDEX_WITH_PIPE_NAME_S_CFE9DE7C =
      "获取 pipe 任务进度索引失败，pipe 名称：%s，共识组 ID：%s。";
  public static final String PIPE_EXCEPTION_EXCEPTION_IN_PIPE_PROCESS_SUBTASK_S_LAST_EVENT_S_ROOT_CAUSE_95B49C24 =
      "pipe 处理发生异常，subtask：%s，最后一个 event：%s，根因：%s";
  public static final String PIPE_EXCEPTION_THE_VISIBILITY_OF_THE_PIPE_S_S_IS_NOT_COMPATIBLE_WITH_THE_30B8BF0A =
      "pipe（%s，%s）的可见性与 source（%s，%s，%s）、processor（%s，%s，%s）和 connector（%s，%s，%s）的可见性不兼容。";
  public static final String PIPE_EXCEPTION_DATA_TYPE_S_IS_NOT_SUPPORTED_WHEN_CONVERT_DATA_AT_CLIENT_405429CC =
      "客户端转换数据时不支持数据类型 %s";
  public static final String PIPE_EXCEPTION_HANDSHAKE_ERROR_WITH_RECEIVER_S_S_CODE_D_MESSAGE_S_4ED82649 =
      "receiver %s:%s 握手错误，code：%d，message：%s。";
  public static final String PIPE_EXCEPTION_THE_WEBSOCKET_SERVER_HAS_ALREADY_BEEN_CREATED_WITH_PORT_FFC420AE =
      "WebSocket server 已使用端口 %d 创建。请将 cdc.port 选项设置为 %d。";
  public static final String PIPE_EXCEPTION_NETWORK_ERROR_WHEN_TRANSFER_TSFILE_INSERTION_EVENT_S_703A2E9E =
      "传输 tsFile insertion event 时发生网络错误：%s。";
  public static final String PIPE_EXCEPTION_CANNOT_SEND_PIPE_DATA_TO_RECEIVER_S_S_BECAUSE_S_25143D54 =
      "无法向 receiver %s:%s 发送 pipe data，原因：%s。";
  public static final String PIPE_EXCEPTION_NETWORK_ERROR_WHEN_TRANSFER_EVENT_S_BECAUSE_S_60A63AD7 =
      "传输 event %s 时发生网络错误，原因：%s。";
  public static final String PIPE_EXCEPTION_NETWORK_ERROR_WHEN_TRANSFER_TABLET_INSERTION_EVENT_S_BECAUSE_A6F87EF5 =
      "传输 tablet insertion event %s 时发生网络错误，原因：%s。";
  public static final String PIPE_EXCEPTION_NETWORK_ERROR_WHEN_TRANSFER_TSFILE_INSERTION_EVENT_S_BECAUSE_BDE61690 =
      "传输 tsfile insertion event %s 时发生网络错误，原因：%s。";
  public static final String PIPE_EXCEPTION_NETWORK_ERROR_WHEN_TRANSFER_TSFILE_EVENT_S_BECAUSE_S_F36D2A6B =
      "传输 tsfile event %s 时发生网络错误，原因：%s。";
  public static final String PIPE_EXCEPTION_FAILED_TO_TRANSFER_TABLET_INSERTION_EVENT_S_BECAUSE_S_9710318F =
      "传输 tablet insertion event %s 失败，原因：%s。";
  public static final String PIPE_EXCEPTION_FAILED_TO_TRANSFER_TSFILE_INSERTION_EVENT_S_BECAUSE_S_21AD3263 =
      "传输 tsfile insertion event %s 失败，原因：%s。";
  public static final String PIPE_EXCEPTION_NETWORK_ERROR_WHEN_TRANSFER_FILE_S_BECAUSE_S_3C673B7A =
      "传输文件 %s 时发生网络错误，原因：%s。";
  public static final String PIPE_EXCEPTION_PARAMETERS_IN_SET_S_ARE_NOT_ALLOWED_IN_SKIPIF_AAF177AD =
      "集合 %s 中的参数不允许出现在 'skipif' 中";
  public static final String PIPE_EXCEPTION_FAILED_TO_CHECK_PASSWORD_FOR_PIPE_S_0B1A5C73 =
      "检查 pipe %s 的密码失败。";
  public static final String PIPE_EXCEPTION_NETWORK_ERROR_WHEN_TRANSFER_DELETION_S_BECAUSE_S_3B250B4B =
      "传输 deletion %s 时发生网络错误，原因：%s。";
  public static final String PIPE_EXCEPTION_NETWORK_ERROR_WHEN_TRANSFER_TABLET_BATCH_BECAUSE_S_6BEC52E7 =
      "传输 tablet batch 时发生网络错误，原因：%s。";
  public static final String PIPE_EXCEPTION_NETWORK_ERROR_WHEN_TRANSFER_INSERT_NODE_TABLET_INSERTION_D993C7AB =
      "传输 insert node tablet insertion event 时发生网络错误，原因：%s。";
  public static final String PIPE_EXCEPTION_NETWORK_ERROR_WHEN_TRANSFER_RAW_TABLET_INSERTION_EVENT_BECAUSE_D8ACEC3C =
      "传输 raw tablet insertion event 时发生网络错误，原因：%s。";
  public static final String PIPE_EXCEPTION_NETWORK_ERROR_WHEN_SEAL_FILE_S_BECAUSE_S_DC87F263 =
      "seal 文件 %s 时发生网络错误，原因：%s。";
  public static final String PIPE_EXCEPTION_NETWORK_ERROR_WHEN_TRANSFER_SCHEMA_REGION_WRITE_PLAN_S_BECAUSE_AEB210C7 =
      "传输 schema region write plan %s 时发生网络错误，原因：%s。";
  public static final String PIPE_EXCEPTION_NETWORK_ERROR_WHEN_SEAL_SNAPSHOT_FILE_S_S_AND_S_BECAUSE_5EF373E6 =
      "seal snapshot 文件 %s、%s 和 %s 时发生网络错误，原因：%s。";
  public static final String PIPE_EXCEPTION_FAILED_TO_TRANSFER_SLICE_ORIGIN_REQ_S_S_SLICE_INDEX_D_SLICE_44E1CF32 =
      "传输 slice 失败。Origin req：%s-%s，slice index：%d，slice count：%d。原因：%s";
  public static final String PIPE_EXCEPTION_THE_EXISTING_SERVER_WITH_TCP_PORT_S_AND_HTTPS_PORT_S_S_S_08C076F7 =
      "现有 server 的 tcp port %s 和 https port %s 的 %s %s 与新的 %s %s 冲突，拒绝复用。";
  public static final String PIPE_EXCEPTION_INVALID_KEYSTORE_THE_SERVERPRIVATEKEY_IS_S_F5F3C02F =
      "无效的 keyStore，serverPrivateKey 为 %s";
  public static final String PIPE_EXCEPTION_THE_FOLDER_NODE_FOR_S_DOES_NOT_EXIST_CC0776AE =
      "路径 %s 的 folder node 不存在。";
  public static final String PIPE_EXCEPTION_THE_NODE_S_DOES_NOT_EXIST_52F98935 =
      "Node %s 不存在。";
  public static final String PIPE_EXCEPTION_THE_EXISTING_SERVER_WITH_NODEURL_S_S_S_S_CONFLICTS_TO_THE_1C06A4F6 =
      "现有 server 的 nodeUrl %s 的 %s %s 与新的 %s %s 冲突，拒绝复用。";
  public static final String PIPE_EXCEPTION_UNKNOWN_INSERTBASESTATEMENT_S_CONSTRUCTED_FROM_PIPETRANSFERTABLETINSERTNODEREQ_FF5ED1D7 =
      "由 PipeTransferTabletInsertNodeReq 构造出的 InsertBaseStatement %s 未知。";
  public static final String PIPE_EXCEPTION_UNKNOWN_INSERTNODE_TYPE_S_WHEN_CONSTRUCTING_STATEMENT_FROM_4A055174 =
      "根据 insert node 构造 statement 时遇到未知 InsertNode 类型 %s。";
  public static final String PIPE_EXCEPTION_UNKNOWN_INSERTBASESTATEMENT_S_CONSTRUCTED_FROM_PIPETRANSFERTABLETBINARYREQV2_06D274D2 =
      "由 PipeTransferTabletBinaryReqV2 构造出的 InsertBaseStatement %s 未知。";
  public static final String PIPE_EXCEPTION_UNKNOWN_INSERTBASESTATEMENT_S_CONSTRUCTED_FROM_PIPETRANSFERTABLETINSERTNODEREQV2_16F399B6 =
      "由 PipeTransferTabletInsertNodeReqV2 构造出的 InsertBaseStatement %s 未知。";
  public static final String PIPE_EXCEPTION_FAILED_TO_CREATE_FILE_DIR_FOR_BATCH_S_8FCD9125 =
      "为 batch %s 创建文件目录失败";
  public static final String PIPE_EXCEPTION_FAILED_TO_CREATE_BATCH_FILE_DIR_BATCH_ID_S_EA8BE86C =
      "创建 batch 文件目录失败。（Batch id = %s）";
  public static final String PIPE_EXCEPTION_PIPETREESTATEMENTTOPLANVISITOR_DOES_NOT_SUPPORT_VISITING_3A4A6524 =
      "PipeTreeStatementToPlanVisitor 不支持访问通用 statement，Statement：%s";
  public static final String PIPE_EXCEPTION_PIPESTATEMENTTOPLANVISITOR_DOES_NOT_SUPPORT_VISITING_GENERAL_590C6BD7 =
      "PipeStatementToPlanVisitor 不支持访问通用 statement，Statement：%s";
  public static final String PIPE_EXCEPTION_THE_PATH_PATTERN_S_IS_NOT_VALID_FOR_THE_SOURCE_ONLY_PREFIX_139F93D6 =
      "source 的 path pattern %s 无效。只允许 prefix 或 full path。";
  public static final String PIPE_EXCEPTION_S_S_S_SHOULD_BE_LESS_THAN_OR_EQUAL_TO_S_S_S_0B9726E1 =
      "%s（%s）[%s] 应小于或等于 %s（%s）[%s]。";
  public static final String PIPE_EXCEPTION_PARAMETERS_IN_SET_S_ARE_NOT_ALLOWED_IN_REALTIME_LOOSE_RANGE_BACD2475 =
      "集合 %s 中的参数不允许出现在 'realtime.loose-range' 中";
  public static final String PIPE_EXCEPTION_UNSUPPORTED_EVENT_TYPE_S_FOR_LOG_REALTIME_EXTRACTOR_S_961C5D2D =
      "event type %s 不支持用于 log realtime extractor %s";
  public static final String PIPE_EXCEPTION_UNSUPPORTED_EVENT_TYPE_S_FOR_HYBRID_REALTIME_EXTRACTOR_S_9C4F4C82 =
      "event type %s 不支持用于 hybrid realtime extractor %s";
  public static final String PIPE_EXCEPTION_UNSUPPORTED_STATE_S_FOR_HYBRID_REALTIME_EXTRACTOR_S_43BD62C2 =
      "state %s 不支持用于 hybrid realtime extractor %s";
  public static final String PIPE_EXCEPTION_UNSUPPORTED_EVENT_TYPE_S_FOR_HYBRID_REALTIME_EXTRACTOR_S_474BAAC2 =
      "event type %s 不支持由 hybrid realtime extractor %s 提供。";
  public static final String PIPE_EXCEPTION_PARAMETERS_IN_SET_S_ARE_NOT_ALLOWED_IN_HISTORY_LOOSE_RANGE_0F685D5C =
      "集合 %s 中的参数不允许出现在 'history.loose-range' 中";
  public static final String PIPE_EXCEPTION_THE_AGGREGATOR_AND_OUTPUT_NAME_S_IS_INVALID_BC22CF92 =
      "aggregator 和 output name %s 无效。";
  public static final String PIPE_EXCEPTION_THE_NEEDED_INTERMEDIATE_VALUES_S_ARE_NOT_DEFINED_3FF0C52D =
      "所需 intermediate values %s 未定义。";
  public static final String PIPE_EXCEPTION_THE_PROCESSOR_S_IS_NOT_A_WINDOWING_PROCESSOR_EA5B59BA =
      "processor %s 不是 windowing processor。";
  public static final String PIPE_EXCEPTION_THE_AGGREGATE_PROCESSOR_DOES_NOT_SUPPORT_PROGRESSINDEXTYPE_35351D27 =
      "aggregate processor 不支持 progressIndexType %s";
  public static final String PIPE_EXCEPTION_THE_TYPE_S_IS_NOT_SUPPORTED_E1A6F05D =
      "不支持类型 %s";
  public static final String PIPE_EXCEPTION_THE_OUTPUT_TABLET_DOES_NOT_SUPPORT_COLUMN_TYPE_S_62F3845C =
      "output tablet 不支持 column type %s";
  public static final String PIPE_EXCEPTION_THE_NEW_DATABASE_NAME_S_IS_INVALID_IT_SHOULD_NOT_CONTAIN_C3AB555E =
      "新数据库名 %s 无效：不能包含 '%s'，必须匹配 pattern %s，且长度不能超过 %d";
  public static final String PIPE_EXCEPTION_THE_TYPE_S_CANNOT_BE_CASTED_TO_BOOLEAN_F19CCF75 =
      "类型 %s 无法转换为 boolean。";
  public static final String PIPE_EXCEPTION_THE_TYPE_S_CANNOT_BE_CASTED_TO_INT_659069CC =
      "类型 %s 无法转换为 int。";
  public static final String PIPE_EXCEPTION_THE_TYPE_S_CANNOT_BE_CASTED_TO_LONG_2D206561 =
      "类型 %s 无法转换为 long。";
  public static final String PIPE_EXCEPTION_THE_TYPE_S_CANNOT_BE_CASTED_TO_FLOAT_C15A8A95 =
      "类型 %s 无法转换为 float。";
  public static final String PIPE_EXCEPTION_THE_TYPE_S_CANNOT_BE_CASTED_TO_DOUBLE_E577C0D7 =
      "类型 %s 无法转换为 double。";
  public static final String PIPE_EXCEPTION_THE_TYPE_S_CANNOT_BE_CASTED_TO_STRING_34983FBD =
      "类型 %s 无法转换为 string。";
  public static final String PIPE_EXCEPTION_UNABLE_TO_CREATE_IOTCONSENSUSV2_DELETION_DIR_AT_S_800EE360 =
      "无法在 %s 创建 iotConsensusV2 deletion dir";
  public static final String PIPE_EXCEPTION_THE_TIMESERIES_S_USED_NEW_TYPE_S_IS_NOT_COMPATIBLE_WITH_455D4D4A =
      "timeseries %s 使用的新类型 %s 与现有类型 %s 不兼容。";
  public static final String PIPE_EXCEPTION_THERE_ARE_TWO_TYPES_OF_PLANNODE_IN_ONE_REQUEST_S_AND_S_30FB3EE5 =
      "同一请求中存在两种 PlanNode 类型：%s 和 %s";
  public static final String PIPE_EXCEPTION_THERE_ARE_TWO_TYPES_OF_PLANNODE_IN_ONE_REQUEST_S_AND_SEARCHNODE_F8B4D860 =
      "同一请求中存在两种 PlanNode 类型：%s 和 SearchNode";
  public static final String COMPLETE_PAGE_BODY_EXPECTED_ACTUAL_FMT =
      "page body 不完整。期望：%s。实际：%s";
  public static final String UNCOMPRESS_PAGE_DATA_FAILED_FMT =
      "解压失败！解压后大小：%s，压缩后大小：%s，page header：%s%s";
  public static final String FAILED_TO_CLOSE_LISTENING_QUEUE_FOR_SCHEMAREGION_BECAUSE_FMT =
      "关闭 SchemaRegion %s 的监听队列失败，原因：%s";
  public static final String PIPE_SINK_HEARTBEAT_OR_TRANSFER_FAILED_FMT =
      "PipeConnector：%s(id：%s) heartbeat 失败，或传输 generic event 时遇到错误。失败原因：%s";
  public static final String FAILED_TO_ADD_ITEM_WITH_OPC_ERROR_CODE_FMT =
      "添加 item %s 失败，opc 错误码：0x%s";
  public static final String FAILED_TO_WRITE_WITH_VALUE_AND_OPC_ERROR_CODE_FMT =
      "写入 %s 失败，值：%s，opc 错误码：0x%s";
  public static final String NO_CERTIFICATE_FOUND =
      "未找到证书";
  public static final String CERTIFICATE_MISSING_APPLICATION_URI =
      "证书缺少 application URI";
  public static final String NULL_VALUE =
      "null";
  public static final String INCREASE_REFERENCE_COUNT_ERROR_HOLDER_FMT =
      "增加引用计数出错。Holder Message：%s";
  public static final String DECREASE_REFERENCE_COUNT_ERROR_HOLDER_FMT =
      "减少引用计数出错。Holder Message：%s";
  public static final String INCREASE_REFERENCE_COUNT_TSFILE_OR_MODFILE_ERROR_HOLDER_FMT =
      "为 TsFile %s 或 modFile %s 增加引用计数出错。Holder Message：%s";
  public static final String DECREASE_REFERENCE_COUNT_TSFILE_ERROR_HOLDER_FMT =
      "为 TsFile %s 减少引用计数出错。Holder Message：%s";
  public static final String INCREASE_REFERENCE_COUNT_MTREE_OR_TLOG_ERROR_HOLDER_FMT =
      "为 mTree 快照 %s 或 tLog %s 增加引用计数出错。Holder Message：%s";
  public static final String DECREASE_REFERENCE_COUNT_MTREE_OR_TLOG_ERROR_HOLDER_FMT =
      "为 mTree 快照 %s 或 tLog %s 减少引用计数出错。Holder Message：%s";
  public static final String CONSENSUS_PREFETCHING_QUEUE_CLOSING_BEFORE_SEEK_SCHEDULED_FMT =
      "ConsensusPrefetchingQueue %s 正在关闭，无法调度 seek(%s)";
  public static final String CONSENSUS_PREFETCHING_QUEUE_RUNTIME_UNAVAILABLE_FOR_SEEK_FMT =
      "ConsensusPrefetchingQueue %s 无法调度 seek(%s)，因为 prefetch runtime 不可用";
  public static final String ERROR_PROGID_INVALID_OR_UNREGISTERED_HRESULT_FMT =
      "错误：ProgID 无效或未注册，(HRESULT=0x%s)";
  public static final String ERROR_RUNNING_OPC_CLIENT_FMT =
      "运行 opc client 出错：%s：%s";
  public static final String ERROR_GETTING_OPC_CLIENT_FMT =
      "获取 opc client 出错：%s：%s";

  // ---------------------------------------------------------------------------
  // slice A1 – datanode pipe (leftover literals)
  // ---------------------------------------------------------------------------
  public static final String MESSAGE_FAILED_TO_LOAD_SNAPSHOT_FROM_ARG_9391AA27 =
      "从 {} 加载快照失败";
  public static final String MESSAGE_PIPE_ARG_ARG_HISTORICAL_TSFILE_SELECTION_SUMMARY_SELECTED_BY_PROGRESS_UNCOVERED_ARG_7B74E18D =
      "Pipe {}@{}：历史 TsFile 选择摘要，按进度未覆盖选择 {}，按未关闭/正在关闭选择 {}，"
          + "按时间/路径过滤 {}（时间 {}，路径 {}），跳过已覆盖 {}，跳过已删除 {}，跳过 pipe 生成 {}，"
          + "pin 失败 {}";
  public static final String EXCEPTION_INVALID_ROW_SIZE_ARG_IN_TABLET_FORMAT_DESERIALIZATION_76405615 =
      "tablet 格式反序列化中行大小 %s 无效。";
  public static final String EXCEPTION_INVALID_SCHEMA_SIZE_ARG_IN_TABLET_FORMAT_DESERIALIZATION_838C5359 =
      "tablet 格式反序列化中 schema 大小 %s 无效。";
  public static final String EXCEPTION_MISSING_COLUMN_CATEGORY_IN_CURRENT_TABLET_FORMAT_DESERIALIZATION_660BD963 =
      "当前 tablet 格式反序列化中缺少列类别。";
  public static final String EXCEPTION_INVALID_COLUMN_CATEGORY_ARG_IN_CURRENT_TABLET_FORMAT_DESERIALIZATION_569FF178 =
      "当前 tablet 格式反序列化中列类别 %s 无效。";
  public static final String EXCEPTION_MISSING_TIMESTAMPS_IN_TABLET_FORMAT_DESERIALIZATION_WITH_NON_EMPTY_ROWS_7550129E =
      "tablet 格式反序列化中行非空但缺少时间戳。";
  public static final String EXCEPTION_MISSING_VALUES_IN_TABLET_FORMAT_DESERIALIZATION_WITH_NON_EMPTY_ROWS_1B9C08D9 =
      "tablet 格式反序列化中行非空但缺少值。";
  public static final String EXCEPTION_MISSING_ARG_FLAG_IN_TABLET_FORMAT_DESERIALIZATION_2F802C0D =
      "tablet 格式反序列化中缺少 %s 标志。";
  public static final String EXCEPTION_INVALID_ARG_FLAG_ARG_IN_TABLET_FORMAT_DESERIALIZATION_40FF35AA =
      "tablet 格式反序列化中 %s 标志 %s 无效。";
  public static final String EXCEPTION_INSUFFICIENT_BYTES_FOR_ARG_IN_TABLET_FORMAT_DESERIALIZATION_EXPECTED_ARG_REMAINING_ARG_3FE76C83 =
      "tablet 格式反序列化中 %s 的字节数不足，期望 %s，剩余 %s。";
  public static final String EXCEPTION_INVALID_BITMAP_SIZE_ARG_IN_TABLET_FORMAT_DESERIALIZATION_832E7C9C =
      "tablet 格式反序列化中 bitmap 大小 %s 无效。";
  public static final String EXCEPTION_UNSUPPORTED_SCHEMA_PLAN_NODE_9A833E0B =
      "不支持的 schema plan 节点 ";
  public static final String EXCEPTION_CANNOT_BUILD_SCHEMA_BATCH_PLAN_NODE_FROM_EMPTY_BATCH_842D9E9B =
      "无法从空 batch 构建 schema batch plan 节点。";
  public static final String EXCEPTION_UNKNOWN_INSERTBASESTATEMENT_ARG_CONSTRUCTED_FROM_PIPETRANSFERTABLETBINARYREQ_20BF2833 =
      "从 PipeTransferTabletBinaryReq 构造的未知 InsertBaseStatement %s。";
  public static final String EXCEPTION_INVALID_BINARY_REQUEST_BODY_LENGTH_ARG_REMAINING_BODY_LENGTH_ARG_5E21BBFC =
      "无效的二进制请求体长度 %s，剩余请求体长度 %s。";
  public static final String EXCEPTION_FAILED_TO_DESERIALIZE_INSERT_NODE_ARG_ARG_IN_TABLET_BATCH_AT_BODY_POSITION_ARG_WITH_REMAINING_BODY_LENGTH_ARG_EC41A1DD =
      "在 tablet batch 的 body 位置 %s 反序列化 insert 节点 %s/%s 失败，剩余请求体长度 %s。";
  public static final String EXCEPTION_FAILED_TO_DESERIALIZE_RAW_TABLET_ARG_ARG_IN_TABLET_BATCH_AT_BODY_POSITION_ARG_WITH_REMAINING_BODY_LENGTH_ARG_D36919BA =
      "在 tablet batch 的 body 位置 %s 反序列化原始 tablet %s/%s 失败，剩余请求体长度 %s。";
  public static final String EXCEPTION_INSUFFICIENT_BYTES_TO_READ_ARG_IN_TABLET_BATCH_REMAINING_BODY_LENGTH_ARG_343C1B9A =
      "tablet batch 中读取 %s 的字节数不足，剩余请求体长度 %s。";
  public static final String EXCEPTION_INVALID_NEGATIVE_ARG_ARG_IN_TABLET_BATCH_89A5F868 =
      "tablet batch 中 %s %s 为负数，无效。";
  public static final String EXCEPTION_FAILED_TO_DESERIALIZE_RAW_TABLET_REQUEST_AT_BODY_POSITION_ARG_WITH_REMAINING_BODY_LENGTH_ARG_45AC3692 =
      "在 body 位置 %s 反序列化原始 tablet 请求失败，剩余请求体长度 %s。";
  public static final String EXCEPTION_INCOMPLETE_SCHEMA_IN_CURRENT_TABLET_FORMAT_DESERIALIZATION_A23A1C30 =
      "当前 tablet 格式反序列化中 schema 不完整。";
  public static final String EXCEPTION_COLUMN_COUNT_IS_INCONSISTENT_WITH_SCHEMA_COUNT_IN_CURRENT_TABLET_FORMAT_DESERIALIZATION_53BA037A =
      "当前 tablet 格式反序列化中列数与 schema 数量不一致。";
  public static final String EXCEPTION_INCOMPLETE_MEASUREMENT_SCHEMA_IN_CURRENT_TABLET_FORMAT_DESERIALIZATION_B8DB28A8 =
      "当前 tablet 格式反序列化中 measurement schema 不完整。";
  public static final String EXCEPTION_INCOMPLETE_COLUMN_VALUES_IN_CURRENT_TABLET_FORMAT_DESERIALIZATION_269782B9 =
      "当前 tablet 格式反序列化中列值不完整。";
  public static final String EXCEPTION_INCOMPLETE_TIMESTAMPS_IN_CURRENT_TABLET_FORMAT_DESERIALIZATION_FE212461 =
      "当前 tablet 格式反序列化中时间戳不完整。";
  public static final String MESSAGE_RECEIVER_ARG_IS_TEMPORARILY_UNAVAILABLE_THROTTLE_REQUESTS_FOR_ARG_MS_STATUS_ARG_F37192D9 =
      "Receiver {} 暂时不可用，对请求限流 {} ms。状态：{}";
  public static final String MESSAGE_SUCCESSFULLY_TRANSFERRED_BATCHED_SCHEMA_EVENTS_BATCH_SIZE_ARG_CF2E881C =
      "成功传输批量的 schema 事件，batch 大小 {}。";
  public static final String EXCEPTION_AUTO_CREATE_TREE_DATABASE_FAILED_ARG_STATUS_CODE_ARG_C6175C27 =
      "自动创建 tree database 失败：%s，状态码：%s";
  public static final String EXCEPTION_ILLEGAL_TREE_DATABASE_ARG_C805A990 =
      "非法的 tree database %s。";
  public static final String EXCEPTION_FAILED_TO_GET_PARENT_DIR_OF_8CE21C1D =
      "获取父目录失败：";
  public static final String EXCEPTION_FAILED_TO_PREPARE_NEXT_TABLET_INSERTION_EVENT_70A57827 =
      "准备下一个 tablet insertion event 失败。";
  public static final String EXCEPTION_INVALID_ALIGNED_VALUE_CHUNK_INDEX_ARG_WHILE_THERE_ARE_ARG_TIME_CHUNKS_A7AE6C57 =
      "对齐值 chunk 索引 %d 无效，而共有 %d 个 time chunk。";
  public static final String MESSAGE_FAILED_TO_ROLLBACK_CREATED_REALTIME_PIPE_ARG_STATUS_ARG_CE14334A =
      "回滚已创建的 realtime pipe {} 失败。状态：{}";
  public static final String LOG_REPORTING_PIPE_META_ARG_ISCOMPLETED_ARG_REMAININGEVENTCOUNT_ARG_8F996DF3 =
      "正在上报 pipe meta：%s，isCompleted：%s，remainingEventCount：%s";
  public static final String LOG_REPORTED_ARG_PIPE_METAS_12068FC6 =
      "已上报 %s 个 pipe meta。";
  public static final String MESSAGE_TRANSFER_FILE_ARG_ERROR_RESULT_STATUS_ARG_E565D9FD =
      "传输文件 %s 出错，结果状态为 %s。";

}
