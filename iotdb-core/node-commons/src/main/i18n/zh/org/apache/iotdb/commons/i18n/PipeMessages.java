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

public final class PipeMessages {

  // ===================== Common / Shared =====================

  public static final String UTILITY_CLASS = "工具类";

  // ===================== PipeConfig (printAllConfigs) =====================

  public static final String CONFIG_PIPE_HARDLINK_BASE_DIR_NAME = "PipeHardlinkBaseDirName: {}";
  public static final String CONFIG_PIPE_HARDLINK_TSFILE_DIR_NAME =
      "PipeHardlinkTsFileDirName: {}";
  public static final String CONFIG_PIPE_FILE_RECEIVER_FSYNC_ENABLED =
      "PipeFileReceiverFsyncEnabled: {}";
  public static final String CONFIG_PIPE_DATA_STRUCTURE_TABLET_ROW_SIZE =
      "PipeDataStructureTabletRowSize: {}";
  public static final String CONFIG_PIPE_DATA_STRUCTURE_TABLET_SIZE_IN_BYTES =
      "PipeDataStructureTabletSizeInBytes: {}";
  public static final String
      CONFIG_PIPE_DATA_STRUCTURE_TABLET_MEMORY_BLOCK_ALLOCATION_REJECT_THRESHOLD =
          "PipeDataStructureTabletMemoryBlockAllocationRejectThreshold: {}";
  public static final String
      CONFIG_PIPE_DATA_STRUCTURE_TSFILE_MEMORY_BLOCK_ALLOCATION_REJECT_THRESHOLD =
          "PipeDataStructureTsFileMemoryBlockAllocationRejectThreshold: {}";
  public static final String CONFIG_PIPE_TOTAL_FLOATING_MEMORY_PROPORTION =
      "PipeTotalFloatingMemoryProportion: {}";
  public static final String CONFIG_IS_PIPE_ENABLE_MEMORY_CHECK =
      "IsPipeEnableMemoryCheck: {}";
  public static final String CONFIG_PIPE_TSFILE_PARSER_MEMORY = "PipeTsFileParserMemory: {}";
  public static final String CONFIG_SINK_BATCH_MEMORY_INSERT_NODE =
      "SinkBatchMemoryInsertNode: {}";
  public static final String CONFIG_SINK_BATCH_MEMORY_TSFILE = "SinkBatchMemoryTsFile: {}";
  public static final String CONFIG_SEND_TSFILE_READ_BUFFER = "SendTsFileReadBuffer: {}";
  public static final String CONFIG_PIPE_RESERVED_MEMORY_PERCENTAGE =
      "PipeReservedMemoryPercentage: {}";
  public static final String CONFIG_PIPE_MINIMUM_RECEIVER_MEMORY =
      "PipeMinimumReceiverMemory: {}";
  public static final String CONFIG_PIPE_REALTIME_QUEUE_POLL_TSFILE_THRESHOLD =
      "PipeRealTimeQueuePollTsFileThreshold: {}";
  public static final String CONFIG_PIPE_REALTIME_QUEUE_POLL_HISTORICAL_TSFILE_THRESHOLD =
      "PipeRealTimeQueuePollHistoricalTsFileThreshold: {}";
  public static final String CONFIG_PIPE_REALTIME_QUEUE_MAX_WAITING_TSFILE_SIZE =
      "PipeRealTimeQueueMaxWaitingTsFileSize: {}";
  public static final String CONFIG_PIPE_REALTIME_FORCE_DOWNGRADING_ENABLED =
      "PipeRealtimeForceDowngradingEnabled: {}";
  public static final String CONFIG_PIPE_REALTIME_FORCE_DOWNGRADING_PROPORTION =
      "PipeRealtimeForceDowngradingProportion: {}";
  public static final String CONFIG_PIPE_SUBTASK_EXECUTOR_MAX_THREAD_NUM =
      "PipeSubtaskExecutorMaxThreadNum: {}";
  public static final String
      CONFIG_PIPE_SUBTASK_EXECUTOR_BASIC_CHECKPOINT_INTERVAL_BY_CONSUMED_EVENT_COUNT =
          "PipeSubtaskExecutorBasicCheckPointIntervalByConsumedEventCount: {}";
  public static final String
      CONFIG_PIPE_SUBTASK_EXECUTOR_BASIC_CHECKPOINT_INTERVAL_BY_TIME_DURATION =
          "PipeSubtaskExecutorBasicCheckPointIntervalByTimeDuration: {}";
  public static final String CONFIG_PIPE_SUBTASK_EXECUTOR_PENDING_QUEUE_MAX_BLOCKING_TIME_MS =
      "PipeSubtaskExecutorPendingQueueMaxBlockingTimeMs: {}";
  public static final String
      CONFIG_PIPE_SUBTASK_EXECUTOR_CRON_HEARTBEAT_EVENT_INTERVAL_SECONDS =
          "PipeSubtaskExecutorCronHeartbeatEventIntervalSeconds: {}";
  public static final String CONFIG_PIPE_MAX_WAIT_FINISH_TIME = "PipeMaxWaitFinishTime: {}";
  public static final String CONFIG_PIPE_SINK_SUBTASK_SLEEP_INTERVAL_INIT_MS =
      "PipeSinkSubtaskSleepIntervalInitMs: {}";
  public static final String CONFIG_PIPE_SINK_SUBTASK_SLEEP_INTERVAL_MAX_MS =
      "PipeSinkSubtaskSleepIntervalMaxMs: {}";
  public static final String CONFIG_PIPE_SOURCE_ASSIGNER_DISRUPTOR_RING_BUFFER_SIZE =
      "PipeSourceAssignerDisruptorRingBufferSize: {}";
  public static final String
      CONFIG_PIPE_SOURCE_ASSIGNER_DISRUPTOR_RING_BUFFER_ENTRY_SIZE_IN_BYTES =
          "PipeSourceAssignerDisruptorRingBufferEntrySizeInBytes: {}";
  public static final String CONFIG_PIPE_SOURCE_MATCHER_CACHE_SIZE =
      "PipeSourceMatcherCacheSize: {}";
  public static final String CONFIG_PIPE_SINK_HANDSHAKE_TIMEOUT_MS =
      "PipeSinkHandshakeTimeoutMs: {}";
  public static final String CONFIG_PIPE_SINK_TRANSFER_TIMEOUT_MS =
      "PipeSinkTransferTimeoutMs: {}";
  public static final String CONFIG_PIPE_SINK_READ_FILE_BUFFER_SIZE =
      "PipeSinkReadFileBufferSize: {}";
  public static final String CONFIG_PIPE_SINK_READ_FILE_BUFFER_MEMORY_CONTROL_ENABLED =
      "PipeSinkReadFileBufferMemoryControlEnabled: {}";
  public static final String CONFIG_PIPE_SINK_RETRY_INTERVAL_MS =
      "PipeSinkRetryIntervalMs: {}";
  public static final String CONFIG_PIPE_SINK_RPC_THRIFT_COMPRESSION_ENABLED =
      "PipeSinkRPCThriftCompressionEnabled: {}";
  public static final String CONFIG_PIPE_LEADER_CACHE_MEMORY_USAGE_PERCENTAGE =
      "PipeLeaderCacheMemoryUsagePercentage: {}";
  public static final String CONFIG_PIPE_MAX_ALIGNED_SERIES_CHUNK_SIZE_IN_ONE_BATCH =
      "PipeMaxAlignedSeriesChunkSizeInOneBatch: {}";
  public static final String CONFIG_PIPE_LISTENING_QUEUE_TRANSFER_SNAPSHOT_THRESHOLD =
      "PipeListeningQueueTransferSnapshotThreshold: {}";
  public static final String CONFIG_PIPE_SNAPSHOT_EXECUTION_MAX_BATCH_SIZE =
      "PipeSnapshotExecutionMaxBatchSize: {}";
  public static final String CONFIG_PIPE_REMAINING_TIME_COMMIT_AUTO_SWITCH_SECONDS =
      "PipeRemainingTimeCommitAutoSwitchSeconds: {}";
  public static final String CONFIG_PIPE_REMAINING_TIME_COMMIT_RATE_AVERAGE_TIME =
      "PipeRemainingTimeCommitRateAverageTime: {}";
  public static final String CONFIG_PIPE_REMAINING_INSERT_EVENT_COUNT_AVERAGE =
      "PipePipeRemainingInsertEventCountAverage: {}";
  public static final String CONFIG_PIPE_TSFILE_SCAN_PARSING_THRESHOLD =
      "PipeTsFileScanParsingThreshold(): {}";
  public static final String CONFIG_PIPE_TRANSFER_TSFILE_SYNC = "PipeTransferTsFileSync: {}";
  public static final String CONFIG_PIPE_CHECK_ALL_SYNC_CLIENT_LIVE_TIME_INTERVAL_MS =
      "PipeCheckAllSyncClientLiveTimeIntervalMs: {}";
  public static final String CONFIG_PIPE_DYNAMIC_MEMORY_HISTORY_WEIGHT =
      "PipeDynamicMemoryHistoryWeight: {}";
  public static final String CONFIG_PIPE_DYNAMIC_MEMORY_ADJUSTMENT_THRESHOLD =
      "PipeDynamicMemoryAdjustmentThreshold: {}";
  public static final String
      CONFIG_PIPE_THRESHOLD_ALLOCATION_STRATEGY_MAXIMUM_MEMORY_INCREMENT_RATIO =
          "PipeThresholdAllocationStrategyMaximumMemoryIncrementRatio: {}";
  public static final String CONFIG_PIPE_THRESHOLD_ALLOCATION_STRATEGY_LOW_USAGE_THRESHOLD =
      "PipeThresholdAllocationStrategyLowUsageThreshold: {}";
  public static final String
      CONFIG_PIPE_THRESHOLD_ALLOCATION_STRATEGY_FIXED_MEMORY_HIGH_USAGE_THRESHOLD =
          "PipeThresholdAllocationStrategyFixedMemoryHighUsageThreshold: {}";
  public static final String
      CONFIG_PIPE_ASYNC_SINK_FORCED_RETRY_TSFILE_EVENT_QUEUE_SIZE_THRESHOLD =
          "PipeAsyncSinkForcedRetryTsFileEventQueueSizeThreshold: {}";
  public static final String
      CONFIG_PIPE_ASYNC_SINK_FORCED_RETRY_TABLET_EVENT_QUEUE_SIZE_THRESHOLD =
          "PipeAsyncSinkForcedRetryTabletEventQueueSizeThreshold: {}";
  public static final String
      CONFIG_PIPE_ASYNC_SINK_FORCED_RETRY_TOTAL_EVENT_QUEUE_SIZE_THRESHOLD =
          "PipeAsyncSinkForcedRetryTotalEventQueueSizeThreshold: {}";
  public static final String CONFIG_PIPE_ASYNC_SINK_MAX_RETRY_EXECUTION_TIME_MS_PER_CALL =
      "PipeAsyncSinkMaxRetryExecutionTimeMsPerCall: {}";
  public static final String CONFIG_PIPE_ASYNC_SINK_SELECTOR_NUMBER =
      "PipeAsyncSinkSelectorNumber: {}";
  public static final String CONFIG_PIPE_ASYNC_SINK_MAX_CLIENT_NUMBER =
      "PipeAsyncSinkMaxClientNumber: {}";
  public static final String CONFIG_PIPE_ASYNC_SINK_MAX_TSFILE_CLIENT_NUMBER =
      "PipeAsyncSinkMaxTsFileClientNumber: {}";
  public static final String CONFIG_PIPE_SEND_TSFILE_RATE_LIMIT_BYTES_PER_SECOND =
      "PipeSendTsFileRateLimitBytesPerSecond: {}";
  public static final String CONFIG_PIPE_ALL_SINKS_RATE_LIMIT_BYTES_PER_SECOND =
      "PipeAllSinksRateLimitBytesPerSecond: {}";
  public static final String CONFIG_RATE_LIMITER_HOT_RELOAD_CHECK_INTERVAL_MS =
      "RateLimiterHotReloadCheckIntervalMs: {}";
  public static final String CONFIG_PIPE_SINK_REQUEST_SLICE_THRESHOLD_BYTES =
      "PipeSinkRequestSliceThresholdBytes: {}";
  public static final String CONFIG_SEPERATED_PIPE_HEARTBEAT_ENABLED =
      "SeperatedPipeHeartbeatEnabled: {}";
  public static final String CONFIG_PIPE_HEARTBEAT_INTERVAL_SECONDS_FOR_COLLECTING_PIPE_META =
      "PipeHeartbeatIntervalSecondsForCollectingPipeMeta: {}";
  public static final String CONFIG_PIPE_META_SYNCER_INITIAL_SYNC_DELAY_MINUTES =
      "PipeMetaSyncerInitialSyncDelayMinutes: {}";
  public static final String CONFIG_PIPE_META_SYNCER_SYNC_INTERVAL_MINUTES =
      "PipeMetaSyncerSyncIntervalMinutes: {}";
  public static final String
      CONFIG_PIPE_META_SYNCER_AUTO_RESTART_PIPE_CHECK_INTERVAL_ROUND =
          "PipeMetaSyncerAutoRestartPipeCheckIntervalRound: {}";
  public static final String CONFIG_PIPE_AUTO_RESTART_ENABLED = "PipeAutoRestartEnabled: {}";
  public static final String CONFIG_PIPE_AIR_GAP_RECEIVER_ENABLED =
      "PipeAirGapReceiverEnabled: {}";
  public static final String CONFIG_PIPE_AIR_GAP_RECEIVER_PORT = "PipeAirGapReceiverPort: {}";
  public static final String CONFIG_PIPE_AIR_GAP_SINK_TABLET_TIMEOUT_MS =
      "PipeAirGapSinkTabletTimeoutMs: {}";
  public static final String CONFIG_PIPE_RECEIVER_LOGIN_PERIODIC_VERIFICATION_INTERVAL_MS =
      "PipeReceiverLoginPeriodicVerificationIntervalMs: {}";
  public static final String CONFIG_PIPE_RECEIVER_ACTUAL_TO_ESTIMATED_MEMORY_RATIO =
      "PipeReceiverActualToEstimatedMemoryRatio: {}";
  public static final String CONFIG_PIPE_RECEIVER_REQ_DECOMPRESSED_MAX_LENGTH_IN_BYTES =
      "PipeReceiverReqDecompressedMaxLengthInBytes: {}";
  public static final String CONFIG_PIPE_AIR_GAP_RECEIVER_MAX_PAYLOAD_SIZE_IN_BYTES =
      "PipeAirGapReceiverMaxPayloadSizeInBytes: {}";
  public static final String CONFIG_PIPE_RECEIVER_LOAD_CONVERSION_ENABLED =
      "PipeReceiverLoadConversionEnabled: {}";
  public static final String CONFIG_PIPE_PERIODICAL_LOG_MIN_INTERVAL_SECONDS =
      "PipePeriodicalLogMinIntervalSeconds: {}";
  public static final String CONFIG_PIPE_RETRY_LOCALLY_FOR_PARALLEL_OR_USER_CONFLICT =
      "PipeRetryLocallyForParallelOrUserConflict: {}";
  public static final String CONFIG_PIPE_META_REPORT_MAX_LOG_NUM_PER_ROUND =
      "PipeMetaReportMaxLogNumPerRound: {}";
  public static final String CONFIG_PIPE_META_REPORT_MAX_LOG_INTERVAL_ROUNDS =
      "PipeMetaReportMaxLogIntervalRounds: {}";
  public static final String CONFIG_PIPE_TSFILE_PIN_MAX_LOG_NUM_PER_ROUND =
      "PipeTsFilePinMaxLogNumPerRound: {}";
  public static final String CONFIG_PIPE_TSFILE_PIN_MAX_LOG_INTERVAL_ROUNDS =
      "PipeTsFilePinMaxLogIntervalRounds: {}";
  public static final String CONFIG_PIPE_LOGGER_CACHE_MAX_SIZE_IN_BYTES =
      "PipeLoggerCacheMaxSizeInBytes: {}";
  public static final String CONFIG_PIPE_MEMORY_MANAGEMENT_ENABLED =
      "PipeMemoryManagementEnabled: {}";
  public static final String CONFIG_PIPE_MEMORY_ALLOCATE_MAX_RETRIES =
      "PipeMemoryAllocateMaxRetries: {}";
  public static final String CONFIG_PIPE_MEMORY_ALLOCATE_RETRY_INTERVAL_IN_MS =
      "PipeMemoryAllocateRetryIntervalInMs: {}";
  public static final String CONFIG_PIPE_MEMORY_ALLOCATE_MIN_SIZE_IN_BYTES =
      "PipeMemoryAllocateMinSizeInBytes: {}";
  public static final String CONFIG_PIPE_MEMORY_ALLOCATE_FOR_TSFILE_SEQUENCE_READER_IN_BYTES =
      "PipeMemoryAllocateForTsFileSequenceReaderInBytes: {}";
  public static final String CONFIG_PIPE_MEMORY_EXPANDER_INTERVAL_SECONDS =
      "PipeMemoryExpanderIntervalSeconds: {}";
  public static final String CONFIG_PIPE_CHECK_MEMORY_ENOUGH_INTERVAL_MS =
      "PipeCheckMemoryEnoughIntervalMs: {}";
  public static final String CONFIG_TWO_STAGE_AGGREGATE_MAX_COMBINER_LIVE_TIME_IN_MS =
      "TwoStageAggregateMaxCombinerLiveTimeInMs: {}";
  public static final String CONFIG_TWO_STAGE_AGGREGATE_DATA_REGION_INFO_CACHE_TIME_IN_MS =
      "TwoStageAggregateDataRegionInfoCacheTimeInMs: {}";
  public static final String CONFIG_TWO_STAGE_AGGREGATE_SENDER_END_POINTS_CACHE_IN_MS =
      "TwoStageAggregateSenderEndPointsCacheInMs: {}";
  public static final String CONFIG_PIPE_EVENT_REFERENCE_TRACKING_ENABLED =
      "PipeEventReferenceTrackingEnabled: {}";
  public static final String CONFIG_PIPE_EVENT_REFERENCE_ELIMINATE_INTERVAL_SECONDS =
      "PipeEventReferenceEliminateIntervalSeconds: {}";
  public static final String CONFIG_PIPE_AUTO_SPLIT_FULL_ENABLED =
      "PipeAutoSplitFullEnabled: {}";

  // ===================== PipePluginAgent =====================

  public static final String FAILED_TO_CLOSE_TEMPORARY_SOURCE =
      "关闭临时 source 失败：{}";
  public static final String FAILED_TO_CLOSE_TEMPORARY_PROCESSOR =
      "关闭临时 processor 失败：{}";
  public static final String FAILED_TO_CLOSE_TEMPORARY_CONNECTOR =
      "关闭临时 connector 失败：{}";

  // ===================== PipePluginExecutableManager =====================

  public static final String FAILED_TO_READ_MD5_FOR_PIPE_PLUGIN =
      "读取 pipe 插件 {} 的 MD5 文件失败";

  // ===================== AbstractPipePeriodicalJobExecutor =====================

  public static final String PERIODICAL_JOB_FAILED = "定时任务 {} 执行失败。";
  public static final String PERIODICAL_JOB_REGISTERED =
      "Pipe 定时任务 {} 注册成功。间隔：{} 秒。";
  public static final String PERIODICAL_JOB_EXECUTOR_STARTED =
      "Pipe 定时任务执行器已成功启动。";
  public static final String PERIODICAL_JOB_EXECUTOR_STOPPED =
      "Pipe 定时任务执行器已成功停止。";
  public static final String PERIODICAL_JOBS_CLEARED =
      "所有 pipe 定时任务已成功清除。";

  // ===================== PipeTaskAgent =====================

  public static final String NOT_ENOUGH_MEMORY_FOR_PIPE = "pipe 内存不足。";
  public static final String NOT_ENOUGH_MEMORY_FOR_PIPE_FORMAT =
      NOT_ENOUGH_MEMORY_FOR_PIPE
          + "需要内存：%d 字节，空闲内存：%d 字节，保留内存：%d 字节，"
          + "总内存：%d 字节";
  public static final String NOT_ENOUGH_FLOATING_MEMORY_FOR_PIPE_FORMAT =
      NOT_ENOUGH_MEMORY_FOR_PIPE + "需要浮动内存：%d 字节，空闲浮动内存：%d 字节";
  public static final String UNKNOWN_PIPE_STATUS = "Pipe %s 的状态 %s 未知";
  public static final String UNEXPECTED_PIPE_STATUS = "意外的 pipe 状态 %s：";
  public static final String INTERRUPTED_ACQUIRING_READ_LOCK =
      "获取 pipeMetaKeeper 读锁时被中断。";
  public static final String INTERRUPTED_ACQUIRING_WRITE_LOCK =
      "获取 pipeMetaKeeper 写锁时被中断。";
  public static final String FAILED_TO_HANDLE_SINGLE_PIPE_META_CHANGES =
      "处理 pipe {} 的单条元数据变更失败";
  public static final String FAILED_TO_HANDLE_SINGLE_PIPE_META_CHANGES_FORMAT =
      "处理 pipe %s 的单条元数据变更失败，原因：%s";
  public static final String FAILED_TO_DROP_PIPE = "删除 pipe {} 失败";
  public static final String FAILED_TO_DROP_PIPE_FORMAT =
      "删除 pipe %s 失败，原因：%s";
  public static final String FAILED_TO_ACQUIRE_LOCK_DROPPING_ALL =
      "删除所有 pipe 任务时获取锁失败，将跳过删除";
  public static final String FAILED_TO_DROP_PIPE_WITH_CREATION_TIME =
      "删除 pipe {}（创建时间 = {}）失败";
  public static final String STOPPED_ALL_PIPES_WITH_CRITICAL_EXCEPTION =
      "已停止所有存在严重异常的 pipe。";
  public static final String FAILED_TO_STOP_ALL_PIPES_RETRY =
      "停止所有存在严重异常的 pipe 失败，重试次数：{}。";
  public static final String INTERRUPTED_STOPPING_ALL_PIPES =
      "尝试停止所有存在严重异常的 pipe 时被中断，异常信息：{}";
  public static final String FAILED_TO_STOP_ALL_PIPES =
      "停止所有存在严重异常的 pipe 失败，异常信息：{}";
  public static final String FAILED_TO_HANDLE_PIPE_META_CHANGES_FORMAT =
      "处理 pipe %s 的元数据变更失败，原因：%s";
  public static final String FAILED_TO_HANDLE_PIPE_META_CHANGES_LOG =
      "处理 pipe %s 的元数据变更失败";
  public static final String CREATE_PIPE_FAILED_ENCRYPTION =
      "创建 Pipe %s 失败，因为 TsFile 配置了加密，禁止使用 Pipe";
  public static final String PIPE_ALREADY_CREATED =
      "Pipe {}（创建时间 = {}）已经创建。当前状态 = {}。跳过创建。";
  public static final String PIPE_ALREADY_DROPPED_RECREATING =
      "Pipe {}（创建时间 = {}）已被删除，但 pipe 任务元数据尚未清理。"
          + "当前状态 = {}。尝试删除并重新创建。";
  public static final String PIPE_ALREADY_DROPPED_SKIP_NO_CREATION_TIME =
      "Pipe {} 已被删除或尚未创建。跳过删除。";
  public static final String PIPE_ALREADY_DROPPED_SKIP_STARTING =
      "Pipe {}（创建时间 = {}）已被删除或尚未创建。跳过启动。";
  public static final String PIPE_ALREADY_DROPPED_SKIP_STOPPING =
      "Pipe {}（创建时间 = {}）已被删除或尚未创建。跳过停止。";
  public static final String PIPE_CREATION_TIME_MISMATCH_STARTING =
      "Pipe {}（创建时间 = {}）已创建但与启动请求中的创建时间（{}）不匹配。跳过启动。";
  public static final String PIPE_CREATION_TIME_MISMATCH_STOPPING =
      "Pipe {}（创建时间 = {}）已创建但与停止请求中的创建时间（{}）不匹配。跳过停止。";
  public static final String PIPE_CREATION_TIME_MISMATCH_DROPPING =
      "Pipe {}（创建时间 = {}）已创建但与删除请求中的创建时间（{}）不匹配。跳过删除。";
  public static final String PIPE_STARTED_CURRENT_STATUS =
      "Pipe {}（创建时间 = {}）已创建。当前状态 = {}。正在启动。";
  public static final String PIPE_ALREADY_STARTED =
      "Pipe {}（创建时间 = {}）已经启动。当前状态 = {}。跳过启动。";
  public static final String PIPE_ALREADY_DROPPED_SKIP_STARTING_STATUS =
      "Pipe {}（创建时间 = {}）已被删除。当前状态 = {}。跳过启动。";
  public static final String PIPE_ALREADY_STOPPED =
      "Pipe {}（创建时间 = {}）已经停止。当前状态 = {}。跳过停止。";
  public static final String PIPE_RUNNING_STOPPING =
      "Pipe {}（创建时间 = {}）已启动。当前状态 = {}。正在停止。";
  public static final String PIPE_ALREADY_DROPPED_SKIP_STOPPING_STATUS =
      "Pipe {}（创建时间 = {}）已被删除。当前状态 = {}。跳过停止。";
  public static final String CREATE_ALL_PIPE_TASKS =
      "Pipe {} 的所有任务在 {} 毫秒内创建成功";
  public static final String DROP_ALL_PIPE_TASKS =
      "Pipe {} 的所有任务在 {} 毫秒内删除成功";
  public static final String START_ALL_PIPE_TASKS =
      "Pipe {} 的所有任务在 {} 毫秒内启动成功";
  public static final String STOP_ALL_PIPE_TASKS =
      "Pipe {} 的所有任务在 {} 毫秒内停止成功";
  public static final String PIPE_ALREADY_DROPPED_OR_NOT_CREATED_SKIP =
      "Pipe {}（创建时间 = {}）已被删除或尚未创建。跳过删除。";
  public static final String PIPE_STOPPED_CRITICAL_EXCEPTION =
      "Pipe %s（创建时间 = %s）将因 connector %s 中的严重异常（发生时间 %s）而停止。";
  public static final String PIPE_WAS_STOPPED_CRITICAL_EXCEPTION =
      "Pipe %s（创建时间 = %s）已因严重异常（发生时间 %s）而停止。";

  // ===================== PipeEventCommitManager =====================

  public static final String PIPE_COMMITTER_REGISTERED =
      "Pipe committer 已在 region 上注册：{}";
  public static final String PIPE_COMMITTER_OVERWRITING =
      "同名 pipe 已在此 region 上注册，将覆盖：{}";
  public static final String PIPE_COMMITTER_DEREGISTERED =
      "Pipe committer 已在 region 上注销：{}";

  // ===================== BlockingPendingQueue =====================

  public static final String PENDING_QUEUE_PUT_INTERRUPTED =
      "待处理队列 put 操作被中断。";
  public static final String PENDING_QUEUE_POLL_INTERRUPTED =
      "待处理队列 poll 操作被中断。";

  // ===================== PipeSubtaskExecutor =====================

  public static final String SUBTASK_ALREADY_REGISTERED = "子任务 {} 已注册。";
  public static final String SUBTASK_NOT_REGISTERED = "子任务 {} 未注册。";
  public static final String SUBTASK_ALREADY_RUNNING = "子任务 {} 已在运行。";
  public static final String SUBTASK_STARTED = "子任务 {} 已开始自提交。";
  public static final String SUBTASK_CLOSED = "子任务 {} 已成功关闭。";
  public static final String SUBTASK_CLOSE_FAILED = "关闭子任务 {} 失败。";

  // ===================== PipeReportableSubtask =====================

  public static final String ON_FAILURE_IGNORED_PIPE_DROPPED =
      "pipe 子任务 onFailure 回调，因 pipe 已删除而忽略。";
  public static final String INTERRUPTED_WAITING_HIGH_PRIORITY =
      "等待高优先级锁任务时被中断。";
  public static final String FAILED_TO_EXECUTE_SUBTASK =
      "执行子任务 {}（创建时间：{}，类名：{}）失败，原因：{}。将重试 {} 次。";
  public static final String RETRY_EXECUTING_SUBTASK =
      "重试执行子任务 {}（创建时间：{}，类名：{}），重试次数 [{}/{}]，上次异常：{}";
  public static final String INTERRUPTED_RETRYING_SUBTASK =
      "重试执行子任务 {}（创建时间：{}，类名：{}）时被中断";
  public static final String SUBTASK_RETRY_EXCEEDED_FORMAT =
      "执行子任务 %s（创建时间：%s，类名：%s）失败，"
          + "重试次数超过最大值 %d，上次异常：%s，根因：%s";
  public static final String SUBTASK_EXCEPTION_REPORTED =
      "最后一个事件是 EnrichedEvent 实例，异常已上报。"
          + "正在本地停止 pipe 子任务 {}（创建时间：{}，类名：{}）... "
          + "查询 pipe 时显示的状态将为 'STOPPED'。"
          + "如需要，请手动执行 'START PIPE' 重新启动任务。";
  public static final String FAILED_TO_EXECUTE_SUBTASK_RETRY_FOREVER =
      "执行子任务 {}（创建时间：{}，类名：{}）失败，原因：{}。将无限重试。";
  public static final String RETRY_EXECUTING_SUBTASK_FOREVER =
      "重试执行子任务 {}（创建时间：{}，类名：{}），重试次数 {}，上次异常：{}";
  public static final String PATTERN_INCLUSION_CANNOT_BE_USED_WITH_PATTERN_OR_PATH =
      "Pipe：%s 不能与 %s 或 %s 同时使用。";
  public static final String PATTERN_INCLUSION_CANNOT_BE_USED_WITH_PATH_EXCLUSION =
      "Pipe：%s 不能与 %s 同时使用。";
  public static final String PATH_AND_PATTERN_CANNOT_BE_USED_TOGETHER =
      "Pipe：%s 和 %s 不能同时使用。";
  public static final String PARAMETER_ONLY_SUPPORTS_SINGLE_PATTERN =
      "Pipe：参数 %s 当前只支持单个 pattern。";
  public static final String FAILED_TO_PERFORM_PATTERN_COVERAGE_CHECK =
      "Pipe：对 inclusion [{}] 和 exclusion [{}] 执行 pattern 覆盖检查失败。";
  public static final String EXCLUSION_PATTERN_FULLY_COVERS_INCLUSION_PATTERN =
      "Pipe：给定 exclusion pattern 完全覆盖了 inclusion pattern。该 pipe pattern 不会匹配任何内容。Inclusion: [%s], Exclusion: [%s]";
  public static final String EXCLUSION_PATTERN_COVERS_PART_OF_INCLUSION_PATHS =
      "Pipe：给定 exclusion pattern 覆盖了 {} / {} 条 inclusion 路径。这些路径将被排除。Inclusion: [{}], Exclusion: [{}]";

  // ===================== PipeAbstractSinkSubtask =====================

  public static final String ON_FAILURE_IGNORED_CONNECTOR_DROPPED =
      "pipe 传输 onFailure 回调，因 connector 子任务已删除而忽略。";
  public static final String ON_FAILURE_IGNORED_EVENT_RELEASED =
      "pipe 传输 onFailure 回调，因失败事件已释放而忽略。";
  public static final String ON_FAILURE_IGNORED_EVENT_PIPE_DROPPED =
      "pipe 传输 onFailure 回调，因失败事件的 pipe 已删除而忽略。";
  public static final String NON_CRITICAL_EXCEPTION_WILL_THROW_CRITICAL =
      "发生非 PipeRuntimeSinkCriticalException，将抛出 PipeRuntimeSinkCriticalException。";
  public static final String PIPE_CONNECTION_EXCEPTION_RETRYING =
      "发生 PipeConnectionException，%s 正在重试与目标系统握手。";
  public static final String HANDSHAKE_SUCCESS = "{} 与目标系统握手成功。";
  public static final String HANDSHAKE_FAILED_RETRYING =
      "{} 与目标系统握手失败，已重试 {} 次，最多重试 {} 次。";
  public static final String INTERRUPTED_WHILE_SLEEPING_RETRY_HANDSHAKE =
      "休眠时被中断，将重试与目标系统握手。";
  public static final String HANDSHAKE_FAILED_STOPPING =
      "{} 与目标系统握手在 {} 次尝试后失败，"
          + "正在停止当前子任务 {}（创建时间：{}，类名：{}）。"
          + "查询 pipe 时显示的状态将为 'STOPPED'。"
          + "如需要，请手动执行 'START PIPE' 重新启动任务。";
  public static final String TEMPORARILY_OUT_OF_MEMORY =
      "pipe 事件传输时暂时内存不足，将等待内存释放。";
  public static final String EXCEPTION_IN_PIPE_TRANSFER_FORMAT =
      "pipe 传输异常，子任务：%s，最后事件：%s，根因：%s";
  public static final String EXCEPTION_IN_PIPE_TRANSFER_IGNORED =
      "pipe 传输异常，因 sink 子任务已删除而忽略。{}";
  public static final String PIPE_EXCEPTION_IGNORED =
      "{} pipe 传输异常，因 connector 子任务已删除而忽略。{}";

  // ===================== PipeCompressorFactory =====================

  public static final String COMPRESSOR_CONFIG_NULL = "PipeCompressorConfig 为空";
  public static final String COMPRESSOR_CONFIG_NAME_NULL =
      "PipeCompressorConfig.getName() 为空";
  public static final String CREATE_ZSTD_COMPRESSOR =
      "创建新的 PipeZSTDCompressor，压缩级别：{}";
  public static final String COMPRESSOR_NOT_FOUND_BY_NAME =
      "未找到名称为 %s 的 PipeCompressor";
  public static final String COMPRESSOR_NOT_FOUND_BY_INDEX =
      "未找到索引为 %s 的 PipeCompressor";

  // ===================== PipeTransferSliceReqHandler =====================

  public static final String INVALID_STATE_SLICE =
      "无效状态：orderId={}，originReqType={}，originBodySize={}，sliceCount={}，sliceBodies.size={}";
  public static final String ORDER_ID_MISMATCH =
      "Order ID 不匹配：期望 {}，实际 {}";
  public static final String SLICE_COUNT_MISMATCH =
      "分片数量不匹配：期望 {}，实际 {}";

  // ===================== IoTDBSink =====================

  public static final String IOTDB_SINK_NODE_URLS = "IoTDBSink nodeUrls：{}";
  public static final String IOTDB_SINK_TABLET_BATCH_MODE =
      "IoTDBSink isTabletBatchModeEnabled：{}";
  public static final String IOTDB_SINK_MARK_AS_PIPE_REQUEST =
      "IoTDBSink shouldMarkAsPipeRequest：{}";
  public static final String IOTDB_SINK_SKIP_IF_NO_PRIVILEGES =
      "IoTDBSink skipIfNoPrivileges：{}";
  public static final String HOST_CANNOT_BE_EMPTY = "主机名不能为空";
  public static final String PORT_CANNOT_BE_EMPTY = "端口不能为空";

  // ===================== IoTDBSslSyncSink =====================

  public static final String SYNC_CLIENT_MANAGER_CLOSED =
      "IoTDB sync 客户端管理器已关闭";
  public static final String REDIRECT_FILE_POSITION = "重定向文件位置到 {}。";

  // ===================== IoTDBAirGapSink =====================

  public static final String AIR_GAP_CUSTOMIZED_HANDSHAKE_TIMEOUT =
      "IoTDBAirGapConnector 自定义握手超时时间：{}。";
  public static final String AIR_GAP_CUSTOMIZED_E_LANGUAGE =
      "IoTDBAirGapConnector 自定义 eLanguageEnable：{}。";
  public static final String CONNECTED_TO_TARGET_SERVER =
      "成功连接到目标服务器 ip：{}，端口：{}。";
  public static final String FAILED_TO_CLOSE_SOCKET =
      "关闭与目标服务器的 socket 失败（ip：{}，端口：{}），原因：{}。已忽略。";
  public static final String FAILED_TO_CONNECT_TO_TARGET =
      "连接目标服务器失败（ip：{}，端口：{}），原因：{}。已忽略。";
  public static final String HANDSHAKE_ERROR_RECEIVING_END =
      "握手错误，可能由接收端错误引起。已忽略。";
  public static final String HANDSHAKE_ERROR_WITH_TARGET =
      "与目标服务器握手失败，socket：%s";
  public static final String HANDSHAKE_SUCCESS_SOCKET = "握手成功。Socket：{}";
  public static final String FAILED_TO_CLOSE_CLIENT = "关闭客户端 {} 失败。";
  public static final String UNKNOWN_LOAD_BALANCE_STRATEGY =
      "未知的负载均衡策略：{}，将使用轮询策略替代。";

  // ===================== IoTDBSyncClientManager =====================

  public static final String CLIENT_CLOSED = "客户端 {}:{} 已关闭。";
  public static final String FAILED_TO_CLOSE_CLIENT_ENDPOINT =
      "关闭客户端 {}:{} 失败，原因：{}。";
  public static final String FAILED_TO_INIT_CLIENT =
      "初始化客户端失败，目标服务器 ip：%s，端口：%s，原因：%s";

  // ===================== IoTDBNonDataRegionSource =====================

  public static final String CANNOT_GET_NEWEST_SNAPSHOT =
      "触发快照后无法获取最新快照。";

  // ===================== PipeInclusionOptions =====================

  public static final String ILLEGAL_PATH_INITIALIZING_OPTIONS =
      "初始化 LEGAL_OPTIONS 时遇到非法路径。";

  // ===================== IoTDBTreePattern / TablePattern =====================

  public static final String ILLEGAL_IOTDB_PIPE_PATTERN =
      "非法的 IoTDBPipePattern：%s";
  // ===================== ConcurrentIterableLinkedQueue =====================

  public static final String NULL_ELEMENT_NOT_ALLOWED = "不允许空元素。";
  public static final String CALLING_NEXT_ON_CLOSED_ITERATOR =
      "在已关闭的迭代器上调用 next()，将返回 null。";
  public static final String INTERRUPTED_WAITING_FOR_NEXT =
      "等待下一个元素时被中断。";

  // ===================== PlainQueueSerializer =====================

  public static final String FAILED_TO_LOAD_SNAPSHOT = "加载快照失败。";

  // ===================== AbstractSerializableListeningQueue =====================

  public static final String UNKNOWN_SERIALIZER_TYPE =
      "未知的序列化器类型：%s";

  // ===================== EnrichedEvent =====================

  // ===================== PipeEventCommitMetrics =====================

  public static final String FAILED_TO_UNBIND_COMMIT_METRICS =
      "从 pipe 事件提交指标解绑失败，事件提交器映射不为空";

  // ===================== PipePhantomReferenceManager =====================

  public static final String CLEAN_PHANTOM_REFERENCES =
      "在 {} 毫秒内成功清理 {} 个 pipe 虚引用，剩余引用数：{}";
  public static final String REMAINING_PHANTOM_REFERENCE_COUNT =
      "剩余 pipe 虚引用数量：{}";
  public static final String PIPE_EVENT_RESOURCE_LEAK =
      "检测到 PIPE 事件资源泄漏（{}）：{}";

  // ===================== PipeSnapshotResourceManager =====================

  public static final String CANNOT_FIND_SNAPSHOT_PATH =
      "无法在 pipe 目录中找到 {} 的正确目标快照路径";
  public static final String CANNOT_FIND_SNAPSHOT_PATH_EXCEPTION =
      "无法在 pipe 目录中找到 %s 的正确目标快照路径";

  // ===================== IoTDBReceiverAgent =====================

  public static final String CLEAN_RECEIVER_DIR_SUCCESS =
      "清理 pipe 接收目录 {} 成功。";
  public static final String CLEAN_RECEIVER_DIR_FAILED =
      "清理 pipe 接收目录 {} 失败。";
  public static final String CREATE_RECEIVER_DIR_SUCCESS =
      "创建 pipe 接收目录 {} 成功。";
  public static final String CREATE_RECEIVER_DIR_FAILED =
      "创建 pipe 接收目录 {} 失败。";

  // ===================== PipeReceiverFilePathUtils =====================

  public static final String ILLEGAL_FILENAME_PATH_TRAVERSAL =
      "非法文件名：%s（检测到路径遍历）";

  // ===================== IoTDBFileReceiver =====================

  public static final String RECEIVER_HANDSHAKE_FAILED =
      "握手失败，响应状态 = %s。";
  public static final String RECEIVER_HANDSHAKE_FAILED_WITH_ID =
      "接收器 id = %s：握手失败，响应状态 = %s。";
  public static final String RECEIVER_HANDSHAKE_FAILED_LOGIN =
      "接收器 id = %s：因登录失败导致握手失败，响应状态 = %s。";
  public static final String RECEIVER_USER_LOGIN_SUCCESS =
      "接收器 id = {}：用户 {} 登录成功。";
  public static final String RECEIVER_EXITED =
      "接收器 id = {}：处理退出：接收器已退出。";
  public static final String RECEIVER_ORIGINAL_DIR_DELETED =
      "接收器 id = {}：原始接收文件目录 {} 已删除。";
  public static final String RECEIVER_FAILED_DELETE_ORIGINAL_DIR =
      "接收器 id = %s：删除原始接收文件目录 %s 失败，原因：%s。";
  public static final String RECEIVER_ORIGINAL_DIR_NOT_EXIST =
      "接收器 id = {}：原始接收文件目录 {} 不存在。无需删除。";
  public static final String RECEIVER_DIR_NULL_NO_DELETE =
      "接收器 id = {}：当前接收文件目录为空。无需删除。";
  public static final String RECEIVER_FAILED_INIT_FOLDER_FULL =
      "接收器 id = %s：初始化 pipe 接收文件目录管理器失败，所有磁盘目录已满。";
  public static final String RECEIVER_FAILED_CREATE_FOLDER_FULL =
      "接收器 id = %s：创建 pipe 接收文件目录失败，所有磁盘目录已满。";
  public static final String RECEIVER_HANDSHAKE_SUCCESS =
      "接收器 id = {}：握手成功！发送方主机 = {}，端口 = {}。接收文件目录 = {}。";
  public static final String RECEIVER_FAILED_CREATE_DIR =
      "接收器 id = %s：创建接收文件目录 %s 失败。";
  public static final String RECEIVER_FAILED_CREATE_DIR_STATUS =
      "创建接收文件目录 %s 失败。";
  public static final String RECEIVER_CANNOT_GET_CLUSTER_ID =
      "接收器无法从配置节点获取 clusterId。";
  public static final String RECEIVER_NO_CLUSTER_ID_IN_REQUEST =
      "握手请求中不包含 clusterId。";
  public static final String RECEIVER_SAME_CLUSTER =
      "接收器和发送器来自同一集群 %s。";
  public static final String RECEIVER_NO_TIMESTAMP_PRECISION =
      "握手请求中不包含 timestampPrecision。";
  public static final String RECEIVER_TIMESTAMP_PRECISION_MISMATCH =
      "IoTDB 接收器的时间精度为 %s，connector 的时间精度为 %s。校验失败。";
  public static final String RECEIVER_FAILED_LOGIN =
      "接收器 id = %s：登录失败，用户名 = %s，响应 = %s。";
  public static final String RECEIVER_FILE_OFFSET_RESET =
      "接收器 id = %s：接收器请求文件偏移量重置，响应状态 = %s。";
  public static final String RECEIVER_FAILED_WRITE_FILE_PIECE =
      "接收器 id = %s：写入文件片段失败，请求 %s。";
  public static final String FAILED_TO_WRITE_FILE_PIECE =
      "写入文件片段失败，原因：%s";
  public static final String RECEIVER_WRITING_FILE_NOT_EXIST =
      "接收器 id = {}：写入文件 {} 不存在或名称不正确，尝试创建。当前写入文件为 {}。";
  public static final String RECEIVER_FILE_DIR_CREATED =
      "接收器 id = {}：接收文件目录 {} 已创建。";
  public static final String RECEIVER_FAILED_CREATE_FILE_DIR =
      "接收器 id = {}：创建接收文件目录 {} 失败。";
  public static final String RECEIVER_WRITING_FILE_CREATED =
      "接收器 id = {}：写入文件 {} 已创建。准备写入文件片段。";
  public static final String RECEIVER_ILLEGAL_FILENAME =
      "接收器 id = %s：检查写入文件时遇到非法文件名 %s。";
  public static final String RECEIVER_PATH_TRAVERSAL =
      "接收器 id = {}：检测到路径遍历攻击！文件名：{}";
  public static final String RECEIVER_WRITER_CLOSED =
      "接收器 id = {}：当前写入文件 writer {} 已关闭，长度 {}。";
  public static final String RECEIVER_FAILED_CLOSE_WRITER =
      "接收器 id = %s：关闭当前写入文件 writer %s 失败，原因：%s。";
  public static final String RECEIVER_WRITER_NULL =
      "接收器 id = {}：当前写入文件 writer 为空。无需关闭。";
  public static final String RECEIVER_FILE_NULL =
      "接收器 id = {}：当前写入文件为空。无需删除。";
  public static final String RECEIVER_ORIGINAL_FILE_DELETED =
      "接收器 id = {}：原始写入文件 {} 已删除。";
  public static final String RECEIVER_FAILED_DELETE_FILE =
      "接收器 id = %s：删除原始写入文件 %s 失败，原因：%s。";
  public static final String RECEIVER_ORIGINAL_FILE_NOT_EXIST =
      "接收器 id = {}：原始文件 {} 不存在。无需删除。";
  public static final String RECEIVER_FILE_OFFSET_MISMATCH =
      "接收器 id = %s：写入文件 %s 的偏移量为 %s，但请求发送方的偏移量为 %s。";
  public static final String RECEIVER_FILE_NOT_AVAILABLE =
      "接收器 id = {}：写入文件 {} 不可用。"
          + "写入文件为空：{}，写入文件存在：{}，写入文件 writer 为空：{}。";
  public static final String FAILED_TO_SEAL_FILE_NOT_AVAILABLE =
      "封存文件失败，因为写入文件 %s 不可用。";
  public static final String FAILED_TO_SEAL_FILE_WRITING_FILE_MISMATCH =
      "封存文件 %s 失败，因为写入文件为 %s。";
  public static final String RECEIVER_FAILED_SEAL_FILE_WRITING =
      "接收器 id = %s：封存文件 %s 失败，因为写入文件为 %s。";
  public static final String FAILED_TO_SEAL_FILE_NOT_EXIST =
      "封存文件 %s 失败，文件不存在。";
  public static final String RECEIVER_FAILED_SEAL_FILE_NOT_EXIST =
      "接收器 id = %s：封存文件 %s 失败，因为文件不存在。";
  public static final String FAILED_TO_SEAL_FILE_LENGTH_INCORRECT =
      "封存文件 %s 失败，文件长度不正确。"
          + "原始文件长度为 %s，但接收文件长度为 %s。";
  public static final String RECEIVER_FAILED_SEAL_FILE_LENGTH_INCORRECT =
      "接收器 id = %s：封存文件 %s 失败，文件长度不正确。"
          + "原始文件长度为 %s，但接收文件长度为 %s。";
  public static final String RECEIVER_SEAL_FILE_SUCCESS =
      "接收器 id = {}：封存文件 {} 成功。";
  public static final String RECEIVER_FAILED_SEAL_FILE =
      "接收器 id = %s：封存文件 %s 失败，原因：%s。";
  public static final String RECEIVER_FAILED_SEAL_FILE_STATUS =
      "接收器 id = %s：封存文件 %s 失败，状态为 %s。";
  public static final String RECEIVER_FAILED_SEAL_FILE_FROM_REQ =
      "接收器 id = %s：从请求 %s 封存文件 %s 失败。";
  public static final String FAILED_TO_SEAL_FILE =
      "封存文件 %s 失败，原因：%s";
  public static final String FAILED_TO_SEAL_FILE_MULTI =
      "封存文件 %s 失败，因为写入文件 %s 不可用。";
  public static final String REQUEST_SENDER_RESET_OFFSET =
      "请求发送方将文件读取器偏移量从 %s 重置为 %s。";
  public static final String RECEIVER_EXIT_WRITER_CLOSED =
      "接收器 id = {}：处理退出：写入文件 writer 已关闭。";
  public static final String RECEIVER_EXIT_CLOSE_WRITER_ERROR =
      "接收器 id = {}：处理退出：关闭写入文件 writer 出错。";
  public static final String RECEIVER_EXIT_WRITER_NULL =
      "接收器 id = {}：处理退出：写入文件 writer 为空。无需关闭。";
  public static final String RECEIVER_EXIT_FILE_DELETED =
      "接收器 id = {}：处理退出：写入文件 {} 已删除。";
  public static final String RECEIVER_EXIT_DELETE_FILE_ERROR =
      "接收器 id = {}：处理退出：删除写入文件 {} 出错。";
  public static final String RECEIVER_EXIT_FILE_NULL =
      "接收器 id = {}：处理退出：写入文件为空。无需删除。";
  public static final String RECEIVER_EXIT_DIR_DELETED =
      "接收器 id = {}：处理退出：原始接收文件目录 {} 已删除。";
  public static final String RECEIVER_EXIT_DELETE_DIR_ERROR =
      "接收器 id = {}：处理退出：删除原始接收文件目录 {} 出错。";
  public static final String RECEIVER_EXIT_DIR_NOT_EXIST =
      "接收器 id = {}：处理退出：原始接收文件目录 {} 不存在。无需删除。";
  public static final String RECEIVER_EXIT_DIR_NULL =
      "接收器 id = {}：处理退出：原始接收文件目录为空。无需删除。";

  // ===================== PipeReceiverStatusHandler =====================

  public static final String IOT_CONSENSUS_RETRY_WITH_INTERVAL =
      "IoTConsensusV2：将以递增间隔重试。状态：{}";
  public static final String IOT_CONSENSUS_WILL_NOT_RETRY =
      "IoTConsensusV2：将不再重试。状态：{}";
  public static final String IDEMPOTENT_CONFLICT_IGNORED =
      "幂等冲突异常：将被忽略。状态：{}";
  public static final String TEMPORARY_UNAVAILABLE_RETRY =
      "临时不可用异常：将无限重试。状态：%s，消息：%s";
  public static final String USER_CONFLICT_IGNORED =
      "{}：无权限时跳过。将被忽略。事件：{}。状态：{}";
  public static final String USER_CONFLICT_RETRY_TIMEOUT =
      "用户冲突异常：重试超时。将被忽略。事件：{}。状态：{}";
  public static final String USER_CONFLICT_WILL_RETRY =
      "用户冲突异常：将重试 {}。状态：{}";
  public static final String USER_CONFLICT_NOT_ALLOWED =
      "用户冲突异常：因不允许重试而被忽略。事件：{}。状态：{}";
  public static final String OTHER_EXCEPTION_RETRY_TIMEOUT =
      "{}：重试超时。将被忽略。事件：{}。状态：{}";
  public static final String OTHER_EXCEPTION_RETRY_FOREVER =
      "%s：将无限重试。状态：%s，消息：%s";
  public static final String OTHER_EXCEPTION_RETRY_SECONDS =
      "{}：将至少重试 {} 秒。状态：{}";

  private PipeMessages() {}

  public static final String THROWING_EXCEPTION_IN_VALIDATE = "在 validate 中抛出异常";
  public static final String THROWING_EXCEPTION_IN_CUSTOMIZE = "在 customize 中抛出异常";
  public static final String THROWING_EXCEPTION_IN_PROCESS_TABLET = "在 process(TabletInsertionEvent, EventCollector) 中抛出异常";
  public static final String THROWING_EXCEPTION_IN_PROCESS_TSFILE = "在 process(TsFileInsertionEvent, EventCollector) 中抛出异常";
  public static final String THROWING_EXCEPTION_IN_PROCESS_EVENT = "在 process(Event, EventCollector) 中抛出异常";
  public static final String THROWING_EXCEPTION_IN_CLOSE = "在 close 中抛出异常";
  public static final String ILLEGAL_DB_OR_TABLE_PATTERN = "非法的数据库或表模式。详情：";
  public static final String EVENT_NOT_SUPPORT_BINDING_PROGRESS_INDEX = "该事件不支持绑定 progressIndex。";
  public static final String UNSUPPORTED_VERSION = "不支持的版本 %s";

  // ===================== PipePluginAgent (additional) =====================

  public static final String FAILED_TO_CLOSE_PROCESSOR_AFTER_INIT =
      "初始化 processor 失败后关闭 processor 失败。忽略此异常。";

  // ===================== PipePluginExecutableManager (additional) =====================

  public static final String FAILED_TO_COMPUTE_MD5 =
      "注册函数 %s 失败，因为计算函数 %s 的 jar 文件 MD5 时发生错误 ";

  // ===================== PipeInclusionOptions (additional) =====================

  public static final String ILLEGAL_OPTIONS_CHECKING_PRESENCE =
      "检查是否存在至少一个选项时解析到非法选项（inclusion：{}，exclusion：{}）：{}";
  public static final String ILLEGAL_OPTIONS_CHECKING_LEGAL =
      "检查所有选项是否合法时解析到非法选项 {}：{}";

  // ===================== PipeEventCommitMetrics (additional) =====================

  public static final String FAILED_TO_DEREGISTER_COMMIT_METRICS =
      "注销 pipe 事件提交指标失败，PipeEventCommitter({}) 不存在";

  // ===================== IoTDBSyncClientManager (additional) =====================

  public static final String FAILED_TO_CLOSE_CLIENT_WITH_TARGET =
      "关闭与目标服务器的客户端失败（ip：{}，端口：{}），原因：{}。已忽略。";
  public static final String HANDSHAKE_ERROR_WITH_TARGET_RETRY =
      "与目标服务器握手出错（ip：{}，端口：{}），原因：{}。"
          + "将使用 PipeTransferHandshakeV1Req 重试握手。";
  public static final String HANDSHAKE_ERROR_WITH_TARGET_SERVER =
      "与目标服务器握手出错（ip：{}，端口：{}），原因：{}。";
  public static final String HANDSHAKE_SUCCESS_TARGET =
      "握手成功。目标服务器 ip：{}，端口：{}";
  public static final String ALL_CLIENTS_DEAD =
      "所有客户端均不可用，请检查与接收器的连接。";
  public static final String ALL_SOCKETS_DEAD =
      "所有 socket 均不可用，请检查与接收器的连接。";
  public static final String ALL_TARGET_SERVERS_NOT_AVAILABLE =
      "所有目标服务器 %s 均不可用。";

  // ===================== IoTDBAirGapSink (additional) =====================

  public static final String BATCH_MODE_NOT_SUPPORTED =
      "参数中启用了批量模式。IoTDBAirGapConnector 不支持批量模式。已禁用批量模式。";
  public static final String FAILED_TO_RECONNECT =
      "重新连接目标服务器失败，原因：{}。稍后将重试连接。";

  // ===================== IoTDBReceiverAgent (additional) =====================

  public static final String RECEIVER_VERSION_MISMATCH =
      "接收器版本 {} 与发送器版本 {} 不同，接收器将被重置为发送器版本。";
  public static final String UNSUPPORTED_PIPE_VERSION = "不支持的 pipe 版本 %d";

  // ===================== PipeEventCommitManager (additional) =====================

  public static final String FAILED_TO_MARK_COMMIT_RATE =
      "标记 pipe 任务提交速率失败：{}，堆栈跟踪：{}";
  public static final String STALE_PIPE_EVENT_COMMITTER =
      "过期的 PipeEventCommitter({})，提交事件：{}，当前重启次数 {}";
  public static final String MISSING_PIPE_EVENT_COMMITTER =
      "缺失的 PipeEventCommitter({})，提交事件：{}，堆栈跟踪：{}";

  // ===================== IoTDBSink (additional) =====================

  public static final String PARSE_URL_ERROR =
      "解析目标服务器节点 URL 时发生异常：{}";
  public static final String PARSE_URL_ERROR_MESSAGE =
      "解析目标服务器节点 URL 时发生错误，请检查指定的 'host':'port' 或 'node-urls'";
  // ===================== SubscriptionConfig (printAllConfigs) =====================

  public static final String CONFIG_SUBSCRIPTION_CACHE_MEMORY_USAGE_PERCENTAGE =
      "SubscriptionCacheMemoryUsagePercentage: {}";
  public static final String CONFIG_SUBSCRIPTION_SUBTASK_EXECUTOR_MAX_THREAD_NUM =
      "SubscriptionSubtaskExecutorMaxThreadNum: {}";
  public static final String CONFIG_SUBSCRIPTION_PREFETCH_TABLET_BATCH_MAX_DELAY_IN_MS =
      "SubscriptionPrefetchTabletBatchMaxDelayInMs: {}";
  public static final String CONFIG_SUBSCRIPTION_PREFETCH_TABLET_BATCH_MAX_SIZE_IN_BYTES =
      "SubscriptionPrefetchTabletBatchMaxSizeInBytes: {}";
  public static final String CONFIG_SUBSCRIPTION_PREFETCH_TSFILE_BATCH_MAX_DELAY_IN_MS =
      "SubscriptionPrefetchTsFileBatchMaxDelayInMs: {}";
  public static final String CONFIG_SUBSCRIPTION_PREFETCH_TSFILE_BATCH_MAX_SIZE_IN_BYTES =
      "SubscriptionPrefetchTsFileBatchMaxSizeInBytes: {}";
  public static final String CONFIG_SUBSCRIPTION_POLL_MAX_BLOCKING_TIME_MS =
      "SubscriptionPollMaxBlockingTimeMs: {}";
  public static final String CONFIG_SUBSCRIPTION_DEFAULT_TIMEOUT_IN_MS =
      "SubscriptionDefaultTimeoutInMs: {}";
  public static final String CONFIG_SUBSCRIPTION_LAUNCH_RETRY_INTERVAL_MS =
      "SubscriptionLaunchRetryIntervalMs: {}";
  public static final String CONFIG_SUBSCRIPTION_RECYCLE_UNCOMMITTED_EVENT_INTERVAL_MS =
      "SubscriptionRecycleUncommittedEventIntervalMs: {}";
  public static final String CONFIG_SUBSCRIPTION_READ_FILE_BUFFER_SIZE =
      "SubscriptionReadFileBufferSize: {}";
  public static final String CONFIG_SUBSCRIPTION_READ_TABLET_BUFFER_SIZE =
      "SubscriptionReadTabletBufferSize: {}";
  public static final String CONFIG_SUBSCRIPTION_TSFILE_DEDUPLICATION_WINDOW_SECONDS =
      "SubscriptionTsFileDeduplicationWindowSeconds: {}";
  public static final String CONFIG_SUBSCRIPTION_CHECK_MEMORY_ENOUGH_INTERVAL_MS =
      "SubscriptionCheckMemoryEnoughIntervalMs: {}";
  public static final String CONFIG_SUBSCRIPTION_ESTIMATED_INSERT_NODE_TABLET_INSERTION_EVENT_SIZE =
      "SubscriptionEstimatedInsertNodeTabletInsertionEventSize: {}";
  public static final String CONFIG_SUBSCRIPTION_ESTIMATED_RAW_TABLET_INSERTION_EVENT_SIZE =
      "SubscriptionEstimatedRawTabletInsertionEventSize: {}";
  public static final String CONFIG_SUBSCRIPTION_MAX_ALLOWED_EVENT_COUNT_IN_TABLET_BATCH =
      "SubscriptionMaxAllowedEventCountInTabletBatch: {}";
  public static final String CONFIG_SUBSCRIPTION_LOG_MANAGER_WINDOW_SECONDS =
      "SubscriptionLogManagerWindowSeconds: {}";
  public static final String CONFIG_SUBSCRIPTION_LOG_MANAGER_BASE_INTERVAL_MS =
      "SubscriptionLogManagerBaseIntervalMs: {}";
  public static final String CONFIG_SUBSCRIPTION_PREFETCH_ENABLED =
      "SubscriptionPrefetchEnabled: {}";
  public static final String CONFIG_SUBSCRIPTION_PREFETCH_MEMORY_THRESHOLD =
      "SubscriptionPrefetchMemoryThreshold: {}";
  public static final String CONFIG_SUBSCRIPTION_PREFETCH_MISSING_RATE_THRESHOLD =
      "SubscriptionPrefetchMissingRateThreshold: {}";
  public static final String CONFIG_SUBSCRIPTION_PREFETCH_EVENT_LOCAL_COUNT_THRESHOLD =
      "SubscriptionPrefetchEventLocalCountThreshold: {}";
  public static final String CONFIG_SUBSCRIPTION_PREFETCH_EVENT_GLOBAL_COUNT_THRESHOLD =
      "SubscriptionPrefetchEventGlobalCountThreshold: {}";
  public static final String CONFIG_SUBSCRIPTION_META_SYNCER_INITIAL_SYNC_DELAY_MINUTES =
      "SubscriptionMetaSyncerInitialSyncDelayMinutes: {}";
  public static final String CONFIG_SUBSCRIPTION_META_SYNCER_SYNC_INTERVAL_MINUTES =
      "SubscriptionMetaSyncerSyncIntervalMinutes: {}";

}
