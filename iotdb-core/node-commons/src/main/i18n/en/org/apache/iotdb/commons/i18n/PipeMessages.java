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

  public static final String UTILITY_CLASS = "Utility class";

  // ===================== PipeConfig (printAllConfigs) =====================
  // All use SLF4J {} placeholder pattern: "ConfigName: {}"

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
      "Failed to close temporary source: {}";
  public static final String FAILED_TO_CLOSE_TEMPORARY_PROCESSOR =
      "Failed to close temporary processor: {}";
  public static final String FAILED_TO_CLOSE_TEMPORARY_CONNECTOR =
      "Failed to close temporary connector: {}";

  // ===================== PipePluginExecutableManager =====================

  public static final String FAILED_TO_READ_MD5_FOR_PIPE_PLUGIN =
      "Failed to read md5 from txt file for pipe plugin {}";

  // ===================== AbstractPipePeriodicalJobExecutor =====================

  public static final String PERIODICAL_JOB_FAILED = "Periodical job {} failed.";
  public static final String PERIODICAL_JOB_REGISTERED =
      "Pipe periodical job {} is registered successfully. Interval: {} seconds.";
  public static final String PERIODICAL_JOB_EXECUTOR_STARTED =
      "Pipe periodical job executor is started successfully.";
  public static final String PERIODICAL_JOB_EXECUTOR_STOPPED =
      "Pipe periodical job executor is stopped successfully.";
  public static final String PERIODICAL_JOBS_CLEARED =
      "All pipe periodical jobs are cleared successfully.";

  // ===================== PipeTaskAgent =====================

  public static final String NOT_ENOUGH_MEMORY_FOR_PIPE = "Not enough memory for pipe.";
  public static final String NOT_ENOUGH_MEMORY_FOR_PIPE_FORMAT =
      NOT_ENOUGH_MEMORY_FOR_PIPE
          + " Need memory: %d bytes, free memory: %d bytes, "
          + "reserved memory: %d bytes, total memory: %d bytes";
  public static final String NOT_ENOUGH_FLOATING_MEMORY_FOR_PIPE_FORMAT =
      NOT_ENOUGH_MEMORY_FOR_PIPE
          + " Need floating memory: %d bytes, free floating memory: %d bytes";
  public static final String UNKNOWN_PIPE_STATUS = "Unknown pipe status %s for pipe %s";
  public static final String UNEXPECTED_PIPE_STATUS = "Unexpected pipe status %s: ";
  public static final String INTERRUPTED_ACQUIRING_READ_LOCK =
      "Interruption during requiring pipeMetaKeeper read lock.";
  public static final String INTERRUPTED_ACQUIRING_WRITE_LOCK =
      "Interruption during requiring pipeMetaKeeper write lock.";
  public static final String FAILED_TO_HANDLE_SINGLE_PIPE_META_CHANGES =
      "Failed to handle single pipe meta changes for {}";
  public static final String FAILED_TO_HANDLE_SINGLE_PIPE_META_CHANGES_FORMAT =
      "Failed to handle single pipe meta changes for %s, because %s";
  public static final String FAILED_TO_DROP_PIPE = "Failed to drop pipe {}";
  public static final String FAILED_TO_DROP_PIPE_FORMAT =
      "Failed to drop pipe %s, because %s";
  public static final String FAILED_TO_ACQUIRE_LOCK_DROPPING_ALL =
      "Failed to acquire lock when dropping all pipe tasks, will skip dropping";
  public static final String FAILED_TO_DROP_PIPE_WITH_CREATION_TIME =
      "Failed to drop pipe {} with creation time {}";
  public static final String STOPPED_ALL_PIPES_WITH_CRITICAL_EXCEPTION =
      "Stopped all pipes with critical exception.";
  public static final String FAILED_TO_STOP_ALL_PIPES_RETRY =
      "Failed to stop all pipes with critical exception, retry count: {}.";
  public static final String INTERRUPTED_STOPPING_ALL_PIPES =
      "Interrupted when trying to stop all pipes with critical exception, exception message: {}";
  public static final String FAILED_TO_STOP_ALL_PIPES =
      "Failed to stop all pipes with critical exception, exception message: {}";
  public static final String FAILED_TO_HANDLE_PIPE_META_CHANGES_FORMAT =
      "Failed to handle pipe meta changes for %s, because %s";
  public static final String FAILED_TO_HANDLE_PIPE_META_CHANGES_LOG =
      "Failed to handle pipe meta changes for %s";
  public static final String CREATE_PIPE_FAILED_ENCRYPTION =
      "Failed to create Pipe %s because TSFile is configured with encryption, which prohibits the use of Pipe";
  public static final String PIPE_ALREADY_CREATED =
      "Pipe {} (creation time = {}) has already been created. Current status = {}. Skip creating.";
  public static final String PIPE_ALREADY_DROPPED_RECREATING =
      "Pipe {} (creation time = {}) has already been dropped, "
          + "but the pipe task meta has not been cleaned up. "
          + "Current status = {}. Try dropping the pipe and recreating it.";
  public static final String PIPE_ALREADY_DROPPED_SKIP_NO_CREATION_TIME =
      "Pipe {} has already been dropped or has not been created. Skip dropping.";
  public static final String PIPE_ALREADY_DROPPED_SKIP_STARTING =
      "Pipe {} (creation time = {}) has already been dropped or has not been created. "
          + "Skip starting.";
  public static final String PIPE_ALREADY_DROPPED_SKIP_STOPPING =
      "Pipe {} (creation time = {}) has already been dropped or has not been created. "
          + "Skip stopping.";
  public static final String PIPE_CREATION_TIME_MISMATCH_STARTING =
      "Pipe {} (creation time = {}) has been created but does not match "
          + "the creation time ({}) in startPipe request. Skip starting.";
  public static final String PIPE_CREATION_TIME_MISMATCH_STOPPING =
      "Pipe {} (creation time = {}) has been created but does not match "
          + "the creation time ({}) in stopPipe request. Skip stopping.";
  public static final String PIPE_CREATION_TIME_MISMATCH_DROPPING =
      "Pipe {} (creation time = {}) has been created but does not match "
          + "the creation time ({}) in dropPipe request. Skip dropping.";
  public static final String PIPE_STARTED_CURRENT_STATUS =
      "Pipe {} (creation time = {}) has been created. Current status = {}. Starting.";
  public static final String PIPE_ALREADY_STARTED =
      "Pipe {} (creation time = {}) has already been started. Current status = {}. "
          + "Skip starting.";
  public static final String PIPE_ALREADY_DROPPED_SKIP_STARTING_STATUS =
      "Pipe {} (creation time = {}) has already been dropped. Current status = {}. "
          + "Skip starting.";
  public static final String PIPE_ALREADY_STOPPED =
      "Pipe {} (creation time = {}) has already been stopped. Current status = {}. "
          + "Skip stopping.";
  public static final String PIPE_RUNNING_STOPPING =
      "Pipe {} (creation time = {}) has been started. Current status = {}. Stopping.";
  public static final String PIPE_ALREADY_DROPPED_SKIP_STOPPING_STATUS =
      "Pipe {} (creation time = {}) has already been dropped. Current status = {}. "
          + "Skip stopping.";
  public static final String CREATE_ALL_PIPE_TASKS =
      "Create all pipe tasks on Pipe {} successfully within {} ms";
  public static final String DROP_ALL_PIPE_TASKS =
      "Drop all pipe tasks on Pipe {} successfully within {} ms";
  public static final String START_ALL_PIPE_TASKS =
      "Start all pipe tasks on Pipe {} successfully within {} ms";
  public static final String STOP_ALL_PIPE_TASKS =
      "Stop all pipe tasks on Pipe {} successfully within {} ms";
  public static final String PIPE_ALREADY_DROPPED_OR_NOT_CREATED_SKIP =
      "Pipe {} (creation time = {}) has already been dropped or has not been created. "
          + "Skip dropping.";
  public static final String PIPE_STOPPED_CRITICAL_EXCEPTION =
      "Pipe %s (creation time = %s) will be stopped because of critical exception "
          + "(occurred time %s) in connector %s.";
  public static final String PIPE_WAS_STOPPED_CRITICAL_EXCEPTION =
      "Pipe %s (creation time = %s) was stopped because of critical exception "
          + "(occurred time %s).";

  // ===================== PipeEventCommitManager =====================

  public static final String PIPE_COMMITTER_REGISTERED =
      "Pipe committer registered for pipe on region: {}";
  public static final String PIPE_COMMITTER_OVERWRITING =
      "Pipe with same name is already registered on this region, overwriting: {}";
  public static final String PIPE_COMMITTER_DEREGISTERED =
      "Pipe committer deregistered for pipe on region: {}";

  // ===================== BlockingPendingQueue =====================

  public static final String PENDING_QUEUE_PUT_INTERRUPTED =
      "pending queue put is interrupted.";
  public static final String PENDING_QUEUE_POLL_INTERRUPTED =
      "pending queue poll is interrupted.";

  // ===================== PipeSubtaskExecutor =====================

  public static final String SUBTASK_ALREADY_REGISTERED =
      "The subtask {} is already registered.";
  public static final String SUBTASK_NOT_REGISTERED = "The subtask {} is not registered.";
  public static final String SUBTASK_ALREADY_RUNNING = "The subtask {} is already running.";
  public static final String SUBTASK_STARTED = "The subtask {} is started to submit self.";
  public static final String SUBTASK_CLOSED = "The subtask {} is closed successfully.";
  public static final String SUBTASK_CLOSE_FAILED = "Failed to close the subtask {}.";

  // ===================== PipeReportableSubtask =====================

  public static final String ON_FAILURE_IGNORED_PIPE_DROPPED =
      "onFailure in pipe subtask, ignored because pipe is dropped.";
  public static final String INTERRUPTED_WAITING_HIGH_PRIORITY =
      "Interrupted while waiting for the high priority lock task.";
  public static final String FAILED_TO_EXECUTE_SUBTASK =
      "Failed to execute subtask {} (creation time: {}, simple class: {}), because of {}. Will retry for {} times.";
  public static final String RETRY_EXECUTING_SUBTASK =
      "Retry executing subtask {} (creation time: {}, simple class: {}), retry count [{}/{}], last exception: {}";
  public static final String INTERRUPTED_RETRYING_SUBTASK =
      "Interrupted when retrying to execute subtask {} (creation time: {}, simple class: {})";
  public static final String SUBTASK_RETRY_EXCEEDED_FORMAT =
      "Failed to execute subtask %s (creation time: %s, simple class: %s), "
          + "retry count exceeds the max retry times %d, last exception: %s, root cause: %s";
  public static final String SUBTASK_EXCEPTION_REPORTED =
      "The last event is an instance of EnrichedEvent, so the exception is reported. "
          + "Stopping current pipe subtask {} (creation time: {}, simple class: {}) locally... "
          + "Status shown when query the pipe will be 'STOPPED'. "
          + "Please restart the task by executing 'START PIPE' manually if needed.";
  public static final String FAILED_TO_EXECUTE_SUBTASK_RETRY_FOREVER =
      "Failed to execute subtask {} (creation time: {}, simple class: {}), "
          + "because of {}. Will retry forever.";
  public static final String RETRY_EXECUTING_SUBTASK_FOREVER =
      "Retry executing subtask {} (creation time: {}, simple class: {}), retry count {}, last exception: {}";
  public static final String PATTERN_INCLUSION_CANNOT_BE_USED_WITH_PATTERN_OR_PATH =
      "Pipe: %s cannot be used together with %s or %s.";
  public static final String PATTERN_INCLUSION_CANNOT_BE_USED_WITH_PATH_EXCLUSION =
      "Pipe: %s cannot be used together with %s.";
  public static final String PATH_AND_PATTERN_CANNOT_BE_USED_TOGETHER =
      "Pipe: %s and %s cannot be used together.";
  public static final String PARAMETER_ONLY_SUPPORTS_SINGLE_PATTERN =
      "Pipe: The parameter %s only supports a single pattern now.";
  public static final String FAILED_TO_PERFORM_PATTERN_COVERAGE_CHECK =
      "Pipe: Failed to perform pattern coverage check for inclusion [{}] and exclusion [{}].";
  public static final String EXCLUSION_PATTERN_FULLY_COVERS_INCLUSION_PATTERN =
      "Pipe: The provided exclusion pattern fully covers the inclusion pattern. This pipe pattern will match nothing. Inclusion: [%s], Exclusion: [%s]";
  public static final String EXCLUSION_PATTERN_COVERS_PART_OF_INCLUSION_PATHS =
      "Pipe: The provided exclusion pattern covers {} out of {} inclusion paths. These paths will be excluded. Inclusion: [{}], Exclusion: [{}]";

  // ===================== PipeAbstractSinkSubtask =====================

  public static final String ON_FAILURE_IGNORED_CONNECTOR_DROPPED =
      "onFailure in pipe transfer, ignored because the connector subtask is dropped.";
  public static final String ON_FAILURE_IGNORED_EVENT_RELEASED =
      "onFailure in pipe transfer, ignored because the failure event is released.";
  public static final String ON_FAILURE_IGNORED_EVENT_PIPE_DROPPED =
      "onFailure in pipe transfer, ignored because the failure event's pipe is dropped.";
  public static final String NON_CRITICAL_EXCEPTION_WILL_THROW_CRITICAL =
      "A non PipeRuntimeSinkCriticalException occurred, will throw a PipeRuntimeSinkCriticalException.";
  public static final String PIPE_CONNECTION_EXCEPTION_RETRYING =
      "PipeConnectionException occurred, %s retries to handshake with the target system.";
  public static final String HANDSHAKE_SUCCESS = "{} handshakes with the target system successfully.";
  public static final String HANDSHAKE_FAILED_RETRYING =
      "{} failed to handshake with the target system for {} times, "
          + "will retry at most {} times.";
  public static final String INTERRUPTED_WHILE_SLEEPING_RETRY_HANDSHAKE =
      "Interrupted while sleeping, will retry to handshake with the target system.";
  public static final String HANDSHAKE_FAILED_STOPPING =
      "{} failed to handshake with the target system after {} times, "
          + "stopping current subtask {} (creation time: {}, simple class: {}). "
          + "Status shown when query the pipe will be 'STOPPED'. "
          + "Please restart the task by executing 'START PIPE' manually if needed.";
  public static final String TEMPORARILY_OUT_OF_MEMORY =
      "Temporarily out of memory in pipe event transferring, will wait for the memory to release.";
  public static final String EXCEPTION_IN_PIPE_TRANSFER_FORMAT =
      "Exception in pipe transfer, subtask: %s, last event: %s, root cause: %s";
  public static final String EXCEPTION_IN_PIPE_TRANSFER_IGNORED =
      "Exception in pipe transfer, ignored because the sink subtask is dropped.{}";
  public static final String PIPE_EXCEPTION_IGNORED =
      "{} in pipe transfer, ignored because the connector subtask is dropped.{}";

  // ===================== PipeCompressorFactory =====================

  public static final String COMPRESSOR_CONFIG_NULL = "PipeCompressorConfig is null";
  public static final String COMPRESSOR_CONFIG_NAME_NULL =
      "PipeCompressorConfig.getName() is null";
  public static final String CREATE_ZSTD_COMPRESSOR =
      "Create new PipeZSTDCompressor with level: {}";
  public static final String COMPRESSOR_NOT_FOUND_BY_NAME =
      "PipeCompressor not found for name: %s";
  public static final String COMPRESSOR_NOT_FOUND_BY_INDEX =
      "PipeCompressor not found for index: %s";

  // ===================== PipeTransferSliceReqHandler =====================

  public static final String INVALID_STATE_SLICE =
      "Invalid state: orderId={}, originReqType={}, originBodySize={}, sliceCount={}, sliceBodies.size={}";
  public static final String ORDER_ID_MISMATCH =
      "Order ID mismatch: expected {}, actual {}";
  public static final String SLICE_COUNT_MISMATCH =
      "Slice count mismatch: expected {}, actual {}";

  // ===================== IoTDBSink =====================

  public static final String IOTDB_SINK_NODE_URLS = "IoTDBSink nodeUrls: {}";
  public static final String IOTDB_SINK_TABLET_BATCH_MODE =
      "IoTDBSink isTabletBatchModeEnabled: {}";
  public static final String IOTDB_SINK_MARK_AS_PIPE_REQUEST =
      "IoTDBSink shouldMarkAsPipeRequest: {}";
  public static final String IOTDB_SINK_SKIP_IF_NO_PRIVILEGES =
      "IoTDBSink skipIfNoPrivileges: {}";
  public static final String HOST_CANNOT_BE_EMPTY = "host cannot be empty";
  public static final String PORT_CANNOT_BE_EMPTY = "port cannot be empty";

  // ===================== IoTDBSslSyncSink =====================

  public static final String SYNC_CLIENT_MANAGER_CLOSED =
      "IoTDB sync client manager has been closed";
  public static final String REDIRECT_FILE_POSITION = "Redirect file position to {}.";

  // ===================== IoTDBAirGapSink =====================

  public static final String AIR_GAP_CUSTOMIZED_HANDSHAKE_TIMEOUT =
      "IoTDBAirGapConnector is customized with handshakeTimeoutMs: {}.";
  public static final String AIR_GAP_CUSTOMIZED_E_LANGUAGE =
      "IoTDBAirGapConnector is customized with eLanguageEnable: {}.";
  public static final String CONNECTED_TO_TARGET_SERVER =
      "Successfully connected to target server ip: {}, port: {}.";
  public static final String FAILED_TO_CLOSE_SOCKET =
      "Failed to close socket with target server ip: {}, port: {}, because: {}. Ignore it.";
  public static final String FAILED_TO_CONNECT_TO_TARGET =
      "Failed to connect to target server ip: {}, port: {}, because: {}. Ignore it.";
  public static final String HANDSHAKE_ERROR_RECEIVING_END =
      "Handshake error occurs. It may be caused by an error on the receiving end. Ignore it.";
  public static final String HANDSHAKE_ERROR_WITH_TARGET =
      "Handshake error with target server, socket: %s";
  public static final String HANDSHAKE_SUCCESS_SOCKET = "Handshake success. Socket: {}";
  public static final String FAILED_TO_CLOSE_CLIENT = "Failed to close client {}.";
  public static final String UNKNOWN_LOAD_BALANCE_STRATEGY =
      "Unknown load balance strategy: {}, use round-robin strategy instead.";

  // ===================== IoTDBSyncClientManager =====================

  public static final String CLIENT_CLOSED = "Client {}:{} closed.";
  public static final String FAILED_TO_CLOSE_CLIENT_ENDPOINT =
      "Failed to close client {}:{}, because: {}.";
  public static final String FAILED_TO_INIT_CLIENT =
      "Failed to initialize client with target server ip: %s, port: %s, because %s";

  // ===================== IoTDBNonDataRegionSource =====================

  public static final String CANNOT_GET_NEWEST_SNAPSHOT =
      "Cannot get the newest snapshot after triggering one.";

  // ===================== PipeInclusionOptions =====================

  public static final String ILLEGAL_PATH_INITIALIZING_OPTIONS =
      "Illegal path encountered when initializing LEGAL_OPTIONS.";

  // ===================== IoTDBTreePattern / TablePattern =====================

  public static final String ILLEGAL_IOTDB_PIPE_PATTERN =
      "Illegal IoTDBPipePattern: %s";
  // ===================== ConcurrentIterableLinkedQueue =====================

  public static final String NULL_ELEMENT_NOT_ALLOWED = "Null element is not allowed.";
  public static final String CALLING_NEXT_ON_CLOSED_ITERATOR =
      "Calling next() to a closed iterator, will return null.";
  public static final String INTERRUPTED_WAITING_FOR_NEXT =
      "Interrupted while waiting for next element.";

  // ===================== PlainQueueSerializer =====================

  public static final String FAILED_TO_LOAD_SNAPSHOT = "Failed to load snapshot.";

  // ===================== AbstractSerializableListeningQueue =====================

  public static final String UNKNOWN_SERIALIZER_TYPE =
      "Unknown serializer type: %s";

  // ===================== EnrichedEvent =====================

  // ===================== PipeEventCommitMetrics =====================

  public static final String FAILED_TO_UNBIND_COMMIT_METRICS =
      "Failed to unbind from pipe event commit metrics, event committer map not empty";

  // ===================== PipePhantomReferenceManager =====================

  public static final String CLEAN_PHANTOM_REFERENCES =
      "Clean {} pipe phantom reference(s) successfully within {} ms, remaining reference count: {}";
  public static final String REMAINING_PHANTOM_REFERENCE_COUNT =
      "Remaining pipe phantom reference count: {}";
  public static final String PIPE_EVENT_RESOURCE_LEAK =
      "PIPE EVENT RESOURCE LEAK DETECTED ({}): {}";

  // ===================== PipeSnapshotResourceManager =====================

  public static final String CANNOT_FIND_SNAPSHOT_PATH =
      "Cannot find correct target snapshot path in pipe dir for {}";
  public static final String CANNOT_FIND_SNAPSHOT_PATH_EXCEPTION =
      "Cannot find correct target snapshot path in pipe dir for %s";

  // ===================== IoTDBReceiverAgent =====================

  public static final String CLEAN_RECEIVER_DIR_SUCCESS =
      "Clean pipe receiver dir {} successfully.";
  public static final String CLEAN_RECEIVER_DIR_FAILED =
      "Clean pipe receiver dir {} failed.";
  public static final String CREATE_RECEIVER_DIR_SUCCESS =
      "Create pipe receiver dir {} successfully.";
  public static final String CREATE_RECEIVER_DIR_FAILED =
      "Create pipe receiver dir {} failed.";

  // ===================== PipeReceiverFilePathUtils =====================

  public static final String ILLEGAL_FILENAME_PATH_TRAVERSAL =
      "Illegal fileName: %s (Path traversal detected)";

  // ===================== IoTDBFileReceiver =====================

  public static final String RECEIVER_HANDSHAKE_FAILED =
      "Handshake failed, response status = %s.";
  public static final String RECEIVER_HANDSHAKE_FAILED_WITH_ID =
      "Receiver id = %s: Handshake failed, response status = %s.";
  public static final String RECEIVER_HANDSHAKE_FAILED_LOGIN =
      "Receiver id = %s: Handshake failed because login failed, response status = %s.";
  public static final String RECEIVER_USER_LOGIN_SUCCESS =
      "Receiver id = {}: User {} login successfully.";
  public static final String RECEIVER_EXITED =
      "Receiver id = {}: Handling exit: Receiver exited.";
  public static final String RECEIVER_ORIGINAL_DIR_DELETED =
      "Receiver id = {}: Original receiver file dir {} was deleted.";
  public static final String RECEIVER_FAILED_DELETE_ORIGINAL_DIR =
      "Receiver id = %s: Failed to delete original receiver file dir %s, because %s.";
  public static final String RECEIVER_ORIGINAL_DIR_NOT_EXIST =
      "Receiver id = {}: Original receiver file dir {} is not existed. No need to delete.";
  public static final String RECEIVER_DIR_NULL_NO_DELETE =
      "Receiver id = {}: Current receiver file dir is null. No need to delete.";
  public static final String RECEIVER_FAILED_INIT_FOLDER_FULL =
      "Receiver id = %s: Failed to init pipe receiver file folder manager because all disks of folders are full.";
  public static final String RECEIVER_FAILED_CREATE_FOLDER_FULL =
      "Receiver id = %s: Failed to create pipe receiver file folder because all disks of folders are full.";
  public static final String RECEIVER_HANDSHAKE_SUCCESS =
      "Receiver id = {}: Handshake successfully! Sender's host = {}, port = {}. Receiver's file dir = {}.";
  public static final String RECEIVER_FAILED_CREATE_DIR =
      "Receiver id = %s: Failed to create receiver file dir %s.";
  public static final String RECEIVER_FAILED_CREATE_DIR_STATUS =
      "Failed to create receiver file dir %s.";
  public static final String RECEIVER_CANNOT_GET_CLUSTER_ID =
      "Receiver can not get clusterId from config node.";
  public static final String RECEIVER_NO_CLUSTER_ID_IN_REQUEST =
      "Handshake request does not contain clusterId.";
  public static final String RECEIVER_SAME_CLUSTER =
      "Receiver and sender are from the same cluster %s.";
  public static final String RECEIVER_NO_TIMESTAMP_PRECISION =
      "Handshake request does not contain timestampPrecision.";
  public static final String RECEIVER_TIMESTAMP_PRECISION_MISMATCH =
      "IoTDB receiver's timestamp precision %s, "
          + "connector's timestamp precision %s. Validation fails.";
  public static final String RECEIVER_FAILED_LOGIN =
      "Receiver id = %s: Failed to login, username = %s, response = %s.";
  public static final String RECEIVER_FILE_OFFSET_RESET =
      "Receiver id = %s: File offset reset requested by receiver, response status = %s.";
  public static final String RECEIVER_FAILED_WRITE_FILE_PIECE =
      "Receiver id = %s: Failed to write file piece from req %s.";
  public static final String FAILED_TO_WRITE_FILE_PIECE =
      "Failed to write file piece, because %s";
  public static final String RECEIVER_WRITING_FILE_NOT_EXIST =
      "Receiver id = {}: Writing file {} is not existed or name is not correct, try to create it. "
          + "Current writing file is {}.";
  public static final String RECEIVER_FILE_DIR_CREATED =
      "Receiver id = {}: Receiver file dir {} was created.";
  public static final String RECEIVER_FAILED_CREATE_FILE_DIR =
      "Receiver id = {}: Failed to create receiver file dir {}.";
  public static final String RECEIVER_WRITING_FILE_CREATED =
      "Receiver id = {}: Writing file {} was created. Ready to write file pieces.";
  public static final String RECEIVER_ILLEGAL_FILENAME =
      "Receiver id = %s: Illegal file name %s when checking writing file.";
  public static final String RECEIVER_PATH_TRAVERSAL =
      "Receiver id = {}: Path traversal attempt detected! Filename: {}";
  public static final String RECEIVER_WRITER_CLOSED =
      "Receiver id = {}: Current writing file writer {} was closed, length {}.";
  public static final String RECEIVER_FAILED_CLOSE_WRITER =
      "Receiver id = %s: Failed to close current writing file writer %s, because %s.";
  public static final String RECEIVER_WRITER_NULL =
      "Receiver id = {}: Current writing file writer is null. No need to close.";
  public static final String RECEIVER_FILE_NULL =
      "Receiver id = {}: Current writing file is null. No need to delete.";
  public static final String RECEIVER_ORIGINAL_FILE_DELETED =
      "Receiver id = {}: Original writing file {} was deleted.";
  public static final String RECEIVER_FAILED_DELETE_FILE =
      "Receiver id = %s: Failed to delete original writing file %s, because %s.";
  public static final String RECEIVER_ORIGINAL_FILE_NOT_EXIST =
      "Receiver id = {}: Original file {} is not existed. No need to delete.";
  public static final String RECEIVER_FILE_OFFSET_MISMATCH =
      "Receiver id = %s: Writing file %s's offset is %s, but request sender's offset is %s.";
  public static final String RECEIVER_FILE_NOT_AVAILABLE =
      "Receiver id = {}: Writing file {} is not available. "
          + "Writing file is null: {}, writing file exists: {}, writing file writer is null: {}.";
  public static final String FAILED_TO_SEAL_FILE_NOT_AVAILABLE =
      "Failed to seal file, because writing file %s is not available.";
  public static final String FAILED_TO_SEAL_FILE_WRITING_FILE_MISMATCH =
      "Failed to seal file %s, because writing file is %s.";
  public static final String RECEIVER_FAILED_SEAL_FILE_WRITING =
      "Receiver id = %s: Failed to seal file %s, because writing file is %s.";
  public static final String FAILED_TO_SEAL_FILE_NOT_EXIST =
      "Failed to seal file %s, the file does not exist.";
  public static final String RECEIVER_FAILED_SEAL_FILE_NOT_EXIST =
      "Receiver id = %s: Failed to seal file %s, because the file does not exist.";
  public static final String FAILED_TO_SEAL_FILE_LENGTH_INCORRECT =
      "Failed to seal file %s, because the length of file is not correct. "
          + "The original file has length %s, but receiver file has length %s.";
  public static final String RECEIVER_FAILED_SEAL_FILE_LENGTH_INCORRECT =
      "Receiver id = %s: Failed to seal file %s, because the length of file is not correct. "
          + "The original file has length %s, but receiver file has length %s.";
  public static final String RECEIVER_SEAL_FILE_SUCCESS =
      "Receiver id = {}: Seal file {} successfully.";
  public static final String RECEIVER_FAILED_SEAL_FILE =
      "Receiver id = %s: Failed to seal file %s, because %s.";
  public static final String RECEIVER_FAILED_SEAL_FILE_STATUS =
      "Receiver id = %s: Failed to seal file %s, status is %s.";
  public static final String RECEIVER_FAILED_SEAL_FILE_FROM_REQ =
      "Receiver id = %s: Failed to seal file %s from req %s.";
  public static final String FAILED_TO_SEAL_FILE =
      "Failed to seal file %s because %s";
  public static final String FAILED_TO_SEAL_FILE_MULTI =
      "Failed to seal file %s, because writing file %s is not available.";
  public static final String REQUEST_SENDER_RESET_OFFSET =
      "Request sender to reset file reader's offset from %s to %s.";
  public static final String RECEIVER_EXIT_WRITER_CLOSED =
      "Receiver id = {}: Handling exit: Writing file writer was closed.";
  public static final String RECEIVER_EXIT_CLOSE_WRITER_ERROR =
      "Receiver id = {}: Handling exit: Close writing file writer error.";
  public static final String RECEIVER_EXIT_WRITER_NULL =
      "Receiver id = {}: Handling exit: Writing file writer is null. No need to close.";
  public static final String RECEIVER_EXIT_FILE_DELETED =
      "Receiver id = {}: Handling exit: Writing file {} was deleted.";
  public static final String RECEIVER_EXIT_DELETE_FILE_ERROR =
      "Receiver id = {}: Handling exit: Delete writing file {} error.";
  public static final String RECEIVER_EXIT_FILE_NULL =
      "Receiver id = {}: Handling exit: Writing file is null. No need to delete.";
  public static final String RECEIVER_EXIT_DIR_DELETED =
      "Receiver id = {}: Handling exit: Original receiver file dir {} was deleted.";
  public static final String RECEIVER_EXIT_DELETE_DIR_ERROR =
      "Receiver id = {}: Handling exit: Delete original receiver file dir {} error.";
  public static final String RECEIVER_EXIT_DIR_NOT_EXIST =
      "Receiver id = {}: Handling exit: Original receiver file dir {} does not exist. No need to delete.";
  public static final String RECEIVER_EXIT_DIR_NULL =
      "Receiver id = {}: Handling exit: Original receiver file dir is null. No need to delete.";

  // ===================== PipeReceiverStatusHandler =====================

  public static final String IOT_CONSENSUS_RETRY_WITH_INTERVAL =
      "IoTConsensusV2: will retry with increasing interval. status: {}";
  public static final String IOT_CONSENSUS_WILL_NOT_RETRY =
      "IoTConsensusV2: will not retry. status: {}";
  public static final String IDEMPOTENT_CONFLICT_IGNORED =
      "Idempotent conflict exception: will be ignored. status: {}";
  public static final String TEMPORARY_UNAVAILABLE_RETRY =
      "Temporary unavailable exception: will retry forever. status: %s, message: %s";
  public static final String USER_CONFLICT_IGNORED =
      "{}: Skip if no privileges. will be ignored. event: {}. status: {}";
  public static final String USER_CONFLICT_RETRY_TIMEOUT =
      "User conflict exception: retry timeout. will be ignored. event: {}. status: {}";
  public static final String USER_CONFLICT_WILL_RETRY =
      "User conflict exception: will retry {}. status: {}";
  public static final String USER_CONFLICT_NOT_ALLOWED =
      "User conflict exception: will be ignored because retry is not allowed. event: {}. status: {}";
  public static final String OTHER_EXCEPTION_RETRY_TIMEOUT =
      "{}: retry timeout. will be ignored. event: {}. status: {}";
  public static final String OTHER_EXCEPTION_RETRY_FOREVER =
      "%s: will retry forever. status: %s, message: %s";
  public static final String OTHER_EXCEPTION_RETRY_SECONDS =
      "{}: will retry for at least {} seconds. status: {}";

  private PipeMessages() {}

  public static final String THROWING_EXCEPTION_IN_VALIDATE = "Throwing exception in validate";
  public static final String THROWING_EXCEPTION_IN_CUSTOMIZE = "Throwing exception in customize";
  public static final String THROWING_EXCEPTION_IN_PROCESS_TABLET = "Throwing exception in process(TabletInsertionEvent, EventCollector)";
  public static final String THROWING_EXCEPTION_IN_PROCESS_TSFILE = "Throwing exception in process(TsFileInsertionEvent, EventCollector)";
  public static final String THROWING_EXCEPTION_IN_PROCESS_EVENT = "Throwing exception in process(Event, EventCollector)";
  public static final String THROWING_EXCEPTION_IN_CLOSE = "Throwing exception in close";
  public static final String ILLEGAL_DB_OR_TABLE_PATTERN = "Illegal database or table pattern. Detail: ";
  public static final String EVENT_NOT_SUPPORT_BINDING_PROGRESS_INDEX = "This event does not support binding progressIndex.";
  public static final String UNSUPPORTED_VERSION = "Unsupported version %s";

  // ===================== PipePluginAgent (additional) =====================

  public static final String FAILED_TO_CLOSE_PROCESSOR_AFTER_INIT =
      "Failed to close processor after failed to initialize processor. Ignore this exception.";

  // ===================== PipePluginExecutableManager (additional) =====================

  public static final String FAILED_TO_COMPUTE_MD5 =
      "Failed to registered function %s, because "
          + "error occurred when trying to compute md5 of jar file for function %s ";

  // ===================== PipeInclusionOptions (additional) =====================

  public static final String ILLEGAL_OPTIONS_CHECKING_PRESENCE =
      "Illegal options (inclusion: {}, exclusion: {}) parsed "
          + "when checking if at least one option is present: {}";
  public static final String ILLEGAL_OPTIONS_CHECKING_LEGAL =
      "Illegal options {} parsed when checking if all options are legal: {}";

  // ===================== PipeEventCommitMetrics (additional) =====================

  public static final String FAILED_TO_DEREGISTER_COMMIT_METRICS =
      "Failed to deregister pipe event commit metrics, PipeEventCommitter({}) does not exist";

  // ===================== IoTDBSyncClientManager (additional) =====================

  public static final String FAILED_TO_CLOSE_CLIENT_WITH_TARGET =
      "Failed to close client with target server ip: {}, port: {}, because: {}. Ignore it.";
  public static final String HANDSHAKE_ERROR_WITH_TARGET_RETRY =
      "Handshake error with target server ip: {}, port: {}, because: {}. "
          + "Retry to handshake by PipeTransferHandshakeV1Req.";
  public static final String HANDSHAKE_ERROR_WITH_TARGET_SERVER =
      "Handshake error with target server ip: {}, port: {}, because: {}.";
  public static final String HANDSHAKE_SUCCESS_TARGET =
      "Handshake success. Target server ip: {}, port: {}";
  public static final String ALL_CLIENTS_DEAD =
      "All clients are dead, please check the connection to the receiver.";
  public static final String ALL_SOCKETS_DEAD =
      "All sockets are dead, please check the connection to the receiver.";
  public static final String ALL_TARGET_SERVERS_NOT_AVAILABLE =
      "All target servers %s are not available.";

  // ===================== IoTDBAirGapSink (additional) =====================

  public static final String BATCH_MODE_NOT_SUPPORTED =
      "Batch mode is enabled by the given parameters. "
          + "IoTDBAirGapConnector does not support batch mode. "
          + "Disable batch mode.";
  public static final String FAILED_TO_RECONNECT =
      "Failed to reconnect to target server, because: {}. Try to reconnect later.";

  // ===================== IoTDBReceiverAgent (additional) =====================

  public static final String RECEIVER_VERSION_MISMATCH =
      "The receiver version {} is different from the sender version {},"
          + " the receiver will be reset to the sender version.";
  public static final String UNSUPPORTED_PIPE_VERSION = "Unsupported pipe version %d";

  // ===================== PipeEventCommitManager (additional) =====================

  public static final String FAILED_TO_MARK_COMMIT_RATE =
      "Failed to mark commit rate for pipe task: {}, stack trace: {}";
  public static final String STALE_PIPE_EVENT_COMMITTER =
      "stale PipeEventCommitter({}) when commit event: {}, current restart times {}";
  public static final String MISSING_PIPE_EVENT_COMMITTER =
      "missing PipeEventCommitter({}) when commit event: {}, stack trace: {}";

  // ===================== IoTDBSink (additional) =====================

  public static final String PARSE_URL_ERROR =
      "Exception occurred while parsing node urls from target servers: {}";
  public static final String PARSE_URL_ERROR_MESSAGE =
      "Error occurred while parsing node urls from target servers, please check the specified 'host':'port' or 'node-urls'";
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
