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

package org.apache.iotdb.commons.pipe.config;

import org.apache.iotdb.commons.conf.CommonConfig;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.enums.PipeRateAverage;
import org.apache.iotdb.commons.i18n.PipeMessages;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PipeConfig {

  private static final CommonConfig COMMON_CONFIG = CommonDescriptor.getInstance().getConfig();

  /////////////////////////////// File ///////////////////////////////

  public String getPipeHardlinkBaseDirName() {
    return COMMON_CONFIG.getPipeHardlinkBaseDirName();
  }

  public String getPipeHardlinkTsFileDirName() {
    return COMMON_CONFIG.getPipeHardlinkTsFileDirName();
  }

  public boolean getPipeFileReceiverFsyncEnabled() {
    return COMMON_CONFIG.getPipeFileReceiverFsyncEnabled();
  }

  /////////////////////////////// Tablet ///////////////////////////////

  public int getPipeDataStructureTabletRowSize() {
    return COMMON_CONFIG.getPipeDataStructureTabletRowSize();
  }

  public int getPipeDataStructureTabletSizeInBytes() {
    return COMMON_CONFIG.getPipeDataStructureTabletSizeInBytes();
  }

  public double getPipeDataStructureTabletMemoryBlockAllocationRejectThreshold() {
    // To avoid too much parsed events causing OOM. If total tablet memory size exceeds this
    // threshold, allocations of memory block for tablets will be rejected.
    return COMMON_CONFIG.getPipeDataStructureTabletMemoryBlockAllocationRejectThreshold();
  }

  public double getPipeDataStructureTsFileMemoryBlockAllocationRejectThreshold() {
    // Used to control the memory allocated for managing slice tsfile.
    return COMMON_CONFIG.getPipeDataStructureTsFileMemoryBlockAllocationRejectThreshold();
  }

  public double getPipeTotalFloatingMemoryProportion() {
    return COMMON_CONFIG.getPipeTotalFloatingMemoryProportion();
  }

  /////////////////////////////// Estimation ///////////////////////////////

  public boolean isPipeEnableMemoryCheck() {
    return COMMON_CONFIG.isPipeEnableMemoryChecked();
  }

  public long getPipeInsertNodeQueueMemory() {
    return COMMON_CONFIG.getPipeInsertNodeQueueMemory();
  }

  public long getTsFileParserMemory() {
    return COMMON_CONFIG.getPipeTsFileParserMemory();
  }

  public int getPipeTsFileParserInFlightMaxNum() {
    return COMMON_CONFIG.getPipeTsFileParserInFlightMaxNum();
  }

  public int getPipeTsFileParserInFlightMaxNumPerPipeRegion() {
    return COMMON_CONFIG.getPipeTsFileParserInFlightMaxNumPerPipeRegion();
  }

  public long getSinkBatchMemoryInsertNode() {
    return COMMON_CONFIG.getPipeSinkBatchMemoryInsertNode();
  }

  public long getSinkBatchMemoryTsFile() {
    return COMMON_CONFIG.getPipeSinkBatchMemoryTsFile();
  }

  public long getSendTsFileReadBuffer() {
    return COMMON_CONFIG.getPipeSendTsFileReadBuffer();
  }

  public double getReservedMemoryPercentage() {
    return COMMON_CONFIG.getPipeReservedMemoryPercentage();
  }

  public long getPipeMinimumReceiverMemory() {
    return COMMON_CONFIG.getPipeMinimumReceiverMemory();
  }

  /////////////////////////////// Subtask Sink ///////////////////////////////

  public int getPipeRealTimeQueuePollTsFileThreshold() {
    return COMMON_CONFIG.getPipeRealTimeQueuePollTsFileThreshold();
  }

  public int getPipeRealTimeQueuePollHistoricalTsFileThreshold() {
    return Math.max(COMMON_CONFIG.getPipeRealTimeQueuePollHistoricalTsFileThreshold(), 1);
  }

  public int getPipeRealTimeQueueMaxWaitingTsFileSize() {
    return COMMON_CONFIG.getPipeRealTimeQueueMaxWaitingTsFileSize();
  }

  public boolean getPipeRealtimeForceDowngradingEnabled() {
    return COMMON_CONFIG.getPipeRealtimeForceDowngradingEnabled();
  }

  public double getPipeRealtimeForceDowngradingProportion() {
    return COMMON_CONFIG.getPipeRealtimeForceDowngradingProportion();
  }

  /////////////////////////////// Subtask Executor ///////////////////////////////

  public int getPipeSubtaskExecutorMaxThreadNum() {
    return COMMON_CONFIG.getPipeSubtaskExecutorMaxThreadNum();
  }

  public int getPipeSubtaskExecutorBasicCheckPointIntervalByConsumedEventCount() {
    return COMMON_CONFIG.getPipeSubtaskExecutorBasicCheckPointIntervalByConsumedEventCount();
  }

  public long getPipeSubtaskExecutorBasicCheckPointIntervalByTimeDuration() {
    return COMMON_CONFIG.getPipeSubtaskExecutorBasicCheckPointIntervalByTimeDuration();
  }

  public long getPipeSubtaskExecutorPendingQueueMaxBlockingTimeMs() {
    return COMMON_CONFIG.getPipeSubtaskExecutorPendingQueueMaxBlockingTimeMs();
  }

  public long getPipeSubtaskExecutorCronHeartbeatEventIntervalSeconds() {
    return COMMON_CONFIG.getPipeSubtaskExecutorCronHeartbeatEventIntervalSeconds();
  }

  public long getPipeMaxWaitFinishTime() {
    return COMMON_CONFIG.getPipeMaxWaitFinishTime();
  }

  public long getPipeSinkSubtaskSleepIntervalInitMs() {
    return COMMON_CONFIG.getPipeSinkSubtaskSleepIntervalInitMs();
  }

  public long getPipeSinkSubtaskSleepIntervalMaxMs() {
    return COMMON_CONFIG.getPipeSinkSubtaskSleepIntervalMaxMs();
  }

  /////////////////////////////// Source ///////////////////////////////

  public int getPipeSourceAssignerDisruptorRingBufferSize() {
    return COMMON_CONFIG.getPipeSourceAssignerDisruptorRingBufferSize();
  }

  public long getPipeSourceAssignerDisruptorRingBufferEntrySizeInBytes() {
    return COMMON_CONFIG.getPipeSourceAssignerDisruptorRingBufferEntrySizeInBytes();
  }

  /////////////////////////////// Sink ///////////////////////////////

  public int getPipeSinkHandshakeTimeoutMs() {
    return COMMON_CONFIG.getPipeSinkHandshakeTimeoutMs();
  }

  public int getPipeAirGapSinkTabletTimeoutMs() {
    return COMMON_CONFIG.getPipeAirGapSinkTabletTimeoutMs();
  }

  public int getPipeSinkTransferTimeoutMs() {
    return COMMON_CONFIG.getPipeSinkTransferTimeoutMs();
  }

  public int getPipeSinkReadFileBufferSize() {
    return COMMON_CONFIG.getPipeSinkReadFileBufferSize();
  }

  public boolean isPipeSinkReadFileBufferMemoryControlEnabled() {
    return COMMON_CONFIG.isPipeSinkReadFileBufferMemoryControlEnabled();
  }

  public long getPipeSinkRetryIntervalMs() {
    return COMMON_CONFIG.getPipeSinkRetryIntervalMs();
  }

  public boolean isPipeSinkRetryLocallyForConnectionError() {
    return COMMON_CONFIG.isPipeSinkRetryLocallyForConnectionError();
  }

  public boolean isPipeSinkRPCThriftCompressionEnabled() {
    return COMMON_CONFIG.isPipeSinkRPCThriftCompressionEnabled();
  }

  public int getPipeAsyncSinkForcedRetryTsFileEventQueueSize() {
    return COMMON_CONFIG.getPipeAsyncSinkForcedRetryTsFileEventQueueSize();
  }

  public int getPipeAsyncSinkForcedRetryTabletEventQueueSize() {
    return COMMON_CONFIG.getPipeAsyncSinkForcedRetryTabletEventQueueSize();
  }

  public int getPipeAsyncSinkForcedRetryTotalEventQueueSize() {
    return COMMON_CONFIG.getPipeAsyncSinkForcedRetryTotalEventQueueSize();
  }

  public long getPipeAsyncSinkMaxRetryExecutionTimeMsPerCall() {
    return COMMON_CONFIG.getPipeAsyncSinkMaxRetryExecutionTimeMsPerCall();
  }

  public long getPipeAsyncSinkRetryMaxDurationMs() {
    return COMMON_CONFIG.getPipeAsyncSinkRetryMaxDurationMs();
  }

  public long getPipeAsyncSinkRetryProbeIntervalMs() {
    return COMMON_CONFIG.getPipeAsyncSinkRetryProbeIntervalMs();
  }

  public int getPipeAsyncSinkSelectorNumber() {
    return COMMON_CONFIG.getPipeAsyncSinkSelectorNumber();
  }

  public int getPipeAsyncSinkMaxClientNumber() {
    return COMMON_CONFIG.getPipeAsyncSinkMaxClientNumber();
  }

  public int getPipeAsyncSinkMaxTsFileClientNumber() {
    return COMMON_CONFIG.getPipeAsyncSinkMaxTsFileClientNumber();
  }

  public double getPipeSendTsFileRateLimitBytesPerSecond() {
    return COMMON_CONFIG.getPipeSendTsFileRateLimitBytesPerSecond();
  }

  public double getPipeAllSinksRateLimitBytesPerSecond() {
    return COMMON_CONFIG.getPipeAllSinksRateLimitBytesPerSecond();
  }

  public int getRateLimiterHotReloadCheckIntervalMs() {
    return COMMON_CONFIG.getRateLimiterHotReloadCheckIntervalMs();
  }

  public int getPipeSinkRequestSliceThresholdBytes() {
    return COMMON_CONFIG.getPipeSinkRequestSliceThresholdBytes();
  }

  public long getPipeMaxReaderChunkSize() {
    return COMMON_CONFIG.getPipeMaxReaderChunkSize();
  }

  public long getPipeRemainingTimeCommitAutoSwitchSeconds() {
    return COMMON_CONFIG.getPipeRemainingTimeCommitRateAutoSwitchSeconds();
  }

  public PipeRateAverage getPipeRemainingTimeCommitRateAverageTime() {
    return COMMON_CONFIG.getPipeRemainingTimeCommitRateAverageTime();
  }

  public double getPipeRemainingInsertNodeCountEMAAlpha() {
    return COMMON_CONFIG.getPipeRemainingInsertNodeCountEMAAlpha();
  }

  public double getPipeTsFileScanParsingThreshold() {
    return COMMON_CONFIG.getPipeTsFileScanParsingThreshold();
  }

  public double getPipeDynamicMemoryHistoryWeight() {
    return COMMON_CONFIG.getPipeDynamicMemoryHistoryWeight();
  }

  public double getPipeDynamicMemoryAdjustmentThreshold() {
    return COMMON_CONFIG.getPipeDynamicMemoryAdjustmentThreshold();
  }

  public double getPipeThresholdAllocationStrategyMaximumMemoryIncrementRatio() {
    return COMMON_CONFIG.getPipeThresholdAllocationStrategyMaximumMemoryIncrementRatio();
  }

  public double getPipeThresholdAllocationStrategyLowUsageThreshold() {
    return COMMON_CONFIG.getPipeThresholdAllocationStrategyLowUsageThreshold();
  }

  public double getPipeThresholdAllocationStrategyFixedMemoryHighUsageThreshold() {
    return COMMON_CONFIG.getPipeThresholdAllocationStrategyFixedMemoryHighUsageThreshold();
  }

  public boolean isTransferTsFileSync() {
    return COMMON_CONFIG.getPipeTransferTsFileSync();
  }

  public long getPipeCheckAllSyncClientLiveTimeIntervalMs() {
    return COMMON_CONFIG.getPipeCheckAllSyncClientLiveTimeIntervalMs();
  }

  public int getPipeTsFileResourceSegmentLockNum() {
    return COMMON_CONFIG.getPipeTsFileResourceSegmentLockNum();
  }

  /////////////////////////////// Meta Consistency ///////////////////////////////

  public boolean isSeperatedPipeHeartbeatEnabled() {
    return COMMON_CONFIG.isSeperatedPipeHeartbeatEnabled();
  }

  public int getPipeHeartbeatIntervalSecondsForCollectingPipeMeta() {
    return COMMON_CONFIG.getPipeHeartbeatIntervalSecondsForCollectingPipeMeta();
  }

  public long getPipeMetaSyncerInitialSyncDelayMinutes() {
    return COMMON_CONFIG.getPipeMetaSyncerInitialSyncDelayMinutes();
  }

  public long getPipeMetaSyncerSyncIntervalMinutes() {
    return COMMON_CONFIG.getPipeMetaSyncerSyncIntervalMinutes();
  }

  public long getPipeMetaSyncerAutoRestartPipeCheckIntervalRound() {
    return COMMON_CONFIG.getPipeMetaSyncerAutoRestartPipeCheckIntervalRound();
  }

  public boolean getPipeAutoRestartEnabled() {
    return COMMON_CONFIG.getPipeAutoRestartEnabled();
  }

  /////////////////////////////// Air Gap Receiver ///////////////////////////////

  public boolean getPipeAirGapReceiverEnabled() {
    return COMMON_CONFIG.getPipeAirGapReceiverEnabled();
  }

  public int getPipeAirGapReceiverPort() {
    return COMMON_CONFIG.getPipeAirGapReceiverPort();
  }

  public long getPipeAirGapRetryLocalIntervalMs() {
    return COMMON_CONFIG.getPipeAirGapRetryLocalIntervalMs();
  }

  public long getPipeAirGapRetryMaxMs() {
    return COMMON_CONFIG.getPipeAirGapRetryMaxMs();
  }

  /////////////////////////////// Receiver ///////////////////////////////

  public long getPipeReceiverLoginPeriodicVerificationIntervalMs() {
    return COMMON_CONFIG.getPipeReceiverLoginPeriodicVerificationIntervalMs();
  }

  public double getPipeReceiverActualToEstimatedMemoryRatio() {
    return COMMON_CONFIG.getPipeReceiverActualToEstimatedMemoryRatio();
  }

  public int getPipeReceiverReqDecompressedMaxLengthInBytes() {
    return COMMON_CONFIG.getPipeReceiverReqDecompressedMaxLengthInBytes();
  }

  public int getPipeAirGapReceiverMaxPayloadSizeInBytes() {
    return COMMON_CONFIG.getPipeAirGapReceiverMaxPayloadSizeInBytes();
  }

  public boolean isPipeReceiverLoadConversionEnabled() {
    return COMMON_CONFIG.isPipeReceiverLoadConversionEnabled();
  }

  public long getPipePeriodicalLogMinIntervalSeconds() {
    return COMMON_CONFIG.getPipePeriodicalLogMinIntervalSeconds();
  }

  public boolean isPipeRetryLocallyForParallelOrUserConflict() {
    return COMMON_CONFIG.isPipeRetryLocallyForParallelOrUserConflict();
  }

  /////////////////////////////// Logger ///////////////////////////////

  public double getPipeMetaReportMaxLogNumPerRound() {
    return COMMON_CONFIG.getPipeMetaReportMaxLogNumPerRound();
  }

  public int getPipeMetaReportMaxLogIntervalRounds() {
    return COMMON_CONFIG.getPipeMetaReportMaxLogIntervalRounds();
  }

  public int getPipeTsFilePinMaxLogNumPerRound() {
    return COMMON_CONFIG.getPipeTsFilePinMaxLogNumPerRound();
  }

  public int getPipeTsFilePinMaxLogIntervalRounds() {
    return COMMON_CONFIG.getPipeTsFilePinMaxLogIntervalRounds();
  }

  public long getPipeLoggerCacheMaxSizeInBytes() {
    return COMMON_CONFIG.getPipeLoggerCacheMaxSizeInBytes();
  }

  /////////////////////////////// Memory ///////////////////////////////

  public boolean getPipeMemoryManagementEnabled() {
    return COMMON_CONFIG.getPipeMemoryManagementEnabled();
  }

  public int getPipeMemoryAllocateMaxRetries() {
    return COMMON_CONFIG.getPipeMemoryAllocateMaxRetries();
  }

  public long getPipeMemoryAllocateRetryIntervalInMs() {
    return COMMON_CONFIG.getPipeMemoryAllocateRetryIntervalInMs();
  }

  public long getPipeMemoryAllocateMinSizeInBytes() {
    return COMMON_CONFIG.getPipeMemoryAllocateMinSizeInBytes();
  }

  public long getPipeMemoryAllocateForTsFileSequenceReaderInBytes() {
    return COMMON_CONFIG.getPipeMemoryAllocateForTsFileSequenceReaderInBytes();
  }

  public long getPipeMemoryExpanderIntervalSeconds() {
    return COMMON_CONFIG.getPipeMemoryExpanderIntervalSeconds();
  }

  public long getPipeCheckMemoryEnoughIntervalMs() {
    return COMMON_CONFIG.getPipeCheckMemoryEnoughIntervalMs();
  }

  public float getPipeLeaderCacheMemoryUsagePercentage() {
    return COMMON_CONFIG.getPipeLeaderCacheMemoryUsagePercentage();
  }

  /////////////////////////////// TwoStage ///////////////////////////////

  public long getTwoStageAggregateMaxCombinerLiveTimeInMs() {
    return COMMON_CONFIG.getTwoStageAggregateMaxCombinerLiveTimeInMs();
  }

  public long getTwoStageAggregateDataRegionInfoCacheTimeInMs() {
    return COMMON_CONFIG.getTwoStageAggregateDataRegionInfoCacheTimeInMs();
  }

  public long getTwoStageAggregateSenderEndPointsCacheInMs() {
    return COMMON_CONFIG.getTwoStageAggregateSenderEndPointsCacheInMs();
  }

  /////////////////////////////// Meta ///////////////////////////////

  public long getPipeListeningQueueTransferSnapshotThreshold() {
    return COMMON_CONFIG.getPipeListeningQueueTransferSnapshotThreshold();
  }

  public int getPipeSnapshotExecutionMaxBatchSize() {
    return COMMON_CONFIG.getPipeSnapshotExecutionMaxBatchSize();
  }

  /////////////////////////////// Ref ///////////////////////////////

  public boolean getPipeEventReferenceTrackingEnabled() {
    return COMMON_CONFIG.getPipeEventReferenceTrackingEnabled();
  }

  public long getPipeEventReferenceEliminateIntervalSeconds() {
    return COMMON_CONFIG.getPipeEventReferenceEliminateIntervalSeconds();
  }

  /////////////////////////////// Syntactic sugar ///////////////////////////////

  public boolean getPipeAutoSplitFullEnabled() {
    return COMMON_CONFIG.getPipeAutoSplitFullEnabled();
  }

  /////////////////////////////// Utils ///////////////////////////////

  private static final Logger LOGGER = LoggerFactory.getLogger(PipeConfig.class);

  public void printAllConfigs() {
    LOGGER.info(PipeMessages.CONFIG_PIPE_HARDLINK_BASE_DIR_NAME, getPipeHardlinkBaseDirName());
    LOGGER.info(PipeMessages.CONFIG_PIPE_HARDLINK_TSFILE_DIR_NAME, getPipeHardlinkTsFileDirName());
    LOGGER.info(
        PipeMessages.CONFIG_PIPE_FILE_RECEIVER_FSYNC_ENABLED, getPipeFileReceiverFsyncEnabled());

    LOGGER.info(
        PipeMessages.CONFIG_PIPE_DATA_STRUCTURE_TABLET_ROW_SIZE,
        getPipeDataStructureTabletRowSize());
    LOGGER.info(
        PipeMessages.CONFIG_PIPE_DATA_STRUCTURE_TABLET_SIZE_IN_BYTES,
        getPipeDataStructureTabletSizeInBytes());
    LOGGER.info(
        PipeMessages.CONFIG_PIPE_DATA_STRUCTURE_TABLET_MEMORY_BLOCK_ALLOCATION_REJECT_THRESHOLD,
        getPipeDataStructureTabletMemoryBlockAllocationRejectThreshold());
    LOGGER.info(
        PipeMessages.CONFIG_PIPE_DATA_STRUCTURE_TSFILE_MEMORY_BLOCK_ALLOCATION_REJECT_THRESHOLD,
        getPipeDataStructureTsFileMemoryBlockAllocationRejectThreshold());
    LOGGER.info(
        PipeMessages.CONFIG_PIPE_TOTAL_FLOATING_MEMORY_PROPORTION,
        getPipeTotalFloatingMemoryProportion());

    LOGGER.info(PipeMessages.CONFIG_IS_PIPE_ENABLE_MEMORY_CHECK, isPipeEnableMemoryCheck());
    LOGGER.info(PipeMessages.CONFIG_PIPE_TSFILE_PARSER_MEMORY, getTsFileParserMemory());
    LOGGER.info(
        PipeMessages.CONFIG_PIPE_TSFILE_PARSER_IN_FLIGHT_MAX_NUM,
        getPipeTsFileParserInFlightMaxNum());
    LOGGER.info(
        PipeMessages.CONFIG_PIPE_TSFILE_PARSER_IN_FLIGHT_MAX_NUM_PER_PIPE_REGION,
        getPipeTsFileParserInFlightMaxNumPerPipeRegion());
    LOGGER.info(PipeMessages.CONFIG_SINK_BATCH_MEMORY_INSERT_NODE, getSinkBatchMemoryInsertNode());
    LOGGER.info(PipeMessages.CONFIG_SINK_BATCH_MEMORY_TSFILE, getSinkBatchMemoryTsFile());
    LOGGER.info(PipeMessages.CONFIG_SEND_TSFILE_READ_BUFFER, getSendTsFileReadBuffer());
    LOGGER.info(PipeMessages.CONFIG_PIPE_RESERVED_MEMORY_PERCENTAGE, getReservedMemoryPercentage());
    LOGGER.info(PipeMessages.CONFIG_PIPE_MINIMUM_RECEIVER_MEMORY, getPipeMinimumReceiverMemory());

    LOGGER.info(
        PipeMessages.CONFIG_PIPE_REALTIME_QUEUE_POLL_TSFILE_THRESHOLD,
        getPipeRealTimeQueuePollTsFileThreshold());
    LOGGER.info(
        PipeMessages.CONFIG_PIPE_REALTIME_QUEUE_POLL_HISTORICAL_TSFILE_THRESHOLD,
        getPipeRealTimeQueuePollHistoricalTsFileThreshold());
    LOGGER.info(
        PipeMessages.CONFIG_PIPE_REALTIME_QUEUE_MAX_WAITING_TSFILE_SIZE,
        getPipeRealTimeQueueMaxWaitingTsFileSize());
    LOGGER.info(
        PipeMessages.CONFIG_PIPE_REALTIME_FORCE_DOWNGRADING_ENABLED,
        getPipeRealtimeForceDowngradingEnabled());
    LOGGER.info(
        PipeMessages.CONFIG_PIPE_REALTIME_FORCE_DOWNGRADING_PROPORTION,
        getPipeRealtimeForceDowngradingProportion());

    LOGGER.info(
        PipeMessages.CONFIG_PIPE_SUBTASK_EXECUTOR_MAX_THREAD_NUM,
        getPipeSubtaskExecutorMaxThreadNum());
    LOGGER.info(
        PipeMessages.CONFIG_PIPE_SUBTASK_EXECUTOR_BASIC_CHECKPOINT_INTERVAL_BY_CONSUMED_EVENT_COUNT,
        getPipeSubtaskExecutorBasicCheckPointIntervalByConsumedEventCount());
    LOGGER.info(
        PipeMessages.CONFIG_PIPE_SUBTASK_EXECUTOR_BASIC_CHECKPOINT_INTERVAL_BY_TIME_DURATION,
        getPipeSubtaskExecutorBasicCheckPointIntervalByTimeDuration());
    LOGGER.info(
        PipeMessages.CONFIG_PIPE_SUBTASK_EXECUTOR_PENDING_QUEUE_MAX_BLOCKING_TIME_MS,
        getPipeSubtaskExecutorPendingQueueMaxBlockingTimeMs());
    LOGGER.info(
        PipeMessages.CONFIG_PIPE_SUBTASK_EXECUTOR_CRON_HEARTBEAT_EVENT_INTERVAL_SECONDS,
        getPipeSubtaskExecutorCronHeartbeatEventIntervalSeconds());
    LOGGER.info(PipeMessages.CONFIG_PIPE_MAX_WAIT_FINISH_TIME, getPipeMaxWaitFinishTime());
    LOGGER.info(
        PipeMessages.CONFIG_PIPE_SINK_SUBTASK_SLEEP_INTERVAL_INIT_MS,
        getPipeSinkSubtaskSleepIntervalInitMs());
    LOGGER.info(
        PipeMessages.CONFIG_PIPE_SINK_SUBTASK_SLEEP_INTERVAL_MAX_MS,
        getPipeSinkSubtaskSleepIntervalMaxMs());

    LOGGER.info(
        PipeMessages.CONFIG_PIPE_SOURCE_ASSIGNER_DISRUPTOR_RING_BUFFER_SIZE,
        getPipeSourceAssignerDisruptorRingBufferSize());
    LOGGER.info(
        PipeMessages.CONFIG_PIPE_SOURCE_ASSIGNER_DISRUPTOR_RING_BUFFER_ENTRY_SIZE_IN_BYTES,
        getPipeSourceAssignerDisruptorRingBufferEntrySizeInBytes());

    LOGGER.info(
        PipeMessages.CONFIG_PIPE_SINK_HANDSHAKE_TIMEOUT_MS, getPipeSinkHandshakeTimeoutMs());
    LOGGER.info(PipeMessages.CONFIG_PIPE_SINK_TRANSFER_TIMEOUT_MS, getPipeSinkTransferTimeoutMs());
    LOGGER.info(
        PipeMessages.CONFIG_PIPE_SINK_READ_FILE_BUFFER_SIZE, getPipeSinkReadFileBufferSize());
    LOGGER.info(
        PipeMessages.CONFIG_PIPE_SINK_READ_FILE_BUFFER_MEMORY_CONTROL_ENABLED,
        isPipeSinkReadFileBufferMemoryControlEnabled());
    LOGGER.info(PipeMessages.CONFIG_PIPE_SINK_RETRY_INTERVAL_MS, getPipeSinkRetryIntervalMs());
    LOGGER.info(
        PipeMessages.CONFIG_PIPE_SINK_RPC_THRIFT_COMPRESSION_ENABLED,
        isPipeSinkRPCThriftCompressionEnabled());
    LOGGER.info(
        PipeMessages.CONFIG_PIPE_LEADER_CACHE_MEMORY_USAGE_PERCENTAGE,
        getPipeLeaderCacheMemoryUsagePercentage());
    LOGGER.info(
        PipeMessages.CONFIG_PIPE_MAX_ALIGNED_SERIES_CHUNK_SIZE_IN_ONE_BATCH,
        getPipeMaxReaderChunkSize());
    LOGGER.info(
        PipeMessages.CONFIG_PIPE_LISTENING_QUEUE_TRANSFER_SNAPSHOT_THRESHOLD,
        getPipeListeningQueueTransferSnapshotThreshold());
    LOGGER.info(
        PipeMessages.CONFIG_PIPE_SNAPSHOT_EXECUTION_MAX_BATCH_SIZE,
        getPipeSnapshotExecutionMaxBatchSize());
    LOGGER.info(
        PipeMessages.CONFIG_PIPE_REMAINING_TIME_COMMIT_AUTO_SWITCH_SECONDS,
        getPipeRemainingTimeCommitAutoSwitchSeconds());
    LOGGER.info(
        PipeMessages.CONFIG_PIPE_REMAINING_TIME_COMMIT_RATE_AVERAGE_TIME,
        getPipeRemainingTimeCommitRateAverageTime());
    LOGGER.info(
        PipeMessages.CONFIG_PIPE_REMAINING_INSERT_EVENT_COUNT_AVERAGE,
        getPipeRemainingInsertNodeCountEMAAlpha());
    LOGGER.info(
        PipeMessages.CONFIG_PIPE_TSFILE_SCAN_PARSING_THRESHOLD,
        getPipeTsFileScanParsingThreshold());
    LOGGER.info(PipeMessages.CONFIG_PIPE_TRANSFER_TSFILE_SYNC, isTransferTsFileSync());
    LOGGER.info(
        PipeMessages.CONFIG_PIPE_CHECK_ALL_SYNC_CLIENT_LIVE_TIME_INTERVAL_MS,
        getPipeCheckAllSyncClientLiveTimeIntervalMs());
    LOGGER.info(
        PipeMessages.CONFIG_PIPE_CHECK_ALL_SYNC_CLIENT_LIVE_TIME_INTERVAL_MS,
        getPipeCheckAllSyncClientLiveTimeIntervalMs());

    LOGGER.info(
        PipeMessages.CONFIG_PIPE_DYNAMIC_MEMORY_HISTORY_WEIGHT,
        getPipeDynamicMemoryHistoryWeight());
    LOGGER.info(
        PipeMessages.CONFIG_PIPE_DYNAMIC_MEMORY_ADJUSTMENT_THRESHOLD,
        getPipeDynamicMemoryAdjustmentThreshold());
    LOGGER.info(
        PipeMessages.CONFIG_PIPE_THRESHOLD_ALLOCATION_STRATEGY_MAXIMUM_MEMORY_INCREMENT_RATIO,
        getPipeThresholdAllocationStrategyMaximumMemoryIncrementRatio());
    LOGGER.info(
        PipeMessages.CONFIG_PIPE_THRESHOLD_ALLOCATION_STRATEGY_LOW_USAGE_THRESHOLD,
        getPipeThresholdAllocationStrategyLowUsageThreshold());
    LOGGER.info(
        PipeMessages.CONFIG_PIPE_THRESHOLD_ALLOCATION_STRATEGY_FIXED_MEMORY_HIGH_USAGE_THRESHOLD,
        getPipeThresholdAllocationStrategyFixedMemoryHighUsageThreshold());

    LOGGER.info(
        PipeMessages.CONFIG_PIPE_ASYNC_SINK_FORCED_RETRY_TSFILE_EVENT_QUEUE_SIZE_THRESHOLD,
        getPipeAsyncSinkForcedRetryTsFileEventQueueSize());
    LOGGER.info(
        PipeMessages.CONFIG_PIPE_ASYNC_SINK_FORCED_RETRY_TABLET_EVENT_QUEUE_SIZE_THRESHOLD,
        getPipeAsyncSinkForcedRetryTabletEventQueueSize());
    LOGGER.info(
        PipeMessages.CONFIG_PIPE_ASYNC_SINK_FORCED_RETRY_TOTAL_EVENT_QUEUE_SIZE_THRESHOLD,
        getPipeAsyncSinkForcedRetryTotalEventQueueSize());
    LOGGER.info(
        PipeMessages.CONFIG_PIPE_ASYNC_SINK_MAX_RETRY_EXECUTION_TIME_MS_PER_CALL,
        getPipeAsyncSinkMaxRetryExecutionTimeMsPerCall());
    LOGGER.info(
        PipeMessages.CONFIG_PIPE_ASYNC_SINK_RETRY_MAX_DURATION_MS,
        getPipeAsyncSinkRetryMaxDurationMs());
    LOGGER.info(
        PipeMessages.CONFIG_PIPE_ASYNC_SINK_RETRY_PROBE_INTERVAL_MS,
        getPipeAsyncSinkRetryProbeIntervalMs());
    LOGGER.info(
        PipeMessages.CONFIG_PIPE_ASYNC_SINK_SELECTOR_NUMBER, getPipeAsyncSinkSelectorNumber());
    LOGGER.info(
        PipeMessages.CONFIG_PIPE_ASYNC_SINK_MAX_CLIENT_NUMBER, getPipeAsyncSinkMaxClientNumber());
    LOGGER.info(
        PipeMessages.CONFIG_PIPE_ASYNC_SINK_MAX_TSFILE_CLIENT_NUMBER,
        getPipeAsyncSinkMaxTsFileClientNumber());

    LOGGER.info(
        PipeMessages.CONFIG_PIPE_SEND_TSFILE_RATE_LIMIT_BYTES_PER_SECOND,
        getPipeSendTsFileRateLimitBytesPerSecond());
    LOGGER.info(
        PipeMessages.CONFIG_PIPE_ALL_SINKS_RATE_LIMIT_BYTES_PER_SECOND,
        getPipeAllSinksRateLimitBytesPerSecond());
    LOGGER.info(
        PipeMessages.CONFIG_RATE_LIMITER_HOT_RELOAD_CHECK_INTERVAL_MS,
        getRateLimiterHotReloadCheckIntervalMs());

    LOGGER.info(
        PipeMessages.CONFIG_PIPE_SINK_REQUEST_SLICE_THRESHOLD_BYTES,
        getPipeSinkRequestSliceThresholdBytes());

    LOGGER.info(
        PipeMessages.CONFIG_SEPERATED_PIPE_HEARTBEAT_ENABLED, isSeperatedPipeHeartbeatEnabled());
    LOGGER.info(
        PipeMessages.CONFIG_PIPE_HEARTBEAT_INTERVAL_SECONDS_FOR_COLLECTING_PIPE_META,
        getPipeHeartbeatIntervalSecondsForCollectingPipeMeta());
    LOGGER.info(
        PipeMessages.CONFIG_PIPE_META_SYNCER_INITIAL_SYNC_DELAY_MINUTES,
        getPipeMetaSyncerInitialSyncDelayMinutes());
    LOGGER.info(
        PipeMessages.CONFIG_PIPE_META_SYNCER_SYNC_INTERVAL_MINUTES,
        getPipeMetaSyncerSyncIntervalMinutes());
    LOGGER.info(
        PipeMessages.CONFIG_PIPE_META_SYNCER_AUTO_RESTART_PIPE_CHECK_INTERVAL_ROUND,
        getPipeMetaSyncerAutoRestartPipeCheckIntervalRound());
    LOGGER.info(PipeMessages.CONFIG_PIPE_AUTO_RESTART_ENABLED, getPipeAutoRestartEnabled());

    LOGGER.info(PipeMessages.CONFIG_PIPE_AIR_GAP_RECEIVER_ENABLED, getPipeAirGapReceiverEnabled());
    LOGGER.info(PipeMessages.CONFIG_PIPE_AIR_GAP_RECEIVER_PORT, getPipeAirGapReceiverPort());
    LOGGER.info(
        PipeMessages.CONFIG_PIPE_AIR_GAP_SINK_TABLET_TIMEOUT_MS,
        getPipeAirGapSinkTabletTimeoutMs());

    LOGGER.info(
        PipeMessages.CONFIG_PIPE_RECEIVER_LOGIN_PERIODIC_VERIFICATION_INTERVAL_MS,
        getPipeReceiverLoginPeriodicVerificationIntervalMs());
    LOGGER.info(
        PipeMessages.CONFIG_PIPE_RECEIVER_ACTUAL_TO_ESTIMATED_MEMORY_RATIO,
        getPipeReceiverActualToEstimatedMemoryRatio());
    LOGGER.info(
        PipeMessages.CONFIG_PIPE_RECEIVER_REQ_DECOMPRESSED_MAX_LENGTH_IN_BYTES,
        getPipeReceiverReqDecompressedMaxLengthInBytes());
    LOGGER.info(
        PipeMessages.CONFIG_PIPE_AIR_GAP_RECEIVER_MAX_PAYLOAD_SIZE_IN_BYTES,
        getPipeAirGapReceiverMaxPayloadSizeInBytes());
    LOGGER.info(
        PipeMessages.CONFIG_PIPE_RECEIVER_LOAD_CONVERSION_ENABLED,
        isPipeReceiverLoadConversionEnabled());
    LOGGER.info(
        PipeMessages.CONFIG_PIPE_PERIODICAL_LOG_MIN_INTERVAL_SECONDS,
        getPipePeriodicalLogMinIntervalSeconds());
    LOGGER.info(
        PipeMessages.CONFIG_PIPE_RETRY_LOCALLY_FOR_PARALLEL_OR_USER_CONFLICT,
        isPipeRetryLocallyForParallelOrUserConflict());

    LOGGER.info(
        PipeMessages.CONFIG_PIPE_META_REPORT_MAX_LOG_NUM_PER_ROUND,
        getPipeMetaReportMaxLogNumPerRound());
    LOGGER.info(
        PipeMessages.CONFIG_PIPE_META_REPORT_MAX_LOG_INTERVAL_ROUNDS,
        getPipeMetaReportMaxLogIntervalRounds());
    LOGGER.info(
        PipeMessages.CONFIG_PIPE_TSFILE_PIN_MAX_LOG_NUM_PER_ROUND,
        getPipeTsFilePinMaxLogNumPerRound());
    LOGGER.info(
        PipeMessages.CONFIG_PIPE_TSFILE_PIN_MAX_LOG_INTERVAL_ROUNDS,
        getPipeTsFilePinMaxLogIntervalRounds());
    LOGGER.info(
        PipeMessages.CONFIG_PIPE_LOGGER_CACHE_MAX_SIZE_IN_BYTES,
        getPipeLoggerCacheMaxSizeInBytes());

    LOGGER.info(
        PipeMessages.CONFIG_PIPE_MEMORY_MANAGEMENT_ENABLED, getPipeMemoryManagementEnabled());
    LOGGER.info(
        PipeMessages.CONFIG_PIPE_MEMORY_ALLOCATE_MAX_RETRIES, getPipeMemoryAllocateMaxRetries());
    LOGGER.info(
        PipeMessages.CONFIG_PIPE_MEMORY_ALLOCATE_RETRY_INTERVAL_IN_MS,
        getPipeMemoryAllocateRetryIntervalInMs());
    LOGGER.info(
        PipeMessages.CONFIG_PIPE_MEMORY_ALLOCATE_MIN_SIZE_IN_BYTES,
        getPipeMemoryAllocateMinSizeInBytes());
    LOGGER.info(
        PipeMessages.CONFIG_PIPE_MEMORY_ALLOCATE_FOR_TSFILE_SEQUENCE_READER_IN_BYTES,
        getPipeMemoryAllocateForTsFileSequenceReaderInBytes());
    LOGGER.info(
        PipeMessages.CONFIG_PIPE_MEMORY_EXPANDER_INTERVAL_SECONDS,
        getPipeMemoryExpanderIntervalSeconds());
    LOGGER.info(
        PipeMessages.CONFIG_PIPE_CHECK_MEMORY_ENOUGH_INTERVAL_MS,
        getPipeCheckMemoryEnoughIntervalMs());

    LOGGER.info(
        PipeMessages.CONFIG_TWO_STAGE_AGGREGATE_MAX_COMBINER_LIVE_TIME_IN_MS,
        getTwoStageAggregateMaxCombinerLiveTimeInMs());
    LOGGER.info(
        PipeMessages.CONFIG_TWO_STAGE_AGGREGATE_DATA_REGION_INFO_CACHE_TIME_IN_MS,
        getTwoStageAggregateDataRegionInfoCacheTimeInMs());
    LOGGER.info(
        PipeMessages.CONFIG_TWO_STAGE_AGGREGATE_SENDER_END_POINTS_CACHE_IN_MS,
        getTwoStageAggregateSenderEndPointsCacheInMs());

    LOGGER.info(
        PipeMessages.CONFIG_PIPE_EVENT_REFERENCE_TRACKING_ENABLED,
        getPipeEventReferenceTrackingEnabled());
    LOGGER.info(
        PipeMessages.CONFIG_PIPE_EVENT_REFERENCE_ELIMINATE_INTERVAL_SECONDS,
        getPipeEventReferenceEliminateIntervalSeconds());

    LOGGER.info(PipeMessages.CONFIG_PIPE_AUTO_SPLIT_FULL_ENABLED, getPipeAutoSplitFullEnabled());
  }

  /////////////////////////////// Singleton ///////////////////////////////

  private PipeConfig() {}

  public static PipeConfig getInstance() {
    return PipeConfigHolder.INSTANCE;
  }

  private static class PipeConfigHolder {
    private static final PipeConfig INSTANCE = new PipeConfig();
  }
}
