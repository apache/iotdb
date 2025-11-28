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

  /////////////////////////////// Subtask Connector ///////////////////////////////

  public int getPipeRealTimeQueuePollTsFileThreshold() {
    return COMMON_CONFIG.getPipeRealTimeQueuePollTsFileThreshold();
  }

  public int getPipeRealTimeQueuePollHistoricalTsFileThreshold() {
    return Math.max(COMMON_CONFIG.getPipeRealTimeQueuePollHistoricalTsFileThreshold(), 1);
  }

  public int getPipeRealTimeQueueMaxWaitingTsFileSize() {
    return COMMON_CONFIG.getPipeRealTimeQueueMaxWaitingTsFileSize();
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

  public long getPipeSourceMatcherCacheSize() {
    return COMMON_CONFIG.getPipeSourceMatcherCacheSize();
  }

  /////////////////////////////// Connector ///////////////////////////////

  public int getPipeConnectorHandshakeTimeoutMs() {
    return COMMON_CONFIG.getPipeConnectorHandshakeTimeoutMs();
  }

  public int getPipeConnectorTransferTimeoutMs() {
    return COMMON_CONFIG.getPipeConnectorTransferTimeoutMs();
  }

  public int getPipeConnectorReadFileBufferSize() {
    return COMMON_CONFIG.getPipeConnectorReadFileBufferSize();
  }

  public boolean isPipeConnectorReadFileBufferMemoryControlEnabled() {
    return COMMON_CONFIG.isPipeConnectorReadFileBufferMemoryControlEnabled();
  }

  public long getPipeConnectorRetryIntervalMs() {
    return COMMON_CONFIG.getPipeConnectorRetryIntervalMs();
  }

  public boolean isPipeConnectorRPCThriftCompressionEnabled() {
    return COMMON_CONFIG.isPipeConnectorRPCThriftCompressionEnabled();
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

  public long getPipeAsyncConnectorMaxRetryExecutionTimeMsPerCall() {
    return COMMON_CONFIG.getPipeAsyncConnectorMaxRetryExecutionTimeMsPerCall();
  }

  public int getPipeAsyncConnectorSelectorNumber() {
    return COMMON_CONFIG.getPipeAsyncConnectorSelectorNumber();
  }

  public int getPipeAsyncConnectorMaxClientNumber() {
    return COMMON_CONFIG.getPipeAsyncConnectorMaxClientNumber();
  }

  public int getPipeAsyncConnectorMaxTsFileClientNumber() {
    return COMMON_CONFIG.getPipeAsyncConnectorMaxTsFileClientNumber();
  }

  public double getPipeSendTsFileRateLimitBytesPerSecond() {
    return COMMON_CONFIG.getPipeSendTsFileRateLimitBytesPerSecond();
  }

  public double getPipeAllConnectorsRateLimitBytesPerSecond() {
    return COMMON_CONFIG.getPipeAllSinksRateLimitBytesPerSecond();
  }

  public int getRateLimiterHotReloadCheckIntervalMs() {
    return COMMON_CONFIG.getRateLimiterHotReloadCheckIntervalMs();
  }

  public int getPipeConnectorRequestSliceThresholdBytes() {
    return COMMON_CONFIG.getPipeConnectorRequestSliceThresholdBytes();
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
    LOGGER.info("PipeHardlinkBaseDirName: {}", getPipeHardlinkBaseDirName());
    LOGGER.info("PipeHardlinkTsFileDirName: {}", getPipeHardlinkTsFileDirName());
    LOGGER.info("PipeFileReceiverFsyncEnabled: {}", getPipeFileReceiverFsyncEnabled());

    LOGGER.info("PipeDataStructureTabletRowSize: {}", getPipeDataStructureTabletRowSize());
    LOGGER.info("PipeDataStructureTabletSizeInBytes: {}", getPipeDataStructureTabletSizeInBytes());
    LOGGER.info(
        "PipeDataStructureTabletMemoryBlockAllocationRejectThreshold: {}",
        getPipeDataStructureTabletMemoryBlockAllocationRejectThreshold());
    LOGGER.info(
        "PipeDataStructureTsFileMemoryBlockAllocationRejectThreshold: {}",
        getPipeDataStructureTsFileMemoryBlockAllocationRejectThreshold());
    LOGGER.info("PipeTotalFloatingMemoryProportion: {}", getPipeTotalFloatingMemoryProportion());

    LOGGER.info("IsPipeEnableMemoryCheck: {}", isPipeEnableMemoryCheck());
    LOGGER.info("PipeTsFileParserMemory: {}", getTsFileParserMemory());
    LOGGER.info("SinkBatchMemoryInsertNode: {}", getSinkBatchMemoryInsertNode());
    LOGGER.info("SinkBatchMemoryTsFile: {}", getSinkBatchMemoryTsFile());
    LOGGER.info("SendTsFileReadBuffer: {}", getSendTsFileReadBuffer());
    LOGGER.info("PipeReservedMemoryPercentage: {}", getReservedMemoryPercentage());
    LOGGER.info("PipeMinimumReceiverMemory: {}", getPipeMinimumReceiverMemory());

    LOGGER.info(
        "PipeRealTimeQueuePollTsFileThreshold: {}", getPipeRealTimeQueuePollTsFileThreshold());
    LOGGER.info(
        "PipeRealTimeQueuePollHistoricalTsFileThreshold: {}",
        getPipeRealTimeQueuePollHistoricalTsFileThreshold());

    LOGGER.info("PipeSubtaskExecutorMaxThreadNum: {}", getPipeSubtaskExecutorMaxThreadNum());
    LOGGER.info(
        "PipeSubtaskExecutorBasicCheckPointIntervalByConsumedEventCount: {}",
        getPipeSubtaskExecutorBasicCheckPointIntervalByConsumedEventCount());
    LOGGER.info(
        "PipeSubtaskExecutorBasicCheckPointIntervalByTimeDuration: {}",
        getPipeSubtaskExecutorBasicCheckPointIntervalByTimeDuration());
    LOGGER.info(
        "PipeSubtaskExecutorPendingQueueMaxBlockingTimeMs: {}",
        getPipeSubtaskExecutorPendingQueueMaxBlockingTimeMs());
    LOGGER.info(
        "PipeSubtaskExecutorCronHeartbeatEventIntervalSeconds: {}",
        getPipeSubtaskExecutorCronHeartbeatEventIntervalSeconds());
    LOGGER.info("PipeMaxWaitFinishTime: {}", getPipeMaxWaitFinishTime());
    LOGGER.info("PipeSinkSubtaskSleepIntervalInitMs: {}", getPipeSinkSubtaskSleepIntervalInitMs());
    LOGGER.info("PipeSinkSubtaskSleepIntervalMaxMs: {}", getPipeSinkSubtaskSleepIntervalMaxMs());

    LOGGER.info(
        "PipeSourceAssignerDisruptorRingBufferSize: {}",
        getPipeSourceAssignerDisruptorRingBufferSize());
    LOGGER.info(
        "PipeSourceAssignerDisruptorRingBufferEntrySizeInBytes: {}",
        getPipeSourceAssignerDisruptorRingBufferEntrySizeInBytes());
    LOGGER.info("PipeSourceMatcherCacheSize: {}", getPipeSourceMatcherCacheSize());

    LOGGER.info("PipeConnectorHandshakeTimeoutMs: {}", getPipeConnectorHandshakeTimeoutMs());
    LOGGER.info("PipeConnectorTransferTimeoutMs: {}", getPipeConnectorTransferTimeoutMs());
    LOGGER.info("PipeConnectorReadFileBufferSize: {}", getPipeConnectorReadFileBufferSize());
    LOGGER.info(
        "PipeConnectorReadFileBufferMemoryControlEnabled: {}",
        isPipeConnectorReadFileBufferMemoryControlEnabled());
    LOGGER.info("PipeConnectorRetryIntervalMs: {}", getPipeConnectorRetryIntervalMs());
    LOGGER.info(
        "PipeConnectorRPCThriftCompressionEnabled: {}",
        isPipeConnectorRPCThriftCompressionEnabled());
    LOGGER.info(
        "PipeLeaderCacheMemoryUsagePercentage: {}", getPipeLeaderCacheMemoryUsagePercentage());
    LOGGER.info("PipeMaxAlignedSeriesChunkSizeInOneBatch: {}", getPipeMaxReaderChunkSize());
    LOGGER.info(
        "PipeListeningQueueTransferSnapshotThreshold: {}",
        getPipeListeningQueueTransferSnapshotThreshold());
    LOGGER.info("PipeSnapshotExecutionMaxBatchSize: {}", getPipeSnapshotExecutionMaxBatchSize());
    LOGGER.info(
        "PipeRemainingTimeCommitAutoSwitchSeconds: {}",
        getPipeRemainingTimeCommitAutoSwitchSeconds());
    LOGGER.info(
        "PipeRemainingTimeCommitRateAverageTime: {}", getPipeRemainingTimeCommitRateAverageTime());
    LOGGER.info(
        "PipePipeRemainingInsertEventCountAverage: {}", getPipeRemainingInsertNodeCountEMAAlpha());
    LOGGER.info("PipeTsFileScanParsingThreshold(): {}", getPipeTsFileScanParsingThreshold());
    LOGGER.info("PipeTransferTsFileSync: {}", isTransferTsFileSync());
    LOGGER.info(
        "PipeCheckAllSyncClientLiveTimeIntervalMs: {}",
        getPipeCheckAllSyncClientLiveTimeIntervalMs());
    LOGGER.info(
        "PipeCheckAllSyncClientLiveTimeIntervalMs: {}",
        getPipeCheckAllSyncClientLiveTimeIntervalMs());

    LOGGER.info("PipeDynamicMemoryHistoryWeight: {}", getPipeDynamicMemoryHistoryWeight());
    LOGGER.info(
        "PipeDynamicMemoryAdjustmentThreshold: {}", getPipeDynamicMemoryAdjustmentThreshold());
    LOGGER.info(
        "PipeThresholdAllocationStrategyMaximumMemoryIncrementRatio: {}",
        getPipeThresholdAllocationStrategyMaximumMemoryIncrementRatio());
    LOGGER.info(
        "PipeThresholdAllocationStrategyLowUsageThreshold: {}",
        getPipeThresholdAllocationStrategyLowUsageThreshold());
    LOGGER.info(
        "PipeThresholdAllocationStrategyFixedMemoryHighUsageThreshold: {}",
        getPipeThresholdAllocationStrategyFixedMemoryHighUsageThreshold());

    LOGGER.info(
        "PipeAsyncConnectorForcedRetryTsFileEventQueueSizeThreshold: {}",
        getPipeAsyncSinkForcedRetryTsFileEventQueueSize());
    LOGGER.info(
        "PipeAsyncConnectorForcedRetryTabletEventQueueSizeThreshold: {}",
        getPipeAsyncSinkForcedRetryTabletEventQueueSize());
    LOGGER.info(
        "PipeAsyncConnectorForcedRetryTotalEventQueueSizeThreshold: {}",
        getPipeAsyncSinkForcedRetryTotalEventQueueSize());
    LOGGER.info(
        "PipeAsyncConnectorMaxRetryExecutionTimeMsPerCall: {}",
        getPipeAsyncConnectorMaxRetryExecutionTimeMsPerCall());
    LOGGER.info("PipeAsyncConnectorSelectorNumber: {}", getPipeAsyncConnectorSelectorNumber());
    LOGGER.info("PipeAsyncConnectorMaxClientNumber: {}", getPipeAsyncConnectorMaxClientNumber());
    LOGGER.info(
        "PipeAsyncConnectorMaxTsFileClientNumber: {}",
        getPipeAsyncConnectorMaxTsFileClientNumber());

    LOGGER.info(
        "PipeSendTsFileRateLimitBytesPerSecond: {}", getPipeSendTsFileRateLimitBytesPerSecond());
    LOGGER.info(
        "PipeAllConnectorsRateLimitBytesPerSecond: {}",
        getPipeAllConnectorsRateLimitBytesPerSecond());
    LOGGER.info(
        "RateLimiterHotReloadCheckIntervalMs: {}", getRateLimiterHotReloadCheckIntervalMs());

    LOGGER.info(
        "PipeConnectorRequestSliceThresholdBytes: {}",
        getPipeConnectorRequestSliceThresholdBytes());

    LOGGER.info("SeperatedPipeHeartbeatEnabled: {}", isSeperatedPipeHeartbeatEnabled());
    LOGGER.info(
        "PipeHeartbeatIntervalSecondsForCollectingPipeMeta: {}",
        getPipeHeartbeatIntervalSecondsForCollectingPipeMeta());
    LOGGER.info(
        "PipeMetaSyncerInitialSyncDelayMinutes: {}", getPipeMetaSyncerInitialSyncDelayMinutes());
    LOGGER.info("PipeMetaSyncerSyncIntervalMinutes: {}", getPipeMetaSyncerSyncIntervalMinutes());
    LOGGER.info(
        "PipeMetaSyncerAutoRestartPipeCheckIntervalRound: {}",
        getPipeMetaSyncerAutoRestartPipeCheckIntervalRound());
    LOGGER.info("PipeAutoRestartEnabled: {}", getPipeAutoRestartEnabled());

    LOGGER.info("PipeAirGapReceiverEnabled: {}", getPipeAirGapReceiverEnabled());
    LOGGER.info("PipeAirGapReceiverPort: {}", getPipeAirGapReceiverPort());

    LOGGER.info(
        "PipeReceiverLoginPeriodicVerificationIntervalMs: {}",
        getPipeReceiverLoginPeriodicVerificationIntervalMs());
    LOGGER.info(
        "PipeReceiverActualToEstimatedMemoryRatio: {}",
        getPipeReceiverActualToEstimatedMemoryRatio());
    LOGGER.info(
        "PipeReceiverReqDecompressedMaxLengthInBytes: {}",
        getPipeReceiverReqDecompressedMaxLengthInBytes());
    LOGGER.info("PipeReceiverLoadConversionEnabled: {}", isPipeReceiverLoadConversionEnabled());
    LOGGER.info(
        "PipePeriodicalLogMinIntervalSeconds: {}", getPipePeriodicalLogMinIntervalSeconds());
    LOGGER.info(
        "PipeRetryLocallyForParallelOrUserConflict: {}",
        isPipeRetryLocallyForParallelOrUserConflict());

    LOGGER.info("PipeMetaReportMaxLogNumPerRound: {}", getPipeMetaReportMaxLogNumPerRound());
    LOGGER.info("PipeMetaReportMaxLogIntervalRounds: {}", getPipeMetaReportMaxLogIntervalRounds());
    LOGGER.info("PipeTsFilePinMaxLogNumPerRound: {}", getPipeTsFilePinMaxLogNumPerRound());
    LOGGER.info("PipeTsFilePinMaxLogIntervalRounds: {}", getPipeTsFilePinMaxLogIntervalRounds());
    LOGGER.info("PipeLoggerCacheMaxSizeInBytes: {}", getPipeLoggerCacheMaxSizeInBytes());

    LOGGER.info("PipeMemoryManagementEnabled: {}", getPipeMemoryManagementEnabled());
    LOGGER.info("PipeMemoryAllocateMaxRetries: {}", getPipeMemoryAllocateMaxRetries());
    LOGGER.info(
        "PipeMemoryAllocateRetryIntervalInMs: {}", getPipeMemoryAllocateRetryIntervalInMs());
    LOGGER.info("PipeMemoryAllocateMinSizeInBytes: {}", getPipeMemoryAllocateMinSizeInBytes());
    LOGGER.info(
        "PipeMemoryAllocateForTsFileSequenceReaderInBytes: {}",
        getPipeMemoryAllocateForTsFileSequenceReaderInBytes());
    LOGGER.info("PipeMemoryExpanderIntervalSeconds: {}", getPipeMemoryExpanderIntervalSeconds());
    LOGGER.info("PipeCheckMemoryEnoughIntervalMs: {}", getPipeCheckMemoryEnoughIntervalMs());

    LOGGER.info(
        "TwoStageAggregateMaxCombinerLiveTimeInMs: {}",
        getTwoStageAggregateMaxCombinerLiveTimeInMs());
    LOGGER.info(
        "TwoStageAggregateDataRegionInfoCacheTimeInMs: {}",
        getTwoStageAggregateDataRegionInfoCacheTimeInMs());
    LOGGER.info(
        "TwoStageAggregateSenderEndPointsCacheInMs: {}",
        getTwoStageAggregateSenderEndPointsCacheInMs());

    LOGGER.info("PipeEventReferenceTrackingEnabled: {}", getPipeEventReferenceTrackingEnabled());
    LOGGER.info(
        "PipeEventReferenceEliminateIntervalSeconds: {}",
        getPipeEventReferenceEliminateIntervalSeconds());

    LOGGER.info("PipeAutoSplitFullEnabled: {}", getPipeAutoSplitFullEnabled());
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
