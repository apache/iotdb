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

  /////////////////////////////// Data Synchronization ///////////////////////////////

  public int getPipeNonForwardingEventsProgressReportInterval() {
    return COMMON_CONFIG.getPipeNonForwardingEventsProgressReportInterval();
  }

  /////////////////////////////// File ///////////////////////////////

  public String getPipeHardlinkBaseDirName() {
    return COMMON_CONFIG.getPipeHardlinkBaseDirName();
  }

  public String getPipeHardlinkTsFileDirName() {
    return COMMON_CONFIG.getPipeHardlinkTsFileDirName();
  }

  public String getPipeProgressIndexPersistDirName() {
    return COMMON_CONFIG.getPipeProgressIndexPersistDirName();
  }

  public boolean isPipeProgressIndexPersistEnabled() {
    return COMMON_CONFIG.isPipeProgressIndexPersistEnabled();
  }

  public long getPipeProgressIndexPersistCheckPointGap() {
    return COMMON_CONFIG.getPipeProgressIndexPersistCheckPointGap();
  }

  public long getPipeProgressIndexFlushIntervalMs() {
    return COMMON_CONFIG.getPipeProgressIndexFlushIntervalMs();
  }

  public String getPipeHardlinkWALDirName() {
    return COMMON_CONFIG.getPipeHardlinkWALDirName();
  }

  public boolean getPipeHardLinkWALEnabled() {
    return COMMON_CONFIG.getPipeHardLinkWALEnabled();
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

  public double getPipeDataStructureWalMemoryProportion() {
    return COMMON_CONFIG.getPipeDataStructureWalMemoryProportion();
  }

  public double getPipeDataStructureBatchMemoryProportion() {
    return COMMON_CONFIG.getPipeDataStructureBatchMemoryProportion();
  }

  public double getPipeTotalFloatingMemoryProportion() {
    return COMMON_CONFIG.getPipeTotalFloatingMemoryProportion();
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

  public long getPipeSubtaskExecutorForcedRestartIntervalMs() {
    return COMMON_CONFIG.getPipeSubtaskExecutorForcedRestartIntervalMs();
  }

  /////////////////////////////// Extractor ///////////////////////////////

  public int getPipeExtractorAssignerDisruptorRingBufferSize() {
    return COMMON_CONFIG.getPipeExtractorAssignerDisruptorRingBufferSize();
  }

  public long getPipeExtractorAssignerDisruptorRingBufferEntrySizeInBytes() {
    return COMMON_CONFIG.getPipeExtractorAssignerDisruptorRingBufferEntrySizeInBytes();
  }

  public int getPipeExtractorMatcherCacheSize() {
    return COMMON_CONFIG.getPipeExtractorMatcherCacheSize();
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

  public int getPipeAsyncConnectorForcedRetryTsFileEventQueueSizeThreshold() {
    return COMMON_CONFIG.getPipeAsyncConnectorForcedRetryTsFileEventQueueSizeThreshold();
  }

  public int getPipeAsyncConnectorForcedRetryTabletEventQueueSizeThreshold() {
    return COMMON_CONFIG.getPipeAsyncConnectorForcedRetryTabletEventQueueSizeThreshold();
  }

  public int getPipeAsyncConnectorForcedRetryTotalEventQueueSizeThreshold() {
    return COMMON_CONFIG.getPipeAsyncConnectorForcedRetryTotalEventQueueSizeThreshold();
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

  public double getPipeAllConnectorsRateLimitBytesPerSecond() {
    return COMMON_CONFIG.getPipeAllSinksRateLimitBytesPerSecond();
  }

  public int getRateLimiterHotReloadCheckIntervalMs() {
    return COMMON_CONFIG.getRateLimiterHotReloadCheckIntervalMs();
  }

  public int getPipeConnectorRequestSliceThresholdBytes() {
    return COMMON_CONFIG.getPipeConnectorRequestSliceThresholdBytes();
  }

  public float getPipeLeaderCacheMemoryUsagePercentage() {
    return COMMON_CONFIG.getPipeLeaderCacheMemoryUsagePercentage();
  }

  public int getPipeMaxAlignedSeriesNumInOneBatch() {
    return COMMON_CONFIG.getPipeMaxAlignedSeriesNumInOneBatch();
  }

  public long getPipeListeningQueueTransferSnapshotThreshold() {
    return COMMON_CONFIG.getPipeListeningQueueTransferSnapshotThreshold();
  }

  public int getPipeSnapshotExecutionMaxBatchSize() {
    return COMMON_CONFIG.getPipeSnapshotExecutionMaxBatchSize();
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

  /////////////////////////////// Hybrid Mode ///////////////////////////////

  public int getPipeMaxAllowedHistoricalTsFilePerDataRegion() {
    return COMMON_CONFIG.getPipeMaxAllowedHistoricalTsFilePerDataRegion();
  }

  public int getPipeMaxAllowedPendingTsFileEpochPerDataRegion() {
    return COMMON_CONFIG.getPipeMaxAllowedPendingTsFileEpochPerDataRegion();
  }

  public int getPipeMaxAllowedPinnedMemTableCount() {
    return COMMON_CONFIG.getPipeMaxAllowedPinnedMemTableCount();
  }

  public long getPipeMaxAllowedLinkedTsFileCount() {
    return COMMON_CONFIG.getPipeMaxAllowedLinkedTsFileCount();
  }

  public float getPipeMaxAllowedLinkedDeletedTsFileDiskUsagePercentage() {
    return COMMON_CONFIG.getPipeMaxAllowedLinkedDeletedTsFileDiskUsagePercentage();
  }

  public long getPipeStuckRestartIntervalSeconds() {
    return COMMON_CONFIG.getPipeStuckRestartIntervalSeconds();
  }

  public long getPipeStuckRestartMinIntervalMs() {
    return COMMON_CONFIG.getPipeStuckRestartMinIntervalMs();
  }

  public boolean isPipeEpochKeepTsFileAfterStuckRestartEnabled() {
    return COMMON_CONFIG.isPipeEpochKeepTsFileAfterStuckRestartEnabled();
  }

  public long getPipeFlushAfterTerminateCount() {
    return COMMON_CONFIG.getPipeFlushAfterTerminateCount();
  }

  public long getPipeFlushAfterLastTerminateSeconds() {
    return COMMON_CONFIG.getPipeFlushAfterLastTerminateSeconds();
  }

  public long getPipeStorageEngineFlushTimeIntervalMs() {
    return COMMON_CONFIG.getPipeStorageEngineFlushTimeIntervalMs();
  }

  public int getPipeMaxAllowedRemainingInsertEventCountPerPipe() {
    return COMMON_CONFIG.getPipeMaxAllowedRemainingInsertEventCountPerPipe();
  }

  public int getPipeMaxAllowedTotalRemainingInsertEventCount() {
    return COMMON_CONFIG.getPipeMaxAllowedTotalRemainingInsertEventCount();
  }

  /////////////////////////////// Logger ///////////////////////////////

  public int getPipeMetaReportMaxLogNumPerRound() {
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

  public int getPipeWalPinMaxLogNumPerRound() {
    return COMMON_CONFIG.getPipeWalPinMaxLogNumPerRound();
  }

  public int getPipeWalPinMaxLogIntervalRounds() {
    return COMMON_CONFIG.getPipeWalPinMaxLogIntervalRounds();
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

  /////////////////////////////// Ref ///////////////////////////////

  public boolean getPipeEventReferenceTrackingEnabled() {
    return COMMON_CONFIG.getPipeEventReferenceTrackingEnabled();
  }

  public long getPipeEventReferenceEliminateIntervalSeconds() {
    return COMMON_CONFIG.getPipeEventReferenceEliminateIntervalSeconds();
  }

  /////////////////////////////// Utils ///////////////////////////////

  private static final Logger LOGGER = LoggerFactory.getLogger(PipeConfig.class);

  public void printAllConfigs() {
    LOGGER.info(
        "PipeNonForwardingEventsProgressReportInterval: {}",
        getPipeNonForwardingEventsProgressReportInterval());

    LOGGER.info("PipeHardlinkBaseDirName: {}", getPipeHardlinkBaseDirName());
    LOGGER.info("PipeHardlinkTsFileDirName: {}", getPipeHardlinkTsFileDirName());
    LOGGER.info("PipeProgressIndexPersistDirName: {}", getPipeProgressIndexPersistDirName());
    LOGGER.info("PipeProgressIndexPersistEnabled: {}", isPipeProgressIndexPersistEnabled());
    LOGGER.info(
        "PipeProgressIndexPersistCheckPointGap: {}", getPipeProgressIndexPersistCheckPointGap());
    LOGGER.info("PipeProgressIndexFlushIntervalMs: {}", getPipeProgressIndexFlushIntervalMs());
    LOGGER.info("PipeHardlinkWALDirName: {}", getPipeHardlinkWALDirName());
    LOGGER.info("PipeHardLinkWALEnabled: {}", getPipeHardLinkWALEnabled());
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
    LOGGER.info(
        "PipeSubtaskExecutorForcedRestartIntervalMs: {}",
        getPipeSubtaskExecutorForcedRestartIntervalMs());

    LOGGER.info(
        "PipeExtractorAssignerDisruptorRingBufferSize: {}",
        getPipeExtractorAssignerDisruptorRingBufferSize());
    LOGGER.info(
        "PipeExtractorAssignerDisruptorRingBufferEntrySizeInBytes: {}",
        getPipeExtractorAssignerDisruptorRingBufferEntrySizeInBytes());
    LOGGER.info("PipeExtractorMatcherCacheSize: {}", getPipeExtractorMatcherCacheSize());

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
    LOGGER.info("PipeMaxAlignedSeriesNumInOneBatch: {}", getPipeMaxAlignedSeriesNumInOneBatch());
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
        getPipeAsyncConnectorForcedRetryTsFileEventQueueSizeThreshold());
    LOGGER.info(
        "PipeAsyncConnectorForcedRetryTabletEventQueueSizeThreshold: {}",
        getPipeAsyncConnectorForcedRetryTabletEventQueueSizeThreshold());
    LOGGER.info(
        "PipeAsyncConnectorForcedRetryTotalEventQueueSizeThreshold: {}",
        getPipeAsyncConnectorForcedRetryTotalEventQueueSizeThreshold());
    LOGGER.info(
        "PipeAsyncConnectorMaxRetryExecutionTimeMsPerCall: {}",
        getPipeAsyncConnectorMaxRetryExecutionTimeMsPerCall());
    LOGGER.info("PipeAsyncConnectorSelectorNumber: {}", getPipeAsyncConnectorSelectorNumber());
    LOGGER.info("PipeAsyncConnectorMaxClientNumber: {}", getPipeAsyncConnectorMaxClientNumber());
    LOGGER.info(
        "PipeAsyncConnectorMaxTsFileClientNumber: {}",
        getPipeAsyncConnectorMaxTsFileClientNumber());

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

    LOGGER.info(
        "PipeMaxAllowedHistoricalTsFilePerDataRegion: {}",
        getPipeMaxAllowedHistoricalTsFilePerDataRegion());
    LOGGER.info(
        "PipeMaxAllowedPendingTsFileEpochPerDataRegion: {}",
        getPipeMaxAllowedPendingTsFileEpochPerDataRegion());
    LOGGER.info("PipeMaxAllowedPinnedMemTableCount: {}", getPipeMaxAllowedPinnedMemTableCount());
    LOGGER.info("PipeMaxAllowedLinkedTsFileCount: {}", getPipeMaxAllowedLinkedTsFileCount());
    LOGGER.info(
        "PipeMaxAllowedLinkedDeletedTsFileDiskUsagePercentage: {}",
        getPipeMaxAllowedLinkedDeletedTsFileDiskUsagePercentage());
    LOGGER.info("PipeStuckRestartIntervalSeconds: {}", getPipeStuckRestartIntervalSeconds());
    LOGGER.info("PipeStuckRestartMinIntervalMs: {}", getPipeStuckRestartMinIntervalMs());
    LOGGER.info(
        "PipeEpochKeepTsFileAfterStuckRestartEnabled: {}",
        isPipeEpochKeepTsFileAfterStuckRestartEnabled());
    LOGGER.info("PipeFlushAfterTerminateCount: {}", getPipeFlushAfterTerminateCount());
    LOGGER.info("PipeFlushAfterLastTerminateSeconds: {}", getPipeFlushAfterLastTerminateSeconds());
    LOGGER.info(
        "PipeStorageEngineFlushTimeIntervalMs: {}", getPipeStorageEngineFlushTimeIntervalMs());
    LOGGER.info(
        "PipeMaxAllowedRemainingInsertEventCountPerPipe: {}",
        getPipeMaxAllowedRemainingInsertEventCountPerPipe());
    LOGGER.info(
        "PipeMaxAllowedTotalRemainingInsertEventCount: {}",
        getPipeMaxAllowedTotalRemainingInsertEventCount());

    LOGGER.info("PipeMetaReportMaxLogNumPerRound: {}", getPipeMetaReportMaxLogNumPerRound());
    LOGGER.info("PipeMetaReportMaxLogIntervalRounds: {}", getPipeMetaReportMaxLogIntervalRounds());
    LOGGER.info("PipeTsFilePinMaxLogNumPerRound: {}", getPipeTsFilePinMaxLogNumPerRound());
    LOGGER.info("PipeTsFilePinMaxLogIntervalRounds: {}", getPipeTsFilePinMaxLogIntervalRounds());
    LOGGER.info("PipeWalPinMaxLogNumPerRound: {}", getPipeWalPinMaxLogNumPerRound());
    LOGGER.info("PipeWalPinMaxLogIntervalRounds: {}", getPipeWalPinMaxLogIntervalRounds());

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
