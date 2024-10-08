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
import org.apache.iotdb.commons.enums.PipeRemainingTimeRateAverageTime;

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

  public String getPipeHardlinkWALDirName() {
    return COMMON_CONFIG.getPipeHardlinkWALDirName();
  }

  public boolean getPipeHardLinkWALEnabled() {
    return COMMON_CONFIG.getPipeHardLinkWALEnabled();
  }

  /////////////////////////////// Tablet ///////////////////////////////

  public int getPipeDataStructureTabletRowSize() {
    return COMMON_CONFIG.getPipeDataStructureTabletRowSize();
  }

  public int getPipeDataStructureTabletSizeInBytes() {
    return COMMON_CONFIG.getPipeDataStructureTabletSizeInBytes();
  }

  public double getPipeDataStructureTabletMemoryBlockAllocationRejectThreshold() {
    return COMMON_CONFIG.getPipeDataStructureTabletMemoryBlockAllocationRejectThreshold();
  }

  /////////////////////////////// Subtask Connector ///////////////////////////////

  public int getPipeRealTimeQueuePollHistoryThreshold() {
    return COMMON_CONFIG.getPipeRealTimeQueuePollHistoryThreshold();
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

  public long getPipeConnectorRetryIntervalMs() {
    return COMMON_CONFIG.getPipeConnectorRetryIntervalMs();
  }

  public boolean isPipeConnectorRPCThriftCompressionEnabled() {
    return COMMON_CONFIG.isPipeConnectorRPCThriftCompressionEnabled();
  }

  public int getPipeAsyncConnectorSelectorNumber() {
    return COMMON_CONFIG.getPipeAsyncConnectorSelectorNumber();
  }

  public int getPipeAsyncConnectorMaxClientNumber() {
    return COMMON_CONFIG.getPipeAsyncConnectorMaxClientNumber();
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

  public long getPipeListeningQueueTransferSnapshotThreshold() {
    return COMMON_CONFIG.getPipeListeningQueueTransferSnapshotThreshold();
  }

  public int getPipeSnapshotExecutionMaxBatchSize() {
    return COMMON_CONFIG.getPipeSnapshotExecutionMaxBatchSize();
  }

  public long getPipeRemainingTimeCommitAutoSwitchSeconds() {
    return COMMON_CONFIG.getPipeRemainingTimeCommitRateAutoSwitchSeconds();
  }

  public PipeRemainingTimeRateAverageTime getPipeRemainingTimeCommitRateAverageTime() {
    return COMMON_CONFIG.getPipeRemainingTimeCommitRateAverageTime();
  }

  public double getPipeTsFileScanParsingThreshold() {
    return COMMON_CONFIG.getPipeTsFileScanParsingThreshold();
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

  /////////////////////////////// Subscription ///////////////////////////////

  public float getSubscriptionCacheMemoryUsagePercentage() {
    return COMMON_CONFIG.getSubscriptionCacheMemoryUsagePercentage();
  }

  /////////////////////////////// Utils ///////////////////////////////

  private static final Logger LOGGER = LoggerFactory.getLogger(PipeConfig.class);

  public void printAllConfigs() {
    LOGGER.info(
        "PipeNonForwardingEventsProgressReportInterval: {}",
        getPipeNonForwardingEventsProgressReportInterval());

    LOGGER.info("PipeHardlinkBaseDirName: {}", getPipeHardlinkBaseDirName());
    LOGGER.info("PipeHardlinkTsFileDirName: {}", getPipeHardlinkTsFileDirName());
    LOGGER.info("PipeHardlinkWALDirName: {}", getPipeHardlinkWALDirName());
    LOGGER.info("PipeHardLinkWALEnabled: {}", getPipeHardLinkWALEnabled());

    LOGGER.info("PipeDataStructureTabletRowSize: {}", getPipeDataStructureTabletRowSize());
    LOGGER.info("PipeDataStructureTabletSizeInBytes: {}", getPipeDataStructureTabletSizeInBytes());
    LOGGER.info(
        "PipeDataStructureTabletMemoryBlockAllocationRejectThreshold: {}",
        getPipeDataStructureTabletMemoryBlockAllocationRejectThreshold());

    LOGGER.info(
        "PipeRealTimeQueuePollHistoryThreshold: {}", getPipeRealTimeQueuePollHistoryThreshold());

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
    LOGGER.info("PipeConnectorRetryIntervalMs: {}", getPipeConnectorRetryIntervalMs());
    LOGGER.info(
        "PipeConnectorRPCThriftCompressionEnabled: {}",
        isPipeConnectorRPCThriftCompressionEnabled());
    LOGGER.info(
        "PipeLeaderCacheMemoryUsagePercentage: {}", getPipeLeaderCacheMemoryUsagePercentage());
    LOGGER.info(
        "PipeListeningQueueTransferSnapshotThreshold: {}",
        getPipeListeningQueueTransferSnapshotThreshold());
    LOGGER.info("PipeSnapshotExecutionMaxBatchSize: {}", getPipeSnapshotExecutionMaxBatchSize());
    LOGGER.info(
        "PipeRemainingTimeCommitAutoSwitchSeconds: {}",
        getPipeRemainingTimeCommitAutoSwitchSeconds());
    LOGGER.info(
        "PipeRemainingTimeCommitRateAverageTime: {}", getPipeRemainingTimeCommitRateAverageTime());
    LOGGER.info("PipeTsFileScanParsingThreshold(): {}", getPipeTsFileScanParsingThreshold());

    LOGGER.info("PipeAsyncConnectorSelectorNumber: {}", getPipeAsyncConnectorSelectorNumber());
    LOGGER.info("PipeAsyncConnectorMaxClientNumber: {}", getPipeAsyncConnectorMaxClientNumber());

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

    LOGGER.info(
        "TwoStageAggregateMaxCombinerLiveTimeInMs: {}",
        getTwoStageAggregateMaxCombinerLiveTimeInMs());
    LOGGER.info(
        "TwoStageAggregateDataRegionInfoCacheTimeInMs: {}",
        getTwoStageAggregateDataRegionInfoCacheTimeInMs());
    LOGGER.info(
        "TwoStageAggregateSenderEndPointsCacheInMs: {}",
        getTwoStageAggregateSenderEndPointsCacheInMs());

    LOGGER.info(
        "SubscriptionCacheMemoryUsagePercentage: {}", getSubscriptionCacheMemoryUsagePercentage());
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
