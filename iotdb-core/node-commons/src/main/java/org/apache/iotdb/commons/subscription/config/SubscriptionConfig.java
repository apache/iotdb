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

package org.apache.iotdb.commons.subscription.config;

import org.apache.iotdb.commons.conf.CommonConfig;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.i18n.PipeMessages;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SubscriptionConfig {

  private static final CommonConfig COMMON_CONFIG = CommonDescriptor.getInstance().getConfig();

  public boolean getSubscriptionEnabled() {
    return COMMON_CONFIG.getSubscriptionEnabled();
  }

  public float getSubscriptionCacheMemoryUsagePercentage() {
    return COMMON_CONFIG.getSubscriptionCacheMemoryUsagePercentage();
  }

  public int getSubscriptionSubtaskExecutorMaxThreadNum() {
    return COMMON_CONFIG.getSubscriptionSubtaskExecutorMaxThreadNum();
  }

  public int getSubscriptionConsensusPrefetchExecutorMaxThreadNum() {
    return COMMON_CONFIG.getSubscriptionConsensusPrefetchExecutorMaxThreadNum();
  }

  public int getSubscriptionPrefetchTabletBatchMaxDelayInMs() {
    return COMMON_CONFIG.getSubscriptionPrefetchTabletBatchMaxDelayInMs();
  }

  public long getSubscriptionPrefetchTabletBatchMaxSizeInBytes() {
    return COMMON_CONFIG.getSubscriptionPrefetchTabletBatchMaxSizeInBytes();
  }

  public int getSubscriptionPrefetchTsFileBatchMaxDelayInMs() {
    return COMMON_CONFIG.getSubscriptionPrefetchTsFileBatchMaxDelayInMs();
  }

  public long getSubscriptionPrefetchTsFileBatchMaxSizeInBytes() {
    return COMMON_CONFIG.getSubscriptionPrefetchTsFileBatchMaxSizeInBytes();
  }

  public int getSubscriptionPollMaxBlockingTimeMs() {
    return COMMON_CONFIG.getSubscriptionPollMaxBlockingTimeMs();
  }

  public int getSubscriptionDefaultTimeoutInMs() {
    return COMMON_CONFIG.getSubscriptionDefaultTimeoutInMs();
  }

  public long getSubscriptionLaunchRetryIntervalMs() {
    return COMMON_CONFIG.getSubscriptionLaunchRetryIntervalMs();
  }

  public int getSubscriptionRecycleUncommittedEventIntervalMs() {
    return COMMON_CONFIG.getSubscriptionRecycleUncommittedEventIntervalMs();
  }

  public long getSubscriptionReadFileBufferSize() {
    return COMMON_CONFIG.getSubscriptionReadFileBufferSize();
  }

  public long getSubscriptionReadTabletBufferSize() {
    return COMMON_CONFIG.getSubscriptionReadTabletBufferSize();
  }

  public long getSubscriptionTsFileDeduplicationWindowSeconds() {
    return COMMON_CONFIG.getSubscriptionTsFileDeduplicationWindowSeconds();
  }

  public long getSubscriptionCheckMemoryEnoughIntervalMs() {
    return COMMON_CONFIG.getSubscriptionCheckMemoryEnoughIntervalMs();
  }

  public long getSubscriptionEstimatedInsertNodeTabletInsertionEventSize() {
    return COMMON_CONFIG.getSubscriptionEstimatedInsertNodeTabletInsertionEventSize();
  }

  public long getSubscriptionEstimatedRawTabletInsertionEventSize() {
    return COMMON_CONFIG.getSubscriptionEstimatedRawTabletInsertionEventSize();
  }

  public long getSubscriptionMaxAllowedEventCountInTabletBatch() {
    return COMMON_CONFIG.getSubscriptionMaxAllowedEventCountInTabletBatch();
  }

  public long getSubscriptionLogManagerWindowSeconds() {
    return COMMON_CONFIG.getSubscriptionLogManagerWindowSeconds();
  }

  public long getSubscriptionLogManagerBaseIntervalMs() {
    return COMMON_CONFIG.getSubscriptionLogManagerBaseIntervalMs();
  }

  public boolean getSubscriptionPrefetchEnabled() {
    return COMMON_CONFIG.getSubscriptionPrefetchEnabled();
  }

  public float getSubscriptionPrefetchMemoryThreshold() {
    return COMMON_CONFIG.getSubscriptionPrefetchMemoryThreshold();
  }

  public float getSubscriptionPrefetchMissingRateThreshold() {
    return COMMON_CONFIG.getSubscriptionPrefetchMissingRateThreshold();
  }

  public int getSubscriptionPrefetchEventLocalCountThreshold() {
    return COMMON_CONFIG.getSubscriptionPrefetchEventLocalCountThreshold();
  }

  public int getSubscriptionPrefetchEventGlobalCountThreshold() {
    return COMMON_CONFIG.getSubscriptionPrefetchEventGlobalCountThreshold();
  }

  public long getSubscriptionMetaSyncerInitialSyncDelayMinutes() {
    return COMMON_CONFIG.getSubscriptionMetaSyncerInitialSyncDelayMinutes();
  }

  public long getSubscriptionMetaSyncerSyncIntervalMinutes() {
    return COMMON_CONFIG.getSubscriptionMetaSyncerSyncIntervalMinutes();
  }

  public long getSubscriptionOwnerLeaseDurationMsMin() {
    return COMMON_CONFIG.getSubscriptionOwnerLeaseDurationMsMin();
  }

  // Consensus subscription batching parameters
  public int getSubscriptionConsensusBatchMaxDelayInMs() {
    return COMMON_CONFIG.getSubscriptionConsensusBatchMaxDelayInMs();
  }

  public long getSubscriptionConsensusBatchMaxSizeInBytes() {
    return COMMON_CONFIG.getSubscriptionConsensusBatchMaxSizeInBytes();
  }

  public int getSubscriptionConsensusBatchMaxTabletCount() {
    return COMMON_CONFIG.getSubscriptionConsensusBatchMaxTabletCount();
  }

  public int getSubscriptionConsensusBatchMaxWalEntries() {
    return COMMON_CONFIG.getSubscriptionConsensusBatchMaxWalEntries();
  }

  public int getSubscriptionConsensusCommitPersistInterval() {
    return COMMON_CONFIG.getSubscriptionConsensusCommitPersistInterval();
  }

  public boolean isSubscriptionConsensusCommitFsyncEnabled() {
    return COMMON_CONFIG.isSubscriptionConsensusCommitFsyncEnabled();
  }

  public long getSubscriptionConsensusConsumerEvictionTimeoutMs() {
    return COMMON_CONFIG.getSubscriptionConsensusConsumerEvictionTimeoutMs();
  }

  public boolean isSubscriptionConsensusLagBasedPriority() {
    return COMMON_CONFIG.isSubscriptionConsensusLagBasedPriority();
  }

  public int getSubscriptionConsensusPrefetchingQueueCapacity() {
    return COMMON_CONFIG.getSubscriptionConsensusPrefetchingQueueCapacity();
  }

  public long getSubscriptionConsensusWatermarkIntervalMs() {
    if (!COMMON_CONFIG.isSubscriptionConsensusWatermarkEnabled()) {
      return -1;
    }
    return COMMON_CONFIG.getSubscriptionConsensusWatermarkIntervalMs();
  }

  public long getSubscriptionConsensusIdleSafeTimeBarrierIntervalMs() {
    return COMMON_CONFIG.getSubscriptionConsensusIdleSafeTimeBarrierIntervalMs();
  }

  /////////////////////////////// Utils ///////////////////////////////

  private static final Logger LOGGER = LoggerFactory.getLogger(SubscriptionConfig.class);

  public void printAllConfigs() {
    LOGGER.info(PipeMessages.LOG_SUBSCRIPTIONENABLED_ARG_6F9EC0F9, getSubscriptionEnabled());
    LOGGER.info(
        PipeMessages.CONFIG_SUBSCRIPTION_CACHE_MEMORY_USAGE_PERCENTAGE,
        getSubscriptionCacheMemoryUsagePercentage());
    LOGGER.info(
        PipeMessages.CONFIG_SUBSCRIPTION_SUBTASK_EXECUTOR_MAX_THREAD_NUM,
        getSubscriptionSubtaskExecutorMaxThreadNum());
    LOGGER.info(
        PipeMessages.LOG_SUBSCRIPTIONCONSENSUSPREFETCHEXECUTORMAXTHREADNUM_ARG_94D0BD76,
        getSubscriptionConsensusPrefetchExecutorMaxThreadNum());

    LOGGER.info(
        PipeMessages.CONFIG_SUBSCRIPTION_PREFETCH_TABLET_BATCH_MAX_DELAY_IN_MS,
        getSubscriptionPrefetchTabletBatchMaxDelayInMs());
    LOGGER.info(
        PipeMessages.CONFIG_SUBSCRIPTION_PREFETCH_TABLET_BATCH_MAX_SIZE_IN_BYTES,
        getSubscriptionPrefetchTabletBatchMaxSizeInBytes());
    LOGGER.info(
        PipeMessages.CONFIG_SUBSCRIPTION_PREFETCH_TSFILE_BATCH_MAX_DELAY_IN_MS,
        getSubscriptionPrefetchTsFileBatchMaxDelayInMs());
    LOGGER.info(
        PipeMessages.CONFIG_SUBSCRIPTION_PREFETCH_TSFILE_BATCH_MAX_SIZE_IN_BYTES,
        getSubscriptionPrefetchTsFileBatchMaxSizeInBytes());
    LOGGER.info(
        PipeMessages.CONFIG_SUBSCRIPTION_POLL_MAX_BLOCKING_TIME_MS,
        getSubscriptionPollMaxBlockingTimeMs());
    LOGGER.info(
        PipeMessages.CONFIG_SUBSCRIPTION_DEFAULT_TIMEOUT_IN_MS,
        getSubscriptionDefaultTimeoutInMs());
    LOGGER.info(
        PipeMessages.CONFIG_SUBSCRIPTION_LAUNCH_RETRY_INTERVAL_MS,
        getSubscriptionLaunchRetryIntervalMs());
    LOGGER.info(
        PipeMessages.CONFIG_SUBSCRIPTION_RECYCLE_UNCOMMITTED_EVENT_INTERVAL_MS,
        getSubscriptionRecycleUncommittedEventIntervalMs());
    LOGGER.info(
        PipeMessages.CONFIG_SUBSCRIPTION_READ_FILE_BUFFER_SIZE,
        getSubscriptionReadFileBufferSize());
    LOGGER.info(
        PipeMessages.CONFIG_SUBSCRIPTION_READ_TABLET_BUFFER_SIZE,
        getSubscriptionReadTabletBufferSize());
    LOGGER.info(
        PipeMessages.CONFIG_SUBSCRIPTION_TSFILE_DEDUPLICATION_WINDOW_SECONDS,
        getSubscriptionTsFileDeduplicationWindowSeconds());
    LOGGER.info(
        PipeMessages.CONFIG_SUBSCRIPTION_CHECK_MEMORY_ENOUGH_INTERVAL_MS,
        getSubscriptionCheckMemoryEnoughIntervalMs());
    LOGGER.info(
        PipeMessages.CONFIG_SUBSCRIPTION_ESTIMATED_INSERT_NODE_TABLET_INSERTION_EVENT_SIZE,
        getSubscriptionEstimatedInsertNodeTabletInsertionEventSize());
    LOGGER.info(
        PipeMessages.CONFIG_SUBSCRIPTION_ESTIMATED_RAW_TABLET_INSERTION_EVENT_SIZE,
        getSubscriptionEstimatedRawTabletInsertionEventSize());
    LOGGER.info(
        PipeMessages.CONFIG_SUBSCRIPTION_MAX_ALLOWED_EVENT_COUNT_IN_TABLET_BATCH,
        getSubscriptionMaxAllowedEventCountInTabletBatch());
    LOGGER.info(
        PipeMessages.CONFIG_SUBSCRIPTION_LOG_MANAGER_WINDOW_SECONDS,
        getSubscriptionLogManagerWindowSeconds());
    LOGGER.info(
        PipeMessages.CONFIG_SUBSCRIPTION_LOG_MANAGER_BASE_INTERVAL_MS,
        getSubscriptionLogManagerBaseIntervalMs());

    LOGGER.info(
        PipeMessages.CONFIG_SUBSCRIPTION_PREFETCH_ENABLED, getSubscriptionPrefetchEnabled());
    LOGGER.info(
        PipeMessages.CONFIG_SUBSCRIPTION_PREFETCH_MEMORY_THRESHOLD,
        getSubscriptionPrefetchMemoryThreshold());
    LOGGER.info(
        PipeMessages.CONFIG_SUBSCRIPTION_PREFETCH_MISSING_RATE_THRESHOLD,
        getSubscriptionPrefetchMissingRateThreshold());
    LOGGER.info(
        PipeMessages.CONFIG_SUBSCRIPTION_PREFETCH_EVENT_LOCAL_COUNT_THRESHOLD,
        getSubscriptionPrefetchEventLocalCountThreshold());
    LOGGER.info(
        PipeMessages.CONFIG_SUBSCRIPTION_PREFETCH_EVENT_GLOBAL_COUNT_THRESHOLD,
        getSubscriptionPrefetchEventGlobalCountThreshold());

    LOGGER.info(
        PipeMessages.CONFIG_SUBSCRIPTION_META_SYNCER_INITIAL_SYNC_DELAY_MINUTES,
        getSubscriptionMetaSyncerInitialSyncDelayMinutes());
    LOGGER.info(
        PipeMessages.CONFIG_SUBSCRIPTION_META_SYNCER_SYNC_INTERVAL_MINUTES,
        getSubscriptionMetaSyncerSyncIntervalMinutes());

    LOGGER.info(
        PipeMessages.LOG_SUBSCRIPTIONCONSENSUSBATCHMAXDELAYINMS_ARG_38C2CB8B,
        getSubscriptionConsensusBatchMaxDelayInMs());
    LOGGER.info(
        PipeMessages.LOG_SUBSCRIPTIONCONSENSUSBATCHMAXSIZEINBYTES_ARG_F8D28441,
        getSubscriptionConsensusBatchMaxSizeInBytes());
    LOGGER.info(
        PipeMessages.LOG_SUBSCRIPTIONCONSENSUSBATCHMAXTABLETCOUNT_ARG_60BB1D6A,
        getSubscriptionConsensusBatchMaxTabletCount());
    LOGGER.info(
        PipeMessages.LOG_SUBSCRIPTIONCONSENSUSBATCHMAXWALENTRIES_ARG_9BF1CAE4,
        getSubscriptionConsensusBatchMaxWalEntries());
    LOGGER.info(
        PipeMessages.LOG_SUBSCRIPTIONCONSENSUSIDLESAFETIMEBARRIERINTERVALMS_ARG_A6944544,
        getSubscriptionConsensusIdleSafeTimeBarrierIntervalMs());
  }

  /////////////////////////////// Singleton ///////////////////////////////

  private SubscriptionConfig() {}

  public static SubscriptionConfig getInstance() {
    return SubscriptionConfig.SubscriptionConfigHolder.INSTANCE;
  }

  private static class SubscriptionConfigHolder {
    private static final SubscriptionConfig INSTANCE = new SubscriptionConfig();
  }
}
