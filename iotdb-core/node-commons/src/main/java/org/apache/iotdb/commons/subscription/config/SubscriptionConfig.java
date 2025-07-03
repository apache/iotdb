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

  /////////////////////////////// Utils ///////////////////////////////

  private static final Logger LOGGER = LoggerFactory.getLogger(SubscriptionConfig.class);

  public void printAllConfigs() {
    LOGGER.info("SubscriptionEnabled: {}", getSubscriptionEnabled());

    LOGGER.info(
        "SubscriptionCacheMemoryUsagePercentage: {}", getSubscriptionCacheMemoryUsagePercentage());
    LOGGER.info(
        "SubscriptionSubtaskExecutorMaxThreadNum: {}",
        getSubscriptionSubtaskExecutorMaxThreadNum());

    LOGGER.info(
        "SubscriptionPrefetchTabletBatchMaxDelayInMs: {}",
        getSubscriptionPrefetchTabletBatchMaxDelayInMs());
    LOGGER.info(
        "SubscriptionPrefetchTabletBatchMaxSizeInBytes: {}",
        getSubscriptionPrefetchTabletBatchMaxSizeInBytes());
    LOGGER.info(
        "SubscriptionPrefetchTsFileBatchMaxDelayInMs: {}",
        getSubscriptionPrefetchTsFileBatchMaxDelayInMs());
    LOGGER.info(
        "SubscriptionPrefetchTsFileBatchMaxSizeInBytes: {}",
        getSubscriptionPrefetchTsFileBatchMaxSizeInBytes());
    LOGGER.info("SubscriptionPollMaxBlockingTimeMs: {}", getSubscriptionPollMaxBlockingTimeMs());
    LOGGER.info("SubscriptionDefaultTimeoutInMs: {}", getSubscriptionDefaultTimeoutInMs());
    LOGGER.info("SubscriptionLaunchRetryIntervalMs: {}", getSubscriptionLaunchRetryIntervalMs());
    LOGGER.info(
        "SubscriptionRecycleUncommittedEventIntervalMs: {}",
        getSubscriptionRecycleUncommittedEventIntervalMs());
    LOGGER.info("SubscriptionReadFileBufferSize: {}", getSubscriptionReadFileBufferSize());
    LOGGER.info("SubscriptionReadTabletBufferSize: {}", getSubscriptionReadTabletBufferSize());
    LOGGER.info(
        "SubscriptionTsFileDeduplicationWindowSeconds: {}",
        getSubscriptionTsFileDeduplicationWindowSeconds());
    LOGGER.info(
        "SubscriptionCheckMemoryEnoughIntervalMs: {}",
        getSubscriptionCheckMemoryEnoughIntervalMs());
    LOGGER.info(
        "SubscriptionEstimatedInsertNodeTabletInsertionEventSize: {}",
        getSubscriptionEstimatedInsertNodeTabletInsertionEventSize());
    LOGGER.info(
        "SubscriptionEstimatedRawTabletInsertionEventSize: {}",
        getSubscriptionEstimatedRawTabletInsertionEventSize());
    LOGGER.info(
        "SubscriptionMaxAllowedEventCountInTabletBatch: {}",
        getSubscriptionMaxAllowedEventCountInTabletBatch());
    LOGGER.info(
        "SubscriptionLogManagerWindowSeconds: {}", getSubscriptionLogManagerWindowSeconds());
    LOGGER.info(
        "SubscriptionLogManagerBaseIntervalMs: {}", getSubscriptionLogManagerBaseIntervalMs());

    LOGGER.info("SubscriptionPrefetchEnabled: {}", getSubscriptionPrefetchEnabled());
    LOGGER.info(
        "SubscriptionPrefetchMemoryThreshold: {}", getSubscriptionPrefetchMemoryThreshold());
    LOGGER.info(
        "SubscriptionPrefetchMissingRateThreshold: {}",
        getSubscriptionPrefetchMissingRateThreshold());
    LOGGER.info(
        "SubscriptionPrefetchEventLocalCountThreshold: {}",
        getSubscriptionPrefetchEventLocalCountThreshold());
    LOGGER.info(
        "SubscriptionPrefetchEventGlobalCountThreshold: {}",
        getSubscriptionPrefetchEventGlobalCountThreshold());

    LOGGER.info(
        "SubscriptionMetaSyncerInitialSyncDelayMinutes: {}",
        getSubscriptionMetaSyncerInitialSyncDelayMinutes());
    LOGGER.info(
        "SubscriptionMetaSyncerSyncIntervalMinutes: {}",
        getSubscriptionMetaSyncerSyncIntervalMinutes());
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
