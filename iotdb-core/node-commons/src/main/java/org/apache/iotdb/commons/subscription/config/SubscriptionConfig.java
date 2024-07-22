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

  /////////////////////////////// Subtask Executor ///////////////////////////////

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

  public int getSubscriptionSerializeMaxBlockingTimeMs() {
    return COMMON_CONFIG.getSubscriptionSerializeMaxBlockingTimeMs();
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

  public long getSubscriptionTsFileDeduplicationWindowSeconds() {
    return COMMON_CONFIG.getSubscriptionTsFileDeduplicationWindowSeconds();
  }

  /////////////////////////////// Utils ///////////////////////////////

  private static final Logger LOGGER = LoggerFactory.getLogger(SubscriptionConfig.class);

  public void printAllConfigs() {
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
    LOGGER.info(
        "SubscriptionSerializeMaxBlockingTimeMs: {}", getSubscriptionSerializeMaxBlockingTimeMs());
    LOGGER.info("SubscriptionLaunchRetryIntervalMs: {}", getSubscriptionLaunchRetryIntervalMs());
    LOGGER.info(
        "SubscriptionRecycleUncommittedEventIntervalMs: {}",
        getSubscriptionRecycleUncommittedEventIntervalMs());
    LOGGER.info("SubscriptionReadFileBufferSize: {}", getSubscriptionReadFileBufferSize());
    LOGGER.info(
        "SubscriptionTsFileDeduplicationWindowSeconds: {}",
        getSubscriptionTsFileDeduplicationWindowSeconds());
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
