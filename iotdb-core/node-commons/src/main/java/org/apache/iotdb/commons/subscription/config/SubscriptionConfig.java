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

  public int getSubscriptionMaxTabletsPerPrefetching() {
    return COMMON_CONFIG.getSubscriptionMaxTabletsPerPrefetching();
  }

  public int getSubscriptionPollMaxBlockingTimeMs() {
    return COMMON_CONFIG.getSubscriptionPollMaxBlockingTimeMs();
  }

  public int getSubscriptionSerializeMaxBlockingTimeMs() {
    return COMMON_CONFIG.getSubscriptionSerializeMaxBlockingTimeMs();
  }

  public int getSubscriptionClearMaxBlockingTimeMs() {
    return COMMON_CONFIG.getSubscriptionClearMaxBlockingTimeMs();
  }

  public long getSubscriptionLaunchRetryIntervalMs() {
    return COMMON_CONFIG.getSubscriptionLaunchRetryIntervalMs();
  }

  public int getSubscriptionClearCommittedEventIntervalSeconds() {
    return COMMON_CONFIG.getSubscriptionClearCommittedEventIntervalSeconds();
  }

  public int getSubscriptionRecycleUncommittedEventIntervalSeconds() {
    return COMMON_CONFIG.getSubscriptionRecycleUncommittedEventIntervalSeconds();
  }

  /////////////////////////////// Utils ///////////////////////////////

  private static final Logger LOGGER = LoggerFactory.getLogger(SubscriptionConfig.class);

  public void printAllConfigs() {
    LOGGER.info(
        "SubscriptionSubtaskExecutorMaxThreadNum: {}",
        getSubscriptionSubtaskExecutorMaxThreadNum());
    LOGGER.info(
        "SubscriptionMaxTabletsPerPrefetching: {}", getSubscriptionMaxTabletsPerPrefetching());
    LOGGER.info("SubscriptionPollMaxBlockingTimeMs: {}", getSubscriptionPollMaxBlockingTimeMs());
    LOGGER.info(
        "SubscriptionSerializeMaxBlockingTimeMs: {}", getSubscriptionSerializeMaxBlockingTimeMs());
    LOGGER.info("SubscriptionClearMaxBlockingTimeMs: {}", getSubscriptionClearMaxBlockingTimeMs());
    LOGGER.info("SubscriptionLaunchRetryIntervalMs: {}", getSubscriptionLaunchRetryIntervalMs());
    LOGGER.info(
        "SubscriptionClearCommittedEventIntervalSeconds: {}",
        getSubscriptionClearCommittedEventIntervalSeconds());
    LOGGER.info(
        "SubscriptionRecycleUncommittedEventIntervalSeconds: {}",
        getSubscriptionRecycleUncommittedEventIntervalSeconds());
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
