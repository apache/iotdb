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

package org.apache.iotdb.confignode.manager.subscription;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.commons.concurrent.ThreadName;
import org.apache.iotdb.commons.concurrent.threadpool.ScheduledExecutorUtil;
import org.apache.iotdb.commons.subscription.config.SubscriptionConfig;
import org.apache.iotdb.confignode.manager.ConfigManager;
import org.apache.iotdb.confignode.manager.ProcedureManager;
import org.apache.iotdb.rpc.TSStatusCode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class SubscriptionMetaSyncer {

  private static final Logger LOGGER = LoggerFactory.getLogger(SubscriptionMetaSyncer.class);

  private static final ScheduledExecutorService SYNC_EXECUTOR =
      IoTDBThreadPoolFactory.newSingleThreadScheduledExecutor(
          ThreadName.SUBSCRIPTION_RUNTIME_META_SYNCER.getName());
  private static final long INITIAL_SYNC_DELAY_MINUTES =
      SubscriptionConfig.getInstance().getSubscriptionMetaSyncerInitialSyncDelayMinutes();
  private static final long SYNC_INTERVAL_MINUTES =
      SubscriptionConfig.getInstance().getSubscriptionMetaSyncerSyncIntervalMinutes();

  private final ConfigManager configManager;

  private Future<?> metaSyncFuture;

  // This variable is used to record whether the last sync operation was successful. If successful,
  // before the next sync operation, it can be determined based on SubscriptionInfoVersion whether
  // the metadata in DN and the latest metadata in CN have been synchronized, thereby skipping
  // unnecessary sync operations.
  private boolean isLastSubscriptionSyncSuccessful = false;

  SubscriptionMetaSyncer(ConfigManager configManager) {
    this.configManager = configManager;
  }

  public synchronized void start() {
    if (metaSyncFuture == null) {
      metaSyncFuture =
          ScheduledExecutorUtil.safelyScheduleWithFixedDelay(
              SYNC_EXECUTOR,
              this::sync,
              INITIAL_SYNC_DELAY_MINUTES,
              SYNC_INTERVAL_MINUTES,
              TimeUnit.MINUTES);
      LOGGER.info("SubscriptionMetaSyncer is started successfully.");
    }
  }

  private synchronized void sync() {
    if (isLastSubscriptionSyncSuccessful
        && configManager.getSubscriptionManager().getSubscriptionCoordinator().canSkipNextSync()) {
      return;
    }

    isLastSubscriptionSyncSuccessful = false;

    if (configManager.getSubscriptionManager().getSubscriptionCoordinator().isLocked()) {
      LOGGER.warn(
          "SubscriptionCoordinatorLock is held by another thread, skip this round of sync to avoid procedure and rpc accumulation as much as possible");
      return;
    }

    final ProcedureManager procedureManager = configManager.getProcedureManager();

    // sync topic meta firstly
    // TODO: consider drop the topic which is subscribed by consumers
    final TSStatus topicMetaSyncStatus = procedureManager.topicMetaSync();
    if (topicMetaSyncStatus.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      LOGGER.warn("Failed to sync topic meta. Result status: {}.", topicMetaSyncStatus);
      return;
    }

    // sync consumer meta if syncing topic meta successfully
    final TSStatus consumerGroupMetaSyncStatus = procedureManager.consumerGroupMetaSync();
    if (consumerGroupMetaSyncStatus.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      LOGGER.warn(
          "Failed to sync consumer group meta. Result status: {}.", consumerGroupMetaSyncStatus);
      return;
    }

    LOGGER.info(
        "After this successful sync, if SubscriptionInfo is empty during this sync and has not been modified afterwards, all subsequent syncs will be skipped");
    isLastSubscriptionSyncSuccessful = true;
  }

  public synchronized void stop() {
    if (metaSyncFuture != null) {
      metaSyncFuture.cancel(false);
      metaSyncFuture = null;
      LOGGER.info("SubscriptionMetaSyncer is stopped successfully.");
    }
  }
}
