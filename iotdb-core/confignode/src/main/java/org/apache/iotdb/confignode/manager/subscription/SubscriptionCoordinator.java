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
import org.apache.iotdb.confignode.consensus.request.read.subscription.ShowSubscriptionPlan;
import org.apache.iotdb.confignode.consensus.request.read.subscription.ShowTopicPlan;
import org.apache.iotdb.confignode.consensus.response.subscription.SubscriptionTableResp;
import org.apache.iotdb.confignode.consensus.response.subscription.TopicTableResp;
import org.apache.iotdb.confignode.manager.ConfigManager;
import org.apache.iotdb.confignode.manager.pipe.coordinator.task.PipeTaskCoordinatorLock;
import org.apache.iotdb.confignode.persistence.subscription.SubscriptionInfo;
import org.apache.iotdb.confignode.rpc.thrift.TCloseConsumerReq;
import org.apache.iotdb.confignode.rpc.thrift.TCreateConsumerReq;
import org.apache.iotdb.confignode.rpc.thrift.TCreateTopicReq;
import org.apache.iotdb.confignode.rpc.thrift.TGetAllSubscriptionInfoResp;
import org.apache.iotdb.confignode.rpc.thrift.TGetAllTopicInfoResp;
import org.apache.iotdb.confignode.rpc.thrift.TShowSubscriptionReq;
import org.apache.iotdb.confignode.rpc.thrift.TShowSubscriptionResp;
import org.apache.iotdb.confignode.rpc.thrift.TShowTopicReq;
import org.apache.iotdb.confignode.rpc.thrift.TShowTopicResp;
import org.apache.iotdb.confignode.rpc.thrift.TSubscribeReq;
import org.apache.iotdb.confignode.rpc.thrift.TUnsubscribeReq;
import org.apache.iotdb.rpc.TSStatusCode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.concurrent.atomic.AtomicReference;

public class SubscriptionCoordinator {

  private static final Logger LOGGER = LoggerFactory.getLogger(SubscriptionCoordinator.class);

  private final ConfigManager configManager;

  // NEVER EXPOSE THIS DIRECTLY TO THE OUTSIDE
  private final SubscriptionInfo subscriptionInfo;

  private final PipeTaskCoordinatorLock coordinatorLock;
  private AtomicReference<SubscriptionInfo> subscriptionInfoHolder;

  private final SubscriptionMetaSyncer subscriptionMetaSyncer;

  public SubscriptionCoordinator(ConfigManager configManager, SubscriptionInfo subscriptionInfo) {
    this.configManager = configManager;
    this.subscriptionInfo = subscriptionInfo;
    this.coordinatorLock = new PipeTaskCoordinatorLock();
    this.subscriptionMetaSyncer = new SubscriptionMetaSyncer(configManager);
  }

  public SubscriptionInfo getSubscriptionInfo() {
    return subscriptionInfo;
  }

  /////////////////////////////// Lock ///////////////////////////////

  public AtomicReference<SubscriptionInfo> tryLock() {
    if (coordinatorLock.tryLock()) {
      subscriptionInfoHolder = new AtomicReference<>(subscriptionInfo);
      return subscriptionInfoHolder;
    }

    return null;
  }

  /**
   * Lock the {@link SubscriptionInfo} coordinator.
   *
   * @return the {@link SubscriptionInfo} holder, which can be used to get the {@link
   *     SubscriptionInfo}. Wait until lock is acquired
   */
  public AtomicReference<SubscriptionInfo> lock() {
    coordinatorLock.lock();
    subscriptionInfoHolder = new AtomicReference<>(subscriptionInfo);
    return subscriptionInfoHolder;
  }

  public boolean unlock() {
    if (subscriptionInfoHolder != null) {
      subscriptionInfoHolder.set(null);
      subscriptionInfoHolder = null;
    }

    try {
      coordinatorLock.unlock();
      return true;
    } catch (IllegalMonitorStateException ignored) {
      // This is thrown if unlock() is called without lock() called first.
      LOGGER.warn("This thread is not holding the lock.");
      return false;
    }
  }

  public boolean isLocked() {
    return coordinatorLock.isLocked();
  }

  /////////////////////////////// Meta sync ///////////////////////////////

  public void startSubscriptionMetaSync() {
    subscriptionMetaSyncer.start();
  }

  public void stopSubscriptionMetaSync() {
    subscriptionMetaSyncer.stop();
  }

  /**
   * Caller should ensure that the method is called in the write lock of {@link #subscriptionInfo}.
   */
  public void updateLastSyncedVersion() {
    subscriptionInfo.updateLastSyncedVersion();
  }

  public boolean canSkipNextSync() {
    return subscriptionInfo.canSkipNextSync();
  }

  /////////////////////////////// Operate ///////////////////////////////

  public TSStatus createTopic(TCreateTopicReq req) {
    final TSStatus status = configManager.getProcedureManager().createTopic(req);
    if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      LOGGER.warn(
          "Failed to create topic {} with attributes {}. Result status: {}.",
          req.getTopicName(),
          req.getTopicAttributes(),
          status);
    }
    return status;
  }

  public TSStatus dropTopic(String topicName) {
    final TSStatus status = configManager.getProcedureManager().dropTopic(topicName);
    if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      LOGGER.warn("Failed to drop topic {}. Result status: {}.", topicName, status);
    }
    return status;
  }

  public TShowTopicResp showTopic(TShowTopicReq req) {
    try {
      return ((TopicTableResp) configManager.getConsensusManager().read(new ShowTopicPlan()))
          .filter(req.getTopicName())
          .convertToTShowTopicResp();
    } catch (Exception e) {
      LOGGER.warn("Failed to show topic info.", e);
      return new TopicTableResp(
              new TSStatus(TSStatusCode.SHOW_TOPIC_ERROR.getStatusCode())
                  .setMessage(e.getMessage()),
              Collections.emptyList())
          .convertToTShowTopicResp();
    }
  }

  public TGetAllTopicInfoResp getAllTopicInfo() {
    try {
      return ((TopicTableResp) configManager.getConsensusManager().read(new ShowTopicPlan()))
          .convertToTGetAllTopicInfoResp();
    } catch (Exception e) {
      LOGGER.warn("Failed to get all topic info.", e);
      return new TGetAllTopicInfoResp(
          new TSStatus(TSStatusCode.SHOW_TOPIC_ERROR.getStatusCode()).setMessage(e.getMessage()),
          Collections.emptyList());
    }
  }

  public TSStatus createConsumer(TCreateConsumerReq req) {
    final TSStatus status = configManager.getProcedureManager().createConsumer(req);
    if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      LOGGER.warn(
          "Failed to create consumer {} in consumer group {}. Result status: {}.",
          req.getConsumerId(),
          req.getConsumerGroupId(),
          status);
    }
    return status;
  }

  public TSStatus dropConsumer(TCloseConsumerReq req) {
    final TSStatus status = configManager.getProcedureManager().dropConsumer(req);
    if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      LOGGER.warn(
          "Failed to close consumer {} in consumer group {}. Result status: {}.",
          req.getConsumerId(),
          req.getConsumerGroupId(),
          status);
    }
    return status;
  }

  public TSStatus createSubscription(TSubscribeReq req) {
    final TSStatus status = configManager.getProcedureManager().createSubscription(req);
    if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      LOGGER.warn(
          "Consumer {} in consumer group {} failed to subscribe topics {}. Result status: {}.",
          req.getConsumerId(),
          req.getConsumerGroupId(),
          req.getTopicNames(),
          status);
    }
    return status;
  }

  public TSStatus dropSubscription(TUnsubscribeReq req) {
    final TSStatus status = configManager.getProcedureManager().dropSubscription(req);
    if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      LOGGER.warn(
          "Consumer {} in consumer group {} failed to unsubscribe topics {}. Result status: {}.",
          req.getConsumerId(),
          req.getConsumerGroupId(),
          req.getTopicNames(),
          status);
    }
    return status;
  }

  public TShowSubscriptionResp showSubscription(TShowSubscriptionReq req) {
    try {
      return ((SubscriptionTableResp)
              configManager.getConsensusManager().read(new ShowSubscriptionPlan()))
          .filter(req.getTopicName())
          .convertToTShowSubscriptionResp();
    } catch (Exception e) {
      LOGGER.warn("Failed to show subscription info.", e);
      return new SubscriptionTableResp(
              new TSStatus(TSStatusCode.SHOW_SUBSCRIPTION_ERROR.getStatusCode())
                  .setMessage(e.getMessage()),
              Collections.emptyList(),
              Collections.emptyList())
          .convertToTShowSubscriptionResp();
    }
  }

  public TGetAllSubscriptionInfoResp getAllSubscriptionInfo() {
    try {
      return ((SubscriptionTableResp)
              configManager.getConsensusManager().read(new ShowSubscriptionPlan()))
          .convertToTGetAllSubscriptionInfoResp();
    } catch (Exception e) {
      LOGGER.warn("Failed to get all subscription info.", e);
      return new TGetAllSubscriptionInfoResp(
          new TSStatus(TSStatusCode.SHOW_SUBSCRIPTION_ERROR.getStatusCode())
              .setMessage(e.getMessage()),
          Collections.emptyList());
    }
  }
}
