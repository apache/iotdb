/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at      http://www.apache.org/licenses/LICENSE-2.0  Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the License for the specific language governing permissions and limitations under the License.
 */

package org.apache.iotdb.db.subscription.agent;

import org.apache.iotdb.commons.client.IClientManager;
import org.apache.iotdb.commons.client.exception.ClientManagerException;
import org.apache.iotdb.commons.consensus.ConfigRegionId;
import org.apache.iotdb.commons.subscription.meta.consumer.ConsumerGroupMeta;
import org.apache.iotdb.commons.subscription.meta.consumer.ConsumerGroupMetaKeeper;
import org.apache.iotdb.confignode.rpc.thrift.TCloseConsumerReq;
import org.apache.iotdb.confignode.rpc.thrift.TCreateConsumerReq;
import org.apache.iotdb.confignode.rpc.thrift.TSubscribeReq;
import org.apache.iotdb.confignode.rpc.thrift.TUnsubscribeReq;
import org.apache.iotdb.db.protocol.client.ConfigNodeClient;
import org.apache.iotdb.db.protocol.client.ConfigNodeClientManager;
import org.apache.iotdb.db.protocol.client.ConfigNodeInfo;
import org.apache.iotdb.mpp.rpc.thrift.TPushConsumerGroupRespExceptionMessage;
import org.apache.iotdb.rpc.subscription.payload.config.ConsumerConfig;

import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.Set;

public class SubscriptionConsumerAgent {

  private static final Logger LOGGER = LoggerFactory.getLogger(SubscriptionConsumerAgent.class);

  private final ConsumerGroupMetaKeeper consumerGroupMetaKeeper;

  private static final IClientManager<ConfigRegionId, ConfigNodeClient> CONFIG_NODE_CLIENT_MANAGER =
      ConfigNodeClientManager.getInstance();

  public SubscriptionConsumerAgent() {
    this.consumerGroupMetaKeeper = new ConsumerGroupMetaKeeper();
  }

  //////////////////////////// provided for subscription agent ////////////////////////////

  public void createConsumer(ConsumerConfig consumerConfig) {
    try (ConfigNodeClient configNodeClient =
        CONFIG_NODE_CLIENT_MANAGER.borrowClient(ConfigNodeInfo.CONFIG_REGION_ID)) {
      configNodeClient.createConsumer(
          new TCreateConsumerReq(
                  consumerConfig.getConsumerId(), consumerConfig.getConsumerGroupId())
              .setConsumerAttributes(consumerConfig.getAttribute()));
    } catch (ClientManagerException | TException e) {
      LOGGER.warn("TODO");
    }
  }

  public void dropConsumer(ConsumerConfig consumerConfig) {
    try (ConfigNodeClient configNodeClient =
        CONFIG_NODE_CLIENT_MANAGER.borrowClient(ConfigNodeInfo.CONFIG_REGION_ID)) {
      configNodeClient.closeConsumer(
          new TCloseConsumerReq(
              consumerConfig.getConsumerId(), consumerConfig.getConsumerGroupId()));
    } catch (ClientManagerException | TException e) {
      LOGGER.warn("TODO");
    }

    // TODO: broker TTL if no consumer in consumer group
  }

  public void subscribe(ConsumerConfig consumerConfig, Set<String> topicNames) {
    try (ConfigNodeClient configNodeClient =
        CONFIG_NODE_CLIENT_MANAGER.borrowClient(ConfigNodeInfo.CONFIG_REGION_ID)) {
      configNodeClient.createSubscription(
          new TSubscribeReq(
              consumerConfig.getConsumerId(), consumerConfig.getConsumerGroupId(), topicNames));
    } catch (ClientManagerException | TException e) {
      LOGGER.warn("TODO");
    }
  }

  public void unsubscribe(ConsumerConfig consumerConfig, Set<String> topicNames) {
    try (ConfigNodeClient configNodeClient =
        CONFIG_NODE_CLIENT_MANAGER.borrowClient(ConfigNodeInfo.CONFIG_REGION_ID)) {
      configNodeClient.dropSubscription(
          new TUnsubscribeReq(
              consumerConfig.getConsumerId(), consumerConfig.getConsumerGroupId(), topicNames));
    } catch (ClientManagerException | TException e) {
      LOGGER.warn("TODO");
    }
  }

  ////////////////////////// ConsumerGroupMeta Lock Control //////////////////////////

  protected void acquireReadLock() {
    consumerGroupMetaKeeper.acquireReadLock();
  }

  protected void releaseReadLock() {
    consumerGroupMetaKeeper.releaseReadLock();
  }

  protected void acquireWriteLock() {
    consumerGroupMetaKeeper.acquireWriteLock();
  }

  protected void releaseWriteLock() {
    consumerGroupMetaKeeper.releaseWriteLock();
  }

  ////////////////////////// ConsumerGroupMeta Management Entry //////////////////////////

  public TPushConsumerGroupRespExceptionMessage handleSingleConsumerGroupMetaChanges(
      ConsumerGroupMeta consumerGroupMetaFromCoordinator) {
    acquireWriteLock();
    try {
      handleSingleConsumerGroupMetaChangesInternal(consumerGroupMetaFromCoordinator);
      return null;
    } catch (Exception e) {
      final String consumerGroupId = consumerGroupMetaFromCoordinator.getConsumerGroupId();
      final String errorMessage =
          String.format(
              "Failed to handle single consumer group meta changes for %s, because %s",
              consumerGroupId, e.getMessage());
      LOGGER.warn("Failed to handle single consumer group meta changes for {}", consumerGroupId, e);
      return new TPushConsumerGroupRespExceptionMessage(
          consumerGroupId, errorMessage, System.currentTimeMillis());
    } finally {
      releaseWriteLock();
    }
  }

  private void handleSingleConsumerGroupMetaChangesInternal(
      final ConsumerGroupMeta metaFromCoordinator) {
    final String consumerGroupId = metaFromCoordinator.getConsumerGroupId();
    consumerGroupMetaKeeper.removeConsumerGroupMeta(consumerGroupId);
    consumerGroupMetaKeeper.addConsumerGroupMeta(consumerGroupId, metaFromCoordinator);
  }

  public TPushConsumerGroupRespExceptionMessage handleDropConsumerGroup(String consumerGroupId) {
    acquireWriteLock();
    try {
      handleDropConsumerGroupInternal(consumerGroupId);
      return null;
    } catch (Exception e) {
      final String errorMessage =
          String.format(
              "Failed to drop consumer group %s, because %s", consumerGroupId, e.getMessage());
      LOGGER.warn("Failed to drop consumer group {}", consumerGroupId, e);
      return new TPushConsumerGroupRespExceptionMessage(
          consumerGroupId, errorMessage, System.currentTimeMillis());
    } finally {
      releaseWriteLock();
    }
  }

  private void handleDropConsumerGroupInternal(String consumerGroupId) {
    consumerGroupMetaKeeper.removeConsumerGroupMeta(consumerGroupId);
  }

  public boolean isConsumerExisted(String consumerId, String consumerGroupId) {
    acquireReadLock();
    try {
      final ConsumerGroupMeta consumerGroupMeta =
          consumerGroupMetaKeeper.getConsumerGroupMeta(consumerGroupId);
      return Objects.nonNull(consumerGroupMeta) && consumerGroupMeta.containsConsumer(consumerId);
    } finally {
      releaseReadLock();
    }
  }
}
