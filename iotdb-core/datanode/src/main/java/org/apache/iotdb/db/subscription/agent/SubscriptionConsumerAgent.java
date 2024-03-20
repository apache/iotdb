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

package org.apache.iotdb.db.subscription.agent;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.client.IClientManager;
import org.apache.iotdb.commons.client.exception.ClientManagerException;
import org.apache.iotdb.commons.consensus.ConfigRegionId;
import org.apache.iotdb.commons.exception.SubscriptionException;
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
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.rpc.subscription.payload.config.ConsumerConfig;

import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
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

  public void createConsumer(ConsumerConfig consumerConfig) throws SubscriptionException {
    try (ConfigNodeClient configNodeClient =
        CONFIG_NODE_CLIENT_MANAGER.borrowClient(ConfigNodeInfo.CONFIG_REGION_ID)) {
      final TCreateConsumerReq req =
          new TCreateConsumerReq()
              .setConsumerId(consumerConfig.getConsumerId())
              .setConsumerGroupId(consumerConfig.getConsumerGroupId())
              .setConsumerAttributes(consumerConfig.getAttribute());
      final TSStatus tsStatus = configNodeClient.createConsumer(req);
      if (TSStatusCode.SUCCESS_STATUS.getStatusCode() != tsStatus.getCode()) {
        final String exceptionMessage =
            String.format(
                "Subscription: Failed to create consumer %s in config node, status is %s.",
                consumerConfig, tsStatus);
        LOGGER.warn(exceptionMessage);
        throw new SubscriptionException(exceptionMessage);
      }
    } catch (ClientManagerException | TException e) {
      final String exceptionMessage =
          String.format(
              "Subscription: Failed to create consumer %s in config node, exception is %s.",
              consumerConfig, e.getMessage());
      LOGGER.warn(exceptionMessage);
      throw new SubscriptionException(exceptionMessage);
    }
  }

  public void dropConsumer(ConsumerConfig consumerConfig) throws SubscriptionException {
    try (ConfigNodeClient configNodeClient =
        CONFIG_NODE_CLIENT_MANAGER.borrowClient(ConfigNodeInfo.CONFIG_REGION_ID)) {
      final TCloseConsumerReq req =
          new TCloseConsumerReq()
              .setConsumerId(consumerConfig.getConsumerId())
              .setConsumerGroupId(consumerConfig.getConsumerGroupId());
      final TSStatus tsStatus = configNodeClient.closeConsumer(req);
      if (TSStatusCode.SUCCESS_STATUS.getStatusCode() != tsStatus.getCode()) {
        final String exceptionMessage =
            String.format(
                "Subscription: Failed to close consumer %s in config node, status is %s.",
                consumerConfig, tsStatus);
        LOGGER.warn(exceptionMessage);
        throw new SubscriptionException(exceptionMessage);
      }
    } catch (ClientManagerException | TException e) {
      final String exceptionMessage =
          String.format(
              "Subscription: Failed to close consumer %s in config node, exception is %s.",
              consumerConfig, e.getMessage());
      LOGGER.warn(exceptionMessage);
      throw new SubscriptionException(exceptionMessage);
    }

    // TODO: broker TTL if no consumer in consumer group
    // Currently, even if there are no consumers left in a consumer group on the CN side, the
    // corresponding ConsumerGroupMeta is still retained. Correspondingly, on the DN side, the
    // SubscriptionBroker is also retained, but there are no SubscriptionPrefetchingQueues left
    // within it.
  }

  public void subscribe(ConsumerConfig consumerConfig, Set<String> topicNames)
      throws SubscriptionException {
    try (ConfigNodeClient configNodeClient =
        CONFIG_NODE_CLIENT_MANAGER.borrowClient(ConfigNodeInfo.CONFIG_REGION_ID)) {
      final TSubscribeReq req =
          new TSubscribeReq()
              .setConsumerId(consumerConfig.getConsumerId())
              .setConsumerGroupId(consumerConfig.getConsumerGroupId())
              .setTopicNames(topicNames);
      final TSStatus tsStatus = configNodeClient.createSubscription(req);
      if (TSStatusCode.SUCCESS_STATUS.getStatusCode() != tsStatus.getCode()) {
        final String exceptionMessage =
            String.format(
                "Subscription: Failed to subscribe topics %s for consumer %s in config node, status is %s.",
                topicNames, consumerConfig, tsStatus);
        LOGGER.warn(exceptionMessage);
        throw new SubscriptionException(exceptionMessage);
      }
    } catch (ClientManagerException | TException e) {
      final String exceptionMessage =
          String.format(
              "Subscription: Failed to subscribe topics %s for consumer %s in config node, exception is %s.",
              topicNames, consumerConfig, e.getMessage());
      LOGGER.warn(exceptionMessage);
      throw new SubscriptionException(exceptionMessage);
    }
  }

  public void unsubscribe(ConsumerConfig consumerConfig, Set<String> topicNames)
      throws SubscriptionException {
    try (ConfigNodeClient configNodeClient =
        CONFIG_NODE_CLIENT_MANAGER.borrowClient(ConfigNodeInfo.CONFIG_REGION_ID)) {
      final TUnsubscribeReq req =
          new TUnsubscribeReq()
              .setConsumerId(consumerConfig.getConsumerId())
              .setConsumerGroupId(consumerConfig.getConsumerGroupId())
              .setTopicNames(topicNames);
      final TSStatus tsStatus = configNodeClient.dropSubscription(req);
      if (TSStatusCode.SUCCESS_STATUS.getStatusCode() != tsStatus.getCode()) {
        final String exceptionMessage =
            String.format(
                "Subscription: Failed to unsubscribe topics %s for consumer %s in config node, status is %s.",
                topicNames, consumerConfig, tsStatus);
        LOGGER.warn(exceptionMessage);
        throw new SubscriptionException(exceptionMessage);
      }
    } catch (ClientManagerException | TException e) {
      final String exceptionMessage =
          String.format(
              "Subscription: Failed to unsubscribe topics %s for consumer %s in config node, exception is %s.",
              topicNames, consumerConfig, e.getMessage());
      LOGGER.warn(exceptionMessage);
      throw new SubscriptionException(exceptionMessage);
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
      final String exceptionMessage =
          String.format(
              "Subscription: Failed to handle single consumer group meta changes for consumer group %s, because %s",
              consumerGroupId, e.getMessage());
      LOGGER.warn(exceptionMessage);
      return new TPushConsumerGroupRespExceptionMessage(
          consumerGroupId, exceptionMessage, System.currentTimeMillis());
    } finally {
      releaseWriteLock();
    }
  }

  private void handleSingleConsumerGroupMetaChangesInternal(
      final ConsumerGroupMeta metaFromCoordinator) {
    final String consumerGroupId = metaFromCoordinator.getConsumerGroupId();
    if (!SubscriptionAgent.broker().isBrokerExist(consumerGroupId)) {
      SubscriptionAgent.broker().createBroker(consumerGroupId);
    }
    consumerGroupMetaKeeper.removeConsumerGroupMeta(consumerGroupId);
    consumerGroupMetaKeeper.addConsumerGroupMeta(consumerGroupId, metaFromCoordinator);
  }

  public TPushConsumerGroupRespExceptionMessage handleConsumerGroupMetaChanges(
      List<ConsumerGroupMeta> consumerGroupMetasFromCoordinator) {
    acquireWriteLock();
    try {
      for (ConsumerGroupMeta consumerGroupMetaFromCoordinator : consumerGroupMetasFromCoordinator) {
        try {
          handleSingleConsumerGroupMetaChangesInternal(consumerGroupMetaFromCoordinator);
          return null;
        } catch (Exception e) {
          final String consumerGroupId = consumerGroupMetaFromCoordinator.getConsumerGroupId();
          final String exceptionMessage =
              String.format(
                  "Subscription: Failed to handle single consumer group meta changes for consumer group %s, because %s",
                  consumerGroupId, e.getMessage());
          LOGGER.warn(exceptionMessage);
          return new TPushConsumerGroupRespExceptionMessage(
              consumerGroupId, exceptionMessage, System.currentTimeMillis());
        }
      }
      return null;
    } finally {
      releaseWriteLock();
    }
  }

  public TPushConsumerGroupRespExceptionMessage handleDropConsumerGroup(String consumerGroupId) {
    acquireWriteLock();
    try {
      handleDropConsumerGroupInternal(consumerGroupId);
      return null;
    } catch (Exception e) {
      final String exceptionMessage =
          String.format(
              "Subscription: Failed to drop consumer group %s, because %s",
              consumerGroupId, e.getMessage());
      LOGGER.warn(exceptionMessage);
      return new TPushConsumerGroupRespExceptionMessage(
          consumerGroupId, exceptionMessage, System.currentTimeMillis());
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
