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

import org.apache.iotdb.commons.subscription.meta.consumer.ConsumerGroupMeta;
import org.apache.iotdb.commons.subscription.meta.consumer.ConsumerGroupMetaKeeper;
import org.apache.iotdb.db.subscription.broker.consensus.ConsensusSubscriptionSetupHandler;
import org.apache.iotdb.mpp.rpc.thrift.TPushConsumerGroupMetaRespExceptionMessage;
import org.apache.iotdb.rpc.subscription.exception.SubscriptionException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Objects;
import java.util.Set;

public class SubscriptionConsumerAgent {

  private static final Logger LOGGER = LoggerFactory.getLogger(SubscriptionConsumerAgent.class);

  private final ConsumerGroupMetaKeeper consumerGroupMetaKeeper;

  public SubscriptionConsumerAgent() {
    this.consumerGroupMetaKeeper = new ConsumerGroupMetaKeeper();
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

  public TPushConsumerGroupMetaRespExceptionMessage handleSingleConsumerGroupMetaChanges(
      final ConsumerGroupMeta consumerGroupMetaFromCoordinator) {
    acquireWriteLock();
    try {
      if (consumerGroupMetaFromCoordinator.isEmpty()) {
        handleDropConsumerGroupInternal(consumerGroupMetaFromCoordinator.getConsumerGroupId());
      } else {
        handleSingleConsumerGroupMetaChangesInternal(consumerGroupMetaFromCoordinator);
      }
      return null;
    } catch (final Exception e) {
      final String consumerGroupId = consumerGroupMetaFromCoordinator.getConsumerGroupId();
      LOGGER.warn(
          "Exception occurred when handling single consumer group meta changes for consumer group {}",
          consumerGroupId,
          e);
      final String exceptionMessage =
          String.format(
              "Subscription: Failed to handle single consumer group meta changes for consumer group %s, because %s",
              consumerGroupId, e);
      return new TPushConsumerGroupMetaRespExceptionMessage(
          consumerGroupId, exceptionMessage, System.currentTimeMillis());
    } finally {
      releaseWriteLock();
    }
  }

  private void handleSingleConsumerGroupMetaChangesInternal(
      final ConsumerGroupMeta metaFromCoordinator) {
    final String consumerGroupId = metaFromCoordinator.getConsumerGroupId();
    final ConsumerGroupMeta metaInAgent =
        consumerGroupMetaKeeper.getConsumerGroupMeta(consumerGroupId);

    // if consumer group meta does not exist on local agent
    if (Objects.isNull(metaInAgent)) {
      consumerGroupMetaKeeper.addConsumerGroupMeta(consumerGroupId, metaFromCoordinator);
      SubscriptionAgent.broker().createBrokerIfNotExist(consumerGroupId);
      return;
    }

    // if the creation time of consumer group meta on local agent is inconsistent with meta from
    // coordinator
    if (metaInAgent.getCreationTime() != metaFromCoordinator.getCreationTime()) {
      if (SubscriptionAgent.broker().isBrokerExist(consumerGroupId)) {
        LOGGER.warn(
            "Subscription: broker bound to consumer group [{}] has already existed when the creation time of consumer group meta on local agent {} is inconsistent with meta from coordinator {}, drop it",
            consumerGroupId,
            metaInAgent,
            metaFromCoordinator);
        if (!SubscriptionAgent.broker().dropBroker(consumerGroupId)) {
          final String exceptionMessage =
              String.format(
                  "Failed to drop stale broker bound to consumer group [%s]", consumerGroupId);
          LOGGER.warn(exceptionMessage);
          throw new SubscriptionException(exceptionMessage);
        }
      }

      consumerGroupMetaKeeper.removeConsumerGroupMeta(consumerGroupId);
      consumerGroupMetaKeeper.addConsumerGroupMeta(consumerGroupId, metaFromCoordinator);
      // no need to create broker manually
      return;
    }

    // remove prefetching queues for topics unsubscribed by the consumer group
    final Set<String> topicsUnsubByGroup =
        ConsumerGroupMeta.getTopicsUnsubByGroup(metaInAgent, metaFromCoordinator);
    for (final String topicName : topicsUnsubByGroup) {
      SubscriptionAgent.broker().removePrefetchingQueue(consumerGroupId, topicName);
    }
    // Tear down consensus-based subscriptions for unsubscribed topics
    if (!topicsUnsubByGroup.isEmpty()) {
      ConsensusSubscriptionSetupHandler.teardownConsensusSubscriptions(
          consumerGroupId, topicsUnsubByGroup);
    }

    // Detect newly subscribed topics (present in new meta but not in old meta)
    final Set<String> newlySubscribedTopics =
        ConsumerGroupMeta.getTopicsNewlySubByGroup(metaInAgent, metaFromCoordinator);

    LOGGER.info(
        "Subscription: consumer group [{}] meta change detected, "
            + "topicsUnsubByGroup={}, newlySubscribedTopics={}",
        consumerGroupId,
        topicsUnsubByGroup,
        newlySubscribedTopics);

    // TODO: Currently we fully replace the entire ConsumerGroupMeta without carefully checking the
    //       changes in its fields.
    consumerGroupMetaKeeper.removeConsumerGroupMeta(consumerGroupId);
    consumerGroupMetaKeeper.addConsumerGroupMeta(consumerGroupId, metaFromCoordinator);

    // Set up consensus-based subscription for newly subscribed live-mode topics.
    // This must happen after the meta is updated so that the broker can find the topic config.
    if (!newlySubscribedTopics.isEmpty()) {
      ConsensusSubscriptionSetupHandler.handleNewSubscriptions(
          consumerGroupId, newlySubscribedTopics);
    }
  }

  public TPushConsumerGroupMetaRespExceptionMessage handleConsumerGroupMetaChanges(
      final List<ConsumerGroupMeta> consumerGroupMetasFromCoordinator) {
    acquireWriteLock();
    try {
      for (final ConsumerGroupMeta consumerGroupMetaFromCoordinator :
          consumerGroupMetasFromCoordinator) {
        try {
          handleSingleConsumerGroupMetaChangesInternal(consumerGroupMetaFromCoordinator);
        } catch (final Exception e) {
          final String consumerGroupId = consumerGroupMetaFromCoordinator.getConsumerGroupId();
          LOGGER.warn(
              "Exception occurred when handling single consumer group meta changes for consumer group {}",
              consumerGroupId,
              e);
          final String exceptionMessage =
              String.format(
                  "Subscription: Failed to handle single consumer group meta changes for consumer group %s, because %s",
                  consumerGroupId, e);
          return new TPushConsumerGroupMetaRespExceptionMessage(
              consumerGroupId, exceptionMessage, System.currentTimeMillis());
        }
      }
      return null;
    } finally {
      releaseWriteLock();
    }
  }

  public TPushConsumerGroupMetaRespExceptionMessage handleDropConsumerGroup(
      final String consumerGroupId) {
    acquireWriteLock();
    try {
      handleDropConsumerGroupInternal(consumerGroupId);
      return null;
    } catch (final Exception e) {
      LOGGER.warn("Exception occurred when dropping consumer group {}", consumerGroupId, e);
      final String exceptionMessage =
          String.format(
              "Subscription: Failed to drop consumer group %s, because %s", consumerGroupId, e);
      return new TPushConsumerGroupMetaRespExceptionMessage(
          consumerGroupId, exceptionMessage, System.currentTimeMillis());
    } finally {
      releaseWriteLock();
    }
  }

  private void handleDropConsumerGroupInternal(final String consumerGroupId) {
    if (SubscriptionAgent.broker().isBrokerExist(consumerGroupId)) {
      if (!SubscriptionAgent.broker().dropBroker(consumerGroupId)) {
        final String exceptionMessage =
            String.format("Failed to drop broker bound to consumer group [%s]", consumerGroupId);
        LOGGER.warn(exceptionMessage);
        throw new SubscriptionException(exceptionMessage);
      }
    } else {
      LOGGER.warn(
          "Subscription: broker bound to consumer group [{}] does not existed when the corresponding consumer group meta has already existed on local agent, ignore it",
          consumerGroupId);
    }

    consumerGroupMetaKeeper.removeConsumerGroupMeta(consumerGroupId);
  }

  public boolean isConsumerExisted(final String consumerGroupId, final String consumerId) {
    acquireReadLock();
    try {
      final ConsumerGroupMeta consumerGroupMeta =
          consumerGroupMetaKeeper.getConsumerGroupMeta(consumerGroupId);
      return Objects.nonNull(consumerGroupMeta) && consumerGroupMeta.containsConsumer(consumerId);
    } finally {
      releaseReadLock();
    }
  }

  public Set<String> getTopicNamesSubscribedByConsumer(
      final String consumerGroupId, final String consumerId) {
    acquireReadLock();
    try {
      return consumerGroupMetaKeeper.getTopicsSubscribedByConsumer(consumerGroupId, consumerId);
    } finally {
      releaseReadLock();
    }
  }

  /**
   * Get all active subscriptions: consumerGroupId → set of subscribed topic names. Used by
   * consensus subscription auto-binding when a new DataRegion is created.
   */
  public java.util.Map<String, Set<String>> getAllSubscriptions() {
    acquireReadLock();
    try {
      final java.util.Map<String, Set<String>> result = new java.util.HashMap<>();
      for (final ConsumerGroupMeta meta : consumerGroupMetaKeeper.getAllConsumerGroupMeta()) {
        final Set<String> topics = meta.getSubscribedTopicNames();
        if (!topics.isEmpty()) {
          result.put(meta.getConsumerGroupId(), new java.util.HashSet<>(topics));
        }
      }
      return result;
    } finally {
      releaseReadLock();
    }
  }
}
