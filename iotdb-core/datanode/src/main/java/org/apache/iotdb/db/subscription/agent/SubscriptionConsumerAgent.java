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
import org.apache.iotdb.db.i18n.DataNodeMiscMessages;
import org.apache.iotdb.db.i18n.DataNodePipeMessages;
import org.apache.iotdb.db.subscription.broker.consensus.ConsensusSubscriptionSetupHandler;
import org.apache.iotdb.mpp.rpc.thrift.TPushConsumerGroupMetaRespExceptionMessage;
import org.apache.iotdb.rpc.subscription.exception.SubscriptionException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashSet;
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
          DataNodePipeMessages
              .PIPE_LOG_EXCEPTION_OCCURRED_WHEN_HANDLING_SINGLE_CONSUMER_GROUP_META_10E7688C,
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
      SubscriptionAgent.broker().createPipeBrokerIfNotExist(consumerGroupId);
      ConsensusSubscriptionSetupHandler.setupConsensusSubscriptions(
          consumerGroupId, metaFromCoordinator.getSubscribedTopicNames());
      consumerGroupMetaKeeper.addConsumerGroupMeta(consumerGroupId, metaFromCoordinator);
      return;
    }

    // if the creation time of consumer group meta on local agent is inconsistent with meta from
    // coordinator
    if (metaInAgent.getCreationTime() != metaFromCoordinator.getCreationTime()) {
      if (SubscriptionAgent.broker().isBrokerExist(consumerGroupId)) {
        LOGGER.warn(
            DataNodePipeMessages
                .PIPE_LOG_SUBSCRIPTION_BROKER_BOUND_TO_CONSUMER_GROUP_HAS_ALREADY_0F37997F,
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

      ConsensusSubscriptionSetupHandler.setupConsensusSubscriptions(
          consumerGroupId, metaFromCoordinator.getSubscribedTopicNames());
      consumerGroupMetaKeeper.removeConsumerGroupMeta(consumerGroupId);
      consumerGroupMetaKeeper.addConsumerGroupMeta(consumerGroupId, metaFromCoordinator);
      // no need to create broker manually
      return;
    }

    // remove prefetching queues for topics unsubscribed by the consumer group
    final Set<String> topicsUnsubByGroup =
        ConsumerGroupMeta.getTopicsUnsubByGroup(metaInAgent, metaFromCoordinator);
    final Set<String> pipeTopicsUnsubByGroup = new LinkedHashSet<>();
    final Set<String> consensusTopicsUnsubByGroup = new LinkedHashSet<>();
    for (final String topicName : topicsUnsubByGroup) {
      if (ConsensusSubscriptionSetupHandler.isConsensusBasedTopic(topicName)) {
        consensusTopicsUnsubByGroup.add(topicName);
        continue;
      }
      pipeTopicsUnsubByGroup.add(topicName);
    }
    // Detect newly subscribed topics (present in new meta but not in old meta)
    final Set<String> newlySubscribedTopics =
        ConsumerGroupMeta.getTopicsNewlySubByGroup(metaInAgent, metaFromCoordinator);

    LOGGER.info(
        DataNodePipeMessages
            .PIPE_LOG_SUBSCRIPTION_CONSUMER_GROUP_META_CHANGE_DETECTED_TOPICSUNSUBBYGROUP_F6DAF20A,
        consumerGroupId,
        topicsUnsubByGroup,
        newlySubscribedTopics);

    applyTopicDiff(
        () -> {
          if (!newlySubscribedTopics.isEmpty()) {
            ConsensusSubscriptionSetupHandler.handleNewSubscriptions(
                consumerGroupId, newlySubscribedTopics);
          }
        },
        () -> {
          for (final String topicName : pipeTopicsUnsubByGroup) {
            SubscriptionAgent.broker().removePrefetchingQueue(consumerGroupId, topicName);
          }
          if (!consensusTopicsUnsubByGroup.isEmpty()) {
            ConsensusSubscriptionSetupHandler.teardownConsensusSubscriptions(
                consumerGroupId, consensusTopicsUnsubByGroup);
          }
        },
        () -> {
          // TODO: Currently we fully replace the entire ConsumerGroupMeta without carefully
          // checking the changes in its fields.
          consumerGroupMetaKeeper.removeConsumerGroupMeta(consumerGroupId);
          consumerGroupMetaKeeper.addConsumerGroupMeta(consumerGroupId, metaFromCoordinator);
        });
  }

  static void applyTopicDiff(
      final Runnable setupNewTopics,
      final Runnable teardownRemovedTopics,
      final Runnable publishMeta) {
    setupNewTopics.run();
    teardownRemovedTopics.run();
    publishMeta.run();
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
              DataNodePipeMessages
                  .PIPE_LOG_EXCEPTION_OCCURRED_WHEN_HANDLING_SINGLE_CONSUMER_GROUP_META_10E7688C,
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
      LOGGER.warn(DataNodeMiscMessages.EXCEPTION_DROPPING_CONSUMER_GROUP, consumerGroupId, e);
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
          DataNodePipeMessages
              .PIPE_LOG_SUBSCRIPTION_BROKER_BOUND_TO_CONSUMER_GROUP_DOES_NOT_EXISTED_9F09E4DE,
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
