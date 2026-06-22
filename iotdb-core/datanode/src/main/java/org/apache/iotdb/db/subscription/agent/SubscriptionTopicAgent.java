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
import org.apache.iotdb.commons.subscription.meta.topic.TopicMeta;
import org.apache.iotdb.commons.subscription.meta.topic.TopicMetaKeeper;
import org.apache.iotdb.db.i18n.DataNodeMiscMessages;
import org.apache.iotdb.mpp.rpc.thrift.TPushTopicMetaRespExceptionMessage;
import org.apache.iotdb.mpp.rpc.thrift.TTopicOwnerLeaseEntry;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.rpc.subscription.config.ConsumerConfig;
import org.apache.iotdb.rpc.subscription.config.TopicConfig;
import org.apache.iotdb.rpc.subscription.config.TopicConstant;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

public class SubscriptionTopicAgent {

  private static final Logger LOGGER = LoggerFactory.getLogger(SubscriptionTopicAgent.class);

  private final TopicMetaKeeper topicMetaKeeper;

  public SubscriptionTopicAgent() {
    this.topicMetaKeeper = new TopicMetaKeeper();
  }

  ////////////////////////// TopicMeta Lock Control //////////////////////////

  protected void acquireReadLock() {
    topicMetaKeeper.acquireReadLock();
  }

  protected void releaseReadLock() {
    topicMetaKeeper.releaseReadLock();
  }

  protected void acquireWriteLock() {
    topicMetaKeeper.acquireWriteLock();
  }

  protected void releaseWriteLock() {
    topicMetaKeeper.releaseWriteLock();
  }

  ////////////////////////// Topic Management Entry //////////////////////////

  public TPushTopicMetaRespExceptionMessage handleSingleTopicMetaChanges(
      final TopicMeta topicMetaFromCoordinator) {
    acquireWriteLock();
    try {
      handleSingleTopicMetaChangesInternal(topicMetaFromCoordinator);
      return null;
    } catch (final Exception e) {
      final String topicName = topicMetaFromCoordinator.getTopicName();
      LOGGER.warn(
          "Exception occurred when handling single topic meta changes for topic {}", topicName, e);
      final String exceptionMessage =
          String.format(
              "Subscription: Failed to handle single topic meta changes for topic %s, because %s",
              topicName, e);
      return new TPushTopicMetaRespExceptionMessage(
          topicName, exceptionMessage, System.currentTimeMillis());
    } finally {
      releaseWriteLock();
    }
  }

  private void handleSingleTopicMetaChangesInternal(final TopicMeta metaFromCoordinator) {
    final String topicName = metaFromCoordinator.getTopicName();
    TopicMeta.validateOwnerProgression(
        topicMetaKeeper.getTopicMeta(topicName), metaFromCoordinator);
    topicMetaKeeper.removeTopicMeta(topicName);
    topicMetaKeeper.addTopicMeta(topicName, metaFromCoordinator);
    SubscriptionAgent.broker()
        .refreshConsensusQueueOrderMode(topicName, metaFromCoordinator.getConfig().getOrderMode());
  }

  public TPushTopicMetaRespExceptionMessage handleTopicMetaChanges(
      final List<TopicMeta> topicMetasFromCoordinator) {
    acquireWriteLock();
    try {
      for (final TopicMeta topicMetaFromCoordinator : topicMetasFromCoordinator) {
        try {
          handleSingleTopicMetaChangesInternal(topicMetaFromCoordinator);
        } catch (final Exception e) {
          final String topicName = topicMetaFromCoordinator.getTopicName();
          LOGGER.warn(
              "Exception occurred when handling single topic meta changes for topic {}",
              topicName,
              e);
          final String exceptionMessage =
              String.format(
                  "Subscription: Failed to handle single topic meta changes for topic %s, because %s",
                  topicName, e);
          return new TPushTopicMetaRespExceptionMessage(
              topicName, exceptionMessage, System.currentTimeMillis());
        }
      }
      return null;
    } finally {
      releaseWriteLock();
    }
  }

  public TPushTopicMetaRespExceptionMessage handleDropTopic(final String topicName) {
    acquireWriteLock();
    try {
      handleDropTopicInternal(topicName);
      return null;
    } catch (final Exception e) {
      LOGGER.warn(DataNodeMiscMessages.EXCEPTION_DROPPING_TOPIC, topicName, e);
      final String exceptionMessage =
          String.format("Subscription: Failed to drop topic %s, because %s", topicName, e);
      return new TPushTopicMetaRespExceptionMessage(
          topicName, exceptionMessage, System.currentTimeMillis());
    } finally {
      releaseWriteLock();
    }
  }

  private void handleDropTopicInternal(final String topicName) {
    topicMetaKeeper.removeTopicMeta(topicName);
  }

  public boolean isTopicExisted(final String topicName) {
    acquireReadLock();
    try {
      return topicMetaKeeper.containsTopicMeta(topicName);
    } finally {
      releaseReadLock();
    }
  }

  public String getTopicFormat(final String topicName) {
    acquireReadLock();
    try {
      return topicMetaKeeper.containsTopicMeta(topicName)
          ? topicMetaKeeper
              .getTopicMeta(topicName)
              .getConfig()
              .getStringOrDefault(TopicConstant.FORMAT_KEY, TopicConstant.FORMAT_DEFAULT_VALUE)
          : null;
    } finally {
      releaseReadLock();
    }
  }

  public String getTopicMode(final String topicName) {
    acquireReadLock();
    try {
      return topicMetaKeeper.containsTopicMeta(topicName)
          ? topicMetaKeeper.getTopicMeta(topicName).getConfig().getMode()
          : null;
    } finally {
      releaseReadLock();
    }
  }

  public String getTopicOrderMode(final String topicName) {
    acquireReadLock();
    try {
      return topicMetaKeeper.getTopicMeta(topicName).getConfig().getOrderMode();
    } finally {
      releaseReadLock();
    }
  }

  public Map<String, TopicConfig> getTopicConfigs(final Set<String> topicNames) {
    acquireReadLock();
    try {
      return topicNames.stream()
          .filter(topicMetaKeeper::containsTopicMeta)
          .collect(
              Collectors.toMap(
                  topicName -> topicName,
                  topicName -> topicMetaKeeper.getTopicMeta(topicName).getConfig()));
    } finally {
      releaseReadLock();
    }
  }

  public TSStatus checkTopicOwner(final ConsumerConfig consumerConfig, final String topicName) {
    acquireReadLock();
    try {
      final TopicMeta topicMeta = topicMetaKeeper.getTopicMeta(topicName);
      if (Objects.isNull(topicMeta) || !topicMeta.isOwnerFencingEnabled()) {
        return RpcUtils.SUCCESS_STATUS;
      }

      final String requestOwnerId = consumerConfig.getOwnerId();
      if (Objects.isNull(requestOwnerId)) {
        return RpcUtils.getStatus(
            TSStatusCode.SUBSCRIPTION_OWNER_REQUIRED,
            String.format(
                "Subscription: topic %s enables owner fencing, but consumer %s does not carry owner-id.",
                topicName, consumerConfig));
      }

      final Long requestOwnerEpoch = consumerConfig.getOwnerEpoch();
      if (Objects.isNull(requestOwnerEpoch)) {
        return RpcUtils.getStatus(
            TSStatusCode.SUBSCRIPTION_OWNER_EPOCH_REQUIRED,
            String.format(
                "Subscription: topic %s enables owner fencing, but consumer %s does not carry owner-epoch.",
                topicName, consumerConfig));
      }

      if (Objects.nonNull(topicMeta.getOwnerLeaseExpireTimeMs())
          && System.currentTimeMillis() > topicMeta.getOwnerLeaseExpireTimeMs()) {
        return RpcUtils.getStatus(
            TSStatusCode.SUBSCRIPTION_OWNER_LEASE_EXPIRED,
            String.format(
                "Subscription: owner lease for topic %s has expired, owner-id: %s, owner-epoch: %s.",
                topicName, topicMeta.getOwnerId(), topicMeta.getOwnerEpoch()));
      }

      if (!topicMeta.matchesOwner(requestOwnerId, requestOwnerEpoch)) {
        return RpcUtils.getStatus(
            TSStatusCode.SUBSCRIPTION_OWNER_FENCED,
            String.format(
                "Subscription: consumer owner is fenced for topic %s, request owner-id: %s,"
                    + " request owner-epoch: %s, current owner-id: %s, current owner-epoch: %s.",
                topicName,
                requestOwnerId,
                requestOwnerEpoch,
                topicMeta.getOwnerId(),
                topicMeta.getOwnerEpoch()));
      }

      return RpcUtils.SUCCESS_STATUS;
    } finally {
      releaseReadLock();
    }
  }

  public TSStatus checkTopicOwners(
      final ConsumerConfig consumerConfig, final Iterable<String> topicNames) {
    for (final String topicName : topicNames) {
      final TSStatus status = checkTopicOwner(consumerConfig, topicName);
      if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        return status;
      }
    }
    return RpcUtils.SUCCESS_STATUS;
  }

  /**
   * Apply owner lease renewals pushed by ConfigNode via the dedicated subscription owner heartbeat.
   * The pushed remaining duration is converted to a DataNode-local expire time on the local clock,
   * so no absolute timestamp is compared across nodes. Owner identity/epoch changes are delivered
   * via the topic-meta push path; here we only refresh the lease for the matching current owner.
   */
  public void handleTopicOwnerLeases(final List<TTopicOwnerLeaseEntry> ownerLeases) {
    if (Objects.isNull(ownerLeases) || ownerLeases.isEmpty()) {
      return;
    }
    acquireWriteLock();
    try {
      for (final TTopicOwnerLeaseEntry lease : ownerLeases) {
        final TopicMeta topicMeta = topicMetaKeeper.getTopicMeta(lease.getTopicName());
        if (Objects.isNull(topicMeta)) {
          continue;
        }
        topicMeta.applyOwnerLeaseFromHeartbeat(
            lease.getOwnerId(), lease.getOwnerEpoch(), lease.getLeaseRemainingMs());
      }
    } finally {
      releaseWriteLock();
    }
  }
}
