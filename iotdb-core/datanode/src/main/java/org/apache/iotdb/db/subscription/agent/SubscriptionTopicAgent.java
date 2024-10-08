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

import org.apache.iotdb.commons.subscription.meta.topic.TopicMeta;
import org.apache.iotdb.commons.subscription.meta.topic.TopicMetaKeeper;
import org.apache.iotdb.mpp.rpc.thrift.TPushTopicMetaRespExceptionMessage;
import org.apache.iotdb.rpc.subscription.config.TopicConfig;
import org.apache.iotdb.rpc.subscription.config.TopicConstant;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
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
    topicMetaKeeper.removeTopicMeta(topicName);
    topicMetaKeeper.addTopicMeta(topicName, metaFromCoordinator);
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
      LOGGER.warn("Exception occurred when dropping topic {}", topicName, e);
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
      return topicMetaKeeper
          .getTopicMeta(topicName)
          .getConfig()
          .getStringOrDefault(TopicConstant.FORMAT_KEY, TopicConstant.FORMAT_DEFAULT_VALUE);
    } finally {
      releaseReadLock();
    }
  }

  public String getTopicMode(final String topicName) {
    acquireReadLock();
    try {
      return topicMetaKeeper
          .getTopicMeta(topicName)
          .getConfig()
          .getStringOrDefault(TopicConstant.MODE_KEY, TopicConstant.MODE_DEFAULT_VALUE);
    } finally {
      releaseReadLock();
    }
  }

  public Map<String, TopicConfig> getTopicConfigs(final Set<String> topicNames) {
    acquireReadLock();
    try {
      return topicNames.stream()
          .collect(
              Collectors.toMap(
                  topicName -> topicName,
                  topicName -> topicMetaKeeper.getTopicMeta(topicName).getConfig()));
    } finally {
      releaseReadLock();
    }
  }
}
