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

package org.apache.iotdb.db.subscription.agent.topic;

import org.apache.iotdb.commons.subscription.meta.topic.TopicMeta;
import org.apache.iotdb.commons.subscription.meta.topic.TopicMetaKeeper;
import org.apache.iotdb.mpp.rpc.thrift.TPushTopicRespExceptionMessage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TopicAgent {

  private static final Logger LOGGER = LoggerFactory.getLogger(TopicAgent.class);

  private final TopicMetaKeeper topicMetaKeeper;

  public TopicAgent() {
    topicMetaKeeper = new TopicMetaKeeper();
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

  public TPushTopicRespExceptionMessage handleSingleTopicMetaChanges(
      TopicMeta topicMetaFromCoordinator) {
    acquireWriteLock();
    try {
      handleSingleTopicMetaChangesInternal(topicMetaFromCoordinator);
      return null;
    } catch (Exception e) {
      final String topicName = topicMetaFromCoordinator.getTopicName();
      final String errorMessage =
          String.format(
              "Failed to handle single topic meta changes for %s, because %s",
              topicName, e.getMessage());
      LOGGER.warn("Failed to handle single topic meta changes for {}", topicName, e);
      return new TPushTopicRespExceptionMessage(
          topicName, errorMessage, System.currentTimeMillis());
    } finally {
      releaseWriteLock();
    }
  }

  private void handleSingleTopicMetaChangesInternal(final TopicMeta metaFromCoordinator) {
    final String topicName = metaFromCoordinator.getTopicName();
    topicMetaKeeper.removeTopicMeta(topicName);
    topicMetaKeeper.addTopicMeta(topicName, metaFromCoordinator);
  }

  public TPushTopicRespExceptionMessage handleDropTopic(String topicName) {
    acquireWriteLock();
    try {
      handleDropTopicInternal(topicName);
      return null;
    } catch (Exception e) {
      final String errorMessage =
          String.format("Failed to drop topic %s, because %s", topicName, e.getMessage());
      LOGGER.warn("Failed to drop topic {}", topicName, e);
      return new TPushTopicRespExceptionMessage(
          topicName, errorMessage, System.currentTimeMillis());
    } finally {
      releaseWriteLock();
    }
  }

  private void handleDropTopicInternal(String topicName) {
    topicMetaKeeper.removeTopicMeta(topicName);
  }
}
