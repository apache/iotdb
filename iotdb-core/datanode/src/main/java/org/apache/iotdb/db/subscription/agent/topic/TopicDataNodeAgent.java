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

import org.apache.iotdb.commons.subscription.config.TopicConfig;
import org.apache.iotdb.commons.subscription.config.TopicConfigValidator;
import org.apache.iotdb.commons.subscription.meta.TopicMeta;
import org.apache.iotdb.commons.subscription.meta.TopicMetaKeeper;
import org.apache.iotdb.mpp.rpc.thrift.TPushTopicRespExceptionMessage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Map;

import static org.apache.iotdb.commons.pipe.datastructure.options.PipeInclusionOptions.optionsAreAllLegal;
import static org.apache.iotdb.commons.subscription.config.TopicConstant.END_TIME_KEY;
import static org.apache.iotdb.commons.subscription.config.TopicConstant.FORMAT_DEFAULT_VALUE;
import static org.apache.iotdb.commons.subscription.config.TopicConstant.FORMAT_KEY;
import static org.apache.iotdb.commons.subscription.config.TopicConstant.PATH_DEFAULT_VALUE;
import static org.apache.iotdb.commons.subscription.config.TopicConstant.PATH_FORMAT_DEFAULT_VALUE;
import static org.apache.iotdb.commons.subscription.config.TopicConstant.PATH_FORMAT_KEY;
import static org.apache.iotdb.commons.subscription.config.TopicConstant.PATH_KEY;
import static org.apache.iotdb.commons.subscription.config.TopicConstant.START_TIME_KEY;

public class TopicDataNodeAgent {

  private static final Logger LOGGER = LoggerFactory.getLogger(TopicDataNodeAgent.class);

  private final TopicMetaKeeper topicMetaKeeper;

  public TopicDataNodeAgent() {
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
      return handleSingleTopicMetaChangesInternal(topicMetaFromCoordinator);
    } finally {
      releaseWriteLock();
    }
  }

  protected TPushTopicRespExceptionMessage handleSingleTopicMetaChangesInternal(
      TopicMeta topicMetaFromCoordinator) {
    // TODO: check if node is removing or removed

    try {
      executeSingleTopicMetaChanges(topicMetaFromCoordinator);
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
    }
  }

  private void executeSingleTopicMetaChanges(final TopicMeta metaFromCoordinator) {
    final String topicName = metaFromCoordinator.getTopicName();
    final TopicMeta metaInAgent = topicMetaKeeper.getTopicMeta(topicName);

    if (metaInAgent == null) {
      createTopic(metaFromCoordinator);
    }
  }

  private boolean createTopic(TopicMeta topicMeta) {
    final String topicName = topicMeta.getTopicName();
    final long creationTime = topicMeta.getCreationTime();

    final TopicMeta existedTopicMeta = topicMetaKeeper.getTopicMeta(topicName);
    if (existedTopicMeta != null) {
      if (!checkBeforeCreatingTopic(existedTopicMeta, topicName, creationTime)) {
        return false;
      }

      // Drop the topic if
      // 1. the topic with the same name but with different creation time has been created before
      // 2. the topic with the same name has been dropped before, but topic meta has not been
      // cleaned up
      dropTopic(topicName, creationTime);
    }

    topicMetaKeeper.addTopicMeta(topicName, topicMeta);
    return true;
  }

  public TPushTopicRespExceptionMessage handleDropTopic(String topicName) {
    acquireWriteLock();
    try {
      return handleDropTopicInternal(topicName);
    } finally {
      releaseWriteLock();
    }
  }

  protected TPushTopicRespExceptionMessage handleDropTopicInternal(String topicName) {
    // TODO: check if node is removing or removed

    try {
      dropTopic(topicName);
      return null;
    } catch (Exception e) {
      final String errorMessage =
          String.format("Failed to drop topic %s, because %s", topicName, e.getMessage());
      LOGGER.warn("Failed to drop topic {}", topicName, e);
      return new TPushTopicRespExceptionMessage(
          topicName, errorMessage, System.currentTimeMillis());
    }
  }

  private void dropTopic(String topicName, long creationTime) {
    final TopicMeta existedTopicMeta = topicMetaKeeper.getTopicMeta(topicName);

    if (!checkBeforeDroppingTopic(existedTopicMeta, topicName, creationTime)) {
      return;
    }

    topicMetaKeeper.removeTopicMeta(topicName);
  }

  private void dropTopic(String topicName) {
    final TopicMeta existedTopicMeta = topicMetaKeeper.getTopicMeta(topicName);

    if (!checkBeforeDroppingTopic(existedTopicMeta, topicName)) {
      return;
    }

    topicMetaKeeper.removeTopicMeta(topicName);
  }

  ////////////////////////// Checker //////////////////////////

  protected boolean checkBeforeCreatingTopic(
      TopicMeta existedTopicMeta, String topicName, long creationTime) {
    if (existedTopicMeta.getCreationTime() == creationTime) {
      LOGGER.info("Topic {} has already been created. Skip creating.", topicName);
      return false;
    }

    return true;
  }

  protected boolean checkBeforeDroppingTopic(
      TopicMeta existedTopicMeta, String topicName, long creationTime) {
    if (existedTopicMeta == null) {
      LOGGER.info(
          "Topic {} has already been dropped or has not been created. Skip dropping.", topicName);
      return false;
    }

    if (existedTopicMeta.getCreationTime() != creationTime) {
      LOGGER.info(
          "Topic {} (creation time = {}) has been created but does not match "
              + "the creation time ({}) in dropTopic request. Skip dropping.",
          topicName,
          existedTopicMeta.getCreationTime(),
          creationTime);
      return false;
    }

    return true;
  }

  protected boolean checkBeforeDroppingTopic(TopicMeta existedTopicMeta, String topicName) {
    if (existedTopicMeta == null) {
      LOGGER.info(
          "Topic {} has already been dropped or has not been created. Skip dropping.", topicName);
      return false;
    }

    return true;
  }

  public void validate(Map<String, String> topicAttributes) {
    final TopicConfig topicConfig = new TopicConfig(topicAttributes);
    try {
      TopicConfigValidator validator = new TopicConfigValidator(topicConfig);
      validator
          .validate(
              args -> optionsAreAllLegal((String) args),
              "The 'path' string contains illegal path.",
              validator
                  .getParameters()
                  .getStringOrDefault(Collections.singletonList(PATH_KEY), PATH_DEFAULT_VALUE))
          .validate(
              args -> optionsAreAllLegal((String) args),
              "The 'path.format' string contains illegal path.",
              validator
                  .getParameters()
                  .getStringOrDefault(
                      Collections.singletonList(PATH_FORMAT_KEY), PATH_FORMAT_DEFAULT_VALUE))
          .validate(
              args -> optionsAreAllLegal((String) args),
              "The 'start-time' string contains illegal path.",
              validator
                  .getParameters()
                  .getLongOrDefault(Collections.singletonList(START_TIME_KEY), Long.MIN_VALUE))
          .validate(
              args -> optionsAreAllLegal((String) args),
              "The 'end-time' string contains illegal path.",
              validator
                  .getParameters()
                  .getLongOrDefault(Collections.singletonList(END_TIME_KEY), Long.MIN_VALUE))
          .validate(
              args -> optionsAreAllLegal((String) args),
              "The 'format' string contains illegal path.",
              validator
                  .getParameters()
                  .getStringOrDefault(Collections.singletonList(FORMAT_KEY), FORMAT_DEFAULT_VALUE));
    } catch (Exception e) {
      LOGGER.warn("Failed to validate topic attributes", e);
    }
  }
}
