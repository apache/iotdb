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

import org.apache.iotdb.db.subscription.meta.TopicMeta;
import org.apache.iotdb.rpc.subscription.payload.request.TopicConfig;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

public class SubscriptionTopicAgent {

  private static final Logger LOGGER = LoggerFactory.getLogger(SubscriptionTopicAgent.class);

  // TODO: sync from node-commons
  // TODO: REMOVE INITIAL VALUE BEFORE LINKING WITH CN
  private final Map<String, TopicMeta> topicNameToTopicMeta =
      new ConcurrentHashMap<>(
          Collections.singletonMap("topic1", new TopicMeta(new TopicConfig("topic1", "root.**"))));

  //////////////////////////// provided for subscription agent ////////////////////////////

  public void createTopic(TopicConfig topicConfig) {
    String topicName = topicConfig.getTopicName();
    if (isTopicExisted(topicName)) {
      LOGGER.warn("Subscription: topic [{}] has already existed", topicName);
      return;
    }
    topicNameToTopicMeta.put(topicName, new TopicMeta(topicConfig));
    // TODO: call CN rpc
  }

  public void dropTopic(String topicName) {
    if (!topicNameToTopicMeta.containsKey(topicName)) {
      LOGGER.warn("Subscription: topic [{}] does not exist", topicName);
      return;
    }
    TopicMeta topicMeta = topicNameToTopicMeta.get(topicName);
    if (topicMeta.hasSubscribedConsumerGroup()) {
      LOGGER.warn(
          "Subscription: drop topic [{}] failed, some consumer groups have subscribed the topic",
          topicName);
      return;
    }
    topicNameToTopicMeta.remove(topicName);
    // TODO: call CN rpc
  }

  public void addSubscribedConsumerGroupID(String topicName, String consumerGroupID) {
    TopicMeta topicMeta = topicNameToTopicMeta.get(topicName);
    if (Objects.isNull(topicMeta)) {
      LOGGER.warn("Subscription: topic [{}] does not exist", topicName);
      return;
    }
    topicMeta.addSubscribedConsumerGroupID(consumerGroupID);
  }

  public boolean isTopicExisted(String topicName) {
    return topicNameToTopicMeta.containsKey(topicName);
  }
}
