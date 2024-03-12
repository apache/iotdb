/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at      http://www.apache.org/licenses/LICENSE-2.0  Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the License for the specific language governing permissions and limitations under the License.
 */

package org.apache.iotdb.db.subscription.agent;

import org.apache.iotdb.db.subscription.meta.ConsumerGroupMeta;
import org.apache.iotdb.rpc.subscription.payload.request.ConsumerConfig;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class SubscriptionConsumerAgent {

  private static final Logger LOGGER = LoggerFactory.getLogger(SubscriptionConsumerAgent.class);

  // TODO: sync from node-commons
  private final Map<String, ConsumerGroupMeta> consumerGroupIDToConsumerGroupMeta =
      new ConcurrentHashMap<>();

  //////////////////////////// provided for subscription agent ////////////////////////////

  public void createConsumer(ConsumerConfig consumerConfig) {
    String consumerGroupID = consumerConfig.getConsumerGroupID();
    ConsumerGroupMeta consumerGroupMeta = consumerGroupIDToConsumerGroupMeta.get(consumerGroupID);
    if (Objects.isNull(consumerGroupMeta)) {
      // new consumer group
      consumerGroupIDToConsumerGroupMeta.put(
          consumerGroupID, new ConsumerGroupMeta(consumerConfig));
      // create broker
      SubscriptionAgent.broker().createSubscriptionBroker(consumerGroupID);
    } else {
      consumerGroupMeta.addConsumer(consumerConfig);
    }
    // TODO: call CN rpc
  }

  public void dropConsumer(ConsumerConfig consumerConfig) {
    String consumerGroupID = consumerConfig.getConsumerGroupID();
    String consumerClientID = consumerConfig.getConsumerClientID();
    ConsumerGroupMeta consumerGroupMeta = consumerGroupIDToConsumerGroupMeta.get(consumerGroupID);
    if (Objects.isNull(consumerGroupMeta)) {
      LOGGER.warn("Subscription: consumer group [{}] does not exist", consumerGroupID);
      return;
    }

    consumerGroupMeta.removeConsumer(consumerClientID);
    if (consumerGroupMeta.isEmpty()) {
      consumerGroupIDToConsumerGroupMeta.remove(consumerGroupID);
      // TODO: broker TTL
    }
    // TODO: call CN rpc
  }

  public void subscribe(ConsumerConfig consumerConfig, List<String> topicNames) {
    String consumerGroupID = consumerConfig.getConsumerGroupID();
    String consumerClientID = consumerConfig.getConsumerClientID();
    ConsumerGroupMeta consumerGroupMeta = consumerGroupIDToConsumerGroupMeta.get(consumerGroupID);
    if (Objects.isNull(consumerGroupMeta)) {
      LOGGER.warn("Subscription: consumer group [{}] does not exist", consumerGroupID);
      return;
    }

    for (String topicName : topicNames) {
      if (!SubscriptionAgent.topic().isTopicExisted(topicName)) {
        LOGGER.warn("Subscription: topic [{}] does not exist", topicName);
      } else {
        if (consumerGroupMeta.subscribe(consumerClientID, topicName)) {
          SubscriptionAgent.topic().addSubscribedConsumerGroupID(topicName, consumerGroupID);
        }
        // TODO: call CN rpc
      }
    }
  }

  public void unsubscribe(ConsumerConfig consumerConfig, List<String> topicNames) {
    String consumerGroupID = consumerConfig.getConsumerGroupID();
    String consumerClientID = consumerConfig.getConsumerClientID();
    ConsumerGroupMeta consumerGroupMeta = consumerGroupIDToConsumerGroupMeta.get(consumerGroupID);
    if (Objects.isNull(consumerGroupMeta)) {
      LOGGER.warn("Subscription: consumer group [{}] does not exist", consumerGroupID);
      return;
    }

    for (String topicName : topicNames) {
      if (!SubscriptionAgent.topic().isTopicExisted(topicName)) {
        LOGGER.warn("Subscription: topic [{}] does not exist", topicName);
      } else {
        consumerGroupMeta.unsubscribe(consumerClientID, topicName);
        // TODO: call CN rpc
      }
    }
  }

  public Set<String> subscribedTopic(ConsumerConfig consumerConfig) {
    String consumerGroupID = consumerConfig.getConsumerGroupID();
    String consumerClientID = consumerConfig.getConsumerClientID();
    ConsumerGroupMeta consumerGroupMeta = consumerGroupIDToConsumerGroupMeta.get(consumerGroupID);
    if (Objects.isNull(consumerGroupMeta)) {
      LOGGER.warn("Subscription: consumer group [{}] does not exist", consumerGroupID);
      return Collections.emptySet();
    }
    return consumerGroupMeta.subscribedTopics(consumerClientID);
  }

  public boolean isConsumerExisted(ConsumerConfig consumerConfig) {
    String consumerGroupID = consumerConfig.getConsumerGroupID();
    String consumerClientID = consumerConfig.getConsumerClientID();
    ConsumerGroupMeta consumerGroupMeta = consumerGroupIDToConsumerGroupMeta.get(consumerGroupID);
    if (Objects.isNull(consumerGroupMeta)) {
      return false;
    }
    return consumerGroupMeta.isConsumerExisted(consumerClientID);
  }
}
