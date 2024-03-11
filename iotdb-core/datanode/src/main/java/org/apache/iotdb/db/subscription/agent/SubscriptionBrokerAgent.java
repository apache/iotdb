/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at      http://www.apache.org/licenses/LICENSE-2.0  Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the License for the specific language governing permissions and limitations under the License.
 */

package org.apache.iotdb.db.subscription.agent;

import org.apache.iotdb.db.subscription.broker.SubscriptionBroker;
import org.apache.iotdb.db.subscription.task.subtask.SubscriptionConnectorSubtask;
import org.apache.iotdb.rpc.subscription.payload.request.ConsumerConfig;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

public class SubscriptionBrokerAgent {

  private static final Logger LOGGER = LoggerFactory.getLogger(SubscriptionBrokerAgent.class);

  private final Map<String, SubscriptionBroker> consumerGroupIDToSubscriptionBroker =
      new ConcurrentHashMap<>();

  public void createSubscriptionBroker(String consumerGroupID) {
    consumerGroupIDToSubscriptionBroker.put(
        consumerGroupID, new SubscriptionBroker(consumerGroupID));
  }

  //////////////////////////// provided for subscription agent ////////////////////////////

  public List<ByteBuffer> poll(ConsumerConfig consumerConfig) {
    String consumerGroupID = consumerConfig.getConsumerGroupID();
    SubscriptionBroker broker = consumerGroupIDToSubscriptionBroker.get(consumerGroupID);
    if (Objects.isNull(broker)) {
      LOGGER.warn("Subscription: consumer group [{}] does not exist", consumerGroupID);
      return Collections.emptyList();
    }
    // poll all subscribed topics
    return broker.poll(SubscriptionAgent.consumer().subscribedTopic(consumerConfig));
  }

  public void commit(
      ConsumerConfig consumerConfig, Map<String, List<String>> topicNameToSubscriptionCommitIds) {
    String consumerGroupID = consumerConfig.getConsumerGroupID();
    SubscriptionBroker broker =
        consumerGroupIDToSubscriptionBroker.get(consumerConfig.getConsumerGroupID());
    if (Objects.isNull(broker)) {
      LOGGER.warn("Subscription: consumer group [{}] does not exist", consumerGroupID);
      return;
    }
    broker.commit(topicNameToSubscriptionCommitIds);
  }

  /////////////////////////////// prefetching queue ///////////////////////////////

  public void bindPrefetchingQueue(SubscriptionConnectorSubtask subtask) {
    String consumerGroupID = subtask.getConsumerGroupID();
    SubscriptionBroker broker = consumerGroupIDToSubscriptionBroker.get(consumerGroupID);
    if (Objects.isNull(broker)) {
      LOGGER.warn("Subscription: consumer group [{}] does not exist", consumerGroupID);
      return;
    }
    broker.bindPrefetchingQueue(subtask.getTopicName(), subtask.getInputPendingQueue());
  }

  public void unbindPrefetchingQueue(SubscriptionConnectorSubtask subtask) {
    String consumerGroupID = subtask.getConsumerGroupID();
    SubscriptionBroker broker = consumerGroupIDToSubscriptionBroker.get(consumerGroupID);
    if (Objects.isNull(broker)) {
      LOGGER.warn("Subscription: consumer group [{}] does not exist", consumerGroupID);
      return;
    }
    broker.unbindPrefetchingQueue(subtask.getTopicName());
  }

  public void executePrefetch(SubscriptionConnectorSubtask subtask) {
    String consumerGroupID = subtask.getConsumerGroupID();
    SubscriptionBroker broker = consumerGroupIDToSubscriptionBroker.get(consumerGroupID);
    if (Objects.isNull(broker)) {
      LOGGER.warn("Subscription: consumer group [{}] does not exist", consumerGroupID);
      return;
    }
    broker.executePrefetch(subtask.getTopicName());
  }
}
