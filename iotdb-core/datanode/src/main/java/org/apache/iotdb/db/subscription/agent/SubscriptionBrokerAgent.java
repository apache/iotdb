/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at      http://www.apache.org/licenses/LICENSE-2.0  Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the License for the specific language governing permissions and limitations under the License.
 */

package org.apache.iotdb.db.subscription.agent;

import org.apache.iotdb.db.subscription.broker.SubscriptionBroker;
import org.apache.iotdb.db.subscription.task.subtask.SubscriptionConnectorSubtask;
import org.apache.iotdb.rpc.subscription.payload.config.ConsumerConfig;
import org.apache.iotdb.rpc.subscription.payload.response.EnrichedTablets;
import org.apache.iotdb.tsfile.utils.Pair;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class SubscriptionBrokerAgent {

  private static final Logger LOGGER = LoggerFactory.getLogger(SubscriptionBrokerAgent.class);

  private final Map<String, SubscriptionBroker> consumerGroupIdToSubscriptionBroker =
      new ConcurrentHashMap<>();

  public void createSubscriptionBroker(String consumerGroupId) {
    consumerGroupIdToSubscriptionBroker.put(
        consumerGroupId, new SubscriptionBroker(consumerGroupId));
  }

  //////////////////////////// provided for subscription agent ////////////////////////////

  public List<Pair<ByteBuffer, EnrichedTablets>> poll(
      ConsumerConfig consumerConfig, Set<String> topicNames) {
    String consumerGroupId = consumerConfig.getConsumerGroupId();
    SubscriptionBroker broker = consumerGroupIdToSubscriptionBroker.get(consumerGroupId);
    if (Objects.isNull(broker)) {
      LOGGER.warn("Subscription: consumer group [{}] does not exist", consumerGroupId);
      return Collections.emptyList();
    }
    // TODO: currently we fetch messages from all topics
    return broker.poll(topicNames);
  }

  public void commit(
      ConsumerConfig consumerConfig, Map<String, List<String>> topicNameToSubscriptionCommitIds) {
    String consumerGroupId = consumerConfig.getConsumerGroupId();
    SubscriptionBroker broker = consumerGroupIdToSubscriptionBroker.get(consumerGroupId);
    if (Objects.isNull(broker)) {
      LOGGER.warn("Subscription: consumer group [{}] does not exist", consumerGroupId);
      return;
    }
    broker.commit(topicNameToSubscriptionCommitIds);
  }

  /////////////////////////////// prefetching queue ///////////////////////////////

  public void bindPrefetchingQueue(SubscriptionConnectorSubtask subtask) {
    String consumerGroupId = subtask.getConsumerGroupId();
    SubscriptionBroker broker = consumerGroupIdToSubscriptionBroker.get(consumerGroupId);
    if (Objects.isNull(broker)) {
      LOGGER.warn("Subscription: consumer group [{}] does not exist", consumerGroupId);
      return;
    }
    broker.bindPrefetchingQueue(subtask.getTopicName(), subtask.getInputPendingQueue());
  }

  public void unbindPrefetchingQueue(SubscriptionConnectorSubtask subtask) {
    String consumerGroupId = subtask.getConsumerGroupId();
    SubscriptionBroker broker = consumerGroupIdToSubscriptionBroker.get(consumerGroupId);
    if (Objects.isNull(broker)) {
      LOGGER.warn("Subscription: consumer group [{}] does not exist", consumerGroupId);
      return;
    }
    broker.unbindPrefetchingQueue(subtask.getTopicName());
  }

  public void executePrefetch(SubscriptionConnectorSubtask subtask) {
    String consumerGroupId = subtask.getConsumerGroupId();
    SubscriptionBroker broker = consumerGroupIdToSubscriptionBroker.get(consumerGroupId);
    if (Objects.isNull(broker)) {
      LOGGER.warn("Subscription: consumer group [{}] does not exist", consumerGroupId);
      return;
    }
    broker.executePrefetch(subtask.getTopicName());
  }
}
