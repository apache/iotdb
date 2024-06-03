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

import org.apache.iotdb.db.subscription.broker.SubscriptionBroker;
import org.apache.iotdb.db.subscription.event.SubscriptionEvent;
import org.apache.iotdb.db.subscription.task.subtask.SubscriptionConnectorSubtask;
import org.apache.iotdb.rpc.subscription.config.ConsumerConfig;
import org.apache.iotdb.rpc.subscription.exception.SubscriptionException;
import org.apache.iotdb.rpc.subscription.payload.poll.SubscriptionCommitContext;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class SubscriptionBrokerAgent {

  private static final Logger LOGGER = LoggerFactory.getLogger(SubscriptionBrokerAgent.class);

  private final Map<String, SubscriptionBroker> consumerGroupIdToSubscriptionBroker =
      new ConcurrentHashMap<>();

  //////////////////////////// provided for subscription agent ////////////////////////////

  public List<SubscriptionEvent> poll(
      final ConsumerConfig consumerConfig, final Set<String> topicNames) {
    final String consumerGroupId = consumerConfig.getConsumerGroupId();
    final SubscriptionBroker broker = consumerGroupIdToSubscriptionBroker.get(consumerGroupId);
    if (Objects.isNull(broker)) {
      final String errorMessage =
          String.format(
              "Subscription: broker bound to consumer group [%s] does not exist", consumerGroupId);
      LOGGER.warn(errorMessage);
      throw new SubscriptionException(errorMessage);
    }
    // TODO: currently we fetch messages from all topics
    final String consumerId = consumerConfig.getConsumerId();
    return broker.poll(consumerId, topicNames);
  }

  public List<SubscriptionEvent> pollTsFile(
      final ConsumerConfig consumerConfig,
      final String topicName,
      final String fileName,
      final long writingOffset) {
    final String consumerGroupId = consumerConfig.getConsumerGroupId();
    final SubscriptionBroker broker = consumerGroupIdToSubscriptionBroker.get(consumerGroupId);
    if (Objects.isNull(broker)) {
      final String errorMessage =
          String.format(
              "Subscription: broker bound to consumer group [%s] does not exist", consumerGroupId);
      LOGGER.warn(errorMessage);
      throw new SubscriptionException(errorMessage);
    }
    final String consumerId = consumerConfig.getConsumerId();
    return broker.pollTsFile(consumerId, topicName, fileName, writingOffset);
  }

  /**
   * @return list of successful commit contexts
   */
  public List<SubscriptionCommitContext> commit(
      final ConsumerConfig consumerConfig,
      final List<SubscriptionCommitContext> commitContexts,
      final boolean nack) {
    final String consumerGroupId = consumerConfig.getConsumerGroupId();
    final SubscriptionBroker broker = consumerGroupIdToSubscriptionBroker.get(consumerGroupId);
    if (Objects.isNull(broker)) {
      final String errorMessage =
          String.format(
              "Subscription: broker bound to consumer group [%s] does not exist", consumerGroupId);
      LOGGER.warn(errorMessage);
      throw new SubscriptionException(errorMessage);
    }
    return broker.commit(commitContexts, nack);
  }

  /////////////////////////////// broker ///////////////////////////////

  public boolean isBrokerExist(final String consumerGroupId) {
    return consumerGroupIdToSubscriptionBroker.containsKey(consumerGroupId);
  }

  public synchronized void createBroker(final String consumerGroupId) {
    final SubscriptionBroker broker = new SubscriptionBroker(consumerGroupId);
    consumerGroupIdToSubscriptionBroker.put(consumerGroupId, broker);
  }

  /**
   * @return true -> if drop broker success
   */
  public synchronized boolean dropBroker(final String consumerGroupId) {
    final SubscriptionBroker broker = consumerGroupIdToSubscriptionBroker.get(consumerGroupId);
    if (Objects.isNull(broker)) {
      LOGGER.warn(
          "Subscription: broker bound to consumer group [{}] does not exist", consumerGroupId);
      // do nothing
      return true;
    }
    if (!broker.isEmpty()) {
      LOGGER.warn(
          "Subscription: broker bound to consumer group [{}] is not empty when dropping",
          consumerGroupId);
      return false;
    }
    consumerGroupIdToSubscriptionBroker.remove(consumerGroupId);
    return true;
  }

  /////////////////////////////// prefetching queue ///////////////////////////////

  public void bindPrefetchingQueue(final SubscriptionConnectorSubtask subtask) {
    final String consumerGroupId = subtask.getConsumerGroupId();
    final SubscriptionBroker broker = consumerGroupIdToSubscriptionBroker.get(consumerGroupId);
    if (Objects.isNull(broker)) {
      LOGGER.warn("Subscription: consumer group [{}] does not exist", consumerGroupId);
      return;
    }
    broker.bindPrefetchingQueue(subtask.getTopicName(), subtask.getInputPendingQueue());
  }

  public void unbindPrefetchingQueue(final SubscriptionConnectorSubtask subtask) {
    final String consumerGroupId = subtask.getConsumerGroupId();
    final SubscriptionBroker broker = consumerGroupIdToSubscriptionBroker.get(consumerGroupId);
    if (Objects.isNull(broker)) {
      LOGGER.warn("Subscription: consumer group [{}] does not exist", consumerGroupId);
      return;
    }
    broker.unbindPrefetchingQueue(subtask.getTopicName());
  }

  public void executePrefetch(final SubscriptionConnectorSubtask subtask) {
    final String consumerGroupId = subtask.getConsumerGroupId();
    final SubscriptionBroker broker = consumerGroupIdToSubscriptionBroker.get(consumerGroupId);
    if (Objects.isNull(broker)) {
      LOGGER.warn("Subscription: consumer group [{}] does not exist", consumerGroupId);
      return;
    }
    broker.executePrefetch(subtask.getTopicName());
  }
}
