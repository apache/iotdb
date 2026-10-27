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

import org.apache.iotdb.commons.pipe.agent.task.connection.UnboundedBlockingPendingQueue;
import org.apache.iotdb.commons.subscription.meta.consumer.ConsumerGroupMeta;
import org.apache.iotdb.commons.subscription.meta.consumer.ConsumerMeta;
import org.apache.iotdb.commons.subscription.meta.topic.TopicMeta;
import org.apache.iotdb.consensus.IConsensus;
import org.apache.iotdb.consensus.iot.IoTConsensus;
import org.apache.iotdb.db.consensus.DataRegionConsensusImpl;
import org.apache.iotdb.db.pipe.metric.source.PipeDataRegionEventCounter;
import org.apache.iotdb.db.subscription.broker.ISubscriptionBroker;
import org.apache.iotdb.db.subscription.broker.SubscriptionBroker;
import org.apache.iotdb.db.subscription.task.subtask.SubscriptionSinkSubtask;
import org.apache.iotdb.pipe.api.event.Event;
import org.apache.iotdb.rpc.subscription.config.ConsumerConfig;
import org.apache.iotdb.rpc.subscription.config.ConsumerConstant;
import org.apache.iotdb.rpc.subscription.config.TopicConstant;

import org.junit.Test;

import java.lang.reflect.Field;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class SubscriptionConsumerAgentBrokerRecoveryTest {

  private static final long TEST_TIMEOUT_SECONDS = 5L;

  @Test
  public void testNonEmptyMetadataRecreatesMissingBroker() {
    final String consumerGroupId = unique("group");
    final String consumerId = unique("consumer");
    final SubscriptionConsumerAgent agent = SubscriptionAgent.consumer();
    try {
      final ConsumerGroupMeta meta = createMeta(consumerGroupId, consumerId);
      assertNull(agent.handleSingleConsumerGroupMetaChanges(meta));
      assertTrue(SubscriptionAgent.broker().isBrokerExist(consumerGroupId));

      assertTrue(SubscriptionAgent.broker().dropBroker(consumerGroupId));
      assertFalse(SubscriptionAgent.broker().isBrokerExist(consumerGroupId));

      agent.ensureConsumerGroupBroker(consumerGroupId);

      assertTrue(SubscriptionAgent.broker().isBrokerExist(consumerGroupId));
      assertTrue(agent.isConsumerExisted(consumerGroupId, consumerId));
    } finally {
      agent.handleDropConsumerGroup(consumerGroupId);
    }
  }

  @Test
  public void testMetadataPushSetsUpNewConsensusTopicOnce() {
    final String consumerGroupId = unique("group");
    final String consumerId = unique("consumer");
    final String topicName = unique("topic");
    final SubscriptionConsumerAgent agent = SubscriptionAgent.consumer();
    final IConsensus originalConsensus = DataRegionConsensusImpl.getInstance();
    final IoTConsensus consensus = mock(IoTConsensus.class);
    when(consensus.getAllConsensusGroupIds()).thenReturn(Collections.emptyList());
    DataRegionConsensusImpl.setInstance(consensus);
    try {
      assertNull(
          SubscriptionAgent.topic()
              .handleSingleTopicMetaChanges(
                  new TopicMeta(
                      topicName,
                      1L,
                      Collections.singletonMap(
                          TopicConstant.MODE_KEY, TopicConstant.MODE_INCREMENTAL_VALUE))));
      final ConsumerGroupMeta meta = createMeta(consumerGroupId, consumerId);
      assertNull(agent.handleSingleConsumerGroupMetaChanges(meta));
      clearInvocations(consensus);

      final ConsumerGroupMeta subscribedMeta = meta.deepCopy();
      subscribedMeta.addSubscription(consumerId, Collections.singleton(topicName));
      assertNull(agent.handleSingleConsumerGroupMetaChanges(subscribedMeta));

      // One discovery for setup logging and one for binding the topic's regions.
      verify(consensus, times(2)).getAllConsensusGroupIds();
      assertEquals(
          Collections.singleton(topicName),
          agent.getTopicNamesSubscribedByConsumer(consumerGroupId, consumerId));
    } finally {
      agent.handleDropConsumerGroup(consumerGroupId);
      SubscriptionAgent.topic().handleDropTopic(topicName, false);
      DataRegionConsensusImpl.setInstance(originalConsensus);
    }
  }

  @Test
  public void testEmptyMetadataDoesNotRecreateBroker() {
    final String consumerGroupId = unique("group");
    final String consumerId = unique("consumer");
    final SubscriptionConsumerAgent agent = SubscriptionAgent.consumer();
    try {
      final ConsumerGroupMeta meta = createMeta(consumerGroupId, consumerId);
      assertNull(agent.handleSingleConsumerGroupMetaChanges(meta));
      assertTrue(SubscriptionAgent.broker().isBrokerExist(consumerGroupId));

      final ConsumerGroupMeta emptyMeta = meta.deepCopy();
      emptyMeta.removeConsumer(consumerId);
      assertTrue(emptyMeta.isEmpty());
      assertNull(agent.handleSingleConsumerGroupMetaChanges(emptyMeta));

      assertFalse(SubscriptionAgent.broker().isBrokerExist(consumerGroupId));
      agent.ensureConsumerGroupBroker(consumerGroupId);
      assertFalse(SubscriptionAgent.broker().isBrokerExist(consumerGroupId));
    } finally {
      agent.handleDropConsumerGroup(consumerGroupId);
    }
  }

  @Test
  public void testConcurrentMetadataPushAndBrokerEnsureCreateOneBroker() throws Exception {
    final String consumerGroupId = unique("group");
    final String consumerId = unique("consumer");
    final SubscriptionConsumerAgent agent = SubscriptionAgent.consumer();
    final ExecutorService executor = Executors.newFixedThreadPool(2);
    try {
      final ConsumerGroupMeta meta = createMeta(consumerGroupId, consumerId);
      assertNull(agent.handleSingleConsumerGroupMetaChanges(meta));
      assertTrue(SubscriptionAgent.broker().dropBroker(consumerGroupId));

      final Future<?> metadataPush =
          executor.submit(() -> agent.handleSingleConsumerGroupMetaChanges(meta.deepCopy()));
      final Future<?> handshakeRepair =
          executor.submit(() -> agent.ensureConsumerGroupBroker(consumerGroupId));
      metadataPush.get(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);
      handshakeRepair.get(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);

      assertEquals(1, getBrokers(consumerGroupId).size());
    } finally {
      executor.shutdownNow();
      executor.awaitTermination(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);
      agent.handleDropConsumerGroup(consumerGroupId);
    }
  }

  @Test
  public void testBrokerAndExistingQueueSurviveProviderReplacementRepair() throws Exception {
    final String consumerGroupId = unique("group");
    final String consumerId = unique("consumer");
    final String topicName = unique("topic");
    final SubscriptionConsumerAgent agent = SubscriptionAgent.consumer();
    try {
      assertNull(
          SubscriptionAgent.topic()
              .handleSingleTopicMetaChanges(new TopicMeta(topicName, 1L, Collections.emptyMap())));
      final ConsumerGroupMeta meta = createMeta(consumerGroupId, consumerId);
      meta.addSubscription(consumerId, Collections.singleton(topicName));
      assertNull(agent.handleSingleConsumerGroupMetaChanges(meta));

      bindPipeQueue(consumerGroupId, topicName);
      final SubscriptionBroker brokerBeforeRepair = getPipeBroker(consumerGroupId);
      assertTrue(brokerBeforeRepair.hasQueue(topicName));

      // A provider replacement closes only its session. The server-side repair must preserve the
      // existing broker and queue when it reconciles the same non-empty metadata.
      agent.ensureConsumerGroupBroker(consumerGroupId);

      assertSame(brokerBeforeRepair, getPipeBroker(consumerGroupId));
      assertTrue(getPipeBroker(consumerGroupId).hasQueue(topicName));
    } finally {
      agent.handleDropConsumerGroup(consumerGroupId);
      SubscriptionAgent.topic().handleDropTopic(topicName, false);
    }
  }

  @Test
  public void testCanceledTopicIsNotRestoredWhenBrokerIsRecreated() throws Exception {
    final String consumerGroupId = unique("group");
    final String consumerId = unique("consumer");
    final String topicName = unique("topic");
    final SubscriptionConsumerAgent agent = SubscriptionAgent.consumer();
    try {
      assertNull(
          SubscriptionAgent.topic()
              .handleSingleTopicMetaChanges(new TopicMeta(topicName, 1L, Collections.emptyMap())));
      final ConsumerGroupMeta meta = createMeta(consumerGroupId, consumerId);
      meta.addSubscription(consumerId, Collections.singleton(topicName));
      assertNull(agent.handleSingleConsumerGroupMetaChanges(meta));

      bindPipeQueue(consumerGroupId, topicName);
      assertTrue(getPipeBroker(consumerGroupId).hasQueue(topicName));

      final ConsumerGroupMeta unsubscribedMeta = meta.deepCopy();
      unsubscribedMeta.removeSubscription(consumerId, Collections.singleton(topicName));
      assertNull(agent.handleSingleConsumerGroupMetaChanges(unsubscribedMeta));
      assertFalse(getPipeBroker(consumerGroupId).hasQueue(topicName));

      assertTrue(SubscriptionAgent.broker().dropBroker(consumerGroupId));
      agent.ensureConsumerGroupBroker(consumerGroupId);

      assertFalse(getPipeBroker(consumerGroupId).hasQueue(topicName));
      assertFalse(agent.getAllSubscriptions().containsKey(consumerGroupId));
    } finally {
      agent.handleDropConsumerGroup(consumerGroupId);
      SubscriptionAgent.topic().handleDropTopic(topicName, false);
    }
  }

  private static ConsumerGroupMeta createMeta(
      final String consumerGroupId, final String consumerId) {
    final Map<String, String> attributes = new HashMap<>();
    attributes.put(ConsumerConstant.CONSUMER_ID_KEY, consumerId);
    attributes.put(ConsumerConstant.CONSUMER_GROUP_ID_KEY, consumerGroupId);
    final ConsumerConfig consumerConfig = new ConsumerConfig(attributes);
    return new ConsumerGroupMeta(
        consumerGroupId, 1L, new ConsumerMeta(consumerId, 1L, consumerConfig.getAttribute()));
  }

  private static void bindPipeQueue(final String consumerGroupId, final String topicName) {
    final UnboundedBlockingPendingQueue<Event> inputQueue =
        new UnboundedBlockingPendingQueue<>(new PipeDataRegionEventCounter());
    final SubscriptionSinkSubtask subtask =
        new SubscriptionSinkSubtask(
            unique("task"), 1L, "", 0, inputQueue, null, topicName, consumerGroupId);
    SubscriptionAgent.broker().bindPrefetchingQueue(subtask);
  }

  @SuppressWarnings("unchecked")
  private static List<ISubscriptionBroker> getBrokers(final String consumerGroupId)
      throws Exception {
    final Field field = SubscriptionBrokerAgent.class.getDeclaredField("consumerGroupIdToBrokers");
    field.setAccessible(true);
    final Map<String, List<ISubscriptionBroker>> brokersByConsumerGroup =
        (Map<String, List<ISubscriptionBroker>>) field.get(SubscriptionAgent.broker());
    return brokersByConsumerGroup.getOrDefault(consumerGroupId, Collections.emptyList());
  }

  private static SubscriptionBroker getPipeBroker(final String consumerGroupId) throws Exception {
    return getBrokers(consumerGroupId).stream()
        .filter(SubscriptionBroker.class::isInstance)
        .map(SubscriptionBroker.class::cast)
        .findFirst()
        .orElseThrow(AssertionError::new);
  }

  private static String unique(final String prefix) {
    return prefix + '-' + UUID.randomUUID();
  }
}
