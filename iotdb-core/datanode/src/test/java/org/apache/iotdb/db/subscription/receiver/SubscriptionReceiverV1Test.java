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

package org.apache.iotdb.db.subscription.receiver;

import org.apache.iotdb.commons.subscription.config.SubscriptionConfig;
import org.apache.iotdb.commons.subscription.meta.topic.TopicMeta;
import org.apache.iotdb.db.subscription.agent.SubscriptionAgent;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.rpc.subscription.config.ConsumerConfig;
import org.apache.iotdb.rpc.subscription.config.ConsumerConstant;
import org.apache.iotdb.rpc.subscription.config.TopicConstant;

import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

public class SubscriptionReceiverV1Test {

  @Test
  public void testHandleTimeoutKeepsRecentlyActiveConsumer() throws Exception {
    final SubscriptionReceiverV1 receiver = new SubscriptionReceiverV1();
    final ConsumerConfig consumerConfig = createConsumerConfig(1_000L);

    setField(receiver, "sharedConsumerConfig", consumerConfig);
    setField(receiver, "lastActivityTimeMs", System.currentTimeMillis() - 1_000L);

    receiver.handleTimeout();

    Assert.assertSame(consumerConfig, getField(receiver, "sharedConsumerConfig"));
    Assert.assertFalse((boolean) getField(receiver, "consumerInvalidated"));
  }

  @Test
  public void testHandleTimeoutSkipsConsumerWithInFlightRequests() throws Exception {
    final SubscriptionReceiverV1 receiver = new SubscriptionReceiverV1();
    final ConsumerConfig consumerConfig = createConsumerConfig(1_000L);

    setField(receiver, "sharedConsumerConfig", consumerConfig);
    setField(receiver, "lastActivityTimeMs", System.currentTimeMillis() - 15_000L);
    ((AtomicLong) getField(receiver, "inFlightRequestCount")).set(1L);

    receiver.handleTimeout();

    Assert.assertSame(consumerConfig, getField(receiver, "sharedConsumerConfig"));
    Assert.assertFalse((boolean) getField(receiver, "consumerInvalidated"));
  }

  @Test
  public void testCalculateConsumerInactivityTimeoutUsesDefaultTimeout() throws Exception {
    final SubscriptionReceiverV1 receiver = new SubscriptionReceiverV1();

    Assert.assertEquals(
        SubscriptionConfig.getInstance().getSubscriptionDefaultTimeoutInMs(),
        invokeCalculateConsumerInactivityTimeoutMs(receiver, createConsumerConfig(1_000L)));
  }

  @Test
  public void testCalculateConsumerInactivityTimeoutUsesHeartbeatMultiple() throws Exception {
    final SubscriptionReceiverV1 receiver = new SubscriptionReceiverV1();

    Assert.assertEquals(
        15_000L,
        invokeCalculateConsumerInactivityTimeoutMs(receiver, createConsumerConfig(5_000L)));
  }

  @Test
  public void testTopicOwnerFencingStatus() {
    final String topicName = "topic-" + UUID.randomUUID();

    SubscriptionAgent.topic()
        .handleSingleTopicMetaChanges(createTopicMeta(topicName, "owner1", 7L));
    try {
      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(),
          SubscriptionAgent.topic()
              .checkTopicOwner(createConsumerConfig(1_000L, "owner1", 7L), topicName)
              .getCode());
      Assert.assertEquals(
          TSStatusCode.SUBSCRIPTION_OWNER_FENCED.getStatusCode(),
          SubscriptionAgent.topic()
              .checkTopicOwner(createConsumerConfig(1_000L, "owner2", 7L), topicName)
              .getCode());
      Assert.assertEquals(
          TSStatusCode.SUBSCRIPTION_OWNER_REQUIRED.getStatusCode(),
          SubscriptionAgent.topic()
              .checkTopicOwner(createConsumerConfig(1_000L), topicName)
              .getCode());
    } finally {
      SubscriptionAgent.topic().handleDropTopic(topicName);
    }
  }

  @Test
  public void testOldOwnerFencedAfterNetworkPartitionAndTopicOwnerTransfer() {
    final String topicName = "topic-" + UUID.randomUUID();
    final TopicMeta topicMeta = createTopicMeta(topicName, "owner1", 5L);
    final ConsumerConfig oldOwnerConsumer = createConsumerConfig(1_000L, "owner1", 5L);
    final ConsumerConfig newOwnerConsumer = createConsumerConfig(1_000L, "owner2", 6L);

    SubscriptionAgent.topic().handleSingleTopicMetaChanges(topicMeta);
    try {
      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(),
          SubscriptionAgent.topic().checkTopicOwner(oldOwnerConsumer, topicName).getCode());

      final TopicMeta transferredTopicMeta = topicMeta.deepCopy();
      transferredTopicMeta.transferOwner("owner2", 6L);
      SubscriptionAgent.topic().handleSingleTopicMetaChanges(transferredTopicMeta);

      Assert.assertEquals(
          TSStatusCode.SUBSCRIPTION_OWNER_FENCED.getStatusCode(),
          SubscriptionAgent.topic().checkTopicOwner(oldOwnerConsumer, topicName).getCode());
      Assert.assertEquals(
          TSStatusCode.SUBSCRIPTION_OWNER_FENCED.getStatusCode(),
          SubscriptionAgent.topic()
              .checkTopicOwners(oldOwnerConsumer, Collections.singleton(topicName))
              .getCode());
      Assert.assertNotNull(
          SubscriptionAgent.topic()
              .handleSingleTopicMetaChanges(createTopicMeta(topicName, "owner1", 5L)));
      Assert.assertEquals(
          TSStatusCode.SUBSCRIPTION_OWNER_FENCED.getStatusCode(),
          SubscriptionAgent.topic().checkTopicOwner(oldOwnerConsumer, topicName).getCode());
      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(),
          SubscriptionAgent.topic().checkTopicOwner(newOwnerConsumer, topicName).getCode());
      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(),
          SubscriptionAgent.topic()
              .checkTopicOwners(newOwnerConsumer, Collections.singleton(topicName))
              .getCode());
    } finally {
      SubscriptionAgent.topic().handleDropTopic(topicName);
    }
  }

  private long invokeCalculateConsumerInactivityTimeoutMs(
      final SubscriptionReceiverV1 receiver, final ConsumerConfig consumerConfig) throws Exception {
    final Method method =
        SubscriptionReceiverV1.class.getDeclaredMethod(
            "calculateConsumerInactivityTimeoutMs", ConsumerConfig.class);
    method.setAccessible(true);
    return (long) method.invoke(receiver, consumerConfig);
  }

  private TopicMeta createTopicMeta(
      final String topicName, final String ownerId, final long ownerEpoch) {
    final Map<String, String> topicAttributes = new HashMap<>();
    topicAttributes.put(TopicConstant.OWNER_ID_KEY, ownerId);
    topicAttributes.put(TopicConstant.OWNER_EPOCH_KEY, String.valueOf(ownerEpoch));
    return new TopicMeta(topicName, 1, topicAttributes);
  }

  private ConsumerConfig createConsumerConfig(final long heartbeatIntervalMs) {
    return createConsumerConfig(heartbeatIntervalMs, null, null);
  }

  private ConsumerConfig createConsumerConfig(
      final long heartbeatIntervalMs, final String ownerId, final Long ownerEpoch) {
    final Map<String, String> attributes = new HashMap<>();
    attributes.put(ConsumerConstant.CONSUMER_ID_KEY, "consumer-" + UUID.randomUUID());
    attributes.put(ConsumerConstant.CONSUMER_GROUP_ID_KEY, "group-" + UUID.randomUUID());
    attributes.put(ConsumerConstant.HEARTBEAT_INTERVAL_MS_KEY, String.valueOf(heartbeatIntervalMs));
    if (ownerId != null) {
      attributes.put(ConsumerConstant.OWNER_ID_KEY, ownerId);
    }
    if (ownerEpoch != null) {
      attributes.put(ConsumerConstant.OWNER_EPOCH_KEY, String.valueOf(ownerEpoch));
    }
    return new ConsumerConfig(attributes);
  }

  private Object getField(final Object target, final String fieldName) throws Exception {
    final Field field = target.getClass().getDeclaredField(fieldName);
    field.setAccessible(true);
    return field.get(target);
  }

  private void setField(final Object target, final String fieldName, final Object value)
      throws Exception {
    final Field field = target.getClass().getDeclaredField(fieldName);
    field.setAccessible(true);
    field.set(target, value);
  }
}
