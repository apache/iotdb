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
import org.apache.iotdb.commons.subscription.meta.consumer.ConsumerGroupMeta;
import org.apache.iotdb.commons.subscription.meta.consumer.ConsumerMeta;
import org.apache.iotdb.commons.subscription.meta.topic.TopicMeta;
import org.apache.iotdb.db.subscription.agent.SubscriptionAgent;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.rpc.subscription.config.ConsumerConfig;
import org.apache.iotdb.rpc.subscription.config.ConsumerConstant;
import org.apache.iotdb.rpc.subscription.config.TopicConstant;
import org.apache.iotdb.rpc.subscription.payload.poll.SubscriptionCommitContext;
import org.apache.iotdb.rpc.subscription.payload.request.PipeSubscribeHandshakeReq;
import org.apache.iotdb.rpc.subscription.payload.request.PipeSubscribeSliceReqBuilder;
import org.apache.iotdb.rpc.subscription.payload.request.SubscriptionHeartbeatReq;
import org.apache.iotdb.service.rpc.thrift.TPipeSubscribeReq;
import org.apache.iotdb.service.rpc.thrift.TPipeSubscribeResp;

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
  public void testHandleTimeoutRetriesAfterInFlightRequestCompletes() throws Exception {
    final AtomicLong closeAttemptCount = new AtomicLong();
    final SubscriptionReceiverV1 receiver =
        new SubscriptionReceiverV1() {
          @Override
          void closeConsumer(final ConsumerConfig consumerConfig) {
            closeAttemptCount.incrementAndGet();
          }
        };
    final ConsumerConfig consumerConfig = createConsumerConfig(1_000L);
    setField(receiver, "sharedConsumerConfig", consumerConfig);
    final long timeoutMs = invokeCalculateConsumerInactivityTimeoutMs(receiver, consumerConfig);
    setField(receiver, "lastActivityTimeMs", System.currentTimeMillis() - timeoutMs - 1L);
    final AtomicLong inFlightRequestCount = (AtomicLong) getField(receiver, "inFlightRequestCount");

    inFlightRequestCount.set(1L);
    receiver.handleTimeout();

    Assert.assertEquals(0L, closeAttemptCount.get());
    Assert.assertSame(consumerConfig, getField(receiver, "sharedConsumerConfig"));

    inFlightRequestCount.set(0L);
    receiver.handleTimeout();

    Assert.assertEquals(1L, closeAttemptCount.get());
    Assert.assertNull(getField(receiver, "sharedConsumerConfig"));
    Assert.assertTrue((boolean) getField(receiver, "consumerInvalidated"));
  }

  @Test
  public void testHandleTimeoutRetriesAfterCleanupFailure() throws Exception {
    final AtomicLong closeAttemptCount = new AtomicLong();
    final SubscriptionReceiverV1 receiver =
        new SubscriptionReceiverV1() {
          @Override
          void closeConsumer(final ConsumerConfig consumerConfig) {
            closeAttemptCount.incrementAndGet();
            throw new RuntimeException("expected cleanup failure");
          }
        };
    final ConsumerConfig consumerConfig = createConsumerConfig(1_000L);
    setField(receiver, "sharedConsumerConfig", consumerConfig);
    final long timeoutMs = invokeCalculateConsumerInactivityTimeoutMs(receiver, consumerConfig);
    setField(receiver, "lastActivityTimeMs", System.currentTimeMillis() - timeoutMs - 1L);

    receiver.handleTimeout();
    receiver.handleTimeout();

    Assert.assertEquals(2L, closeAttemptCount.get());
    Assert.assertSame(consumerConfig, getField(receiver, "sharedConsumerConfig"));
    Assert.assertFalse((boolean) getField(receiver, "consumerInvalidated"));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testHandleExitKeepsSharedConsumerStateForTimeoutCleanup() throws Exception {
    final SubscriptionReceiverV1 receiver = new SubscriptionReceiverV1();
    final ConsumerConfig consumerConfig = createConsumerConfig(1_000L);
    setField(receiver, "sharedConsumerConfig", consumerConfig);
    final ThreadLocal<ConsumerConfig> consumerConfigThreadLocal =
        (ThreadLocal<ConsumerConfig>) getField(receiver, "consumerConfigThreadLocal");
    consumerConfigThreadLocal.set(consumerConfig);

    receiver.handleExit();

    Assert.assertSame(consumerConfig, getField(receiver, "sharedConsumerConfig"));
    Assert.assertFalse((boolean) getField(receiver, "consumerInvalidated"));
    Assert.assertTrue(receiver.hasActiveConsumer());
    Assert.assertNull(consumerConfigThreadLocal.get());
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testHandleExitClearsThreadLocalStateAfterInvalidation() throws Exception {
    final SubscriptionReceiverV1 receiver = new SubscriptionReceiverV1();
    final ConsumerConfig consumerConfig = createConsumerConfig(1_000L);
    setField(receiver, "sharedConsumerConfig", consumerConfig);
    final ThreadLocal<ConsumerConfig> consumerConfigThreadLocal =
        (ThreadLocal<ConsumerConfig>) getField(receiver, "consumerConfigThreadLocal");
    consumerConfigThreadLocal.set(consumerConfig);

    receiver.invalidateConsumer();
    receiver.handleExit();

    Assert.assertFalse(receiver.hasActiveConsumer());
    Assert.assertTrue((boolean) getField(receiver, "consumerInvalidated"));
    Assert.assertNull(consumerConfigThreadLocal.get());
  }

  @Test
  public void testInvalidatedConsumerReturnsFencedStatus() throws Exception {
    final SubscriptionReceiverV1 receiver = new SubscriptionReceiverV1();
    final ConsumerConfig consumerConfig = createConsumerConfig(1_000L);
    setField(receiver, "sharedConsumerConfig", consumerConfig);

    receiver.invalidateConsumer();

    Assert.assertEquals(
        TSStatusCode.SUBSCRIPTION_CONSUMER_FENCED.getStatusCode(),
        receiver.handle(SubscriptionHeartbeatReq.toThriftReq()).getStatus().getCode());
  }

  @Test
  public void testFencedConsumerCannotHandshakeAgain() throws Exception {
    final SubscriptionReceiverV1 receiver = new SubscriptionReceiverV1();
    final ConsumerConfig consumerConfig = createConsumerConfig(1_000L);

    setField(receiver, "sharedConsumerConfig", consumerConfig);
    receiver.invalidateConsumer();

    Assert.assertEquals(
        TSStatusCode.SUBSCRIPTION_CONSUMER_FENCED.getStatusCode(),
        receiver
            .handle(PipeSubscribeHandshakeReq.toTPipeSubscribeReq(consumerConfig))
            .getStatus()
            .getCode());
    Assert.assertTrue((boolean) getField(receiver, "consumerFenced"));
  }

  @Test
  public void testNeverHandshakenConsumerStillReturnsMissingConsumerStatus() {
    final SubscriptionReceiverV1 receiver = new SubscriptionReceiverV1();

    Assert.assertEquals(
        TSStatusCode.SUBSCRIPTION_MISSING_CONSUMER.getStatusCode(),
        receiver.handle(SubscriptionHeartbeatReq.toThriftReq()).getStatus().getCode());
  }

  @Test
  public void testHandleSlicesDispatchesReassembledRequest() throws Exception {
    final SubscriptionReceiverV1 receiver = new SubscriptionReceiverV1();
    final TPipeSubscribeReq heartbeatReq =
        SubscriptionHeartbeatReq.toThriftReq(Collections.emptyList());
    final int bodySizeLimit = 2;
    final int sliceCount = PipeSubscribeSliceReqBuilder.getSliceCount(heartbeatReq, bodySizeLimit);

    final TPipeSubscribeResp firstResp =
        receiver.handle(
            PipeSubscribeSliceReqBuilder.buildSliceReq(
                heartbeatReq, 1, 0, sliceCount, bodySizeLimit));
    Assert.assertEquals(
        TSStatusCode.SUCCESS_STATUS.getStatusCode(), firstResp.getStatus().getCode());

    final TPipeSubscribeResp lastResp =
        receiver.handle(
            PipeSubscribeSliceReqBuilder.buildSliceReq(
                heartbeatReq, 1, 1, sliceCount, bodySizeLimit));
    Assert.assertEquals(
        TSStatusCode.SUBSCRIPTION_MISSING_CONSUMER.getStatusCode(), lastResp.getStatus().getCode());
  }

  @Test
  public void testNonSliceRequestClearsIncompleteSlices() throws Exception {
    final SubscriptionReceiverV1 receiver = new SubscriptionReceiverV1();
    final TPipeSubscribeReq heartbeatReq =
        SubscriptionHeartbeatReq.toThriftReq(Collections.emptyList());
    final int bodySizeLimit = 2;
    final int sliceCount = PipeSubscribeSliceReqBuilder.getSliceCount(heartbeatReq, bodySizeLimit);

    Assert.assertEquals(
        TSStatusCode.SUCCESS_STATUS.getStatusCode(),
        receiver
            .handle(
                PipeSubscribeSliceReqBuilder.buildSliceReq(
                    heartbeatReq, 1, 0, sliceCount, bodySizeLimit))
            .getStatus()
            .getCode());
    Assert.assertEquals(
        TSStatusCode.SUBSCRIPTION_MISSING_CONSUMER.getStatusCode(),
        receiver.handle(SubscriptionHeartbeatReq.toThriftReq()).getStatus().getCode());
    Assert.assertEquals(
        TSStatusCode.SUBSCRIPTION_TYPE_ERROR.getStatusCode(),
        receiver
            .handle(
                PipeSubscribeSliceReqBuilder.buildSliceReq(
                    heartbeatReq, 1, 1, sliceCount, bodySizeLimit))
            .getStatus()
            .getCode());
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

  @Test
  @SuppressWarnings("unchecked")
  public void testFencedOwnerHeartbeatDoesNotRefreshBufferedEventLeases() throws Exception {
    final String topicName = "topic-" + UUID.randomUUID();
    final String consumerGroupId = "group-" + UUID.randomUUID();
    final String consumerId = "consumer-" + UUID.randomUUID();
    final ConsumerConfig oldOwnerConsumer =
        createConsumerConfig(1_000L, "owner1", 5L, consumerId, consumerGroupId);
    final SubscriptionCommitContext bufferedContext =
        new SubscriptionCommitContext(1, 0, topicName, consumerGroupId, 1L);

    final TopicMeta topicMeta = createTopicMeta(topicName, "owner1", 5L);
    SubscriptionAgent.topic().handleSingleTopicMetaChanges(topicMeta);
    final ConsumerGroupMeta consumerGroupMeta =
        new ConsumerGroupMeta(
            consumerGroupId,
            System.currentTimeMillis(),
            new ConsumerMeta(
                consumerId, System.currentTimeMillis(), oldOwnerConsumer.getAttribute()));
    consumerGroupMeta.addSubscription(consumerId, Collections.singleton(topicName));
    SubscriptionAgent.consumer().handleSingleConsumerGroupMetaChanges(consumerGroupMeta);

    try {
      final TopicMeta transferredTopicMeta = topicMeta.deepCopy();
      transferredTopicMeta.transferOwner("owner2", 6L);
      SubscriptionAgent.topic().handleSingleTopicMetaChanges(transferredTopicMeta);

      final AtomicLong refreshCount = new AtomicLong();
      final SubscriptionReceiverV1 receiver =
          new SubscriptionReceiverV1() {
            @Override
            protected int refreshInFlightEventLeases(
                final ConsumerConfig consumerConfig,
                final java.util.List<SubscriptionCommitContext> processorBufferedCommitContexts) {
              refreshCount.incrementAndGet();
              return 1;
            }
          };
      setField(receiver, "sharedConsumerConfig", oldOwnerConsumer);
      final ThreadLocal<ConsumerConfig> consumerConfigThreadLocal =
          (ThreadLocal<ConsumerConfig>) getField(receiver, "consumerConfigThreadLocal");
      consumerConfigThreadLocal.set(oldOwnerConsumer);

      Assert.assertEquals(
          TSStatusCode.SUBSCRIPTION_OWNER_FENCED.getStatusCode(),
          receiver
              .handle(
                  SubscriptionHeartbeatReq.toThriftReq(Collections.singletonList(bufferedContext)))
              .getStatus()
              .getCode());
      Assert.assertEquals(0L, refreshCount.get());
    } finally {
      SubscriptionAgent.consumer().handleDropConsumerGroup(consumerGroupId);
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
    return createConsumerConfig(
        heartbeatIntervalMs,
        ownerId,
        ownerEpoch,
        "consumer-" + UUID.randomUUID(),
        "group-" + UUID.randomUUID());
  }

  private ConsumerConfig createConsumerConfig(
      final long heartbeatIntervalMs,
      final String ownerId,
      final Long ownerEpoch,
      final String consumerId,
      final String consumerGroupId) {
    final Map<String, String> attributes = new HashMap<>();
    attributes.put(ConsumerConstant.CONSUMER_ID_KEY, consumerId);
    attributes.put(ConsumerConstant.CONSUMER_GROUP_ID_KEY, consumerGroupId);
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
    final Field field = SubscriptionReceiverV1.class.getDeclaredField(fieldName);
    field.setAccessible(true);
    return field.get(target);
  }

  private void setField(final Object target, final String fieldName, final Object value)
      throws Exception {
    final Field field = SubscriptionReceiverV1.class.getDeclaredField(fieldName);
    field.setAccessible(true);
    field.set(target, value);
  }
}
