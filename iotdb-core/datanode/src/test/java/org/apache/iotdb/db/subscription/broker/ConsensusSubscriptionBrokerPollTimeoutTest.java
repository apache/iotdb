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

package org.apache.iotdb.db.subscription.broker;

import org.apache.iotdb.commons.consensus.DataRegionId;
import org.apache.iotdb.db.subscription.agent.SubscriptionAgent;
import org.apache.iotdb.db.subscription.agent.SubscriptionReceiverAgent;
import org.apache.iotdb.db.subscription.broker.consensus.ConsensusPrefetchingQueue;
import org.apache.iotdb.db.subscription.event.SubscriptionEvent;
import org.apache.iotdb.db.subscription.receiver.SubscriptionReceiver;
import org.apache.iotdb.rpc.subscription.payload.request.PipeSubscribeRequestVersion;
import org.apache.iotdb.session.subscription.util.PollTimer;

import org.junit.Test;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ConsensusSubscriptionBrokerPollTimeoutTest {

  private static final String CONSUMER_ID = "consumer";

  @Test
  public void testSharesRemainingBudgetAcrossRegionsAndTopics() throws Exception {
    final ConsensusSubscriptionBroker broker = new ConsensusSubscriptionBroker("group");
    final ConsensusPrefetchingQueue first = newQueue(1);
    final ConsensusPrefetchingQueue second = newQueue(2);
    final ConsensusPrefetchingQueue third = newQueue(3);
    bindQueues(broker, "first", first, second);
    bindQueues(broker, "second", third);
    final PollTimer timer = newManualTimer(1000L);
    when(first.poll(eq(CONSUMER_ID), isNull(), any(PollTimer.class)))
        .thenAnswer(
            invocation -> {
              assertSame(timer, invocation.getArgument(2));
              timer.update(timer.currentTimeMs() + 400L);
              return null;
            });
    when(second.poll(eq(CONSUMER_ID), isNull(), any(PollTimer.class)))
        .thenAnswer(
            invocation -> {
              assertSame(timer, invocation.getArgument(2));
              assertEquals(600L, timer.remainingMs());
              timer.update(timer.currentTimeMs() + 400L);
              return null;
            });
    when(third.poll(eq(CONSUMER_ID), isNull(), any(PollTimer.class)))
        .thenAnswer(
            invocation -> {
              assertSame(timer, invocation.getArgument(2));
              assertEquals(200L, timer.remainingMs());
              return null;
            });

    assertTrue(
        broker
            .poll(
                CONSUMER_ID,
                new LinkedHashSet<>(Arrays.asList("first", "second")),
                100L,
                Collections.emptyMap(),
                timer)
            .isEmpty());
    verify(third).poll(eq(CONSUMER_ID), isNull(), any(PollTimer.class));
  }

  @Test
  public void testExpirationStopsLaterRegionsAndTopicsButKeepsCollectedEvents() throws Exception {
    final ConsensusSubscriptionBroker broker = new ConsensusSubscriptionBroker("group");
    final ConsensusPrefetchingQueue first = newQueue(1);
    final ConsensusPrefetchingQueue second = newQueue(2);
    final ConsensusPrefetchingQueue third = newQueue(3);
    bindQueues(broker, "first", first, second);
    bindQueues(broker, "second", third);
    final PollTimer timer = newManualTimer(1000L);
    final SubscriptionEvent event = mock(SubscriptionEvent.class);
    when(event.getCurrentResponseSize()).thenReturn(10);
    when(first.poll(eq(CONSUMER_ID), isNull(), any(PollTimer.class)))
        .thenAnswer(
            invocation -> {
              timer.update(timer.currentTimeMs() + 1000L);
              return event;
            });

    assertEquals(
        Collections.singletonList(event),
        broker.poll(
            CONSUMER_ID,
            new LinkedHashSet<>(Arrays.asList("first", "second")),
            100L,
            Collections.emptyMap(),
            timer));
    verify(second, never()).poll(eq(CONSUMER_ID), isNull(), any(PollTimer.class));
    verify(third, never()).poll(eq(CONSUMER_ID), isNull(), any(PollTimer.class));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testUsesReceiverRemainingBudget() throws Exception {
    final Field field = SubscriptionReceiverAgent.class.getDeclaredField("receiverThreadLocal");
    field.setAccessible(true);
    final ThreadLocal<SubscriptionReceiver> receivers =
        (ThreadLocal<SubscriptionReceiver>) field.get(SubscriptionAgent.receiver());
    final SubscriptionReceiver previousReceiver = receivers.get();
    final SubscriptionReceiver receiver = mock(SubscriptionReceiver.class);
    when(receiver.getVersion()).thenReturn(PipeSubscribeRequestVersion.VERSION_1);
    when(receiver.remainingMs()).thenReturn(2000L);
    receivers.set(receiver);
    try {
      final ConsensusSubscriptionBroker broker = new ConsensusSubscriptionBroker("group");
      final ConsensusPrefetchingQueue queue = newQueue(1);
      bindQueues(broker, "topic", queue);
      when(queue.poll(eq(CONSUMER_ID), isNull(), any(PollTimer.class)))
          .thenAnswer(
              invocation -> {
                final PollTimer timer = invocation.getArgument(2);
                assertEquals(2000L, timer.timeoutMs());
                assertTrue(timer.remainingMs() <= 2000L);
                return null;
              });

      broker.poll(CONSUMER_ID, Collections.singleton("topic"), 100L);
      verify(queue).poll(eq(CONSUMER_ID), isNull(), any(PollTimer.class));

      when(receiver.remainingMs()).thenReturn(0L);
      final ConsensusPrefetchingQueue expiredQueue = newQueue(2);
      bindQueues(broker, "expired", expiredQueue);
      assertTrue(broker.poll(CONSUMER_ID, Collections.singleton("expired"), 100L).isEmpty());
      verify(expiredQueue, never()).poll(eq(CONSUMER_ID), isNull(), any(PollTimer.class));
    } finally {
      if (previousReceiver == null) {
        receivers.remove();
      } else {
        receivers.set(previousReceiver);
      }
    }
  }

  private static PollTimer newManualTimer(final long timeoutMs) {
    return new PollTimer(System.currentTimeMillis(), timeoutMs) {
      @Override
      public void update() {
        // Tests advance the shared clock explicitly without wall-clock timing assertions.
      }
    };
  }

  private static ConsensusPrefetchingQueue newQueue(final int regionId) {
    final ConsensusPrefetchingQueue queue = mock(ConsensusPrefetchingQueue.class);
    when(queue.getConsensusGroupId()).thenReturn(new DataRegionId(regionId));
    return queue;
  }

  @SuppressWarnings("unchecked")
  private static void bindQueues(
      final ConsensusSubscriptionBroker broker,
      final String topic,
      final ConsensusPrefetchingQueue... queues)
      throws Exception {
    final Field field =
        ConsensusSubscriptionBroker.class.getDeclaredField("topicNameToConsensusPrefetchingQueues");
    field.setAccessible(true);
    ((Map<String, List<ConsensusPrefetchingQueue>>) field.get(broker))
        .put(topic, Arrays.asList(queues));
  }
}
