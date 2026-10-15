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

import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.subscription.meta.topic.TopicMeta;
import org.apache.iotdb.db.subscription.agent.SubscriptionAgent;
import org.apache.iotdb.db.subscription.event.SubscriptionEvent;
import org.apache.iotdb.rpc.subscription.config.TopicConstant;
import org.apache.iotdb.rpc.subscription.payload.poll.SubscriptionCommitContext;
import org.apache.iotdb.rpc.subscription.payload.poll.SubscriptionPollResponseType;
import org.apache.iotdb.rpc.subscription.payload.poll.TerminationPayload;
import org.apache.iotdb.session.subscription.util.PollTimer;

import org.junit.Test;

import java.lang.reflect.Field;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SubscriptionPrefetchingQueueRecycleIntervalTest {

  @Test
  public void testRecycleIntervalForV1Queue() throws Exception {
    verifyRecycleInterval(false);
  }

  @Test
  public void testRecycleIntervalForV2Queue() throws Exception {
    verifyRecycleInterval(true);
  }

  private static void verifyRecycleInterval(final boolean useV2) throws Exception {
    final String topicName = "ordinary_recycle_topic_" + useV2;
    final int originalInterval =
        CommonDescriptor.getInstance()
            .getConfig()
            .getSubscriptionRecycleUncommittedEventIntervalMs();
    final SubscriptionBlockingPendingQueue inputQueue =
        mock(SubscriptionBlockingPendingQueue.class);
    when(inputQueue.isEmpty()).thenReturn(true);
    final SubscriptionPrefetchingQueue queue =
        new SubscriptionPrefetchingTabletQueue(
            "recycle_group", topicName, inputQueue, new AtomicLong());
    try {
      CommonDescriptor.getInstance()
          .getConfig()
          .setSubscriptionRecycleUncommittedEventIntervalMs(600000);
      updateInterval(topicName, "1200000");
      final SubscriptionEvent event =
          new SubscriptionEvent(
              SubscriptionPollResponseType.TERMINATION.getType(),
              new TerminationPayload(),
              new SubscriptionCommitContext(1, 1, topicName, "recycle_group", 1));
      assertTrue(event.pollable(0L));
      queue.prefetchEvent(event);
      assertSame(event, poll(queue, useV2));

      final Field timestampField = SubscriptionEvent.class.getDeclaredField("lastPolledTimestamp");
      timestampField.setAccessible(true);
      ((AtomicLong) timestampField.get(event)).set(System.currentTimeMillis() - 900000L);
      assertTrue(event.pollable());
      executePrefetch(queue, useV2);
      assertEquals(1L, queue.getSubscriptionUncommittedEventCount());
      assertEquals(0, queue.getPrefetchedEventCount());

      for (final long intervalMs : new long[] {0L, -1L, Long.MIN_VALUE}) {
        updateInterval(topicName, Long.toString(intervalMs));
        executePrefetch(queue, useV2);
        assertEquals(1L, queue.getSubscriptionUncommittedEventCount());
        assertEquals(0, queue.getPrefetchedEventCount());
        assertEquals(0L, event.getNackCount());
      }

      updateInterval(topicName, null);
      for (final int intervalMs : new int[] {0, -1, Integer.MIN_VALUE}) {
        CommonDescriptor.getInstance()
            .getConfig()
            .setSubscriptionRecycleUncommittedEventIntervalMs(intervalMs);
        assertFalse(event.pollable());
        executePrefetch(queue, useV2);
        assertEquals(1L, queue.getSubscriptionUncommittedEventCount());
        assertEquals(0, queue.getPrefetchedEventCount());
      }

      updateInterval(topicName, "300000");
      executePrefetch(queue, useV2);
      assertEquals(0L, queue.getSubscriptionUncommittedEventCount());
      assertEquals(1, queue.getPrefetchedEventCount());
      assertEquals(1L, event.getNackCount());
      assertSame(event, poll(queue, useV2));

      updateInterval(topicName, "0");
      assertTrue(queue.nack("consumer", event.getCommitContext()));
      executePrefetch(queue, useV2);
      assertSame(event, poll(queue, useV2));
      assertTrue(queue.ack("consumer", event.getCommitContext()));
    } finally {
      queue.cleanUp();
      SubscriptionAgent.topic().handleDropTopic(topicName, false);
      CommonDescriptor.getInstance()
          .getConfig()
          .setSubscriptionRecycleUncommittedEventIntervalMs(originalInterval);
    }
  }

  private static void updateInterval(final String topicName, final String interval) {
    assertNull(
        SubscriptionAgent.topic()
            .handleSingleTopicMetaChanges(
                new TopicMeta(
                    topicName,
                    1L,
                    interval == null
                        ? Collections.emptyMap()
                        : Collections.singletonMap(
                            TopicConstant.RECYCLE_UNCOMMITTED_EVENT_INTERVAL_MS_KEY, interval))));
  }

  private static SubscriptionEvent poll(
      final SubscriptionPrefetchingQueue queue, final boolean useV2) {
    return useV2
        ? queue.pollV2("consumer", new PollTimer(System.currentTimeMillis(), 0L))
        : queue.poll("consumer");
  }

  private static void executePrefetch(
      final SubscriptionPrefetchingQueue queue, final boolean useV2) {
    if (useV2) {
      queue.executePrefetchV2();
    } else {
      queue.executePrefetch();
    }
  }
}
