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

package org.apache.iotdb.session.subscription.consumer.base;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.rpc.subscription.payload.poll.RegionProgress;
import org.apache.iotdb.rpc.subscription.payload.poll.SubscriptionCommitContext;
import org.apache.iotdb.rpc.subscription.payload.poll.TopicProgress;
import org.apache.iotdb.rpc.subscription.payload.poll.WriterId;
import org.apache.iotdb.rpc.subscription.payload.poll.WriterProgress;
import org.apache.iotdb.session.subscription.payload.SubscriptionMessage;

import org.junit.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class AbstractSubscriptionConsumerProgressTest {

  private static final String TOPIC = "topic_progress_test";
  private static final String GROUP = "group_progress_test";
  private static final String REGION = "1_100";

  @Test
  public void testAdvanceCurrentPositionsWithWriterProgress() throws Exception {
    final TestSubscriptionConsumer consumer = newConsumer();
    final WriterId writerId = new WriterId(REGION, 1, 2L);
    final WriterProgress writerProgress = new WriterProgress(100L, 10L);
    final SubscriptionMessage message =
        new SubscriptionMessage(
            new SubscriptionCommitContext(1, 0, TOPIC, GROUP, 0L, writerId, writerProgress),
            Collections.emptyMap());

    invokeAdvanceCurrentPositions(consumer, Collections.singletonList(message));

    final TopicProgress positions = consumer.positions(TOPIC);
    assertEquals(
        writerProgress,
        positions.getRegionProgress().get(REGION).getWriterPositions().get(writerId));
  }

  @Test
  public void testAdvanceCommittedPositionsFallsBackToLegacyFields() throws Exception {
    final TestSubscriptionConsumer consumer = newConsumer();
    final SubscriptionCommitContext legacyContext =
        new SubscriptionCommitContext(7, 0, TOPIC, GROUP, 11L, 0L, REGION, 101L);

    invokeAdvanceCommittedPositions(consumer, Collections.singletonList(legacyContext));

    final TopicProgress committed = consumer.committedPositions(TOPIC);
    final RegionProgress regionProgress = committed.getRegionProgress().get(REGION);
    assertNotNull(regionProgress);
    assertEquals(1, regionProgress.getWriterPositions().size());
    final Map.Entry<WriterId, WriterProgress> onlyEntry =
        regionProgress.getWriterPositions().entrySet().iterator().next();
    assertEquals(new WriterId(REGION, 7, 0L), onlyEntry.getKey());
    assertEquals(new WriterProgress(101L, 11L), onlyEntry.getValue());
  }

  @Test
  public void testAdvanceCurrentPositionsMergesPerWriterAndKeepsNewest() throws Exception {
    final TestSubscriptionConsumer consumer = newConsumer();

    final WriterId writer1 = new WriterId(REGION, 1, 1L);
    final WriterId writer2 = new WriterId(REGION, 2, 1L);
    final SubscriptionMessage olderWriter1 =
        new SubscriptionMessage(
            new SubscriptionCommitContext(
                1, 0, TOPIC, GROUP, 0L, writer1, new WriterProgress(100L, 8L)),
            Collections.emptyMap());
    final SubscriptionMessage newerWriter1 =
        new SubscriptionMessage(
            new SubscriptionCommitContext(
                1, 0, TOPIC, GROUP, 0L, writer1, new WriterProgress(100L, 10L)),
            Collections.emptyMap());
    final SubscriptionMessage writer2Message =
        new SubscriptionMessage(
            new SubscriptionCommitContext(
                2, 0, TOPIC, GROUP, 0L, writer2, new WriterProgress(95L, 7L)),
            Collections.emptyMap());

    invokeAdvanceCurrentPositions(
        consumer, Arrays.asList(olderWriter1, newerWriter1, writer2Message));

    final RegionProgress regionProgress = consumer.positions(TOPIC).getRegionProgress().get(REGION);
    assertEquals(2, regionProgress.getWriterPositions().size());
    assertEquals(new WriterProgress(100L, 10L), regionProgress.getWriterPositions().get(writer1));
    assertEquals(new WriterProgress(95L, 7L), regionProgress.getWriterPositions().get(writer2));
  }

  private static TestSubscriptionConsumer newConsumer() throws Exception {
    final TestSubscriptionConsumer consumer =
        new TestSubscriptionConsumer(
            new AbstractSubscriptionConsumerBuilder()
                .consumerId("progress_consumer")
                .consumerGroupId(GROUP));
    final Field isClosedField = AbstractSubscriptionConsumer.class.getDeclaredField("isClosed");
    isClosedField.setAccessible(true);
    ((AtomicBoolean) isClosedField.get(consumer)).set(false);
    return consumer;
  }

  @SuppressWarnings("unchecked")
  private static void invokeAdvanceCurrentPositions(
      final AbstractSubscriptionConsumer consumer, final List<SubscriptionMessage> messages)
      throws Exception {
    final Method method =
        AbstractSubscriptionConsumer.class.getDeclaredMethod("advanceCurrentPositions", List.class);
    method.setAccessible(true);
    method.invoke(consumer, messages);
  }

  private static void invokeAdvanceCommittedPositions(
      final AbstractSubscriptionConsumer consumer,
      final List<SubscriptionCommitContext> commitContexts)
      throws Exception {
    final Method method =
        AbstractSubscriptionConsumer.class.getDeclaredMethod(
            "advanceCommittedPositions", List.class);
    method.setAccessible(true);
    method.invoke(consumer, commitContexts);
  }

  private static final class TestSubscriptionConsumer extends AbstractSubscriptionConsumer {

    private TestSubscriptionConsumer(final AbstractSubscriptionConsumerBuilder builder) {
      super(builder);
    }

    @Override
    protected AbstractSubscriptionProvider constructSubscriptionProvider(
        final TEndPoint endPoint,
        final String username,
        final String password,
        final String consumerId,
        final String consumerGroupId,
        final int thriftMaxFrameSize,
        final long heartbeatIntervalMs,
        final int connectionTimeoutInMs) {
      throw new UnsupportedOperationException("No provider needed for progress unit tests");
    }
  }
}
