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
import org.apache.iotdb.rpc.subscription.payload.poll.TopicProgress;
import org.apache.iotdb.rpc.subscription.payload.poll.WriterId;
import org.apache.iotdb.rpc.subscription.payload.poll.WriterProgress;

import org.junit.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class AbstractSubscriptionConsumerSeekProgressTest {

  private static final String TOPIC = "topic_seek_progress_test";
  private static final String REGION_A = "1_100";
  private static final String REGION_B = "1_101";

  @Test
  public void testOverlayTopicProgressPreservesMissingRegions() throws Exception {
    final TestSubscriptionConsumer consumer = newConsumer();

    invokeSetCurrentPositions(
        consumer,
        TOPIC,
        buildTopicProgress(
            REGION_A,
            new WriterId(REGION_A, 1, 1L),
            new WriterProgress(100L, 10L),
            REGION_B,
            new WriterId(REGION_B, 2, 1L),
            new WriterProgress(200L, 20L)));

    invokeOverlayCurrentPositions(
        consumer,
        TOPIC,
        new TopicProgress(
            Collections.singletonMap(
                REGION_A,
                new RegionProgress(
                    Collections.singletonMap(
                        new WriterId(REGION_A, 1, 1L), new WriterProgress(50L, 5L))))));

    final TopicProgress positions = consumer.positions(TOPIC);
    assertNotNull(positions.getRegionProgress().get(REGION_A));
    assertNotNull(positions.getRegionProgress().get(REGION_B));
    assertEquals(
        new WriterProgress(50L, 5L),
        positions
            .getRegionProgress()
            .get(REGION_A)
            .getWriterPositions()
            .values()
            .iterator()
            .next());
    assertEquals(
        new WriterProgress(200L, 20L),
        positions
            .getRegionProgress()
            .get(REGION_B)
            .getWriterPositions()
            .values()
            .iterator()
            .next());
  }

  @Test
  public void testOverlayTopicProgressAllowsSeekBackwardsForSpecifiedRegion() throws Exception {
    final TestSubscriptionConsumer consumer = newConsumer();

    invokeSetCommittedPositions(
        consumer,
        TOPIC,
        new TopicProgress(
            Collections.singletonMap(
                REGION_A,
                new RegionProgress(
                    Collections.singletonMap(
                        new WriterId(REGION_A, 1, 1L), new WriterProgress(100L, 10L))))));

    invokeOverlayCommittedPositions(
        consumer,
        TOPIC,
        new TopicProgress(
            Collections.singletonMap(
                REGION_A,
                new RegionProgress(
                    Collections.singletonMap(
                        new WriterId(REGION_A, 1, 1L), new WriterProgress(80L, 4L))))));

    assertEquals(
        new WriterProgress(80L, 4L),
        consumer
            .committedPositions(TOPIC)
            .getRegionProgress()
            .get(REGION_A)
            .getWriterPositions()
            .values()
            .iterator()
            .next());
  }

  private static TestSubscriptionConsumer newConsumer() throws Exception {
    final TestSubscriptionConsumer consumer =
        new TestSubscriptionConsumer(
            new AbstractSubscriptionConsumerBuilder()
                .consumerId("seek_progress_consumer")
                .consumerGroupId("seek_progress_group"));
    final Field isClosedField = AbstractSubscriptionConsumer.class.getDeclaredField("isClosed");
    isClosedField.setAccessible(true);
    ((AtomicBoolean) isClosedField.get(consumer)).set(false);
    return consumer;
  }

  private static void invokeSetCurrentPositions(
      final AbstractSubscriptionConsumer consumer,
      final String topicName,
      final TopicProgress topicProgress)
      throws Exception {
    final Method method =
        AbstractSubscriptionConsumer.class.getDeclaredMethod(
            "setCurrentPositions", String.class, TopicProgress.class);
    method.setAccessible(true);
    method.invoke(consumer, topicName, topicProgress);
  }

  private static void invokeSetCommittedPositions(
      final AbstractSubscriptionConsumer consumer,
      final String topicName,
      final TopicProgress topicProgress)
      throws Exception {
    final Method method =
        AbstractSubscriptionConsumer.class.getDeclaredMethod(
            "setCommittedPositions", String.class, TopicProgress.class);
    method.setAccessible(true);
    method.invoke(consumer, topicName, topicProgress);
  }

  private static void invokeOverlayCurrentPositions(
      final AbstractSubscriptionConsumer consumer,
      final String topicName,
      final TopicProgress topicProgress)
      throws Exception {
    final Method method =
        AbstractSubscriptionConsumer.class.getDeclaredMethod(
            "overlayCurrentPositions", String.class, TopicProgress.class);
    method.setAccessible(true);
    method.invoke(consumer, topicName, topicProgress);
  }

  private static void invokeOverlayCommittedPositions(
      final AbstractSubscriptionConsumer consumer,
      final String topicName,
      final TopicProgress topicProgress)
      throws Exception {
    final Method method =
        AbstractSubscriptionConsumer.class.getDeclaredMethod(
            "overlayCommittedPositions", String.class, TopicProgress.class);
    method.setAccessible(true);
    method.invoke(consumer, topicName, topicProgress);
  }

  private static TopicProgress buildTopicProgress(
      final String regionA,
      final WriterId writerA,
      final WriterProgress writerProgressA,
      final String regionB,
      final WriterId writerB,
      final WriterProgress writerProgressB) {
    final Map<String, RegionProgress> regionProgress = new LinkedHashMap<>();
    regionProgress.put(
        regionA, new RegionProgress(Collections.singletonMap(writerA, writerProgressA)));
    regionProgress.put(
        regionB, new RegionProgress(Collections.singletonMap(writerB, writerProgressB)));
    return new TopicProgress(regionProgress);
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
      throw new UnsupportedOperationException("No provider needed for seek progress unit tests");
    }
  }
}
