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
import org.apache.iotdb.db.subscription.broker.consensus.ConsensusPrefetchingQueue;
import org.apache.iotdb.rpc.subscription.payload.poll.RegionProgress;
import org.apache.iotdb.rpc.subscription.payload.poll.TopicProgress;
import org.apache.iotdb.rpc.subscription.payload.poll.WriterId;
import org.apache.iotdb.rpc.subscription.payload.poll.WriterProgress;

import org.junit.Test;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ConsensusSubscriptionBrokerSeekTest {

  private static final String TOPIC = "topic_seek_test";

  @Test
  public void testSeekAfterTopicProgressLeavesMissingRegionsUntouched() throws Exception {
    final ConsensusSubscriptionBroker broker = new ConsensusSubscriptionBroker("broker");
    final ConsensusPrefetchingQueue queue1 = mockQueue(1);
    final ConsensusPrefetchingQueue queue2 = mockQueue(2);
    injectQueues(broker, Arrays.asList(queue1, queue2));

    final String region1 = new DataRegionId(1).toString();
    final RegionProgress regionProgress =
        new RegionProgress(
            Collections.singletonMap(new WriterId(region1, 1, 1L), new WriterProgress(100L, 10L)));

    broker.seekAfter(TOPIC, new TopicProgress(Collections.singletonMap(region1, regionProgress)));

    verify(queue1).seekAfterRegionProgress(regionProgress);
    verify(queue2, never()).seekAfterRegionProgress(any());
    verify(queue2, never()).seekToRegionProgress(any());
    verify(queue2, never()).seekToEnd();
  }

  @Test
  public void testSeekTopicProgressLeavesMissingRegionsUntouched() throws Exception {
    final ConsensusSubscriptionBroker broker = new ConsensusSubscriptionBroker("broker");
    final ConsensusPrefetchingQueue queue1 = mockQueue(1);
    final ConsensusPrefetchingQueue queue2 = mockQueue(2);
    injectQueues(broker, Arrays.asList(queue1, queue2));

    final String region2 = new DataRegionId(2).toString();
    final RegionProgress regionProgress =
        new RegionProgress(
            Collections.singletonMap(new WriterId(region2, 2, 1L), new WriterProgress(200L, 20L)));

    broker.seek(TOPIC, new TopicProgress(Collections.singletonMap(region2, regionProgress)));

    verify(queue2).seekToRegionProgress(regionProgress);
    verify(queue1, never()).seekToRegionProgress(any());
    verify(queue1, never()).seekAfterRegionProgress(any());
    verify(queue1, never()).seekToEnd();
  }

  private static ConsensusPrefetchingQueue mockQueue(final int regionId) {
    final ConsensusPrefetchingQueue queue = mock(ConsensusPrefetchingQueue.class);
    when(queue.isClosed()).thenReturn(false);
    when(queue.getConsensusGroupId()).thenReturn(new DataRegionId(regionId));
    return queue;
  }

  @SuppressWarnings("unchecked")
  private static void injectQueues(
      final ConsensusSubscriptionBroker broker,
      final java.util.List<ConsensusPrefetchingQueue> queues)
      throws Exception {
    final Field field =
        ConsensusSubscriptionBroker.class.getDeclaredField("topicNameToConsensusPrefetchingQueues");
    field.setAccessible(true);
    final Map<String, java.util.List<ConsensusPrefetchingQueue>> topicToQueues =
        (Map<String, java.util.List<ConsensusPrefetchingQueue>>) field.get(broker);
    topicToQueues.put(TOPIC, queues);
  }
}
