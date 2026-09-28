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
import org.apache.iotdb.db.subscription.event.SubscriptionEvent;
import org.apache.iotdb.rpc.subscription.payload.poll.SubscriptionCommitContext;

import org.junit.Test;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ConsensusSubscriptionBrokerPayloadLimitTest {

  private static final String CONSUMER_GROUP_ID = "consumerGroup";
  private static final String CONSUMER_ID = "consumer";
  private static final String OTHER_CONSUMER_ID = "otherConsumer";
  private static final String TOPIC_NAME = "topic";

  @Test
  public void testPollRequeuesEventThatWouldExceedPayloadLimit() throws Exception {
    final ConsensusSubscriptionBroker broker = new ConsensusSubscriptionBroker(CONSUMER_GROUP_ID);
    final ConsensusPrefetchingQueue firstQueue = mock(ConsensusPrefetchingQueue.class);
    final ConsensusPrefetchingQueue secondQueue = mock(ConsensusPrefetchingQueue.class);
    final SubscriptionEvent firstEvent = mock(SubscriptionEvent.class);
    final SubscriptionEvent secondEvent = mock(SubscriptionEvent.class);
    final SubscriptionCommitContext firstCommitContext = newCommitContext(1, 1);
    final SubscriptionCommitContext secondCommitContext = newCommitContext(2, 2);

    when(firstQueue.getConsensusGroupId()).thenReturn(new DataRegionId(1));
    when(secondQueue.getConsensusGroupId()).thenReturn(new DataRegionId(2));
    when(firstQueue.poll(CONSUMER_ID, null)).thenReturn(firstEvent);
    when(secondQueue.poll(CONSUMER_ID, null)).thenReturn(secondEvent);
    when(firstEvent.getCurrentResponseSize()).thenReturn(40);
    when(secondEvent.getCurrentResponseSize()).thenReturn(30);
    when(firstEvent.getCommitContext()).thenReturn(firstCommitContext);
    when(secondEvent.getCommitContext()).thenReturn(secondCommitContext);
    when(secondQueue.requeue(CONSUMER_ID, secondCommitContext)).thenReturn(true);
    bindQueues(broker, Arrays.asList(firstQueue, secondQueue));

    final List<SubscriptionEvent> events =
        broker.poll(CONSUMER_ID, Collections.singleton(TOPIC_NAME), 60L);

    assertEquals(1, events.size());
    assertSame(firstEvent, events.get(0));
    verify(secondQueue).requeue(CONSUMER_ID, secondCommitContext);
  }

  @Test
  public void testPollBorrowsReadyEventWhenAssignedQueueIsEmpty() throws Exception {
    final ConsensusSubscriptionBroker broker = new ConsensusSubscriptionBroker(CONSUMER_GROUP_ID);
    final ConsensusPrefetchingQueue fallbackQueue = mock(ConsensusPrefetchingQueue.class);
    final ConsensusPrefetchingQueue assignedQueue = mock(ConsensusPrefetchingQueue.class);
    final SubscriptionEvent fallbackEvent = mock(SubscriptionEvent.class);

    when(fallbackQueue.getConsensusGroupId()).thenReturn(new DataRegionId(1));
    when(assignedQueue.getConsensusGroupId()).thenReturn(new DataRegionId(2));
    when(fallbackQueue.getPrefetchedEventCount()).thenReturn(1);
    when(fallbackQueue.poll(CONSUMER_ID, null)).thenReturn(fallbackEvent);
    when(fallbackEvent.getCurrentResponseSize()).thenReturn(30);
    bindQueues(broker, Arrays.asList(fallbackQueue, assignedQueue));

    // The first consumer initially owns both regions. After the second consumer joins, the stable
    // ownership balancer moves DataRegion[2] to it and keeps DataRegion[1] with the first consumer.
    broker.poll(OTHER_CONSUMER_ID, Collections.singleton(TOPIC_NAME), 60L);
    final List<SubscriptionEvent> events =
        broker.poll(CONSUMER_ID, Collections.singleton(TOPIC_NAME), 60L);

    assertEquals(1, events.size());
    assertSame(fallbackEvent, events.get(0));
    verify(assignedQueue).poll(CONSUMER_ID, null);
    verify(fallbackQueue).poll(CONSUMER_ID, null);
  }

  @Test
  public void testPollKeepsOwnershipAffinityWhenAssignedQueueHasData() throws Exception {
    final ConsensusSubscriptionBroker broker = new ConsensusSubscriptionBroker(CONSUMER_GROUP_ID);
    final ConsensusPrefetchingQueue fallbackQueue = mock(ConsensusPrefetchingQueue.class);
    final ConsensusPrefetchingQueue assignedQueue = mock(ConsensusPrefetchingQueue.class);
    final SubscriptionEvent fallbackEvent = mock(SubscriptionEvent.class);
    final SubscriptionEvent assignedEvent = mock(SubscriptionEvent.class);

    when(fallbackQueue.getConsensusGroupId()).thenReturn(new DataRegionId(1));
    when(assignedQueue.getConsensusGroupId()).thenReturn(new DataRegionId(2));
    when(fallbackQueue.getPrefetchedEventCount()).thenReturn(1);
    when(fallbackQueue.poll(CONSUMER_ID, null)).thenReturn(fallbackEvent);
    when(assignedQueue.poll(CONSUMER_ID, null)).thenReturn(assignedEvent);
    when(assignedEvent.getCurrentResponseSize()).thenReturn(30);
    bindQueues(broker, Arrays.asList(fallbackQueue, assignedQueue));

    broker.poll(OTHER_CONSUMER_ID, Collections.singleton(TOPIC_NAME), 60L);
    final List<SubscriptionEvent> events =
        broker.poll(CONSUMER_ID, Collections.singleton(TOPIC_NAME), 60L);

    assertEquals(1, events.size());
    assertSame(assignedEvent, events.get(0));
    verify(fallbackQueue, never()).poll(CONSUMER_ID, null);
  }

  private static SubscriptionCommitContext newCommitContext(
      final int regionId, final int commitId) {
    return new SubscriptionCommitContext(
        1, 1, TOPIC_NAME, CONSUMER_GROUP_ID, commitId, "DataRegion[" + regionId + "]", 0L);
  }

  @SuppressWarnings("unchecked")
  private static void bindQueues(
      final ConsensusSubscriptionBroker broker, final List<ConsensusPrefetchingQueue> queues)
      throws Exception {
    final Field field =
        ConsensusSubscriptionBroker.class.getDeclaredField("topicNameToConsensusPrefetchingQueues");
    field.setAccessible(true);
    final Map<String, List<ConsensusPrefetchingQueue>> queuesByTopic =
        (Map<String, List<ConsensusPrefetchingQueue>>) field.get(broker);
    queuesByTopic.put(TOPIC_NAME, queues);
  }
}
