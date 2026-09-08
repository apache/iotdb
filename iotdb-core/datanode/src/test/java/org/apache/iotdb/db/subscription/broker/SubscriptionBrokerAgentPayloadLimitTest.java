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

import org.apache.iotdb.db.subscription.agent.SubscriptionBrokerAgent;
import org.apache.iotdb.db.subscription.event.SubscriptionEvent;
import org.apache.iotdb.rpc.subscription.config.ConsumerConfig;
import org.apache.iotdb.rpc.subscription.config.ConsumerConstant;
import org.apache.iotdb.rpc.subscription.payload.poll.SubscriptionCommitContext;
import org.apache.iotdb.rpc.subscription.payload.poll.SubscriptionPollResponseType;
import org.apache.iotdb.rpc.subscription.payload.poll.TerminationPayload;

import org.junit.Test;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class SubscriptionBrokerAgentPayloadLimitTest {

  private static final String CONSUMER_GROUP_ID = "consumerGroup";
  private static final String CONSUMER_ID = "consumer";
  private static final String TOPIC_NAME = "topic";

  @Test
  public void testPollRequeuesFirstEventFromNextBrokerWhenRemainingBudgetIsInsufficient()
      throws Exception {
    final SubscriptionBrokerAgent agent = new SubscriptionBrokerAgent();
    final ISubscriptionBroker firstBroker = mock(ISubscriptionBroker.class);
    final ISubscriptionBroker secondBroker = mock(ISubscriptionBroker.class);
    final SubscriptionEvent firstEvent = newEvent(1);
    final SubscriptionEvent secondEvent = newEvent(2);
    final long firstEventSize = firstEvent.getCurrentResponseSize();
    final long secondEventSize = secondEvent.getCurrentResponseSize();
    final long maxBytes = firstEventSize + secondEventSize - 1L;
    final Set<String> topicNames = Collections.singleton(TOPIC_NAME);

    when(firstBroker.poll(CONSUMER_ID, topicNames, maxBytes, Collections.emptyMap()))
        .thenReturn(Collections.singletonList(firstEvent));
    when(secondBroker.poll(
            CONSUMER_ID, topicNames, maxBytes - firstEventSize, Collections.emptyMap()))
        .thenReturn(Collections.singletonList(secondEvent));
    when(secondBroker.requeue(CONSUMER_ID, secondEvent.getCommitContext())).thenReturn(true);
    bindBrokers(agent, firstBroker, secondBroker);

    final List<SubscriptionEvent> events = agent.poll(createConsumerConfig(), topicNames, maxBytes);

    assertEquals(1, events.size());
    assertSame(firstEvent, events.get(0));
    verify(secondBroker).requeue(CONSUMER_ID, secondEvent.getCommitContext());
    assertEquals(0L, secondEvent.getNackCount());
  }

  private static SubscriptionEvent newEvent(final int commitId) {
    return new SubscriptionEvent(
        SubscriptionPollResponseType.TERMINATION.getType(),
        new TerminationPayload(),
        new SubscriptionCommitContext(1, 1, TOPIC_NAME, CONSUMER_GROUP_ID, commitId));
  }

  private static ConsumerConfig createConsumerConfig() {
    final Map<String, String> attributes = new HashMap<>();
    attributes.put(ConsumerConstant.CONSUMER_ID_KEY, CONSUMER_ID);
    attributes.put(ConsumerConstant.CONSUMER_GROUP_ID_KEY, CONSUMER_GROUP_ID);
    return new ConsumerConfig(attributes);
  }

  @SuppressWarnings("unchecked")
  private static void bindBrokers(
      final SubscriptionBrokerAgent agent, final ISubscriptionBroker... brokers) throws Exception {
    final Field field = SubscriptionBrokerAgent.class.getDeclaredField("consumerGroupIdToBrokers");
    field.setAccessible(true);
    final Map<String, List<ISubscriptionBroker>> brokersByConsumerGroup =
        (Map<String, List<ISubscriptionBroker>>) field.get(agent);
    brokersByConsumerGroup.put(CONSUMER_GROUP_ID, new ArrayList<>(List.of(brokers)));
  }
}
