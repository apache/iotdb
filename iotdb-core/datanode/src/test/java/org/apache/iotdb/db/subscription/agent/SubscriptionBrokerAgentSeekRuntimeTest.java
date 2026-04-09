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

import org.apache.iotdb.db.subscription.broker.ConsensusSubscriptionBroker;
import org.apache.iotdb.db.subscription.task.execution.ConsensusSubscriptionPrefetchExecutorManager;
import org.apache.iotdb.rpc.subscription.config.ConsumerConfig;
import org.apache.iotdb.rpc.subscription.config.ConsumerConstant;
import org.apache.iotdb.rpc.subscription.exception.SubscriptionException;
import org.apache.iotdb.rpc.subscription.payload.poll.TopicProgress;
import org.apache.iotdb.rpc.subscription.payload.request.PipeSubscribeSeekReq;

import org.junit.Test;

import java.lang.reflect.Field;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyShort;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class SubscriptionBrokerAgentSeekRuntimeTest {

  private static final String CONSUMER_GROUP_ID = "cg_seek_runtime_test";
  private static final String TOPIC = "topic_seek_runtime_test";

  @Test
  public void testConsensusSeekApisRejectWhenRuntimeUnavailable() throws Exception {
    ConsensusSubscriptionPrefetchExecutorManager.getInstance().stop();

    final SubscriptionBrokerAgent agent = new SubscriptionBrokerAgent();
    final ConsensusSubscriptionBroker consensusBroker = mock(ConsensusSubscriptionBroker.class);
    when(consensusBroker.hasQueue(TOPIC)).thenReturn(true);
    injectConsensusBroker(agent, consensusBroker);

    assertRuntimeUnavailable(
        () -> agent.seek(createConsumerConfig(), TOPIC, PipeSubscribeSeekReq.SEEK_TO_BEGINNING));
    assertRuntimeUnavailable(
        () ->
            agent.seekToTopicProgress(
                createConsumerConfig(), TOPIC, new TopicProgress(Collections.emptyMap())));
    assertRuntimeUnavailable(
        () ->
            agent.seekAfterTopicProgress(
                createConsumerConfig(), TOPIC, new TopicProgress(Collections.emptyMap())));

    verify(consensusBroker, never()).seek(eq(TOPIC), anyShort());
    verify(consensusBroker, never()).seek(eq(TOPIC), any(TopicProgress.class));
    verify(consensusBroker, never()).seekAfter(eq(TOPIC), any(TopicProgress.class));
  }

  private static void assertRuntimeUnavailable(final Runnable action) {
    try {
      action.run();
      fail("expected consensus seek to fail when runtime is unavailable");
    } catch (final SubscriptionException e) {
      assertTrue(e.getMessage().contains("runtime is stopped"));
    }
  }

  private static ConsumerConfig createConsumerConfig() {
    final Map<String, String> attributes = new HashMap<>();
    attributes.put(ConsumerConstant.CONSUMER_ID_KEY, "consumer-seek-runtime");
    attributes.put(ConsumerConstant.CONSUMER_GROUP_ID_KEY, CONSUMER_GROUP_ID);
    return new ConsumerConfig(attributes);
  }

  @SuppressWarnings("unchecked")
  private static void injectConsensusBroker(
      final SubscriptionBrokerAgent agent, final ConsensusSubscriptionBroker broker)
      throws Exception {
    final Field field =
        SubscriptionBrokerAgent.class.getDeclaredField("consumerGroupIdToConsensusBroker");
    field.setAccessible(true);
    ((Map<String, ConsensusSubscriptionBroker>) field.get(agent)).put(CONSUMER_GROUP_ID, broker);
  }
}
