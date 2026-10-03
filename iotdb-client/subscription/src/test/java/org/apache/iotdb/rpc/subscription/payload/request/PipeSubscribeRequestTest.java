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

package org.apache.iotdb.rpc.subscription.payload.request;

import org.apache.iotdb.rpc.subscription.config.ConsumerConfig;
import org.apache.iotdb.rpc.subscription.config.ConsumerConstant;
import org.apache.iotdb.rpc.subscription.payload.poll.PollFilePayload;
import org.apache.iotdb.rpc.subscription.payload.poll.PollPayload;
import org.apache.iotdb.rpc.subscription.payload.poll.PollTabletsPayload;
import org.apache.iotdb.rpc.subscription.payload.poll.SubscriptionCommitContext;
import org.apache.iotdb.rpc.subscription.payload.poll.SubscriptionPollRequest;
import org.apache.iotdb.rpc.subscription.payload.poll.SubscriptionPollRequestType;
import org.apache.iotdb.service.rpc.thrift.TPipeSubscribeReq;

import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class PipeSubscribeRequestTest {

  private static final SubscriptionCommitContext COMMIT_CONTEXT =
      new SubscriptionCommitContext(1, 2, "topic", "group", 3L);

  @Test
  public void testHandshakeRoundTripAndEmptyBody() throws Exception {
    final Map<String, String> attributes = new HashMap<>();
    attributes.put(ConsumerConstant.CONSUMER_ID_KEY, "consumer");
    attributes.put(ConsumerConstant.CONSUMER_GROUP_ID_KEY, "group");
    final PipeSubscribeHandshakeReq request =
        PipeSubscribeHandshakeReq.toTPipeSubscribeReq(new ConsumerConfig(attributes));

    final PipeSubscribeHandshakeReq parsed =
        PipeSubscribeHandshakeReq.fromTPipeSubscribeReq(request);
    assertEquals("consumer", parsed.getConsumerConfig().getConsumerId());
    assertEquals("group", parsed.getConsumerConfig().getConsumerGroupId());
    assertEquals(PipeSubscribeRequestType.HANDSHAKE.getType(), parsed.getType());

    final TPipeSubscribeReq empty = copyWithoutBody(request);
    assertTrue(
        PipeSubscribeHandshakeReq.fromTPipeSubscribeReq(empty)
            .getConsumerConfig()
            .getAttribute()
            .isEmpty());
  }

  @Test
  public void testSubscribeAndUnsubscribeRoundTripAndEmptyBody() throws Exception {
    final Set<String> topics = new HashSet<>(Arrays.asList("topic1", "topic2"));

    final PipeSubscribeSubscribeReq subscribe =
        PipeSubscribeSubscribeReq.toTPipeSubscribeReq(topics);
    assertEquals(
        topics, PipeSubscribeSubscribeReq.fromTPipeSubscribeReq(subscribe).getTopicNames());
    assertEquals(PipeSubscribeRequestType.SUBSCRIBE.getType(), subscribe.getType());
    assertTrue(
        PipeSubscribeSubscribeReq.fromTPipeSubscribeReq(copyWithoutBody(subscribe))
            .getTopicNames()
            .isEmpty());

    final PipeSubscribeUnsubscribeReq unsubscribe =
        PipeSubscribeUnsubscribeReq.toTPipeSubscribeReq(topics);
    assertEquals(
        topics, PipeSubscribeUnsubscribeReq.fromTPipeSubscribeReq(unsubscribe).getTopicNames());
    assertEquals(PipeSubscribeRequestType.UNSUBSCRIBE.getType(), unsubscribe.getType());
    assertTrue(
        PipeSubscribeUnsubscribeReq.fromTPipeSubscribeReq(copyWithoutBody(unsubscribe))
            .getTopicNames()
            .isEmpty());
  }

  @Test
  public void testCommitRoundTripForAckNackAndEmptyBody() throws Exception {
    for (final boolean nack : Arrays.asList(false, true)) {
      final PipeSubscribeCommitReq request =
          PipeSubscribeCommitReq.toTPipeSubscribeReq(
              Collections.singletonList(COMMIT_CONTEXT), nack);
      final PipeSubscribeCommitReq parsed = PipeSubscribeCommitReq.fromTPipeSubscribeReq(request);
      assertEquals(Collections.singletonList(COMMIT_CONTEXT), parsed.getCommitContexts());
      assertEquals(nack, parsed.isNack());
      assertEquals(PipeSubscribeRequestType.COMMIT.getType(), parsed.getType());
    }

    final PipeSubscribeCommitReq empty =
        PipeSubscribeCommitReq.fromTPipeSubscribeReq(
            copyWithoutBody(
                PipeSubscribeCommitReq.toTPipeSubscribeReq(Collections.emptyList(), false)));
    assertTrue(empty.getCommitContexts().isEmpty());
    assertFalse(empty.isNack());
  }

  @Test
  public void testAllPollRequestPayloadsRoundTrip() throws Exception {
    assertPollRoundTrip(
        new SubscriptionPollRequest(
            SubscriptionPollRequestType.POLL.getType(),
            new PollPayload(new HashSet<>(Arrays.asList("topic1", "topic2"))),
            100L,
            1024L));
    assertPollRoundTrip(
        new SubscriptionPollRequest(
            SubscriptionPollRequestType.POLL_FILE.getType(),
            new PollFilePayload(COMMIT_CONTEXT, 99L),
            200L,
            2048L));
    assertPollRoundTrip(
        new SubscriptionPollRequest(
            SubscriptionPollRequestType.POLL_TABLETS.getType(),
            new PollTabletsPayload(COMMIT_CONTEXT, 7),
            300L,
            4096L));
  }

  @Test
  public void testPollRequestWithEmptyBodyAndCloseRoundTrip() throws Exception {
    final SubscriptionPollRequest pollRequest =
        new SubscriptionPollRequest(
            SubscriptionPollRequestType.POLL.getType(),
            new PollPayload(Collections.singleton("topic")),
            100L,
            1024L);
    final PipeSubscribePollReq request = PipeSubscribePollReq.toTPipeSubscribeReq(pollRequest);
    assertNull(PipeSubscribePollReq.fromTPipeSubscribeReq(copyWithoutBody(request)).getRequest());

    final PipeSubscribeCloseReq close = PipeSubscribeCloseReq.toTPipeSubscribeReq();
    assertEquals(PipeSubscribeRequestType.CLOSE.getType(), close.getType());
    assertEquals(close, PipeSubscribeCloseReq.fromTPipeSubscribeReq(close));
  }

  private static void assertPollRoundTrip(final SubscriptionPollRequest pollRequest)
      throws Exception {
    final PipeSubscribePollReq request = PipeSubscribePollReq.toTPipeSubscribeReq(pollRequest);
    final SubscriptionPollRequest parsed =
        PipeSubscribePollReq.fromTPipeSubscribeReq(request).getRequest();
    assertEquals(pollRequest.getRequestType(), parsed.getRequestType());
    assertEquals(pollRequest.getPayload(), parsed.getPayload());
    assertEquals(pollRequest.getTimeoutMs(), parsed.getTimeoutMs());
    assertEquals(pollRequest.getMaxBytes(), parsed.getMaxBytes());
  }

  private static TPipeSubscribeReq copyWithoutBody(final TPipeSubscribeReq request) {
    return new TPipeSubscribeReq(request.getVersion(), request.getType());
  }
}
