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

package org.apache.iotdb.rpc.subscription.payload.response;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.rpc.subscription.config.TopicConfig;
import org.apache.iotdb.rpc.subscription.payload.poll.ErrorPayload;
import org.apache.iotdb.rpc.subscription.payload.poll.RegionProgress;
import org.apache.iotdb.rpc.subscription.payload.poll.SubscriptionCommitContext;
import org.apache.iotdb.rpc.subscription.payload.poll.SubscriptionPollResponse;
import org.apache.iotdb.rpc.subscription.payload.poll.SubscriptionPollResponseType;
import org.apache.iotdb.rpc.subscription.payload.poll.TabletsPayload;
import org.apache.iotdb.rpc.subscription.payload.poll.TopicProgress;
import org.apache.iotdb.rpc.subscription.payload.poll.WriterId;
import org.apache.iotdb.rpc.subscription.payload.poll.WriterProgress;
import org.apache.iotdb.service.rpc.thrift.TPipeSubscribeResp;

import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class PipeSubscribeResponseTest {

  private static final TSStatus SUCCESS = new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
  private static final SubscriptionCommitContext COMMIT_CONTEXT =
      new SubscriptionCommitContext(1, 2, "topic", "group", 3L);

  @Test
  public void testHandshakeRoundTripAndSparseBody() {
    final PipeSubscribeHandshakeResp response =
        PipeSubscribeHandshakeResp.toTPipeSubscribeResp(SUCCESS, 7, "consumer", "consumer-group");
    final PipeSubscribeHandshakeResp parsed =
        PipeSubscribeHandshakeResp.fromTPipeSubscribeResp(response);
    assertEquals(7, parsed.getDataNodeId());
    assertEquals("consumer", parsed.getConsumerId());
    assertEquals("consumer-group", parsed.getConsumerGroupId());

    final TPipeSubscribeResp sparse =
        copyWithBody(response, Arrays.asList(null, ByteBuffer.allocate(0)));
    assertEquals(0, PipeSubscribeHandshakeResp.fromTPipeSubscribeResp(sparse).getDataNodeId());
  }

  @Test
  public void testSubscribeAndUnsubscribeRoundTripAndStatusOnly() throws Exception {
    final Map<String, TopicConfig> topics = new HashMap<>();
    topics.put("topic", new TopicConfig(Collections.singletonMap("mode", "live")));

    final PipeSubscribeSubscribeResp subscribe =
        PipeSubscribeSubscribeResp.toTPipeSubscribeResp(SUCCESS, topics);
    assertEquals(topics, PipeSubscribeSubscribeResp.fromTPipeSubscribeResp(subscribe).getTopics());
    assertTrue(
        PipeSubscribeSubscribeResp.fromTPipeSubscribeResp(
                PipeSubscribeSubscribeResp.toTPipeSubscribeResp(SUCCESS))
            .getTopics()
            .isEmpty());

    final PipeSubscribeUnsubscribeResp unsubscribe =
        PipeSubscribeUnsubscribeResp.toTPipeSubscribeResp(SUCCESS, topics);
    assertEquals(
        topics, PipeSubscribeUnsubscribeResp.fromTPipeSubscribeResp(unsubscribe).getTopics());
    assertTrue(
        PipeSubscribeUnsubscribeResp.fromTPipeSubscribeResp(
                PipeSubscribeUnsubscribeResp.toTPipeSubscribeResp(SUCCESS))
            .getTopics()
            .isEmpty());
  }

  @Test
  public void testCommitRoundTripAndStatusOnly() throws Exception {
    final WriterId writerId = new WriterId("1_100", 7);
    final WriterProgress writerProgress = new WriterProgress(100L, 11L);
    final TopicProgress progress =
        new TopicProgress(
            Collections.singletonMap(
                "1_100", new RegionProgress(Collections.singletonMap(writerId, writerProgress))));
    final Map<String, TopicProgress> progressByTopic = new LinkedHashMap<>();
    progressByTopic.put("topic", progress);

    final PipeSubscribeCommitResp response =
        PipeSubscribeCommitResp.toTPipeSubscribeResp(
            SUCCESS, Collections.singletonList(COMMIT_CONTEXT), progressByTopic);
    final PipeSubscribeCommitResp parsed = PipeSubscribeCommitResp.fromTPipeSubscribeResp(response);
    assertEquals(Collections.singletonList(COMMIT_CONTEXT), parsed.getAcceptedCommitContexts());
    assertEquals(progressByTopic, parsed.getCommittedProgressByTopic());

    final PipeSubscribeCommitResp statusOnly =
        PipeSubscribeCommitResp.fromTPipeSubscribeResp(
            PipeSubscribeCommitResp.toTPipeSubscribeResp(SUCCESS));
    assertTrue(statusOnly.getAcceptedCommitContexts().isEmpty());
    assertTrue(statusOnly.getCommittedProgressByTopic().isEmpty());
  }

  @Test
  public void testPollRoundTripSkipsNullAndEmptyBuffers() throws Exception {
    final SubscriptionPollResponse first =
        new SubscriptionPollResponse(
            SubscriptionPollResponseType.TABLETS.getType(),
            new TabletsPayload(Collections.emptyMap(), 0),
            COMMIT_CONTEXT);
    final SubscriptionPollResponse second =
        new SubscriptionPollResponse(
            SubscriptionPollResponseType.ERROR.getType(),
            new ErrorPayload("retry", false),
            COMMIT_CONTEXT);
    final List<ByteBuffer> buffers =
        Arrays.asList(
            null,
            ByteBuffer.allocate(0),
            SubscriptionPollResponse.serialize(first),
            SubscriptionPollResponse.serialize(second));

    final PipeSubscribePollResp parsed =
        PipeSubscribePollResp.fromTPipeSubscribeResp(
            PipeSubscribePollResp.toTPipeSubscribeResp(SUCCESS, buffers));
    assertEquals(2, parsed.getResponses().size());
    assertEquals(first.getResponseType(), parsed.getResponses().get(0).getResponseType());
    assertEquals(second.getPayload(), parsed.getResponses().get(1).getPayload());
  }

  @Test
  public void testCloseAndSeekRoundTrip() {
    final PipeSubscribeCloseResp close = PipeSubscribeCloseResp.toTPipeSubscribeResp(SUCCESS);
    assertEquals(close, PipeSubscribeCloseResp.fromTPipeSubscribeResp(close));

    final PipeSubscribeSeekResp seek = PipeSubscribeSeekResp.toTPipeSubscribeResp(SUCCESS);
    assertEquals(seek, PipeSubscribeSeekResp.fromTPipeSubscribeResp(seek));
  }

  private static TPipeSubscribeResp copyWithBody(
      final TPipeSubscribeResp response, final List<ByteBuffer> body) {
    return new TPipeSubscribeResp(response.getStatus(), response.getVersion(), response.getType())
        .setBody(body);
  }
}
