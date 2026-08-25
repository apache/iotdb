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
import org.apache.iotdb.rpc.subscription.config.TopicConfig;
import org.apache.iotdb.rpc.subscription.exception.SubscriptionException;
import org.apache.iotdb.rpc.subscription.payload.poll.SubscriptionCommitContext;
import org.apache.iotdb.rpc.subscription.payload.poll.SubscriptionPollResponse;
import org.apache.iotdb.rpc.subscription.payload.poll.SubscriptionPollResponseType;
import org.apache.iotdb.rpc.subscription.payload.poll.TabletsPayload;
import org.apache.iotdb.rpc.subscription.payload.poll.TopicProgress;
import org.apache.iotdb.rpc.subscription.payload.response.PipeSubscribeHeartbeatResp;
import org.apache.iotdb.session.AbstractSessionBuilder;
import org.apache.iotdb.session.subscription.SubscriptionTreeSessionBuilder;
import org.apache.iotdb.session.subscription.payload.SubscriptionMessage;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class SubscriptionConsumerMultiProviderPollTest {

  private static final String HOST = "127.0.0.1";
  private static final int FIRST_PORT = 10_001;
  private static final String TOPIC = "topic1";
  private static final String CONSUMER_ID = "test_consumer";
  private static final String CONSUMER_GROUP_ID = "test_consumer_group";
  private static final long LONG_INTERVAL_MS = 86_400_000L;

  @Test
  public void testPollTriesNextProviderBeforeBackingOff() throws SubscriptionException {
    final TestPullConsumer consumer = new TestPullConsumer(2, 0);
    try {
      consumer.open();
      consumer.subscribeTopic();

      final List<SubscriptionMessage> messages = consumer.pollForTest(1_000L);

      Assert.assertEquals(1, messages.size());
      Assert.assertEquals(1, consumer.getPollCount(1));
      Assert.assertEquals(1, consumer.getPollCount(2));
      Assert.assertEquals(0, consumer.getPollCount(3));
      Assert.assertEquals(0, consumer.getBackoffCount());
    } finally {
      consumer.close();
    }
  }

  @Test
  public void testPollBacksOffAfterAllProvidersAreEmpty() throws SubscriptionException {
    final TestPullConsumer consumer = new TestPullConsumer(1, 1);
    try {
      consumer.open();
      consumer.subscribeTopic();

      final List<SubscriptionMessage> messages = consumer.pollForTest(1_000L);

      Assert.assertEquals(1, messages.size());
      Assert.assertEquals(2, consumer.getPollCount(1));
      Assert.assertEquals(1, consumer.getPollCount(2));
      Assert.assertEquals(1, consumer.getPollCount(3));
      Assert.assertEquals(1, consumer.getBackoffCount());
    } finally {
      consumer.close();
    }
  }

  private static class TestPullConsumer extends AbstractSubscriptionPullConsumer {

    private final Map<Integer, Integer> pollCounts = new HashMap<>();
    private final int dataProviderId;
    private final int emptyPollsBeforeData;
    private int backoffCount;

    private TestPullConsumer(final int dataProviderId, final int emptyPollsBeforeData) {
      super(
          new AbstractSubscriptionPullConsumerBuilder()
              .host(HOST)
              .port(FIRST_PORT)
              .consumerId(CONSUMER_ID)
              .consumerGroupId(CONSUMER_GROUP_ID)
              .heartbeatIntervalMs(LONG_INTERVAL_MS)
              .endpointsSyncIntervalMs(LONG_INTERVAL_MS)
              .autoCommit(false));
      this.dataProviderId = dataProviderId;
      this.emptyPollsBeforeData = emptyPollsBeforeData;
    }

    @Override
    protected AbstractSubscriptionProvider constructSubscriptionProvider(
        final TEndPoint endPoint,
        final String username,
        final String password,
        final String encryptedPassword,
        final String consumerId,
        final String consumerGroupId,
        final String ownerId,
        final Long ownerEpoch,
        final int thriftMaxFrameSize,
        final long heartbeatIntervalMs,
        final int connectionTimeoutInMs) {
      return new TestSubscriptionProvider(
          endPoint,
          username,
          password,
          encryptedPassword,
          consumerId,
          consumerGroupId,
          ownerId,
          ownerEpoch,
          thriftMaxFrameSize,
          heartbeatIntervalMs,
          connectionTimeoutInMs,
          pollCounts,
          dataProviderId,
          emptyPollsBeforeData);
    }

    private void subscribeTopic() {
      subscribedTopics = Collections.singletonMap(TOPIC, new TopicConfig());
    }

    private List<SubscriptionMessage> pollForTest(final long timeoutMs)
        throws SubscriptionException {
      return poll(timeoutMs);
    }

    private int getPollCount(final int dataNodeId) {
      return pollCounts.getOrDefault(dataNodeId, 0);
    }

    private int getBackoffCount() {
      return backoffCount;
    }

    @Override
    void sleepAfterEmptyPollRound() {
      backoffCount++;
    }
  }

  private static class TestSubscriptionProvider extends AbstractSubscriptionProvider {

    private final int dataNodeId;
    private final Map<Integer, Integer> pollCounts;
    private final int dataProviderId;
    private final int emptyPollsBeforeData;

    private TestSubscriptionProvider(
        final TEndPoint endPoint,
        final String username,
        final String password,
        final String encryptedPassword,
        final String consumerId,
        final String consumerGroupId,
        final String ownerId,
        final Long ownerEpoch,
        final int thriftMaxFrameSize,
        final long heartbeatIntervalMs,
        final int connectionTimeoutInMs,
        final Map<Integer, Integer> pollCounts,
        final int dataProviderId,
        final int emptyPollsBeforeData) {
      super(
          endPoint,
          username,
          password,
          encryptedPassword,
          consumerId,
          consumerGroupId,
          ownerId,
          ownerEpoch,
          thriftMaxFrameSize,
          heartbeatIntervalMs,
          connectionTimeoutInMs);
      this.dataNodeId = endPoint.port - FIRST_PORT + 1;
      this.pollCounts = pollCounts;
      this.dataProviderId = dataProviderId;
      this.emptyPollsBeforeData = emptyPollsBeforeData;
    }

    @Override
    protected AbstractSessionBuilder constructSubscriptionSessionBuilder(
        final String host,
        final int port,
        final String username,
        final String password,
        final String encryptedPassword,
        final int thriftMaxFrameSize,
        final int connectionTimeoutInMs) {
      final boolean useEncryptedPassword = Objects.nonNull(encryptedPassword);
      return new SubscriptionTreeSessionBuilder()
          .host(host)
          .port(port)
          .username(username)
          .password(useEncryptedPassword ? encryptedPassword : password)
          .useEncryptedPassword(useEncryptedPassword)
          .thriftMaxFrameSize(thriftMaxFrameSize)
          .connectionTimeoutInMs(connectionTimeoutInMs);
    }

    @Override
    synchronized void handshake() {
      setAvailable();
    }

    @Override
    synchronized void close() {
      setUnavailable();
    }

    @Override
    int getDataNodeId() {
      return dataNodeId;
    }

    @Override
    PipeSubscribeHeartbeatResp heartbeat(
        final List<SubscriptionCommitContext> processorBufferedCommitContexts) {
      final PipeSubscribeHeartbeatResp response = new PipeSubscribeHeartbeatResp();
      response.getTopics().put(TOPIC, new TopicConfig());
      response.getEndPoints().put(1, new TEndPoint(HOST, FIRST_PORT));
      response.getEndPoints().put(2, new TEndPoint(HOST, FIRST_PORT + 1));
      response.getEndPoints().put(3, new TEndPoint(HOST, FIRST_PORT + 2));
      return response;
    }

    @Override
    List<SubscriptionPollResponse> poll(
        final Set<String> topicNames,
        final long timeoutMs,
        final Map<String, TopicProgress> progressByTopic)
        throws SubscriptionException {
      pollCounts.merge(dataNodeId, 1, Integer::sum);
      if (dataNodeId != dataProviderId || pollCounts.get(dataNodeId) <= emptyPollsBeforeData) {
        return Collections.emptyList();
      }
      final SubscriptionCommitContext commitContext =
          new SubscriptionCommitContext(dataNodeId, 0, TOPIC, CONSUMER_GROUP_ID, 0L);
      final List<IMeasurementSchema> schemas =
          Collections.singletonList(new MeasurementSchema("s1", TSDataType.INT64));
      final Tablet tablet = new Tablet("root.sg.d1", schemas, 1);
      tablet.setTimestamps(new long[] {1L});
      ((long[]) tablet.getValues()[0])[0] = 1L;
      tablet.setRowSize(1);
      return Collections.singletonList(
          new SubscriptionPollResponse(
              SubscriptionPollResponseType.TABLETS.getType(),
              new TabletsPayload(Collections.singletonList(tablet), -1),
              commitContext));
    }
  }
}
