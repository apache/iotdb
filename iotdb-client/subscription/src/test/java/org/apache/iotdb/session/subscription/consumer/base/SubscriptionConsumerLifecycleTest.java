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
import org.apache.iotdb.rpc.subscription.exception.SubscriptionException;
import org.apache.iotdb.rpc.subscription.payload.poll.SubscriptionCommitContext;
import org.apache.iotdb.rpc.subscription.payload.response.PipeSubscribeHeartbeatResp;
import org.apache.iotdb.session.AbstractSessionBuilder;
import org.apache.iotdb.session.subscription.SubscriptionTreeSessionBuilder;

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.BooleanSupplier;

public class SubscriptionConsumerLifecycleTest {

  private static final String HOST = "127.0.0.1";
  private static final int PORT = 6667;
  private static final String CONSUMER_ID = "test_consumer";
  private static final String CONSUMER_GROUP_ID = "test_consumer_group";
  private static final long LONG_INTERVAL_MS = 86_400_000L;

  @Test
  public void testPushConsumerIsOpenBeforeProviderHandshake() throws SubscriptionException {
    final TestPushConsumer consumer = new TestPushConsumer();

    try {
      consumer.open();

      Assert.assertEquals(1, consumer.closedStatesDuringHandshake.size());
      Assert.assertFalse(consumer.closedStatesDuringHandshake.get(0));
    } finally {
      consumer.close();
    }
  }

  @Test
  public void testPushConsumerIsClosedBeforeProviderClose() throws SubscriptionException {
    final TestPushConsumer consumer = new TestPushConsumer();

    consumer.open();
    consumer.close();

    Assert.assertEquals(1, consumer.closedStatesDuringClose.size());
    Assert.assertTrue(consumer.closedStatesDuringClose.get(0));
  }

  @Test
  public void testPullConsumerIsOpenBeforeProviderHandshake() throws SubscriptionException {
    final TestPullConsumer consumer = new TestPullConsumer();

    try {
      consumer.open();

      Assert.assertEquals(1, consumer.closedStatesDuringHandshake.size());
      Assert.assertFalse(consumer.closedStatesDuringHandshake.get(0));
    } finally {
      consumer.close();
    }
  }

  @Test
  public void testPullConsumerIsClosedBeforeProviderClose() throws SubscriptionException {
    final TestPullConsumer consumer = new TestPullConsumer();

    consumer.open();
    consumer.close();

    Assert.assertEquals(1, consumer.closedStatesDuringClose.size());
    Assert.assertTrue(consumer.closedStatesDuringClose.get(0));
  }

  private static class TestPushConsumer extends AbstractSubscriptionPushConsumer {

    private final List<Boolean> closedStatesDuringHandshake = new ArrayList<>();
    private final List<Boolean> closedStatesDuringClose = new ArrayList<>();

    private TestPushConsumer() {
      super(
          new AbstractSubscriptionPushConsumerBuilder()
              .host(HOST)
              .port(PORT)
              .consumerId(CONSUMER_ID)
              .consumerGroupId(CONSUMER_GROUP_ID)
              .heartbeatIntervalMs(LONG_INTERVAL_MS)
              .endpointsSyncIntervalMs(LONG_INTERVAL_MS)
              .autoPollIntervalMs(LONG_INTERVAL_MS));
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
          this::isClosed,
          closedStatesDuringHandshake,
          closedStatesDuringClose);
    }
  }

  private static class TestPullConsumer extends AbstractSubscriptionPullConsumer {

    private final List<Boolean> closedStatesDuringHandshake = new ArrayList<>();
    private final List<Boolean> closedStatesDuringClose = new ArrayList<>();

    private TestPullConsumer() {
      super(
          new AbstractSubscriptionPullConsumerBuilder()
              .host(HOST)
              .port(PORT)
              .consumerId(CONSUMER_ID)
              .consumerGroupId(CONSUMER_GROUP_ID)
              .heartbeatIntervalMs(LONG_INTERVAL_MS)
              .endpointsSyncIntervalMs(LONG_INTERVAL_MS)
              .autoCommit(false));
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
          this::isClosed,
          closedStatesDuringHandshake,
          closedStatesDuringClose);
    }
  }

  private static class TestSubscriptionProvider extends AbstractSubscriptionProvider {

    private final BooleanSupplier consumerClosedSupplier;
    private final List<Boolean> closedStatesDuringHandshake;
    private final List<Boolean> closedStatesDuringClose;

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
        final BooleanSupplier consumerClosedSupplier,
        final List<Boolean> closedStatesDuringHandshake,
        final List<Boolean> closedStatesDuringClose) {
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
      this.consumerClosedSupplier = consumerClosedSupplier;
      this.closedStatesDuringHandshake = closedStatesDuringHandshake;
      this.closedStatesDuringClose = closedStatesDuringClose;
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
      closedStatesDuringHandshake.add(consumerClosedSupplier.getAsBoolean());
      setAvailable();
    }

    @Override
    synchronized void close() {
      closedStatesDuringClose.add(consumerClosedSupplier.getAsBoolean());
      setUnavailable();
    }

    @Override
    PipeSubscribeHeartbeatResp heartbeat(
        final List<SubscriptionCommitContext> processorBufferedCommitContexts) {
      return new PipeSubscribeHeartbeatResp();
    }
  }
}
