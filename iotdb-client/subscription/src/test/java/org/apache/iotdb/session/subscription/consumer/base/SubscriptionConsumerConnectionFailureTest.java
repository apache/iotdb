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
import org.apache.iotdb.rpc.subscription.exception.SubscriptionConnectionException;
import org.apache.iotdb.rpc.subscription.exception.SubscriptionException;
import org.apache.iotdb.rpc.subscription.payload.poll.SubscriptionCommitContext;
import org.apache.iotdb.rpc.subscription.payload.response.PipeSubscribeHeartbeatResp;
import org.apache.iotdb.session.AbstractSessionBuilder;
import org.apache.iotdb.session.subscription.SubscriptionTreeSessionBuilder;

import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

public class SubscriptionConsumerConnectionFailureTest {

  private static final String HOST = "127.0.0.1";
  private static final int FIRST_PORT = 10_011;
  private static final String PASSWORD = "test-password";

  @Test
  public void testOpenReportsAllInitialEndpointHandshakeFailures() {
    final TestPullConsumer consumer = new TestPullConsumer();

    final SubscriptionConnectionException exception =
        Assert.assertThrows(SubscriptionConnectionException.class, consumer::open);

    Assert.assertTrue(exception.getMessage().contains(HOST));
    Assert.assertTrue(exception.getMessage().contains(String.valueOf(FIRST_PORT)));
    Assert.assertTrue(exception.getMessage().contains(String.valueOf(FIRST_PORT + 1)));
    Assert.assertTrue(exception.getMessage().contains("first endpoint handshake failed"));
    Assert.assertTrue(exception.getMessage().contains("second endpoint handshake failed"));
    Assert.assertFalse(exception.getMessage().contains(PASSWORD));
    Assert.assertNotNull(exception.getCause());
    Assert.assertEquals(1, exception.getSuppressed().length);
  }

  private static class TestPullConsumer extends AbstractSubscriptionPullConsumer {

    private TestPullConsumer() {
      super(
          new AbstractSubscriptionPullConsumerBuilder()
              .nodeUrls(Arrays.asList(HOST + ":" + FIRST_PORT, HOST + ":" + (FIRST_PORT + 1)))
              .username("test-user")
              .password(PASSWORD));
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
          connectionTimeoutInMs);
    }
  }

  private static class TestSubscriptionProvider extends AbstractSubscriptionProvider {

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
        final int connectionTimeoutInMs) {
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
    synchronized void handshake() throws SubscriptionException {
      throw new SubscriptionConnectionException(
          (getEndPoint().port == FIRST_PORT
                  ? "first endpoint handshake failed"
                  : "second endpoint handshake failed")
              + " with password "
              + PASSWORD);
    }

    @Override
    synchronized void close() {
      setUnavailable();
    }

    @Override
    PipeSubscribeHeartbeatResp heartbeat(
        final List<SubscriptionCommitContext> processorBufferedCommitContexts) {
      return new PipeSubscribeHeartbeatResp();
    }
  }
}
