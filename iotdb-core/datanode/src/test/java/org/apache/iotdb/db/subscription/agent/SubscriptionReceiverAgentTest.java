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

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.db.subscription.receiver.SubscriptionReceiver;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.rpc.subscription.config.ConsumerConfig;
import org.apache.iotdb.rpc.subscription.config.ConsumerConstant;
import org.apache.iotdb.rpc.subscription.payload.request.PipeSubscribeCloseReq;
import org.apache.iotdb.rpc.subscription.payload.request.PipeSubscribeHandshakeReq;
import org.apache.iotdb.rpc.subscription.payload.request.PipeSubscribeRequestType;
import org.apache.iotdb.rpc.subscription.payload.request.PipeSubscribeRequestVersion;
import org.apache.iotdb.rpc.subscription.payload.response.PipeSubscribeResponseType;
import org.apache.iotdb.rpc.subscription.payload.response.PipeSubscribeResponseVersion;
import org.apache.iotdb.service.rpc.thrift.TPipeSubscribeReq;
import org.apache.iotdb.service.rpc.thrift.TPipeSubscribeResp;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

public class SubscriptionReceiverAgentTest {

  @Test
  public void testDisconnectedReceiverIsRetainedUntilTimeout() throws IOException {
    final CopyOnWriteArrayList<FakeSubscriptionReceiver> receivers = new CopyOnWriteArrayList<>();
    final SubscriptionReceiverAgent agent = createAgent(receivers, true /* closeOnTimeout */);
    final TPipeSubscribeReq handshake = createHandshakeRequest("group", "consumer");

    Assert.assertEquals(
        TSStatusCode.SUCCESS_STATUS.getStatusCode(),
        agent.handle(handshake, "root").getStatus().getCode());
    agent.handleClientExit();

    final FakeSubscriptionReceiver receiver = receivers.get(0);
    Assert.assertEquals(1, receiver.exitCount.get());
    Assert.assertEquals(0, receiver.timeoutCount.get());
    agent.checkReceiverTimeouts();

    Assert.assertEquals(1, receiver.timeoutCount.get());
    Assert.assertFalse(receiver.hasActiveConsumer());
    agent.checkReceiverTimeouts();
    Assert.assertEquals(1, receiver.timeoutCount.get());
  }

  @Test
  public void testReconnectInvalidatesOldReceiverBeforeTimeoutCleanup() throws IOException {
    final CopyOnWriteArrayList<FakeSubscriptionReceiver> receivers = new CopyOnWriteArrayList<>();
    final SubscriptionReceiverAgent agent = createAgent(receivers, false /* closeOnTimeout */);
    final TPipeSubscribeReq handshake = createHandshakeRequest("group", "consumer");

    agent.handle(handshake, "root");
    agent.handleClientExit();
    agent.handle(handshake, "root");

    final FakeSubscriptionReceiver oldReceiver = receivers.get(0);
    final FakeSubscriptionReceiver newReceiver = receivers.get(1);
    Assert.assertTrue(oldReceiver.invalidated);
    Assert.assertFalse(newReceiver.invalidated);

    agent.checkReceiverTimeouts();

    Assert.assertEquals(0, oldReceiver.timeoutCount.get());
    Assert.assertEquals(1, newReceiver.timeoutCount.get());
  }

  @Test
  public void testLateExitFromOldConnectionKeepsNewReceiverRegistered() throws Exception {
    final CopyOnWriteArrayList<FakeSubscriptionReceiver> receivers = new CopyOnWriteArrayList<>();
    final SubscriptionReceiverAgent agent = createAgent(receivers, false /* closeOnTimeout */);
    final CountDownLatch oldHandshakeCompleted = new CountDownLatch(1);
    final CountDownLatch allowOldConnectionToExit = new CountDownLatch(1);
    final AtomicReference<Throwable> threadFailure = new AtomicReference<>();

    final Thread oldConnection =
        new Thread(
            () -> {
              try {
                agent.handle(createHandshakeRequest("group", "consumer"), "root");
                oldHandshakeCompleted.countDown();
                allowOldConnectionToExit.await();
                agent.handleClientExit();
              } catch (final Throwable t) {
                threadFailure.set(t);
                oldHandshakeCompleted.countDown();
              }
            });
    oldConnection.start();
    final Thread newConnection =
        new Thread(
            () -> {
              try {
                agent.handle(createHandshakeRequest("group", "consumer"), "root");
              } catch (final Throwable t) {
                threadFailure.set(t);
              }
            });
    try {
      Assert.assertTrue(oldHandshakeCompleted.await(10, TimeUnit.SECONDS));
      newConnection.start();
      newConnection.join(TimeUnit.SECONDS.toMillis(10));
      Assert.assertFalse(newConnection.isAlive());
    } finally {
      allowOldConnectionToExit.countDown();
      oldConnection.join(TimeUnit.SECONDS.toMillis(10));
    }
    Assert.assertFalse(oldConnection.isAlive());

    if (threadFailure.get() != null) {
      throw new AssertionError(threadFailure.get());
    }
    final FakeSubscriptionReceiver oldReceiver = receivers.get(0);
    final FakeSubscriptionReceiver newReceiver = receivers.get(1);
    agent.checkReceiverTimeouts();

    Assert.assertTrue(oldReceiver.invalidated);
    Assert.assertEquals(0, oldReceiver.timeoutCount.get());
    Assert.assertEquals(1, newReceiver.timeoutCount.get());
  }

  @Test
  public void testSuccessfulCloseRemovesReceiverFromTimeoutRegistry() throws IOException {
    final CopyOnWriteArrayList<FakeSubscriptionReceiver> receivers = new CopyOnWriteArrayList<>();
    final SubscriptionReceiverAgent agent = createAgent(receivers, true /* closeOnTimeout */);
    final TPipeSubscribeReq handshake = createHandshakeRequest("group", "consumer");

    agent.handle(handshake, "root");
    final TPipeSubscribeResp closeResponse =
        agent.handle(PipeSubscribeCloseReq.toTPipeSubscribeReq(), "root");

    Assert.assertEquals(
        TSStatusCode.SUCCESS_STATUS.getStatusCode(), closeResponse.getStatus().getCode());
    final FakeSubscriptionReceiver receiver = receivers.get(0);
    Assert.assertEquals(0, receiver.timeoutCount.get());
    agent.checkReceiverTimeouts();
    Assert.assertEquals(0, receiver.timeoutCount.get());
  }

  private SubscriptionReceiverAgent createAgent(
      final CopyOnWriteArrayList<FakeSubscriptionReceiver> receivers,
      final boolean closeOnTimeout) {
    final Supplier<SubscriptionReceiver> constructor =
        () -> {
          final FakeSubscriptionReceiver receiver = new FakeSubscriptionReceiver(closeOnTimeout);
          receivers.add(receiver);
          return receiver;
        };
    return new SubscriptionReceiverAgent(constructor, false);
  }

  private TPipeSubscribeReq createHandshakeRequest(
      final String consumerGroupId, final String consumerId) throws IOException {
    final Map<String, String> attributes = new HashMap<>();
    attributes.put(ConsumerConstant.CONSUMER_GROUP_ID_KEY, consumerGroupId);
    attributes.put(ConsumerConstant.CONSUMER_ID_KEY, consumerId);
    return PipeSubscribeHandshakeReq.toTPipeSubscribeReq(new ConsumerConfig(attributes));
  }

  private static class FakeSubscriptionReceiver implements SubscriptionReceiver {

    private final boolean closeOnTimeout;
    private final AtomicInteger timeoutCount = new AtomicInteger();
    private final AtomicInteger exitCount = new AtomicInteger();
    private ConsumerConfig consumerConfig;
    private boolean invalidated;

    private FakeSubscriptionReceiver(final boolean closeOnTimeout) {
      this.closeOnTimeout = closeOnTimeout;
    }

    @Override
    public TPipeSubscribeResp handle(final TPipeSubscribeReq req) {
      if (req.getType() == PipeSubscribeRequestType.HANDSHAKE.getType()) {
        consumerConfig = ConsumerConfig.deserialize(req.bufferForBody());
        invalidated = false;
        return response(TSStatusCode.SUCCESS_STATUS);
      }
      if (req.getType() == PipeSubscribeRequestType.CLOSE.getType()) {
        consumerConfig = null;
        invalidated = true;
        return response(TSStatusCode.SUCCESS_STATUS);
      }
      return response(
          invalidated ? TSStatusCode.SUBSCRIPTION_MISSING_CONSUMER : TSStatusCode.SUCCESS_STATUS);
    }

    @Override
    public void setAuthenticatedUsername(final String username) {
      // no-op
    }

    @Override
    public PipeSubscribeRequestVersion getVersion() {
      return PipeSubscribeRequestVersion.VERSION_1;
    }

    @Override
    public void handleExit() {
      exitCount.incrementAndGet();
    }

    @Override
    public void handleTimeout() {
      timeoutCount.incrementAndGet();
      if (closeOnTimeout) {
        consumerConfig = null;
        invalidated = true;
      }
    }

    @Override
    public String getConsumerId() {
      return consumerConfig == null ? null : consumerConfig.getConsumerId();
    }

    @Override
    public String getConsumerGroupId() {
      return consumerConfig == null ? null : consumerConfig.getConsumerGroupId();
    }

    @Override
    public void invalidateConsumer() {
      consumerConfig = null;
      invalidated = true;
    }

    @Override
    public boolean hasActiveConsumer() {
      return consumerConfig != null;
    }

    @Override
    public long remainingMs() {
      return 0;
    }

    private TPipeSubscribeResp response(final TSStatusCode statusCode) {
      final TSStatus status = RpcUtils.getStatus(statusCode);
      return new TPipeSubscribeResp(
          status,
          PipeSubscribeResponseVersion.VERSION_1.getVersion(),
          PipeSubscribeResponseType.ACK.getType());
    }
  }
}
