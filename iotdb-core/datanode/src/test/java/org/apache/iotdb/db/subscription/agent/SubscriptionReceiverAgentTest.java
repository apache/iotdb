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
import org.apache.iotdb.rpc.subscription.payload.request.PipeSubscribeSubscribeReq;
import org.apache.iotdb.rpc.subscription.payload.request.SubscriptionHeartbeatReq;
import org.apache.iotdb.rpc.subscription.payload.response.PipeSubscribeResponseType;
import org.apache.iotdb.rpc.subscription.payload.response.PipeSubscribeResponseVersion;
import org.apache.iotdb.service.rpc.thrift.TPipeSubscribeReq;
import org.apache.iotdb.service.rpc.thrift.TPipeSubscribeResp;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

public class SubscriptionReceiverAgentTest {

  @Test
  public void testDisabledSubscriptionRejectsRequest() throws IOException {
    final SubscriptionReceiverAgent agent =
        new SubscriptionReceiverAgent(
            () -> {
              throw new AssertionError(
                  "Receiver must not be created when subscription is disabled");
            },
            false,
            () -> false);

    Assert.assertEquals(
        TSStatusCode.SUBSCRIPTION_NOT_ENABLED_ERROR.getStatusCode(),
        agent.handle(createHandshakeRequest("group", "consumer"), "root").getStatus().getCode());
  }

  @Test
  public void testTimeoutCheckerIsNotScheduledWhenSubscriptionIsDisabled() throws Exception {
    final SubscriptionReceiverAgent agent = new SubscriptionReceiverAgent();

    Assert.assertNull(getReceiverTimeoutChecker(agent));
  }

  @Test
  public void testTimeoutCheckerIsScheduledWhenSubscriptionIsEnabled() throws Exception {
    SubscriptionReceiverAgent agent = null;
    try {
      agent =
          new SubscriptionReceiverAgent(() -> new FakeSubscriptionReceiver(true), true, () -> true);

      Assert.assertNotNull(getReceiverTimeoutChecker(agent));
    } finally {
      if (agent != null) {
        final ScheduledExecutorService receiverTimeoutChecker = getReceiverTimeoutChecker(agent);
        if (receiverTimeoutChecker != null) {
          receiverTimeoutChecker.shutdownNow();
        }
      }
    }
  }

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
  public void testDuplicateConnectionFencesOldReceiverWithoutInvalidatingNewReceiver()
      throws Exception {
    final CopyOnWriteArrayList<FakeSubscriptionReceiver> receivers = new CopyOnWriteArrayList<>();
    final SubscriptionReceiverAgent agent = createAgent(receivers, false /* closeOnTimeout */);
    final TPipeSubscribeReq handshake = createHandshakeRequest("group", "consumer");

    Assert.assertEquals(
        TSStatusCode.SUCCESS_STATUS.getStatusCode(),
        agent.handle(handshake, "root").getStatus().getCode());
    final AtomicReference<TPipeSubscribeResp> duplicateResponse = new AtomicReference<>();
    final Thread newConnection =
        new Thread(() -> duplicateResponse.set(agent.handle(handshake, "root")));
    newConnection.start();
    newConnection.join(TimeUnit.SECONDS.toMillis(10));
    Assert.assertFalse(newConnection.isAlive());
    Assert.assertEquals(
        TSStatusCode.SUCCESS_STATUS.getStatusCode(), duplicateResponse.get().getStatus().getCode());
    final FakeSubscriptionReceiver oldReceiver = receivers.get(0);
    final FakeSubscriptionReceiver newReceiver = receivers.get(1);
    Assert.assertTrue(oldReceiver.invalidated);
    Assert.assertNotNull(newReceiver.consumerConfig);
    Assert.assertEquals(
        TSStatusCode.SUBSCRIPTION_CONSUMER_FENCED.getStatusCode(),
        agent.handle(SubscriptionHeartbeatReq.toThriftReq(), "root").getStatus().getCode());

    agent.checkReceiverTimeouts();

    Assert.assertEquals(0, oldReceiver.timeoutCount.get());
    Assert.assertEquals(1, newReceiver.timeoutCount.get());
  }

  @Test
  public void testConcurrentHandshakeWithSameIdentityFencesOldReceiver() throws Exception {
    final CopyOnWriteArrayList<FakeSubscriptionReceiver> receivers = new CopyOnWriteArrayList<>();
    final CountDownLatch oldHandshakeEntered = new CountDownLatch(1);
    final CountDownLatch releaseOldHandshake = new CountDownLatch(1);
    final CountDownLatch newReceiverCreated = new CountDownLatch(1);
    final CountDownLatch newHandshakeFinished = new CountDownLatch(1);
    final CountDownLatch oldHeartbeatFinished = new CountDownLatch(1);
    final AtomicInteger receiverIndex = new AtomicInteger();
    final AtomicReference<Throwable> threadFailure = new AtomicReference<>();
    final SubscriptionReceiverAgent agent =
        new SubscriptionReceiverAgent(
            () -> {
              final boolean isOldReceiver = receiverIndex.getAndIncrement() == 0;
              final FakeSubscriptionReceiver receiver =
                  new FakeSubscriptionReceiver(
                      false,
                      false,
                      isOldReceiver ? oldHandshakeEntered : null,
                      isOldReceiver ? releaseOldHandshake : null);
              receivers.add(receiver);
              if (!isOldReceiver) {
                newReceiverCreated.countDown();
              }
              return receiver;
            },
            false,
            () -> true);
    final TPipeSubscribeReq oldHandshake = createHandshakeRequest("group", "consumer");
    final TPipeSubscribeReq newHandshake = createHandshakeRequest("group", "consumer");
    final AtomicReference<TPipeSubscribeResp> oldHandshakeResponse = new AtomicReference<>();
    final AtomicReference<TPipeSubscribeResp> newHandshakeResponse = new AtomicReference<>();
    final AtomicReference<TPipeSubscribeResp> oldHeartbeatResponse = new AtomicReference<>();
    final AtomicReference<TPipeSubscribeResp> newHeartbeatResponse = new AtomicReference<>();
    final AtomicReference<TPipeSubscribeResp> newSubscribeResponse = new AtomicReference<>();

    final Thread oldConnection =
        new Thread(
            () -> {
              try {
                oldHandshakeResponse.set(agent.handle(oldHandshake, "root"));
                if (!newHandshakeFinished.await(10, TimeUnit.SECONDS)) {
                  throw new AssertionError("The new handshake did not finish");
                }
                oldHeartbeatResponse.set(
                    agent.handle(SubscriptionHeartbeatReq.toThriftReq(), "root"));
              } catch (final Throwable t) {
                threadFailure.compareAndSet(null, t);
              } finally {
                oldHeartbeatFinished.countDown();
              }
            });
    final Thread newConnection =
        new Thread(
            () -> {
              try {
                newHandshakeResponse.set(agent.handle(newHandshake, "root"));
                newHandshakeFinished.countDown();
                if (!oldHeartbeatFinished.await(10, TimeUnit.SECONDS)) {
                  throw new AssertionError("The old heartbeat did not finish");
                }
                newHeartbeatResponse.set(
                    agent.handle(SubscriptionHeartbeatReq.toThriftReq(), "root"));
                newSubscribeResponse.set(
                    agent.handle(
                        PipeSubscribeSubscribeReq.toTPipeSubscribeReq(Set.of("topic")), "root"));
              } catch (final Throwable t) {
                threadFailure.compareAndSet(null, t);
              } finally {
                newHandshakeFinished.countDown();
              }
            });

    oldConnection.start();
    Assert.assertTrue(oldHandshakeEntered.await(10, TimeUnit.SECONDS));
    newConnection.start();
    try {
      Assert.assertTrue(newReceiverCreated.await(10, TimeUnit.SECONDS));
    } finally {
      releaseOldHandshake.countDown();
      oldConnection.join(TimeUnit.SECONDS.toMillis(10));
      newConnection.join(TimeUnit.SECONDS.toMillis(10));
    }

    Assert.assertFalse(oldConnection.isAlive());
    Assert.assertFalse(newConnection.isAlive());
    if (threadFailure.get() != null) {
      throw new AssertionError(threadFailure.get());
    }
    Assert.assertEquals(
        TSStatusCode.SUCCESS_STATUS.getStatusCode(),
        oldHandshakeResponse.get().getStatus().getCode());
    Assert.assertEquals(
        TSStatusCode.SUCCESS_STATUS.getStatusCode(),
        newHandshakeResponse.get().getStatus().getCode());
    Assert.assertEquals(
        TSStatusCode.SUBSCRIPTION_CONSUMER_FENCED.getStatusCode(),
        oldHeartbeatResponse.get().getStatus().getCode());
    Assert.assertEquals(
        TSStatusCode.SUCCESS_STATUS.getStatusCode(),
        newHeartbeatResponse.get().getStatus().getCode());
    Assert.assertEquals(
        TSStatusCode.SUCCESS_STATUS.getStatusCode(),
        newSubscribeResponse.get().getStatus().getCode());
    Assert.assertEquals(2, receivers.size());
    Assert.assertTrue(receivers.get(0).invalidated);
    Assert.assertFalse(receivers.get(1).invalidated);
  }

  @Test
  public void testReconnectSucceedsAfterActiveConnectionExits() throws IOException {
    final CopyOnWriteArrayList<FakeSubscriptionReceiver> receivers = new CopyOnWriteArrayList<>();
    final SubscriptionReceiverAgent agent = createAgent(receivers, false /* closeOnTimeout */);
    final TPipeSubscribeReq handshake = createHandshakeRequest("group", "consumer");

    Assert.assertEquals(
        TSStatusCode.SUCCESS_STATUS.getStatusCode(),
        agent.handle(handshake, "root").getStatus().getCode());
    agent.handleClientExit();
    Assert.assertEquals(
        TSStatusCode.SUCCESS_STATUS.getStatusCode(),
        agent.handle(handshake, "root").getStatus().getCode());

    Assert.assertTrue(receivers.get(0).invalidated);
    Assert.assertFalse(receivers.get(1).invalidated);
  }

  @Test
  public void testLateHandshakeCannotTakeOverNewReceiver() throws Exception {
    final CopyOnWriteArrayList<FakeSubscriptionReceiver> receivers = new CopyOnWriteArrayList<>();
    final CountDownLatch oldHandshakeEntered = new CountDownLatch(1);
    final CountDownLatch releaseOldHandshake = new CountDownLatch(1);
    final CountDownLatch newOwnerReady = new CountDownLatch(1);
    final CountDownLatch oldHandshakeFinished = new CountDownLatch(1);
    final AtomicInteger receiverIndex = new AtomicInteger();
    final AtomicReference<Throwable> threadFailure = new AtomicReference<>();
    final SubscriptionReceiverAgent agent =
        new SubscriptionReceiverAgent(
            () -> {
              final boolean isOldReceiver = receiverIndex.getAndIncrement() == 0;
              final FakeSubscriptionReceiver receiver =
                  new FakeSubscriptionReceiver(
                      false,
                      true,
                      isOldReceiver ? oldHandshakeEntered : null,
                      isOldReceiver ? releaseOldHandshake : null);
              receivers.add(receiver);
              return receiver;
            },
            false,
            () -> true);
    final TPipeSubscribeReq handshake = createHandshakeRequestWithoutIdentity();
    final AtomicReference<TPipeSubscribeResp> oldHandshakeResponse = new AtomicReference<>();
    final AtomicReference<TPipeSubscribeResp> newHandshakeResponse = new AtomicReference<>();
    final AtomicReference<TPipeSubscribeResp> newHeartbeatResponse = new AtomicReference<>();
    final AtomicReference<TPipeSubscribeResp> newSubscribeResponse = new AtomicReference<>();

    final Thread oldConnection =
        new Thread(
            () -> {
              try {
                oldHandshakeResponse.set(agent.handle(handshake, "root"));
              } catch (final Throwable t) {
                threadFailure.set(t);
              } finally {
                oldHandshakeFinished.countDown();
              }
            });
    oldConnection.start();
    Assert.assertTrue(oldHandshakeEntered.await(10, TimeUnit.SECONDS));

    final Thread newConnection =
        new Thread(
            () -> {
              try {
                newHandshakeResponse.set(agent.handle(handshake, "root"));
                newOwnerReady.countDown();
                oldHandshakeFinished.await(10, TimeUnit.SECONDS);
                newHeartbeatResponse.set(
                    agent.handle(SubscriptionHeartbeatReq.toThriftReq(), "root"));
                newSubscribeResponse.set(
                    agent.handle(
                        PipeSubscribeSubscribeReq.toTPipeSubscribeReq(Set.of("topic")), "root"));
              } catch (final Throwable t) {
                threadFailure.set(t);
                newOwnerReady.countDown();
              }
            });
    newConnection.start();
    try {
      Assert.assertTrue(newOwnerReady.await(10, TimeUnit.SECONDS));
    } finally {
      releaseOldHandshake.countDown();
      oldConnection.join(TimeUnit.SECONDS.toMillis(10));
      newConnection.join(TimeUnit.SECONDS.toMillis(10));
    }

    Assert.assertFalse(oldConnection.isAlive());
    Assert.assertFalse(newConnection.isAlive());
    if (threadFailure.get() != null) {
      throw new AssertionError(threadFailure.get());
    }
    Assert.assertEquals(
        TSStatusCode.SUBSCRIPTION_CONSUMER_FENCED.getStatusCode(),
        oldHandshakeResponse.get().getStatus().getCode());
    Assert.assertEquals(
        TSStatusCode.SUCCESS_STATUS.getStatusCode(),
        newHandshakeResponse.get().getStatus().getCode());
    Assert.assertEquals(
        TSStatusCode.SUCCESS_STATUS.getStatusCode(),
        newHeartbeatResponse.get().getStatus().getCode());
    Assert.assertEquals(
        TSStatusCode.SUCCESS_STATUS.getStatusCode(),
        newSubscribeResponse.get().getStatus().getCode());
    Assert.assertEquals(2, receivers.size());
    Assert.assertTrue(receivers.get(0).invalidated);
    Assert.assertFalse(receivers.get(1).invalidated);
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
    return new SubscriptionReceiverAgent(constructor, false, () -> true);
  }

  private ScheduledExecutorService getReceiverTimeoutChecker(final SubscriptionReceiverAgent agent)
      throws Exception {
    final Field field = SubscriptionReceiverAgent.class.getDeclaredField("receiverTimeoutChecker");
    field.setAccessible(true);
    return (ScheduledExecutorService) field.get(agent);
  }

  private TPipeSubscribeReq createHandshakeRequest(
      final String consumerGroupId, final String consumerId) throws IOException {
    final Map<String, String> attributes = new HashMap<>();
    attributes.put(ConsumerConstant.CONSUMER_GROUP_ID_KEY, consumerGroupId);
    attributes.put(ConsumerConstant.CONSUMER_ID_KEY, consumerId);
    return PipeSubscribeHandshakeReq.toTPipeSubscribeReq(new ConsumerConfig(attributes));
  }

  private TPipeSubscribeReq createHandshakeRequestWithoutIdentity() throws IOException {
    return PipeSubscribeHandshakeReq.toTPipeSubscribeReq(new ConsumerConfig(new HashMap<>()));
  }

  private static class FakeSubscriptionReceiver implements SubscriptionReceiver {

    private final boolean closeOnTimeout;
    private final boolean assignDefaultIdentity;
    private final CountDownLatch handshakeEntered;
    private final CountDownLatch releaseHandshake;
    private final AtomicInteger timeoutCount = new AtomicInteger();
    private final AtomicInteger exitCount = new AtomicInteger();
    private volatile ConsumerConfig consumerConfig;
    private volatile boolean invalidated;

    private FakeSubscriptionReceiver(final boolean closeOnTimeout) {
      this(closeOnTimeout, false, null, null);
    }

    private FakeSubscriptionReceiver(
        final boolean closeOnTimeout,
        final boolean assignDefaultIdentity,
        final CountDownLatch handshakeEntered,
        final CountDownLatch releaseHandshake) {
      this.closeOnTimeout = closeOnTimeout;
      this.assignDefaultIdentity = assignDefaultIdentity;
      this.handshakeEntered = handshakeEntered;
      this.releaseHandshake = releaseHandshake;
    }

    @Override
    public TPipeSubscribeResp handle(final TPipeSubscribeReq req) {
      if (req.getType() == PipeSubscribeRequestType.HANDSHAKE.getType()) {
        consumerConfig = ConsumerConfig.deserialize(req.bufferForBody());
        if (assignDefaultIdentity) {
          consumerConfig.setConsumerGroupId("group");
          consumerConfig.setConsumerId("consumer");
        }
        if (handshakeEntered != null) {
          handshakeEntered.countDown();
          try {
            Assert.assertTrue(releaseHandshake.await(10, TimeUnit.SECONDS));
          } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError(e);
          }
        }
        invalidated = false;
        return response(TSStatusCode.SUCCESS_STATUS);
      }
      if (req.getType() == PipeSubscribeRequestType.CLOSE.getType()) {
        consumerConfig = null;
        invalidated = true;
        return response(TSStatusCode.SUCCESS_STATUS);
      }
      return response(
          invalidated ? TSStatusCode.SUBSCRIPTION_CONSUMER_FENCED : TSStatusCode.SUCCESS_STATUS);
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
