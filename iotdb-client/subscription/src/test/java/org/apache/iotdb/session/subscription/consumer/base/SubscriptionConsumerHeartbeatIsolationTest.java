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
import org.apache.iotdb.rpc.subscription.config.ConsumerConstant;
import org.apache.iotdb.rpc.subscription.config.TopicConfig;
import org.apache.iotdb.rpc.subscription.payload.poll.SubscriptionCommitContext;
import org.apache.iotdb.rpc.subscription.payload.response.PipeSubscribeHeartbeatResp;
import org.apache.iotdb.session.AbstractSessionBuilder;
import org.apache.iotdb.session.subscription.SubscriptionTreeSessionBuilder;

import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class SubscriptionConsumerHeartbeatIsolationTest {

  private static final String HOST = "127.0.0.1";
  private static final int FIRST_PORT = 6667;
  private static final long LONG_INTERVAL_MS = 86_400_000L;
  private static final String TOPIC = "topic";

  @Test
  public void testSubscriptionConsumerUsesFiniteConnectionTimeoutByDefault() {
    Assert.assertEquals(
        ConsumerConstant.CONNECTION_TIMEOUT_MS_DEFAULT_VALUE,
        new AbstractSubscriptionPullConsumerBuilder().connectionTimeoutInMs);
    Assert.assertTrue(ConsumerConstant.CONNECTION_TIMEOUT_MS_DEFAULT_VALUE > 0);
  }

  @Test
  public void testBlockedProviderDoesNotDelayOtherHeartbeatsOrProviderReads() throws Exception {
    final CountDownLatch blockedHeartbeatStarted = new CountDownLatch(1);
    final CountDownLatch releaseBlockedHeartbeat = new CountDownLatch(1);
    final CountDownLatch healthyHeartbeatCompleted = new CountDownLatch(1);
    final TestPullConsumer consumer =
        new TestPullConsumer(
            blockedHeartbeatStarted, releaseBlockedHeartbeat, healthyHeartbeatCompleted);
    final ExecutorService executor = Executors.newSingleThreadExecutor();

    try {
      consumer.open();
      consumer.blockProviderOne.set(true);

      final AbstractSubscriptionProviders providers = getProviders(consumer);
      final int blockedProviderInitialHeartbeatCount = consumer.getHeartbeatCount(1);
      final int healthyProviderInitialHeartbeatCount = consumer.getHeartbeatCount(2);
      final long startNanos = System.nanoTime();
      final Future<?> heartbeat = executor.submit(() -> providers.heartbeat(consumer));
      heartbeat.get(1, TimeUnit.SECONDS);
      final long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);

      Assert.assertTrue(
          "Heartbeat scheduling was blocked for " + elapsedMs + " ms", elapsedMs < 500);
      Assert.assertTrue(blockedHeartbeatStarted.await(5, TimeUnit.SECONDS));
      Assert.assertTrue(healthyHeartbeatCompleted.await(5, TimeUnit.SECONDS));

      providers.acquireReadLock();
      try {
        Assert.assertEquals(2, providers.getAllProviders().size());
      } finally {
        providers.releaseReadLock();
      }

      providers.heartbeat(consumer);
      Thread.sleep(100L);
      Assert.assertEquals(blockedProviderInitialHeartbeatCount + 1, consumer.getHeartbeatCount(1));
      Assert.assertTrue(consumer.getHeartbeatCount(2) >= healthyProviderInitialHeartbeatCount + 2);
    } finally {
      releaseBlockedHeartbeat.countDown();
      consumer.close();
      executor.shutdownNow();
      executor.awaitTermination(5, TimeUnit.SECONDS);
    }
  }

  private AbstractSubscriptionProviders getProviders(final AbstractSubscriptionConsumer consumer)
      throws Exception {
    final Field field = AbstractSubscriptionConsumer.class.getDeclaredField("providers");
    field.setAccessible(true);
    return (AbstractSubscriptionProviders) field.get(consumer);
  }

  private static class TestPullConsumer extends AbstractSubscriptionPullConsumer {

    private final CountDownLatch blockedHeartbeatStarted;
    private final CountDownLatch releaseBlockedHeartbeat;
    private final CountDownLatch healthyHeartbeatCompleted;
    private final AtomicBoolean blockProviderOne = new AtomicBoolean(false);
    private final Map<Integer, AtomicInteger> heartbeatCounts = new ConcurrentHashMap<>();

    private TestPullConsumer(
        final CountDownLatch blockedHeartbeatStarted,
        final CountDownLatch releaseBlockedHeartbeat,
        final CountDownLatch healthyHeartbeatCompleted) {
      super(
          new AbstractSubscriptionPullConsumerBuilder()
              .host(HOST)
              .port(FIRST_PORT)
              .consumerId("consumer")
              .consumerGroupId("group")
              .heartbeatIntervalMs(LONG_INTERVAL_MS)
              .endpointsSyncIntervalMs(LONG_INTERVAL_MS)
              .autoCommit(false));
      this.blockedHeartbeatStarted = blockedHeartbeatStarted;
      this.releaseBlockedHeartbeat = releaseBlockedHeartbeat;
      this.healthyHeartbeatCompleted = healthyHeartbeatCompleted;
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
          blockedHeartbeatStarted,
          releaseBlockedHeartbeat,
          healthyHeartbeatCompleted,
          blockProviderOne,
          heartbeatCounts);
    }

    private int getHeartbeatCount(final int dataNodeId) {
      return heartbeatCounts.getOrDefault(dataNodeId, new AtomicInteger()).get();
    }
  }

  private static class TestSubscriptionProvider extends AbstractSubscriptionProvider {

    private final int dataNodeId;
    private final CountDownLatch blockedHeartbeatStarted;
    private final CountDownLatch releaseBlockedHeartbeat;
    private final CountDownLatch healthyHeartbeatCompleted;
    private final AtomicBoolean blockProviderOne;
    private final Map<Integer, AtomicInteger> heartbeatCounts;

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
        final CountDownLatch blockedHeartbeatStarted,
        final CountDownLatch releaseBlockedHeartbeat,
        final CountDownLatch healthyHeartbeatCompleted,
        final AtomicBoolean blockProviderOne,
        final Map<Integer, AtomicInteger> heartbeatCounts) {
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
      this.blockedHeartbeatStarted = blockedHeartbeatStarted;
      this.releaseBlockedHeartbeat = releaseBlockedHeartbeat;
      this.healthyHeartbeatCompleted = healthyHeartbeatCompleted;
      this.blockProviderOne = blockProviderOne;
      this.heartbeatCounts = heartbeatCounts;
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
    synchronized void closeSession() {
      setUnavailable();
    }

    @Override
    int getDataNodeId() {
      return dataNodeId;
    }

    @Override
    PipeSubscribeHeartbeatResp heartbeat(
        final List<SubscriptionCommitContext> processorBufferedCommitContexts) {
      heartbeatCounts.computeIfAbsent(dataNodeId, ignored -> new AtomicInteger()).incrementAndGet();
      if (dataNodeId == 1 && blockProviderOne.get()) {
        blockedHeartbeatStarted.countDown();
        try {
          releaseBlockedHeartbeat.await();
        } catch (final InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }
      if (dataNodeId == 2) {
        healthyHeartbeatCompleted.countDown();
      }

      final PipeSubscribeHeartbeatResp response = new PipeSubscribeHeartbeatResp();
      response.getTopics().put(TOPIC, new TopicConfig());
      response.getEndPoints().put(1, new TEndPoint(HOST, FIRST_PORT));
      response.getEndPoints().put(2, new TEndPoint(HOST, FIRST_PORT + 1));
      return response;
    }
  }
}
