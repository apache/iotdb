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
import org.apache.iotdb.rpc.subscription.exception.SubscriptionConsumerFencedException;
import org.apache.iotdb.rpc.subscription.exception.SubscriptionException;
import org.apache.iotdb.rpc.subscription.exception.SubscriptionRuntimeNonCriticalException;
import org.apache.iotdb.rpc.subscription.payload.poll.SubscriptionCommitContext;
import org.apache.iotdb.rpc.subscription.payload.poll.SubscriptionPollResponse;
import org.apache.iotdb.rpc.subscription.payload.poll.SubscriptionPollResponseType;
import org.apache.iotdb.rpc.subscription.payload.poll.TabletsPayload;
import org.apache.iotdb.rpc.subscription.payload.poll.TopicProgress;
import org.apache.iotdb.rpc.subscription.payload.response.PipeSubscribeHeartbeatResp;
import org.apache.iotdb.session.AbstractSessionBuilder;
import org.apache.iotdb.session.subscription.SubscriptionTreeSessionBuilder;
import org.apache.iotdb.session.subscription.consumer.AsyncCommitCallback;
import org.apache.iotdb.session.subscription.payload.SubscriptionMessage;

import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
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

  @Test
  public void testFencedHeartbeatStopsBackgroundReconnect() throws Exception {
    final TestPullConsumer consumer = new TestPullConsumer();
    final AbstractSubscriptionProviders providers = getProviders(consumer);
    try {
      consumer.open();
      consumer.fenceOnHeartbeat = true;
      providers.heartbeat(consumer);

      waitUntil(consumer::isFenced);
      providers.sync(consumer);
      providers.heartbeat(consumer);
      Assert.assertEquals(1, consumer.createdProviders.size());
      try {
        consumer.multiplePoll(Collections.singleton("topic"), 100L);
        Assert.fail("The fenced consumer must not poll or reconnect");
      } catch (final SubscriptionConsumerFencedException expected) {
        Assert.assertEquals("consumer connection fenced", expected.getMessage());
      }
      consumer.close();
      Assert.assertEquals(0, consumer.closeRequestCount);
      Assert.assertEquals(1, consumer.sessionCloseCount);
      Assert.assertEquals(1, consumer.closedStatesDuringClose.size());
    } finally {
      consumer.close();
    }
  }

  @Test
  public void testFencedDuringOpenClosesPartiallyOpenedProviders() throws Exception {
    final TestPullConsumer consumer = new TestPullConsumer();
    consumer.fenceOnHeartbeat = true;
    try {
      consumer.open();
      Assert.fail("The consumer must fail to open when its handshake is fenced");
    } catch (final SubscriptionConsumerFencedException expected) {
      Assert.assertTrue(consumer.isFenced());
      Assert.assertEquals(1, consumer.createdProviders.size());
      Assert.assertEquals(0, consumer.closeRequestCount);
      Assert.assertEquals(1, consumer.sessionCloseCount);
      Assert.assertEquals(1, consumer.closedStatesDuringClose.size());
    }

    try {
      consumer.open();
      Assert.fail("The fenced consumer must not retry the handshake");
    } catch (final SubscriptionConsumerFencedException expected) {
      Assert.assertEquals(1, consumer.createdProviders.size());
    }
  }

  @Test
  public void testFencedHandshakeClosesOpenedSession() throws Exception {
    final TestPullConsumer consumer = new TestPullConsumer();
    consumer.fenceOnHandshake = true;

    try {
      consumer.open();
      Assert.fail("The consumer must fail to open when its handshake is fenced");
    } catch (final SubscriptionConsumerFencedException expected) {
      Assert.assertTrue(consumer.isFenced());
      Assert.assertEquals(1, consumer.createdProviders.size());
      Assert.assertEquals(0, consumer.closeRequestCount);
      Assert.assertEquals(1, consumer.sessionCloseCount);
      Assert.assertEquals(1, consumer.closedStatesDuringClose.size());
    }
  }

  @Test
  public void testFencedTabletContinuationDoesNotSendNack() throws Exception {
    final TestPullConsumer consumer = new TestPullConsumer();
    try {
      consumer.open();
      consumer.returnPartialTablets = true;
      consumer.fenceOnPollTablets = true;

      try {
        consumer.multiplePoll(Collections.singleton("topic"), 1_000L);
        Assert.fail("A fenced tablet continuation must fail the poll");
      } catch (final SubscriptionConsumerFencedException expected) {
        Assert.assertEquals("consumer connection fenced", expected.getMessage());
      }

      Assert.assertTrue(consumer.isFenced());
      Assert.assertEquals(0, consumer.commitRequestCount);
    } finally {
      consumer.close();
    }
  }

  @Test
  public void testFencedParallelPollDoesNotDeliverSiblingMessages() throws Exception {
    final TestPullConsumer consumer = new TestPullConsumer();
    final SubscriptionConsumerFencedException fencedException =
        new SubscriptionConsumerFencedException("consumer connection fenced");
    final SubscriptionMessage message =
        new SubscriptionMessage(
            new SubscriptionCommitContext(0, 0, "topic", CONSUMER_GROUP_ID, 0L), 1L);
    final CompletableFuture<List<SubscriptionMessage>> fencedFuture = new CompletableFuture<>();
    fencedFuture.completeExceptionally(fencedException);

    try {
      consumer.collectMultiplePollResults(
          Arrays.asList(
              CompletableFuture.completedFuture(Collections.singletonList(message)), fencedFuture),
          Collections.singleton("topic"));
      Assert.fail("A fenced poll task must discard messages returned by sibling tasks");
    } catch (final SubscriptionConsumerFencedException expected) {
      Assert.assertSame(fencedException, expected);
    }

    Assert.assertTrue(consumer.isFenced());
  }

  @Test
  public void testFencedAsyncCommitFailsBeforeReadingMessages() throws Exception {
    final TestPullConsumer consumer = new TestPullConsumer();
    try {
      final SubscriptionConsumerFencedException fencedException =
          new SubscriptionConsumerFencedException("consumer connection fenced");
      consumer.fence(fencedException);
      final AtomicBoolean messagesIterated = new AtomicBoolean(false);
      final Iterable<SubscriptionMessage> messages =
          () -> {
            messagesIterated.set(true);
            return Collections.emptyIterator();
          };

      final CountDownLatch callbackCompleted = new CountDownLatch(1);
      final AtomicBoolean callbackSucceeded = new AtomicBoolean(false);
      final AtomicReference<Throwable> callbackFailure = new AtomicReference<>();
      consumer.commitAsync(
          messages,
          new AsyncCommitCallback() {
            @Override
            public void onComplete() {
              callbackSucceeded.set(true);
              callbackCompleted.countDown();
            }

            @Override
            public void onFailure(final Throwable e) {
              callbackFailure.set(e);
              callbackCompleted.countDown();
            }
          });

      Assert.assertTrue(callbackCompleted.await(5, TimeUnit.SECONDS));
      Assert.assertFalse(callbackSucceeded.get());
      Assert.assertSame(fencedException, callbackFailure.get());

      final CompletableFuture<Void> future = consumer.commitAsync(messages);
      try {
        future.get(5, TimeUnit.SECONDS);
        Assert.fail("A fenced async commit must complete exceptionally");
      } catch (final ExecutionException expected) {
        Assert.assertSame(fencedException, expected.getCause());
      }

      Assert.assertFalse(messagesIterated.get());
      Assert.assertEquals(0, consumer.commitRequestCount);
    } finally {
      consumer.close();
    }
  }

  @Test
  public void testSyncCommitClearsAcceptedContextsFromAutoCommitBuffer() throws Exception {
    final TestPullConsumer consumer = new TestPullConsumer(true);
    final SubscriptionCommitContext commitContext =
        new SubscriptionCommitContext(0, 0, "topic", CONSUMER_GROUP_ID, 1L);
    try {
      consumer.open();
      addUncommittedCommitContexts(consumer, commitContext);

      consumer.commitSync(new SubscriptionMessage(commitContext, 1L));

      Assert.assertTrue(getUncommittedCommitContexts(consumer).isEmpty());
      Assert.assertEquals(1, consumer.commitRequestCount);
    } finally {
      consumer.close();
    }
  }

  @Test
  public void testAsyncCommitClearsAcceptedContextsFromAutoCommitBuffer() throws Exception {
    final TestPullConsumer consumer = new TestPullConsumer(true);
    final SubscriptionCommitContext commitContext =
        new SubscriptionCommitContext(0, 0, "topic", CONSUMER_GROUP_ID, 1L);
    try {
      consumer.open();
      addUncommittedCommitContexts(consumer, commitContext);

      consumer.commitAsync(new SubscriptionMessage(commitContext, 1L)).get(5, TimeUnit.SECONDS);

      Assert.assertTrue(getUncommittedCommitContexts(consumer).isEmpty());
      Assert.assertEquals(1, consumer.commitRequestCount);
    } finally {
      consumer.close();
    }
  }

  @Test
  public void testPartialCommitKeepsRejectedContextsInAutoCommitBuffer() throws Exception {
    final TestPullConsumer consumer = new TestPullConsumer(true);
    final SubscriptionCommitContext acceptedCommitContext =
        new SubscriptionCommitContext(0, 0, "topic", CONSUMER_GROUP_ID, 1L);
    final SubscriptionCommitContext rejectedCommitContext =
        new SubscriptionCommitContext(0, 0, "topic", CONSUMER_GROUP_ID, 2L);
    consumer.rejectedCommitContexts.add(rejectedCommitContext);
    try {
      consumer.open();
      addUncommittedCommitContexts(consumer, acceptedCommitContext, rejectedCommitContext);

      try {
        consumer.commitSync(
            Arrays.asList(
                new SubscriptionMessage(acceptedCommitContext, 1L),
                new SubscriptionMessage(rejectedCommitContext, 2L)));
        Assert.fail("A partially accepted commit must fail");
      } catch (final SubscriptionRuntimeNonCriticalException expected) {
        Assert.assertTrue(expected.getMessage().contains("partially accepted"));
      }

      final SortedMap<Long, Set<SubscriptionCommitContext>> uncommittedCommitContexts =
          getUncommittedCommitContexts(consumer);
      Assert.assertEquals(1, uncommittedCommitContexts.size());
      Assert.assertEquals(
          Collections.singleton(rejectedCommitContext), uncommittedCommitContexts.get(0L));
      Assert.assertEquals(1, consumer.commitRequestCount);
    } finally {
      consumer.close();
    }
  }

  private static void addUncommittedCommitContexts(
      final TestPullConsumer consumer, final SubscriptionCommitContext... commitContexts)
      throws Exception {
    getUncommittedCommitContexts(consumer)
        .computeIfAbsent(0L, ignored -> new HashSet<>())
        .addAll(Arrays.asList(commitContexts));
  }

  @SuppressWarnings("unchecked")
  private static SortedMap<Long, Set<SubscriptionCommitContext>> getUncommittedCommitContexts(
      final TestPullConsumer consumer) throws Exception {
    final Field field =
        AbstractSubscriptionPullConsumer.class.getDeclaredField("uncommittedCommitContexts");
    field.setAccessible(true);
    return (SortedMap<Long, Set<SubscriptionCommitContext>>) field.get(consumer);
  }

  private AbstractSubscriptionProviders getProviders(final AbstractSubscriptionConsumer consumer)
      throws Exception {
    final Field field = AbstractSubscriptionConsumer.class.getDeclaredField("providers");
    field.setAccessible(true);
    return (AbstractSubscriptionProviders) field.get(consumer);
  }

  private void waitUntil(final BooleanSupplier condition) throws InterruptedException {
    final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
    while (!condition.getAsBoolean() && System.nanoTime() < deadline) {
      Thread.sleep(10L);
    }
    Assert.assertTrue(condition.getAsBoolean());
  }

  @Test
  public void testConcurrentPullConsumerCloseReturnsWithoutWaiting() throws Exception {
    final CountDownLatch providerCloseStarted = new CountDownLatch(1);
    final CountDownLatch allowProviderClose = new CountDownLatch(1);
    final TestPullConsumer consumer =
        new TestPullConsumer(providerCloseStarted, allowProviderClose);
    final ExecutorService executor = Executors.newFixedThreadPool(2);

    try {
      consumer.open();

      final Future<?> firstClose = executor.submit(consumer::close);
      Assert.assertTrue(providerCloseStarted.await(5, TimeUnit.SECONDS));

      final Future<?> concurrentClose = executor.submit(consumer::close);
      concurrentClose.get(1, TimeUnit.SECONDS);
      Assert.assertFalse(firstClose.isDone());

      allowProviderClose.countDown();
      firstClose.get(5, TimeUnit.SECONDS);
      Assert.assertEquals(1, consumer.closedStatesDuringClose.size());
    } finally {
      allowProviderClose.countDown();
      executor.shutdownNow();
      executor.awaitTermination(5, TimeUnit.SECONDS);
    }
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
          closedStatesDuringClose,
          () -> false,
          () -> false,
          () -> false,
          () -> false,
          () -> {},
          () -> {},
          () -> {},
          Collections.emptySet(),
          null,
          null);
    }
  }

  private static class TestPullConsumer extends AbstractSubscriptionPullConsumer {

    private final List<Boolean> closedStatesDuringHandshake = new ArrayList<>();
    private final List<Boolean> closedStatesDuringClose = new ArrayList<>();
    private final List<TestSubscriptionProvider> createdProviders = new ArrayList<>();
    private boolean fenceOnHandshake;
    private boolean fenceOnHeartbeat;
    private boolean fenceOnPollTablets;
    private boolean returnPartialTablets;
    private int closeRequestCount;
    private int sessionCloseCount;
    private int commitRequestCount;
    private final Set<SubscriptionCommitContext> rejectedCommitContexts = new HashSet<>();
    private final CountDownLatch providerCloseStarted;
    private final CountDownLatch allowProviderClose;

    private TestPullConsumer() {
      this(false, null, null);
    }

    private TestPullConsumer(final boolean autoCommit) {
      this(autoCommit, null, null);
    }

    private TestPullConsumer(
        final CountDownLatch providerCloseStarted, final CountDownLatch allowProviderClose) {
      this(false, providerCloseStarted, allowProviderClose);
    }

    private TestPullConsumer(
        final boolean autoCommit,
        final CountDownLatch providerCloseStarted,
        final CountDownLatch allowProviderClose) {
      super(
          new AbstractSubscriptionPullConsumerBuilder()
              .host(HOST)
              .port(PORT)
              .consumerId(CONSUMER_ID)
              .consumerGroupId(CONSUMER_GROUP_ID)
              .heartbeatIntervalMs(LONG_INTERVAL_MS)
              .endpointsSyncIntervalMs(LONG_INTERVAL_MS)
              .autoCommit(autoCommit)
              .autoCommitIntervalMs(LONG_INTERVAL_MS));
      this.providerCloseStarted = providerCloseStarted;
      this.allowProviderClose = allowProviderClose;
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
      final TestSubscriptionProvider provider =
          new TestSubscriptionProvider(
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
              closedStatesDuringClose,
              () -> fenceOnHandshake,
              () -> fenceOnHeartbeat,
              () -> fenceOnPollTablets,
              () -> returnPartialTablets,
              () -> closeRequestCount++,
              () -> commitRequestCount++,
              () -> sessionCloseCount++,
              rejectedCommitContexts,
              providerCloseStarted,
              allowProviderClose);
      createdProviders.add(provider);
      return provider;
    }
  }

  private static class TestSubscriptionProvider extends AbstractSubscriptionProvider {

    private final BooleanSupplier consumerClosedSupplier;
    private final List<Boolean> closedStatesDuringHandshake;
    private final List<Boolean> closedStatesDuringClose;
    private final BooleanSupplier fenceOnHandshake;
    private final BooleanSupplier fenceOnHeartbeat;
    private final BooleanSupplier fenceOnPollTablets;
    private final BooleanSupplier returnPartialTablets;
    private final Runnable closeRequest;
    private final Runnable commitRequest;
    private final Runnable sessionClose;
    private final Set<SubscriptionCommitContext> rejectedCommitContexts;
    private final CountDownLatch providerCloseStarted;
    private final CountDownLatch allowProviderClose;

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
        final List<Boolean> closedStatesDuringClose,
        final BooleanSupplier fenceOnHandshake,
        final BooleanSupplier fenceOnHeartbeat,
        final BooleanSupplier fenceOnPollTablets,
        final BooleanSupplier returnPartialTablets,
        final Runnable closeRequest,
        final Runnable commitRequest,
        final Runnable sessionClose,
        final Set<SubscriptionCommitContext> rejectedCommitContexts,
        final CountDownLatch providerCloseStarted,
        final CountDownLatch allowProviderClose) {
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
      this.fenceOnHandshake = fenceOnHandshake;
      this.fenceOnHeartbeat = fenceOnHeartbeat;
      this.fenceOnPollTablets = fenceOnPollTablets;
      this.returnPartialTablets = returnPartialTablets;
      this.closeRequest = closeRequest;
      this.commitRequest = commitRequest;
      this.sessionClose = sessionClose;
      this.rejectedCommitContexts = rejectedCommitContexts;
      this.providerCloseStarted = providerCloseStarted;
      this.allowProviderClose = allowProviderClose;
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
      if (fenceOnHandshake.getAsBoolean()) {
        throw new SubscriptionConsumerFencedException("consumer connection fenced");
      }
      setAvailable();
    }

    @Override
    synchronized void close() {
      closedStatesDuringClose.add(consumerClosedSupplier.getAsBoolean());
      closeRequest.run();
      if (Objects.nonNull(providerCloseStarted)) {
        providerCloseStarted.countDown();
      }
      if (Objects.nonNull(allowProviderClose)) {
        try {
          allowProviderClose.await();
        } catch (final InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }
      setUnavailable();
    }

    @Override
    synchronized void closeSession() {
      closedStatesDuringClose.add(consumerClosedSupplier.getAsBoolean());
      sessionClose.run();
      setUnavailable();
    }

    @Override
    PipeSubscribeHeartbeatResp heartbeat(
        final List<SubscriptionCommitContext> processorBufferedCommitContexts) {
      if (fenceOnHeartbeat.getAsBoolean()) {
        throw new SubscriptionConsumerFencedException("consumer connection fenced");
      }
      return new PipeSubscribeHeartbeatResp();
    }

    @Override
    List<SubscriptionPollResponse> poll(
        final Set<String> topicNames,
        final long timeoutMs,
        final Map<String, TopicProgress> progressByTopic) {
      if (!returnPartialTablets.getAsBoolean()) {
        return Collections.emptyList();
      }
      return Collections.singletonList(
          new SubscriptionPollResponse(
              SubscriptionPollResponseType.TABLETS.getType(),
              new TabletsPayload(Collections.emptyMap(), 1),
              new SubscriptionCommitContext(0, 0, "topic", CONSUMER_GROUP_ID, 0L)));
    }

    @Override
    List<SubscriptionPollResponse> pollTablets(
        final SubscriptionCommitContext commitContext, final int offset, final long timeoutMs) {
      if (fenceOnPollTablets.getAsBoolean()) {
        throw new SubscriptionConsumerFencedException("consumer connection fenced");
      }
      return Collections.emptyList();
    }

    @Override
    CommitResult commit(
        final List<SubscriptionCommitContext> subscriptionCommitContexts, final boolean nack) {
      commitRequest.run();
      final List<SubscriptionCommitContext> acceptedCommitContexts = new ArrayList<>();
      for (final SubscriptionCommitContext commitContext : subscriptionCommitContexts) {
        if (!rejectedCommitContexts.contains(commitContext)) {
          acceptedCommitContexts.add(commitContext);
        }
      }
      return new CommitResult(acceptedCommitContexts, Collections.emptyMap());
    }
  }
}
