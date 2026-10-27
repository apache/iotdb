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
import org.apache.iotdb.rpc.subscription.exception.SubscriptionConnectionException;
import org.apache.iotdb.rpc.subscription.exception.SubscriptionConsumerFencedException;
import org.apache.iotdb.rpc.subscription.exception.SubscriptionException;
import org.apache.iotdb.rpc.subscription.exception.SubscriptionOwnerFencedException;
import org.apache.iotdb.rpc.subscription.exception.SubscriptionRuntimeCriticalException;
import org.apache.iotdb.rpc.subscription.exception.SubscriptionRuntimeNonCriticalException;
import org.apache.iotdb.rpc.subscription.payload.poll.FileInitPayload;
import org.apache.iotdb.rpc.subscription.payload.poll.RegionProgress;
import org.apache.iotdb.rpc.subscription.payload.poll.SubscriptionCommitContext;
import org.apache.iotdb.rpc.subscription.payload.poll.SubscriptionPollResponse;
import org.apache.iotdb.rpc.subscription.payload.poll.SubscriptionPollResponseType;
import org.apache.iotdb.rpc.subscription.payload.poll.TabletsPayload;
import org.apache.iotdb.rpc.subscription.payload.poll.TopicProgress;
import org.apache.iotdb.rpc.subscription.payload.poll.WriterId;
import org.apache.iotdb.rpc.subscription.payload.poll.WriterProgress;
import org.apache.iotdb.rpc.subscription.payload.response.PipeSubscribeHeartbeatResp;
import org.apache.iotdb.session.AbstractSessionBuilder;
import org.apache.iotdb.session.subscription.SubscriptionTreeSessionBuilder;
import org.apache.iotdb.session.subscription.payload.SubscriptionMessage;
import org.apache.iotdb.session.subscription.util.PollTimer;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.SocketTimeoutException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

public class SubscriptionConsumerMultiProviderPollTest {

  private static final String HOST = "127.0.0.1";
  private static final int FIRST_PORT = 10_001;
  private static final String TOPIC = "topic1";
  private static final String CONSUMER_ID = "test_consumer";
  private static final String CONSUMER_GROUP_ID = "test_consumer_group";
  private static final long LONG_INTERVAL_MS = 86_400_000L;

  @Rule public final TemporaryFolder temporaryFolder = new TemporaryFolder();

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

  @Test
  public void testFullyFilteredTabletBatchIsAcknowledgedAndPollReturnsNoMessages()
      throws SubscriptionException {
    final TestPullConsumer consumer = new TestPullConsumer(1, 0, true);
    try {
      consumer.open();
      consumer.subscribeTopic();

      final List<SubscriptionMessage> messages = consumer.pollForTest(1_000L);

      Assert.assertTrue(messages.isEmpty());
      Assert.assertEquals(1, consumer.getAcknowledgedCommitContexts().size());
      Assert.assertEquals(
          new SubscriptionCommitContext(1, 0, TOPIC, CONSUMER_GROUP_ID, 0L),
          consumer.getAcknowledgedCommitContexts().get(0));
    } finally {
      consumer.close();
    }
  }

  @Test
  public void testPollReconnectsWhenProviderBecomesUnavailable() throws SubscriptionException {
    final TestPullConsumer consumer = new TestPullConsumer(1, 0, false, true, 1);
    try {
      consumer.open();
      consumer.subscribeTopic();

      final List<SubscriptionMessage> messages = consumer.pollForTest(1_000L);

      Assert.assertEquals(1, messages.size());
      Assert.assertEquals(2, consumer.getCreatedProviderCount());
      Assert.assertEquals(2, consumer.getPollCount(1));
    } finally {
      consumer.close();
    }
  }

  @Test
  public void testPollReconnectsWhenOneOfMultipleProvidersBecomesUnavailable()
      throws SubscriptionException {
    final TestPullConsumer consumer = new TestPullConsumer(1, 0, false, true, 3);
    try {
      consumer.open();
      consumer.subscribeTopic();

      final List<SubscriptionMessage> messages = consumer.pollForTest(2_000L);

      Assert.assertEquals(1, messages.size());
      Assert.assertEquals(4, consumer.getCreatedProviderCount());
      Assert.assertEquals(2, consumer.getPollCount(1));
    } finally {
      consumer.close();
    }
  }

  @Test
  public void testPartialRecoveryPreservesHealthyProvidersAndClosesOnlyFailedSession()
      throws Exception {
    final TestPullConsumer consumer = new TestPullConsumer(1, 0);
    try {
      consumer.open();
      final AbstractSubscriptionProviders providers = getProviders(consumer);
      final TestSubscriptionProvider failed = consumer.createdProviders.get(0);
      final AbstractSubscriptionProvider healthy = providers.getProvider(2);
      failed.setUnavailable();

      providers.sync(consumer);

      Assert.assertEquals(4, consumer.getCreatedProviderCount());
      Assert.assertNotSame(failed, providers.getProvider(1));
      Assert.assertSame(healthy, providers.getProvider(2));
      Assert.assertEquals(1, failed.sessionCloseCount.get());
      Assert.assertEquals(0, failed.closeCount.get());
      Assert.assertTrue(providers.getProvider(1).isAvailable());
    } finally {
      consumer.close();
    }
  }

  @Test
  public void testAllUnavailableRecoveryClosesSessionsWithoutClosingConsumer() throws Exception {
    final TestPullConsumer consumer = new TestPullConsumer(1, 0);
    try {
      consumer.open();
      final List<TestSubscriptionProvider> failedProviders =
          new ArrayList<>(consumer.createdProviders);
      failedProviders.forEach(AbstractSubscriptionProvider::setUnavailable);

      getProviders(consumer).sync(consumer);

      Assert.assertEquals(6, consumer.getCreatedProviderCount());
      for (final TestSubscriptionProvider failed : failedProviders) {
        Assert.assertEquals(1, failed.sessionCloseCount.get());
        Assert.assertEquals(0, failed.closeCount.get());
      }
    } finally {
      consumer.close();
    }
  }

  @Test
  public void testLateHeartbeatCannotReviveFailedProvider() throws Exception {
    assertLateHeartbeatIgnored(false);
  }

  @Test
  public void testRetiredHeartbeatCannotFenceRecoveredConsumer() throws Exception {
    assertLateHeartbeatIgnored(true);
  }

  private void assertLateHeartbeatIgnored(final boolean fenced) throws Exception {
    final TestPullConsumer consumer = new TestPullConsumer(1, 0);
    final ExecutorService executor = Executors.newSingleThreadExecutor();
    final CountDownLatch heartbeatStarted = new CountDownLatch(1);
    final CountDownLatch releaseHeartbeat = new CountDownLatch(1);
    try {
      consumer.open();
      consumer.subscribeTopic();
      final AbstractSubscriptionProviders providers = getProviders(consumer);
      final TestSubscriptionProvider failed = consumer.createdProviders.get(0);
      failed.emptyHeartbeatTopics = true;
      failed.heartbeatAction =
          () -> {
            heartbeatStarted.countDown();
            await(releaseHeartbeat);
            if (fenced) {
              throw new SubscriptionConsumerFencedException("retired connection fenced");
            }
          };
      final Method heartbeat =
          AbstractSubscriptionProviders.class.getDeclaredMethod(
              "heartbeatProvider",
              AbstractSubscriptionConsumer.class,
              AbstractSubscriptionProvider.class);
      heartbeat.setAccessible(true);
      final Future<?> result = executor.submit(() -> heartbeat.invoke(providers, consumer, failed));
      Assert.assertTrue(heartbeatStarted.await(5, TimeUnit.SECONDS));

      failed.setUnavailable();
      if (fenced) {
        providers.sync(consumer);
      }
      releaseHeartbeat.countDown();
      result.get(5, TimeUnit.SECONDS);

      Assert.assertFalse(failed.isAvailable());
      Assert.assertFalse(consumer.isFenced());
      Assert.assertTrue(consumer.subscribedTopics.containsKey(TOPIC));
      providers.sync(consumer);
      Assert.assertNotSame(failed, providers.getProvider(1));
    } finally {
      releaseHeartbeat.countDown();
      consumer.close();
      executor.shutdownNow();
      executor.awaitTermination(5, TimeUnit.SECONDS);
    }
  }

  @Test
  public void testCloseDuringReplacementHandshakeDoesNotOpenMoreProviders() throws Exception {
    assertCloseDuringReplacementHandshake(false);
    assertCloseDuringReplacementHandshake(true);
  }

  private void assertCloseDuringReplacementHandshake(final boolean allUnavailable)
      throws Exception {
    final TestPullConsumer consumer = new TestPullConsumer(1, 0);
    final ExecutorService executor = Executors.newSingleThreadExecutor();
    final CountDownLatch handshakeStarted = new CountDownLatch(1);
    final CountDownLatch releaseHandshake = new CountDownLatch(1);
    try {
      consumer.open();
      final AbstractSubscriptionProviders providers = getProviders(consumer);
      if (allUnavailable) {
        consumer.createdProviders.forEach(AbstractSubscriptionProvider::setUnavailable);
      } else {
        consumer.createdProviders.get(0).setUnavailable();
      }
      consumer.handshakeAction =
          () -> {
            handshakeStarted.countDown();
            await(releaseHandshake);
          };
      final Future<?> sync = executor.submit(() -> providers.sync(consumer));
      Assert.assertTrue(handshakeStarted.await(5, TimeUnit.SECONDS));

      consumer.close();
      releaseHandshake.countDown();
      sync.get(5, TimeUnit.SECONDS);

      Assert.assertTrue(consumer.isClosed());
      Assert.assertTrue(providers.hasNoProviders());
      Assert.assertEquals(4, consumer.getCreatedProviderCount());
      Assert.assertEquals(1, consumer.createdProviders.get(3).closeCount.get());
    } finally {
      releaseHandshake.countDown();
      consumer.close();
      executor.shutdownNow();
      executor.awaitTermination(5, TimeUnit.SECONDS);
    }
  }

  @Test
  public void testContinuationAndCommitFailuresSchedulePartialRecovery() throws Exception {
    for (final String operation :
        new String[] {"pollFileInternal", "pollTabletsInternal", "commitInternal"}) {
      final TestPullConsumer consumer = new TestPullConsumer(1, 0);
      try {
        consumer.open();
        final TestSubscriptionProvider failed = consumer.createdProviders.get(0);
        failed.failContinuation = true;
        final CountDownLatch replacementStarted = new CountDownLatch(1);
        consumer.handshakeAction = replacementStarted::countDown;
        final SubscriptionCommitContext context =
            new SubscriptionCommitContext(1, 0, TOPIC, CONSUMER_GROUP_ID, 0L);
        final Method method;
        final Object[] arguments;
        if ("commitInternal".equals(operation)) {
          method =
              AbstractSubscriptionConsumer.class.getDeclaredMethod(
                  operation, int.class, List.class, boolean.class);
          arguments = new Object[] {1, Collections.singletonList(context), false};
        } else {
          method =
              AbstractSubscriptionConsumer.class.getDeclaredMethod(
                  operation,
                  SubscriptionCommitContext.class,
                  "pollFileInternal".equals(operation) ? long.class : int.class,
                  long.class);
          arguments =
              new Object[] {
                context, "pollFileInternal".equals(operation) ? (Object) 0L : 0, 1_000L
              };
        }
        method.setAccessible(true);
        try {
          method.invoke(consumer, arguments);
          Assert.assertNotEquals("commitInternal", operation);
        } catch (final InvocationTargetException e) {
          Assert.assertEquals("commitInternal", operation);
          Assert.assertTrue(e.getCause() instanceof SubscriptionConnectionException);
        }

        Assert.assertTrue(replacementStarted.await(5, TimeUnit.SECONDS));
        final AbstractSubscriptionProviders providers = getProviders(consumer);
        providers.acquireReadLock();
        try {
          Assert.assertNotSame(failed, providers.getProvider(1));
          Assert.assertEquals(3, providers.getAvailableProviderCount());
          Assert.assertEquals("commitInternal".equals(operation) ? 1 : 0, failed.commitCount.get());
          Assert.assertEquals(0, consumer.createdProviders.get(3).commitCount.get());
        } finally {
          providers.releaseReadLock();
        }
      } finally {
        consumer.close();
      }
    }
  }

  @Test(timeout = 20_000)
  public void testSyncCommitRetriesFailedProviderWithoutRepeatingAcceptedGroups() throws Exception {
    final TestPullConsumer consumer = new TestPullConsumer(1, 0, false, false, 3, true);
    try {
      consumer.open();
      final TestSubscriptionProvider healthy = consumer.createdProviders.get(0);
      final TestSubscriptionProvider failed = consumer.createdProviders.get(1);
      failed.failContinuation = true;
      final SubscriptionMessage first =
          new SubscriptionMessage(
              new SubscriptionCommitContext(1, 0, TOPIC, CONSUMER_GROUP_ID, 1L), 1L);
      final SubscriptionMessage second =
          new SubscriptionMessage(
              new SubscriptionCommitContext(2, 0, TOPIC, CONSUMER_GROUP_ID, 2L), 2L);
      final Field field =
          AbstractSubscriptionPullConsumer.class.getDeclaredField("uncommittedCommitContexts");
      field.setAccessible(true);
      @SuppressWarnings("unchecked")
      final SortedMap<Long, Set<SubscriptionCommitContext>> uncommitted =
          (SortedMap<Long, Set<SubscriptionCommitContext>>) field.get(consumer);
      uncommitted.put(0L, ConcurrentHashMap.newKeySet());
      uncommitted.get(0L).add(first.getCommitContext());
      uncommitted.get(0L).add(second.getCommitContext());

      consumer.commitSync(Arrays.asList(first, second));

      Assert.assertTrue(uncommitted.isEmpty());
      Assert.assertEquals(1, healthy.commitCount.get());
      Assert.assertEquals(1, failed.commitCount.get());
      Assert.assertEquals(0, failed.closeCount.get());
      Assert.assertEquals(1, failed.sessionCloseCount.get());
      final TestSubscriptionProvider replacement =
          (TestSubscriptionProvider) getProviders(consumer).getProvider(2);
      Assert.assertNotSame(failed, replacement);
      Assert.assertEquals(1, replacement.commitCount.get());
      Assert.assertEquals(
          Collections.singletonList(second.getCommitContext()), replacement.committed);
    } finally {
      consumer.close();
    }
  }

  @Test(timeout = 20_000)
  public void testSyncCommitRecoversAlreadyUnavailableProvider() throws Exception {
    final TestPullConsumer consumer = new TestPullConsumer(1, 0);
    try {
      consumer.open();
      final TestSubscriptionProvider failed = consumer.createdProviders.get(0);
      failed.setUnavailable();

      consumer.commitSync(
          new SubscriptionMessage(
              new SubscriptionCommitContext(1, 0, TOPIC, CONSUMER_GROUP_ID, 1L), 1L));

      Assert.assertEquals(0, failed.commitCount.get());
      final TestSubscriptionProvider replacement =
          (TestSubscriptionProvider) getProviders(consumer).getProvider(1);
      Assert.assertNotSame(failed, replacement);
      Assert.assertEquals(1, replacement.commitCount.get());
    } finally {
      consumer.close();
    }
  }

  @Test(timeout = 25_000)
  public void testSyncCommitConnectionRetriesAreBounded() throws Exception {
    final TestPullConsumer consumer = new TestPullConsumer(1, 0);
    final SubscriptionConnectionException failure =
        new SubscriptionConnectionException("read timed out", new SocketTimeoutException());
    try {
      consumer.open();
      final TestSubscriptionProvider provider = consumer.createdProviders.get(0);
      provider.commitAction =
          () -> {
            throw failure;
          };

      Assert.assertSame(
          failure,
          Assert.assertThrows(
              SubscriptionConnectionException.class, () -> consumer.commitSync(commitMessage())));

      Assert.assertEquals(4, provider.commitCount.get());
    } finally {
      consumer.close();
    }
  }

  @Test
  public void testSyncCommitDoesNotRetryServerErrors() throws Exception {
    for (final SubscriptionException failure :
        new SubscriptionException[] {
          new SubscriptionRuntimeNonCriticalException("commit rejected"),
          new SubscriptionRuntimeCriticalException("commit failed"),
          new SubscriptionConsumerFencedException("consumer fenced")
        }) {
      final TestPullConsumer consumer = new TestPullConsumer(1, 0);
      try {
        consumer.open();
        final TestSubscriptionProvider provider = consumer.createdProviders.get(0);
        provider.commitAction =
            () -> {
              throw failure;
            };

        Assert.assertSame(
            failure,
            Assert.assertThrows(
                SubscriptionException.class, () -> consumer.commitSync(commitMessage())));
        Assert.assertEquals(1, provider.commitCount.get());
        Assert.assertEquals(
            failure instanceof SubscriptionConsumerFencedException, consumer.isFenced());
      } finally {
        consumer.close();
      }
    }
  }

  @Test(timeout = 20_000)
  public void testSyncCommitRetriesOnlyRejectedContexts() throws Exception {
    assertPartialCommitRetry(false);
  }

  @Test(timeout = 25_000)
  public void testSyncCommitRetainsAcceptedContextsAfterPartialRetryExhaustion() throws Exception {
    assertPartialCommitRetry(true);
  }

  private void assertPartialCommitRetry(final boolean exhaustRetries) throws Exception {
    final TestPullConsumer consumer = new TestPullConsumer(1, 0, false, false, 3, true);
    try {
      consumer.open();
      final List<SubscriptionMessage> messages = new ArrayList<>();
      final List<SubscriptionCommitContext> contexts = new ArrayList<>();
      for (int id = 1; id <= 10; id++) {
        final SubscriptionCommitContext context =
            new SubscriptionCommitContext(1, 0, TOPIC, CONSUMER_GROUP_ID, id);
        contexts.add(context);
        messages.add(new SubscriptionMessage(context, (long) id));
      }
      // Accepted contexts need not form a prefix of the request.
      final List<SubscriptionCommitContext> rejected =
          Arrays.asList(contexts.get(2), contexts.get(6));
      final List<SubscriptionCommitContext> accepted = new ArrayList<>(contexts);
      accepted.removeAll(rejected);
      final SortedMap<Long, Set<SubscriptionCommitContext>> uncommitted =
          getUncommittedCommitContexts(consumer);
      uncommitted.put(0L, ConcurrentHashMap.newKeySet());
      uncommitted.get(0L).addAll(contexts);
      final List<List<SubscriptionCommitContext>> requests = new ArrayList<>();
      final TestSubscriptionProvider provider = consumer.createdProviders.get(0);
      provider.commitResponse =
          requested -> {
            requests.add(new ArrayList<>(requested));
            if (requests.size() == 1) {
              return new AbstractSubscriptionProvider.CommitResult(
                  accepted, Collections.singletonMap(TOPIC, batchProgress(contexts.get(0))));
            }
            Assert.assertEquals(2, uncommitted.get(0L).size());
            Assert.assertTrue(uncommitted.get(0L).containsAll(rejected));
            if (!exhaustRetries && requests.size() == 2) {
              throw new SubscriptionConnectionException("retry read timed out");
            }
            return new AbstractSubscriptionProvider.CommitResult(
                exhaustRetries ? Collections.emptyList() : new ArrayList<>(requested),
                Collections.emptyMap());
          };

      if (exhaustRetries) {
        Assert.assertThrows(
            SubscriptionRuntimeNonCriticalException.class, () -> consumer.commitSync(messages));
        Assert.assertEquals(2, uncommitted.get(0L).size());
        Assert.assertTrue(uncommitted.get(0L).containsAll(rejected));
      } else {
        consumer.commitSync(messages);
        Assert.assertTrue(uncommitted.isEmpty());
      }
      Assert.assertEquals(exhaustRetries ? 4 : 3, requests.size());
      Assert.assertEquals(contexts, requests.get(0));
      for (int i = 1; i < requests.size(); i++) {
        Assert.assertEquals(rejected, requests.get(i));
      }
      Assert.assertEquals(batchProgress(contexts.get(0)), consumer.committedPositions(TOPIC));
    } finally {
      consumer.close();
    }
  }

  @Test(timeout = 10_000)
  public void testSyncCommitRetryPreservesInterruption() throws Exception {
    final TestPullConsumer consumer = new TestPullConsumer(1, 0);
    final ExecutorService executor = Executors.newSingleThreadExecutor();
    final CountDownLatch commitFailed = new CountDownLatch(1);
    final AtomicReference<Thread> commitThread = new AtomicReference<>();
    try {
      consumer.open();
      final TestSubscriptionProvider provider = consumer.createdProviders.get(0);
      provider.commitAction =
          () -> {
            commitFailed.countDown();
            throw new SubscriptionConnectionException("read timed out");
          };
      final Future<Boolean> interrupted =
          executor.submit(
              () -> {
                commitThread.set(Thread.currentThread());
                Assert.assertThrows(
                    SubscriptionConnectionException.class,
                    () -> consumer.commitSync(commitMessage()));
                return Thread.currentThread().isInterrupted();
              });
      Assert.assertTrue(commitFailed.await(5, TimeUnit.SECONDS));
      commitThread.get().interrupt();

      Assert.assertTrue(interrupted.get(1, TimeUnit.SECONDS));
      Assert.assertEquals(1, provider.commitCount.get());
    } finally {
      consumer.close();
      executor.shutdownNow();
      executor.awaitTermination(5, TimeUnit.SECONDS);
    }
  }

  @Test(timeout = 10_000)
  public void testSyncCommitStopsRetryingAfterCloseOrFencing() throws Exception {
    for (final boolean closeConsumer : new boolean[] {true, false}) {
      final TestPullConsumer consumer = new TestPullConsumer(1, 0);
      try {
        consumer.open();
        final TestSubscriptionProvider provider = consumer.createdProviders.get(0);
        final SubscriptionConsumerFencedException fenced =
            new SubscriptionConsumerFencedException("consumer fenced");
        provider.commitAction =
            () -> {
              if (closeConsumer) {
                consumer.close();
              } else {
                consumer.fence(fenced);
              }
              throw new SubscriptionConnectionException("read timed out");
            };

        final SubscriptionException failure =
            Assert.assertThrows(
                SubscriptionException.class, () -> consumer.commitSync(commitMessage()));

        if (closeConsumer) {
          Assert.assertTrue(failure instanceof SubscriptionConnectionException);
        } else {
          Assert.assertSame(fenced, failure);
        }
        Assert.assertEquals(1, provider.commitCount.get());
      } finally {
        consumer.close();
      }
    }
  }

  private static SubscriptionMessage commitMessage() {
    return new SubscriptionMessage(
        new SubscriptionCommitContext(1, 0, TOPIC, CONSUMER_GROUP_ID, 1L), 1L);
  }

  @Test
  public void testPollCleanupConnectionFailureDoesNotMakePollCritical() throws Exception {
    for (final boolean filePoll : new boolean[] {false, true}) {
      final TestPullConsumer consumer = new TestPullConsumer(1, 0);
      final SubscriptionRuntimeNonCriticalException pollFailure =
          new SubscriptionRuntimeNonCriticalException("continuation poll failed");
      final SubscriptionConnectionException nackFailure =
          new SubscriptionConnectionException("NACK read timed out");
      try {
        consumer.open();
        final TestSubscriptionProvider provider = consumer.createdProviders.get(0);
        provider.continuationAction =
            () -> {
              throw pollFailure;
            };
        provider.commitAction =
            () -> {
              throw nackFailure;
            };

        final Throwable failure = invokeFailingContinuationPoll(consumer, filePoll);

        Assert.assertTrue(failure instanceof SubscriptionRuntimeNonCriticalException);
        Assert.assertSame(pollFailure, failure.getCause());
        Assert.assertArrayEquals(new Throwable[] {nackFailure}, pollFailure.getSuppressed());
        Assert.assertEquals(1, provider.commitCount.get());
        Assert.assertFalse(consumer.isFenced());
      } finally {
        consumer.close();
      }
    }
  }

  @Test
  public void testPollCleanupStillPropagatesFencingAndServerErrors() throws Exception {
    for (final boolean filePoll : new boolean[] {false, true}) {
      for (final SubscriptionException nackFailure :
          new SubscriptionException[] {
            new SubscriptionConsumerFencedException("consumer fenced"),
            new SubscriptionRuntimeCriticalException("NACK failed"),
            new SubscriptionRuntimeNonCriticalException("NACK rejected")
          }) {
        final TestPullConsumer consumer = new TestPullConsumer(1, 0);
        try {
          consumer.open();
          final TestSubscriptionProvider provider = consumer.createdProviders.get(0);
          provider.continuationAction =
              () -> {
                throw new SubscriptionRuntimeNonCriticalException("continuation poll failed");
              };
          provider.commitAction =
              () -> {
                throw nackFailure;
              };

          Assert.assertSame(nackFailure, invokeFailingContinuationPoll(consumer, filePoll));
          Assert.assertEquals(1, provider.commitCount.get());
          Assert.assertEquals(
              nackFailure instanceof SubscriptionConsumerFencedException, consumer.isFenced());
        } finally {
          consumer.close();
        }
      }
    }
  }

  private Throwable invokeFailingContinuationPoll(
      final TestPullConsumer consumer, final boolean filePoll) throws Exception {
    final Field directory = AbstractSubscriptionConsumer.class.getDeclaredField("fileSaveDir");
    directory.setAccessible(true);
    directory.set(consumer, temporaryFolder.newFolder().getAbsolutePath());
    final SubscriptionCommitContext context = commitMessage().getCommitContext();
    final SubscriptionPollResponse response =
        filePoll
            ? new SubscriptionPollResponse(
                SubscriptionPollResponseType.FILE_INIT.getType(),
                new FileInitPayload("test.tsfile"),
                context)
            : new SubscriptionPollResponse(
                SubscriptionPollResponseType.TABLETS.getType(),
                new TabletsPayload(Collections.emptyMap(), 1),
                context);
    final Method poll =
        AbstractSubscriptionConsumer.class.getDeclaredMethod(
            filePoll ? "pollFile" : "pollTablets", SubscriptionPollResponse.class, PollTimer.class);
    poll.setAccessible(true);
    return Assert.assertThrows(
            InvocationTargetException.class,
            () ->
                poll.invoke(consumer, response, new PollTimer(System.currentTimeMillis(), 1_000L)))
        .getCause();
  }

  @Test(timeout = 20_000)
  public void testSyncCommitRetriesOnlyUnconfirmedBatches() throws Exception {
    assertCommitBatchRetry(false);
  }

  @Test(timeout = 25_000)
  public void testSyncCommitRetainsAcceptedBatchesWhenRetriesAreExhausted() throws Exception {
    assertCommitBatchRetry(true);
  }

  @Test(timeout = 20_000)
  public void testAsyncCommitRetryDoesNotOverwriteNewerCommittedProgress() throws Exception {
    final TestPullConsumer consumer =
        new TestPullConsumer(1, 0) {
          @Override
          protected void ack(final Iterable<SubscriptionMessage> messages)
              throws SubscriptionException {
            commitSync(messages);
          }
        };
    final CountDownLatch retryStarted = new CountDownLatch(1);
    final CountDownLatch newerCommitCompleted = new CountDownLatch(1);
    try {
      consumer.open();
      final TestSubscriptionProvider provider = consumer.createdProviders.get(0);
      final SubscriptionMessage first = commitMessage();
      final SubscriptionMessage second =
          new SubscriptionMessage(
              new SubscriptionCommitContext(1, 0, TOPIC, CONSUMER_GROUP_ID, 2L), 2L);
      enableCommitBatching(provider, first.getCommitContext());
      provider.commitAction =
          () -> {
            if (provider.commitBatches.size() == 3) {
              retryStarted.countDown();
              await(newerCommitCompleted);
            }
          };

      final CompletableFuture<Void> retry = consumer.commitAsync(Arrays.asList(first, second));
      Assert.assertTrue(retryStarted.await(10, TimeUnit.SECONDS));
      Assert.assertEquals(
          batchProgress(first.getCommitContext()), consumer.committedPositions(TOPIC));

      final TopicProgress newerProgress =
          new TopicProgress(
              Collections.singletonMap(
                  "region1",
                  new RegionProgress(
                      Collections.singletonMap(
                          new WriterId("region1", 1), new WriterProgress(1L, 2L)))));
      final TestSubscriptionProvider otherProvider = consumer.createdProviders.get(1);
      otherProvider.committedProgressByTopic = Collections.singletonMap(TOPIC, newerProgress);
      consumer.commitSync(
          new SubscriptionMessage(
              new SubscriptionCommitContext(2, 0, TOPIC, CONSUMER_GROUP_ID, 3L), 3L));
      Assert.assertEquals(newerProgress, consumer.committedPositions(TOPIC));

      newerCommitCompleted.countDown();
      retry.get(5, TimeUnit.SECONDS);

      Assert.assertEquals(
          newerProgress.getRegionProgress().get("region1"),
          consumer.committedPositions(TOPIC).getRegionProgress().get("region1"));
      Assert.assertEquals(2, consumer.committedPositions(TOPIC).getRegionProgress().size());
    } finally {
      newerCommitCompleted.countDown();
      consumer.close();
    }
  }

  private void assertCommitBatchRetry(final boolean exhaustRetries) throws Exception {
    final TestPullConsumer consumer = new TestPullConsumer(1, 0, false, false, 3, true);
    try {
      consumer.open();
      final TestSubscriptionProvider provider = consumer.createdProviders.get(0);
      final SubscriptionMessage first = commitMessage();
      final SubscriptionMessage second =
          new SubscriptionMessage(
              new SubscriptionCommitContext(1, 0, TOPIC, CONSUMER_GROUP_ID, 2L), 2L);
      enableCommitBatching(provider, first.getCommitContext());
      provider.failAllRemainingBatches = exhaustRetries;
      final SortedMap<Long, Set<SubscriptionCommitContext>> uncommitted =
          getUncommittedCommitContexts(consumer);
      uncommitted.put(0L, ConcurrentHashMap.newKeySet());
      uncommitted.get(0L).add(first.getCommitContext());
      uncommitted.get(0L).add(second.getCommitContext());

      if (exhaustRetries) {
        Assert.assertThrows(
            SubscriptionConnectionException.class,
            () -> consumer.commitSync(Arrays.asList(first, second)));
        Assert.assertEquals(Collections.singleton(second.getCommitContext()), uncommitted.get(0L));
      } else {
        consumer.commitSync(Arrays.asList(first, second));
        Assert.assertTrue(uncommitted.isEmpty());
      }

      Assert.assertEquals(
          batchProgress(first.getCommitContext()).getRegionProgress().get("region1"),
          consumer.committedPositions(TOPIC).getRegionProgress().get("region1"));
      Assert.assertEquals(
          exhaustRetries ? 1 : 2, consumer.committedPositions(TOPIC).getRegionProgress().size());

      Assert.assertEquals(exhaustRetries ? 5 : 3, provider.commitBatches.size());
      Assert.assertEquals(
          Collections.singletonList(first.getCommitContext()), provider.commitBatches.get(0));
      for (int i = 1; i < provider.commitBatches.size(); i++) {
        Assert.assertEquals(
            Collections.singletonList(second.getCommitContext()), provider.commitBatches.get(i));
      }
    } finally {
      consumer.close();
    }
  }

  @Test
  public void testAutoCommitPreservesConfirmedBatchesAfterConnectionFailure() throws Exception {
    for (final boolean consensus : new boolean[] {false, true}) {
      final TestPullConsumer consumer = new TestPullConsumer(1, 0);
      try {
        consumer.open();
        final TestSubscriptionProvider provider = consumer.createdProviders.get(0);
        final SubscriptionCommitContext first = batchCommitContext(1L, consensus);
        final SubscriptionCommitContext second = batchCommitContext(2L, consensus);
        enableCommitBatching(provider, first);

        final Set<SubscriptionCommitContext> removable =
            consumer.ackCommitContextsWithPartialProgress(Arrays.asList(first, second));

        Assert.assertTrue(removable.contains(first));
        Assert.assertEquals(consensus, removable.contains(second));
        Assert.assertEquals(consensus ? 2 : 1, removable.size());
        Assert.assertEquals(batchProgress(first), consumer.committedPositions(TOPIC));
        final Map<String, Set<SubscriptionCommitContext>> pending =
            getPendingRedirectAcks(consumer);
        if (consensus) {
          Assert.assertEquals(1, pending.size());
          Assert.assertEquals(
              Collections.singleton(second), pending.get(TOPIC + '\u0001' + "region1"));
        } else {
          Assert.assertTrue(pending.isEmpty());
          Assert.assertEquals(
              Collections.singleton(second),
              consumer.ackCommitContextsWithPartialProgress(Collections.singletonList(second)));
          Assert.assertEquals(Collections.singletonList(second), provider.commitBatches.get(2));
        }
      } finally {
        consumer.close();
      }
    }
  }

  @Test
  public void testRedirectAckRetriesOnlyUnconfirmedBatches() throws Exception {
    final TestPullConsumer consumer = new TestPullConsumer(1, 0);
    try {
      consumer.open();
      final TestSubscriptionProvider provider = consumer.createdProviders.get(0);
      final SubscriptionCommitContext first = batchCommitContext(1L, true);
      final SubscriptionCommitContext second = batchCommitContext(2L, true);
      enableCommitBatching(provider, first);
      final Map<String, Set<SubscriptionCommitContext>> pending = getPendingRedirectAcks(consumer);
      final Set<SubscriptionCommitContext> contexts = ConcurrentHashMap.newKeySet();
      contexts.add(first);
      contexts.add(second);
      pending.put(TOPIC + '\u0001' + "region1", contexts);
      final Method flush =
          AbstractSubscriptionConsumer.class.getDeclaredMethod(
              "flushPendingRedirectAcks", List.class);
      flush.setAccessible(true);
      final List<SubscriptionMessage> currentMessages =
          Collections.singletonList(new SubscriptionMessage(first, 1L));

      flush.invoke(consumer, currentMessages);

      final SubscriptionCommitContext accepted = provider.commitBatches.get(0).get(0);
      final SubscriptionCommitContext unconfirmed = provider.commitBatches.get(1).get(0);
      Assert.assertEquals(Collections.singleton(unconfirmed), contexts);
      Assert.assertEquals(batchProgress(accepted), consumer.committedPositions(TOPIC));

      flush.invoke(consumer, currentMessages);

      Assert.assertTrue(pending.isEmpty());
      Assert.assertEquals(3, provider.commitBatches.size());
      Assert.assertEquals(Collections.singletonList(unconfirmed), provider.commitBatches.get(2));
    } finally {
      consumer.close();
    }
  }

  @Test
  public void testSyncCommitPreservesConfirmedBatchesAfterServerErrors() throws Exception {
    for (final SubscriptionException failure : commitServerFailures()) {
      assertConfirmedBatchesAfterServerError(failure, "sync");
    }
  }

  @Test
  public void testAsyncCommitPreservesConfirmedBatchesAfterServerErrors() throws Exception {
    for (final SubscriptionException failure : commitServerFailures()) {
      assertConfirmedBatchesAfterServerError(failure, "async");
    }
  }

  @Test
  public void testAutoCommitPreservesConfirmedBatchesAfterServerErrors() throws Exception {
    for (final SubscriptionException failure : commitServerFailures()) {
      assertConfirmedBatchesAfterServerError(failure, "auto");
    }
  }

  @Test
  public void testCloseCommitPreservesConfirmedBatchesAfterServerErrors() throws Exception {
    for (final SubscriptionException failure : commitServerFailures()) {
      assertConfirmedBatchesAfterServerError(failure, "close");
    }
  }

  @Test
  public void testRedirectAckPreservesConfirmedBatchesAfterServerErrors() throws Exception {
    for (final SubscriptionException failure : commitServerFailures()) {
      assertConfirmedBatchesAfterServerError(failure, "redirect");
    }
  }

  private static List<SubscriptionException> commitServerFailures() {
    return Arrays.asList(
        new SubscriptionRuntimeNonCriticalException("commit rejected"),
        new SubscriptionRuntimeCriticalException("commit failed"),
        new SubscriptionOwnerFencedException("owner fenced"),
        new SubscriptionConsumerFencedException("consumer fenced"));
  }

  private void assertConfirmedBatchesAfterServerError(
      final SubscriptionException failure, final String operation) throws Exception {
    final TestPullConsumer consumer =
        new TestPullConsumer(1, 0, false, false, 3, true) {
          @Override
          protected void ack(final Iterable<SubscriptionMessage> messages)
              throws SubscriptionException {
            commitSync(messages);
          }
        };
    try {
      consumer.open();
      final SubscriptionCommitContext first = batchCommitContext(1L, false);
      final SubscriptionCommitContext second = batchCommitContext(2L, false);
      final TestSubscriptionProvider provider = consumer.createdProviders.get(0);
      enableCommitBatching(provider, first);
      provider.commitAction =
          () -> {
            if (provider.commitBatches.size() > 1) {
              throw failure;
            }
          };
      final SortedMap<Long, Set<SubscriptionCommitContext>> uncommitted =
          getUncommittedCommitContexts(consumer);
      final Set<SubscriptionCommitContext> contexts = ConcurrentHashMap.newKeySet();
      contexts.addAll(Arrays.asList(first, second));
      uncommitted.put(0L, contexts);
      final List<SubscriptionMessage> messages =
          Arrays.asList(new SubscriptionMessage(first, 1L), new SubscriptionMessage(second, 2L));

      switch (operation) {
        case "sync":
          Assert.assertSame(
              failure,
              Assert.assertThrows(
                  SubscriptionException.class, () -> consumer.commitSync(messages)));
          break;
        case "async":
          Assert.assertSame(
              failure,
              Assert.assertThrows(
                      java.util.concurrent.ExecutionException.class,
                      () -> consumer.commitAsync(messages).get(5, TimeUnit.SECONDS))
                  .getCause());
          break;
        case "auto":
          final Class<?> worker =
              Class.forName(AbstractSubscriptionPullConsumer.class.getName() + "$AutoCommitWorker");
          final java.lang.reflect.Constructor<?> constructor =
              worker.getDeclaredConstructor(AbstractSubscriptionPullConsumer.class);
          constructor.setAccessible(true);
          ((Runnable) constructor.newInstance(consumer)).run();
          break;
        case "close":
          consumer.close();
          break;
        case "redirect":
          getPendingRedirectAcks(consumer).put(TOPIC + '\u0001' + "region1", contexts);
          final Method flush =
              AbstractSubscriptionConsumer.class.getDeclaredMethod(
                  "flushPendingRedirectAcks", List.class);
          flush.setAccessible(true);
          flush.invoke(
              consumer,
              Collections.singletonList(new SubscriptionMessage(batchCommitContext(1L, true), 1L)));
          break;
        default:
          throw new AssertionError(operation);
      }

      final SubscriptionCommitContext accepted = provider.commitBatches.get(0).get(0);
      final SubscriptionCommitContext unconfirmed = provider.commitBatches.get(1).get(0);
      Assert.assertEquals(Collections.singleton(unconfirmed), contexts);
      final Field positions =
          AbstractSubscriptionConsumer.class.getDeclaredField("committedPositionsByTopic");
      positions.setAccessible(true);
      Assert.assertEquals(
          batchProgress(accepted), ((Map<?, ?>) positions.get(consumer)).get(TOPIC));
      Assert.assertEquals(2, provider.commitBatches.size());
      Assert.assertEquals(
          failure instanceof SubscriptionConsumerFencedException, consumer.isFenced());
    } finally {
      consumer.close();
    }
  }

  @Test(timeout = 20_000)
  public void testCommitFailureDoesNotOverwriteNewerConcurrentProgress() throws Exception {
    final TestPullConsumer consumer =
        new TestPullConsumer(1, 0) {
          @Override
          protected void ack(final Iterable<SubscriptionMessage> messages)
              throws SubscriptionException {
            commitSync(messages);
          }
        };
    final CountDownLatch firstBatchCompleted = new CountDownLatch(1);
    final CountDownLatch newerCommitCompleted = new CountDownLatch(1);
    try {
      consumer.open();
      final SubscriptionMessage first = commitMessage();
      final SubscriptionMessage second =
          new SubscriptionMessage(
              new SubscriptionCommitContext(1, 0, TOPIC, CONSUMER_GROUP_ID, 2L), 2L);
      final TestSubscriptionProvider provider = consumer.createdProviders.get(0);
      enableCommitBatching(provider, first.getCommitContext());
      provider.commitAction =
          () -> {
            if (provider.commitBatches.size() == 2) {
              firstBatchCompleted.countDown();
              await(newerCommitCompleted);
            }
          };
      final CompletableFuture<Void> older = consumer.commitAsync(Arrays.asList(first, second));
      Assert.assertTrue(firstBatchCompleted.await(5, TimeUnit.SECONDS));

      final Map<WriterId, WriterProgress> writers = new HashMap<>();
      writers.put(new WriterId("region1", 1), new WriterProgress(1L, 2L));
      writers.put(new WriterId("region1", 2), new WriterProgress(1L, 3L));
      final TopicProgress newerProgress =
          new TopicProgress(Collections.singletonMap("region1", new RegionProgress(writers)));
      final TestSubscriptionProvider otherProvider = consumer.createdProviders.get(1);
      otherProvider.committedProgressByTopic = Collections.singletonMap(TOPIC, newerProgress);
      consumer.commitSync(
          new SubscriptionMessage(
              new SubscriptionCommitContext(2, 0, TOPIC, CONSUMER_GROUP_ID, 3L), 3L));

      newerCommitCompleted.countDown();
      older.get(10, TimeUnit.SECONDS);

      Assert.assertEquals(
          newerProgress.getRegionProgress().get("region1"),
          consumer.committedPositions(TOPIC).getRegionProgress().get("region1"));
      Assert.assertEquals(2, consumer.committedPositions(TOPIC).getRegionProgress().size());
    } finally {
      newerCommitCompleted.countDown();
      consumer.close();
    }
  }

  private static SubscriptionCommitContext batchCommitContext(
      final long commitId, final boolean consensus) {
    return consensus
        ? new SubscriptionCommitContext(
            1,
            0,
            TOPIC,
            CONSUMER_GROUP_ID,
            1L,
            new WriterId("region1", 1),
            new WriterProgress(1L, commitId))
        : new SubscriptionCommitContext(1, 0, TOPIC, CONSUMER_GROUP_ID, commitId);
  }

  @Test(timeout = 20_000)
  public void testSyncCommitRetriesRejectedBatchAfterLaterConnectionFailure() throws Exception {
    final TestPullConsumer consumer = new TestPullConsumer(1, 0);
    try {
      consumer.open();
      final TestSubscriptionProvider provider = consumer.createdProviders.get(0);
      final SubscriptionMessage first = commitMessage();
      final SubscriptionMessage second =
          new SubscriptionMessage(
              new SubscriptionCommitContext(1, 0, TOPIC, CONSUMER_GROUP_ID, 2L), 2L);
      enableCommitBatching(provider, first.getCommitContext());
      provider.rejectFirstBatch = true;

      consumer.commitSync(Arrays.asList(first, second));

      Assert.assertEquals(4, provider.commitBatches.size());
      Assert.assertEquals(
          Arrays.asList(
              Collections.singletonList(first.getCommitContext()),
              Collections.singletonList(second.getCommitContext()),
              Collections.singletonList(first.getCommitContext()),
              Collections.singletonList(second.getCommitContext())),
          provider.commitBatches);
    } finally {
      consumer.close();
    }
  }

  private static void enableCommitBatching(
      final TestSubscriptionProvider provider, final SubscriptionCommitContext context)
      throws Exception {
    final Field field = AbstractSubscriptionProvider.class.getDeclaredField("requestBodySizeLimit");
    field.setAccessible(true);
    // Reserve exactly enough room for one context per commit RPC, including the response headroom.
    field.setInt(
        provider,
        3
            * (Integer.BYTES
                + Byte.BYTES
                + SubscriptionCommitContext.serialize(context).remaining()));
    provider.batchCommits = true;
  }

  private static TopicProgress batchProgress(final SubscriptionCommitContext context) {
    final String regionId = "region" + context.getCommitId();
    return new TopicProgress(
        Collections.singletonMap(
            regionId,
            new RegionProgress(
                Collections.singletonMap(
                    new WriterId(regionId, 1), new WriterProgress(1L, context.getCommitId())))));
  }

  @SuppressWarnings("unchecked")
  private static SortedMap<Long, Set<SubscriptionCommitContext>> getUncommittedCommitContexts(
      final TestPullConsumer consumer) throws Exception {
    final Field field =
        AbstractSubscriptionPullConsumer.class.getDeclaredField("uncommittedCommitContexts");
    field.setAccessible(true);
    return (SortedMap<Long, Set<SubscriptionCommitContext>>) field.get(consumer);
  }

  @SuppressWarnings("unchecked")
  private static Map<String, Set<SubscriptionCommitContext>> getPendingRedirectAcks(
      final TestPullConsumer consumer) throws Exception {
    final Field field =
        AbstractSubscriptionConsumer.class.getDeclaredField("pendingRedirectAcksByTopicRegion");
    field.setAccessible(true);
    return (Map<String, Set<SubscriptionCommitContext>>) field.get(consumer);
  }

  private static AbstractSubscriptionProviders getProviders(
      final AbstractSubscriptionConsumer consumer) throws Exception {
    final Field field = AbstractSubscriptionConsumer.class.getDeclaredField("providers");
    field.setAccessible(true);
    return (AbstractSubscriptionProviders) field.get(consumer);
  }

  private static void await(final CountDownLatch latch) {
    try {
      Assert.assertTrue(latch.await(5, TimeUnit.SECONDS));
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new AssertionError(e);
    }
  }

  private static class TestPullConsumer extends AbstractSubscriptionPullConsumer {

    private final Map<Integer, Integer> pollCounts = new HashMap<>();
    private final List<SubscriptionCommitContext> acknowledgedCommitContexts = new ArrayList<>();
    private final AtomicInteger createdProviderCount = new AtomicInteger();
    private final List<TestSubscriptionProvider> createdProviders = new CopyOnWriteArrayList<>();
    private volatile Runnable handshakeAction = () -> {};
    private final AtomicBoolean failFirstPoll;
    private final int dataProviderId;
    private final int emptyPollsBeforeData;
    private final boolean emptyTabletBatch;
    private final int providerCount;
    private int backoffCount;

    private TestPullConsumer(final int dataProviderId, final int emptyPollsBeforeData) {
      this(dataProviderId, emptyPollsBeforeData, false, false, 3);
    }

    private TestPullConsumer(
        final int dataProviderId, final int emptyPollsBeforeData, final boolean emptyTabletBatch) {
      this(dataProviderId, emptyPollsBeforeData, emptyTabletBatch, false, 3);
    }

    private TestPullConsumer(
        final int dataProviderId,
        final int emptyPollsBeforeData,
        final boolean emptyTabletBatch,
        final boolean failFirstPoll,
        final int providerCount) {
      this(
          dataProviderId,
          emptyPollsBeforeData,
          emptyTabletBatch,
          failFirstPoll,
          providerCount,
          false);
    }

    private TestPullConsumer(
        final int dataProviderId,
        final int emptyPollsBeforeData,
        final boolean emptyTabletBatch,
        final boolean failFirstPoll,
        final int providerCount,
        final boolean autoCommit) {
      super(
          new AbstractSubscriptionPullConsumerBuilder()
              .host(HOST)
              .port(FIRST_PORT)
              .consumerId(CONSUMER_ID)
              .consumerGroupId(CONSUMER_GROUP_ID)
              .heartbeatIntervalMs(LONG_INTERVAL_MS)
              .endpointsSyncIntervalMs(LONG_INTERVAL_MS)
              .autoCommit(autoCommit)
              .autoCommitIntervalMs(LONG_INTERVAL_MS));
      this.dataProviderId = dataProviderId;
      this.emptyPollsBeforeData = emptyPollsBeforeData;
      this.emptyTabletBatch = emptyTabletBatch;
      this.failFirstPoll = new AtomicBoolean(failFirstPoll);
      this.providerCount = providerCount;
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
      createdProviderCount.incrementAndGet();
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
              pollCounts,
              dataProviderId,
              emptyPollsBeforeData,
              emptyTabletBatch,
              failFirstPoll,
              providerCount);
      provider.handshakeAction = () -> handshakeAction.run();
      createdProviders.add(provider);
      return provider;
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

    private List<SubscriptionCommitContext> getAcknowledgedCommitContexts() {
      return acknowledgedCommitContexts;
    }

    private int getCreatedProviderCount() {
      return createdProviderCount.get();
    }

    @Override
    protected void ack(final Iterable<SubscriptionMessage> messages) throws SubscriptionException {
      for (final SubscriptionMessage message : messages) {
        acknowledgedCommitContexts.add(message.getCommitContext());
      }
    }

    @Override
    void sleepAfterEmptyPollRound() {
      backoffCount++;
    }
  }

  private static class TestSubscriptionProvider extends AbstractSubscriptionProvider {

    private final AtomicInteger closeCount = new AtomicInteger();
    private final AtomicInteger sessionCloseCount = new AtomicInteger();
    private final AtomicInteger commitCount = new AtomicInteger();
    private volatile Runnable heartbeatAction = () -> {};
    private boolean emptyHeartbeatTopics;
    private Runnable handshakeAction = () -> {};
    private boolean failContinuation;
    private Runnable continuationAction = () -> {};
    private Runnable commitAction = () -> {};
    private Function<List<SubscriptionCommitContext>, CommitResult> commitResponse;
    private List<SubscriptionCommitContext> committed = Collections.emptyList();
    private Map<String, TopicProgress> committedProgressByTopic = Collections.emptyMap();
    private boolean batchCommits;
    private boolean failAllRemainingBatches;
    private boolean rejectFirstBatch;
    private final List<List<SubscriptionCommitContext>> commitBatches = new ArrayList<>();

    private final int dataNodeId;
    private final Map<Integer, Integer> pollCounts;
    private final int dataProviderId;
    private final int emptyPollsBeforeData;
    private final boolean emptyTabletBatch;
    private final AtomicBoolean failFirstPoll;
    private final int providerCount;

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
        final int emptyPollsBeforeData,
        final boolean emptyTabletBatch,
        final AtomicBoolean failFirstPoll,
        final int providerCount) {
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
      this.emptyTabletBatch = emptyTabletBatch;
      this.failFirstPoll = failFirstPoll;
      this.providerCount = providerCount;
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
      handshakeAction.run();
      setAvailable();
    }

    @Override
    synchronized void close() {
      closeCount.incrementAndGet();
      setUnavailable();
    }

    @Override
    synchronized void closeSession() {
      sessionCloseCount.incrementAndGet();
      setUnavailable();
    }

    @Override
    int getDataNodeId() {
      return dataNodeId;
    }

    @Override
    PipeSubscribeHeartbeatResp heartbeat(
        final List<SubscriptionCommitContext> processorBufferedCommitContexts) {
      heartbeatAction.run();
      final PipeSubscribeHeartbeatResp response = new PipeSubscribeHeartbeatResp();
      if (!emptyHeartbeatTopics) {
        response.getTopics().put(TOPIC, new TopicConfig());
      }
      for (int id = 1; id <= providerCount; id++) {
        response.getEndPoints().put(id, new TEndPoint(HOST, FIRST_PORT + id - 1));
      }
      return response;
    }

    @Override
    List<SubscriptionPollResponse> poll(
        final Set<String> topicNames,
        final long timeoutMs,
        final Map<String, TopicProgress> progressByTopic)
        throws SubscriptionException {
      pollCounts.merge(dataNodeId, 1, Integer::sum);
      if (dataNodeId == dataProviderId && failFirstPoll.compareAndSet(true, false)) {
        setUnavailable();
        throw new SubscriptionConnectionException("poll timed out");
      }
      if (dataNodeId != dataProviderId || pollCounts.get(dataNodeId) <= emptyPollsBeforeData) {
        return Collections.emptyList();
      }
      final SubscriptionCommitContext commitContext =
          new SubscriptionCommitContext(dataNodeId, 0, TOPIC, CONSUMER_GROUP_ID, 0L);
      if (emptyTabletBatch) {
        final int pollCount = pollCounts.get(dataNodeId);
        if (pollCount != emptyPollsBeforeData + 1) {
          return Collections.emptyList();
        }
        return new ArrayList<>(
            Collections.singletonList(
                new SubscriptionPollResponse(
                    SubscriptionPollResponseType.TABLETS.getType(),
                    new TabletsPayload(Collections.emptyMap(), 1),
                    commitContext)));
      }
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

    @Override
    List<SubscriptionPollResponse> pollTablets(
        final SubscriptionCommitContext commitContext, final int offset, final long timeoutMs)
        throws SubscriptionException {
      continuationAction.run();
      failContinuationIfRequested();
      if (emptyTabletBatch && dataNodeId == dataProviderId && offset == 1) {
        return Collections.singletonList(
            new SubscriptionPollResponse(
                SubscriptionPollResponseType.TABLETS.getType(),
                new TabletsPayload(Collections.emptyMap(), 0),
                commitContext));
      }
      return Collections.emptyList();
    }

    @Override
    List<SubscriptionPollResponse> pollFile(
        final SubscriptionCommitContext commitContext,
        final long writingOffset,
        final long timeoutMs) {
      continuationAction.run();
      failContinuationIfRequested();
      return Collections.emptyList();
    }

    @Override
    CommitResult commit(final List<SubscriptionCommitContext> contexts, final boolean nack) {
      commitCount.incrementAndGet();
      if (batchCommits) {
        return super.commit(contexts, nack);
      }
      commitAction.run();
      failContinuationIfRequested();
      if (commitResponse != null) {
        return commitResponse.apply(contexts);
      }
      committed = new ArrayList<>(contexts);
      return new CommitResult(contexts, committedProgressByTopic);
    }

    @Override
    CommitResult commitOnce(final List<SubscriptionCommitContext> contexts, final boolean nack) {
      commitBatches.add(new ArrayList<>(contexts));
      commitAction.run();
      if (commitBatches.size() == 2 || (failAllRemainingBatches && commitBatches.size() > 1)) {
        throw new SubscriptionConnectionException("read timed out", new SocketTimeoutException());
      }
      if (rejectFirstBatch && commitBatches.size() == 1) {
        return CommitResult.empty();
      }
      return new CommitResult(
          contexts, Collections.singletonMap(TOPIC, batchProgress(contexts.get(0))));
    }

    private void failContinuationIfRequested() {
      if (failContinuation) {
        setUnavailable();
        throw new SubscriptionConnectionException("transport failed");
      }
    }
  }
}
