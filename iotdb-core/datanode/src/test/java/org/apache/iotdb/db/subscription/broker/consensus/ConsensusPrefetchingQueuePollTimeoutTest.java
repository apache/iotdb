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

package org.apache.iotdb.db.subscription.broker.consensus;

import org.apache.iotdb.commons.consensus.DataRegionId;
import org.apache.iotdb.commons.subscription.config.SubscriptionConfig;
import org.apache.iotdb.consensus.iot.IoTConsensusServerImpl;
import org.apache.iotdb.consensus.iot.SubscriptionWalRetentionPolicy;
import org.apache.iotdb.consensus.iot.log.ConsensusReqReader;
import org.apache.iotdb.db.subscription.event.SubscriptionEvent;
import org.apache.iotdb.rpc.subscription.config.TopicConstant;
import org.apache.iotdb.rpc.subscription.payload.poll.RegionProgress;
import org.apache.iotdb.rpc.subscription.payload.poll.SubscriptionCommitContext;
import org.apache.iotdb.rpc.subscription.payload.poll.SubscriptionPollResponse;
import org.apache.iotdb.rpc.subscription.payload.poll.SubscriptionPollResponseType;
import org.apache.iotdb.session.subscription.util.PollTimer;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.Collections;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ConsensusPrefetchingQueuePollTimeoutTest {

  private ConsensusPrefetchingQueue queue;
  private PriorityBlockingQueue<SubscriptionEvent> events;

  @Before
  @SuppressWarnings("unchecked")
  public void setUp() throws Exception {
    final IoTConsensusServerImpl server = mock(IoTConsensusServerImpl.class);
    when(server.getConsensusReqReader()).thenReturn(mock(ConsensusReqReader.class));
    queue =
        new ConsensusPrefetchingQueue(
            "group",
            "topic",
            TopicConstant.ORDER_MODE_MULTI_WRITER_VALUE,
            new DataRegionId(1),
            server,
            mock(SubscriptionWalRetentionPolicy.class),
            mock(ConsensusLogToTabletConverter.class),
            mock(ConsensusSubscriptionCommitManager.class),
            new RegionProgress(Collections.emptyMap()),
            1L,
            1L,
            true);
    final Field initialized =
        ConsensusPrefetchingQueue.class.getDeclaredField("prefetchInitialized");
    initialized.setAccessible(true);
    initialized.setBoolean(queue, true);
    events = mock(PriorityBlockingQueue.class);
    final Field eventQueue = ConsensusPrefetchingQueue.class.getDeclaredField("prefetchingQueue");
    eventQueue.setAccessible(true);
    eventQueue.set(queue, events);
  }

  @After
  public void tearDown() {
    queue.close();
  }

  @Test
  public void testQueueWaitIsLimitedByRemainingBudget() throws Exception {
    when(events.size()).thenReturn(1);
    final PollTimer timer = newManualTimer(20L);
    when(events.poll(anyLong(), eq(TimeUnit.MILLISECONDS)))
        .thenAnswer(
            invocation -> {
              assertEquals(
                  Math.min(
                      20L, SubscriptionConfig.getInstance().getSubscriptionPollMaxBlockingTimeMs()),
                  (long) invocation.getArgument(0));
              return null;
            });

    assertNull(queue.poll("consumer", null, timer));
    verify(events).poll(anyLong(), eq(TimeUnit.MILLISECONDS));
  }

  @Test
  public void testRetryUsesDecreasingBudgetAndStopsBeforeRemovingAnotherEvent() throws Exception {
    final SubscriptionEvent committed = mock(SubscriptionEvent.class);
    final SubscriptionPollResponse response = mock(SubscriptionPollResponse.class);
    when(response.getResponseType()).thenReturn(SubscriptionPollResponseType.TABLETS.getType());
    when(committed.getCurrentResponse()).thenReturn(response);
    when(committed.getCommitContext()).thenReturn(mock(SubscriptionCommitContext.class));
    when(committed.isCommitted()).thenReturn(true);
    when(events.size()).thenReturn(3);
    final PollTimer timer = newManualTimer(20L);
    final AtomicInteger attempts = new AtomicInteger();
    when(events.poll(anyLong(), eq(TimeUnit.MILLISECONDS)))
        .thenAnswer(
            invocation -> {
              final int attempt = attempts.getAndIncrement();
              assertEquals(
                  Math.min(
                      attempt == 0 ? 20L : 10L,
                      SubscriptionConfig.getInstance().getSubscriptionPollMaxBlockingTimeMs()),
                  (long) invocation.getArgument(0));
              timer.update(timer.currentTimeMs() + 10L);
              return committed;
            });

    assertNull(queue.poll("consumer", null, timer));
    assertEquals(2, attempts.get());
  }

  @Test
  public void testExpiredBudgetDoesNotRemoveQueuedEvents() throws Exception {
    when(events.size()).thenReturn(1);
    assertNull(queue.poll("consumer", null, newManualTimer(0L)));
    verify(events, never()).poll(anyLong(), eq(TimeUnit.MILLISECONDS));
  }

  @Test
  public void testReadLockWaitIsLimitedByRemainingBudget() throws Exception {
    final Field field = ConsensusPrefetchingQueue.class.getDeclaredField("lock");
    field.setAccessible(true);
    final ReentrantReadWriteLock lock = (ReentrantReadWriteLock) field.get(queue);
    final ExecutorService executor = Executors.newSingleThreadExecutor();
    lock.writeLock().lock();
    try {
      final Future<SubscriptionEvent> result =
          executor.submit(
              () -> queue.poll("consumer", null, new PollTimer(System.currentTimeMillis(), 50L)));
      // The writer stays locked until the poll returns; an unbounded read lock would time out here.
      assertNull(result.get(5L, TimeUnit.SECONDS));
      verify(events, never()).poll(anyLong(), eq(TimeUnit.MILLISECONDS));
    } finally {
      lock.writeLock().unlock();
      executor.shutdownNow();
      assertTrue(executor.awaitTermination(5L, TimeUnit.SECONDS));
    }
  }

  private static PollTimer newManualTimer(final long timeoutMs) {
    return new PollTimer(System.currentTimeMillis(), timeoutMs) {
      @Override
      public void update() {
        // Tests advance the shared clock explicitly without wall-clock timing assertions.
      }
    };
  }
}
