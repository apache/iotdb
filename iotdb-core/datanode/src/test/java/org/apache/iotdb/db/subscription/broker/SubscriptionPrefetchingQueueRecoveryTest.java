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

package org.apache.iotdb.db.subscription.broker;

import org.apache.iotdb.db.pipe.event.common.terminate.PipeTerminateEvent;
import org.apache.iotdb.db.subscription.event.SubscriptionEvent;
import org.apache.iotdb.db.subscription.event.pipe.SubscriptionPipeEvents;
import org.apache.iotdb.rpc.subscription.payload.poll.SubscriptionCommitContext;
import org.apache.iotdb.rpc.subscription.payload.poll.SubscriptionPollResponseType;
import org.apache.iotdb.rpc.subscription.payload.poll.TerminationPayload;
import org.apache.iotdb.session.subscription.util.PollTimer;

import org.junit.Test;

import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SubscriptionPrefetchingQueueRecoveryTest {

  @Test
  public void testDisconnectRestartsTsFileWithoutPoisoning() throws Exception {
    final SubscriptionPrefetchingQueue queue = newQueue();
    final Path file = Files.createTempFile("subscription-recovery-", ".tsfile");
    try {
      Files.write(file, new byte[] {1, 2, 3, 4});
      final SubscriptionEvent event =
          new SubscriptionEvent(
              mock(SubscriptionPipeEvents.class), file.toFile(), null, newContext());
      queue.prefetchEvent(event);
      assertSame(event, poll(queue));
      event.fetchNextResponse(Files.size(file));
      assertEquals(
          SubscriptionPollResponseType.FILE_SEAL.getType(),
          event.getCurrentResponse().getResponseType());

      // Response-size rejection retries the same response on the same transfer.
      assertTrue(queue.requeue("consumer", event.getCommitContext()));
      assertSame(event, poll(queue));
      assertEquals(
          SubscriptionPollResponseType.FILE_SEAL.getType(),
          event.getCurrentResponse().getResponseType());

      // Disconnects restart the protocol, even after enough retries to poison a nacked event.
      for (int retry = 0; retry < 12; retry++) {
        assertEquals(1, queue.requeueInFlightEvents("consumer"));
        assertSame(event, poll(queue));
        assertEquals(
            SubscriptionPollResponseType.FILE_INIT.getType(),
            event.getCurrentResponse().getResponseType());
        assertFalse(event.isCommittable());
        assertEquals(0L, event.getNackCount());
        event.fetchNextResponse(Files.size(file));
      }
      assertTrue(queue.ack("consumer", event.getCommitContext()));
      assertEquals(0L, queue.getSubscriptionUncommittedEventCount());
    } finally {
      queue.cleanUp();
      Files.deleteIfExists(file);
    }
  }

  @Test
  public void testSnapshotCannotCompleteDuringPollV2Transfer() throws Exception {
    final SubscriptionPrefetchingQueue queue = newQueue();
    final CountDownLatch transferStarted = new CountDownLatch(1);
    final CountDownLatch resumeTransfer = new CountDownLatch(1);
    final CountDownLatch prefetchStarted = new CountDownLatch(1);
    final CountDownLatch completion = new CountDownLatch(1);
    final ExecutorService executor = Executors.newFixedThreadPool(2);
    try {
      final SubscriptionEvent event =
          new SubscriptionEvent(
              SubscriptionPollResponseType.TERMINATION.getType(),
              new TerminationPayload(),
              newContext()) {
            @Override
            public void recordLastPolledTimestamp() {
              transferStarted.countDown();
              try {
                assertTrue(resumeTransfer.await(5, TimeUnit.SECONDS));
              } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new AssertionError(e);
              }
              super.recordLastPolledTimestamp();
            }
          };
      queue.prefetchEvent(event);
      final PipeTerminateEvent terminate = mock(PipeTerminateEvent.class);
      doAnswer(
              invocation -> {
                completion.countDown();
                return null;
              })
          .when(terminate)
          .addOnCommittedHook(any(Runnable.class));
      final Field terminateField =
          SubscriptionPrefetchingQueue.class.getDeclaredField("currentTerminateEvent");
      terminateField.setAccessible(true);
      terminateField.set(queue, terminate);

      final Future<SubscriptionEvent> polled = executor.submit(() -> poll(queue));
      assertTrue(transferStarted.await(5, TimeUnit.SECONDS));
      // The event has left the prefetch queue but is not yet in the in-flight map.
      final Future<?> prefetched =
          executor.submit(
              () -> {
                prefetchStarted.countDown();
                queue.executePrefetchV2();
              });
      assertTrue(prefetchStarted.await(5, TimeUnit.SECONDS));
      assertFalse(
          "Snapshot completion must wait for the unacknowledged delivery",
          completion.await(200, TimeUnit.MILLISECONDS));
      resumeTransfer.countDown();
      assertSame(event, polled.get(5, TimeUnit.SECONDS));
      prefetched.get(5, TimeUnit.SECONDS);
      assertEquals(1L, completion.getCount());
      assertTrue(queue.ack("consumer", event.getCommitContext()));
      queue.executePrefetchV2();
      assertEquals(0L, completion.getCount());
    } finally {
      resumeTransfer.countDown();
      executor.shutdownNow();
      assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
      queue.cleanUp();
    }
  }

  private static SubscriptionPrefetchingQueue newQueue() {
    final SubscriptionBlockingPendingQueue inputQueue =
        mock(SubscriptionBlockingPendingQueue.class);
    when(inputQueue.isEmpty()).thenReturn(true);
    return new SubscriptionPrefetchingTabletQueue(
        "recovery_group", "recovery_topic", inputQueue, new AtomicLong());
  }

  private static SubscriptionCommitContext newContext() {
    return new SubscriptionCommitContext(1, 1, "recovery_topic", "recovery_group", 1);
  }

  private static SubscriptionEvent poll(final SubscriptionPrefetchingQueue queue) {
    return queue.pollV2("consumer", new PollTimer(System.currentTimeMillis(), 0L));
  }
}
