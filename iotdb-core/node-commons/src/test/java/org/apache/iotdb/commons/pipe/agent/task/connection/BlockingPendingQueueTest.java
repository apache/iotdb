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

package org.apache.iotdb.commons.pipe.agent.task.connection;

import org.apache.iotdb.commons.pipe.agent.task.connection.BlockingPendingQueue.PendingEventMemoryReservation;
import org.apache.iotdb.commons.pipe.metric.PipeEventCounter;
import org.apache.iotdb.pipe.api.event.Event;

import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.mockito.Mockito.mock;

public class BlockingPendingQueueTest {

  @Test
  public void testMemoryReservationReleasedAfterPolling() throws Exception {
    final UnboundedBlockingPendingQueue<Event> queue =
        new UnboundedBlockingPendingQueue<>(mock(PipeEventCounter.class));
    final Event firstEvent = mock(Event.class);
    final Event secondEvent = mock(Event.class);

    final PendingEventMemoryReservation firstReservation = queue.waitForMemoryReservation(6, 10);
    Assert.assertNotNull(firstReservation);
    Assert.assertTrue(queue.offer(firstEvent, firstReservation));
    Assert.assertEquals(6, queue.getPendingEventMemoryUsageInBytes());

    final ExecutorService executor = Executors.newSingleThreadExecutor();
    final CountDownLatch waiterStarted = new CountDownLatch(1);
    final Future<PendingEventMemoryReservation> blockedReservation =
        executor.submit(
            () -> {
              waiterStarted.countDown();
              return queue.waitForMemoryReservation(5, 10);
            });
    try {
      Assert.assertTrue(waiterStarted.await(5, TimeUnit.SECONDS));
      assertStillBlocked(blockedReservation);

      Assert.assertSame(firstEvent, queue.directPoll());
      final PendingEventMemoryReservation secondReservation =
          blockedReservation.get(5, TimeUnit.SECONDS);
      Assert.assertNotNull(secondReservation);
      Assert.assertEquals(5, queue.getPendingEventMemoryUsageInBytes());

      Assert.assertTrue(queue.offer(secondEvent, secondReservation));
      Assert.assertSame(secondEvent, queue.pollLast());
      Assert.assertEquals(0, queue.getPendingEventMemoryUsageInBytes());
    } finally {
      blockedReservation.cancel(true);
      queue.discardAllEvents();
      executor.shutdownNow();
      Assert.assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
    }
  }

  @Test
  public void testClearWakesBlockedMemoryReservation() throws Exception {
    final UnboundedBlockingPendingQueue<Event> queue =
        new UnboundedBlockingPendingQueue<>(mock(PipeEventCounter.class));
    final PendingEventMemoryReservation firstReservation = queue.waitForMemoryReservation(8, 10);
    Assert.assertNotNull(firstReservation);

    final ExecutorService executor = Executors.newSingleThreadExecutor();
    final CountDownLatch waiterStarted = new CountDownLatch(1);
    final Future<PendingEventMemoryReservation> blockedReservation =
        executor.submit(
            () -> {
              waiterStarted.countDown();
              return queue.waitForMemoryReservation(8, 10);
            });
    try {
      Assert.assertTrue(waiterStarted.await(5, TimeUnit.SECONDS));
      assertStillBlocked(blockedReservation);

      queue.clear();
      Assert.assertNull(blockedReservation.get(5, TimeUnit.SECONDS));
      Assert.assertEquals(0, queue.getPendingEventMemoryUsageInBytes());
    } finally {
      firstReservation.close();
      blockedReservation.cancel(true);
      executor.shutdownNow();
      Assert.assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
    }
  }

  private static void assertStillBlocked(final Future<?> future) throws Exception {
    try {
      future.get(200, TimeUnit.MILLISECONDS);
      Assert.fail("Expected memory reservation to remain blocked");
    } catch (final TimeoutException expected) {
      // Expected.
    }
  }
}
