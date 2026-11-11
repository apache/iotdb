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

package org.apache.iotdb.confignode.procedure.scheduler;

import org.apache.iotdb.confignode.procedure.Procedure;

import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DeleteTimeSeriesProcedureSchedulerTest {
  private Procedure procedure(long id) {
    final Procedure procedure = mock(Procedure.class);
    when(procedure.getProcId()).thenReturn(id);
    return procedure;
  }

  @Test
  public void testConcurrencySlotSurvivesStateTransitionsAndRollback() {
    final DeleteTimeSeriesProcedureScheduler scheduler =
        new DeleteTimeSeriesProcedureScheduler(1, 1);
    final Procedure first = procedure(1);
    final Procedure second = procedure(2);
    scheduler.start();
    scheduler.addBack(first);
    scheduler.addBack(second);
    assertSame(first, scheduler.poll());
    assertTrue(scheduler.isQueued(2));
    assertEquals(1, scheduler.getWaitingTaskCount());
    scheduler.addFront(first);
    assertSame(first, scheduler.poll(0, TimeUnit.MILLISECONDS));
    // A root remains admitted even while none of its states is runnable.
    assertNull(scheduler.poll(0, TimeUnit.MILLISECONDS));
    scheduler.completionCleanup(first);
    assertSame(second, scheduler.poll(1, TimeUnit.SECONDS));
    scheduler.stop();
  }

  @Test
  public void testStartIntervalAlsoAppliesWhenMultipleSlotsAreAvailable() {
    final DeleteTimeSeriesProcedureScheduler scheduler =
        new DeleteTimeSeriesProcedureScheduler(2, 100);
    final Procedure first = procedure(1);
    final Procedure second = procedure(2);
    final Procedure third = procedure(3);
    // All tasks may be queued before any worker starts. Their execution starts still need spacing.
    scheduler.addBack(first);
    scheduler.addBack(second);
    scheduler.addBack(third);
    scheduler.start();
    final long started = System.nanoTime();
    assertSame(first, scheduler.poll());
    assertSame(second, scheduler.poll(2, TimeUnit.SECONDS));
    assertTrue(System.nanoTime() - started >= TimeUnit.MILLISECONDS.toNanos(100));
    assertNull(scheduler.poll(0, TimeUnit.MILLISECONDS));
    scheduler.completionCleanup(first);
    assertSame(third, scheduler.poll(2, TimeUnit.SECONDS));
    scheduler.stop();
  }

  @Test
  public void testExecutionIntervalCanBeReloadedWhileWorkerWaits() throws Exception {
    final DeleteTimeSeriesProcedureScheduler scheduler =
        new DeleteTimeSeriesProcedureScheduler(2, TimeUnit.HOURS.toMillis(1));
    final Procedure first = procedure(1);
    final Procedure second = procedure(2);
    final ExecutorService executor = Executors.newSingleThreadExecutor();
    try {
      scheduler.start();
      scheduler.addBack(first);
      scheduler.addBack(second);
      assertSame(first, scheduler.poll());
      final CountDownLatch polling = new CountDownLatch(1);
      final Future<Procedure> waiting =
          executor.submit(
              () -> {
                polling.countDown();
                return scheduler.poll();
              });
      assertTrue(polling.await(1, TimeUnit.SECONDS));
      try {
        waiting.get(50, TimeUnit.MILLISECONDS);
        throw new AssertionError("The second task should wait for the configured interval");
      } catch (TimeoutException expected) {
        // The worker is waiting for the current interval.
      }

      scheduler.setExecutionIntervalMs(1);

      assertSame(second, waiting.get(1, TimeUnit.SECONDS));
    } finally {
      scheduler.stop();
      executor.shutdownNow();
    }
  }

  @Test
  public void testIncreasingIntervalDoesNotThrottleAnActiveTask() {
    final DeleteTimeSeriesProcedureScheduler scheduler =
        new DeleteTimeSeriesProcedureScheduler(2, 1);
    final Procedure first = procedure(1);
    final Procedure second = procedure(2);
    scheduler.start();
    try {
      scheduler.addBack(first);
      scheduler.addBack(second);
      assertSame(first, scheduler.poll());
      scheduler.setExecutionIntervalMs(TimeUnit.HOURS.toMillis(1));
      assertNull(scheduler.poll(0, TimeUnit.MILLISECONDS));
      scheduler.addFront(first);
      assertSame(first, scheduler.poll(0, TimeUnit.MILLISECONDS));
      scheduler.completionCleanup(first);
      assertNull(scheduler.poll(0, TimeUnit.MILLISECONDS));
      scheduler.setExecutionIntervalMs(1);
      assertSame(second, scheduler.poll(1, TimeUnit.SECONDS));
    } finally {
      scheduler.stop();
    }
  }

  @Test
  public void testEnqueueDoesNotWaitForExecutionInterval() throws Exception {
    final DeleteTimeSeriesProcedureScheduler scheduler =
        new DeleteTimeSeriesProcedureScheduler(1, TimeUnit.HOURS.toMillis(1));
    final ExecutorService submitter = Executors.newSingleThreadExecutor();
    try {
      final Future<?> submitted =
          submitter.submit(
              () -> {
                scheduler.addBack(procedure(1));
                scheduler.addBack(procedure(2));
                scheduler.addBack(procedure(3));
              });
      submitted.get(5, TimeUnit.SECONDS);
      assertEquals(3, scheduler.getWaitingTaskCount());
    } finally {
      scheduler.stop();
      submitter.shutdownNow();
    }
  }

  @Test
  public void testStopUnblocksAdmissionAndClearAllowsRecovery() throws Exception {
    final DeleteTimeSeriesProcedureScheduler scheduler =
        new DeleteTimeSeriesProcedureScheduler(1, 1);
    final Procedure first = procedure(1);
    final Procedure recovered = procedure(2);
    scheduler.start();
    scheduler.addBack(first);
    assertSame(first, scheduler.poll());
    scheduler.addBack(recovered);
    final ExecutorService executor = Executors.newSingleThreadExecutor();
    try {
      final CountDownLatch started = new CountDownLatch(1);
      final Future<Procedure> waiting =
          executor.submit(
              () -> {
                started.countDown();
                return scheduler.poll();
              });
      assertTrue(started.await(1, TimeUnit.SECONDS));
      scheduler.stop();
      assertNull(waiting.get(1, TimeUnit.SECONDS));
      scheduler.clear();
      scheduler.start();
      scheduler.addBack(recovered);
      assertSame(recovered, scheduler.poll(0, TimeUnit.MILLISECONDS));
    } finally {
      scheduler.stop();
      executor.shutdownNow();
    }
  }
}
