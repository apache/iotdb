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

package org.apache.iotdb.db.pipe.agent.task;

import org.apache.iotdb.commons.pipe.agent.task.connection.UnboundedBlockingPendingQueue;
import org.apache.iotdb.commons.pipe.agent.task.subtask.PipeSubtask;
import org.apache.iotdb.commons.pipe.sink.protocol.PipeSinkWithSchedulingDelay;
import org.apache.iotdb.db.pipe.agent.task.execution.PipeSinkSubtaskExecutor;
import org.apache.iotdb.db.pipe.agent.task.subtask.sink.PipeSinkSubtask;
import org.apache.iotdb.pipe.api.PipeConnector;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.lang.reflect.Field;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.mockito.Mockito.mock;

public class PipeSinkSubtaskExecutorTest extends PipeSubtaskExecutorTest {

  @Before
  public void setUp() throws Exception {
    executor = new PipeSinkSubtaskExecutor();

    subtask =
        Mockito.spy(
            new PipeSinkSubtask(
                "PipeConnectorSubtaskExecutorTest",
                System.currentTimeMillis(),
                "TestAttributeSortedString",
                "TestAttributeSortedString",
                0,
                mock(UnboundedBlockingPendingQueue.class),
                mock(PipeConnector.class)));
  }

  /**
   * A shut-down executor must reject {@code start()} and must not leave the subtask in the
   * "submitting" state.
   */
  @Test
  public void testStartAfterShutdown() {
    executor.register(subtask);
    executor.shutdown();
    Assert.assertTrue(executor.isShutdown());

    for (int i = 0; i < 2; ++i) {
      try {
        executor.start(subtask.getTaskID());
        Assert.fail("The start of a subtask should fail if the executor is shut down");
      } catch (final RejectedExecutionException e) {
        // expected
      }
      Assert.assertFalse(subtask.isSubmittingSelf());
    }
  }

  /** A rejected self-submission must be reported instead of being swallowed. */
  @Test
  public void testSubmitSelfAfterShutdown() throws Exception {
    executor.register(subtask);
    executor.shutdown();
    allowSubmittingSelfAgain(subtask);

    for (int i = 0; i < 2; ++i) {
      try {
        subtask.submitSelf();
        Assert.fail("The submission of a subtask should fail if the executor is shut down");
      } catch (final RejectedExecutionException e) {
        // expected
      }
    }
  }

  /**
   * A rejected delayed submission must clear the submitted flag, otherwise the following
   * submissions would be skipped.
   */
  @Test
  public void testDelayedSubmitSelfAfterShutdown() throws Exception {
    final SinkWithSchedulingDelay sink = mock(SinkWithSchedulingDelay.class);
    Mockito.when(sink.consumeSchedulingDelayMs()).thenReturn(1000L);

    final PipeSubtask delayedSubtask =
        Mockito.spy(
            new PipeSinkSubtask(
                "PipeConnectorSubtaskExecutorTest",
                System.currentTimeMillis(),
                "TestAttributeSortedString",
                "TestAttributeSortedString",
                0,
                mock(UnboundedBlockingPendingQueue.class),
                sink));

    executor.register(delayedSubtask);
    executor.shutdown();
    allowSubmittingSelfAgain(delayedSubtask);

    for (int i = 0; i < 2; ++i) {
      try {
        delayedSubtask.submitSelf();
        Assert.fail("The submission of a subtask should fail if the executor is shut down");
      } catch (final RejectedExecutionException e) {
        // expected
      }
    }
  }

  /**
   * Stopping and restarting a subtask while its previous call is still running must not start a
   * second concurrent call.
   */
  @Test
  public void testStartWhileThePreviousCallIsStillRunning() throws Exception {
    final AtomicInteger concurrentCallCount = new AtomicInteger(0);
    final AtomicInteger maxConcurrentCallCount = new AtomicInteger(0);
    final CountDownLatch firstCallEntered = new CountDownLatch(1);
    final CountDownLatch firstCallReleased = new CountDownLatch(1);
    final CountDownLatch secondCallEntered = new CountDownLatch(1);

    final PipeSubtask blockedSubtask =
        new PipeSinkSubtask(
            "PipeConnectorSubtaskExecutorTest",
            System.currentTimeMillis(),
            "TestAttributeSortedString",
            "TestAttributeSortedString",
            0,
            mock(UnboundedBlockingPendingQueue.class),
            mock(PipeConnector.class)) {
          @Override
          protected boolean executeOnce() {
            final int current = concurrentCallCount.incrementAndGet();
            maxConcurrentCallCount.accumulateAndGet(current, Math::max);
            if (current == 1) {
              firstCallEntered.countDown();
              try {
                firstCallReleased.await(5, TimeUnit.SECONDS);
              } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
              }
            } else {
              secondCallEntered.countDown();
            }
            concurrentCallCount.decrementAndGet();
            return false;
          }
        };

    executor.register(blockedSubtask);
    executor.start(blockedSubtask.getTaskID());
    Assert.assertTrue(firstCallEntered.await(5, TimeUnit.SECONDS));

    executor.stop(blockedSubtask.getTaskID());
    executor.start(blockedSubtask.getTaskID());

    try {
      Assert.assertFalse(
          "The subtask must not be executed in parallel with itself",
          secondCallEntered.await(1, TimeUnit.SECONDS));
      Assert.assertEquals(1, maxConcurrentCallCount.get());
    } finally {
      executor.stop(blockedSubtask.getTaskID());
      firstCallReleased.countDown();
    }
  }

  /**
   * {@code start()} must fail after shutdown even if a pending delayed submission already marked
   * the subtask as submitted.
   */
  @Test
  public void testStartAfterShutdownWithPendingDelayedSubmission() {
    final SinkWithSchedulingDelay sink = mock(SinkWithSchedulingDelay.class);
    Mockito.when(sink.consumeSchedulingDelayMs()).thenReturn(60_000L);

    final PipeSubtask delayedSubtask =
        new PipeSinkSubtask(
            "PipeConnectorSubtaskExecutorTest",
            System.currentTimeMillis(),
            "TestAttributeSortedString",
            "TestAttributeSortedString",
            0,
            mock(UnboundedBlockingPendingQueue.class),
            sink);

    executor.register(delayedSubtask);
    executor.start(delayedSubtask.getTaskID());

    executor.shutdown();
    try {
      executor.start(delayedSubtask.getTaskID());
      Assert.fail("The start of a subtask should fail if the executor is shut down");
    } catch (final RejectedExecutionException e) {
      // expected
    }
  }

  /** Clears the stop flag to simulate a subtask that has not been told to stop yet. */
  private void allowSubmittingSelfAgain(final PipeSubtask subtask) throws Exception {
    final Field shouldStopSubmittingSelfField =
        PipeSubtask.class.getDeclaredField("shouldStopSubmittingSelf");
    shouldStopSubmittingSelfField.setAccessible(true);
    ((AtomicBoolean) shouldStopSubmittingSelfField.get(subtask)).set(false);
  }

  private interface SinkWithSchedulingDelay extends PipeConnector, PipeSinkWithSchedulingDelay {}
}
