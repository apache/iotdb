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
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

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

  @Test
  public void testStartAfterShutdown() {
    executor.register(subtask);
    executor.shutdown();
    Assert.assertTrue(executor.isShutdown());

    // The submission is rejected by the shut-down executor, and the rejection must be reported
    // instead of being swallowed as "started successfully"...
    for (int i = 0; i < 2; ++i) {
      try {
        executor.start(subtask.getTaskID());
        Assert.fail("The start of a subtask should fail if the executor is shut down");
      } catch (final RejectedExecutionException e) {
        // expected
      }
      // ... and the failed submission must not leave the subtask in the "submitting" state.
      Assert.assertFalse(subtask.isSubmittingSelf());
    }
  }

  @Test
  public void testSubmitSelfAfterShutdown() throws Exception {
    executor.register(subtask);
    executor.shutdown();
    allowSubmittingSelfAgain(subtask);

    // The subtask submits itself again after each round, so a rejected submission must be reported
    // instead of being swallowed. Otherwise the subtask would stop transferring events silently.
    for (int i = 0; i < 2; ++i) {
      try {
        subtask.submitSelf();
        Assert.fail("The submission of a subtask should fail if the executor is shut down");
      } catch (final RejectedExecutionException e) {
        // expected
      }
    }
  }

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

    // A delayed submission marks the subtask as submitted before scheduling it. If the scheduling
    // is rejected, the sign must be cleared as well, otherwise every following submission would be
    // skipped and the subtask would never be submitted again.
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
   * Simulates the race where a subtask is submitting itself while its executor is being shut down:
   * the executor can not submit it any more, but the subtask has not been told to stop yet.
   */
  private void allowSubmittingSelfAgain(final PipeSubtask subtask) throws Exception {
    final Field shouldStopSubmittingSelfField =
        PipeSubtask.class.getDeclaredField("shouldStopSubmittingSelf");
    shouldStopSubmittingSelfField.setAccessible(true);
    ((AtomicBoolean) shouldStopSubmittingSelfField.get(subtask)).set(false);
  }

  private interface SinkWithSchedulingDelay extends PipeConnector, PipeSinkWithSchedulingDelay {}
}
