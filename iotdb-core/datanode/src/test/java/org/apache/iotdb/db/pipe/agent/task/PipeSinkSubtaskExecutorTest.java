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
import org.apache.iotdb.db.pipe.agent.task.execution.PipeSinkSubtaskExecutor;
import org.apache.iotdb.db.pipe.agent.task.subtask.sink.PipeSinkSubtask;
import org.apache.iotdb.pipe.api.PipeConnector;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.concurrent.RejectedExecutionException;

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
}
