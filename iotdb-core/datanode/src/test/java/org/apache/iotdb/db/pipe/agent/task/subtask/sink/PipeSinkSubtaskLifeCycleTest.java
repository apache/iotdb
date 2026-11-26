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

package org.apache.iotdb.db.pipe.agent.task.subtask.sink;

import org.apache.iotdb.commons.pipe.agent.task.connection.UnboundedBlockingPendingQueue;
import org.apache.iotdb.db.pipe.agent.task.execution.PipeSinkSubtaskExecutor;

import org.junit.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class PipeSinkSubtaskLifeCycleTest {

  @Test
  public void testStopDoesNotDiscardReceiverRuntimeSessions() {
    final PipeSinkSubtask subtask = mock(PipeSinkSubtask.class);
    when(subtask.getTaskID()).thenReturn("test-subtask");
    when(subtask.getDisplayTaskID()).thenReturn("test-subtask");

    final PipeSinkSubtaskExecutor executor =
        new PipeSinkSubtaskExecutor(1, "PipeSinkSubtaskLifeCycleTest");
    executor.register(subtask);
    final PipeSinkSubtaskLifeCycle lifeCycle =
        new PipeSinkSubtaskLifeCycle(executor, subtask, mock(UnboundedBlockingPendingQueue.class));
    lifeCycle.runningTaskCount = 1;

    try {
      lifeCycle.stop();

      verify(subtask, never()).discardReceiverRuntimeSessions();
      lifeCycle.discardReceiverRuntimeSessions("pipe", 1);
      verify(subtask).discardReceiverRuntimeSessions("pipe", 1);
    } finally {
      executor.deregister(subtask.getTaskID());
      executor.shutdown();
    }
  }
}
