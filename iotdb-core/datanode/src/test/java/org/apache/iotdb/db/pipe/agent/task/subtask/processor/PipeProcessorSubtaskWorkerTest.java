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

package org.apache.iotdb.db.pipe.agent.task.subtask.processor;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.InOrder;

import java.util.LinkedHashSet;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class PipeProcessorSubtaskWorkerTest {

  @Test
  public void testYieldingPipesDoNotBlockAnotherPipeOnSameWorker() throws Exception {
    final PipeProcessorSubtaskWorker worker = new PipeProcessorSubtaskWorker(new LinkedHashSet<>());
    final PipeProcessorSubtask stoppedPipe = createRunnableSubtask("stoppedPipe");
    final PipeProcessorSubtask parserWaitingPipe = createRunnableSubtask("parserWaitingPipe");
    final PipeProcessorSubtask runningPipe = createRunnableSubtask("runningPipe");

    when(stoppedPipe.call()).thenThrow(PipeProcessorSubtaskYieldException.pauseRequested());
    when(parserWaitingPipe.call())
        .thenThrow(PipeProcessorSubtaskYieldException.parserNotAdmitted());
    when(runningPipe.call()).thenReturn(true);

    worker.schedule(stoppedPipe);
    worker.schedule(parserWaitingPipe);
    worker.schedule(runningPipe);

    Assert.assertFalse(worker.runSubtasks());

    final InOrder inOrder = inOrder(stoppedPipe, parserWaitingPipe, runningPipe);
    inOrder.verify(stoppedPipe).call();
    inOrder.verify(parserWaitingPipe).call();
    inOrder.verify(runningPipe).call();
    verify(runningPipe).onSuccess(true);
    verify(stoppedPipe, never()).onSuccess(any());
    verify(stoppedPipe, never()).onFailure(any());
    verify(parserWaitingPipe, never()).onSuccess(any());
    verify(parserWaitingPipe, never()).onFailure(any());
  }

  private PipeProcessorSubtask createRunnableSubtask(final String mockName) {
    final PipeProcessorSubtask subtask = mock(PipeProcessorSubtask.class, mockName);
    when(subtask.isClosed()).thenReturn(false);
    when(subtask.isSubmittingSelf()).thenReturn(true);
    when(subtask.isStoppedByException()).thenReturn(false);
    return subtask;
  }
}
