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

import org.apache.iotdb.db.pipe.agent.task.connection.PipeEventCollector;
import org.apache.iotdb.pipe.api.PipeProcessor;
import org.apache.iotdb.pipe.api.event.Event;

import org.junit.Test;
import org.mockito.ArgumentCaptor;

import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

public class PipeProcessorSubtaskTest {

  @Test
  public void testGenericControlEventBypassesProcessor() throws Exception {
    final Event event =
        new Event() {
          @Override
          public boolean shouldSkipProcessing() {
            return true;
          }
        };
    final PipeProcessor processor = mock(PipeProcessor.class);
    final PipeEventCollector collector = mock(PipeEventCollector.class);
    final PipeProcessorSubtask subtask =
        new PipeProcessorSubtask("test", "pipe", 1L, 1, () -> event, processor, collector);
    try {
      assertTrue(executeOnce(subtask, collector));
      verify(collector).collect(event);
      verifyNoMoreInteractions(processor);
    } finally {
      subtask.close();
    }
  }

  @Test
  public void testOrdinaryEventStillReachesProcessor() throws Exception {
    final Event event = new Event() {};
    final PipeProcessor processor = mock(PipeProcessor.class);
    final PipeEventCollector collector = mock(PipeEventCollector.class);
    final PipeProcessorSubtask subtask =
        new PipeProcessorSubtask("test", "pipe", 1L, 1, () -> event, processor, collector);
    try {
      assertTrue(executeOnce(subtask, collector));
      verify(processor).process(event, collector);
    } finally {
      subtask.close();
    }
  }

  private static boolean executeOnce(
      final PipeProcessorSubtask subtask, final PipeEventCollector collector) throws Exception {
    final ArgumentCaptor<PipeProcessorSubtaskExecutionGuard> guardCaptor =
        ArgumentCaptor.forClass(PipeProcessorSubtaskExecutionGuard.class);
    verify(collector).setProcessorExecutionGuard(guardCaptor.capture());
    final PipeProcessorSubtaskExecutionGuard guard = guardCaptor.getValue();
    subtask.onAllowSubmittingSelf();
    guard.enter();
    try {
      return subtask.executeOnce();
    } finally {
      guard.exit();
    }
  }
}
