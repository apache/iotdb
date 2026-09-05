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

import org.apache.iotdb.commons.pipe.agent.plugin.builtin.processor.donothing.DoNothingProcessor;
import org.apache.iotdb.commons.pipe.agent.task.connection.EventSupplier;
import org.apache.iotdb.commons.pipe.agent.task.execution.PipeSubtaskScheduler;
import org.apache.iotdb.commons.pipe.event.ProgressReportEvent;
import org.apache.iotdb.db.pipe.agent.task.connection.PipeEventCollector;
import org.apache.iotdb.db.pipe.agent.task.execution.PipeProcessorSubtaskExecutor;
import org.apache.iotdb.db.pipe.agent.task.subtask.processor.PipeProcessorSubtask;
import org.apache.iotdb.db.pipe.event.common.heartbeat.PipeHeartbeatEvent;
import org.apache.iotdb.db.pipe.event.common.tsfile.PipeTsFileInsertionEvent;
import org.apache.iotdb.pipe.api.PipeProcessor;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class PipeProcessorSubtaskExecutorTest extends PipeSubtaskExecutorTest {

  @Before
  public void setUp() throws Exception {
    executor = new PipeProcessorSubtaskExecutor();

    subtask =
        Mockito.spy(
            new PipeProcessorSubtask(
                "PipeProcessorSubtaskExecutorTest",
                "TestPipe",
                System.currentTimeMillis(),
                0,
                mock(EventSupplier.class),
                mock(PipeProcessor.class),
                mock(PipeEventCollector.class)));
  }

  @Test
  public void testProgressReportEventDoesNotWaitForInFlightTsFileParser() throws Exception {
    assertControlEventDoesNotWaitForInFlightTsFileParser(mock(ProgressReportEvent.class), false);
  }

  @Test
  public void testHeartbeatEventDoesNotWaitForInFlightTsFileParser() throws Exception {
    assertControlEventDoesNotWaitForInFlightTsFileParser(mock(PipeHeartbeatEvent.class), true);
  }

  private void assertControlEventDoesNotWaitForInFlightTsFileParser(
      final org.apache.iotdb.pipe.api.event.Event controlEvent, final boolean isHeartbeat)
      throws Exception {
    final EventSupplier eventSupplier = mock(EventSupplier.class);
    final PipeEventCollector pipeEventCollector = mock(PipeEventCollector.class);
    final PipeEventCollector firstParserCollector = mock(PipeEventCollector.class);
    final PipeEventCollector secondParserCollector = mock(PipeEventCollector.class);
    final PipeTsFileInsertionEvent firstTsFileEvent = mock(PipeTsFileInsertionEvent.class);
    final PipeTsFileInsertionEvent secondTsFileEvent = mock(PipeTsFileInsertionEvent.class);

    when(eventSupplier.supply())
        .thenReturn(firstTsFileEvent, controlEvent, secondTsFileEvent, null);
    when(pipeEventCollector.shouldParseTsFileEvent(any(PipeTsFileInsertionEvent.class)))
        .thenReturn(true);
    when(pipeEventCollector.forkForTsFileParser())
        .thenReturn(firstParserCollector, secondParserCollector);
    when(firstTsFileEvent.tryReserveTsFileParserMemory()).thenReturn(true);
    when(secondTsFileEvent.tryReserveTsFileParserMemory()).thenReturn(true);

    final CountDownLatch parsersStarted = new CountDownLatch(2);
    final CountDownLatch releaseParsers = new CountDownLatch(1);
    doAnswer(
            invocation -> {
              parsersStarted.countDown();
              releaseParsers.await(5, TimeUnit.SECONDS);
              return null;
            })
        .when(firstParserCollector)
        .collect(firstTsFileEvent);
    doAnswer(
            invocation -> {
              parsersStarted.countDown();
              releaseParsers.await(5, TimeUnit.SECONDS);
              return null;
            })
        .when(secondParserCollector)
        .collect(secondTsFileEvent);

    final TestablePipeProcessorSubtask pipeProcessorSubtask =
        new TestablePipeProcessorSubtask(
            isHeartbeat ? "heartbeat-barrier-test" : "progress-report-barrier-test",
            eventSupplier,
            pipeEventCollector);
    try {
      Assert.assertTrue(pipeProcessorSubtask.executeOnceForTest());
      Assert.assertTrue(pipeProcessorSubtask.executeOnceForTest());
      Mockito.verify(pipeEventCollector).collect(controlEvent);
      if (isHeartbeat) {
        Mockito.verify((PipeHeartbeatEvent) controlEvent).onProcessed();
      }
      Assert.assertTrue(pipeProcessorSubtask.executeOnceForTest());
      Assert.assertTrue(parsersStarted.await(5, TimeUnit.SECONDS));
    } finally {
      releaseParsers.countDown();
      pipeProcessorSubtask.close();
    }
  }

  private static class TestablePipeProcessorSubtask extends PipeProcessorSubtask {

    private TestablePipeProcessorSubtask(
        final String taskID,
        final EventSupplier inputEventSupplier,
        final PipeEventCollector outputEventCollector) {
      super(
          taskID,
          "pipe",
          System.currentTimeMillis(),
          0,
          inputEventSupplier,
          new DoNothingProcessor(),
          outputEventCollector,
          2);
      subtaskScheduler = new SingleStepPipeSubtaskScheduler();
      allowSubmittingSelf();
    }

    private boolean executeOnceForTest() throws Exception {
      return call();
    }
  }

  private static class SingleStepPipeSubtaskScheduler extends PipeSubtaskScheduler {

    private boolean hasScheduled;

    private SingleStepPipeSubtaskScheduler() {
      super(null);
    }

    @Override
    public boolean schedule() {
      if (hasScheduled) {
        return false;
      }
      hasScheduled = true;
      return true;
    }

    @Override
    public void reset() {
      hasScheduled = false;
    }
  }
}
