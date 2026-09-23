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

import org.apache.iotdb.commons.pipe.event.EnrichedEvent;

import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.InOrder;

import java.util.LinkedHashSet;
import java.util.concurrent.TimeUnit;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
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

  @Test
  public void testLongRunningEventReportIsRateLimited() {
    final PipeProcessorSubtaskWorker worker = new PipeProcessorSubtaskWorker(new LinkedHashSet<>());
    final long startTimeInNanos = 100;
    final PipeProcessorSubtask.EventProcessingContext context =
        new PipeProcessorSubtask.EventProcessingContext(
            mock(EnrichedEvent.class), startTimeInNanos);
    final long initialReportDelayInNanos = TimeUnit.MINUTES.toNanos(10);
    final long reportIntervalInNanos = TimeUnit.MINUTES.toNanos(30);

    Assert.assertFalse(
        worker.isLongRunningEventReportDue(
            context, startTimeInNanos + initialReportDelayInNanos - 1));
    Assert.assertTrue(
        worker.isLongRunningEventReportDue(context, startTimeInNanos + initialReportDelayInNanos));

    final long firstReportTimeInNanos = startTimeInNanos + initialReportDelayInNanos;
    worker.markLongRunningEventReported(context, firstReportTimeInNanos);
    Assert.assertFalse(
        worker.isLongRunningEventReportDue(
            context, firstReportTimeInNanos + reportIntervalInNanos - 1));
    Assert.assertTrue(
        worker.isLongRunningEventReportDue(
            context, firstReportTimeInNanos + reportIntervalInNanos));

    final long nextEventStartTimeInNanos = firstReportTimeInNanos + 1;
    final PipeProcessorSubtask.EventProcessingContext nextContext =
        new PipeProcessorSubtask.EventProcessingContext(
            mock(EnrichedEvent.class), nextEventStartTimeInNanos);
    Assert.assertFalse(
        worker.isLongRunningEventReportDue(
            nextContext, nextEventStartTimeInNanos + initialReportDelayInNanos - 1));
    Assert.assertTrue(
        worker.isLongRunningEventReportDue(
            nextContext, nextEventStartTimeInNanos + initialReportDelayInNanos));
  }

  @Test
  public void testLongRunningEventLogPayloadIsBounded() {
    final EnrichedEvent event = mock(EnrichedEvent.class);
    when(event.coreReportMessage()).thenReturn("x".repeat(2048) + "\nmore");

    final String eventReport = PipeProcessorSubtaskWorker.getEventReport(event);
    Assert.assertEquals(1027, eventReport.length());
    Assert.assertFalse(eventReport.contains("\n"));
    Assert.assertTrue(eventReport.endsWith("..."));

    final StackTraceElement[] stackTrace = new StackTraceElement[100];
    for (int i = 0; i < stackTrace.length; ++i) {
      stackTrace[i] = new StackTraceElement("Class", "method" + i, "File.java", i);
    }
    final String formattedStackTrace = PipeProcessorSubtaskWorker.formatStackTrace(stackTrace);
    Assert.assertTrue(formattedStackTrace.contains("method63"));
    Assert.assertFalse(formattedStackTrace.contains("method64"));
    Assert.assertTrue(formattedStackTrace.contains("... (36)"));
  }

  @Test
  @SuppressWarnings("unsafeThreadSchedule")
  public void testWorkerManagerSchedulesWatcher() {
    final ListeningExecutorService workerThreadPoolExecutor = mock(ListeningExecutorService.class);
    final ListeningScheduledExecutorService watcherScheduledExecutor =
        mock(ListeningScheduledExecutorService.class);

    new PipeProcessorSubtaskWorkerManager(workerThreadPoolExecutor, watcherScheduledExecutor);

    verify(workerThreadPoolExecutor, atLeastOnce()).submit(any(Runnable.class));
    verify(watcherScheduledExecutor)
        .scheduleWithFixedDelay(any(Runnable.class), eq(1L), eq(1L), eq(TimeUnit.MINUTES));
  }

  private PipeProcessorSubtask createRunnableSubtask(final String mockName) {
    final PipeProcessorSubtask subtask = mock(PipeProcessorSubtask.class, mockName);
    when(subtask.isClosed()).thenReturn(false);
    when(subtask.isSubmittingSelf()).thenReturn(true);
    when(subtask.isStoppedByException()).thenReturn(false);
    return subtask;
  }
}
