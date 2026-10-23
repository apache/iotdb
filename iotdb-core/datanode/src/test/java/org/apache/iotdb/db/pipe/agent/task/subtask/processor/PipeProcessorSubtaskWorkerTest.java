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

import org.apache.iotdb.commons.conf.CommonConfig;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.pipe.event.EnrichedEvent;
import org.apache.iotdb.db.pipe.resource.PipeDataNodeResourceManager;
import org.apache.iotdb.db.pipe.resource.memory.PipeMemoryManager;
import org.apache.iotdb.db.pipe.resource.memory.PipeMemoryManager.TsFileParserMemoryReservation;

import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.InOrder;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class PipeProcessorSubtaskWorkerTest {

  @BeforeClass
  public static void initializeSubtaskMock() {
    // Mockito/Byte Buddy initialization can be slow in a fresh CI fork. Keep that class loading
    // outside the test timeouts so they measure worker notifications instead of mock startup.
    mock(PipeProcessorSubtask.class);
  }

  @Test(timeout = 30000)
  public void testOneHundredWaitingPipesDoNotWaitForWorkerPolling() throws Exception {
    final CommonConfig config = CommonDescriptor.getInstance().getConfig();
    final PipeMemoryManager memoryManager = PipeDataNodeResourceManager.memory();
    final long originalParserMemory = config.getPipeTsFileParserMemory();
    final int originalGlobalLimit = config.getPipeTsFileParserInFlightMaxNum();
    final int originalRegionLimit = config.getPipeTsFileParserInFlightMaxNumPerPipeRegion();
    final List<PipeProcessorSubtaskWorker> workers = new ArrayList<>();
    final List<AtomicReference<Thread>> workerThreads = new ArrayList<>();
    final List<TsFileParserMemoryReservation> requests = new ArrayList<>();
    final List<Future<?>> futures = new ArrayList<>();
    final CountDownLatch processed = new CountDownLatch(100);
    final ExecutorService executor = Executors.newFixedThreadPool(5);
    final boolean[] reservedBlockers = new boolean[4];
    try {
      config.setPipeTsFileParserMemory(1);
      config.setPipeTsFileParserInFlightMaxNum(4);
      config.setPipeTsFileParserInFlightMaxNumPerPipeRegion(1);
      for (int i = 0; i < 4; i++) {
        Assert.assertTrue(
            memoryManager.tryReserveTsFileParserMemory(
                "blocker" + i, 0, "0", new TsFileParserMemoryReservation()));
        reservedBlockers[i] = true;
      }
      for (int i = 0; i < 5; i++) {
        final PipeProcessorSubtaskWorker worker =
            new PipeProcessorSubtaskWorker(new LinkedHashSet<>());
        workers.add(worker);
        workerThreads.add(new AtomicReference<>());
      }
      for (int i = 0; i < 100; i++) {
        final String pipeName = "waitingPipe" + i;
        final TsFileParserMemoryReservation request = new TsFileParserMemoryReservation();
        requests.add(request);
        final PipeProcessorSubtask subtask = createRunnableSubtask(pipeName);
        doAnswer(
                invocation -> {
                  request.setWakeUpCallback(invocation.getArgument(0));
                  return null;
                })
            .when(subtask)
            .setWorkerWakeUpCallback(any());
        final AtomicBoolean hasProcessed = new AtomicBoolean();
        when(subtask.call())
            .thenAnswer(
                invocation -> {
                  if (hasProcessed.get()) {
                    return false;
                  }
                  if (!memoryManager.tryReserveTsFileParserMemory(pipeName, 0, "0", request)) {
                    throw PipeProcessorSubtaskYieldException.parserNotAdmitted();
                  }
                  hasProcessed.set(true);
                  memoryManager.releaseTsFileParserMemory(pipeName, 0, "0");
                  processed.countDown();
                  return true;
                });
        workers.get(i % workers.size()).schedule(subtask);
        Assert.assertFalse(memoryManager.tryReserveTsFileParserMemory(pipeName, 0, "0", request));
      }
      Assert.assertEquals(100, memoryManager.getWaitingTsFileParserCount());
      for (int i = 0; i < workers.size(); i++) {
        final PipeProcessorSubtaskWorker worker = workers.get(i);
        final AtomicReference<Thread> workerThread = workerThreads.get(i);
        // Consume the scheduling notification before all workers become idle on admission.
        worker.sleepIfNecessary(true);
        // Make a missed wake-up observable without relying on a tight performance assertion.
        worker.setSleepingTimeInMilliSecond(60000L);
        futures.add(
            executor.submit(
                () -> {
                  workerThread.set(Thread.currentThread());
                  while (!Thread.currentThread().isInterrupted()) {
                    worker.sleepIfNecessary(worker.runSubtasks());
                  }
                }));
      }
      await()
          .atMost(5, TimeUnit.SECONDS)
          .until(
              () ->
                  workerThreads.stream()
                      .allMatch(
                          thread ->
                              thread.get() != null
                                  && thread.get().getState() == Thread.State.TIMED_WAITING));
      for (int i = 0; i < 4; i++) {
        memoryManager.releaseTsFileParserMemory("blocker" + i, 0, "0");
        reservedBlockers[i] = false;
      }
      Assert.assertTrue(
          "Admission notifications must wake every queue head",
          processed.await(5, TimeUnit.SECONDS));
      Assert.assertEquals(0, memoryManager.getWaitingTsFileParserCount());
      Assert.assertEquals(0, memoryManager.getReservedTsFileParserCount());
    } finally {
      futures.forEach(future -> future.cancel(true));
      executor.shutdownNow();
      Assert.assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
      for (int i = 0; i < requests.size(); i++) {
        memoryManager.cancelTsFileParserMemoryReservation(
            "waitingPipe" + i, 0, "0", requests.get(i));
      }
      for (int i = 0; i < reservedBlockers.length; i++) {
        if (reservedBlockers[i]) {
          memoryManager.releaseTsFileParserMemory("blocker" + i, 0, "0");
        }
      }
      config.setPipeTsFileParserMemory(originalParserMemory);
      config.setPipeTsFileParserInFlightMaxNum(originalGlobalLimit);
      config.setPipeTsFileParserInFlightMaxNumPerPipeRegion(originalRegionLimit);
    }
  }

  @Test(timeout = 10000)
  public void testNotificationBeforeIdleWaitIsNotLost() throws Exception {
    final PipeProcessorSubtaskWorker worker = new PipeProcessorSubtaskWorker(new LinkedHashSet<>());
    worker.setSleepingTimeInMilliSecond(60000L);
    final PipeProcessorSubtask subtask = createRunnableSubtask("waitingPipe");
    final AtomicReference<Runnable> wakeUpCallback = new AtomicReference<>();
    doAnswer(
            invocation -> {
              wakeUpCallback.set(invocation.getArgument(0));
              return null;
            })
        .when(subtask)
        .setWorkerWakeUpCallback(any());
    worker.schedule(subtask);
    worker.sleepIfNecessary(true);
    final ExecutorService executor = Executors.newSingleThreadExecutor();
    try {
      wakeUpCallback.get().run();
      executor.submit(() -> worker.sleepIfNecessary(true)).get(3, TimeUnit.SECONDS);
    } finally {
      executor.shutdownNow();
      Assert.assertTrue(executor.awaitTermination(3, TimeUnit.SECONDS));
    }
  }

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
