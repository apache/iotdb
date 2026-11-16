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

import org.apache.iotdb.commons.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.commons.concurrent.threadpool.WrappedThreadPoolExecutor;
import org.apache.iotdb.commons.conf.CommonConfig;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.pipe.event.EnrichedEvent;
import org.apache.iotdb.db.pipe.resource.PipeDataNodeResourceManager;
import org.apache.iotdb.db.pipe.resource.memory.PipeMemoryManager;
import org.apache.iotdb.db.pipe.resource.memory.PipeMemoryManager.TsFileParserMemoryReservation;

import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.InOrder;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
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
  public void testRequestsOnOneWorkerDoNotStrandOtherWorkersParserSlots() throws Exception {
    final CommonConfig config = CommonDescriptor.getInstance().getConfig();
    final PipeMemoryManager manager = PipeDataNodeResourceManager.memory();
    final long originalParserMemory = config.getPipeTsFileParserMemory();
    final int originalGlobalLimit = config.getPipeTsFileParserInFlightMaxNum();
    final int originalRegionLimit = config.getPipeTsFileParserInFlightMaxNumPerPipeRegion();
    final List<PipeProcessorSubtaskWorker> workers = new ArrayList<>();
    final List<TsFileParserMemoryReservation> requests = new ArrayList<>();
    final boolean[] reservedBlockers = new boolean[4];
    final CountDownLatch parsing = new CountDownLatch(4);
    final CountDownLatch finishParsing = new CountDownLatch(1);
    final ExecutorService executor = Executors.newFixedThreadPool(4);
    try {
      config.setPipeTsFileParserMemory(1);
      config.setPipeTsFileParserInFlightMaxNum(4);
      config.setPipeTsFileParserInFlightMaxNumPerPipeRegion(1);
      for (int i = 0; i < 4; i++) {
        reservedBlockers[i] =
            manager.tryReserveTsFileParserMemory(
                "blocker" + i, 0, "0", new TsFileParserMemoryReservation());
        Assert.assertTrue(reservedBlockers[i]);
        workers.add(newWorker(new LinkedHashSet<>()));
      }
      // Queue four pipes on the same serial worker before the other workers' requests. A fair
      // window based only on pipe order leaves all four free slots waiting for that one worker.
      for (int i = 0; i < 7; i++) {
        final String pipeName = "skewed" + i;
        final TsFileParserMemoryReservation request = new TsFileParserMemoryReservation();
        requests.add(request);
        final PipeProcessorSubtask subtask = createRunnableSubtask(pipeName);
        final AtomicBoolean processed = new AtomicBoolean();
        when(subtask.isWaitingForTsFileParserMemory())
            .thenAnswer(ignored -> request.isWaitingForSignal());
        doAnswer(
                invocation -> {
                  request.setWakeUpCallback(invocation.getArgument(0));
                  return null;
                })
            .when(subtask)
            .setWorkerWakeUpCallback(any());
        when(subtask.call())
            .thenAnswer(
                ignored -> {
                  if (processed.get()) {
                    return false;
                  }
                  if (!manager.tryReserveTsFileParserMemory(pipeName, 0, "0", request)) {
                    throw PipeProcessorSubtaskYieldException.parserNotAdmitted();
                  }
                  try {
                    parsing.countDown();
                    Assert.assertTrue(finishParsing.await(10, TimeUnit.SECONDS));
                    processed.set(true);
                    return true;
                  } finally {
                    manager.releaseTsFileParserMemory(pipeName, 0, "0");
                  }
                });
        workers.get(i < 4 ? 0 : i - 3).schedule(subtask);
        Assert.assertFalse(manager.tryReserveTsFileParserMemory(pipeName, 0, "0", request));
      }
      for (int i = 0; i < 4; i++) {
        manager.releaseTsFileParserMemory("blocker" + i, 0, "0");
        reservedBlockers[i] = false;
      }
      final List<Future<?>> rounds = new ArrayList<>();
      for (final PipeProcessorSubtaskWorker worker : workers) {
        rounds.add(executor.submit(worker::runSubtasks));
      }
      Assert.assertTrue(
          "Free parser slots must run on distinct available workers",
          parsing.await(5, TimeUnit.SECONDS));
      Assert.assertEquals(4, manager.getReservedTsFileParserCount());
      finishParsing.countDown();
      for (final Future<?> round : rounds) {
        round.get(5, TimeUnit.SECONDS);
      }
      Assert.assertEquals(0, manager.getReservedTsFileParserCount());
      Assert.assertEquals(0, manager.getWaitingTsFileParserCount());
    } finally {
      finishParsing.countDown();
      executor.shutdownNow();
      Assert.assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
      for (int i = 0; i < requests.size(); i++) {
        manager.cancelTsFileParserMemoryReservation("skewed" + i, 0, "0", requests.get(i));
      }
      for (int i = 0; i < reservedBlockers.length; i++) {
        if (reservedBlockers[i]) {
          manager.releaseTsFileParserMemory("blocker" + i, 0, "0");
        }
      }
      config.setPipeTsFileParserMemory(originalParserMemory);
      config.setPipeTsFileParserInFlightMaxNum(originalGlobalLimit);
      config.setPipeTsFileParserInFlightMaxNumPerPipeRegion(originalRegionLimit);
    }
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
        final PipeProcessorSubtaskWorker worker = newWorker(new LinkedHashSet<>());
        workers.add(worker);
        workerThreads.add(new AtomicReference<>());
      }
      for (int i = 0; i < 100; i++) {
        final String pipeName = "waitingPipe" + i;
        final TsFileParserMemoryReservation request = new TsFileParserMemoryReservation();
        requests.add(request);
        final PipeProcessorSubtask subtask = createRunnableSubtask(pipeName);
        when(subtask.isWaitingForTsFileParserMemory())
            .thenAnswer(ignored -> request.isWaitingForSignal());
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
  public void testWaitingParserIsSkippedUntilAdmissionNotification() throws Exception {
    final CommonConfig config = CommonDescriptor.getInstance().getConfig();
    final PipeMemoryManager memoryManager = PipeDataNodeResourceManager.memory();
    final long originalParserMemory = config.getPipeTsFileParserMemory();
    final int originalGlobalLimit = config.getPipeTsFileParserInFlightMaxNum();
    final int originalRegionLimit = config.getPipeTsFileParserInFlightMaxNumPerPipeRegion();
    final TsFileParserMemoryReservation blocker = new TsFileParserMemoryReservation();
    final TsFileParserMemoryReservation request = new TsFileParserMemoryReservation();
    boolean blockerReserved = false;
    try {
      config.setPipeTsFileParserMemory(1);
      config.setPipeTsFileParserInFlightMaxNum(1);
      config.setPipeTsFileParserInFlightMaxNumPerPipeRegion(1);
      blockerReserved = memoryManager.tryReserveTsFileParserMemory("blocker", 0, "0", blocker);
      Assert.assertTrue(blockerReserved);
      final PipeProcessorSubtaskWorker worker = newWorker(new LinkedHashSet<>());
      final PipeProcessorSubtask subtask = createRunnableSubtask("waitingPipe");
      when(subtask.isWaitingForTsFileParserMemory())
          .thenAnswer(ignored -> request.isWaitingForSignal());
      when(subtask.call())
          .thenAnswer(
              ignored -> {
                if (!memoryManager.tryReserveTsFileParserMemory("waitingPipe", 0, "0", request)) {
                  throw PipeProcessorSubtaskYieldException.parserNotAdmitted();
                }
                memoryManager.releaseTsFileParserMemory("waitingPipe", 0, "0");
                return true;
              });
      worker.schedule(subtask);
      Assert.assertTrue(worker.runSubtasks());
      for (int i = 0; i < 100; i++) {
        Assert.assertTrue(worker.runSubtasks());
      }
      verify(subtask, times(1)).call();
      memoryManager.releaseTsFileParserMemory("blocker", 0, "0");
      blockerReserved = false;
      Assert.assertFalse(worker.runSubtasks());
      verify(subtask, times(2)).call();
      Assert.assertEquals(0, memoryManager.getWaitingTsFileParserCount());
      Assert.assertEquals(0, memoryManager.getReservedTsFileParserCount());
    } finally {
      memoryManager.cancelTsFileParserMemoryReservation("waitingPipe", 0, "0", request);
      if (blockerReserved) {
        memoryManager.releaseTsFileParserMemory("blocker", 0, "0");
      }
      config.setPipeTsFileParserMemory(originalParserMemory);
      config.setPipeTsFileParserInFlightMaxNum(originalGlobalLimit);
      config.setPipeTsFileParserInFlightMaxNumPerPipeRegion(originalRegionLimit);
    }
  }

  @Test(timeout = 10000)
  public void testNotificationBeforeIdleWaitIsNotLost() throws Exception {
    final PipeProcessorSubtaskWorker worker = newWorker(new LinkedHashSet<>());
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

  @Test(timeout = 15000)
  public void testBusyFirstWorkerDoesNotSerializeOtherEligibleWorker() throws Exception {
    final CommonConfig config = CommonDescriptor.getInstance().getConfig();
    final PipeMemoryManager manager = PipeDataNodeResourceManager.memory();
    final long originalParserMemory = config.getPipeTsFileParserMemory();
    final int originalGlobalLimit = config.getPipeTsFileParserInFlightMaxNum();
    final int originalRegionLimit = config.getPipeTsFileParserInFlightMaxNumPerPipeRegion();
    final boolean[] reservedBlockers = new boolean[2];
    final TsFileParserMemoryReservation first = new TsFileParserMemoryReservation();
    final TsFileParserMemoryReservation second = new TsFileParserMemoryReservation();
    final PipeProcessorSubtaskWorker firstWorker = newWorker(new LinkedHashSet<>());
    final PipeProcessorSubtaskWorker secondWorker = newWorker(new LinkedHashSet<>());
    final CountDownLatch firstWorkerBusy = new CountDownLatch(1);
    final CountDownLatch unblockFirstWorker = new CountDownLatch(1);
    final CountDownLatch secondAdmitted = new CountDownLatch(1);
    final AtomicBoolean blockFirstWorker = new AtomicBoolean();
    final AtomicBoolean secondProcessed = new AtomicBoolean();
    final ExecutorService executor = Executors.newFixedThreadPool(2);
    try {
      config.setPipeTsFileParserMemory(1);
      config.setPipeTsFileParserInFlightMaxNum(2);
      config.setPipeTsFileParserInFlightMaxNumPerPipeRegion(1);
      for (int i = 0; i < reservedBlockers.length; i++) {
        reservedBlockers[i] =
            manager.tryReserveTsFileParserMemory(
                "blocker" + i, 0, "0", new TsFileParserMemoryReservation());
        Assert.assertTrue(reservedBlockers[i]);
      }
      final PipeProcessorSubtask busySubtask = createRunnableSubtask("busy");
      when(busySubtask.call())
          .thenAnswer(
              ignored -> {
                if (blockFirstWorker.get()) {
                  firstWorkerBusy.countDown();
                  Assert.assertTrue(unblockFirstWorker.await(5, TimeUnit.SECONDS));
                }
                return false;
              });
      firstWorker.schedule(busySubtask);
      final PipeProcessorSubtask firstSubtask = createRunnableSubtask("first");
      final PipeProcessorSubtask secondSubtask = createRunnableSubtask("second");
      when(firstSubtask.isWaitingForTsFileParserMemory())
          .thenAnswer(ignored -> first.isWaitingForSignal());
      when(secondSubtask.isWaitingForTsFileParserMemory())
          .thenAnswer(ignored -> second.isWaitingForSignal());
      doAnswer(
              invocation -> {
                first.setWakeUpCallback(invocation.getArgument(0));
                return null;
              })
          .when(firstSubtask)
          .setWorkerWakeUpCallback(any());
      doAnswer(
              invocation -> {
                second.setWakeUpCallback(invocation.getArgument(0));
                return null;
              })
          .when(secondSubtask)
          .setWorkerWakeUpCallback(any());
      when(firstSubtask.call())
          .thenAnswer(
              ignored -> {
                if (!manager.tryReserveTsFileParserMemory("first", 0, "0", first)) {
                  throw PipeProcessorSubtaskYieldException.parserNotAdmitted();
                }
                manager.releaseTsFileParserMemory("first", 0, "0");
                return true;
              });
      when(secondSubtask.call())
          .thenAnswer(
              ignored -> {
                if (secondProcessed.get()) {
                  return false;
                }
                if (!manager.tryReserveTsFileParserMemory("second", 0, "0", second)) {
                  throw PipeProcessorSubtaskYieldException.parserNotAdmitted();
                }
                manager.releaseTsFileParserMemory("second", 0, "0");
                secondProcessed.set(true);
                secondAdmitted.countDown();
                return true;
              });
      firstWorker.schedule(firstSubtask);
      secondWorker.schedule(secondSubtask);
      Assert.assertTrue(firstWorker.runSubtasks());
      Assert.assertTrue(secondWorker.runSubtasks());
      blockFirstWorker.set(true);
      final Future<?> busyRound = executor.submit(firstWorker::runSubtasks);
      Assert.assertTrue(firstWorkerBusy.await(3, TimeUnit.SECONDS));
      for (int i = 0; i < reservedBlockers.length; i++) {
        manager.releaseTsFileParserMemory("blocker" + i, 0, "0");
        reservedBlockers[i] = false;
      }
      executor.submit(
          () -> {
            while (!Thread.currentThread().isInterrupted() && !secondProcessed.get()) {
              secondWorker.sleepIfNecessary(secondWorker.runSubtasks());
            }
          });
      Assert.assertTrue(secondAdmitted.await(3, TimeUnit.SECONDS));
      verify(firstSubtask, times(1)).call();
      Assert.assertFalse(busyRound.isDone());
      unblockFirstWorker.countDown();
      busyRound.get(3, TimeUnit.SECONDS);
      Assert.assertEquals(0, manager.getWaitingTsFileParserCount());
      Assert.assertEquals(0, manager.getReservedTsFileParserCount());
    } finally {
      unblockFirstWorker.countDown();
      executor.shutdownNow();
      Assert.assertTrue(executor.awaitTermination(3, TimeUnit.SECONDS));
      manager.cancelTsFileParserMemoryReservation("first", 0, "0", first);
      manager.cancelTsFileParserMemoryReservation("second", 0, "0", second);
      for (int i = 0; i < reservedBlockers.length; i++) {
        if (reservedBlockers[i]) {
          manager.releaseTsFileParserMemory("blocker" + i, 0, "0");
        }
      }
      config.setPipeTsFileParserMemory(originalParserMemory);
      config.setPipeTsFileParserInFlightMaxNum(originalGlobalLimit);
      config.setPipeTsFileParserInFlightMaxNumPerPipeRegion(originalRegionLimit);
    }
  }

  @Test
  public void testYieldingPipesDoNotBlockAnotherPipeOnSameWorker() throws Exception {
    final PipeProcessorSubtaskWorker worker = newWorker(new LinkedHashSet<>());
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
    final PipeProcessorSubtaskWorker worker = newWorker(new LinkedHashSet<>());
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

    final PipeProcessorSubtaskWorkerManager manager =
        new PipeProcessorSubtaskWorkerManager(workerThreadPoolExecutor, watcherScheduledExecutor);

    // Workers are started on demand, so no worker thread is submitted until a subtask is
    // scheduled to the manager.
    for (int i = 0; i < manager.getWorkerNumber(); ++i) {
      Assert.assertFalse(manager.getWorker(i).isRunning());
    }
    verify(workerThreadPoolExecutor, never()).submit(any(Runnable.class));
    verify(watcherScheduledExecutor)
        .scheduleWithFixedDelay(any(Runnable.class), eq(1L), eq(1L), eq(TimeUnit.MINUTES));

    final PipeProcessorSubtask subtask = createRunnableSubtask("scheduledSubtask");
    manager.schedule(subtask);
    verify(subtask).setWorkerWakeUpCallback(any(Runnable.class));
    verify(workerThreadPoolExecutor, times(1)).submit(any(Runnable.class));
    Assert.assertTrue(manager.getWorker(0).isRunning());
  }

  @Test(timeout = 60000)
  @SuppressWarnings("unsafeThreadSchedule")
  public void testWorkerThreadIsReleasedWhenIdleAndStartedAgainOnDemand() throws Exception {
    final WrappedThreadPoolExecutor executor =
        (WrappedThreadPoolExecutor)
            IoTDBThreadPoolFactory.newFixedThreadPoolWithIdleThreadTimeout(
                2, 100, TimeUnit.MILLISECONDS, "Pipe-Processor-Worker-Test-" + System.nanoTime());
    final ListeningExecutorService listeningExecutor = MoreExecutors.listeningDecorator(executor);
    try {
      final PipeProcessorSubtaskWorker worker = new PipeProcessorSubtaskWorker(listeningExecutor);
      worker.setSleepingTimeInMilliSecond(10);
      Assert.assertFalse(worker.isRunning());
      Assert.assertEquals(0, executor.getPoolSize());

      final AtomicInteger calls = new AtomicInteger();
      final AtomicBoolean subtaskClosed = new AtomicBoolean();
      final PipeProcessorSubtask subtask = createRunnableSubtask("subtask");
      // Configure the mock before scheduling; restubbing while the worker calls it can race with
      // Mockito's invocation tracking. Change only the atomic state once the worker is running.
      when(subtask.isClosed()).thenAnswer(ignored -> subtaskClosed.get());
      doAnswer(
              invocation -> {
                calls.incrementAndGet();
                return false;
              })
          .when(subtask)
          .call();
      worker.schedule(subtask);

      await().atMost(10, TimeUnit.SECONDS).until(() -> worker.isRunning());
      await().atMost(10, TimeUnit.SECONDS).until(() -> calls.get() > 0);
      Assert.assertEquals(1, executor.getPoolSize());

      // Close the only subtask of this worker. Both the worker and its thread should be released
      // instead of being kept alive until the whole subtask executor is shut down.
      subtaskClosed.set(true);
      await().atMost(10, TimeUnit.SECONDS).until(() -> !worker.isRunning());
      await().atMost(10, TimeUnit.SECONDS).until(() -> executor.getPoolSize() == 0);

      // A newly scheduled subtask must start a new worker thread.
      final AtomicInteger restartedCalls = new AtomicInteger();
      final AtomicBoolean restartedSubtaskClosed = new AtomicBoolean();
      final PipeProcessorSubtask restartedSubtask = createRunnableSubtask("restartedSubtask");
      when(restartedSubtask.isClosed()).thenAnswer(ignored -> restartedSubtaskClosed.get());
      doAnswer(
              invocation -> {
                restartedCalls.incrementAndGet();
                return false;
              })
          .when(restartedSubtask)
          .call();
      worker.schedule(restartedSubtask);

      await().atMost(10, TimeUnit.SECONDS).until(() -> worker.isRunning());
      await().atMost(10, TimeUnit.SECONDS).until(() -> restartedCalls.get() > 0);
      Assert.assertEquals(1, executor.getPoolSize());

      restartedSubtaskClosed.set(true);
      await().atMost(10, TimeUnit.SECONDS).until(() -> !worker.isRunning());
      await().atMost(10, TimeUnit.SECONDS).until(() -> executor.getPoolSize() == 0);
    } finally {
      executor.shutdownNow();
      Assert.assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));
    }
  }

  @Test(timeout = 30000)
  public void testConcurrentSchedulesSubmitAtMostOneWorker() throws Exception {
    final ListeningExecutorService executor = mock(ListeningExecutorService.class);
    final AtomicInteger submissions = new AtomicInteger();
    doAnswer(
            invocation -> {
              submissions.incrementAndGet();
              return null;
            })
        .when(executor)
        .submit(any(Runnable.class));

    final PipeProcessorSubtaskWorker worker = new PipeProcessorSubtaskWorker(executor);
    final int threadNum = 16;
    final ExecutorService scheduleExecutor = Executors.newFixedThreadPool(threadNum);
    try {
      // Concurrent schedules must result in exactly one worker submission.
      final List<PipeProcessorSubtask> subtasks = new ArrayList<>();
      final CountDownLatch start = new CountDownLatch(1);
      final List<Future<?>> futures = new ArrayList<>();
      for (int i = 0; i < threadNum; ++i) {
        final PipeProcessorSubtask subtask = createRunnableSubtask("concurrentSubtask" + i);
        subtasks.add(subtask);
        futures.add(
            scheduleExecutor.submit(
                () -> {
                  start.await();
                  worker.schedule(subtask);
                  return null;
                }));
      }
      start.countDown();
      for (final Future<?> future : futures) {
        future.get(10, TimeUnit.SECONDS);
      }
      Assert.assertEquals(1, submissions.get());

      // Let the simulated worker exit because all its subtasks are closed.
      for (final PipeProcessorSubtask subtask : subtasks) {
        when(subtask.isClosed()).thenReturn(true);
      }
      worker.runMayThrow();
      Assert.assertFalse(worker.isRunning());

      // Concurrent schedules after the worker has exited must start exactly one new worker.
      final CountDownLatch restart = new CountDownLatch(1);
      futures.clear();
      for (int i = 0; i < threadNum; ++i) {
        final PipeProcessorSubtask subtask = createRunnableSubtask("restartSubtask" + i);
        futures.add(
            scheduleExecutor.submit(
                () -> {
                  restart.await();
                  worker.schedule(subtask);
                  return null;
                }));
      }
      restart.countDown();
      for (final Future<?> future : futures) {
        future.get(10, TimeUnit.SECONDS);
      }
      Assert.assertEquals(2, submissions.get());
      Assert.assertTrue(worker.isRunning());
    } finally {
      scheduleExecutor.shutdownNow();
      Assert.assertTrue(scheduleExecutor.awaitTermination(10, TimeUnit.SECONDS));
    }
  }

  private static PipeProcessorSubtaskWorker newWorker(final Set<PipeProcessorSubtask> subtasks) {
    return new PipeProcessorSubtaskWorker(subtasks, mock(ListeningExecutorService.class));
  }

  private PipeProcessorSubtask createRunnableSubtask(final String mockName) {
    final PipeProcessorSubtask subtask = mock(PipeProcessorSubtask.class, mockName);
    when(subtask.isClosed()).thenReturn(false);
    when(subtask.isSubmittingSelf()).thenReturn(true);
    when(subtask.isStoppedByException()).thenReturn(false);
    return subtask;
  }
}
