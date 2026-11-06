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
import org.apache.iotdb.commons.pipe.agent.plugin.builtin.processor.donothing.DoNothingProcessor;
import org.apache.iotdb.commons.pipe.agent.task.connection.EventSupplier;
import org.apache.iotdb.commons.pipe.agent.task.connection.UnboundedBlockingPendingQueue;
import org.apache.iotdb.commons.pipe.agent.task.execution.PipeSubtaskExecutor;
import org.apache.iotdb.commons.pipe.agent.task.execution.PipeSubtaskScheduler;
import org.apache.iotdb.commons.pipe.datastructure.pattern.PrefixTreePattern;
import org.apache.iotdb.commons.utils.FileUtils;
import org.apache.iotdb.db.pipe.agent.task.connection.PipeEventCollector;
import org.apache.iotdb.db.pipe.event.common.tablet.PipeRawTabletInsertionEvent;
import org.apache.iotdb.db.pipe.event.common.tsfile.PipeTsFileInsertionEvent;
import org.apache.iotdb.db.pipe.metric.source.PipeDataRegionEventCounter;
import org.apache.iotdb.db.pipe.resource.PipeDataNodeResourceManager;
import org.apache.iotdb.db.pipe.resource.memory.PipeMemoryManager;
import org.apache.iotdb.db.pipe.resource.memory.PipeMemoryManager.TsFileParserMemoryReservation;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResourceStatus;
import org.apache.iotdb.pipe.api.collector.EventCollector;
import org.apache.iotdb.pipe.api.event.Event;
import org.apache.iotdb.pipe.api.event.dml.insertion.TsFileInsertionEvent;

import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.utils.TsFileGeneratorUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.awaitility.Awaitility.await;
import static org.mockito.Mockito.mock;

public class PipeProcessorSubtaskExecutionGuardTest {

  @Test
  public void testStopAndImmediateRestartInvalidateCurrentInvocation() {
    final PipeProcessorSubtaskExecutionGuard executionGuard =
        new PipeProcessorSubtaskExecutionGuard();

    executionGuard.start();
    executionGuard.enter();
    executionGuard.check();

    executionGuard.stop();
    executionGuard.start();
    Assert.assertThrows(PipeProcessorSubtaskYieldException.class, executionGuard::check);

    executionGuard.exit();
    executionGuard.enter();
    executionGuard.check();
    executionGuard.exit();
  }

  @Test(timeout = 60000)
  public void testParserAdmissionYieldsWithoutBlockingAndResumes() throws Exception {
    final CommonConfig commonConfig = CommonDescriptor.getInstance().getConfig();
    final PipeMemoryManager memoryManager = PipeDataNodeResourceManager.memory();
    final long originalParserMemoryInBytes = commonConfig.getPipeTsFileParserMemory();
    final int originalGlobalLimit = commonConfig.getPipeTsFileParserInFlightMaxNum();
    final int originalPerPipeRegionLimit =
        commonConfig.getPipeTsFileParserInFlightMaxNumPerPipeRegion();
    final TsFileParserMemoryReservation blockerReservation = new TsFileParserMemoryReservation();
    final TsFileParserMemoryReservation competitorReservation = new TsFileParserMemoryReservation();

    final File tempDir = Files.createTempDirectory("pipeProcessorAdmissionYield").toFile();
    PipeTsFileInsertionEvent event = null;
    boolean isBlockerReserved = false;
    boolean isCompetitorReserved = false;
    try {
      commonConfig.setPipeTsFileParserMemory(1);
      commonConfig.setPipeTsFileParserInFlightMaxNum(1);
      commonConfig.setPipeTsFileParserInFlightMaxNumPerPipeRegion(1);
      isBlockerReserved =
          memoryManager.tryReserveTsFileParserMemory("blocker", 0, "0", blockerReservation);
      Assert.assertTrue(isBlockerReserved);

      event = createEvent(tempDir, "admission.tsfile", "admissionPipe");
      final PipeTsFileInsertionEvent eventToConsume = event;
      final PipeProcessorSubtaskExecutionGuard executionGuard =
          new PipeProcessorSubtaskExecutionGuard();
      final AtomicInteger wakeUpCount = new AtomicInteger();
      executionGuard.setWorkerWakeUpCallback(wakeUpCount::incrementAndGet);
      executionGuard.start();
      wakeUpCount.set(0);
      executionGuard.enter();

      final long startTimeInNanos = System.nanoTime();
      final PipeProcessorSubtaskYieldException admissionYield =
          Assert.assertThrows(
              PipeProcessorSubtaskYieldException.class,
              () ->
                  eventToConsume.consumeTabletInsertionEventsWithRetry(
                      parsedEvent -> parsedEvent.clearReferenceCount(getClass().getName()),
                      "test",
                      executionGuard));
      Assert.assertEquals(
          PipeProcessorSubtaskYieldException.Reason.PARSER_NOT_ADMITTED,
          admissionYield.getReason());
      Assert.assertTrue(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTimeInNanos) < 1000);
      Assert.assertTrue(event.isWaitingForTsFileParserMemory());
      executionGuard.exit();

      // A yielded invocation must keep its fair queue position and receive the release signal.
      memoryManager.releaseTsFileParserMemory("blocker", 0, "0");
      isBlockerReserved = false;
      Assert.assertEquals(1, wakeUpCount.get());
      Assert.assertFalse(event.isWaitingForTsFileParserMemory());
      executionGuard.stop();
      event.cancelTsFileParserMemoryReservationIfPending();
      isCompetitorReserved =
          memoryManager.tryReserveTsFileParserMemory("competitor", 0, "0", competitorReservation);
      Assert.assertTrue(isCompetitorReserved);
      memoryManager.releaseTsFileParserMemory("competitor", 0, "0");
      isCompetitorReserved = false;

      final AtomicInteger consumedTabletCount = new AtomicInteger(0);
      executionGuard.start();
      Assert.assertEquals(2, wakeUpCount.get());
      executionGuard.enter();
      event.consumeTabletInsertionEventsWithRetry(
          parsedEvent -> {
            consumedTabletCount.incrementAndGet();
            parsedEvent.clearReferenceCount(getClass().getName());
          },
          "test",
          executionGuard);
      executionGuard.exit();
      Assert.assertTrue(consumedTabletCount.get() > 0);
    } finally {
      if (event != null) {
        event.close();
      }
      memoryManager.cancelTsFileParserMemoryReservation("blocker", 0, "0", blockerReservation);
      memoryManager.cancelTsFileParserMemoryReservation(
          "competitor", 0, "0", competitorReservation);
      if (isBlockerReserved) {
        memoryManager.releaseTsFileParserMemory("blocker", 0, "0");
      }
      if (isCompetitorReserved) {
        memoryManager.releaseTsFileParserMemory("competitor", 0, "0");
      }
      commonConfig.setPipeTsFileParserMemory(originalParserMemoryInBytes);
      commonConfig.setPipeTsFileParserInFlightMaxNum(originalGlobalLimit);
      commonConfig.setPipeTsFileParserInFlightMaxNumPerPipeRegion(originalPerPipeRegionLimit);
      FileUtils.deleteFileOrDirectory(tempDir);
    }
  }

  @Test(timeout = 60000)
  public void testCloseCancelsReservationEnqueuedByAWorkerInvocation() throws Exception {
    final CommonConfig config = CommonDescriptor.getInstance().getConfig();
    final PipeMemoryManager manager = PipeDataNodeResourceManager.memory();
    final long originalParserMemory = config.getPipeTsFileParserMemory();
    final int originalGlobalLimit = config.getPipeTsFileParserInFlightMaxNum();
    final int originalRegionLimit = config.getPipeTsFileParserInFlightMaxNumPerPipeRegion();
    final File tempDir = Files.createTempDirectory("pipeProcessorCloseAdmission").toFile();
    final ExecutorService executor = Executors.newSingleThreadExecutor();
    final AtomicReference<Thread> closingThread = new AtomicReference<>();
    PipeTsFileInsertionEvent event = null;
    PipeProcessorSubtask subtask = null;
    boolean blockerReserved = false;
    try {
      config.setPipeTsFileParserMemory(1);
      config.setPipeTsFileParserInFlightMaxNum(1);
      config.setPipeTsFileParserInFlightMaxNumPerPipeRegion(1);
      blockerReserved =
          manager.tryReserveTsFileParserMemory(
              "blocker", 0, "0", new TsFileParserMemoryReservation());
      Assert.assertTrue(blockerReserved);
      event = createEvent(tempDir, "close.tsfile", "closingPipe");
      Assert.assertTrue(event.increaseReferenceCount(getClass().getName()));
      final PipeTsFileInsertionEvent eventToClose = event;
      final AtomicReference<Event> suppliedEvent = new AtomicReference<>(event);
      final UnboundedBlockingPendingQueue<Event> outputQueue =
          new UnboundedBlockingPendingQueue<>(new PipeDataRegionEventCounter());
      subtask =
          new ParserSubtask(
              "closingPipe",
              () -> suppliedEvent.getAndSet(null),
              new DoNothingProcessor(),
              outputQueue);
      final PipeProcessorSubtaskWorker worker = new PipeProcessorSubtaskWorker();
      worker.schedule(subtask);
      subtask.allowSubmittingSelf();
      final Field parserField = PipeTsFileInsertionEvent.class.getDeclaredField("eventParser");
      parserField.setAccessible(true);
      final Object parserMonitor = parserField.get(event);
      final Future<?> closing;
      synchronized (parserMonitor) {
        closing =
            executor.submit(
                () -> {
                  closingThread.set(Thread.currentThread());
                  eventToClose.close();
                });
        await()
            .atMost(5, TimeUnit.SECONDS)
            .until(
                () ->
                    closingThread.get() != null
                        && closingThread.get().getState() == Thread.State.BLOCKED);
        // Delay only the closer. This thread re-enters the monitor through the real worker,
        // subtask and collector, including both generation checks around admission. This models
        // close being descheduled just before acquiring eventParser, without bypassing parsing.
        Assert.assertTrue(worker.runSubtasks());
        Assert.assertNull(suppliedEvent.get());
        Assert.assertTrue(event.isWaitingForTsFileParserMemory());
        Assert.assertTrue(subtask.isWaitingForTsFileParserMemory());
        Assert.assertEquals(1, manager.getWaitingTsFileParserCount());
        Assert.assertTrue(outputQueue.isEmpty());
      }
      closing.get(5, TimeUnit.SECONDS);
      Assert.assertFalse(event.isWaitingForTsFileParserMemory());
      Assert.assertEquals(0, manager.getWaitingTsFileParserCount());
      Assert.assertEquals(1, manager.getReservedTsFileParserCount());
    } finally {
      executor.shutdownNow();
      Assert.assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
      if (subtask != null) {
        subtask.disallowSubmittingSelf();
        subtask.close();
      }
      if (event != null) {
        event.close();
      }
      if (blockerReserved) {
        manager.releaseTsFileParserMemory("blocker", 0, "0");
      }
      config.setPipeTsFileParserMemory(originalParserMemory);
      config.setPipeTsFileParserInFlightMaxNum(originalGlobalLimit);
      config.setPipeTsFileParserInFlightMaxNumPerPipeRegion(originalRegionLimit);
      FileUtils.deleteFileOrDirectory(tempDir);
    }
  }

  @Test(timeout = 60000)
  public void testRealWorkerParserWaitAcrossStopStartAndDrop() throws Exception {
    final CommonConfig config = CommonDescriptor.getInstance().getConfig();
    final PipeMemoryManager manager = PipeDataNodeResourceManager.memory();
    final long originalParserMemory = config.getPipeTsFileParserMemory();
    final int originalGlobalLimit = config.getPipeTsFileParserInFlightMaxNum();
    final int originalRegionLimit = config.getPipeTsFileParserInFlightMaxNumPerPipeRegion();
    final File tempDir = Files.createTempDirectory("pipeProcessorWorkerLifecycle").toFile();
    final AtomicReference<Event> suppliedEvent = new AtomicReference<>();
    final UnboundedBlockingPendingQueue<Event> outputQueue =
        new UnboundedBlockingPendingQueue<>(new PipeDataRegionEventCounter());
    final AtomicInteger processorCalls = new AtomicInteger();
    final DoNothingProcessor processor =
        new DoNothingProcessor() {
          @Override
          public void process(final TsFileInsertionEvent event, final EventCollector collector)
              throws IOException {
            processorCalls.incrementAndGet();
            super.process(event, collector);
          }
        };
    final PipeProcessorSubtask subtask =
        new ParserSubtask(
            "lifecyclePipe", () -> suppliedEvent.getAndSet(null), processor, outputQueue);
    final PipeProcessorSubtaskWorker worker = new PipeProcessorSubtaskWorker();
    PipeTsFileInsertionEvent first = null;
    PipeTsFileInsertionEvent dropped = null;
    boolean blockerReserved = false;
    try {
      config.setPipeTsFileParserMemory(1);
      config.setPipeTsFileParserInFlightMaxNum(1);
      config.setPipeTsFileParserInFlightMaxNumPerPipeRegion(1);
      blockerReserved =
          manager.tryReserveTsFileParserMemory(
              "blocker", 0, "0", new TsFileParserMemoryReservation());
      Assert.assertTrue(blockerReserved);
      first = createEvent(tempDir, "resume.tsfile", "lifecyclePipe");
      Assert.assertTrue(first.increaseReferenceCount(getClass().getName()));
      suppliedEvent.set(first);
      worker.schedule(subtask);
      subtask.allowSubmittingSelf();

      Assert.assertTrue(worker.runSubtasks());
      Assert.assertTrue(subtask.isWaitingForTsFileParserMemory());
      Assert.assertTrue(worker.runSubtasks());
      Assert.assertEquals(1, processorCalls.get());
      Assert.assertTrue(subtask.disallowSubmittingSelf());
      Assert.assertFalse(first.isWaitingForTsFileParserMemory());
      Assert.assertEquals(0, manager.getWaitingTsFileParserCount());
      subtask.allowSubmittingSelf();
      Assert.assertTrue(worker.runSubtasks());
      Assert.assertTrue(subtask.isWaitingForTsFileParserMemory());
      Assert.assertEquals(2, processorCalls.get());
      Assert.assertTrue(outputQueue.isEmpty());

      manager.releaseTsFileParserMemory("blocker", 0, "0");
      blockerReserved = false;
      Assert.assertFalse(subtask.isWaitingForTsFileParserMemory());
      Assert.assertFalse(worker.runSubtasks());
      Assert.assertEquals(3, processorCalls.get());
      Assert.assertTrue(first.isReleased());
      Assert.assertTrue(outputQueue.getTabletInsertionEventCount() > 0);
      Assert.assertEquals(0, manager.getWaitingTsFileParserCount());
      Assert.assertEquals(0, manager.getReservedTsFileParserCount());
      int rowCount = 0;
      Event tablet;
      while ((tablet = outputQueue.directPoll()) != null) {
        Assert.assertTrue(tablet instanceof PipeRawTabletInsertionEvent);
        rowCount += ((PipeRawTabletInsertionEvent) tablet).convertToTablet().getRowSize();
        ((PipeRawTabletInsertionEvent) tablet).clearReferenceCount(getClass().getName());
      }
      Assert.assertEquals(10, rowCount);

      blockerReserved =
          manager.tryReserveTsFileParserMemory(
              "blocker", 0, "0", new TsFileParserMemoryReservation());
      Assert.assertTrue(blockerReserved);
      dropped = createEvent(tempDir, "drop.tsfile", "lifecyclePipe");
      Assert.assertTrue(dropped.increaseReferenceCount(getClass().getName()));
      suppliedEvent.set(dropped);
      Assert.assertTrue(worker.runSubtasks());
      Assert.assertTrue(subtask.isWaitingForTsFileParserMemory());
      Assert.assertTrue(subtask.disallowSubmittingSelf());
      subtask.close();
      Assert.assertTrue(dropped.isReleased());
      Assert.assertFalse(dropped.isWaitingForTsFileParserMemory());
      Assert.assertEquals(0, manager.getWaitingTsFileParserCount());
      Assert.assertEquals(1, manager.getReservedTsFileParserCount());
      Assert.assertTrue(worker.runSubtasks());
      Assert.assertEquals(4, processorCalls.get());
    } finally {
      subtask.disallowSubmittingSelf();
      subtask.close();
      if (first != null) {
        if (!first.isReleased()) {
          first.clearReferenceCount(getClass().getName());
        }
        first.close();
      }
      if (dropped != null) {
        if (!dropped.isReleased()) {
          dropped.clearReferenceCount(getClass().getName());
        }
        dropped.close();
      }
      outputQueue.discardAllEvents();
      if (blockerReserved) {
        manager.releaseTsFileParserMemory("blocker", 0, "0");
      }
      config.setPipeTsFileParserMemory(originalParserMemory);
      config.setPipeTsFileParserInFlightMaxNum(originalGlobalLimit);
      config.setPipeTsFileParserInFlightMaxNumPerPipeRegion(originalRegionLimit);
      FileUtils.deleteFileOrDirectory(tempDir);
    }
  }

  private static final class ParserSubtask extends PipeProcessorSubtask {
    private ParserSubtask(
        final String pipeName,
        final EventSupplier supplier,
        final DoNothingProcessor processor,
        final UnboundedBlockingPendingQueue<Event> outputQueue) {
      super(
          pipeName,
          pipeName,
          0,
          0,
          supplier,
          processor,
          new PipeEventCollector(outputQueue, 0, 0, 0, true, false, false));
      // Use the production scheduler without creating the process-wide worker thread pool.
      subtaskScheduler = new PipeSubtaskScheduler(mock(PipeSubtaskExecutor.class));
    }
  }

  @Test(timeout = 60000)
  public void testPauseAfterTabletResumesWithoutDuplicateConsumption() throws Exception {
    final CommonConfig commonConfig = CommonDescriptor.getInstance().getConfig();
    final long originalParserMemoryInBytes = commonConfig.getPipeTsFileParserMemory();
    final int originalGlobalLimit = commonConfig.getPipeTsFileParserInFlightMaxNum();
    final int originalPerPipeRegionLimit =
        commonConfig.getPipeTsFileParserInFlightMaxNumPerPipeRegion();
    final File tempDir = Files.createTempDirectory("pipeProcessorPauseResume").toFile();
    final PipeTsFileInsertionEvent event = createEvent(tempDir, "resume.tsfile", "resumePipe");
    final PipeProcessorSubtaskExecutionGuard executionGuard =
        new PipeProcessorSubtaskExecutionGuard();
    final AtomicInteger consumedTabletCount = new AtomicInteger(0);
    final AtomicReference<Object> firstTablet = new AtomicReference<>();

    try {
      commonConfig.setPipeTsFileParserMemory(1);
      commonConfig.setPipeTsFileParserInFlightMaxNum(1);
      commonConfig.setPipeTsFileParserInFlightMaxNumPerPipeRegion(1);
      executionGuard.start();
      executionGuard.enter();
      final PipeProcessorSubtaskYieldException pauseYield =
          Assert.assertThrows(
              PipeProcessorSubtaskYieldException.class,
              () ->
                  event.consumeTabletInsertionEventsWithRetry(
                      parsedEvent -> {
                        firstTablet.set(parsedEvent);
                        consumedTabletCount.incrementAndGet();
                        parsedEvent.clearReferenceCount(getClass().getName());
                        executionGuard.stop();
                      },
                      "test",
                      executionGuard));
      Assert.assertEquals(
          PipeProcessorSubtaskYieldException.Reason.PAUSE_REQUESTED, pauseYield.getReason());
      executionGuard.exit();

      executionGuard.start();
      executionGuard.enter();
      try {
        event.consumeTabletInsertionEventsWithRetry(
            parsedEvent -> {
              Assert.assertNotSame(firstTablet.get(), parsedEvent);
              consumedTabletCount.incrementAndGet();
              parsedEvent.clearReferenceCount(getClass().getName());
            },
            "test",
            executionGuard);
      } catch (final PipeProcessorSubtaskYieldException e) {
        Assert.fail("Unexpected yield reason: " + e.getReason());
      }
      executionGuard.exit();

      Assert.assertTrue(consumedTabletCount.get() > 0);
    } finally {
      event.close();
      commonConfig.setPipeTsFileParserMemory(originalParserMemoryInBytes);
      commonConfig.setPipeTsFileParserInFlightMaxNum(originalGlobalLimit);
      commonConfig.setPipeTsFileParserInFlightMaxNumPerPipeRegion(originalPerPipeRegionLimit);
      FileUtils.deleteFileOrDirectory(tempDir);
    }
  }

  private PipeTsFileInsertionEvent createEvent(
      final File tempDir, final String fileName, final String pipeName) throws Exception {
    final File dataDir = new File(tempDir, "sequence/root.testsg/0/0");
    Files.createDirectories(dataDir.toPath());
    final File tsFile =
        TsFileGeneratorUtils.generateNonAlignedTsFile(
            new File(dataDir, fileName).getAbsolutePath(), 1, 1, 10, 0, 100, 10, 10);
    final TsFileResource resource = new TsFileResource(tsFile);
    resource.setStatusForTest(TsFileResourceStatus.NORMAL);
    final IDeviceID deviceID = IDeviceID.Factory.DEFAULT_FACTORY.create("root.testsg.d0");
    resource.updateStartTime(deviceID, 0);
    resource.updateEndTime(deviceID, 9);

    return new PipeTsFileInsertionEvent(
        false,
        "root",
        resource,
        null,
        false,
        false,
        false,
        null,
        pipeName,
        0,
        null,
        new PrefixTreePattern("root"),
        null,
        null,
        "root",
        null,
        true,
        Long.MIN_VALUE,
        Long.MAX_VALUE);
  }
}
