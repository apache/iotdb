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
import org.apache.iotdb.commons.pipe.datastructure.pattern.PrefixPipePattern;
import org.apache.iotdb.commons.utils.FileUtils;
import org.apache.iotdb.db.pipe.event.common.tsfile.PipeTsFileInsertionEvent;
import org.apache.iotdb.db.pipe.resource.PipeDataNodeResourceManager;
import org.apache.iotdb.db.pipe.resource.memory.PipeMemoryManager;
import org.apache.iotdb.db.pipe.resource.memory.PipeMemoryManager.TsFileParserMemoryReservation;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResourceStatus;

import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.PlainDeviceID;
import org.apache.tsfile.utils.TsFileGeneratorUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.nio.file.Files;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

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
      executionGuard.start();
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
      executionGuard.exit();

      executionGuard.stop();
      event.cancelTsFileParserMemoryReservationIfPending();
      memoryManager.releaseTsFileParserMemory("blocker", 0, "0");
      isBlockerReserved = false;
      isCompetitorReserved =
          memoryManager.tryReserveTsFileParserMemory("competitor", 0, "0", competitorReservation);
      Assert.assertTrue(isCompetitorReserved);
      memoryManager.releaseTsFileParserMemory("competitor", 0, "0");
      isCompetitorReserved = false;

      final AtomicInteger consumedTabletCount = new AtomicInteger(0);
      executionGuard.start();
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
    final File tsFile =
        TsFileGeneratorUtils.generateNonAlignedTsFile(
            new File(tempDir, fileName).getAbsolutePath(), 1, 1, 10, 0, 100, 10, 10);
    final TsFileResource resource = new TsFileResource(tsFile);
    resource.setStatusForTest(TsFileResourceStatus.NORMAL);
    final IDeviceID deviceID = new PlainDeviceID("root.testsg.d0");
    resource.updateStartTime(deviceID, 0);
    resource.updateEndTime(deviceID, 9);

    return new PipeTsFileInsertionEvent(
        resource,
        null,
        false,
        false,
        false,
        pipeName,
        0,
        null,
        new PrefixPipePattern("root"),
        Long.MIN_VALUE,
        Long.MAX_VALUE);
  }
}
