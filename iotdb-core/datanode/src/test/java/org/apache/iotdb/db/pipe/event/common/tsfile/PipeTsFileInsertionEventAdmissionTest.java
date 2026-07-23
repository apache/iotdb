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

package org.apache.iotdb.db.pipe.event.common.tsfile;

import org.apache.iotdb.commons.conf.CommonConfig;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.pipe.datastructure.pattern.PrefixTreePattern;
import org.apache.iotdb.db.pipe.resource.PipeDataNodeResourceManager;
import org.apache.iotdb.db.pipe.resource.memory.PipeMemoryManager;
import org.apache.iotdb.db.pipe.resource.memory.PipeMemoryManager.TsFileParserMemoryReservation;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResourceStatus;
import org.apache.iotdb.pipe.api.event.dml.insertion.TabletInsertionEvent;

import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.utils.TsFileGeneratorUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class PipeTsFileInsertionEventAdmissionTest {

  @Test(timeout = 10000)
  public void testParserAdmissionIsWokenWhenMemoryIsReleased() throws Exception {
    final CommonConfig commonConfig = CommonDescriptor.getInstance().getConfig();
    final PipeMemoryManager memoryManager = PipeDataNodeResourceManager.memory();
    final long originalParserMemoryInBytes = commonConfig.getPipeTsFileParserMemory();
    final int originalGlobalLimit = commonConfig.getPipeTsFileParserInFlightMaxNum();
    final int originalPerPipeRegionLimit =
        commonConfig.getPipeTsFileParserInFlightMaxNumPerPipeRegion();
    final TsFileParserMemoryReservation blockerReservation = new TsFileParserMemoryReservation();

    File tsFile = null;
    PipeTsFileInsertionEvent event = null;
    ExecutorService executor = null;
    Future<Iterable<TabletInsertionEvent>> parsingFuture = null;
    boolean isBlockerReserved = false;
    try {
      commonConfig.setPipeTsFileParserMemory(1);
      commonConfig.setPipeTsFileParserInFlightMaxNum(1);
      commonConfig.setPipeTsFileParserInFlightMaxNumPerPipeRegion(1);
      isBlockerReserved =
          memoryManager.tryReserveTsFileParserMemory("blocker", 0, "0", blockerReservation);
      Assert.assertTrue(isBlockerReserved);

      tsFile =
          TsFileGeneratorUtils.generateNonAlignedTsFile(
              "parser-admission-backoff.tsfile", 1, 1, 10, 0, 100, 10, 10);
      final TsFileResource resource = new TsFileResource(tsFile);
      resource.setStatusForTest(TsFileResourceStatus.NORMAL);
      final IDeviceID deviceID = IDeviceID.Factory.DEFAULT_FACTORY.create("root.testsg.d0");
      resource.updateStartTime(deviceID, 0);
      resource.updateEndTime(deviceID, 9);
      event =
          new PipeTsFileInsertionEvent(
              false,
              "root",
              resource,
              null,
              false,
              false,
              false,
              null,
              "testPipe",
              0,
              null,
              new PrefixTreePattern("root"),
              null,
              null,
              null,
              null,
              true,
              Long.MIN_VALUE,
              Long.MAX_VALUE);

      executor = Executors.newSingleThreadExecutor();
      final PipeTsFileInsertionEvent eventToParse = event;
      parsingFuture = executor.submit(() -> eventToParse.toTabletInsertionEvents(30000));

      final Future<Iterable<TabletInsertionEvent>> blockedParsingFuture = parsingFuture;
      Assert.assertThrows(
          TimeoutException.class, () -> blockedParsingFuture.get(200, TimeUnit.MILLISECONDS));

      final long releaseTimeInNanos = System.nanoTime();
      memoryManager.releaseTsFileParserMemory("blocker", 0, "0");
      isBlockerReserved = false;

      Assert.assertNotNull(parsingFuture.get(3, TimeUnit.SECONDS));
      Assert.assertTrue(
          TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - releaseTimeInNanos) < 3000);
    } finally {
      if (parsingFuture != null) {
        parsingFuture.cancel(true);
      }
      if (executor != null) {
        executor.shutdownNow();
        executor.awaitTermination(3, TimeUnit.SECONDS);
      }
      if (event != null) {
        event.close();
      }
      memoryManager.cancelTsFileParserMemoryReservation("blocker", 0, "0", blockerReservation);
      if (isBlockerReserved) {
        memoryManager.releaseTsFileParserMemory("blocker", 0, "0");
      }
      commonConfig.setPipeTsFileParserMemory(originalParserMemoryInBytes);
      commonConfig.setPipeTsFileParserInFlightMaxNum(originalGlobalLimit);
      commonConfig.setPipeTsFileParserInFlightMaxNumPerPipeRegion(originalPerPipeRegionLimit);
      if (tsFile != null) {
        tsFile.delete();
      }
    }
  }
}
