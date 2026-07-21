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

package org.apache.iotdb.db.pipe.resource.memory;

import org.apache.iotdb.commons.conf.CommonConfig;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.db.pipe.resource.PipeDataNodeResourceManager;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class PipeMemoryManagerTest {

  private final PipeMemoryManager memoryManager = PipeDataNodeResourceManager.memory();
  private final CommonConfig commonConfig = CommonDescriptor.getInstance().getConfig();
  private final List<Reservation> reservations = new ArrayList<>();
  private int originalGlobalLimit;
  private int originalPerPipeLimit;
  private long originalParserMemoryInBytes;

  @Before
  public void setUp() {
    originalGlobalLimit = commonConfig.getPipeTsFileParserInFlightMaxNum();
    originalPerPipeLimit = commonConfig.getPipeTsFileParserInFlightMaxNumPerPipe();
    originalParserMemoryInBytes = commonConfig.getPipeTsFileParserMemory();
    commonConfig.setPipeTsFileParserMemory(1);
  }

  @After
  public void tearDown() {
    for (final Reservation reservation : reservations) {
      memoryManager.cancelTsFileParserMemoryReservation(
          reservation.pipeName, reservation.creationTime, reservation.key);
      if (reservation.acquired) {
        memoryManager.releaseTsFileParserMemory(reservation.pipeName, reservation.creationTime);
      }
    }
    commonConfig.setPipeTsFileParserInFlightMaxNum(originalGlobalLimit);
    commonConfig.setPipeTsFileParserInFlightMaxNumPerPipe(originalPerPipeLimit);
    commonConfig.setPipeTsFileParserMemory(originalParserMemoryInBytes);
  }

  @Test
  public void testWaitingPipesAreAdmittedInRoundRobinOrder() {
    commonConfig.setPipeTsFileParserInFlightMaxNum(1);
    commonConfig.setPipeTsFileParserInFlightMaxNumPerPipe(1);

    final Reservation pipeAActive = new Reservation("pipeA", 1);
    final Reservation pipeAFirstWaiting = new Reservation("pipeA", 1);
    final Reservation pipeASecondWaiting = new Reservation("pipeA", 1);
    final Reservation pipeBWaiting = new Reservation("pipeB", 2);

    Assert.assertTrue(tryAcquire(pipeAActive));
    Assert.assertFalse(tryAcquire(pipeAFirstWaiting));
    Assert.assertFalse(tryAcquire(pipeBWaiting));
    Assert.assertFalse(tryAcquire(pipeASecondWaiting));

    release(pipeAActive);
    Assert.assertTrue(tryAcquire(pipeAFirstWaiting));
    release(pipeAFirstWaiting);

    // Pipe A still has another waiting TsFile, but it was rotated behind pipe B after admission.
    Assert.assertFalse(tryAcquire(pipeASecondWaiting));
    Assert.assertTrue(tryAcquire(pipeBWaiting));
    release(pipeBWaiting);

    Assert.assertTrue(tryAcquire(pipeASecondWaiting));
  }

  @Test
  public void testGlobalAndPerPipeLimitsAreBothEnforced() {
    commonConfig.setPipeTsFileParserInFlightMaxNum(2);
    commonConfig.setPipeTsFileParserInFlightMaxNumPerPipe(1);

    final Reservation pipeAFirst = new Reservation("pipeA", 1);
    final Reservation pipeASecond = new Reservation("pipeA", 1);
    final Reservation pipeB = new Reservation("pipeB", 2);
    final Reservation pipeC = new Reservation("pipeC", 3);

    Assert.assertTrue(tryAcquire(pipeAFirst));
    Assert.assertFalse(tryAcquire(pipeASecond));
    Assert.assertTrue(tryAcquire(pipeB));
    Assert.assertFalse(tryAcquire(pipeC));

    release(pipeAFirst);
    Assert.assertTrue(tryAcquire(pipeASecond));
    Assert.assertFalse(tryAcquire(pipeC));

    release(pipeB);
    Assert.assertTrue(tryAcquire(pipeC));
  }

  @Test
  public void testSoftMemoryHeadroomIsReservedForPipeWithoutParser() {
    commonConfig.setPipeTsFileParserInFlightMaxNum(2);
    commonConfig.setPipeTsFileParserInFlightMaxNumPerPipe(2);

    final double tabletMemoryLimit =
        (commonConfig.getPipeDataStructureTabletMemoryBlockAllocationRejectThreshold()
                + commonConfig.getPipeDataStructureTsFileMemoryBlockAllocationRejectThreshold() / 2)
            * memoryManager.getTotalNonFloatingMemorySizeInBytes();
    final double tabletAndTsFileMemoryLimit =
        (commonConfig.getPipeDataStructureTabletMemoryBlockAllocationRejectThreshold()
                + commonConfig.getPipeDataStructureTsFileMemoryBlockAllocationRejectThreshold())
            * memoryManager.getTotalNonFloatingMemorySizeInBytes();
    commonConfig.setPipeTsFileParserMemory(
        Math.max(1, (long) (Math.min(tabletMemoryLimit, tabletAndTsFileMemoryLimit) * 0.49)));

    final Reservation pipeAActive = new Reservation("pipeA", 1);
    final Reservation pipeAWaiting = new Reservation("pipeA", 1);
    final Reservation pipeBWaiting = new Reservation("pipeB", 2);

    Assert.assertTrue(tryAcquire(pipeAActive));
    Assert.assertFalse(tryAcquire(pipeAWaiting));

    // The second parser would fit only below the hard threshold. Pipe A already has a parser, so
    // the headroom must go to pipe B even though pipe A is ahead in the waiting queue.
    Assert.assertTrue(tryAcquire(pipeBWaiting));
  }

  private boolean tryAcquire(final Reservation reservation) {
    if (!reservations.contains(reservation)) {
      reservations.add(reservation);
    }
    reservation.acquired =
        memoryManager.tryReserveTsFileParserMemory(
            reservation.pipeName, reservation.creationTime, reservation.key);
    return reservation.acquired;
  }

  private void release(final Reservation reservation) {
    if (!reservation.acquired) {
      return;
    }
    memoryManager.releaseTsFileParserMemory(reservation.pipeName, reservation.creationTime);
    reservation.acquired = false;
  }

  private static class Reservation {

    private final String pipeName;
    private final long creationTime;
    private final Object key = new Object();
    private boolean acquired;

    private Reservation(final String pipeName, final long creationTime) {
      this.pipeName = pipeName;
      this.creationTime = creationTime;
    }
  }
}
