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
import org.apache.iotdb.db.pipe.resource.memory.PipeMemoryManager.TsFileParserMemoryReservation;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class PipeMemoryManagerTest {

  private final PipeMemoryManager memoryManager = PipeDataNodeResourceManager.memory();
  private final CommonConfig commonConfig = CommonDescriptor.getInstance().getConfig();
  private final List<Reservation> reservations = new ArrayList<>();
  private int originalGlobalLimit;
  private int originalPerPipeRegionLimit;
  private long originalParserMemoryInBytes;

  @Before
  public void setUp() {
    originalGlobalLimit = commonConfig.getPipeTsFileParserInFlightMaxNum();
    originalPerPipeRegionLimit = commonConfig.getPipeTsFileParserInFlightMaxNumPerPipeRegion();
    originalParserMemoryInBytes = commonConfig.getPipeTsFileParserMemory();
    commonConfig.setPipeTsFileParserMemory(1);
  }

  @After
  public void tearDown() {
    for (final Reservation reservation : reservations) {
      memoryManager.cancelTsFileParserMemoryReservation(
          reservation.pipeName,
          reservation.creationTime,
          reservation.dataRegionId,
          reservation.key);
      if (reservation.acquired) {
        memoryManager.releaseTsFileParserMemory(
            reservation.pipeName, reservation.creationTime, reservation.dataRegionId);
      }
    }
    commonConfig.setPipeTsFileParserInFlightMaxNum(originalGlobalLimit);
    commonConfig.setPipeTsFileParserInFlightMaxNumPerPipeRegion(originalPerPipeRegionLimit);
    commonConfig.setPipeTsFileParserMemory(originalParserMemoryInBytes);
  }

  @Test
  public void testWaitingPipesAreAdmittedInRoundRobinOrder() {
    commonConfig.setPipeTsFileParserInFlightMaxNum(1);
    commonConfig.setPipeTsFileParserInFlightMaxNumPerPipeRegion(1);

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
  public void testGlobalAndPerPipeRegionLimitsAreBothEnforced() {
    commonConfig.setPipeTsFileParserInFlightMaxNum(2);
    commonConfig.setPipeTsFileParserInFlightMaxNumPerPipeRegion(1);

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
  public void testDifferentRegionsOfSamePipeCanRunConcurrently() {
    commonConfig.setPipeTsFileParserInFlightMaxNum(2);
    commonConfig.setPipeTsFileParserInFlightMaxNumPerPipeRegion(1);

    final Reservation pipeARegion1First = new Reservation("pipeA", 1, "1");
    final Reservation pipeARegion1Second = new Reservation("pipeA", 1, "1");
    final Reservation pipeARegion2 = new Reservation("pipeA", 1, "2");

    Assert.assertTrue(tryAcquire(pipeARegion1First));
    Assert.assertFalse(tryAcquire(pipeARegion1Second));
    Assert.assertTrue(tryAcquire(pipeARegion2));
  }

  @Test
  public void testWaitingRegionsWithinPipeAreAdmittedInRoundRobinOrder() {
    commonConfig.setPipeTsFileParserInFlightMaxNum(1);
    commonConfig.setPipeTsFileParserInFlightMaxNumPerPipeRegion(1);

    final Reservation blocker = new Reservation("blocker", 0);
    final Reservation pipeARegion1First = new Reservation("pipeA", 1, "1");
    final Reservation pipeARegion1Second = new Reservation("pipeA", 1, "1");
    final Reservation pipeARegion2 = new Reservation("pipeA", 1, "2");

    Assert.assertTrue(tryAcquire(blocker));
    Assert.assertFalse(tryAcquire(pipeARegion1First));
    Assert.assertFalse(tryAcquire(pipeARegion1Second));
    Assert.assertFalse(tryAcquire(pipeARegion2));

    release(blocker);
    Assert.assertTrue(tryAcquire(pipeARegion1First));
    release(pipeARegion1First);

    Assert.assertFalse(tryAcquire(pipeARegion1Second));
    Assert.assertTrue(tryAcquire(pipeARegion2));
    release(pipeARegion2);

    Assert.assertTrue(tryAcquire(pipeARegion1Second));
  }

  @Test
  public void testPipeFairnessIsNotWeightedByRegionCount() {
    commonConfig.setPipeTsFileParserInFlightMaxNum(1);
    commonConfig.setPipeTsFileParserInFlightMaxNumPerPipeRegion(1);

    final Reservation blocker = new Reservation("blocker", 0);
    final Reservation pipeARegion1 = new Reservation("pipeA", 1, "1");
    final Reservation pipeARegion2 = new Reservation("pipeA", 1, "2");
    final Reservation pipeARegion3 = new Reservation("pipeA", 1, "3");
    final Reservation pipeBRegion1 = new Reservation("pipeB", 2, "1");

    Assert.assertTrue(tryAcquire(blocker));
    Assert.assertFalse(tryAcquire(pipeARegion1));
    Assert.assertFalse(tryAcquire(pipeARegion2));
    Assert.assertFalse(tryAcquire(pipeARegion3));
    Assert.assertFalse(tryAcquire(pipeBRegion1));

    release(blocker);
    Assert.assertTrue(tryAcquire(pipeARegion1));
    release(pipeARegion1);

    Assert.assertFalse(tryAcquire(pipeARegion2));
    Assert.assertTrue(tryAcquire(pipeBRegion1));
    release(pipeBRegion1);

    Assert.assertTrue(tryAcquire(pipeARegion2));
  }

  @Test
  public void testSoftMemoryHeadroomIsReservedForPipeWithoutParser() {
    commonConfig.setPipeTsFileParserInFlightMaxNum(2);
    commonConfig.setPipeTsFileParserInFlightMaxNumPerPipeRegion(2);

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

    final Reservation pipeAActive = new Reservation("pipeA", 1, "1");
    final Reservation pipeAWaiting = new Reservation("pipeA", 1, "2");
    final Reservation pipeBWaiting = new Reservation("pipeB", 2, "1");

    Assert.assertTrue(tryAcquire(pipeAActive));
    Assert.assertFalse(tryAcquire(pipeAWaiting));

    // The second parser would fit only below the hard threshold. Pipe A already has a parser, so
    // the headroom must go to pipe B even though pipe A is ahead in the waiting queue.
    Assert.assertTrue(tryAcquire(pipeBWaiting));
  }

  @Test
  public void testConcurrentTsFilesFromMultiplePipesAreNotStarved() throws Exception {
    commonConfig.setPipeTsFileParserInFlightMaxNum(1);
    commonConfig.setPipeTsFileParserInFlightMaxNumPerPipeRegion(1);

    final Reservation blocker = new Reservation("blocker", 0);
    Assert.assertTrue(tryAcquire(blocker));

    // Each reservation represents a distinct TsFile event. Pipe A deliberately has more waiting
    // TsFiles so the test can detect whether it monopolizes the single parser slot.
    final List<Reservation> waitingTsFiles = new ArrayList<>();
    for (int i = 0; i < 5; i++) {
      waitingTsFiles.add(new Reservation("pipeA", 1));
    }
    for (int i = 0; i < 2; i++) {
      waitingTsFiles.add(new Reservation("pipeB", 2));
      waitingTsFiles.add(new Reservation("pipeC", 3));
    }
    reservations.addAll(waitingTsFiles);

    final List<String> acquisitionOrder = Collections.synchronizedList(new ArrayList<>());
    final CountDownLatch ready = new CountDownLatch(waitingTsFiles.size());
    final CountDownLatch start = new CountDownLatch(1);
    final CountDownLatch enqueued = new CountDownLatch(waitingTsFiles.size());
    final ExecutorService executor = Executors.newFixedThreadPool(waitingTsFiles.size());
    final List<Future<Boolean>> futures = new ArrayList<>();

    try {
      for (final Reservation reservation : waitingTsFiles) {
        futures.add(
            executor.submit(
                () -> {
                  ready.countDown();
                  start.await();

                  boolean acquired = tryAcquireWithoutTracking(reservation);
                  enqueued.countDown();
                  final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
                  while (!acquired && System.nanoTime() < deadline) {
                    reservation.key.await(
                        Math.max(
                            1,
                            TimeUnit.NANOSECONDS.toMillis(
                                Math.max(1, deadline - System.nanoTime()))));
                    acquired = tryAcquireWithoutTracking(reservation);
                  }
                  if (!acquired) {
                    return false;
                  }

                  acquisitionOrder.add(reservation.pipeName);
                  Thread.sleep(5);
                  release(reservation);
                  return true;
                }));
      }

      Assert.assertTrue(ready.await(5, TimeUnit.SECONDS));
      start.countDown();
      Assert.assertTrue(enqueued.await(5, TimeUnit.SECONDS));
      release(blocker);

      for (final Future<Boolean> future : futures) {
        Assert.assertTrue(future.get(15, TimeUnit.SECONDS));
      }
      Assert.assertEquals(waitingTsFiles.size(), acquisitionOrder.size());
      Assert.assertEquals(3, new HashSet<>(acquisitionOrder.subList(0, 3)).size());
    } finally {
      release(blocker);
      executor.shutdownNow();
      Assert.assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
    }
  }

  private boolean tryAcquire(final Reservation reservation) {
    if (!reservations.contains(reservation)) {
      reservations.add(reservation);
    }
    reservation.acquired =
        memoryManager.tryReserveTsFileParserMemory(
            reservation.pipeName,
            reservation.creationTime,
            reservation.dataRegionId,
            reservation.key);
    return reservation.acquired;
  }

  private boolean tryAcquireWithoutTracking(final Reservation reservation) {
    reservation.acquired =
        memoryManager.tryReserveTsFileParserMemory(
            reservation.pipeName,
            reservation.creationTime,
            reservation.dataRegionId,
            reservation.key);
    return reservation.acquired;
  }

  private void release(final Reservation reservation) {
    if (!reservation.acquired) {
      return;
    }
    memoryManager.releaseTsFileParserMemory(
        reservation.pipeName, reservation.creationTime, reservation.dataRegionId);
    reservation.acquired = false;
  }

  private static class Reservation {

    private final String pipeName;
    private final long creationTime;
    private final String dataRegionId;
    private final TsFileParserMemoryReservation key = new TsFileParserMemoryReservation();
    private volatile boolean acquired;

    private Reservation(final String pipeName, final long creationTime) {
      this(pipeName, creationTime, "0");
    }

    private Reservation(final String pipeName, final long creationTime, final String dataRegionId) {
      this.pipeName = pipeName;
      this.creationTime = creationTime;
      this.dataRegionId = dataRegionId;
    }
  }
}
