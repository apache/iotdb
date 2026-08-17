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

import org.apache.iotdb.commons.exception.pipe.PipeRuntimeOutOfMemoryCriticalException;
import org.apache.iotdb.commons.pipe.config.PipeConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.pipe.agent.PipeDataNodeAgent;
import org.apache.iotdb.db.pipe.resource.memory.strategy.ThresholdAllocationStrategy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.LongUnaryOperator;

public class PipeMemoryManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(PipeMemoryManager.class);

  private static final PipeConfig PIPE_CONFIG = PipeConfig.getInstance();

  private static final boolean PIPE_MEMORY_MANAGEMENT_ENABLED =
      PipeConfig.getInstance().getPipeMemoryManagementEnabled();
  private static final long TOTAL_MEMORY_SIZE_IN_BYTES =
      IoTDBDescriptor.getInstance().getConfig().getAllocateMemoryForPipe();
  private static final long MEMORY_ALLOCATE_MIN_SIZE_IN_BYTES =
      PipeConfig.getInstance().getPipeMemoryAllocateMinSizeInBytes();

  private long usedMemorySizeInBytes;

  private static final double EXCEED_PROTECT_THRESHOLD = 0.95;

  private volatile long usedMemorySizeInBytesOfTablets;

  private volatile long usedMemorySizeInBytesOfTsFiles;

  private volatile long reservedTsFileParserCount;

  private final Map<PipeIdentity, Integer> reservedTsFileParserCountByPipe = new HashMap<>();
  private final Map<PipeRegionIdentity, Integer> reservedTsFileParserCountByPipeRegion =
      new HashMap<>();
  private final Map<PipeRegionIdentity, LinkedHashSet<TsFileParserMemoryReservation>>
      waitingTsFileParserRequestsByPipeRegion = new HashMap<>();
  private final Map<PipeIdentity, ArrayDeque<PipeRegionIdentity>>
      waitingTsFileParserRegionOrderByPipe = new HashMap<>();
  private final ArrayDeque<PipeIdentity> waitingTsFileParserPipeOrder = new ArrayDeque<>();
  private PipeIdentity lastAdmittedWaitingTsFileParserPipe;

  // Only non-zero memory blocks will be added to this set.
  private final Set<PipeMemoryBlock> allocatedBlocks = new HashSet<>();
  private final Set<PipeMemoryBlock> shrinkableBlocks = new HashSet<>();
  private final Set<PipeMemoryBlock> expandableBlocks = new HashSet<>();

  public PipeMemoryManager() {
    PipeDataNodeAgent.runtime()
        .registerPeriodicalJob(
            "PipeMemoryManager#tryExpandAll()",
            this::tryExpandAllAndCheckConsistency,
            PipeConfig.getInstance().getPipeMemoryExpanderIntervalSeconds());
  }

  // NOTE: Here we unify the memory threshold judgment for tablet and tsfile memory block, because
  // introducing too many heuristic rules not conducive to flexible dynamic adjustment of memory
  // configuration:
  // 1. Proportion of memory occupied by tablet memory block: [TABLET_MEMORY_REJECT_THRESHOLD / 2,
  // TABLET_MEMORY_REJECT_THRESHOLD + TS_FILE_MEMORY_REJECT_THRESHOLD / 2]
  // 2. Proportion of memory occupied by tsfile memory block: [TS_FILE_MEMORY_REJECT_THRESHOLD / 2,
  // TS_FILE_MEMORY_REJECT_THRESHOLD + TABLET_MEMORY_REJECT_THRESHOLD / 2]
  // 3. The sum of the memory proportion occupied by the tablet memory block and the tsfile memory
  // block does not exceed TABLET_MEMORY_REJECT_THRESHOLD + TS_FILE_MEMORY_REJECT_THRESHOLD

  private static double allowedMaxMemorySizeInBytesOfTabletsAndTsFiles() {
    return (PipeConfig.getInstance()
                .getPipeDataStructureTabletMemoryBlockAllocationRejectThreshold()
            + PipeConfig.getInstance()
                .getPipeDataStructureTsFileMemoryBlockAllocationRejectThreshold())
        * getTotalNonFloatingMemorySizeInBytes();
  }

  private static double allowedMaxMemorySizeInBytesOfTablets() {
    return (PipeConfig.getInstance()
                .getPipeDataStructureTabletMemoryBlockAllocationRejectThreshold()
            + PipeConfig.getInstance()
                    .getPipeDataStructureTsFileMemoryBlockAllocationRejectThreshold()
                / 2)
        * getTotalNonFloatingMemorySizeInBytes();
  }

  private static double allowedMaxMemorySizeInBytesOfTsTiles() {
    return (PipeConfig.getInstance()
                .getPipeDataStructureTsFileMemoryBlockAllocationRejectThreshold()
            + PipeConfig.getInstance()
                    .getPipeDataStructureTabletMemoryBlockAllocationRejectThreshold()
                / 2)
        * getTotalNonFloatingMemorySizeInBytes();
  }

  private static long getTsFileParserMemorySizeInBytes() {
    return Math.max(
        PipeConfig.getInstance().getTsFileParserMemory(), MEMORY_ALLOCATE_MIN_SIZE_IN_BYTES);
  }

  private long getReservedTsFileParserMemorySizeInBytes() {
    return reservedTsFileParserCount * getTsFileParserMemorySizeInBytes();
  }

  private boolean isEnough4TabletParsingWithReservedParserMemory(final long extraMemoryInBytes) {
    final double tabletMemoryWithParserMemory =
        (double) usedMemorySizeInBytesOfTablets
            + getReservedTsFileParserMemorySizeInBytes()
            + extraMemoryInBytes;
    return tabletMemoryWithParserMemory + (double) usedMemorySizeInBytesOfTsFiles
            < EXCEED_PROTECT_THRESHOLD * allowedMaxMemorySizeInBytesOfTabletsAndTsFiles()
        && tabletMemoryWithParserMemory
            < EXCEED_PROTECT_THRESHOLD * allowedMaxMemorySizeInBytesOfTablets();
  }

  private boolean isHardEnough4TabletParsingWithReservedParserMemory(
      final long extraMemoryInBytes) {
    final double tabletMemoryWithParserMemory =
        (double) usedMemorySizeInBytesOfTablets
            + getReservedTsFileParserMemorySizeInBytes()
            + extraMemoryInBytes;
    return tabletMemoryWithParserMemory + (double) usedMemorySizeInBytesOfTsFiles
            < allowedMaxMemorySizeInBytesOfTabletsAndTsFiles()
        && tabletMemoryWithParserMemory < allowedMaxMemorySizeInBytesOfTablets();
  }

  public boolean isEnough4TabletParsing() {
    return (double) usedMemorySizeInBytesOfTablets + (double) usedMemorySizeInBytesOfTsFiles
            < EXCEED_PROTECT_THRESHOLD * allowedMaxMemorySizeInBytesOfTabletsAndTsFiles()
        && (double) usedMemorySizeInBytesOfTablets
            < EXCEED_PROTECT_THRESHOLD * allowedMaxMemorySizeInBytesOfTablets();
  }

  private boolean isHardEnough4TabletParsing() {
    return (double) usedMemorySizeInBytesOfTablets + (double) usedMemorySizeInBytesOfTsFiles
            < allowedMaxMemorySizeInBytesOfTabletsAndTsFiles()
        && (double) usedMemorySizeInBytesOfTablets < allowedMaxMemorySizeInBytesOfTablets();
  }

  public synchronized boolean tryReserveTsFileParserMemory(
      final String pipeName,
      final long creationTime,
      final String dataRegionId,
      final TsFileParserMemoryReservation reservationKey) {
    if (reservationKey == null) {
      return false;
    }

    final PipeIdentity pipeIdentity = new PipeIdentity(pipeName, creationTime);
    final PipeRegionIdentity pipeRegionIdentity =
        new PipeRegionIdentity(pipeIdentity, dataRegionId);
    final boolean wasRequestAlreadyWaiting =
        enqueueTsFileParserReservationRequest(pipeRegionIdentity, reservationKey);

    final int globalLimit = Math.max(1, PIPE_CONFIG.getPipeTsFileParserInFlightMaxNum());
    final int perPipeRegionLimit = getTsFileParserInFlightMaxNumPerPipeRegion(globalLimit);
    final int reservedCountOfPipeRegion =
        reservedTsFileParserCountByPipeRegion.getOrDefault(pipeRegionIdentity, 0);
    if (reservedTsFileParserCount >= globalLimit
        || reservedCountOfPipeRegion >= perPipeRegionLimit) {
      notifyNextTsFileParserMemoryReservationInternal();
      return false;
    }

    final long parserMemorySizeInBytes = getTsFileParserMemorySizeInBytes();
    final boolean isSoftMemoryEnough =
        !PIPE_MEMORY_MANAGEMENT_ENABLED
            || isEnough4TabletParsingWithReservedParserMemory(parserMemorySizeInBytes);
    if (!isSoftMemoryEnough
        && !isHardEnough4TabletParsingWithReservedParserMemory(parserMemorySizeInBytes)) {
      return false;
    }

    final PipeRegionIdentity nextPipeRegion =
        getNextEligibleTsFileParserPipeRegion(perPipeRegionLimit, !isSoftMemoryEnough);
    final LinkedHashSet<TsFileParserMemoryReservation> requestsOfPipeRegion =
        waitingTsFileParserRequestsByPipeRegion.get(pipeRegionIdentity);
    if (!pipeRegionIdentity.equals(nextPipeRegion)
        || requestsOfPipeRegion == null
        || !reservationKey.equals(requestsOfPipeRegion.iterator().next())) {
      notifyNextTsFileParserMemoryReservationInternal();
      return false;
    }

    removeTsFileParserReservationRequest(pipeRegionIdentity, reservationKey, true);
    if (wasRequestAlreadyWaiting) {
      lastAdmittedWaitingTsFileParserPipe = pipeIdentity;
    }
    reservedTsFileParserCount++;
    reservedTsFileParserCountByPipe.merge(pipeIdentity, 1, Integer::sum);
    reservedTsFileParserCountByPipeRegion.put(pipeRegionIdentity, reservedCountOfPipeRegion + 1);
    notifyNextTsFileParserMemoryReservationInternal();
    return true;
  }

  public synchronized void cancelTsFileParserMemoryReservation(
      final String pipeName,
      final long creationTime,
      final String dataRegionId,
      final TsFileParserMemoryReservation reservationKey) {
    if (reservationKey == null) {
      return;
    }
    removeTsFileParserReservationRequest(
        new PipeRegionIdentity(new PipeIdentity(pipeName, creationTime), dataRegionId),
        reservationKey,
        false);
    notifyNextTsFileParserMemoryReservationInternal();
  }

  public synchronized void releaseTsFileParserMemory(
      final String pipeName, final long creationTime, final String dataRegionId) {
    final PipeIdentity pipeIdentity = new PipeIdentity(pipeName, creationTime);
    final PipeRegionIdentity pipeRegionIdentity =
        new PipeRegionIdentity(pipeIdentity, dataRegionId);
    final int reservedCountOfPipeRegion =
        reservedTsFileParserCountByPipeRegion.getOrDefault(pipeRegionIdentity, 0);
    if (reservedCountOfPipeRegion <= 0) {
      LOGGER.warn(
          "Failed to release TsFile parser memory for pipe {} (creation time {}) in DataRegion {} because no reservation exists.",
          pipeName,
          creationTime,
          dataRegionId);
      return;
    }

    if (reservedCountOfPipeRegion == 1) {
      reservedTsFileParserCountByPipeRegion.remove(pipeRegionIdentity);
    } else {
      reservedTsFileParserCountByPipeRegion.put(pipeRegionIdentity, reservedCountOfPipeRegion - 1);
    }
    final int reservedCountOfPipe = reservedTsFileParserCountByPipe.getOrDefault(pipeIdentity, 0);
    if (reservedCountOfPipe == 1) {
      reservedTsFileParserCountByPipe.remove(pipeIdentity);
    } else {
      reservedTsFileParserCountByPipe.put(pipeIdentity, reservedCountOfPipe - 1);
    }
    reservedTsFileParserCount--;
    clearTsFileParserAdmissionCursorIfIdle();
    notifyNextTsFileParserMemoryReservationInternal();
  }

  private boolean enqueueTsFileParserReservationRequest(
      final PipeRegionIdentity pipeRegionIdentity,
      final TsFileParserMemoryReservation reservationKey) {
    final LinkedHashSet<TsFileParserMemoryReservation> requestsOfPipeRegion =
        waitingTsFileParserRequestsByPipeRegion.computeIfAbsent(
            pipeRegionIdentity,
            key -> {
              final ArrayDeque<PipeRegionIdentity> regionOrder =
                  waitingTsFileParserRegionOrderByPipe.computeIfAbsent(
                      key.pipeIdentity,
                      pipe -> {
                        waitingTsFileParserPipeOrder.addLast(pipe);
                        return new ArrayDeque<>();
                      });
              regionOrder.addLast(key);
              return new LinkedHashSet<>();
            });
    return !requestsOfPipeRegion.add(reservationKey);
  }

  public synchronized void notifyNextTsFileParserMemoryReservation() {
    notifyNextTsFileParserMemoryReservationInternal();
  }

  private void notifyNextTsFileParserMemoryReservationInternal() {
    final int globalLimit = Math.max(1, PIPE_CONFIG.getPipeTsFileParserInFlightMaxNum());
    if (reservedTsFileParserCount >= globalLimit) {
      return;
    }

    final long parserMemorySizeInBytes = getTsFileParserMemorySizeInBytes();
    final boolean isSoftMemoryEnough =
        !PIPE_MEMORY_MANAGEMENT_ENABLED
            || isEnough4TabletParsingWithReservedParserMemory(parserMemorySizeInBytes);
    if (!isSoftMemoryEnough
        && !isHardEnough4TabletParsingWithReservedParserMemory(parserMemorySizeInBytes)) {
      return;
    }

    final int perPipeRegionLimit = getTsFileParserInFlightMaxNumPerPipeRegion(globalLimit);
    final PipeRegionIdentity nextPipeRegion =
        getNextEligibleTsFileParserPipeRegion(perPipeRegionLimit, !isSoftMemoryEnough);
    if (nextPipeRegion == null) {
      return;
    }

    final LinkedHashSet<TsFileParserMemoryReservation> requestsOfPipeRegion =
        waitingTsFileParserRequestsByPipeRegion.get(nextPipeRegion);
    if (requestsOfPipeRegion != null && !requestsOfPipeRegion.isEmpty()) {
      requestsOfPipeRegion.iterator().next().signal();
    }
  }

  private PipeRegionIdentity getNextEligibleTsFileParserPipeRegion(
      final int perPipeRegionLimit, final boolean requirePipeWithoutReservedParser) {
    PipeRegionIdentity firstEligiblePipeRegion = null;
    boolean hasVisitedLastAdmittedPipe = lastAdmittedWaitingTsFileParserPipe == null;
    for (final PipeIdentity pipeIdentity : waitingTsFileParserPipeOrder) {
      final boolean isLastAdmittedPipe = pipeIdentity.equals(lastAdmittedWaitingTsFileParserPipe);
      if (isLastAdmittedPipe) {
        hasVisitedLastAdmittedPipe = true;
      }

      // Under soft memory pressure, reserve the hard-threshold headroom for a pipe that has no
      // parser yet. Otherwise a busy pipe at the queue head can block every pipe behind it.
      if (requirePipeWithoutReservedParser
          && reservedTsFileParserCountByPipe.getOrDefault(pipeIdentity, 0) > 0) {
        continue;
      }

      final ArrayDeque<PipeRegionIdentity> regionOrder =
          waitingTsFileParserRegionOrderByPipe.get(pipeIdentity);
      if (regionOrder == null) {
        continue;
      }
      PipeRegionIdentity eligiblePipeRegion = null;
      for (final PipeRegionIdentity pipeRegionIdentity : regionOrder) {
        if (reservedTsFileParserCountByPipeRegion.getOrDefault(pipeRegionIdentity, 0)
            < perPipeRegionLimit) {
          eligiblePipeRegion = pipeRegionIdentity;
          break;
        }
      }
      if (eligiblePipeRegion == null) {
        continue;
      }

      if (firstEligiblePipeRegion == null) {
        firstEligiblePipeRegion = eligiblePipeRegion;
      }
      if (hasVisitedLastAdmittedPipe && !isLastAdmittedPipe) {
        return eligiblePipeRegion;
      }
    }
    return firstEligiblePipeRegion;
  }

  private static int getTsFileParserInFlightMaxNumPerPipeRegion(final int globalLimit) {
    final int configuredLimit = PIPE_CONFIG.getPipeTsFileParserInFlightMaxNumPerPipeRegion();
    return configuredLimit <= 0 ? globalLimit : Math.min(globalLimit, configuredLimit);
  }

  private void clearTsFileParserAdmissionCursorIfIdle() {
    if (reservedTsFileParserCount == 0 && waitingTsFileParserPipeOrder.isEmpty()) {
      lastAdmittedWaitingTsFileParserPipe = null;
    }
  }

  private void removeTsFileParserReservationRequest(
      final PipeRegionIdentity pipeRegionIdentity,
      final TsFileParserMemoryReservation reservationKey,
      final boolean rotateAfterAdmission) {
    final LinkedHashSet<TsFileParserMemoryReservation> requestsOfPipeRegion =
        waitingTsFileParserRequestsByPipeRegion.get(pipeRegionIdentity);
    if (requestsOfPipeRegion == null || !requestsOfPipeRegion.remove(reservationKey)) {
      return;
    }

    final PipeIdentity pipeIdentity = pipeRegionIdentity.pipeIdentity;
    final ArrayDeque<PipeRegionIdentity> regionOrder =
        waitingTsFileParserRegionOrderByPipe.get(pipeIdentity);
    if (requestsOfPipeRegion.isEmpty()) {
      waitingTsFileParserRequestsByPipeRegion.remove(pipeRegionIdentity);
      if (regionOrder != null) {
        regionOrder.remove(pipeRegionIdentity);
        if (regionOrder.isEmpty()) {
          waitingTsFileParserRegionOrderByPipe.remove(pipeIdentity);
          waitingTsFileParserPipeOrder.remove(pipeIdentity);
          if (!rotateAfterAdmission) {
            clearTsFileParserAdmissionCursorIfIdle();
          }
          return;
        }
      }
    } else if (rotateAfterAdmission && regionOrder != null) {
      regionOrder.remove(pipeRegionIdentity);
      regionOrder.addLast(pipeRegionIdentity);
    }

    if (rotateAfterAdmission) {
      waitingTsFileParserPipeOrder.remove(pipeIdentity);
      waitingTsFileParserPipeOrder.addLast(pipeIdentity);
    } else {
      clearTsFileParserAdmissionCursorIfIdle();
    }
  }

  public static final class TsFileParserMemoryReservation {

    private boolean isSignaled;

    public synchronized void await(final long timeoutInMs) throws InterruptedException {
      if (!isSignaled) {
        wait(timeoutInMs);
      }
      isSignaled = false;
    }

    private synchronized void signal() {
      isSignaled = true;
      notify();
    }
  }

  public boolean shouldReleaseTsFileParserOnOutOfMemory(
      final long firstOutOfMemoryTimeInMs, final int retryCount) {
    final long retryIntervalInMs =
        PipeConfig.getInstance().getPipeMemoryAllocateRetryIntervalInMs();
    final long minRetryTimeInMs = Math.max(retryIntervalInMs * 2, 1);
    final long maxRetryTimeInMs =
        Math.max(
            minRetryTimeInMs,
            retryIntervalInMs * PipeConfig.getInstance().getPipeMemoryAllocateMaxRetries());

    final long elapsedTimeInMs = System.currentTimeMillis() - firstOutOfMemoryTimeInMs;
    if (elapsedTimeInMs < minRetryTimeInMs) {
      return false;
    }

    if (!PIPE_MEMORY_MANAGEMENT_ENABLED) {
      return elapsedTimeInMs >= maxRetryTimeInMs;
    }

    if (!isHardEnough4TabletParsingWithReservedParserMemory(0)) {
      return true;
    }

    return retryCount >= PipeConfig.getInstance().getPipeMemoryAllocateMaxRetries()
        || elapsedTimeInMs >= maxRetryTimeInMs;
  }

  public boolean isEnough4TsFileSlicing() {
    return (double) usedMemorySizeInBytesOfTablets + (double) usedMemorySizeInBytesOfTsFiles
            < EXCEED_PROTECT_THRESHOLD * allowedMaxMemorySizeInBytesOfTabletsAndTsFiles()
        && (double) usedMemorySizeInBytesOfTsFiles
            < EXCEED_PROTECT_THRESHOLD * allowedMaxMemorySizeInBytesOfTsTiles();
  }

  private boolean isHardEnough4TsFileSlicing() {
    return (double) usedMemorySizeInBytesOfTablets + (double) usedMemorySizeInBytesOfTsFiles
            < allowedMaxMemorySizeInBytesOfTabletsAndTsFiles()
        && (double) usedMemorySizeInBytesOfTsFiles < allowedMaxMemorySizeInBytesOfTsTiles();
  }

  private boolean isHardEnoughForResizing(
      final PipeMemoryBlock block, final long extraMemoryInBytes) {
    if (block instanceof PipeTabletMemoryBlock) {
      return (double) usedMemorySizeInBytesOfTablets
                  + (double) extraMemoryInBytes
                  + (double) usedMemorySizeInBytesOfTsFiles
              < allowedMaxMemorySizeInBytesOfTabletsAndTsFiles()
          && (double) usedMemorySizeInBytesOfTablets + (double) extraMemoryInBytes
              < allowedMaxMemorySizeInBytesOfTablets();
    }
    if (block instanceof PipeTsFileMemoryBlock) {
      return (double) usedMemorySizeInBytesOfTablets
                  + (double) usedMemorySizeInBytesOfTsFiles
                  + (double) extraMemoryInBytes
              < allowedMaxMemorySizeInBytesOfTabletsAndTsFiles()
          && (double) usedMemorySizeInBytesOfTsFiles + (double) extraMemoryInBytes
              < allowedMaxMemorySizeInBytesOfTsTiles();
    }
    return true;
  }

  public synchronized PipeMemoryBlock forceAllocate(long sizeInBytes)
      throws PipeRuntimeOutOfMemoryCriticalException {
    if (!PIPE_MEMORY_MANAGEMENT_ENABLED) {
      // No need to calculate the tablet size, skip it to save time
      return new PipeMemoryBlock(0);
    }

    if (sizeInBytes == 0) {
      return registerMemoryBlock(0);
    }

    return forceAllocateWithRetry(sizeInBytes, PipeMemoryBlockType.NORMAL);
  }

  public PipeTabletMemoryBlock forceAllocateForTabletWithRetry(long tabletSizeInBytes)
      throws PipeRuntimeOutOfMemoryCriticalException {
    if (!PIPE_MEMORY_MANAGEMENT_ENABLED) {
      // No need to calculate the tablet size, skip it to save time
      return new PipeTabletMemoryBlock(0);
    }

    if (tabletSizeInBytes == 0) {
      return (PipeTabletMemoryBlock) registerMemoryBlock(0, PipeMemoryBlockType.TABLET);
    }

    for (int i = 1, size = PipeConfig.getInstance().getPipeMemoryAllocateMaxRetries();
        i <= size;
        i++) {
      if (isHardEnough4TabletParsing()) {
        break;
      }

      try {
        Thread.sleep(PipeConfig.getInstance().getPipeMemoryAllocateRetryIntervalInMs());
      } catch (InterruptedException ex) {
        Thread.currentThread().interrupt();
        LOGGER.warn("forceAllocateWithRetry: interrupted while waiting for available memory", ex);
      }
    }

    if (!isHardEnough4TabletParsing()) {
      throw new PipeRuntimeOutOfMemoryCriticalException(
          String.format(
              "forceAllocateForTablet: failed to allocate because there's too much memory for tablets, "
                  + "total memory size %d bytes, used memory for tablet size %d bytes, requested memory size %d bytes",
              getTotalNonFloatingMemorySizeInBytes(),
              usedMemorySizeInBytesOfTablets,
              tabletSizeInBytes));
    }

    synchronized (this) {
      final PipeTabletMemoryBlock block =
          (PipeTabletMemoryBlock)
              forceAllocateWithRetry(tabletSizeInBytes, PipeMemoryBlockType.TABLET);
      usedMemorySizeInBytesOfTablets += block.getMemoryUsageInBytes();
      return block;
    }
  }

  public PipeTsFileMemoryBlock forceAllocateForTsFileWithRetry(long tsFileSizeInBytes)
      throws PipeRuntimeOutOfMemoryCriticalException {
    if (!PIPE_MEMORY_MANAGEMENT_ENABLED) {
      return new PipeTsFileMemoryBlock(0);
    }

    if (tsFileSizeInBytes == 0) {
      return (PipeTsFileMemoryBlock) registerMemoryBlock(0, PipeMemoryBlockType.TS_FILE);
    }

    for (int i = 1, size = PipeConfig.getInstance().getPipeMemoryAllocateMaxRetries();
        i <= size;
        i++) {
      if (isHardEnough4TsFileSlicing()) {
        break;
      }

      try {
        Thread.sleep(PipeConfig.getInstance().getPipeMemoryAllocateRetryIntervalInMs());
      } catch (InterruptedException ex) {
        Thread.currentThread().interrupt();
        LOGGER.warn("forceAllocateWithRetry: interrupted while waiting for available memory", ex);
      }
    }

    if (!isHardEnough4TsFileSlicing()) {
      throw new PipeRuntimeOutOfMemoryCriticalException(
          String.format(
              "forceAllocateForTsFile: failed to allocate because there's too much memory for tsfiles, "
                  + "total memory size %d bytes, used memory for tsfile size %d bytes, requested memory size %d bytes",
              getTotalNonFloatingMemorySizeInBytes(),
              usedMemorySizeInBytesOfTsFiles,
              tsFileSizeInBytes));
    }

    synchronized (this) {
      final PipeTsFileMemoryBlock block =
          (PipeTsFileMemoryBlock)
              forceAllocateWithRetry(tsFileSizeInBytes, PipeMemoryBlockType.TS_FILE);
      usedMemorySizeInBytesOfTsFiles += block.getMemoryUsageInBytes();
      return block;
    }
  }

  public PipeModelFixedMemoryBlock forceAllocateForModelFixedMemoryBlock(
      long fixedSizeInBytes, PipeMemoryBlockType type)
      throws PipeRuntimeOutOfMemoryCriticalException {
    if (!PIPE_MEMORY_MANAGEMENT_ENABLED) {
      return new PipeModelFixedMemoryBlock(Long.MAX_VALUE, new ThresholdAllocationStrategy());
    }

    if (fixedSizeInBytes == 0) {
      return (PipeModelFixedMemoryBlock) registerMemoryBlock(0, type);
    }

    for (int i = 1, size = PipeConfig.getInstance().getPipeMemoryAllocateMaxRetries();
        i <= size;
        i++) {
      if (getFreeMemorySizeInBytes() >= fixedSizeInBytes) {
        break;
      }

      try {
        Thread.sleep(PipeConfig.getInstance().getPipeMemoryAllocateRetryIntervalInMs());
      } catch (InterruptedException ex) {
        Thread.currentThread().interrupt();
        LOGGER.warn("forceAllocateWithRetry: interrupted while waiting for available memory", ex);
      }
    }

    synchronized (this) {
      if (getFreeMemorySizeInBytes() < fixedSizeInBytes) {
        return (PipeModelFixedMemoryBlock) forceAllocateWithRetry(getFreeMemorySizeInBytes(), type);
      }

      return (PipeModelFixedMemoryBlock) forceAllocateWithRetry(fixedSizeInBytes, type);
    }
  }

  private PipeMemoryBlock forceAllocateWithRetry(long sizeInBytes, PipeMemoryBlockType type)
      throws PipeRuntimeOutOfMemoryCriticalException {
    if (!PIPE_MEMORY_MANAGEMENT_ENABLED) {
      switch (type) {
        case TABLET:
          return new PipeTabletMemoryBlock(sizeInBytes);
        case TS_FILE:
          return new PipeTsFileMemoryBlock(sizeInBytes);
        case BATCH:
        case WAL:
          return new PipeModelFixedMemoryBlock(sizeInBytes, new ThresholdAllocationStrategy());
        default:
          return new PipeMemoryBlock(sizeInBytes);
      }
    }

    final int memoryAllocateMaxRetries = PipeConfig.getInstance().getPipeMemoryAllocateMaxRetries();
    for (int i = 1; i <= memoryAllocateMaxRetries; i++) {
      if (getTotalNonFloatingMemorySizeInBytes() - usedMemorySizeInBytes >= sizeInBytes) {
        return registerMemoryBlock(sizeInBytes, type);
      }

      try {
        tryShrinkUntilFreeMemorySatisfy(sizeInBytes);
        this.wait(PipeConfig.getInstance().getPipeMemoryAllocateRetryIntervalInMs());
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        LOGGER.warn("forceAllocate: interrupted while waiting for available memory", e);
      }
    }

    throw new PipeRuntimeOutOfMemoryCriticalException(
        String.format(
            "forceAllocate: failed to allocate memory after %d retries, "
                + "total memory size %d bytes, used memory size %d bytes, "
                + "requested memory size %d bytes",
            memoryAllocateMaxRetries,
            getTotalNonFloatingMemorySizeInBytes(),
            usedMemorySizeInBytes,
            sizeInBytes));
  }

  public void forceResize(final PipeMemoryBlock block, final long targetSize) {
    resize(block, targetSize, true);
  }

  public void forceResizeWithReservedMemory(
      final PipeMemoryBlock block, final long targetSize, final long reservedMemoryInBytes) {
    resize(block, targetSize, true, Math.max(0, reservedMemoryInBytes));
  }

  public void resize(final PipeMemoryBlock block, final long targetSize, final boolean force) {
    resize(block, targetSize, force, 0);
  }

  private synchronized void resize(
      final PipeMemoryBlock block,
      final long targetSize,
      final boolean force,
      final long reservedMemoryInBytes) {
    if (block == null || block.isReleased()) {
      LOGGER.warn("forceResize: cannot resize a null or released memory block");
      return;
    }

    if (!PIPE_MEMORY_MANAGEMENT_ENABLED) {
      block.setMemoryUsageInBytes(targetSize);
      return;
    }

    final long oldSize = block.getMemoryUsageInBytes();
    if (oldSize >= targetSize) {
      usedMemorySizeInBytes -= oldSize - targetSize;
      if (block instanceof PipeTabletMemoryBlock) {
        usedMemorySizeInBytesOfTablets -= oldSize - targetSize;
      }
      if (block instanceof PipeTsFileMemoryBlock) {
        usedMemorySizeInBytesOfTsFiles -= oldSize - targetSize;
      }
      block.setMemoryUsageInBytes(targetSize);

      // If no memory is used in the block, we can remove it from the allocated blocks.
      if (targetSize == 0) {
        allocatedBlocks.remove(block);
      }

      notifyNextTsFileParserMemoryReservationInternal();
      this.notifyAll();
      return;
    }

    final long sizeInBytes = targetSize - oldSize;
    final long requiredFreeMemoryInBytes =
        sizeInBytes > Long.MAX_VALUE - reservedMemoryInBytes
            ? Long.MAX_VALUE
            : sizeInBytes + reservedMemoryInBytes;
    final int memoryAllocateMaxRetries = PipeConfig.getInstance().getPipeMemoryAllocateMaxRetries();
    for (int i = 1; i <= memoryAllocateMaxRetries; i++) {
      // Dynamically resized data-structure blocks must obey the same admission thresholds as
      // blocks allocated with a non-zero initial size. Otherwise they can exhaust the pool and
      // prevent downstream consumers from allocating the memory needed to release them.
      if (isHardEnoughForResizing(block, sizeInBytes)
          && getTotalNonFloatingMemorySizeInBytes() - usedMemorySizeInBytes
              >= requiredFreeMemoryInBytes) {
        usedMemorySizeInBytes += sizeInBytes;
        if (oldSize == 0) {
          // If the memory block is not registered, we need to register it first.
          // Otherwise, the memory usage will be inconsistent.
          // See registerMemoryBlock for more details.
          allocatedBlocks.add(block);
        }
        if (block instanceof PipeTabletMemoryBlock) {
          usedMemorySizeInBytesOfTablets += sizeInBytes;
        }
        if (block instanceof PipeTsFileMemoryBlock) {
          usedMemorySizeInBytesOfTsFiles += sizeInBytes;
        }
        block.setMemoryUsageInBytes(targetSize);
        return;
      }

      try {
        tryShrinkUntilFreeMemorySatisfy(requiredFreeMemoryInBytes);
        this.wait(PipeConfig.getInstance().getPipeMemoryAllocateRetryIntervalInMs());
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        LOGGER.warn("forceResize: interrupted while waiting for available memory", e);
      }
    }

    if (force) {
      throw new PipeRuntimeOutOfMemoryCriticalException(
          String.format(
              "forceResize: failed to allocate memory after %d retries, "
                  + "total memory size %d bytes, used memory size %d bytes, "
                  + "requested memory size %d bytes, reserved memory size %d bytes",
              memoryAllocateMaxRetries,
              getTotalNonFloatingMemorySizeInBytes(),
              usedMemorySizeInBytes,
              sizeInBytes,
              reservedMemoryInBytes));
    }
  }

  /**
   * Allocate a {@link PipeMemoryBlock} for pipe only if memory used after allocation is less than
   * the specified threshold.
   *
   * @param sizeInBytes size of memory needed to allocate
   * @param usedThreshold proportion of memory used, ranged from 0.0 to 1.0
   * @return {@code null} if the proportion of memory used after allocation exceeds {@code
   *     usedThreshold}. Will return a memory block otherwise.
   */
  public synchronized PipeMemoryBlock forceAllocateIfSufficient(
      long sizeInBytes, float usedThreshold) {
    if (usedThreshold < 0.0f || usedThreshold > 1.0f) {
      return null;
    }

    if (!PIPE_MEMORY_MANAGEMENT_ENABLED) {
      return new PipeMemoryBlock(sizeInBytes);
    }

    if (sizeInBytes == 0) {
      return registerMemoryBlock(0);
    }

    if ((float) (usedMemorySizeInBytes + sizeInBytes)
        <= getTotalNonFloatingMemorySizeInBytes() * usedThreshold) {
      return forceAllocate(sizeInBytes);
    }

    return null;
  }

  public synchronized PipeMemoryBlock tryAllocate(long sizeInBytes) {
    return tryAllocate(sizeInBytes, currentSize -> currentSize * 2 / 3);
  }

  public synchronized PipeMemoryBlock tryAllocate(
      long sizeInBytes, LongUnaryOperator customAllocateStrategy) {
    if (!PIPE_MEMORY_MANAGEMENT_ENABLED) {
      return new PipeMemoryBlock(sizeInBytes);
    }

    if (sizeInBytes == 0
        || getTotalNonFloatingMemorySizeInBytes() - usedMemorySizeInBytes >= sizeInBytes) {
      return registerMemoryBlock(sizeInBytes);
    }

    long sizeToAllocateInBytes = sizeInBytes;
    while (sizeToAllocateInBytes > MEMORY_ALLOCATE_MIN_SIZE_IN_BYTES) {
      if (getTotalNonFloatingMemorySizeInBytes() - usedMemorySizeInBytes >= sizeToAllocateInBytes) {
        LOGGER.info(
            "tryAllocate: allocated memory, "
                + "total memory size {} bytes, used memory size {} bytes, "
                + "original requested memory size {} bytes, "
                + "actual requested memory size {} bytes",
            getTotalNonFloatingMemorySizeInBytes(),
            usedMemorySizeInBytes,
            sizeInBytes,
            sizeToAllocateInBytes);
        return registerMemoryBlock(sizeToAllocateInBytes);
      }

      sizeToAllocateInBytes =
          Math.max(
              customAllocateStrategy.applyAsLong(sizeToAllocateInBytes),
              MEMORY_ALLOCATE_MIN_SIZE_IN_BYTES);
    }

    if (tryShrinkUntilFreeMemorySatisfy(sizeToAllocateInBytes)) {
      LOGGER.info(
          "tryAllocate: allocated memory, "
              + "total memory size {} bytes, used memory size {} bytes, "
              + "original requested memory size {} bytes, "
              + "actual requested memory size {} bytes",
          getTotalNonFloatingMemorySizeInBytes(),
          usedMemorySizeInBytes,
          sizeInBytes,
          sizeToAllocateInBytes);
      return registerMemoryBlock(sizeToAllocateInBytes);
    } else {
      LOGGER.warn(
          "tryAllocate: failed to allocate memory, "
              + "total memory size {} bytes, used memory size {} bytes, "
              + "requested memory size {} bytes",
          getTotalNonFloatingMemorySizeInBytes(),
          usedMemorySizeInBytes,
          sizeInBytes);
      return registerMemoryBlock(0);
    }
  }

  public synchronized boolean tryAllocate(
      PipeMemoryBlock block, long memoryInBytesNeededToBeAllocated) {
    if (!PIPE_MEMORY_MANAGEMENT_ENABLED || block == null || block.isReleased()) {
      return false;
    }

    if (getTotalNonFloatingMemorySizeInBytes() - usedMemorySizeInBytes
        >= memoryInBytesNeededToBeAllocated) {
      usedMemorySizeInBytes += memoryInBytesNeededToBeAllocated;
      if (block.getMemoryUsageInBytes() == 0) {
        allocatedBlocks.add(block);
      }
      if (block instanceof PipeTabletMemoryBlock) {
        usedMemorySizeInBytesOfTablets += memoryInBytesNeededToBeAllocated;
      }
      if (block instanceof PipeTsFileMemoryBlock) {
        usedMemorySizeInBytesOfTsFiles += memoryInBytesNeededToBeAllocated;
      }
      block.setMemoryUsageInBytes(block.getMemoryUsageInBytes() + memoryInBytesNeededToBeAllocated);
      return true;
    }

    return false;
  }

  private PipeMemoryBlock registerMemoryBlock(long sizeInBytes) {
    return registerMemoryBlock(sizeInBytes, PipeMemoryBlockType.NORMAL);
  }

  private PipeMemoryBlock registerMemoryBlock(long sizeInBytes, PipeMemoryBlockType type) {
    final PipeMemoryBlock returnedMemoryBlock;
    switch (type) {
      case TABLET:
        returnedMemoryBlock = new PipeTabletMemoryBlock(sizeInBytes);
        break;
      case TS_FILE:
        returnedMemoryBlock = new PipeTsFileMemoryBlock(sizeInBytes);
        break;
      case BATCH:
      case WAL:
        returnedMemoryBlock =
            new PipeModelFixedMemoryBlock(sizeInBytes, new ThresholdAllocationStrategy());
        break;
      default:
        returnedMemoryBlock = new PipeMemoryBlock(sizeInBytes);
        break;
    }

    // For memory block whose size is 0, we do not need to add it to the allocated blocks now.
    // It's good for performance and will not trigger concurrent issues.
    // If forceResize is called on it, we will add it to the allocated blocks.
    if (sizeInBytes > 0) {
      usedMemorySizeInBytes += sizeInBytes;
      allocatedBlocks.add(returnedMemoryBlock);
    }

    return returnedMemoryBlock;
  }

  // Single-threaded logic
  private boolean tryShrinkUntilFreeMemorySatisfy(long sizeInBytes) {
    final List<PipeMemoryBlock> shuffledBlocks = new ArrayList<>(shrinkableBlocks);
    Collections.shuffle(shuffledBlocks);

    while (true) {
      boolean hasAtLeastOneBlockShrinkable = false;
      for (final PipeMemoryBlock block : shuffledBlocks) {
        if (block.shrink()) {
          hasAtLeastOneBlockShrinkable = true;
          if (getTotalNonFloatingMemorySizeInBytes() - usedMemorySizeInBytes >= sizeInBytes) {
            return true;
          }
        }
      }
      if (!hasAtLeastOneBlockShrinkable) {
        return false;
      }
    }
  }

  void addShrinkableBlock(final PipeMemoryBlock block) {
    shrinkableBlocks.add(block);
  }

  void removeShrinkableBlock(final PipeMemoryBlock block) {
    shrinkableBlocks.remove(block);
  }

  public synchronized void tryExpandAllAndCheckConsistency() {
    expandableBlocks.forEach(PipeMemoryBlock::expand);

    if (LOGGER.isDebugEnabled()) {
      final long blockSum =
          allocatedBlocks.stream().mapToLong(PipeMemoryBlock::getMemoryUsageInBytes).sum();
      if (blockSum != usedMemorySizeInBytes) {
        LOGGER.debug(
            "tryExpandAllAndCheckConsistency: memory usage is not consistent with allocated blocks,"
                + " usedMemorySizeInBytes is {} but sum of all blocks is {}",
            usedMemorySizeInBytes,
            blockSum);
      }

      final long tabletBlockSum =
          allocatedBlocks.stream()
              .filter(PipeTabletMemoryBlock.class::isInstance)
              .mapToLong(PipeMemoryBlock::getMemoryUsageInBytes)
              .sum();
      if (tabletBlockSum != usedMemorySizeInBytesOfTablets) {
        LOGGER.debug(
            "tryExpandAllAndCheckConsistency: memory usage of tablets is not consistent with allocated blocks,"
                + " usedMemorySizeInBytesOfTablets is {} but sum of all tablet blocks is {}",
            usedMemorySizeInBytesOfTablets,
            tabletBlockSum);
      }

      final long tsFileBlockSum =
          allocatedBlocks.stream()
              .filter(PipeTsFileMemoryBlock.class::isInstance)
              .mapToLong(PipeMemoryBlock::getMemoryUsageInBytes)
              .sum();
      if (tsFileBlockSum != usedMemorySizeInBytesOfTsFiles) {
        LOGGER.debug(
            "tryExpandAllAndCheckConsistency: memory usage of tsfiles is not consistent with allocated blocks,"
                + " usedMemorySizeInBytesOfTsFiles is {} but sum of all tsfile blocks is {}",
            usedMemorySizeInBytesOfTsFiles,
            tsFileBlockSum);
      }
    }
  }

  void addExpandableBlock(final PipeMemoryBlock block) {
    expandableBlocks.add(block);
  }

  void removeExpandableBlock(final PipeMemoryBlock block) {
    expandableBlocks.remove(block);
  }

  public synchronized void release(PipeMemoryBlock block) {
    if (!PIPE_MEMORY_MANAGEMENT_ENABLED || block == null || block.isReleased()) {
      return;
    }

    allocatedBlocks.remove(block);
    usedMemorySizeInBytes -= block.getMemoryUsageInBytes();
    if (block instanceof PipeTabletMemoryBlock) {
      usedMemorySizeInBytesOfTablets -= block.getMemoryUsageInBytes();
    }
    if (block instanceof PipeTsFileMemoryBlock) {
      usedMemorySizeInBytesOfTsFiles -= block.getMemoryUsageInBytes();
    }
    block.markAsReleased();

    notifyNextTsFileParserMemoryReservationInternal();
    this.notifyAll();
  }

  public synchronized boolean release(PipeMemoryBlock block, long sizeInBytes) {
    if (!PIPE_MEMORY_MANAGEMENT_ENABLED || block == null || block.isReleased()) {
      return false;
    }

    usedMemorySizeInBytes -= sizeInBytes;
    if (block instanceof PipeTabletMemoryBlock) {
      usedMemorySizeInBytesOfTablets -= sizeInBytes;
    }
    if (block instanceof PipeTsFileMemoryBlock) {
      usedMemorySizeInBytesOfTsFiles -= sizeInBytes;
    }
    block.setMemoryUsageInBytes(block.getMemoryUsageInBytes() - sizeInBytes);

    notifyNextTsFileParserMemoryReservationInternal();
    this.notifyAll();

    return true;
  }

  public long getUsedMemorySizeInBytes() {
    return usedMemorySizeInBytes;
  }

  public long getUsedMemorySizeInBytesOfTablets() {
    return usedMemorySizeInBytesOfTablets;
  }

  public long getUsedMemorySizeInBytesOfTsFiles() {
    return usedMemorySizeInBytesOfTsFiles;
  }

  public long getFreeMemorySizeInBytes() {
    return TOTAL_MEMORY_SIZE_IN_BYTES - usedMemorySizeInBytes;
  }

  public static long getTotalNonFloatingMemorySizeInBytes() {
    return (long)
        (TOTAL_MEMORY_SIZE_IN_BYTES
            * (1 - PipeConfig.getInstance().getPipeTotalFloatingMemoryProportion()));
  }

  public static long getTotalFloatingMemorySizeInBytes() {
    return (long)
        (TOTAL_MEMORY_SIZE_IN_BYTES
            * PipeConfig.getInstance().getPipeTotalFloatingMemoryProportion());
  }

  public static long getTotalMemorySizeInBytes() {
    return TOTAL_MEMORY_SIZE_IN_BYTES;
  }

  private static class PipeIdentity {

    private final String pipeName;
    private final long creationTime;

    private PipeIdentity(final String pipeName, final long creationTime) {
      this.pipeName = pipeName;
      this.creationTime = creationTime;
    }

    @Override
    public boolean equals(final Object object) {
      if (this == object) {
        return true;
      }
      if (!(object instanceof PipeIdentity)) {
        return false;
      }
      final PipeIdentity that = (PipeIdentity) object;
      return creationTime == that.creationTime && Objects.equals(pipeName, that.pipeName);
    }

    @Override
    public int hashCode() {
      return Objects.hash(pipeName, creationTime);
    }
  }

  private static class PipeRegionIdentity {

    private final PipeIdentity pipeIdentity;
    private final String dataRegionId;

    private PipeRegionIdentity(final PipeIdentity pipeIdentity, final String dataRegionId) {
      this.pipeIdentity = pipeIdentity;
      this.dataRegionId = dataRegionId;
    }

    @Override
    public boolean equals(final Object object) {
      if (this == object) {
        return true;
      }
      if (!(object instanceof PipeRegionIdentity)) {
        return false;
      }
      final PipeRegionIdentity that = (PipeRegionIdentity) object;
      return Objects.equals(pipeIdentity, that.pipeIdentity)
          && Objects.equals(dataRegionId, that.dataRegionId);
    }

    @Override
    public int hashCode() {
      return Objects.hash(pipeIdentity, dataRegionId);
    }
  }
}
