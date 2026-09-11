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
import org.apache.iotdb.commons.memory.IMemoryBlock;
import org.apache.iotdb.commons.memory.MemoryBlockType;
import org.apache.iotdb.commons.pipe.config.PipeConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.i18n.DataNodePipeMessages;
import org.apache.iotdb.db.pipe.agent.PipeDataNodeAgent;
import org.apache.iotdb.db.pipe.resource.memory.strategy.ThresholdAllocationStrategy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.WeakHashMap;
import java.util.function.LongSupplier;
import java.util.function.LongUnaryOperator;

public class PipeMemoryManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(PipeMemoryManager.class);

  public static final String FLOATING_MEMORY_BLOCK_NAME = "FloatingMemory";

  private static final PipeConfig PIPE_CONFIG = PipeConfig.getInstance();

  private static final boolean PIPE_MEMORY_MANAGEMENT_ENABLED =
      PipeConfig.getInstance().getPipeMemoryManagementEnabled();

  // TODO @spricoder: consider combine memory block and used MemorySizeInBytes
  private final IMemoryBlock memoryBlock;

  private final LongSupplier floatingMemoryUsageSupplier;

  private final long floatingMemoryAllocationTime = System.currentTimeMillis();
  private volatile long floatingMemoryMaxUsageInBytes;

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

  // Keep zero-sized diagnostic blocks observable while their owner is alive without extending the
  // owner's lifetime. Some parser APIs create temporary zero-sized event blocks that are normally
  // reclaimed by GC rather than explicitly closed.
  private final Set<PipeMemoryBlock> memoryBlocks = Collections.newSetFromMap(new WeakHashMap<>());

  // Keep blocks with a non-zero direct allocation strongly reachable until the allocation is
  // released. This preserves accounting and parent cascade semantics even though the diagnostic
  // registry above is weak.
  private final Set<PipeMemoryBlock> allocatedBlocks = new HashSet<>();
  private final Set<PipeMemoryBlock> shrinkableBlocks = new HashSet<>();
  private final Set<PipeMemoryBlock> expandableBlocks = new HashSet<>();

  public PipeMemoryManager() {
    this(
        IoTDBDescriptor.getInstance()
            .getMemoryConfig()
            .getPipeMemoryManager()
            .exactAllocate("Stream", MemoryBlockType.DYNAMIC),
        () -> PipeDataNodeAgent.task().getAllFloatingMemoryUsageInByte());
    PipeDataNodeAgent.runtime()
        .registerPeriodicalJob(
            "PipeMemoryManager#tryExpandAll()",
            this::tryExpandAllAndCheckConsistency,
            PipeConfig.getInstance().getPipeMemoryExpanderIntervalSeconds());
  }

  PipeMemoryManager(final IMemoryBlock memoryBlock) {
    this(memoryBlock, () -> PipeDataNodeAgent.task().getAllFloatingMemoryUsageInByte());
  }

  PipeMemoryManager(
      final IMemoryBlock memoryBlock, final LongSupplier floatingMemoryUsageSupplier) {
    this.memoryBlock = memoryBlock;
    this.floatingMemoryUsageSupplier = floatingMemoryUsageSupplier;
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

  private double allowedMaxMemorySizeInBytesOfTabletsAndTsFiles() {
    return (PIPE_CONFIG.getPipeDataStructureTabletMemoryBlockAllocationRejectThreshold()
            + PIPE_CONFIG.getPipeDataStructureTsFileMemoryBlockAllocationRejectThreshold())
        * getTotalNonFloatingMemorySizeInBytes();
  }

  private double allowedMaxMemorySizeInBytesOfTablets() {
    return (PIPE_CONFIG.getPipeDataStructureTabletMemoryBlockAllocationRejectThreshold()
            + PIPE_CONFIG.getPipeDataStructureTsFileMemoryBlockAllocationRejectThreshold() / 2)
        * getTotalNonFloatingMemorySizeInBytes();
  }

  private double allowedMaxMemorySizeInBytesOfTsTiles() {
    return (PIPE_CONFIG.getPipeDataStructureTsFileMemoryBlockAllocationRejectThreshold()
            + PIPE_CONFIG.getPipeDataStructureTabletMemoryBlockAllocationRejectThreshold() / 2)
        * getTotalNonFloatingMemorySizeInBytes();
  }

  private static long getTsFileParserMemorySizeInBytes() {
    return Math.max(
        PIPE_CONFIG.getTsFileParserMemory(), PIPE_CONFIG.getPipeMemoryAllocateMinSizeInBytes());
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
    final int perPipeRegionLimit =
        Math.max(
            1, Math.min(globalLimit, PIPE_CONFIG.getPipeTsFileParserInFlightMaxNumPerPipeRegion()));
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
          DataNodePipeMessages
              .LOG_FAILED_TO_RELEASE_TSFILE_PARSER_MEMORY_FOR_PIPE_ARG_CREATION_TIME_ARG_IN_DATAREGION_ARG_BECAUSE_NO_RESERVATION_EXISTS_BB8321C0,
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

    final int perPipeRegionLimit =
        Math.max(
            1, Math.min(globalLimit, PIPE_CONFIG.getPipeTsFileParserInFlightMaxNumPerPipeRegion()));
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
    final long retryIntervalInMs = PIPE_CONFIG.getPipeMemoryAllocateRetryIntervalInMs();
    final long minRetryTimeInMs = Math.max(retryIntervalInMs * 2, 1);
    final long maxRetryTimeInMs =
        Math.max(
            minRetryTimeInMs, retryIntervalInMs * PIPE_CONFIG.getPipeMemoryAllocateMaxRetries());

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

    return retryCount >= PIPE_CONFIG.getPipeMemoryAllocateMaxRetries()
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

  public synchronized PipeMemoryBlock forceAllocate(final String name, final long sizeInBytes)
      throws PipeRuntimeOutOfMemoryCriticalException {
    return forceAllocate(name, sizeInBytes, PipeMemoryBlockCategory.OTHER, null, null);
  }

  /**
   * Backward-compatible allocation entry point for callers that do not provide a diagnostic name.
   * Such blocks intentionally fall back to the generic category and a null assigner.
   */
  @Deprecated
  public synchronized PipeMemoryBlock forceAllocate(final long sizeInBytes)
      throws PipeRuntimeOutOfMemoryCriticalException {
    return forceAllocate(PipeMemoryBlock.class.getSimpleName(), sizeInBytes);
  }

  /**
   * Allocate a named block with explicit diagnostic metadata. The maximum size recorded for the
   * block is its lifetime high-water mark; it is not a new hard limit.
   */
  public synchronized PipeMemoryBlock forceAllocate(
      final String name,
      final long sizeInBytes,
      final PipeMemoryBlockCategory category,
      final Object assigner,
      final PipeMemoryBlock parent)
      throws PipeRuntimeOutOfMemoryCriticalException {
    if (!PIPE_MEMORY_MANAGEMENT_ENABLED) {
      // No need to calculate the tablet size, skip it to save time
      return registerMemoryBlock(name, 0, PipeMemoryBlockType.NORMAL, category, assigner, parent);
    }

    if (sizeInBytes == 0) {
      return registerMemoryBlock(name, 0, PipeMemoryBlockType.NORMAL, category, assigner, parent);
    }

    return forceAllocateWithRetry(
        name, sizeInBytes, PipeMemoryBlockType.NORMAL, category, assigner, parent);
  }

  /** Convenience overload for callers that only need a category and an assigner. */
  public synchronized PipeMemoryBlock forceAllocate(
      final String name,
      final long sizeInBytes,
      final PipeMemoryBlockCategory category,
      final Object assigner)
      throws PipeRuntimeOutOfMemoryCriticalException {
    return forceAllocate(name, sizeInBytes, category, assigner, null);
  }

  /** Allocate a child block using the supplied event block as its accounting parent. */
  public synchronized PipeMemoryBlock forceAllocate(
      final String name,
      final long sizeInBytes,
      final PipeMemoryBlock parent,
      final Object assigner)
      throws PipeRuntimeOutOfMemoryCriticalException {
    return forceAllocate(
        name,
        sizeInBytes,
        parent == null ? PipeMemoryBlockCategory.OTHER : PipeMemoryBlockCategory.EVENT_CHILD,
        assigner,
        parent);
  }

  public synchronized PipeMemoryBlock forceAllocate(
      final PipeMemoryBlock parent, final String name, final long sizeInBytes)
      throws PipeRuntimeOutOfMemoryCriticalException {
    return forceAllocate(name, sizeInBytes, PipeMemoryBlockCategory.EVENT_CHILD, null, parent);
  }

  /** Allocate a child block and charge its bytes through the parent to the global pool. */
  public synchronized PipeMemoryBlock forceAllocateChild(
      final PipeMemoryBlock parent, final String name, final long sizeInBytes)
      throws PipeRuntimeOutOfMemoryCriticalException {
    return forceAllocate(parent, name, sizeInBytes);
  }

  public PipeTabletMemoryBlock forceAllocateForTabletWithRetry(
      final String name, final long tabletSizeInBytes)
      throws PipeRuntimeOutOfMemoryCriticalException {
    return forceAllocateForTabletWithRetry(
        name, tabletSizeInBytes, PipeMemoryBlockCategory.TABLET, null, null);
  }

  /** Backward-compatible tablet allocation entry point without diagnostic metadata. */
  @Deprecated
  public PipeTabletMemoryBlock forceAllocateForTabletWithRetry(final long tabletSizeInBytes)
      throws PipeRuntimeOutOfMemoryCriticalException {
    return forceAllocateForTabletWithRetry(
        PipeTabletMemoryBlock.class.getSimpleName(), tabletSizeInBytes);
  }

  public PipeTabletMemoryBlock forceAllocateForTabletWithRetry(
      final String name,
      final long tabletSizeInBytes,
      final PipeMemoryBlockCategory category,
      final Object assigner,
      final PipeMemoryBlock parent)
      throws PipeRuntimeOutOfMemoryCriticalException {
    if (!PIPE_MEMORY_MANAGEMENT_ENABLED) {
      // No need to calculate the tablet size, skip it to save time
      return (PipeTabletMemoryBlock)
          registerMemoryBlock(name, 0, PipeMemoryBlockType.TABLET, category, assigner, parent);
    }

    if (tabletSizeInBytes == 0) {
      return (PipeTabletMemoryBlock)
          registerMemoryBlock(name, 0, PipeMemoryBlockType.TABLET, category, assigner, parent);
    }

    for (int i = 1, size = PIPE_CONFIG.getPipeMemoryAllocateMaxRetries(); i <= size; i++) {
      if (isHardEnough4TabletParsing()) {
        break;
      }

      try {
        Thread.sleep(PIPE_CONFIG.getPipeMemoryAllocateRetryIntervalInMs());
      } catch (InterruptedException ex) {
        Thread.currentThread().interrupt();
        LOGGER.warn(
            DataNodePipeMessages
                .FORCEALLOCATEWITHRETRY_INTERRUPTED_WHILE_WAITING_FOR_AVAILABLE_MEMORY,
            ex);
      }
    }

    if (!isHardEnough4TabletParsing()) {
      throw new PipeRuntimeOutOfMemoryCriticalException(
          String.format(
              DataNodePipeMessages
                  .PIPE_EXCEPTION_FORCEALLOCATEFORTABLET_FAILED_TO_ALLOCATE_BECAUSE_THERE_F878474D,
              getTotalNonFloatingMemorySizeInBytes(),
              usedMemorySizeInBytesOfTablets,
              tabletSizeInBytes));
    }

    synchronized (this) {
      final PipeTabletMemoryBlock block =
          (PipeTabletMemoryBlock)
              forceAllocateWithRetry(
                  name, tabletSizeInBytes, PipeMemoryBlockType.TABLET, category, assigner, parent);
      return block;
    }
  }

  /** Convenience overload for a root tablet block with explicit diagnostic metadata. */
  public PipeTabletMemoryBlock forceAllocateForTabletWithRetry(
      final String name,
      final long tabletSizeInBytes,
      final PipeMemoryBlockCategory category,
      final Object assigner)
      throws PipeRuntimeOutOfMemoryCriticalException {
    return forceAllocateForTabletWithRetry(name, tabletSizeInBytes, category, assigner, null);
  }

  /** Allocate a tablet/parser child block using the supplied event block as its parent. */
  public PipeTabletMemoryBlock forceAllocateForTabletWithRetry(
      final String name,
      final long tabletSizeInBytes,
      final PipeMemoryBlock parent,
      final Object assigner)
      throws PipeRuntimeOutOfMemoryCriticalException {
    return forceAllocateForTabletWithRetry(
        name,
        tabletSizeInBytes,
        parent == null ? PipeMemoryBlockCategory.TABLET : PipeMemoryBlockCategory.EVENT_CHILD,
        assigner,
        parent);
  }

  public PipeTabletMemoryBlock forceAllocateForTabletWithRetry(
      final PipeMemoryBlock parent, final String name, final long tabletSizeInBytes)
      throws PipeRuntimeOutOfMemoryCriticalException {
    return forceAllocateForTabletWithRetry(
        name, tabletSizeInBytes, PipeMemoryBlockCategory.EVENT_CHILD, null, parent);
  }

  public PipeTsFileMemoryBlock forceAllocateForTsFileWithRetry(
      final String name, final long tsFileSizeInBytes)
      throws PipeRuntimeOutOfMemoryCriticalException {
    return forceAllocateForTsFileWithRetry(
        name, tsFileSizeInBytes, PipeMemoryBlockCategory.TS_FILE, null, null);
  }

  /** Backward-compatible TsFile allocation entry point without diagnostic metadata. */
  @Deprecated
  public PipeTsFileMemoryBlock forceAllocateForTsFileWithRetry(final long tsFileSizeInBytes)
      throws PipeRuntimeOutOfMemoryCriticalException {
    return forceAllocateForTsFileWithRetry(
        PipeTsFileMemoryBlock.class.getSimpleName(), tsFileSizeInBytes);
  }

  public PipeTsFileMemoryBlock forceAllocateForTsFileWithRetry(
      final String name,
      final long tsFileSizeInBytes,
      final PipeMemoryBlockCategory category,
      final Object assigner,
      final PipeMemoryBlock parent)
      throws PipeRuntimeOutOfMemoryCriticalException {
    if (!PIPE_MEMORY_MANAGEMENT_ENABLED) {
      return (PipeTsFileMemoryBlock)
          registerMemoryBlock(name, 0, PipeMemoryBlockType.TS_FILE, category, assigner, parent);
    }

    if (tsFileSizeInBytes == 0) {
      return (PipeTsFileMemoryBlock)
          registerMemoryBlock(name, 0, PipeMemoryBlockType.TS_FILE, category, assigner, parent);
    }

    for (int i = 1, size = PIPE_CONFIG.getPipeMemoryAllocateMaxRetries(); i <= size; i++) {
      if (isHardEnough4TsFileSlicing()) {
        break;
      }

      try {
        Thread.sleep(PIPE_CONFIG.getPipeMemoryAllocateRetryIntervalInMs());
      } catch (InterruptedException ex) {
        Thread.currentThread().interrupt();
        LOGGER.warn(
            DataNodePipeMessages
                .FORCEALLOCATEWITHRETRY_INTERRUPTED_WHILE_WAITING_FOR_AVAILABLE_MEMORY,
            ex);
      }
    }

    if (!isHardEnough4TsFileSlicing()) {
      throw new PipeRuntimeOutOfMemoryCriticalException(
          String.format(
              DataNodePipeMessages
                  .PIPE_EXCEPTION_FORCEALLOCATEFORTSFILE_FAILED_TO_ALLOCATE_BECAUSE_THERE_6D614467,
              getTotalNonFloatingMemorySizeInBytes(),
              usedMemorySizeInBytesOfTsFiles,
              tsFileSizeInBytes));
    }

    synchronized (this) {
      final PipeTsFileMemoryBlock block =
          (PipeTsFileMemoryBlock)
              forceAllocateWithRetry(
                  name, tsFileSizeInBytes, PipeMemoryBlockType.TS_FILE, category, assigner, parent);
      return block;
    }
  }

  /** Convenience overload for a root TsFile block with explicit diagnostic metadata. */
  public PipeTsFileMemoryBlock forceAllocateForTsFileWithRetry(
      final String name,
      final long tsFileSizeInBytes,
      final PipeMemoryBlockCategory category,
      final Object assigner)
      throws PipeRuntimeOutOfMemoryCriticalException {
    return forceAllocateForTsFileWithRetry(name, tsFileSizeInBytes, category, assigner, null);
  }

  /** Allocate a TsFile/parser child block using the supplied event block as its parent. */
  public PipeTsFileMemoryBlock forceAllocateForTsFileWithRetry(
      final String name,
      final long tsFileSizeInBytes,
      final PipeMemoryBlock parent,
      final Object assigner)
      throws PipeRuntimeOutOfMemoryCriticalException {
    return forceAllocateForTsFileWithRetry(
        name,
        tsFileSizeInBytes,
        parent == null ? PipeMemoryBlockCategory.TS_FILE : PipeMemoryBlockCategory.EVENT_CHILD,
        assigner,
        parent);
  }

  public PipeTsFileMemoryBlock forceAllocateForTsFileWithRetry(
      final PipeMemoryBlock parent, final String name, final long tsFileSizeInBytes)
      throws PipeRuntimeOutOfMemoryCriticalException {
    return forceAllocateForTsFileWithRetry(
        name, tsFileSizeInBytes, PipeMemoryBlockCategory.EVENT_CHILD, null, parent);
  }

  public PipeModelFixedMemoryBlock forceAllocateForModelFixedMemoryBlock(
      final String name, final long fixedSizeInBytes, final PipeMemoryBlockType type)
      throws PipeRuntimeOutOfMemoryCriticalException {
    if (!PIPE_MEMORY_MANAGEMENT_ENABLED) {
      return (PipeModelFixedMemoryBlock) registerMemoryBlock(name, Long.MAX_VALUE, type);
    }

    if (fixedSizeInBytes == 0) {
      return (PipeModelFixedMemoryBlock) registerMemoryBlock(name, 0, type);
    }

    for (int i = 1, size = PIPE_CONFIG.getPipeMemoryAllocateMaxRetries(); i <= size; i++) {
      if (getFreeMemorySizeInBytes() >= fixedSizeInBytes) {
        break;
      }

      try {
        Thread.sleep(PIPE_CONFIG.getPipeMemoryAllocateRetryIntervalInMs());
      } catch (InterruptedException ex) {
        Thread.currentThread().interrupt();
        LOGGER.warn(
            DataNodePipeMessages
                .FORCEALLOCATEWITHRETRY_INTERRUPTED_WHILE_WAITING_FOR_AVAILABLE_MEMORY,
            ex);
      }
    }

    synchronized (this) {
      if (getFreeMemorySizeInBytes() < fixedSizeInBytes) {
        return (PipeModelFixedMemoryBlock)
            forceAllocateWithRetry(name, getFreeMemorySizeInBytes(), type);
      }

      return (PipeModelFixedMemoryBlock) forceAllocateWithRetry(name, fixedSizeInBytes, type);
    }
  }

  /** Backward-compatible fixed-block allocation entry point without a diagnostic name. */
  @Deprecated
  public PipeModelFixedMemoryBlock forceAllocateForModelFixedMemoryBlock(
      final long fixedSizeInBytes, final PipeMemoryBlockType type)
      throws PipeRuntimeOutOfMemoryCriticalException {
    return forceAllocateForModelFixedMemoryBlock(
        PipeModelFixedMemoryBlock.class.getSimpleName(), fixedSizeInBytes, type);
  }

  private PipeMemoryBlock forceAllocateWithRetry(
      final String name, final long sizeInBytes, final PipeMemoryBlockType type)
      throws PipeRuntimeOutOfMemoryCriticalException {
    return forceAllocateWithRetry(
        name, sizeInBytes, type, PipeMemoryBlockCategory.fromType(type), null, null);
  }

  private PipeMemoryBlock forceAllocateWithRetry(
      final String name,
      final long sizeInBytes,
      final PipeMemoryBlockType type,
      final PipeMemoryBlockCategory category,
      final Object assigner,
      final PipeMemoryBlock parent)
      throws PipeRuntimeOutOfMemoryCriticalException {
    if (!PIPE_MEMORY_MANAGEMENT_ENABLED) {
      return registerMemoryBlock(name, sizeInBytes, type, category, assigner, parent);
    }

    final int memoryAllocateMaxRetries = PIPE_CONFIG.getPipeMemoryAllocateMaxRetries();
    for (int i = 1; i <= memoryAllocateMaxRetries; i++) {
      if (getTotalNonFloatingMemorySizeInBytes() - memoryBlock.getUsedMemoryInBytes()
          >= sizeInBytes) {
        return registerMemoryBlock(name, sizeInBytes, type, category, assigner, parent);
      }

      try {
        tryShrinkUntilFreeMemorySatisfy(sizeInBytes);
        this.wait(PIPE_CONFIG.getPipeMemoryAllocateRetryIntervalInMs());
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        LOGGER.warn(
            DataNodePipeMessages.FORCEALLOCATE_INTERRUPTED_WHILE_WAITING_FOR_AVAILABLE_MEMORY, e);
      }
    }

    throw new PipeRuntimeOutOfMemoryCriticalException(
        String.format(
            DataNodePipeMessages
                .PIPE_EXCEPTION_FORCEALLOCATE_FAILED_TO_ALLOCATE_MEMORY_AFTER_D_RETRIES_44EF7AE7,
            memoryAllocateMaxRetries,
            getTotalNonFloatingMemorySizeInBytes(),
            memoryBlock.getUsedMemoryInBytes(),
            sizeInBytes));
  }

  public void forceResize(final PipeMemoryBlock block, final long targetSize) {
    resize(block, targetSize, true);
  }

  /**
   * Attempts a single resize without waiting for other pipe tasks to release memory.
   *
   * <p>This is intended for callers that hold payload/batch locks and can actively release memory
   * after a failed attempt. Waiting in that situation can prevent the caller itself from making
   * forward progress.
   */
  public synchronized boolean tryResize(final PipeMemoryBlock block, final long targetSize) {
    if (targetSize < 0) {
      return false;
    }
    if (block == null || block.isReleased()) {
      LOGGER.warn(DataNodePipeMessages.FORCERESIZE_CANNOT_RESIZE_A_NULL_OR_RELEASED);
      return false;
    }
    return tryResizeInternal(block, targetSize);
  }

  public synchronized void resize(
      final PipeMemoryBlock block, final long targetSize, final boolean force) {
    if (block == null || block.isReleased()) {
      LOGGER.warn(DataNodePipeMessages.FORCERESIZE_CANNOT_RESIZE_A_NULL_OR_RELEASED);
      return;
    }

    if (tryResizeInternal(block, targetSize)) {
      return;
    }

    final long sizeInBytes =
        Math.max(Math.max(0, targetSize), getChildMemoryUsageInBytes(block))
            - block.getMemoryUsageInBytes();
    final int memoryAllocateMaxRetries = PIPE_CONFIG.getPipeMemoryAllocateMaxRetries();
    for (int i = 1; i <= memoryAllocateMaxRetries; i++) {
      try {
        tryShrinkUntilFreeMemorySatisfy(sizeInBytes);
        if (tryResizeInternal(block, targetSize)) {
          return;
        }
        this.wait(PIPE_CONFIG.getPipeMemoryAllocateRetryIntervalInMs());
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        LOGGER.warn(
            DataNodePipeMessages.FORCERESIZE_INTERRUPTED_WHILE_WAITING_FOR_AVAILABLE_MEMORY, e);
      }
    }

    if (force) {
      throw new PipeRuntimeOutOfMemoryCriticalException(
          String.format(
              DataNodePipeMessages
                  .PIPE_EXCEPTION_FORCERESIZE_FAILED_TO_ALLOCATE_MEMORY_AFTER_D_RETRIES_TOTAL_8C6948BC,
              memoryAllocateMaxRetries,
              getTotalNonFloatingMemorySizeInBytes(),
              memoryBlock.getUsedMemoryInBytes(),
              sizeInBytes));
    }
  }

  private boolean tryResizeInternal(final PipeMemoryBlock block, final long targetSize) {
    // Parent blocks expose an aggregate usage. Do not let a direct resize reduce that aggregate
    // below the bytes still owned by live children.
    final long normalizedTargetSize =
        Math.max(Math.max(0, targetSize), getChildMemoryUsageInBytes(block));

    if (!PIPE_MEMORY_MANAGEMENT_ENABLED) {
      adjustMemoryUsageHierarchy(block, normalizedTargetSize - block.getMemoryUsageInBytes());
      return true;
    }

    final long oldSize = block.getMemoryUsageInBytes();
    if (oldSize >= normalizedTargetSize) {
      final long releasedSize = oldSize - normalizedTargetSize;
      if (releasedSize > 0) {
        releaseMemoryForBlock(block, releasedSize);
        notifyNextTsFileParserMemoryReservationInternal();
        this.notifyAll();
      }
      return true;
    }

    final long sizeInBytes = normalizedTargetSize - oldSize;
    // Dynamically resized data-structure blocks must obey the same admission thresholds as blocks
    // allocated with a non-zero initial size. Otherwise they can exhaust the pool and prevent
    // downstream consumers from allocating the memory needed to release them.
    if (!isHardEnoughForResizing(block, sizeInBytes)
        || getTotalNonFloatingMemorySizeInBytes() - memoryBlock.getUsedMemoryInBytes()
            < sizeInBytes) {
      return false;
    }

    memoryBlock.forceAllocateWithoutLimitation(sizeInBytes);
    adjustMemoryUsageHierarchy(block, sizeInBytes);
    return true;
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
      final String name, final long sizeInBytes, final float usedThreshold) {
    return forceAllocateIfSufficient(
        name, sizeInBytes, usedThreshold, PipeMemoryBlockCategory.OTHER, null, null);
  }

  /** Backward-compatible threshold allocation entry point without a diagnostic name. */
  @Deprecated
  public synchronized PipeMemoryBlock forceAllocateIfSufficient(
      final long sizeInBytes, final float usedThreshold) {
    return forceAllocateIfSufficient(
        PipeMemoryBlock.class.getSimpleName(), sizeInBytes, usedThreshold);
  }

  /**
   * Allocate a block subject to a usage threshold while retaining diagnostic metadata.
   *
   * <p>The threshold applies to the actual global pool charge. A parent, when supplied, receives
   * the same allocation as an aggregate value but is not charged a second time.
   */
  public synchronized PipeMemoryBlock forceAllocateIfSufficient(
      final String name,
      final long sizeInBytes,
      final float usedThreshold,
      final PipeMemoryBlockCategory category,
      final Object assigner,
      final PipeMemoryBlock parent) {
    if (usedThreshold < 0.0f || usedThreshold > 1.0f) {
      return null;
    }

    if (!PIPE_MEMORY_MANAGEMENT_ENABLED) {
      return registerMemoryBlock(
          name, sizeInBytes, PipeMemoryBlockType.NORMAL, category, assigner, parent);
    }

    if (sizeInBytes == 0) {
      return registerMemoryBlock(name, 0, PipeMemoryBlockType.NORMAL, category, assigner, parent);
    }

    if ((float) (memoryBlock.getUsedMemoryInBytes() + sizeInBytes)
        <= getTotalNonFloatingMemorySizeInBytes() * usedThreshold) {
      return forceAllocate(name, sizeInBytes, category, assigner, parent);
    }

    return null;
  }

  public synchronized PipeMemoryBlock tryAllocate(final String name, final long sizeInBytes) {
    return tryAllocate(
        name,
        sizeInBytes,
        currentSize -> currentSize * 2 / 3,
        PipeMemoryBlockCategory.OTHER,
        null,
        null);
  }

  /** Backward-compatible gradual allocation entry point without a diagnostic name. */
  @Deprecated
  public synchronized PipeMemoryBlock tryAllocate(final long sizeInBytes) {
    return tryAllocate(PipeMemoryBlock.class.getSimpleName(), sizeInBytes);
  }

  public synchronized PipeMemoryBlock tryAllocate(
      final String name, final long sizeInBytes, final LongUnaryOperator customAllocateStrategy) {
    return tryAllocate(
        name, sizeInBytes, customAllocateStrategy, PipeMemoryBlockCategory.OTHER, null, null);
  }

  /** Backward-compatible gradual allocation entry point without a diagnostic name. */
  @Deprecated
  public synchronized PipeMemoryBlock tryAllocate(
      final long sizeInBytes, final LongUnaryOperator customAllocateStrategy) {
    return tryAllocate(PipeMemoryBlock.class.getSimpleName(), sizeInBytes, customAllocateStrategy);
  }

  /** Convenience overload using the default gradual-allocation strategy. */
  public synchronized PipeMemoryBlock tryAllocate(
      final String name,
      final long sizeInBytes,
      final PipeMemoryBlockCategory category,
      final Object assigner) {
    return tryAllocate(
        name, sizeInBytes, currentSize -> currentSize * 2 / 3, category, assigner, null);
  }

  /** Try to allocate a block with explicit diagnostic metadata. */
  public synchronized PipeMemoryBlock tryAllocate(
      final String name,
      final long sizeInBytes,
      final LongUnaryOperator customAllocateStrategy,
      final PipeMemoryBlockCategory category,
      final Object assigner,
      final PipeMemoryBlock parent) {
    if (!PIPE_MEMORY_MANAGEMENT_ENABLED) {
      return registerMemoryBlock(
          name, sizeInBytes, PipeMemoryBlockType.NORMAL, category, assigner, parent);
    }

    if (sizeInBytes == 0
        || getTotalNonFloatingMemorySizeInBytes() - memoryBlock.getUsedMemoryInBytes()
            >= sizeInBytes) {
      return registerMemoryBlock(
          name, sizeInBytes, PipeMemoryBlockType.NORMAL, category, assigner, parent);
    }

    long sizeToAllocateInBytes = sizeInBytes;
    final long memoryAllocateMinSizeInBytes = PIPE_CONFIG.getPipeMemoryAllocateMinSizeInBytes();

    while (sizeToAllocateInBytes > memoryAllocateMinSizeInBytes) {
      if (getTotalNonFloatingMemorySizeInBytes() - memoryBlock.getUsedMemoryInBytes()
          >= sizeToAllocateInBytes) {
        LOGGER.info(
            DataNodePipeMessages.TRYALLOCATE_ALLOCATED_MEMORY_TOTAL_MEMORY_SIZE_BYTES,
            getTotalNonFloatingMemorySizeInBytes(),
            memoryBlock.getUsedMemoryInBytes(),
            sizeInBytes,
            sizeToAllocateInBytes);
        return registerMemoryBlock(
            name, sizeToAllocateInBytes, PipeMemoryBlockType.NORMAL, category, assigner, parent);
      }

      sizeToAllocateInBytes =
          Math.max(
              customAllocateStrategy.applyAsLong(sizeToAllocateInBytes),
              memoryAllocateMinSizeInBytes);
    }

    if (tryShrinkUntilFreeMemorySatisfy(sizeToAllocateInBytes)) {
      LOGGER.info(
          DataNodePipeMessages.TRYALLOCATE_ALLOCATED_MEMORY_TOTAL_MEMORY_SIZE_BYTES,
          getTotalNonFloatingMemorySizeInBytes(),
          memoryBlock.getUsedMemoryInBytes(),
          sizeInBytes,
          sizeToAllocateInBytes);
      return registerMemoryBlock(
          name, sizeToAllocateInBytes, PipeMemoryBlockType.NORMAL, category, assigner, parent);
    } else {
      LOGGER.warn(
          DataNodePipeMessages.TRYALLOCATE_FAILED_TO_ALLOCATE_MEMORY_TOTAL_MEMORY,
          getTotalNonFloatingMemorySizeInBytes(),
          memoryBlock.getUsedMemoryInBytes(),
          sizeInBytes);
      return registerMemoryBlock(name, 0, PipeMemoryBlockType.NORMAL, category, assigner, parent);
    }
  }

  public synchronized boolean tryAllocate(
      PipeMemoryBlock block, long memoryInBytesNeededToBeAllocated) {
    if (!PIPE_MEMORY_MANAGEMENT_ENABLED
        || block == null
        || block.isReleased()
        || memoryInBytesNeededToBeAllocated <= 0) {
      return false;
    }

    return reserveMemoryForBlock(block, memoryInBytesNeededToBeAllocated);
  }

  private PipeMemoryBlock registerMemoryBlock(final String name, final long sizeInBytes) {
    return registerMemoryBlock(name, sizeInBytes, PipeMemoryBlockType.NORMAL);
  }

  private synchronized PipeMemoryBlock registerMemoryBlock(
      final String name, final long sizeInBytes, final PipeMemoryBlockType type) {
    return registerMemoryBlock(
        name, sizeInBytes, type, PipeMemoryBlockCategory.fromType(type), null, null);
  }

  private synchronized PipeMemoryBlock registerMemoryBlock(
      final String name,
      final long sizeInBytes,
      final PipeMemoryBlockType type,
      final PipeMemoryBlockCategory category,
      final Object assigner,
      final PipeMemoryBlock parent) {
    // Never link blocks owned by another manager (or an already released parent). Such a link
    // would make release charge the wrong global pool and could leave an orphaned accounting
    // chain. Falling back to a root block keeps the allocation observable and safe.
    final PipeMemoryBlock normalizedParent =
        parent != null && parent.getPipeMemoryManager() == this && !parent.isReleased()
            ? parent
            : null;
    final PipeMemoryBlockCategory normalizedCategory =
        inferCategory(name, type, category, normalizedParent);
    final PipeMemoryBlock returnedMemoryBlock;
    switch (type) {
      case TABLET:
        returnedMemoryBlock =
            new PipeTabletMemoryBlock(
                this,
                name,
                0,
                normalizedCategory,
                PipeMemoryBlock.snapshotAssigner(assigner),
                normalizedParent);
        break;
      case TS_FILE:
        returnedMemoryBlock =
            new PipeTsFileMemoryBlock(
                this,
                name,
                0,
                normalizedCategory,
                PipeMemoryBlock.snapshotAssigner(assigner),
                normalizedParent);
        break;
      case BATCH:
      case WAL:
        returnedMemoryBlock =
            new PipeModelFixedMemoryBlock(
                this,
                name,
                0,
                new ThresholdAllocationStrategy(),
                normalizedCategory,
                PipeMemoryBlock.snapshotAssigner(assigner),
                normalizedParent);
        break;
      default:
        returnedMemoryBlock =
            new PipeMemoryBlock(
                this,
                name,
                0,
                normalizedCategory,
                PipeMemoryBlock.snapshotAssigner(assigner),
                normalizedParent);
        break;
    }

    memoryBlocks.add(returnedMemoryBlock);

    // Zero-sized blocks do not participate in memory accounting until they are resized. For a
    // child block, the same bytes are charged once through the root block while every ancestor is
    // updated for diagnostics.
    if (sizeInBytes > 0) {
      if (!reserveMemoryForBlock(returnedMemoryBlock, sizeInBytes)) {
        // Callers normally check availability before registering. Keep the block observable even
        // if a concurrent allocation wins the race; it starts at zero and can be resized later.
        returnedMemoryBlock.setMemoryUsageInBytes(0);
      }
    }

    return returnedMemoryBlock;
  }

  private static PipeMemoryBlockCategory inferCategory(
      final String name,
      final PipeMemoryBlockType type,
      final PipeMemoryBlockCategory requestedCategory,
      final PipeMemoryBlock parent) {
    if (parent != null) {
      return requestedCategory == null || requestedCategory == PipeMemoryBlockCategory.OTHER
          ? PipeMemoryBlockCategory.EVENT_CHILD
          : requestedCategory;
    }
    if (requestedCategory != null && requestedCategory != PipeMemoryBlockCategory.OTHER) {
      return requestedCategory;
    }
    final String normalizedName = name == null ? "" : name.toLowerCase(java.util.Locale.ROOT);
    if (normalizedName.contains("parser")) {
      return PipeMemoryBlockCategory.PARSER;
    }
    if (normalizedName.contains("receiver")) {
      return PipeMemoryBlockCategory.RECEIVER;
    }
    if (normalizedName.contains("sink")) {
      return PipeMemoryBlockCategory.SINK;
    }
    if (normalizedName.contains("cache") || normalizedName.contains("logger")) {
      return PipeMemoryBlockCategory.CACHE;
    }
    if (normalizedName.contains("subscription")) {
      return PipeMemoryBlockCategory.SUBSCRIPTION;
    }
    if (normalizedName.contains("event")) {
      return PipeMemoryBlockCategory.EVENT;
    }
    return PipeMemoryBlockCategory.fromType(type);
  }

  private boolean reserveMemoryForBlock(final PipeMemoryBlock block, final long sizeInBytes) {
    if (sizeInBytes <= 0 || block == null || block.isReleased()) {
      return sizeInBytes == 0 && block != null && !block.isReleased();
    }
    if (PIPE_MEMORY_MANAGEMENT_ENABLED
        && getTotalNonFloatingMemorySizeInBytes() - memoryBlock.getUsedMemoryInBytes()
            < sizeInBytes) {
      return false;
    }
    if (PIPE_MEMORY_MANAGEMENT_ENABLED) {
      memoryBlock.forceAllocateWithoutLimitation(sizeInBytes);
    }
    adjustMemoryUsageHierarchy(block, sizeInBytes);
    return true;
  }

  private void adjustMemoryUsageHierarchy(final PipeMemoryBlock block, final long delta) {
    PipeMemoryBlock current = block;
    while (current != null) {
      current.setMemoryUsageInBytes(safeAdd(current.getMemoryUsageInBytes(), delta));
      current = current.getParentBlock();
    }

    if (PIPE_MEMORY_MANAGEMENT_ENABLED && delta > 0) {
      allocatedBlocks.add(block);
    } else if (PIPE_MEMORY_MANAGEMENT_ENABLED
        && delta < 0
        && getDirectMemoryUsageInBytes(block) == 0) {
      allocatedBlocks.remove(block);
    }

    if (block instanceof PipeTabletMemoryBlock) {
      usedMemorySizeInBytesOfTablets = safeAdd(usedMemorySizeInBytesOfTablets, delta);
    }
    if (block instanceof PipeTsFileMemoryBlock) {
      usedMemorySizeInBytesOfTsFiles = safeAdd(usedMemorySizeInBytesOfTsFiles, delta);
    }
  }

  /** Saturating add keeps diagnostic counters valid even for the unbounded disabled-mode block. */
  private static long safeAdd(final long value, final long delta) {
    if (delta > 0 && value > Long.MAX_VALUE - delta) {
      return Long.MAX_VALUE;
    }
    if (delta < 0 && value < Long.MIN_VALUE - delta) {
      return Long.MIN_VALUE;
    }
    return value + delta;
  }

  private boolean releaseMemoryForBlock(
      final PipeMemoryBlock block, final long requestedSizeInBytes) {
    if (block == null || block.isReleased()) {
      return false;
    }
    // A parent row reports an aggregate usage. Only bytes owned directly by that row may be
    // released here; descendants are released through their own blocks (or by release(parent)'s
    // cascade). This prevents a parent resize from stealing a child's global reservation.
    final long directMemoryUsageInBytes = getDirectMemoryUsageInBytes(block);
    final long sizeInBytes = Math.min(Math.max(0, requestedSizeInBytes), directMemoryUsageInBytes);
    if (sizeInBytes <= 0) {
      return false;
    }
    if (PIPE_MEMORY_MANAGEMENT_ENABLED) {
      memoryBlock.release(sizeInBytes);
    }
    adjustMemoryUsageHierarchy(block, -sizeInBytes);
    return true;
  }

  private static long getDirectMemoryUsageInBytes(final PipeMemoryBlock block) {
    final long childUsageInBytes = getChildMemoryUsageInBytes(block);
    return Math.max(0, block.getMemoryUsageInBytes() - childUsageInBytes);
  }

  private static long getChildMemoryUsageInBytes(final PipeMemoryBlock block) {
    long childUsageInBytes = 0;
    for (final PipeMemoryBlock child : block.getChildrenSnapshot()) {
      childUsageInBytes = safeAdd(childUsageInBytes, child.getMemoryUsageInBytes());
    }
    return Math.max(0, childUsageInBytes);
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
          if (getTotalNonFloatingMemorySizeInBytes() - memoryBlock.getUsedMemoryInBytes()
              >= sizeInBytes) {
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
          allocatedBlocks.stream().mapToLong(PipeMemoryManager::getDirectMemoryUsageInBytes).sum();
      if (blockSum != memoryBlock.getUsedMemoryInBytes()) {
        LOGGER.debug(
            DataNodePipeMessages
                .TRYEXPANDALLANDCHECKCONSISTENCY_MEMORY_USAGE_IS_NOT_CONSISTENT_WITH,
            memoryBlock.getUsedMemoryInBytes(),
            blockSum);
      }

      final long tabletBlockSum =
          memoryBlocks.stream()
              .filter(PipeTabletMemoryBlock.class::isInstance)
              .mapToLong(PipeMemoryManager::getDirectMemoryUsageInBytes)
              .sum();
      if (tabletBlockSum != usedMemorySizeInBytesOfTablets) {
        LOGGER.debug(
            DataNodePipeMessages.TRYEXPANDALLANDCHECKCONSISTENCY_MEMORY_USAGE_OF_TABLETS_IS_NOT,
            usedMemorySizeInBytesOfTablets,
            tabletBlockSum);
      }

      final long tsFileBlockSum =
          memoryBlocks.stream()
              .filter(PipeTsFileMemoryBlock.class::isInstance)
              .mapToLong(PipeMemoryManager::getDirectMemoryUsageInBytes)
              .sum();
      if (tsFileBlockSum != usedMemorySizeInBytesOfTsFiles) {
        LOGGER.debug(
            DataNodePipeMessages.TRYEXPANDALLANDCHECKCONSISTENCY_MEMORY_USAGE_OF_TSFILES_IS_NOT,
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
    if (block == null || block.isReleased()) {
      return;
    }

    // A parent owns the lifetime of its descendants. Release children first so their bytes are
    // removed from the parent aggregate before the parent itself is released.
    for (final PipeMemoryBlock child : block.getChildrenSnapshot()) {
      release(child);
    }
    // A cascaded release does not invoke each child's close() method. Remove every released block
    // from the shrink/expand registries here so the periodic maintenance task cannot touch it.
    shrinkableBlocks.remove(block);
    expandableBlocks.remove(block);
    memoryBlocks.remove(block);
    releaseMemoryForBlock(block, block.getMemoryUsageInBytes());
    allocatedBlocks.remove(block);
    block.removeFromParent();
    block.markAsReleased();

    notifyNextTsFileParserMemoryReservationInternal();
    this.notifyAll();
  }

  public synchronized boolean release(PipeMemoryBlock block, long sizeInBytes) {
    // Keep the historical disabled-mode behavior: dynamic shrink callbacks do not participate in
    // memory management when the feature is turned off. A full close still removes diagnostics.
    if (!PIPE_MEMORY_MANAGEMENT_ENABLED || !releaseMemoryForBlock(block, sizeInBytes)) {
      return false;
    }

    notifyNextTsFileParserMemoryReservationInternal();
    this.notifyAll();

    return true;
  }

  public long getUsedMemorySizeInBytes() {
    return memoryBlock.getUsedMemoryInBytes();
  }

  public long getUsedMemorySizeInBytesOfTablets() {
    return usedMemorySizeInBytesOfTablets;
  }

  public long getUsedMemorySizeInBytesOfTsFiles() {
    return usedMemorySizeInBytesOfTsFiles;
  }

  public long getFreeMemorySizeInBytes() {
    return Math.max(0, getTotalNonFloatingMemorySizeInBytes() - memoryBlock.getUsedMemoryInBytes());
  }

  public long getTotalNonFloatingMemorySizeInBytes() {
    // Floating memory is an upper limit for retained InsertNodes instead of a statically reserved
    // partition. Non-floating allocations can borrow all floating memory that is not actually in
    // use, which is especially important for TsFile-only pipes.
    return Math.max(
        0, memoryBlock.getTotalMemorySizeInBytes() - getUsedFloatingMemorySizeInBytes());
  }

  public long getTotalFloatingMemorySizeInBytes() {
    final long configuredUpperLimit =
        Math.max(
            0,
            (long)
                (memoryBlock.getTotalMemorySizeInBytes()
                    * PipeConfig.getInstance().getPipeTotalFloatingMemoryProportion()));
    final long memoryNotUsedByNonFloatingAllocations =
        Math.max(0, memoryBlock.getTotalMemorySizeInBytes() - memoryBlock.getUsedMemoryInBytes());
    return Math.min(configuredUpperLimit, memoryNotUsedByNonFloatingAllocations);
  }

  private long getUsedFloatingMemorySizeInBytes() {
    final long usageInBytes = Math.max(0, floatingMemoryUsageSupplier.getAsLong());
    floatingMemoryMaxUsageInBytes = Math.max(floatingMemoryMaxUsageInBytes, usageInBytes);
    return usageInBytes;
  }

  public long getTotalMemorySizeInBytes() {
    return memoryBlock.getTotalMemorySizeInBytes();
  }

  public synchronized List<PipeMemoryBlockInfo> getPipeMemoryBlockInfoList() {
    final List<PipeMemoryBlockInfo> memoryBlockInfoList = new ArrayList<>();
    memoryBlocks.forEach(
        block ->
            memoryBlockInfoList.add(
                new PipeMemoryBlockInfo(
                    block.getBlockId(),
                    block.getName(),
                    block.getCategory().name(),
                    block.getMemoryUsageInBytes(),
                    block.getMaxMemorySizeInBytes(),
                    block.getAllocationTimeInMillis(),
                    block.getAssigner(),
                    block.getParentBlockId(),
                    block.getHierarchyLevel(),
                    block.getAccountedMemoryUsageInBytes())));
    final long floatingMemoryUsageInBytes = getUsedFloatingMemorySizeInBytes();
    floatingMemoryMaxUsageInBytes =
        Math.max(floatingMemoryMaxUsageInBytes, floatingMemoryUsageInBytes);
    memoryBlockInfoList.add(
        new PipeMemoryBlockInfo(
            0,
            FLOATING_MEMORY_BLOCK_NAME,
            PipeMemoryBlockCategory.FLOATING.name(),
            floatingMemoryUsageInBytes,
            floatingMemoryMaxUsageInBytes,
            floatingMemoryAllocationTime,
            "PipeMemoryManager",
            null,
            0,
            floatingMemoryUsageInBytes));
    memoryBlockInfoList.sort(
        Comparator.comparing(PipeMemoryBlockInfo::getName)
            .thenComparingLong(PipeMemoryBlockInfo::getMemoryUsageInBytes)
            .thenComparingLong(PipeMemoryBlockInfo::getBlockId));
    return memoryBlockInfoList;
  }

  public static final class PipeMemoryBlockInfo {

    private final long blockId;
    private final String name;
    private final String category;
    private final long memoryUsageInBytes;
    private final long maxMemorySizeInBytes;
    private final long allocationTime;
    private final String assigner;
    private final Long parentBlockId;
    private final int hierarchyLevel;
    private final long accountedMemoryUsageInBytes;

    private PipeMemoryBlockInfo(
        final long blockId,
        final String name,
        final String category,
        final long memoryUsageInBytes,
        final long maxMemorySizeInBytes,
        final long allocationTime,
        final String assigner,
        final Long parentBlockId,
        final int hierarchyLevel,
        final long accountedMemoryUsageInBytes) {
      this.blockId = blockId;
      this.name = name;
      this.category = category;
      this.memoryUsageInBytes = memoryUsageInBytes;
      this.maxMemorySizeInBytes = maxMemorySizeInBytes;
      this.allocationTime = allocationTime;
      this.assigner = assigner;
      this.parentBlockId = parentBlockId;
      this.hierarchyLevel = hierarchyLevel;
      this.accountedMemoryUsageInBytes = accountedMemoryUsageInBytes;
    }

    private PipeMemoryBlockInfo(final String name, final long memoryUsageInBytes) {
      this(
          0,
          name,
          PipeMemoryBlockCategory.OTHER.name(),
          memoryUsageInBytes,
          memoryUsageInBytes,
          System.currentTimeMillis(),
          null,
          null,
          0,
          memoryUsageInBytes);
    }

    public long getBlockId() {
      return blockId;
    }

    public String getName() {
      return name;
    }

    public String getCategory() {
      return category;
    }

    public long getMemoryUsageInBytes() {
      return memoryUsageInBytes;
    }

    public long getMaxMemorySizeInBytes() {
      return maxMemorySizeInBytes;
    }

    public long getAllocationTime() {
      return allocationTime;
    }

    public long getAllocationTimeInMillis() {
      return allocationTime;
    }

    public String getAssigner() {
      return assigner;
    }

    public Long getParentBlockId() {
      return parentBlockId;
    }

    public int getHierarchyLevel() {
      return hierarchyLevel;
    }

    public long getAccountedMemoryUsageInBytes() {
      return accountedMemoryUsageInBytes;
    }
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
