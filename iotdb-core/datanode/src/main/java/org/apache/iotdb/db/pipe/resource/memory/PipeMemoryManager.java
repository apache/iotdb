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
import org.apache.iotdb.db.i18n.DataNodePipeMessages;
import org.apache.iotdb.db.pipe.agent.PipeDataNodeAgent;
import org.apache.iotdb.db.pipe.metric.overview.PipeResourceMetrics;
import org.apache.iotdb.db.pipe.metric.overview.PipeResourceMetrics.TsFileParserAdmissionRejectReason;
import org.apache.iotdb.db.pipe.resource.memory.strategy.ThresholdAllocationStrategy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.LongSupplier;
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

  private final long totalMemorySizeInBytes;
  private final LongSupplier floatingMemoryUsageSupplier;

  private long usedMemorySizeInBytes;

  private static final double EXCEED_PROTECT_THRESHOLD = 0.95;

  private volatile long usedMemorySizeInBytesOfTablets;

  private volatile long usedMemorySizeInBytesOfTsFiles;

  private volatile long reservedTsFileParserCount;
  private volatile long waitingTsFileParserCount;

  private final Map<PipeIdentity, Integer> reservedTsFileParserCountByPipe = new HashMap<>();
  private final Map<PipeRegionIdentity, Integer> reservedTsFileParserCountByPipeRegion =
      new HashMap<>();
  private final Map<PipeRegionIdentity, Map<TsFileParserMemoryReservation, Runnable>>
      reservedTsFileParserWorkersByPipeRegion = new HashMap<>();
  private final Map<Runnable, Integer> reservedTsFileParserCountByWorker = new HashMap<>();
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
    this(
        TOTAL_MEMORY_SIZE_IN_BYTES,
        () -> PipeDataNodeAgent.task().getAllFloatingMemoryUsageInByte());
    PipeDataNodeAgent.runtime()
        .registerPeriodicalJob(
            "PipeMemoryManager#tryExpandAll()",
            this::tryExpandAllAndCheckConsistency,
            PipeConfig.getInstance().getPipeMemoryExpanderIntervalSeconds());
    PipeDataNodeAgent.runtime()
        .registerPeriodicalJob(
            "PipeMemoryManager#notifyTsFileParserMemoryReservation()",
            this::notifyNextTsFileParserMemoryReservation,
            PipeConfig.getInstance().getPipeSubtaskExecutorCronHeartbeatEventIntervalSeconds());
  }

  PipeMemoryManager(
      final long totalMemorySizeInBytes, final LongSupplier floatingMemoryUsageSupplier) {
    this.totalMemorySizeInBytes = totalMemorySizeInBytes;
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

  private double allowedMaxMemorySizeInBytesOfTabletsAndTsFiles(
      final long totalNonFloatingMemorySizeInBytes) {
    return (PIPE_CONFIG.getPipeDataStructureTabletMemoryBlockAllocationRejectThreshold()
            + PIPE_CONFIG.getPipeDataStructureTsFileMemoryBlockAllocationRejectThreshold())
        * totalNonFloatingMemorySizeInBytes;
  }

  private double allowedMaxMemorySizeInBytesOfTablets(
      final long totalNonFloatingMemorySizeInBytes) {
    return (PIPE_CONFIG.getPipeDataStructureTabletMemoryBlockAllocationRejectThreshold()
            + PIPE_CONFIG.getPipeDataStructureTsFileMemoryBlockAllocationRejectThreshold() / 2)
        * totalNonFloatingMemorySizeInBytes;
  }

  private double allowedMaxMemorySizeInBytesOfTsTiles(
      final long totalNonFloatingMemorySizeInBytes) {
    return (PIPE_CONFIG.getPipeDataStructureTsFileMemoryBlockAllocationRejectThreshold()
            + PIPE_CONFIG.getPipeDataStructureTabletMemoryBlockAllocationRejectThreshold() / 2)
        * totalNonFloatingMemorySizeInBytes;
  }

  private boolean isEnoughForTsFileParserMemory(
      final long extraMemoryInBytes, final long totalNonFloatingMemorySizeInBytes) {
    return (double) usedMemorySizeInBytesOfTablets
            + (double) usedMemorySizeInBytesOfTsFiles
            + (double) extraMemoryInBytes
        < EXCEED_PROTECT_THRESHOLD
            * allowedMaxMemorySizeInBytesOfTabletsAndTsFiles(totalNonFloatingMemorySizeInBytes);
  }

  /** Reports both global availability and the category limits that can reject a resize. */
  public synchronized String getResizeFailureMessage(final int retries, final long requestedSize) {
    final long totalNonFloatingMemorySizeInBytes = getTotalNonFloatingMemorySizeInBytes();
    final double combinedLimit =
        allowedMaxMemorySizeInBytesOfTabletsAndTsFiles(totalNonFloatingMemorySizeInBytes);
    return String.format(
        DataNodePipeMessages
            .EXCEPTION_FORCERESIZE_FAILED_AFTER_ARG_RETRIES_TOTAL_ARG_BYTES_USED_ARG_BYTES_USEDTABLETS_ARG_BYTES_USEDTSFILES_ARG_BYTES_TABLETLIMIT_ARG_BYTES_TSFILELIMIT_ARG_BYTES_TABLETANDTSFILELIMIT_ARG_BYTES_PROTECTEDPARSERLIMIT_ARG_BYTES_REQUESTED_ARG_BYTES_311C8C7E,
        retries,
        totalNonFloatingMemorySizeInBytes,
        usedMemorySizeInBytes,
        usedMemorySizeInBytesOfTablets,
        usedMemorySizeInBytesOfTsFiles,
        Math.round(allowedMaxMemorySizeInBytesOfTablets(totalNonFloatingMemorySizeInBytes)),
        Math.round(allowedMaxMemorySizeInBytesOfTsTiles(totalNonFloatingMemorySizeInBytes)),
        Math.round(combinedLimit),
        Math.round(EXCEED_PROTECT_THRESHOLD * combinedLimit),
        requestedSize);
  }

  private static long getTsFileParserMemorySizeInBytes() {
    return Math.max(
        PipeConfig.getInstance().getTsFileParserMemory(), MEMORY_ALLOCATE_MIN_SIZE_IN_BYTES);
  }

  private long getReservedTsFileParserMemorySizeInBytes() {
    return reservedTsFileParserCount * getTsFileParserMemorySizeInBytes();
  }

  private boolean isEnough4TabletParsingWithReservedParserMemory(
      final long extraMemoryInBytes, final long totalNonFloatingMemorySizeInBytes) {
    final double tabletMemoryWithParserMemory =
        (double) usedMemorySizeInBytesOfTablets
            + getReservedTsFileParserMemorySizeInBytes()
            + extraMemoryInBytes;
    return tabletMemoryWithParserMemory + (double) usedMemorySizeInBytesOfTsFiles
            < EXCEED_PROTECT_THRESHOLD
                * allowedMaxMemorySizeInBytesOfTabletsAndTsFiles(totalNonFloatingMemorySizeInBytes)
        && tabletMemoryWithParserMemory
            < EXCEED_PROTECT_THRESHOLD
                * allowedMaxMemorySizeInBytesOfTablets(totalNonFloatingMemorySizeInBytes);
  }

  private boolean isHardEnough4TabletParsingWithReservedParserMemory(
      final long extraMemoryInBytes, final long totalNonFloatingMemorySizeInBytes) {
    final double tabletMemoryWithParserMemory =
        (double) usedMemorySizeInBytesOfTablets
            + getReservedTsFileParserMemorySizeInBytes()
            + extraMemoryInBytes;
    return tabletMemoryWithParserMemory + (double) usedMemorySizeInBytesOfTsFiles
            < allowedMaxMemorySizeInBytesOfTabletsAndTsFiles(totalNonFloatingMemorySizeInBytes)
        && tabletMemoryWithParserMemory
            < allowedMaxMemorySizeInBytesOfTablets(totalNonFloatingMemorySizeInBytes);
  }

  public boolean isEnough4TabletParsing() {
    final long totalNonFloatingMemorySizeInBytes = getTotalNonFloatingMemorySizeInBytes();
    return (double) usedMemorySizeInBytesOfTablets + (double) usedMemorySizeInBytesOfTsFiles
            < EXCEED_PROTECT_THRESHOLD
                * allowedMaxMemorySizeInBytesOfTabletsAndTsFiles(totalNonFloatingMemorySizeInBytes)
        && (double) usedMemorySizeInBytesOfTablets
            < EXCEED_PROTECT_THRESHOLD
                * allowedMaxMemorySizeInBytesOfTablets(totalNonFloatingMemorySizeInBytes);
  }

  private boolean isHardEnough4TabletParsing() {
    final long totalNonFloatingMemorySizeInBytes = getTotalNonFloatingMemorySizeInBytes();
    return (double) usedMemorySizeInBytesOfTablets + (double) usedMemorySizeInBytesOfTsFiles
            < allowedMaxMemorySizeInBytesOfTabletsAndTsFiles(totalNonFloatingMemorySizeInBytes)
        && (double) usedMemorySizeInBytesOfTablets
            < allowedMaxMemorySizeInBytesOfTablets(totalNonFloatingMemorySizeInBytes);
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
    final Runnable worker = reservationKey.wakeUpCallback;
    if (reservedTsFileParserCount >= globalLimit
        || reservedCountOfPipeRegion >= perPipeRegionLimit
        || (worker != null && reservedTsFileParserCountByWorker.containsKey(worker))) {
      PipeResourceMetrics.getInstance()
          .recordTsFileParserAdmissionReject(TsFileParserAdmissionRejectReason.CAP);
      notifyNextTsFileParserMemoryReservationInternal();
      return false;
    }

    final long parserMemorySizeInBytes = getTsFileParserMemorySizeInBytes();
    // Reuse one live snapshot for both thresholds. The supplier traverses all pipe metadata, so
    // repeatedly sampling it while holding this monitor amplifies contention with many pipes.
    final long totalNonFloatingMemorySizeInBytes =
        PIPE_MEMORY_MANAGEMENT_ENABLED ? getTotalNonFloatingMemorySizeInBytes() : 0;
    final boolean isSoftMemoryEnough =
        !PIPE_MEMORY_MANAGEMENT_ENABLED
            || isEnough4TabletParsingWithReservedParserMemory(
                parserMemorySizeInBytes, totalNonFloatingMemorySizeInBytes);
    if (!isSoftMemoryEnough
        && !isHardEnough4TabletParsingWithReservedParserMemory(
            parserMemorySizeInBytes, totalNonFloatingMemorySizeInBytes)) {
      PipeResourceMetrics.getInstance()
          .recordTsFileParserAdmissionReject(TsFileParserAdmissionRejectReason.MEMORY);
      return false;
    }

    final List<TsFileParserMemoryReservation> eligibleRequests =
        getEligibleTsFileParserMemoryReservations(
            globalLimit, perPipeRegionLimit, totalNonFloatingMemorySizeInBytes);
    if (!eligibleRequests.contains(reservationKey)) {
      PipeResourceMetrics.getInstance()
          .recordTsFileParserAdmissionReject(TsFileParserAdmissionRejectReason.FAIRNESS);
      eligibleRequests.forEach(TsFileParserMemoryReservation::signal);
      return false;
    }

    removeTsFileParserReservationRequest(pipeRegionIdentity, reservationKey, true);
    // First-attempt admissions also spend a fair turn when other requests are queued. Otherwise
    // continuously arriving pipes can bypass the same waiting pipe without advancing the cursor.
    if (wasRequestAlreadyWaiting || waitingTsFileParserCount > 0) {
      lastAdmittedWaitingTsFileParserPipe = pipeIdentity;
    }
    if (wasRequestAlreadyWaiting) {
      PipeResourceMetrics.getInstance()
          .recordTsFileParserAdmissionWait(
              System.nanoTime() - reservationKey.waitingStartTimeInNanos);
    }
    reservedTsFileParserCount++;
    reservedTsFileParserCountByPipe.merge(pipeIdentity, 1, Integer::sum);
    reservedTsFileParserCountByPipeRegion.put(pipeRegionIdentity, reservedCountOfPipeRegion + 1);
    reservedTsFileParserWorkersByPipeRegion
        .computeIfAbsent(pipeRegionIdentity, key -> new LinkedHashMap<>())
        .put(reservationKey, worker);
    if (worker != null) {
      reservedTsFileParserCountByWorker.merge(worker, 1, Integer::sum);
    }
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
    releaseTsFileParserMemory(pipeName, creationTime, dataRegionId, null);
  }

  public synchronized void releaseTsFileParserMemory(
      final String pipeName,
      final long creationTime,
      final String dataRegionId,
      final TsFileParserMemoryReservation reservationKey) {
    final PipeIdentity pipeIdentity = new PipeIdentity(pipeName, creationTime);
    final PipeRegionIdentity pipeRegionIdentity =
        new PipeRegionIdentity(pipeIdentity, dataRegionId);
    final int reservedCountOfPipeRegion =
        reservedTsFileParserCountByPipeRegion.getOrDefault(pipeRegionIdentity, 0);
    final Map<TsFileParserMemoryReservation, Runnable> workersOfPipeRegion =
        reservedTsFileParserWorkersByPipeRegion.get(pipeRegionIdentity);
    if (reservedCountOfPipeRegion <= 0
        || (reservationKey != null && !workersOfPipeRegion.containsKey(reservationKey))) {
      LOGGER.warn(
          "Failed to release TsFile parser memory for pipe {} (creation time {}) in DataRegion {} because no reservation exists.",
          pipeName,
          creationTime,
          dataRegionId);
      return;
    }

    final Runnable worker =
        workersOfPipeRegion.remove(
            reservationKey != null
                ? reservationKey
                : workersOfPipeRegion.keySet().iterator().next());
    if (workersOfPipeRegion.isEmpty()) {
      reservedTsFileParserWorkersByPipeRegion.remove(pipeRegionIdentity);
    }
    if (worker != null) {
      reservedTsFileParserCountByWorker.computeIfPresent(
          worker, (key, count) -> count == 1 ? null : count - 1);
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
                        // Keep the remembered cursor at the tail. A newly arriving pipe must not
                        // jump ahead of older, already notified requests by landing after it.
                        if (lastAdmittedWaitingTsFileParserPipe != null
                            && lastAdmittedWaitingTsFileParserPipe.equals(
                                waitingTsFileParserPipeOrder.peekLast())) {
                          final PipeIdentity cursor = waitingTsFileParserPipeOrder.removeLast();
                          waitingTsFileParserPipeOrder.addLast(pipe);
                          waitingTsFileParserPipeOrder.addLast(cursor);
                        } else {
                          waitingTsFileParserPipeOrder.addLast(pipe);
                        }
                        return new ArrayDeque<>();
                      });
              regionOrder.addLast(key);
              return new LinkedHashSet<>();
            });
    if (requestsOfPipeRegion.add(reservationKey)) {
      waitingTsFileParserCount++;
      reservationKey.waitingStartTimeInNanos = System.nanoTime();
      reservationKey.markWaiting();
      return false;
    }
    reservationKey.markWaiting();
    return true;
  }

  public synchronized void notifyNextTsFileParserMemoryReservation() {
    notifyNextTsFileParserMemoryReservationInternal();
  }

  private void notifyNextTsFileParserMemoryReservationInternal() {
    final int globalLimit = Math.max(1, PIPE_CONFIG.getPipeTsFileParserInFlightMaxNum());
    if (waitingTsFileParserPipeOrder.isEmpty() || reservedTsFileParserCount >= globalLimit) {
      return;
    }

    final long totalNonFloatingMemorySizeInBytes =
        PIPE_MEMORY_MANAGEMENT_ENABLED ? getTotalNonFloatingMemorySizeInBytes() : 0;
    final int perPipeRegionLimit =
        Math.max(
            1, Math.min(globalLimit, PIPE_CONFIG.getPipeTsFileParserInFlightMaxNumPerPipeRegion()));
    getEligibleTsFileParserMemoryReservations(
            globalLimit, perPipeRegionLimit, totalNonFloatingMemorySizeInBytes)
        .forEach(TsFileParserMemoryReservation::signal);
  }

  private List<TsFileParserMemoryReservation> getEligibleTsFileParserMemoryReservations(
      final int globalLimit,
      final int perPipeRegionLimit,
      final long totalNonFloatingMemorySizeInBytes) {
    final List<TsFileParserMemoryReservation> eligibleRequests = new ArrayList<>();
    final Map<PipeRegionIdentity, Integer> selectedRequestsByPipeRegion = new HashMap<>();
    final Set<PipeIdentity> selectedPipes = new HashSet<>();
    final Set<Runnable> selectedWorkers = new HashSet<>();
    PipeIdentity lastSelectedPipe = lastAdmittedWaitingTsFileParserPipe;
    // Expose one fair turn per available slot, instead of serializing all free slots behind a
    // single worker. Account for every earlier turn's memory even if its worker has not run yet.
    // Thus later requests cannot take the capacity needed by an earlier waiting request.
    while (eligibleRequests.size() < globalLimit - reservedTsFileParserCount) {
      final long extraMemoryInBytes =
          (eligibleRequests.size() + 1L) * getTsFileParserMemorySizeInBytes();
      final boolean isSoftMemoryEnough =
          !PIPE_MEMORY_MANAGEMENT_ENABLED
              || isEnough4TabletParsingWithReservedParserMemory(
                  extraMemoryInBytes, totalNonFloatingMemorySizeInBytes);
      if (!isSoftMemoryEnough
          && !isHardEnough4TabletParsingWithReservedParserMemory(
              extraMemoryInBytes, totalNonFloatingMemorySizeInBytes)) {
        break;
      }
      final PipeRegionIdentity nextPipeRegion =
          getNextEligibleTsFileParserPipeRegion(
              perPipeRegionLimit,
              !isSoftMemoryEnough,
              lastSelectedPipe,
              selectedRequestsByPipeRegion,
              selectedPipes,
              selectedWorkers,
              true);
      final PipeRegionIdentity nextRegionWhenNoOtherPipeIsEligible =
          nextPipeRegion == null
              ? getNextEligibleTsFileParserPipeRegion(
                  perPipeRegionLimit,
                  !isSoftMemoryEnough,
                  lastSelectedPipe,
                  selectedRequestsByPipeRegion,
                  selectedPipes,
                  selectedWorkers,
                  false)
              : nextPipeRegion;
      if (nextRegionWhenNoOtherPipeIsEligible == null) {
        break;
      }
      final java.util.Iterator<TsFileParserMemoryReservation> requests =
          waitingTsFileParserRequestsByPipeRegion
              .get(nextRegionWhenNoOtherPipeIsEligible)
              .iterator();
      for (int i =
              selectedRequestsByPipeRegion.getOrDefault(nextRegionWhenNoOtherPipeIsEligible, 0);
          i > 0;
          i--) {
        requests.next();
      }
      final TsFileParserMemoryReservation request = requests.next();
      eligibleRequests.add(request);
      if (request.wakeUpCallback != null) {
        selectedWorkers.add(request.wakeUpCallback);
      }
      selectedRequestsByPipeRegion.merge(nextRegionWhenNoOtherPipeIsEligible, 1, Integer::sum);
      selectedPipes.add(nextRegionWhenNoOtherPipeIsEligible.pipeIdentity);
      lastSelectedPipe = nextRegionWhenNoOtherPipeIsEligible.pipeIdentity;
    }
    return eligibleRequests;
  }

  private PipeRegionIdentity getNextEligibleTsFileParserPipeRegion(
      final int perPipeRegionLimit,
      final boolean requirePipeWithoutReservedParser,
      final PipeIdentity lastSelectedPipe,
      final Map<PipeRegionIdentity, Integer> selectedRequestsByPipeRegion,
      final Set<PipeIdentity> selectedPipes,
      final Set<Runnable> selectedWorkers,
      final boolean requireUnselectedPipe) {
    PipeRegionIdentity firstEligiblePipeRegion = null;
    boolean hasVisitedLastAdmittedPipe = lastSelectedPipe == null;
    for (final PipeIdentity pipeIdentity : waitingTsFileParserPipeOrder) {
      final boolean isLastAdmittedPipe = pipeIdentity.equals(lastSelectedPipe);
      if (isLastAdmittedPipe) {
        hasVisitedLastAdmittedPipe = true;
      }

      // Under soft memory pressure, reserve the hard-threshold headroom for a pipe that has no
      // parser yet. Otherwise a busy pipe at the queue head can block every pipe behind it.
      if (requirePipeWithoutReservedParser
          && (reservedTsFileParserCountByPipe.getOrDefault(pipeIdentity, 0) > 0
              || selectedPipes.contains(pipeIdentity))) {
        continue;
      }
      if (requireUnselectedPipe && selectedPipes.contains(pipeIdentity)) {
        continue;
      }

      final ArrayDeque<PipeRegionIdentity> regionOrder =
          waitingTsFileParserRegionOrderByPipe.get(pipeIdentity);
      if (regionOrder == null) {
        continue;
      }
      PipeRegionIdentity eligiblePipeRegion = null;
      for (final PipeRegionIdentity pipeRegionIdentity : regionOrder) {
        final int selectedCount = selectedRequestsByPipeRegion.getOrDefault(pipeRegionIdentity, 0);
        if (selectedCount < waitingTsFileParserRequestsByPipeRegion.get(pipeRegionIdentity).size()
            && reservedTsFileParserCountByPipeRegion.getOrDefault(pipeRegionIdentity, 0)
                    + selectedCount
                < perPipeRegionLimit) {
          final java.util.Iterator<TsFileParserMemoryReservation> requests =
              waitingTsFileParserRequestsByPipeRegion.get(pipeRegionIdentity).iterator();
          for (int i = 0; i < selectedCount; i++) {
            requests.next();
          }
          final Runnable worker = requests.next().wakeUpCallback;
          // A processor worker executes its subtasks serially. Leaving multiple fair turns for
          // that worker strands free parser slots while other workers have runnable requests.
          // Requests without a processor worker (for example subscriptions) remain independent.
          if (worker != null
              && (reservedTsFileParserCountByWorker.containsKey(worker)
                  || selectedWorkers.contains(worker))) {
            continue;
          }
          if (eligiblePipeRegion == null) {
            eligiblePipeRegion = pipeRegionIdentity;
          }
          // Give every eligible region of the pipe one turn before reusing a region's remaining
          // per-pipe-region capacity in this admission window.
          if (selectedCount == 0) {
            eligiblePipeRegion = pipeRegionIdentity;
            break;
          }
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
    waitingTsFileParserCount--;
    reservationKey.clearWaiting();

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
    private boolean isWaiting;
    private volatile Runnable wakeUpCallback;
    // Accessed only under the PipeMemoryManager monitor while this request is queued.
    private long waitingStartTimeInNanos;

    /**
     * The callback must only wake the worker; it runs while the memory manager monitor is held.
     * Requests on the same serial processor worker must share the same callback instance.
     */
    public void setWakeUpCallback(final Runnable wakeUpCallback) {
      this.wakeUpCallback = wakeUpCallback;
    }

    public synchronized boolean isWaitingForSignal() {
      return isWaiting && !isSignaled;
    }

    private synchronized void markWaiting() {
      isWaiting = true;
      isSignaled = false;
    }

    private synchronized void clearWaiting() {
      isWaiting = false;
      isSignaled = false;
    }

    public synchronized void await(final long timeoutInMs) throws InterruptedException {
      if (!isSignaled) {
        wait(timeoutInMs);
      }
      isSignaled = false;
    }

    private synchronized void signal() {
      if (isSignaled) {
        return;
      }
      isSignaled = true;
      notify();
      final Runnable callback = wakeUpCallback;
      if (callback != null) {
        callback.run();
      }
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

    if (!isHardEnough4TabletParsingWithReservedParserMemory(
        0, getTotalNonFloatingMemorySizeInBytes())) {
      return true;
    }

    return retryCount >= PipeConfig.getInstance().getPipeMemoryAllocateMaxRetries()
        || elapsedTimeInMs >= maxRetryTimeInMs;
  }

  public boolean isEnough4TsFileSlicing() {
    final long totalNonFloatingMemorySizeInBytes = getTotalNonFloatingMemorySizeInBytes();
    return (double) usedMemorySizeInBytesOfTablets + (double) usedMemorySizeInBytesOfTsFiles
            < EXCEED_PROTECT_THRESHOLD
                * allowedMaxMemorySizeInBytesOfTabletsAndTsFiles(totalNonFloatingMemorySizeInBytes)
        && (double) usedMemorySizeInBytesOfTsFiles
            < EXCEED_PROTECT_THRESHOLD
                * allowedMaxMemorySizeInBytesOfTsTiles(totalNonFloatingMemorySizeInBytes);
  }

  private boolean isHardEnough4TsFileSlicing() {
    final long totalNonFloatingMemorySizeInBytes = getTotalNonFloatingMemorySizeInBytes();
    return (double) usedMemorySizeInBytesOfTablets + (double) usedMemorySizeInBytesOfTsFiles
            < allowedMaxMemorySizeInBytesOfTabletsAndTsFiles(totalNonFloatingMemorySizeInBytes)
        && (double) usedMemorySizeInBytesOfTsFiles
            < allowedMaxMemorySizeInBytesOfTsTiles(totalNonFloatingMemorySizeInBytes);
  }

  private boolean isHardEnoughForResizing(
      final PipeMemoryBlock block, final long extraMemoryInBytes) {
    if (block instanceof PipeTsFileParserMemoryBlock) {
      return isEnoughForTsFileParserMemory(
          extraMemoryInBytes, getTotalNonFloatingMemorySizeInBytes());
    }
    if (!(block instanceof PipeTabletMemoryBlock) && !(block instanceof PipeTsFileMemoryBlock)) {
      return true;
    }
    final long totalNonFloatingMemorySizeInBytes = getTotalNonFloatingMemorySizeInBytes();
    if (block instanceof PipeTabletMemoryBlock) {
      return (double) usedMemorySizeInBytesOfTablets
                  + (double) extraMemoryInBytes
                  + (double) usedMemorySizeInBytesOfTsFiles
              < allowedMaxMemorySizeInBytesOfTabletsAndTsFiles(totalNonFloatingMemorySizeInBytes)
          && (double) usedMemorySizeInBytesOfTablets + (double) extraMemoryInBytes
              < allowedMaxMemorySizeInBytesOfTablets(totalNonFloatingMemorySizeInBytes);
    }
    if (block instanceof PipeTsFileMemoryBlock) {
      return (double) usedMemorySizeInBytesOfTablets
                  + (double) usedMemorySizeInBytesOfTsFiles
                  + (double) extraMemoryInBytes
              < allowedMaxMemorySizeInBytesOfTabletsAndTsFiles(totalNonFloatingMemorySizeInBytes)
          && (double) usedMemorySizeInBytesOfTsFiles + (double) extraMemoryInBytes
              < allowedMaxMemorySizeInBytesOfTsTiles(totalNonFloatingMemorySizeInBytes);
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

  /** Allocates TsFile parser memory against the protected tablet + TsFile combined quota. */
  public synchronized PipeTsFileParserMemoryBlock forceAllocateForTsFileParserWithRetry(
      final long parserMemorySizeInBytes) {
    if (!PIPE_MEMORY_MANAGEMENT_ENABLED) {
      return new PipeTsFileParserMemoryBlock(0);
    }

    final PipeTsFileParserMemoryBlock block =
        (PipeTsFileParserMemoryBlock) registerMemoryBlock(0, PipeMemoryBlockType.TS_FILE_PARSER);
    if (parserMemorySizeInBytes == 0) {
      return block;
    }

    try {
      forceResize(block, parserMemorySizeInBytes);
      return block;
    } catch (final RuntimeException e) {
      release(block);
      throw e;
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
      LOGGER.warn("forceResize: cannot resize a null or released memory block");
      return false;
    }
    return tryResizeInternal(block, targetSize);
  }

  public synchronized void resize(
      final PipeMemoryBlock block, final long targetSize, final boolean force) {
    if (block == null || block.isReleased()) {
      LOGGER.warn("forceResize: cannot resize a null or released memory block");
      return;
    }

    if (tryResizeInternal(block, targetSize)) {
      return;
    }

    final long sizeInBytes = targetSize - block.getMemoryUsageInBytes();
    final int memoryAllocateMaxRetries = PipeConfig.getInstance().getPipeMemoryAllocateMaxRetries();
    for (int i = 1; i <= memoryAllocateMaxRetries; i++) {
      try {
        tryShrinkUntilFreeMemorySatisfy(sizeInBytes);
        if (tryResizeInternal(block, targetSize)) {
          return;
        }
        this.wait(PipeConfig.getInstance().getPipeMemoryAllocateRetryIntervalInMs());
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        LOGGER.warn("forceResize: interrupted while waiting for available memory", e);
      }
    }

    if (force) {
      throw new PipeRuntimeOutOfMemoryCriticalException(
          getResizeFailureMessage(memoryAllocateMaxRetries, sizeInBytes));
    }
  }

  private boolean tryResizeInternal(final PipeMemoryBlock block, final long targetSize) {
    if (block.isReleased()) {
      return false;
    }
    if (!PIPE_MEMORY_MANAGEMENT_ENABLED) {
      block.setMemoryUsageInBytes(targetSize);
      return true;
    }

    final long oldSize = block.getMemoryUsageInBytes();
    if (oldSize >= targetSize) {
      final long releasedSize = oldSize - targetSize;
      usedMemorySizeInBytes -= releasedSize;
      if (block instanceof PipeTabletMemoryBlock) {
        usedMemorySizeInBytesOfTablets -= releasedSize;
      }
      if (block instanceof PipeTsFileMemoryBlock) {
        usedMemorySizeInBytesOfTsFiles -= releasedSize;
      }
      block.setMemoryUsageInBytes(targetSize);

      if (targetSize == 0) {
        allocatedBlocks.remove(block);
      }

      notifyNextTsFileParserMemoryReservationInternal();
      this.notifyAll();
      return true;
    }

    final long sizeInBytes = targetSize - oldSize;
    // Dynamically resized data-structure blocks must obey the same admission thresholds as blocks
    // allocated with a non-zero initial size. Otherwise they can exhaust the pool and prevent
    // downstream consumers from allocating the memory needed to release them.
    if (!isHardEnoughForResizing(block, sizeInBytes)
        || getTotalNonFloatingMemorySizeInBytes() - usedMemorySizeInBytes < sizeInBytes) {
      return false;
    }

    usedMemorySizeInBytes += sizeInBytes;
    if (oldSize == 0) {
      // Zero-sized blocks are registered lazily on their first successful expansion.
      allocatedBlocks.add(block);
    }
    if (block instanceof PipeTabletMemoryBlock) {
      usedMemorySizeInBytesOfTablets += sizeInBytes;
    }
    if (block instanceof PipeTsFileMemoryBlock) {
      usedMemorySizeInBytesOfTsFiles += sizeInBytes;
    }
    block.setMemoryUsageInBytes(targetSize);
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
      case TS_FILE_PARSER:
        returnedMemoryBlock = new PipeTsFileParserMemoryBlock(sizeInBytes);
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
    // Floating-memory usage and the parent pool capacity can change without releasing a pipe
    // memory block. Re-evaluate blocked admissions even when no release notification was sent.
    notifyNextTsFileParserMemoryReservationInternal();

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

  public long getReservedTsFileParserCount() {
    return reservedTsFileParserCount;
  }

  public long getWaitingTsFileParserCount() {
    return waitingTsFileParserCount;
  }

  public long getFreeMemorySizeInBytes() {
    return Math.max(0, getTotalNonFloatingMemorySizeInBytes() - usedMemorySizeInBytes);
  }

  public long getTotalNonFloatingMemorySizeInBytes() {
    // Floating memory is an upper limit for retained InsertNodes instead of a statically reserved
    // partition. Non-floating allocations can borrow all floating memory that is not actually in
    // use, which is especially important for TsFile-only pipes.
    return Math.max(0, totalMemorySizeInBytes - getUsedFloatingMemorySizeInBytes());
  }

  public long getTotalFloatingMemorySizeInBytes() {
    final long configuredUpperLimit =
        Math.max(
            0,
            (long)
                (totalMemorySizeInBytes
                    * PipeConfig.getInstance().getPipeTotalFloatingMemoryProportion()));
    final long memoryNotUsedByNonFloatingAllocations =
        Math.max(0, totalMemorySizeInBytes - usedMemorySizeInBytes);
    return Math.min(configuredUpperLimit, memoryNotUsedByNonFloatingAllocations);
  }

  private long getUsedFloatingMemorySizeInBytes() {
    return Math.max(0, floatingMemoryUsageSupplier.getAsLong());
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
