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

package org.apache.iotdb.db.pipe.consensus.deletion;

import org.apache.iotdb.commons.utils.TestOnly;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * An ordered barrier for coordinating delete materialization and TsFile transfer.
 *
 * <p>Delete materialization and realtime TsFile transfer are observed by followers through
 * replicateIndex order, but the leader locally materializes delete mods after publishing delete
 * events. This barrier ensures that a TsFile snapshot:
 *
 * <p>1. includes every delete that entered the delete path before the TsFile got its replicate
 * index, and
 *
 * <p>2. excludes deletes that entered after that replicate index.
 */
public class PipeTsFileDeletionBarrier {

  private static final Logger LOGGER = LoggerFactory.getLogger(PipeTsFileDeletionBarrier.class);

  private static final long INITIAL_DELETE_SEQ = 0L;

  private final ConcurrentMap<Integer, RegionDeletionState> regionId2DeletionState =
      new ConcurrentHashMap<>();
  private final ConcurrentMap<String, TsFileBarrierState> tsFilePath2BarrierState =
      new ConcurrentHashMap<>();

  public long beginDeletion(final int regionId) {
    return getOrCreateRegionDeletionState(regionId).beginDeletion();
  }

  public void resolveDeletionTargets(
      final int regionId, final long deleteSeq, final Collection<String> tsFilePaths) {
    if (deleteSeq <= INITIAL_DELETE_SEQ) {
      return;
    }

    final RegionDeletionState regionDeletionState = getOrCreateRegionDeletionState(regionId);
    final Set<String> sanitizedTsFilePaths = sanitizeTsFilePaths(tsFilePaths);

    synchronized (regionDeletionState.monitor) {
      for (final String tsFilePath : sanitizedTsFilePaths) {
        getOrCreateTsFileBarrierState(tsFilePath).registerPendingDeletion(deleteSeq);
      }
      regionDeletionState.resolveDeletion(deleteSeq);
    }
  }

  public long beginSnapshot(final int regionId, final String tsFilePath) {
    if (Objects.isNull(tsFilePath)) {
      return INITIAL_DELETE_SEQ;
    }

    final RegionDeletionState regionDeletionState = getOrCreateRegionDeletionState(regionId);
    synchronized (regionDeletionState.monitor) {
      final long snapshotUpperBound = regionDeletionState.getMaxAllocatedDeletionSeq();
      getOrCreateTsFileBarrierState(tsFilePath).registerSnapshot(snapshotUpperBound);
      return snapshotUpperBound;
    }
  }

  public void awaitDeletionResolutionUpTo(final int regionId, final long deleteSeqUpperBound)
      throws InterruptedException {
    if (deleteSeqUpperBound <= INITIAL_DELETE_SEQ) {
      return;
    }

    getOrCreateRegionDeletionState(regionId).awaitDeletionResolutionUpTo(deleteSeqUpperBound);
  }

  public void awaitPendingDeletionsUpTo(final String tsFilePath, final long deleteSeqUpperBound)
      throws InterruptedException {
    if (Objects.isNull(tsFilePath) || deleteSeqUpperBound <= INITIAL_DELETE_SEQ) {
      return;
    }

    final TsFileBarrierState barrierState = tsFilePath2BarrierState.get(tsFilePath);
    if (Objects.isNull(barrierState)) {
      return;
    }

    barrierState.awaitPendingDeletionsUpTo(deleteSeqUpperBound);
  }

  public void awaitSnapshotsBeforeMaterialization(final String tsFilePath, final long deleteSeq)
      throws InterruptedException {
    if (Objects.isNull(tsFilePath) || deleteSeq <= INITIAL_DELETE_SEQ) {
      return;
    }

    final TsFileBarrierState barrierState = tsFilePath2BarrierState.get(tsFilePath);
    if (Objects.isNull(barrierState)) {
      return;
    }

    barrierState.awaitSnapshotsBeforeMaterialization(deleteSeq);
  }

  public void finishSnapshot(final String tsFilePath, final long snapshotUpperBound) {
    if (Objects.isNull(tsFilePath)) {
      return;
    }

    final TsFileBarrierState barrierState = tsFilePath2BarrierState.get(tsFilePath);
    if (Objects.isNull(barrierState)) {
      return;
    }

    barrierState.finishSnapshot(snapshotUpperBound);
    cleanupTsFileBarrierStateIfEmpty(tsFilePath, barrierState);
  }

  public void finishDeletion(final long deleteSeq, final Collection<String> tsFilePaths) {
    if (deleteSeq <= INITIAL_DELETE_SEQ) {
      return;
    }

    final Set<String> sanitizedTsFilePaths = sanitizeTsFilePaths(tsFilePaths);
    for (final String tsFilePath : sanitizedTsFilePaths) {
      final TsFileBarrierState barrierState = tsFilePath2BarrierState.get(tsFilePath);
      if (Objects.isNull(barrierState)) {
        continue;
      }

      barrierState.finishDeletion(deleteSeq);
      cleanupTsFileBarrierStateIfEmpty(tsFilePath, barrierState);
    }
  }

  @TestOnly
  public void clear() {
    regionId2DeletionState.clear();
    tsFilePath2BarrierState.clear();
  }

  private RegionDeletionState getOrCreateRegionDeletionState(final int regionId) {
    return regionId2DeletionState.computeIfAbsent(regionId, ignored -> new RegionDeletionState());
  }

  private TsFileBarrierState getOrCreateTsFileBarrierState(final String tsFilePath) {
    return tsFilePath2BarrierState.computeIfAbsent(tsFilePath, ignored -> new TsFileBarrierState());
  }

  private Set<String> sanitizeTsFilePaths(final Collection<String> tsFilePaths) {
    if (Objects.isNull(tsFilePaths) || tsFilePaths.isEmpty()) {
      return Collections.emptySet();
    }

    final Set<String> sanitizedTsFilePaths = new HashSet<>();
    for (final String tsFilePath : tsFilePaths) {
      if (Objects.nonNull(tsFilePath)) {
        sanitizedTsFilePaths.add(tsFilePath);
      }
    }
    return sanitizedTsFilePaths;
  }

  private void cleanupTsFileBarrierStateIfEmpty(
      final String tsFilePath, final TsFileBarrierState barrierState) {
    tsFilePath2BarrierState.computeIfPresent(
        tsFilePath,
        (ignored, state) -> {
          if (state != barrierState) {
            return state;
          }
          synchronized (state.monitor) {
            return state.isEmptyUnsafe() ? null : state;
          }
        });
  }

  private static class RegionDeletionState {
    private final Object monitor = new Object();
    private final NavigableSet<Long> unresolvedDeletionSeqs = new TreeSet<>();

    private long maxAllocatedDeletionSeq = INITIAL_DELETE_SEQ;
    private long maxResolvedDeletionSeq = INITIAL_DELETE_SEQ;

    private long beginDeletion() {
      synchronized (monitor) {
        final long deleteSeq = ++maxAllocatedDeletionSeq;
        unresolvedDeletionSeqs.add(deleteSeq);
        return deleteSeq;
      }
    }

    private long getMaxAllocatedDeletionSeq() {
      return maxAllocatedDeletionSeq;
    }

    private void resolveDeletion(final long deleteSeq) {
      synchronized (monitor) {
        if (deleteSeq > maxAllocatedDeletionSeq || deleteSeq <= INITIAL_DELETE_SEQ) {
          LOGGER.warn(
              "PipeTsFileDeletionBarrier: try to resolve invalid delete seq {}, max allocated {}.",
              deleteSeq,
              maxAllocatedDeletionSeq);
          return;
        }

        unresolvedDeletionSeqs.remove(deleteSeq);
        while (maxResolvedDeletionSeq < maxAllocatedDeletionSeq
            && !unresolvedDeletionSeqs.contains(maxResolvedDeletionSeq + 1)) {
          maxResolvedDeletionSeq++;
        }
        monitor.notifyAll();
      }
    }

    private void awaitDeletionResolutionUpTo(final long deleteSeqUpperBound)
        throws InterruptedException {
      synchronized (monitor) {
        while (maxResolvedDeletionSeq < deleteSeqUpperBound) {
          monitor.wait();
        }
      }
    }
  }

  private static class TsFileBarrierState {
    private final Object monitor = new Object();
    private final NavigableSet<Long> pendingDeletionSeqs = new TreeSet<>();
    private final NavigableMap<Long, Integer> snapshotUpperBound2ReferenceCount = new TreeMap<>();

    private void registerPendingDeletion(final long deleteSeq) {
      synchronized (monitor) {
        pendingDeletionSeqs.add(deleteSeq);
        monitor.notifyAll();
      }
    }

    private void finishDeletion(final long deleteSeq) {
      synchronized (monitor) {
        pendingDeletionSeqs.remove(deleteSeq);
        monitor.notifyAll();
      }
    }

    private void registerSnapshot(final long snapshotUpperBound) {
      synchronized (monitor) {
        snapshotUpperBound2ReferenceCount.merge(snapshotUpperBound, 1, Integer::sum);
        monitor.notifyAll();
      }
    }

    private void finishSnapshot(final long snapshotUpperBound) {
      synchronized (monitor) {
        snapshotUpperBound2ReferenceCount.computeIfPresent(
            snapshotUpperBound, (ignored, count) -> count == 1 ? null : count - 1);
        monitor.notifyAll();
      }
    }

    private void awaitPendingDeletionsUpTo(final long deleteSeqUpperBound)
        throws InterruptedException {
      synchronized (monitor) {
        while (!pendingDeletionSeqs.headSet(deleteSeqUpperBound, true).isEmpty()) {
          monitor.wait();
        }
      }
    }

    private void awaitSnapshotsBeforeMaterialization(final long deleteSeq)
        throws InterruptedException {
      synchronized (monitor) {
        while (hasSnapshotEarlierThan(deleteSeq)) {
          monitor.wait();
        }
      }
    }

    private boolean hasSnapshotEarlierThan(final long deleteSeq) {
      final Map.Entry<Long, Integer> firstSnapshot = snapshotUpperBound2ReferenceCount.firstEntry();
      return Objects.nonNull(firstSnapshot) && firstSnapshot.getKey() < deleteSeq;
    }

    private boolean isEmptyUnsafe() {
      return pendingDeletionSeqs.isEmpty() && snapshotUpperBound2ReferenceCount.isEmpty();
    }
  }

  /////////////////////////////// singleton ///////////////////////////////

  private static class PipeTsFileDeletionBarrierHolder {
    private static final PipeTsFileDeletionBarrier INSTANCE = new PipeTsFileDeletionBarrier();

    private PipeTsFileDeletionBarrierHolder() {
      // empty constructor
    }
  }

  public static PipeTsFileDeletionBarrier getInstance() {
    return PipeTsFileDeletionBarrierHolder.INSTANCE;
  }

  private PipeTsFileDeletionBarrier() {
    // empty constructor
  }
}
