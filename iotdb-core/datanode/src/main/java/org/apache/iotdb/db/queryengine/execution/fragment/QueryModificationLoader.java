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

package org.apache.iotdb.db.queryengine.execution.fragment;

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.PatternTreeMap;
import org.apache.iotdb.db.queryengine.exception.MemoryNotEnoughException;
import org.apache.iotdb.db.queryengine.plan.planner.memory.MemoryReservationManager;
import org.apache.iotdb.db.storageengine.dataregion.modification.Modification;
import org.apache.iotdb.db.storageengine.dataregion.modification.ModificationFile;
import org.apache.iotdb.db.storageengine.dataregion.modification.io.ModificationIterator;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileID;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.utils.datastructure.PatternTreeMapFactory;

import org.apache.tsfile.utils.RamUsageEstimator;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;

class QueryModificationLoader implements AutoCloseable {

  private final TsFileResource resource;
  private final MemoryReservationManager memoryReservationManager;
  private final long modsCacheSizeLimitPerFI;
  private final int modsMemoryEstimateReadInterval;
  private final Map<TsFileID, PatternTreeMap<Modification, PatternTreeMapFactory.ModsSerializer>>
      fileModCache;
  private final AtomicLong cachedModEntriesSize;
  private final Predicate<Modification> modificationMatcher;
  private final ModsTreeMatcher modsTreeMatcher;

  private ModificationIterator currentIterator;

  QueryModificationLoader(
      TsFileResource resource,
      MemoryReservationManager memoryReservationManager,
      long modsCacheSizeLimitPerFI,
      int modsMemoryEstimateReadInterval,
      Map<TsFileID, PatternTreeMap<Modification, PatternTreeMapFactory.ModsSerializer>>
          fileModCache,
      AtomicLong cachedModEntriesSize,
      Predicate<Modification> modificationMatcher,
      ModsTreeMatcher modsTreeMatcher) {
    this.resource = resource;
    this.memoryReservationManager = memoryReservationManager;
    this.modsCacheSizeLimitPerFI = modsCacheSizeLimitPerFI;
    this.modsMemoryEstimateReadInterval = modsMemoryEstimateReadInterval;
    this.fileModCache = fileModCache;
    this.cachedModEntriesSize = cachedModEntriesSize;
    this.modificationMatcher = modificationMatcher;
    this.modsTreeMatcher = modsTreeMatcher;
  }

  List<Modification> getPathModifications() throws IllegalPathException {
    AtomicReference<LoadModsResult> loadedResult = new AtomicReference<>();
    PatternTreeMap<Modification, PatternTreeMapFactory.ModsSerializer> cachedMods =
        fileModCache.computeIfAbsent(
            resource.getTsFileID(), ignored -> loadAllModificationsForCache(loadedResult));
    if (cachedMods != null) {
      return modsTreeMatcher.match(cachedMods);
    }

    LoadModsResult result = loadedResult.get();
    try {
      if (result.loadedAllModEntries) {
        return fallbackByMatchLoadedPatternTree(result);
      } else {
        return fallbackByMatchedScan(result);
      }
    } finally {
      close();
    }
  }

  private PatternTreeMap<Modification, PatternTreeMapFactory.ModsSerializer>
      loadAllModificationsForCache(AtomicReference<LoadModsResult> loadedResult) {
    LoadModsResult result = loadAllModificationsWithQuotaControl();
    loadedResult.set(result);
    if (!result.cacheable) {
      return null;
    }

    closeCurrentIterator();
    return result.mods;
  }

  private LoadModsResult loadAllModificationsWithQuotaControl() {
    PatternTreeMap<Modification, PatternTreeMapFactory.ModsSerializer> modifications =
        PatternTreeMapFactory.getModsPatternTreeMap();
    LoadModsResult result = new LoadModsResult(modifications);
    if (resource.getModFile().getSize() > getRemainingCacheQuota()) {
      currentIterator = resource.getModFile().getModificationsIter();
      result.loadedAllModEntries = false;
      result.cacheable = false;
      return result;
    }

    currentIterator = resource.getModFile().getModificationsIter();

    int appendedModCount = 0;
    boolean estimatedAfterLastAppend = false;

    while (currentIterator.hasNext()) {
      Modification modification = currentIterator.next();
      modifications.append(modification.getPath(), modification);
      appendedModCount++;
      estimatedAfterLastAppend = false;

      if (appendedModCount % modsMemoryEstimateReadInterval == 0) {
        if (!tryEstimateAndReserveTreeMemory(result)) {
          result.loadedAllModEntries = false;
          result.cacheable = false;
          return result;
        }
        estimatedAfterLastAppend = true;
      }
    }

    if (!estimatedAfterLastAppend) {
      result.cacheable = tryEstimateAndReserveTreeMemory(result);
    } else {
      result.cacheable = true;
    }

    result.loadedAllModEntries = true;
    return result;
  }

  private boolean tryEstimateAndReserveTreeMemory(LoadModsResult result) {
    long currentEstimatedSize = estimateModsTreeMemory(result.mods);
    long delta = currentEstimatedSize - result.reservedTreeMemoryBytes;
    if (delta < 0) {
      throw new IllegalStateException(
          String.format(
              "Estimated mods tree size decreased from %d to %d for TsFile %s.",
              result.reservedTreeMemoryBytes, currentEstimatedSize, resource));
    }
    if (delta == 0) {
      return true;
    }

    if (!tryClaimCacheQuota(delta)) {
      return false;
    }
    result.cacheQuotaBytes += delta;

    try {
      memoryReservationManager.reserveMemoryImmediately(delta);
    } catch (MemoryNotEnoughException e) {
      return false;
    }

    result.reservedTreeMemoryBytes = currentEstimatedSize;
    return true;
  }

  private boolean tryClaimCacheQuota(long delta) {
    if (delta <= 0) {
      return true;
    }

    long alreadyUsedMemoryForCachedModEntries = cachedModEntriesSize.get();
    while (alreadyUsedMemoryForCachedModEntries + delta <= modsCacheSizeLimitPerFI) {
      if (cachedModEntriesSize.compareAndSet(
          alreadyUsedMemoryForCachedModEntries, alreadyUsedMemoryForCachedModEntries + delta)) {
        return true;
      }
      alreadyUsedMemoryForCachedModEntries = cachedModEntriesSize.get();
    }
    return false;
  }

  private long getRemainingCacheQuota() {
    return modsCacheSizeLimitPerFI - cachedModEntriesSize.get();
  }

  private List<Modification> fallbackByMatchedScan(LoadModsResult partialTree)
      throws IllegalPathException {
    List<Modification> matchedMods = matchLoadedTreeAndRelease(partialTree);
    long reservedMatchedModsMemoryBytes = reserveMatchedModsMemory(matchedMods);
    int matchedModCount = matchedMods.size();

    while (currentIterator.hasNext()) {
      Modification modification = currentIterator.next();
      if (modificationMatcher.test(modification)) {
        matchedMods.add(modification);
        matchedModCount++;
        if (matchedModCount % modsMemoryEstimateReadInterval == 0) {
          reservedMatchedModsMemoryBytes =
              reserveMatchedModsMemoryIncrementally(matchedMods, reservedMatchedModsMemoryBytes);
        }
      }
    }

    List<Modification> sortedAndMergedMods = ModificationFile.sortAndMerge(matchedMods);
    adjustMatchedModsMemoryReservation(sortedAndMergedMods, reservedMatchedModsMemoryBytes);
    return sortedAndMergedMods;
  }

  private List<Modification> fallbackByMatchLoadedPatternTree(LoadModsResult loadedTree)
      throws IllegalPathException {
    List<Modification> matchedMods = matchLoadedTreeAndRelease(loadedTree);
    reserveMatchedModsMemory(matchedMods);
    return matchedMods;
  }

  private List<Modification> matchLoadedTreeAndRelease(LoadModsResult loadedTree)
      throws IllegalPathException {
    try {
      return new ArrayList<>(modsTreeMatcher.match(loadedTree.mods));
    } finally {
      loadedTree.mods = null;
      cachedModEntriesSize.addAndGet(-loadedTree.cacheQuotaBytes);
      loadedTree.cacheQuotaBytes = 0;
      memoryReservationManager.releaseMemoryCumulatively(loadedTree.reservedTreeMemoryBytes);
      loadedTree.reservedTreeMemoryBytes = 0;
    }
  }

  private long reserveMatchedModsMemory(List<Modification> matchedMods) {
    long estimatedSize = RamUsageEstimator.sizeOfArrayList(matchedMods);
    memoryReservationManager.reserveMemoryCumulatively(estimatedSize);
    return estimatedSize;
  }

  private long reserveMatchedModsMemoryIncrementally(
      List<Modification> matchedMods, long reservedMatchedModsMemoryBytes) {
    long currentEstimatedSize = RamUsageEstimator.sizeOfArrayList(matchedMods);
    long delta = currentEstimatedSize - reservedMatchedModsMemoryBytes;
    memoryReservationManager.reserveMemoryCumulatively(delta);
    return currentEstimatedSize;
  }

  private void adjustMatchedModsMemoryReservation(
      List<Modification> matchedMods, long reservedMatchedModsMemoryBytes) {
    long currentEstimatedSize = RamUsageEstimator.sizeOfArrayList(matchedMods);
    long delta = currentEstimatedSize - reservedMatchedModsMemoryBytes;
    if (delta >= 0) {
      memoryReservationManager.reserveMemoryCumulatively(delta);
    } else {
      memoryReservationManager.releaseMemoryCumulatively(-delta);
    }
  }

  private long estimateModsTreeMemory(
      PatternTreeMap<Modification, PatternTreeMapFactory.ModsSerializer> modifications) {
    return RamUsageEstimator.sizeOfObject(modifications)
        + RamUsageEstimator.SHALLOW_SIZE_OF_CONCURRENT_HASHMAP_ENTRY;
  }

  @Override
  public void close() {
    closeCurrentIterator();
  }

  private void closeCurrentIterator() {
    if (currentIterator != null) {
      currentIterator.close();
      currentIterator = null;
    }
  }

  private static class LoadModsResult {

    private PatternTreeMap<Modification, PatternTreeMapFactory.ModsSerializer> mods;
    private long cacheQuotaBytes;
    private long reservedTreeMemoryBytes;
    private boolean loadedAllModEntries;
    private boolean cacheable;

    private LoadModsResult(
        PatternTreeMap<Modification, PatternTreeMapFactory.ModsSerializer> mods) {
      this.mods = mods;
    }
  }

  @FunctionalInterface
  interface ModsTreeMatcher {

    List<Modification> match(
        PatternTreeMap<Modification, PatternTreeMapFactory.ModsSerializer> modsTree)
        throws IllegalPathException;
  }
}
