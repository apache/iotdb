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

import org.apache.iotdb.calc.exception.MemoryNotEnoughException;
import org.apache.iotdb.calc.plan.planner.memory.MemoryReservationManager;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.PatternTreeMap;
import org.apache.iotdb.db.i18n.DataNodeQueryMessages;
import org.apache.iotdb.db.storageengine.dataregion.modification.ModEntry;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileID;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.utils.ModificationUtils;
import org.apache.iotdb.db.utils.datastructure.PatternTreeMapFactory;

import org.apache.tsfile.utils.RamUsageEstimator;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;

class QueryModificationLoader implements AutoCloseable {

  private final QueryContext queryContext;
  private final TsFileResource resource;
  private final MemoryReservationManager memoryReservationManager;
  private final long modsCacheSizeLimitPerFI;
  private final int modsMemoryEstimateReadInterval;
  private final Map<TsFileID, PatternTreeMap<ModEntry, PatternTreeMapFactory.ModsSerializer>>
      fileModCache;
  private final AtomicLong cachedModEntriesSize;
  private final Predicate<ModEntry> modificationMatcher;
  private final ModsTreeMatcher modsTreeMatcher;

  private TsFileResource.ModIterator currentIterator;

  QueryModificationLoader(
      QueryContext queryContext,
      TsFileResource resource,
      MemoryReservationManager memoryReservationManager,
      long modsCacheSizeLimitPerFI,
      int modsMemoryEstimateReadInterval,
      Map<TsFileID, PatternTreeMap<ModEntry, PatternTreeMapFactory.ModsSerializer>> fileModCache,
      AtomicLong cachedModEntriesSize,
      Predicate<ModEntry> modificationMatcher,
      ModsTreeMatcher modsTreeMatcher) {
    this.queryContext = queryContext;
    this.resource = resource;
    this.memoryReservationManager = memoryReservationManager;
    this.modsCacheSizeLimitPerFI = modsCacheSizeLimitPerFI;
    this.modsMemoryEstimateReadInterval = modsMemoryEstimateReadInterval;
    this.fileModCache = fileModCache;
    this.cachedModEntriesSize = cachedModEntriesSize;
    this.modificationMatcher = modificationMatcher;
    this.modsTreeMatcher = modsTreeMatcher;
  }

  List<ModEntry> getPathModifications() throws IllegalPathException {
    AtomicReference<LoadModsResult> loadedResult = new AtomicReference<>();
    PatternTreeMap<ModEntry, PatternTreeMapFactory.ModsSerializer> cachedMods =
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

  private PatternTreeMap<ModEntry, PatternTreeMapFactory.ModsSerializer>
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
    PatternTreeMap<ModEntry, PatternTreeMapFactory.ModsSerializer> modifications =
        PatternTreeMapFactory.getModsPatternTreeMap();
    LoadModsResult result = new LoadModsResult(modifications);
    if (resource.getTotalModSizeInByte() > getRemainingCacheQuota()) {
      currentIterator = resource.getModEntryIterator();
      result.loadedAllModEntries = false;
      result.cacheable = false;
      return result;
    }

    currentIterator = resource.getModEntryIterator();

    int appendedModCount = 0;
    boolean estimatedAfterLastAppend = false;

    while (currentIterator.hasNext()) {
      ModEntry modification = currentIterator.next();
      if (queryContext.shouldSkipModification(modification)) {
        continue;
      }

      modifications.append(modification.keyOfPatternTree(), modification);
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
              DataNodeQueryMessages.ESTIMATED_MODS_TREE_SIZE_DECREASED,
              result.reservedTreeMemoryBytes,
              currentEstimatedSize,
              resource));
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

  private List<ModEntry> fallbackByMatchedScan(LoadModsResult partialTree)
      throws IllegalPathException {
    List<ModEntry> matchedMods = matchLoadedTreeAndRelease(partialTree);
    long reservedMatchedModsMemoryBytes = reserveMatchedModsMemory(matchedMods);
    int matchedModCount = matchedMods.size();

    while (currentIterator.hasNext()) {
      ModEntry modification = currentIterator.next();
      if (queryContext.shouldSkipModification(modification)) {
        continue;
      }
      if (modificationMatcher.test(modification)) {
        matchedMods.add(modification);
        matchedModCount++;
        if (matchedModCount % modsMemoryEstimateReadInterval == 0) {
          reservedMatchedModsMemoryBytes =
              reserveMatchedModsMemoryIncrementally(matchedMods, reservedMatchedModsMemoryBytes);
        }
      }
    }

    List<ModEntry> sortedAndMergedMods = ModificationUtils.sortAndMerge(matchedMods);
    adjustMatchedModsMemoryReservation(sortedAndMergedMods, reservedMatchedModsMemoryBytes);
    return sortedAndMergedMods;
  }

  private List<ModEntry> fallbackByMatchLoadedPatternTree(LoadModsResult loadedTree)
      throws IllegalPathException {
    List<ModEntry> matchedMods = matchLoadedTreeAndRelease(loadedTree);
    reserveMatchedModsMemory(matchedMods);
    return matchedMods;
  }

  private List<ModEntry> matchLoadedTreeAndRelease(LoadModsResult loadedTree)
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

  private long reserveMatchedModsMemory(List<ModEntry> matchedMods) {
    long estimatedSize = RamUsageEstimator.sizeOfArrayList(matchedMods);
    memoryReservationManager.reserveMemoryCumulatively(estimatedSize);
    return estimatedSize;
  }

  private long reserveMatchedModsMemoryIncrementally(
      List<ModEntry> matchedMods, long reservedMatchedModsMemoryBytes) {
    long currentEstimatedSize = RamUsageEstimator.sizeOfArrayList(matchedMods);
    long delta = currentEstimatedSize - reservedMatchedModsMemoryBytes;
    memoryReservationManager.reserveMemoryCumulatively(delta);
    return currentEstimatedSize;
  }

  private void adjustMatchedModsMemoryReservation(
      List<ModEntry> matchedMods, long reservedMatchedModsMemoryBytes) {
    long currentEstimatedSize = RamUsageEstimator.sizeOfArrayList(matchedMods);
    long delta = currentEstimatedSize - reservedMatchedModsMemoryBytes;
    if (delta >= 0) {
      memoryReservationManager.reserveMemoryCumulatively(delta);
    } else {
      memoryReservationManager.releaseMemoryCumulatively(-delta);
    }
  }

  private long estimateModsTreeMemory(
      PatternTreeMap<ModEntry, PatternTreeMapFactory.ModsSerializer> modifications) {
    return RamUsageEstimator.sizeOfObject(modifications);
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

    private PatternTreeMap<ModEntry, PatternTreeMapFactory.ModsSerializer> mods;
    private long cacheQuotaBytes;
    private long reservedTreeMemoryBytes;
    private boolean loadedAllModEntries;
    private boolean cacheable;

    private LoadModsResult(PatternTreeMap<ModEntry, PatternTreeMapFactory.ModsSerializer> mods) {
      this.mods = mods;
    }
  }

  @FunctionalInterface
  interface ModsTreeMatcher {

    List<ModEntry> match(PatternTreeMap<ModEntry, PatternTreeMapFactory.ModsSerializer> modsTree)
        throws IllegalPathException;
  }
}
