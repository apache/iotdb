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
    AtomicReference<LoadedModsResult> loadedResult = new AtomicReference<>();
    PatternTreeMap<ModEntry, PatternTreeMapFactory.ModsSerializer> cachedMods =
        fileModCache.computeIfAbsent(
            resource.getTsFileID(), ignored -> loadAllModificationsForCache(loadedResult));
    if (cachedMods != null) {
      // Either this loader cached the full tree successfully, or another loader had cached it.
      return modsTreeMatcher.match(cachedMods);
    }

    LoadedModsResult result = loadedResult.get();
    if (result.loadedAllModEntries) {
      return fallbackByMatchLoadedPatternTree(result);
    } else {
      return fallbackByMatchedScan(result);
    }
  }

  private PatternTreeMap<ModEntry, PatternTreeMapFactory.ModsSerializer>
      loadAllModificationsForCache(AtomicReference<LoadedModsResult> loadedResult) {
    LoadedModsResult result = loadAllModificationsWithQuotaControl();
    loadedResult.set(result);
    if (!result.loadedAllModEntries) {
      return null;
    }

    try {
      // The cache quota has already been claimed while loading. Reserve real query memory only
      // once, just before the fully loaded tree becomes visible in fileModCache.
      reserveMemoryImmediately(result.cacheQuotaBytes);
    } catch (MemoryNotEnoughException e) {
      return null;
    }

    closeCurrentIterator();
    return result.mods;
  }

  private LoadedModsResult loadAllModificationsWithQuotaControl() {
    PatternTreeMap<ModEntry, PatternTreeMapFactory.ModsSerializer> modifications =
        PatternTreeMapFactory.getModsPatternTreeMap();
    currentIterator = resource.getModEntryIterator();

    long claimedCacheQuotaBytes = 0;
    int readModCount = 0;
    boolean estimatedAfterLastAppend = false;

    while (currentIterator.hasNext()) {
      ModEntry modification = currentIterator.next();
      readModCount++;
      if (!queryContext.shouldSkipModification(modification)) {
        modifications.append(modification.keyOfPatternTree(), modification);
        estimatedAfterLastAppend = false;
      }

      if (readModCount % modsMemoryEstimateReadInterval == 0) {
        long currentEstimatedSize = estimateModsTreeMemory(modifications);
        checkEstimatedSizeNotDecreased(claimedCacheQuotaBytes, currentEstimatedSize);
        long delta = currentEstimatedSize - claimedCacheQuotaBytes;
        if (!tryClaimCacheQuota(delta)) {
          return new LoadedModsResult(modifications, claimedCacheQuotaBytes, false);
        }
        claimedCacheQuotaBytes = currentEstimatedSize;
        estimatedAfterLastAppend = true;
      }
    }

    if (!estimatedAfterLastAppend) {
      long finalEstimatedSize = estimateModsTreeMemory(modifications);
      checkEstimatedSizeNotDecreased(claimedCacheQuotaBytes, finalEstimatedSize);
      long delta = finalEstimatedSize - claimedCacheQuotaBytes;
      if (!tryClaimCacheQuota(delta)) {
        return new LoadedModsResult(modifications, claimedCacheQuotaBytes, false);
      }
      claimedCacheQuotaBytes = finalEstimatedSize;
    }
    return new LoadedModsResult(modifications, claimedCacheQuotaBytes, true);
  }

  private void checkEstimatedSizeNotDecreased(
      long previousEstimatedSize, long currentEstimatedSize) {
    if (currentEstimatedSize < previousEstimatedSize) {
      throw new IllegalStateException(
          String.format(
              DataNodeQueryMessages.ESTIMATED_MODS_TREE_SIZE_DECREASED,
              previousEstimatedSize,
              currentEstimatedSize,
              resource));
    }
  }

  private boolean tryClaimCacheQuota(long delta) {
    if (delta <= 0) {
      return true;
    }

    // During loading, this only claims FI-level cache quota. Real memory is reserved once after the
    // full tree is loaded and before computeIfAbsent publishes it to fileModCache.
    long alreadyUsedMemoryForCachedModEntries = cachedModEntriesSize.get();
    while (alreadyUsedMemoryForCachedModEntries + delta < modsCacheSizeLimitPerFI) {
      if (cachedModEntriesSize.compareAndSet(
          alreadyUsedMemoryForCachedModEntries, alreadyUsedMemoryForCachedModEntries + delta)) {
        return true;
      }
      alreadyUsedMemoryForCachedModEntries = cachedModEntriesSize.get();
    }
    return false;
  }

  private List<ModEntry> fallbackByMatchedScan(LoadedModsResult partialTree)
      throws IllegalPathException {
    try {
      // The full tree exceeded the FI mods cache quota. Reuse the partial tree for the part that
      // has already been read, then continue scanning the same iterator by path.
      List<ModEntry> matchedMods;
      try {
        matchedMods = new ArrayList<>(modsTreeMatcher.match(partialTree.mods));
      } finally {
        partialTree.mods = null;
        releaseClaimedCacheQuota(partialTree);
      }
      reserveMatchedModsMemory(matchedMods);

      while (currentIterator.hasNext()) {
        ModEntry modification = currentIterator.next();
        if (queryContext.shouldSkipModification(modification)) {
          continue;
        }
        if (modificationMatcher.test(modification)) {
          reserveMatchedModMemory(modification);
          matchedMods.add(modification);
        }
      }

      matchedMods = ModificationUtils.sortAndMerge(matchedMods);
      return matchedMods;
    } finally {
      close();
    }
  }

  private List<ModEntry> fallbackByMatchLoadedPatternTree(LoadedModsResult loadedTree)
      throws IllegalPathException {
    try {
      // The tree was fully loaded but not cached, usually because final memory reservation failed.
      // Return only the matched mods and release the cache quota claimed during loading.
      List<ModEntry> matchedMods;
      try {
        matchedMods = new ArrayList<>(modsTreeMatcher.match(loadedTree.mods));
      } finally {
        loadedTree.mods = null;
        releaseClaimedCacheQuota(loadedTree);
      }
      reserveMatchedModsMemory(matchedMods);
      return matchedMods;
    } finally {
      close();
    }
  }

  private void reserveMatchedModsMemory(List<ModEntry> matchedMods) {
    reserveMemoryImmediately(RamUsageEstimator.shallowSizeOf(matchedMods));
    for (ModEntry matchedMod : matchedMods) {
      reserveMatchedModMemory(matchedMod);
    }
  }

  private void reserveMatchedModMemory(ModEntry matchedMod) {
    reserveMemoryImmediately(RamUsageEstimator.NUM_BYTES_OBJECT_REF + matchedMod.ramBytesUsed());
  }

  private void reserveMemoryImmediately(long bytes) {
    if (bytes > 0) {
      synchronized (memoryReservationManager) {
        memoryReservationManager.reserveMemoryImmediately(bytes);
      }
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

  private void releaseClaimedCacheQuota(LoadedModsResult loadedResult) {
    if (loadedResult.cacheQuotaBytes > 0) {
      cachedModEntriesSize.addAndGet(-loadedResult.cacheQuotaBytes);
      loadedResult.cacheQuotaBytes = 0;
    }
  }

  private static class LoadedModsResult {

    private PatternTreeMap<ModEntry, PatternTreeMapFactory.ModsSerializer> mods;
    private long cacheQuotaBytes;
    private final boolean loadedAllModEntries;

    private LoadedModsResult(
        PatternTreeMap<ModEntry, PatternTreeMapFactory.ModsSerializer> mods,
        long cacheQuotaBytes,
        boolean loadedAllModEntries) {
      this.mods = mods;
      this.cacheQuotaBytes = cacheQuotaBytes;
      this.loadedAllModEntries = loadedAllModEntries;
    }
  }

  @FunctionalInterface
  interface ModsTreeMatcher {

    List<ModEntry> match(PatternTreeMap<ModEntry, PatternTreeMapFactory.ModsSerializer> modsTree)
        throws IllegalPathException;
  }
}
