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

package org.apache.iotdb.db.storageengine.dataregion.wal.utils;

import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.pipe.metric.PipeWALInsertNodeCacheMetrics;
import org.apache.iotdb.db.pipe.resource.PipeDataNodeResourceManager;
import org.apache.iotdb.db.pipe.resource.memory.InsertNodeMemoryEstimator;
import org.apache.iotdb.db.pipe.resource.memory.PipeMemoryBlock;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertNode;
import org.apache.iotdb.db.storageengine.dataregion.wal.buffer.WALEntry;
import org.apache.iotdb.db.storageengine.dataregion.wal.buffer.WALEntryType;
import org.apache.iotdb.db.storageengine.dataregion.wal.io.WALByteBufReader;

import com.github.benmanes.caffeine.cache.CacheLoader;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.github.benmanes.caffeine.cache.Weigher;
import com.google.common.util.concurrent.AtomicDouble;
import org.apache.tsfile.utils.Pair;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

/** This cache is used by {@link WALEntryPosition}. */
public class WALInsertNodeCache {

  private static final Logger LOGGER = LoggerFactory.getLogger(WALInsertNodeCache.class);
  private static final IoTDBConfig CONFIG = IoTDBDescriptor.getInstance().getConfig();

  private final PipeMemoryBlock allocatedMemoryBlock;
  // Used to adjust the memory usage of the cache
  private final AtomicDouble memoryUsageCheatFactor = new AtomicDouble(1);
  private final AtomicBoolean isBatchLoadEnabled = new AtomicBoolean(true);
  // LRU cache, find Pair<ByteBuffer, InsertNode> by WALEntryPosition
  private final LoadingCache<WALEntryPosition, Pair<ByteBuffer, InsertNode>> lruCache;

  // ids of all pinned memTables
  private final Set<Long> memTablesNeedSearch = ConcurrentHashMap.newKeySet();

  private volatile boolean hasPipeRunning = false;

  private WALInsertNodeCache(final Integer dataRegionId) {
    final long requestedAllocateSize =
        (long)
            Math.min(
                (double) 2 * CONFIG.getWalFileSizeThresholdInByte(),
                CONFIG.getAllocateMemoryForPipe() * 0.8 / 5);
    allocatedMemoryBlock =
        PipeDataNodeResourceManager.memory()
            .tryAllocate(requestedAllocateSize)
            .setShrinkMethod(oldMemory -> Math.max(oldMemory / 2, 1))
            .setExpandMethod(
                oldMemory -> Math.min(Math.max(oldMemory, 1) * 2, requestedAllocateSize))
            .setExpandCallback(
                (oldMemory, newMemory) -> {
                  memoryUsageCheatFactor.updateAndGet(
                      factor -> factor / ((double) newMemory / oldMemory));
                  isBatchLoadEnabled.set(newMemory >= CONFIG.getWalFileSizeThresholdInByte());
                  LOGGER.info(
                      "WALInsertNodeCache.allocatedMemoryBlock of dataRegion {} has expanded from {} to {}.",
                      dataRegionId,
                      oldMemory,
                      newMemory);
                });
    isBatchLoadEnabled.set(
        allocatedMemoryBlock.getMemoryUsageInBytes() >= CONFIG.getWalFileSizeThresholdInByte());
    lruCache =
        Caffeine.newBuilder()
            .maximumWeight(allocatedMemoryBlock.getMemoryUsageInBytes())
            .weigher(
                (Weigher<WALEntryPosition, Pair<ByteBuffer, InsertNode>>)
                    (position, pair) -> {
                      long weightInLong = 0L;
                      if (pair.right != null) {
                        weightInLong =
                            (long)
                                (InsertNodeMemoryEstimator.sizeOf(pair.right)
                                    * memoryUsageCheatFactor.get());
                      } else {
                        weightInLong = (long) (position.getSize() * memoryUsageCheatFactor.get());
                      }
                      if (weightInLong <= 0) {
                        return Integer.MAX_VALUE;
                      }
                      final int weightInInt = (int) weightInLong;
                      return weightInInt != weightInLong ? Integer.MAX_VALUE : weightInInt;
                    })
            .recordStats()
            .build(new WALInsertNodeCacheLoader());
    allocatedMemoryBlock.setShrinkCallback(
        (oldMemory, newMemory) -> {
          memoryUsageCheatFactor.updateAndGet(factor -> factor * ((double) oldMemory / newMemory));
          isBatchLoadEnabled.set(newMemory >= CONFIG.getWalFileSizeThresholdInByte());
          LOGGER.info(
              "WALInsertNodeCache.allocatedMemoryBlock of dataRegion {} has shrunk from {} to {}.",
              dataRegionId,
              oldMemory,
              newMemory);
          if (CONFIG.getWALCacheShrinkClearEnabled()) {
            try {
              lruCache.cleanUp();
            } catch (Exception e) {
              LOGGER.warn(
                  "Failed to clear WALInsertNodeCache for dataRegion ID: {}.", dataRegionId, e);
              return;
            }
            LOGGER.info(
                "Successfully cleared WALInsertNodeCache for dataRegion ID: {}.", dataRegionId);
          }
        });
    PipeWALInsertNodeCacheMetrics.getInstance().register(this, dataRegionId);
  }

  /////////////////////////// Getter & Setter ///////////////////////////

  public InsertNode getInsertNode(final WALEntryPosition position) {
    final Pair<ByteBuffer, InsertNode> pair = getByteBufferOrInsertNode(position);

    if (pair.getRight() != null) {
      return pair.getRight();
    }

    if (pair.getLeft() == null) {
      throw new IllegalStateException();
    }

    try {
      // multi pipes may share the same wal entry, so we need to wrap the byte[] into
      // different ByteBuffer for each pipe
      final InsertNode insertNode = parse(ByteBuffer.wrap(pair.getLeft().array()));
      pair.setRight(insertNode);
      return insertNode;
    } catch (final Exception e) {
      LOGGER.error(
          "Parsing failed when recovering insertNode from wal, walFile:{}, position:{}, size:{}, exception:",
          position.getWalFile(),
          position.getPosition(),
          position.getSize(),
          e);
      throw e;
    }
  }

  private InsertNode parse(final ByteBuffer buffer) {
    final PlanNode node = WALEntry.deserializeForConsensus(buffer);
    if (node instanceof InsertNode) {
      return (InsertNode) node;
    } else {
      return null;
    }
  }

  public ByteBuffer getByteBuffer(final WALEntryPosition position) {
    Pair<ByteBuffer, InsertNode> pair = getByteBufferOrInsertNode(position);

    if (pair.getLeft() != null) {
      // multi pipes may share the same wal entry, so we need to wrap the byte[] into
      // different ByteBuffer for each pipe
      return ByteBuffer.wrap(pair.getLeft().array());
    }

    // forbid multi threads to invalidate and load the same entry
    synchronized (this) {
      lruCache.invalidate(position);
      pair = getByteBufferOrInsertNode(position);
    }

    if (pair.getLeft() == null) {
      throw new IllegalStateException();
    }

    return ByteBuffer.wrap(pair.getLeft().array());
  }

  public Pair<ByteBuffer, InsertNode> getByteBufferOrInsertNode(final WALEntryPosition position) {
    hasPipeRunning = true;

    final Pair<ByteBuffer, InsertNode> pair =
        isBatchLoadEnabled.get()
            ? lruCache.getAll(Collections.singleton(position)).get(position)
            : lruCache.get(position);

    if (pair == null) {
      throw new IllegalStateException();
    }

    return pair;
  }

  public void cacheInsertNodeIfNeeded(
      final WALEntryPosition walEntryPosition, final InsertNode insertNode) {
    // reduce memory usage
    if (hasPipeRunning) {
      lruCache.put(walEntryPosition, new Pair<>(null, insertNode));
    }
  }

  //////////////////////////// APIs provided for metric framework ////////////////////////////

  public double getCacheHitRate() {
    return Objects.nonNull(lruCache) ? lruCache.stats().hitRate() : 0;
  }

  public double getCacheHitCount() {
    return Objects.nonNull(lruCache) ? lruCache.stats().hitCount() : 0;
  }

  public double getCacheRequestCount() {
    return Objects.nonNull(lruCache) ? lruCache.stats().requestCount() : 0;
  }

  /////////////////////////// MemTable ///////////////////////////

  public void addMemTable(final long memTableId) {
    memTablesNeedSearch.add(memTableId);
  }

  public void removeMemTable(final long memTableId) {
    memTablesNeedSearch.remove(memTableId);
  }

  /////////////////////////// Cache Loader ///////////////////////////

  class WALInsertNodeCacheLoader
      implements CacheLoader<WALEntryPosition, Pair<ByteBuffer, InsertNode>> {

    @Override
    public @Nullable Pair<ByteBuffer, InsertNode> load(@NonNull final WALEntryPosition key)
        throws Exception {
      return new Pair<>(key.read(), null);
    }

    /** Batch load all wal entries in the file when any one key is absent. */
    @Override
    public @NonNull Map<@NonNull WALEntryPosition, @NonNull Pair<ByteBuffer, InsertNode>> loadAll(
        @NonNull final Iterable<? extends @NonNull WALEntryPosition> walEntryPositions) {
      final Map<WALEntryPosition, Pair<ByteBuffer, InsertNode>> loadedEntries = new HashMap<>();

      for (final WALEntryPosition walEntryPosition : walEntryPositions) {
        if (loadedEntries.containsKey(walEntryPosition) || !walEntryPosition.canRead()) {
          continue;
        }

        final long walFileVersionId = walEntryPosition.getWalFileVersionId();

        // load one when wal file is not sealed
        if (!walEntryPosition.isInSealedFile()) {
          try {
            loadedEntries.put(walEntryPosition, load(walEntryPosition));
          } catch (final Exception e) {
            LOGGER.info(
                "Fail to cache wal entries from the wal file with version id {}",
                walFileVersionId,
                e);
          }
          continue;
        }

        // batch load when wal file is sealed
        long position = 0;
        try (final WALByteBufReader walByteBufReader = new WALByteBufReader(walEntryPosition)) {
          while (walByteBufReader.hasNext()) {
            // see WALInfoEntry#serialize, entry type + memtable id + plan node type
            final ByteBuffer buffer = walByteBufReader.next();

            final int size = buffer.capacity();
            final WALEntryType type = WALEntryType.valueOf(buffer.get());
            final long memTableId = buffer.getLong();

            if ((memTablesNeedSearch.contains(memTableId)
                    || walEntryPosition.getPosition() == position)
                && type.needSearch()) {
              buffer.clear();
              loadedEntries.put(
                  new WALEntryPosition(
                      walEntryPosition.getIdentifier(), walFileVersionId, position, size),
                  new Pair<>(buffer, null));
            }

            position += size;
          }
        } catch (final IOException e) {
          LOGGER.info(
              "Fail to cache wal entries from the wal file with version id {}",
              walFileVersionId,
              e);
        }
      }

      return loadedEntries;
    }
  }

  /////////////////////////// Singleton ///////////////////////////

  public static WALInsertNodeCache getInstance(final Integer regionId) {
    return InstanceHolder.getOrCreateInstance(regionId);
  }

  private static class InstanceHolder {

    private static final Map<Integer, WALInsertNodeCache> INSTANCE_MAP = new ConcurrentHashMap<>();

    public static WALInsertNodeCache getOrCreateInstance(final Integer key) {
      return INSTANCE_MAP.computeIfAbsent(key, k -> new WALInsertNodeCache(key));
    }

    private InstanceHolder() {
      // forbidding instantiation
    }
  }

  /////////////////////////// Test Only ///////////////////////////

  @TestOnly
  public boolean isBatchLoadEnabled() {
    return isBatchLoadEnabled.get();
  }

  @TestOnly
  public void setIsBatchLoadEnabled(final boolean isBatchLoadEnabled) {
    this.isBatchLoadEnabled.set(isBatchLoadEnabled);
  }

  @TestOnly
  boolean contains(WALEntryPosition position) {
    return lruCache.getIfPresent(position) != null;
  }

  @TestOnly
  public void clear() {
    lruCache.invalidateAll();
    allocatedMemoryBlock.close();
    memTablesNeedSearch.clear();
  }
}
