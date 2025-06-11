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

import org.apache.iotdb.commons.pipe.config.PipeConfig;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.db.conf.DataNodeMemoryConfig;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.pipe.resource.PipeDataNodeResourceManager;
import org.apache.iotdb.db.pipe.resource.memory.InsertNodeMemoryEstimator;
import org.apache.iotdb.db.pipe.resource.memory.PipeMemoryBlockType;
import org.apache.iotdb.db.pipe.resource.memory.PipeModelFixedMemoryBlock;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertNode;
import org.apache.iotdb.db.storageengine.dataregion.wal.buffer.WALEntry;
import org.apache.iotdb.db.storageengine.dataregion.wal.buffer.WALEntryType;
import org.apache.iotdb.db.storageengine.dataregion.wal.io.WALByteBufReader;

import com.github.benmanes.caffeine.cache.CacheLoader;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.github.benmanes.caffeine.cache.Weigher;
import org.apache.tsfile.utils.Pair;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/** This cache is used by {@link WALEntryPosition}. */
public class WALInsertNodeCache {

  private static final Logger LOGGER = LoggerFactory.getLogger(WALInsertNodeCache.class);
  private static final IoTDBConfig CONFIG = IoTDBDescriptor.getInstance().getConfig();
  private static final DataNodeMemoryConfig MEMORY_CONFIG =
      IoTDBDescriptor.getInstance().getMemoryConfig();
  private static final PipeConfig PIPE_CONFIG = PipeConfig.getInstance();

  private static PipeModelFixedMemoryBlock walModelFixedMemory = null;

  // LRU cache, find Pair<ByteBuffer, InsertNode> by WALEntryPosition
  private final LoadingCache<WALEntryPosition, Pair<ByteBuffer, InsertNode>> lruCache;

  // ids of all pinned memTables
  private final Set<Long> memTablesNeedSearch = ConcurrentHashMap.newKeySet();

  private volatile boolean hasPipeRunning = false;

  private WALInsertNodeCache() {
    if (walModelFixedMemory == null) {
      init();
    }

    final long requestedAllocateSize =
        (long)
            (PipeDataNodeResourceManager.memory().getTotalNonFloatingMemorySizeInBytes()
                * PIPE_CONFIG.getPipeDataStructureWalMemoryProportion());

    lruCache =
        Caffeine.newBuilder()
            .maximumWeight(requestedAllocateSize)
            .weigher(
                (Weigher<WALEntryPosition, Pair<ByteBuffer, InsertNode>>)
                    (position, pair) -> {
                      long weightInLong = 0L;
                      if (pair.right != null) {
                        weightInLong = InsertNodeMemoryEstimator.sizeOf(pair.right);
                      } else {
                        weightInLong = position.getSize();
                      }
                      if (weightInLong <= 0) {
                        return Integer.MAX_VALUE;
                      }
                      final int weightInInt = (int) weightInLong;
                      return weightInInt != weightInLong ? Integer.MAX_VALUE : weightInInt;
                    })
            .recordStats()
            .build(new WALInsertNodeCacheLoader());
  }

  // please call this method at PipeLauncher
  public static void init() {
    if (walModelFixedMemory != null) {
      return;
    }
    try {
      // Allocate memory for the fixed memory block of WAL
      walModelFixedMemory =
          PipeDataNodeResourceManager.memory()
              .forceAllocateForModelFixedMemoryBlock(
                  (long)
                      (PipeDataNodeResourceManager.memory().getTotalNonFloatingMemorySizeInBytes()
                          * PIPE_CONFIG.getPipeDataStructureWalMemoryProportion()),
                  PipeMemoryBlockType.WAL);
    } catch (Exception e) {
      LOGGER.error("Failed to initialize WAL model fixed memory block", e);
      walModelFixedMemory =
          PipeDataNodeResourceManager.memory()
              .forceAllocateForModelFixedMemoryBlock(0, PipeMemoryBlockType.WAL);
    }
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

    final Pair<ByteBuffer, InsertNode> pair = lruCache.get(position);

    if (pair == null) {
      throw new IllegalStateException();
    }

    return pair;
  }

  public Pair<ByteBuffer, InsertNode> getByteBufferOrInsertNodeIfPossible(
      final WALEntryPosition position) {
    hasPipeRunning = true;
    return lruCache.getIfPresent(position);
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

  public static WALInsertNodeCache getInstance() {
    return InstanceHolder.INSTANCE;
  }

  private static class InstanceHolder {

    public static final WALInsertNodeCache INSTANCE = new WALInsertNodeCache();

    private InstanceHolder() {
      // forbidding instantiation
    }
  }

  /////////////////////////// Test Only ///////////////////////////

  @TestOnly
  boolean contains(WALEntryPosition position) {
    return lruCache.getIfPresent(position) != null;
  }

  @TestOnly
  public void clear() {
    lruCache.invalidateAll();
    memTablesNeedSearch.clear();
  }
}
