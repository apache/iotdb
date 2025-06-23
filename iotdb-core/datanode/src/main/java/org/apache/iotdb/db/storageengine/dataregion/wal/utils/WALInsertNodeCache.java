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
import org.apache.iotdb.db.pipe.resource.PipeDataNodeResourceManager;
import org.apache.iotdb.db.pipe.resource.memory.InsertNodeMemoryEstimator;
import org.apache.iotdb.db.pipe.resource.memory.PipeMemoryBlockType;
import org.apache.iotdb.db.pipe.resource.memory.PipeModelFixedMemoryBlock;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertNode;
import org.apache.iotdb.db.storageengine.dataregion.wal.buffer.WALEntry;
import org.apache.iotdb.db.storageengine.dataregion.wal.buffer.WALEntryType;
import org.apache.iotdb.db.storageengine.dataregion.wal.io.WALByteBufReader;

import com.github.benmanes.caffeine.cache.Cache;
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
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/** This cache is used by {@link WALEntryPosition}. */
public class WALInsertNodeCache {

  private static final Logger LOGGER = LoggerFactory.getLogger(WALInsertNodeCache.class);
  private static final PipeConfig PIPE_CONFIG = PipeConfig.getInstance();

  private static PipeModelFixedMemoryBlock walModelFixedMemory = null;

  // LRU cache, find ByteBuffer or InsertNode by WALEntryPosition
  private final LoadingCache<WALEntryPosition, ByteBuffer> bufferCache;

  private final Cache<WALEntryPosition, InsertNode> insertNodeCache;

  // ids of all pinned memTables
  private final Set<Long> memTablesNeedSearch = ConcurrentHashMap.newKeySet();

  private volatile boolean hasPipeRunning = false;
  private long batchLoadSize = 1 << 12;

  private WALInsertNodeCache() {
    if (walModelFixedMemory == null) {
      init();
    }

    final long requestedAllocateSize = walModelFixedMemory.getMemoryUsageInBytes();

    bufferCache =
        Caffeine.newBuilder()
            .maximumWeight(requestedAllocateSize / 2)
            .weigher(
                (Weigher<WALEntryPosition, ByteBuffer>)
                    (position, buffer) -> {
                      return position.getSize();
                    })
            .recordStats()
            .build(new WALInsertNodeCacheLoader());

    insertNodeCache =
        Caffeine.newBuilder()
            .maximumWeight(requestedAllocateSize / 2)
            .weigher(
                (Weigher<WALEntryPosition, InsertNode>)
                    (position, insertNode) -> {
                      long weightInLong = InsertNodeMemoryEstimator.sizeOf(insertNode);
                      if (weightInLong <= 0) {
                        return Integer.MAX_VALUE;
                      }
                      final int weightInInt = (int) weightInLong;
                      return weightInInt != weightInLong ? Integer.MAX_VALUE : weightInInt;
                    })
            .build();
  }

  // please call this method at PipeLauncher
  public static void init() {
    if (walModelFixedMemory != null) {
      return;
    }

    Exception e = null;
    // Allocate memory for the fixed memory block of WAL. If the allocation fails, try again until
    // the appropriate memory is allocated.
    for (long i = PipeDataNodeResourceManager.memory().getAllocatedMemorySizeInBytesOfWAL();
        i > 0;
        i = i / 2) {
      try {
        // Allocate memory for the fixed memory block of WAL
        walModelFixedMemory =
            PipeDataNodeResourceManager.memory()
                .forceAllocateForModelFixedMemoryBlock(i, PipeMemoryBlockType.WAL);
        LOGGER.info("Successfully initialized WAL model fixed memory block, size: {}", i);
        break;
      } catch (Exception exception) {
        e = exception;
      }
    }

    // If the allocation fails, we will log an error and force allocate a memory block of size 0.
    if (walModelFixedMemory == null) {
      LOGGER.error("Failed to initialize WAL model fixed memory block", e);
      walModelFixedMemory =
          PipeDataNodeResourceManager.memory()
              .forceAllocateForModelFixedMemoryBlock(0, PipeMemoryBlockType.WAL);
    }
  }

  /////////////////////////// Getter & Setter ///////////////////////////

  public InsertNode getInsertNodeIfPossible(final WALEntryPosition position) {
    hasPipeRunning = true;
    return insertNodeCache.getIfPresent(position);
  }

  public InsertNode getInsertNode(final WALEntryPosition position) {
    InsertNode insertNode = getInsertNodeIfPossible(position);

    if (insertNode != null) {
      return insertNode;
    }

    ByteBuffer buffer = bufferCache.getIfPresent(position);

    if (buffer == null) {
      throw new IllegalStateException();
    }

    try {
      // multi pipes may share the same wal entry, so we need to wrap the byte[] into
      // different ByteBuffer for each pipe
      insertNode = parse(ByteBuffer.wrap(buffer.array()));
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
    final ByteBuffer buffer = getByteBufferIfPossible(position);

    if (buffer == null) {
      throw new IllegalStateException();
    }

    return buffer;
  }

  public ByteBuffer getByteBufferIfPossible(final WALEntryPosition position) {
    hasPipeRunning = true;
    return PIPE_CONFIG.getWALCacheBatchLoadEnabled()
        ? bufferCache.getAll(Collections.singleton(position)).get(position)
        : bufferCache.getIfPresent(position);
  }

  public void cacheInsertNodeIfNeeded(
      final WALEntryPosition walEntryPosition, final InsertNode insertNode) {
    // reduce memory usage
    if (hasPipeRunning) {
      insertNodeCache.put(walEntryPosition, insertNode);
    }
  }

  public Pair<ByteBuffer, InsertNode> getByteBufferOrInsertNodeIfPossible(
      final WALEntryPosition position) {
    final InsertNode insertNode = getInsertNodeIfPossible(position);

    if (insertNode != null) {
      // multi pipes may share the same wal entry, so we need to wrap the byte[] into
      // different ByteBuffer for each pipe
      return new Pair<>(null, insertNode);
    }

    // forbid multi threads to invalidate and load the same entry
    final ByteBuffer byteBuffer;
    synchronized (this) {
      byteBuffer = getByteBufferIfPossible(position);
    }

    if (byteBuffer == null) {
      throw new IllegalStateException();
    }

    return new Pair<>(ByteBuffer.wrap(byteBuffer.array()), null);
  }

  //////////////////////////// APIs provided for metric framework ////////////////////////////

  public double getInsertNodeCacheHitRate() {
    return Objects.nonNull(bufferCache) ? insertNodeCache.stats().hitRate() : 0;
  }

  public double getInsertNodeCacheHitCount() {
    return Objects.nonNull(bufferCache) ? insertNodeCache.stats().hitCount() : 0;
  }

  public double getInsertNodeCacheRequestCount() {
    return Objects.nonNull(bufferCache) ? insertNodeCache.stats().requestCount() : 0;
  }

  public double getBufferCacheHitRate() {
    return Objects.nonNull(bufferCache) ? bufferCache.stats().hitRate() : 0;
  }

  public double getBufferCacheHitCount() {
    return Objects.nonNull(bufferCache) ? bufferCache.stats().hitCount() : 0;
  }

  public double getBufferCacheRequestCount() {
    return Objects.nonNull(bufferCache) ? bufferCache.stats().requestCount() : 0;
  }

  /////////////////////////// MemTable ///////////////////////////

  public void addMemTable(final long memTableId) {
    memTablesNeedSearch.add(memTableId);
  }

  public void removeMemTable(final long memTableId) {
    memTablesNeedSearch.remove(memTableId);
  }

  /////////////////////////// Cache Loader ///////////////////////////

  class WALInsertNodeCacheLoader implements CacheLoader<WALEntryPosition, ByteBuffer> {

    @Override
    public @Nullable ByteBuffer load(@NonNull final WALEntryPosition key) throws Exception {
      return key.read();
    }

    /** Batch load all wal entries in the file when any one key is absent. */
    @Override
    public @NonNull Map<@NonNull WALEntryPosition, @NonNull ByteBuffer> loadAll(
        @NonNull final Iterable<? extends @NonNull WALEntryPosition> walEntryPositions) {
      final Map<WALEntryPosition, ByteBuffer> loadedEntries = new HashMap<>();

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
                  buffer);
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
    return bufferCache.getIfPresent(position) != null;
  }

  @TestOnly
  public void clear() {
    bufferCache.invalidateAll();
    memTablesNeedSearch.clear();
  }
}
