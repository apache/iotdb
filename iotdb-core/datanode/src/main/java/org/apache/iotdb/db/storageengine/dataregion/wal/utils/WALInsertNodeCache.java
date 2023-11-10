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
import org.apache.iotdb.db.pipe.resource.PipeResourceManager;
import org.apache.iotdb.db.pipe.resource.memory.PipeMemoryBlock;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertNode;
import org.apache.iotdb.db.storageengine.dataregion.wal.buffer.WALEntry;
import org.apache.iotdb.db.storageengine.dataregion.wal.buffer.WALEntryType;
import org.apache.iotdb.db.storageengine.dataregion.wal.io.WALByteBufReader;
import org.apache.iotdb.tsfile.utils.Pair;

import com.github.benmanes.caffeine.cache.CacheLoader;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.github.benmanes.caffeine.cache.Weigher;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/** This cache is used by {@link WALEntryPosition}. */
public class WALInsertNodeCache {

  private static final Logger LOGGER = LoggerFactory.getLogger(WALInsertNodeCache.class);
  private static final IoTDBConfig CONFIG = IoTDBDescriptor.getInstance().getConfig();

  // LRU cache, find Pair<ByteBuffer, InsertNode> by WALEntryPosition
  private final PipeMemoryBlock allocatedMemoryBlock;
  private boolean isBatchLoadEnabled;
  private final LoadingCache<WALEntryPosition, Pair<ByteBuffer, InsertNode>> lruCache;

  // ids of all pinned memTables
  private final Set<Long> memTablesNeedSearch = ConcurrentHashMap.newKeySet();

  private volatile boolean hasPipeRunning = false;

  private WALInsertNodeCache(Integer dataRegionId) {
    allocatedMemoryBlock =
        PipeResourceManager.memory()
            .tryAllocate(
                (long)
                    Math.min(
                        2 * CONFIG.getWalFileSizeThresholdInByte(),
                        CONFIG.getAllocateMemoryForPipe() * 0.8 / 5));
    isBatchLoadEnabled =
        allocatedMemoryBlock.getMemoryUsageInBytes() >= CONFIG.getWalFileSizeThresholdInByte();
    lruCache =
        Caffeine.newBuilder()
            .maximumWeight(allocatedMemoryBlock.getMemoryUsageInBytes())
            .weigher(
                (Weigher<WALEntryPosition, Pair<ByteBuffer, InsertNode>>)
                    (position, pair) -> position.getSize())
            .recordStats()
            .build(new WALInsertNodeCacheLoader());
    PipeWALInsertNodeCacheMetrics.getInstance().register(this, dataRegionId);
  }

  /////////////////////////// Getter & Setter ///////////////////////////

  public InsertNode getInsertNode(WALEntryPosition position) {
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
    } catch (Exception e) {
      LOGGER.error(
          "Parsing failed when recovering insertNode from wal, walFile:{}, position:{}, size:{}, exception:",
          position.getWalFile(),
          position.getPosition(),
          position.getSize(),
          e);
      throw e;
    }
  }

  private InsertNode parse(ByteBuffer buffer) {
    final PlanNode node = WALEntry.deserializeForConsensus(buffer);
    if (node instanceof InsertNode) {
      return (InsertNode) node;
    } else {
      return null;
    }
  }

  public ByteBuffer getByteBuffer(WALEntryPosition position) {
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

  public Pair<ByteBuffer, InsertNode> getByteBufferOrInsertNode(WALEntryPosition position) {
    hasPipeRunning = true;

    final Pair<ByteBuffer, InsertNode> pair =
        isBatchLoadEnabled
            ? lruCache.getAll(Collections.singleton(position)).get(position)
            : lruCache.get(position);

    if (pair == null) {
      throw new IllegalStateException();
    }

    return pair;
  }

  public void cacheInsertNodeIfNeeded(WALEntryPosition walEntryPosition, InsertNode insertNode) {
    // reduce memory usage
    if (hasPipeRunning) {
      lruCache.put(walEntryPosition, new Pair<>(null, insertNode));
    }
  }

  public double getCacheHitRate() {
    return Objects.nonNull(lruCache) ? lruCache.stats().hitRate() : 0;
  }

  /////////////////////////// MemTable ///////////////////////////

  public void addMemTable(long memTableId) {
    memTablesNeedSearch.add(memTableId);
  }

  public void removeMemTable(long memTableId) {
    memTablesNeedSearch.remove(memTableId);
  }

  /////////////////////////// Cache Loader ///////////////////////////

  class WALInsertNodeCacheLoader
      implements CacheLoader<WALEntryPosition, Pair<ByteBuffer, InsertNode>> {

    @Override
    public @Nullable Pair<ByteBuffer, InsertNode> load(@NonNull WALEntryPosition key)
        throws Exception {
      return new Pair<>(key.read(), null);
    }

    /** Batch load all wal entries in the file when any one key is absent. */
    @Override
    public @NonNull Map<@NonNull WALEntryPosition, @NonNull Pair<ByteBuffer, InsertNode>> loadAll(
        @NonNull Iterable<? extends @NonNull WALEntryPosition> walEntryPositions) {
      final Map<WALEntryPosition, Pair<ByteBuffer, InsertNode>> loadedEntries = new HashMap<>();

      for (WALEntryPosition walEntryPosition : walEntryPositions) {
        if (loadedEntries.containsKey(walEntryPosition) || !walEntryPosition.canRead()) {
          continue;
        }

        final long walFileVersionId = walEntryPosition.getWalFileVersionId();

        // load one when wal file is not sealed
        if (!walEntryPosition.isInSealedFile()) {
          try {
            loadedEntries.put(walEntryPosition, load(walEntryPosition));
          } catch (Exception e) {
            LOGGER.info(
                "Fail to cache wal entries from the wal file with version id {}",
                walFileVersionId,
                e);
          }
          continue;
        }

        // batch load when wal file is sealed
        long position = 0;
        try (final FileChannel channel = walEntryPosition.openReadFileChannel();
            final WALByteBufReader walByteBufReader =
                new WALByteBufReader(walEntryPosition.getWalFile(), channel)) {
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
        } catch (IOException e) {
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

  public static WALInsertNodeCache getInstance(Integer regionId) {
    return InstanceHolder.getOrCreateInstance(regionId);
  }

  private static class InstanceHolder {

    private static final Map<Integer, WALInsertNodeCache> INSTANCE_MAP = new ConcurrentHashMap<>();

    public static WALInsertNodeCache getOrCreateInstance(Integer key) {
      return INSTANCE_MAP.computeIfAbsent(key, k -> new WALInsertNodeCache(key));
    }

    private InstanceHolder() {
      // forbidding instantiation
    }
  }

  /////////////////////////// Test Only ///////////////////////////

  @TestOnly
  public boolean isBatchLoadEnabled() {
    return isBatchLoadEnabled;
  }

  @TestOnly
  public void setIsBatchLoadEnabled(boolean isBatchLoadEnabled) {
    this.isBatchLoadEnabled = isBatchLoadEnabled;
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
