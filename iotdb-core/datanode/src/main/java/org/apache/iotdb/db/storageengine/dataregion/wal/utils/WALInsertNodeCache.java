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

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Weigher;
import org.apache.tsfile.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/** This cache is used by {@link WALEntrySegmentPosition}. */
public class WALInsertNodeCache {

  private static final Logger LOGGER = LoggerFactory.getLogger(WALInsertNodeCache.class);
  private static final PipeConfig PIPE_CONFIG = PipeConfig.getInstance();

  private static PipeModelFixedMemoryBlock walModelFixedMemory = null;

  // LRU cache, find ByteBuffer or InsertNode by WALEntryPosition
  private final WALCache bufferCache;

  private final Cache<WALEntrySegmentPosition, InsertNode> insertNodeCache;

  // ids of all pinned memTables
  private final Set<Long> memTablesNeedSearch = ConcurrentHashMap.newKeySet();

  private volatile boolean hasPipeRunning = false;

  private WALInsertNodeCache() {
    if (walModelFixedMemory == null) {
      init();
    }

    final long requestedAllocateSize = walModelFixedMemory.getMemoryUsageInBytes();

    final long insertNodeCacheSize =
        (long) (PIPE_CONFIG.getPipeWALCacheInsertNodeMemoryProportion() * requestedAllocateSize);
    final long bufferCacheSize =
        Math.min(
            requestedAllocateSize - insertNodeCacheSize,
            (long) (PIPE_CONFIG.getPipeWAlCacheBufferMemoryProportion() * requestedAllocateSize));
    insertNodeCache =
        Caffeine.newBuilder()
            .maximumWeight(requestedAllocateSize / 2)
            .weigher(
                (Weigher<WALEntrySegmentPosition, InsertNode>)
                    (position, insertNode) -> {
                      long weightInLong = InsertNodeMemoryEstimator.sizeOf(insertNode);
                      if (weightInLong <= 0) {
                        return Integer.MAX_VALUE;
                      }
                      final int weightInInt = (int) weightInLong;
                      return weightInInt != weightInLong ? Integer.MAX_VALUE : weightInInt;
                    })
            .build();

    bufferCache =
        PipeConfig.getInstance().getPipeWALCacheSegmentUnitEnabled()
            ? new WALSegmentCache(bufferCacheSize, memTablesNeedSearch)
            : new WALEntryCache(bufferCacheSize, memTablesNeedSearch);
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

  public InsertNode getInsertNodeIfPossible(final WALEntrySegmentPosition position) {
    hasPipeRunning = true;
    return insertNodeCache.getIfPresent(position);
  }

  public InsertNode getInsertNode(final WALEntrySegmentPosition position) {
    InsertNode insertNode = getInsertNodeIfPossible(position);

    if (insertNode != null) {
      return insertNode;
    }

    ByteBuffer buffer = bufferCache.load(position);

    if (buffer == null) {
      throw new IllegalStateException(
          "WALInsertNodeCache getInsertNode failed, position: " + position);
    }

    try {
      // multi pipes may share the same wal entry, so we need to wrap the byte[] into
      // different ByteBuffer for each pipe
      insertNode = parse(buffer);
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

  public ByteBuffer getByteBuffer(final WALEntrySegmentPosition position) {
    final ByteBuffer buffer = getByteBufferIfPossible(position);

    if (buffer == null) {
      throw new IllegalStateException();
    }

    return buffer;
  }

  public ByteBuffer getByteBufferIfPossible(final WALEntrySegmentPosition position) {
    hasPipeRunning = true;
    return bufferCache.load(position);
  }

  public void cacheInsertNodeIfNeeded(
      final WALEntrySegmentPosition walEntrySegmentPosition, final InsertNode insertNode) {
    // reduce memory usage
    if (hasPipeRunning) {
      insertNodeCache.put(walEntrySegmentPosition, insertNode);
    }
  }

  public Pair<ByteBuffer, InsertNode> getByteBufferOrInsertNodeIfPossible(
      final WALEntrySegmentPosition position) {
    final InsertNode insertNode = getInsertNodeIfPossible(position);

    if (insertNode != null) {
      // multi pipes may share the same wal entry, so we need to wrap the byte[] into
      // different ByteBuffer for each pipe
      return new Pair<>(null, insertNode);
    }

    // forbid multi threads to invalidate and load the same entry
    final ByteBuffer byteBuffer = getByteBufferIfPossible(position);

    if (byteBuffer == null) {
      throw new IllegalStateException(
          "WALInsertNodeCache getByteBuffer failed, position: " + position);
    }

    return new Pair<>(byteBuffer, null);
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
  public void clear() {
    bufferCache.invalidateAll();
    memTablesNeedSearch.clear();
  }
}
