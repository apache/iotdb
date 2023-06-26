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

import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.storageengine.dataregion.wal.buffer.WALEntry;
import org.apache.iotdb.db.storageengine.dataregion.wal.buffer.WALEntryType;
import org.apache.iotdb.db.storageengine.dataregion.wal.io.WALByteBufReader;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertNode;

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
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/** This cache is used by {@link WALEntryPosition} */
public class WALInsertNodeCache {
  private static final Logger logger = LoggerFactory.getLogger(WALInsertNodeCache.class);
  private static final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
  /** LRU cache, find InsertNode by WALEntryPosition */
  private final LoadingCache<WALEntryPosition, InsertNode> lruCache;

  /** ids of all pinned memTables */
  private final Set<Long> memTablesNeedSearch = ConcurrentHashMap.newKeySet();

  private WALInsertNodeCache() {
    lruCache =
        Caffeine.newBuilder()
            // TODO: pipe module should determine how to configure this param
            .maximumWeight(config.getAllocateMemoryForWALPipeCache())
            .weigher(
                (Weigher<WALEntryPosition, InsertNode>) (position, buffer) -> position.getSize())
            .build(new WALInsertNodeCacheLoader());
  }

  public InsertNode get(WALEntryPosition position) {
    InsertNode res = lruCache.getIfPresent(position);
    // batch load from the wal file
    if (res == null) {
      res = lruCache.getAll(Collections.singleton(position)).get(position);
    }
    return res;
  }

  boolean contains(WALEntryPosition position) {
    return lruCache.getIfPresent(position) != null;
  }

  public void addMemTable(long memTableId) {
    memTablesNeedSearch.add(memTableId);
  }

  public void removeMemTable(long memTableId) {
    memTablesNeedSearch.remove(memTableId);
  }

  public void clear() {
    lruCache.invalidateAll();
    memTablesNeedSearch.clear();
  }

  class WALInsertNodeCacheLoader implements CacheLoader<WALEntryPosition, InsertNode> {
    private InsertNode parse(ByteBuffer buffer) {
      PlanNode node = WALEntry.deserializeForConsensus(buffer);
      if (node instanceof InsertNode) {
        return (InsertNode) node;
      } else {
        return null;
      }
    }

    @Override
    public @Nullable InsertNode load(@NonNull WALEntryPosition key) throws Exception {
      return parse(key.read());
    }

    /** Batch load all wal entries in the file when any one key is absent. */
    @Override
    public @NonNull Map<@NonNull WALEntryPosition, @NonNull InsertNode> loadAll(
        @NonNull Iterable<? extends @NonNull WALEntryPosition> keys) {
      Map<WALEntryPosition, InsertNode> res = new HashMap<>();
      for (WALEntryPosition pos : keys) {
        if (res.containsKey(pos) || !pos.canRead()) {
          continue;
        }
        long walFileVersionId = pos.getWalFileVersionId();
        // load one when wal file is not sealed
        if (!pos.isInSealedFile()) {
          try {
            res.put(pos, load(pos));
          } catch (Exception e) {
            logger.info(
                "Fail to cache wal entries from the wal file with version id {}",
                walFileVersionId,
                e);
          }
          continue;
        }
        // batch load when wal file is sealed
        long position = 0;
        try (FileChannel channel = pos.openReadFileChannel();
            WALByteBufReader walByteBufReader = new WALByteBufReader(pos.getWalFile(), channel)) {
          while (walByteBufReader.hasNext()) {
            // see WALInfoEntry#serialize, entry type + memtable id + plan node type
            ByteBuffer buffer = walByteBufReader.next();
            int size = buffer.capacity();
            WALEntryType type = WALEntryType.valueOf(buffer.get());
            long memTableId = buffer.getLong();
            if ((memTablesNeedSearch.contains(memTableId) || pos.getPosition() == position)
                && type.needSearch()) {
              buffer.clear();
              InsertNode node = parse(buffer);
              if (node != null) {
                res.put(
                    new WALEntryPosition(pos.getIdentifier(), walFileVersionId, position, size),
                    node);
              }
            }
            position += size;
          }
        } catch (IOException e) {
          logger.info(
              "Fail to cache wal entries from the wal file with version id {}",
              walFileVersionId,
              e);
        }
      }
      return res;
    }
  }

  public static WALInsertNodeCache getInstance() {
    return InstanceHolder.INSTANCE;
  }

  private static class InstanceHolder {
    private InstanceHolder() {}

    private static final WALInsertNodeCache INSTANCE = new WALInsertNodeCache();
  }
}
