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
package org.apache.iotdb.db.wal.utils;

import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.write.InsertNode;
import org.apache.iotdb.db.wal.buffer.WALEntry;
import org.apache.iotdb.db.wal.buffer.WALEntryType;
import org.apache.iotdb.db.wal.io.WALByteBufReader;

import com.github.benmanes.caffeine.cache.CacheLoader;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.github.benmanes.caffeine.cache.Weigher;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/** This cache is used by {@link WALEntryPosition} */
public class WALInsertNodeCache {
  private static final Logger logger = LoggerFactory.getLogger(WALInsertNodeCache.class);
  private static final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
  /** LRU cache, find InsertNode by WALEntryPosition */
  private final LoadingCache<WALEntryPosition, InsertNode> lruCache;

  /** ids of all pinned memTables */
  private final Map<Long, Object> memTablesNeedSearch = new ConcurrentHashMap<>();

  private final Object DEFAULT_VALUE = new Object();

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

  @TestOnly
  boolean contains(WALEntryPosition position) {
    return lruCache.getIfPresent(position) != null;
  }

  public void addMemTable(long memTableId) {
    memTablesNeedSearch.put(memTableId, DEFAULT_VALUE);
  }

  public void removeMemTable(long memTableId) {
    memTablesNeedSearch.remove(memTableId);
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
        if (res.containsKey(pos)) {
          continue;
        }
        File file = pos.getWalFile();
        long position = 0;
        try (WALByteBufReader walByteBufReader = new WALByteBufReader(file)) {
          while (walByteBufReader.hasNext()) {
            // see WALInfoEntry#serialize, entry type + memtable id + plan node type
            ByteBuffer buffer = walByteBufReader.next();
            int size = buffer.capacity();
            WALEntryType type = WALEntryType.valueOf(buffer.get());
            long memTableId = buffer.getLong();
            if (memTablesNeedSearch.containsKey(memTableId) && type.needSearch()) {
              buffer.clear();
              InsertNode node = parse(buffer);
              if (node != null) {
                res.put(new WALEntryPosition(file, position, size), node);
              }
            }
            position += size;
          }
        } catch (IOException e) {
          logger.info("Fail to cache wal entries from the wal file {}", file, e);
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
