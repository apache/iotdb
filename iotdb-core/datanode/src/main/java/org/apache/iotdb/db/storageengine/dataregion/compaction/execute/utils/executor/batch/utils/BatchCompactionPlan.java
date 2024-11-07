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

package org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.executor.batch.utils;

import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.executor.ModifiedStatus;

import org.apache.tsfile.file.metadata.ChunkMetadata;
import org.apache.tsfile.read.TsFileSequenceReader;
import org.apache.tsfile.read.common.Chunk;
import org.apache.tsfile.read.common.TimeRange;
import org.apache.tsfile.utils.Pair;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BatchCompactionPlan {
  public static long maxCachedTimeChunksSize = 2 * 1024 * 1024;
  private final List<CompactChunkPlan> compactChunkPlans = new ArrayList<>();
  private final Map<String, Map<TimeRange, ModifiedStatus>> alignedPageModifiedStatusCache =
      new HashMap<>();
  // key is <filename, chunk offset in file>
  private final Map<Pair<String, Long>, Chunk> cachedTimeChunks = new HashMap<>();
  private long cachedTimeChunkSize = 0;

  public Chunk getTimeChunkFromCache(TsFileSequenceReader reader, ChunkMetadata chunkMetadata)
      throws IOException {
    Pair<String, Long> key =
        new Pair<>(reader.getFileName(), chunkMetadata.getOffsetOfChunkHeader());
    Chunk chunk = cachedTimeChunks.get(key);
    if (chunk == null) {
      chunk = reader.readMemChunk(chunkMetadata);
    }
    chunk.getData().rewind();
    return chunk;
  }

  public void addTimeChunkToCache(String file, long offset, Chunk chunk) {
    if (cachedTimeChunkSize >= maxCachedTimeChunksSize) {
      return;
    }
    cachedTimeChunks.put(
        new Pair<>(file, offset),
        new Chunk(
            chunk.getHeader(),
            chunk.getData(),
            chunk.getDeleteIntervalList(),
            chunk.getChunkStatistic(),
            chunk.getEncryptParam()));
    cachedTimeChunkSize += chunk.getHeader().getDataSize();
  }

  public void recordCompactedChunk(CompactChunkPlan compactChunkPlan) {
    compactChunkPlans.add(compactChunkPlan);
  }

  public CompactChunkPlan getCompactChunkPlan(int i) {
    return compactChunkPlans.get(i);
  }

  public void recordPageModifiedStatus(
      String file, TimeRange timeRange, ModifiedStatus modifiedStatus) {
    alignedPageModifiedStatusCache
        .computeIfAbsent(file, k1 -> new HashMap<>())
        .computeIfAbsent(timeRange, k2 -> modifiedStatus);
  }

  public ModifiedStatus getAlignedPageModifiedStatus(String file, TimeRange timeRange) {
    return alignedPageModifiedStatusCache.getOrDefault(file, Collections.emptyMap()).get(timeRange);
  }

  public int compactedChunkNum() {
    return compactChunkPlans.size();
  }

  public boolean isEmpty() {
    return compactChunkPlans.isEmpty();
  }

  @TestOnly
  public static void setMaxCachedTimeChunksSize(long size) {
    maxCachedTimeChunksSize = size;
  }

  @TestOnly
  public static long getMaxCachedTimeChunksSize() {
    return maxCachedTimeChunksSize;
  }

  @Override
  public String toString() {
    return compactChunkPlans.toString();
  }
}
