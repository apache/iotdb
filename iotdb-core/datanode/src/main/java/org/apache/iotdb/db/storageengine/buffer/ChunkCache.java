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

package org.apache.iotdb.db.storageengine.buffer;

import org.apache.iotdb.commons.service.metric.MetricService;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.queryengine.metric.ChunkCacheMetrics;
import org.apache.iotdb.db.queryengine.metric.SeriesScanCostMetricSet;
import org.apache.iotdb.db.storageengine.dataregion.read.control.FileReaderManager;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileID;
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.common.Chunk;
import org.apache.iotdb.tsfile.read.common.TimeRange;
import org.apache.iotdb.tsfile.utils.RamUsageEstimator;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.github.benmanes.caffeine.cache.Weigher;
import org.openjdk.jol.info.ClassLayout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

import static org.apache.iotdb.db.queryengine.metric.SeriesScanCostMetricSet.READ_CHUNK_ALL;
import static org.apache.iotdb.db.queryengine.metric.SeriesScanCostMetricSet.READ_CHUNK_FILE;

/**
 * This class is used to cache <code>Chunk</code> of <code>ChunkMetaData</code> in IoTDB. The
 * caching strategy is LRU.
 */
@SuppressWarnings("squid:S6548")
public class ChunkCache {

  private static final Logger LOGGER = LoggerFactory.getLogger(ChunkCache.class);
  private static final Logger DEBUG_LOGGER = LoggerFactory.getLogger("QUERY_DEBUG");
  private static final IoTDBConfig CONFIG = IoTDBDescriptor.getInstance().getConfig();
  private static final long MEMORY_THRESHOLD_IN_CHUNK_CACHE =
      CONFIG.getAllocateMemoryForChunkCache();
  private static final boolean CACHE_ENABLE = CONFIG.isMetaDataCacheEnable();

  private static final SeriesScanCostMetricSet SERIES_SCAN_COST_METRIC_SET =
      SeriesScanCostMetricSet.getInstance();

  // to save memory footprint, we don't save measurementId in ChunkHeader of Chunk
  private final LoadingCache<ChunkCacheKey, Chunk> lruCache;

  private ChunkCache() {
    if (CACHE_ENABLE) {
      LOGGER.info("ChunkCache size = {}", MEMORY_THRESHOLD_IN_CHUNK_CACHE);
    }
    lruCache =
        Caffeine.newBuilder()
            .maximumWeight(MEMORY_THRESHOLD_IN_CHUNK_CACHE)
            .weigher(
                (Weigher<ChunkCacheKey, Chunk>)
                    (key, chunk) ->
                        (int) (key.getRetainedSizeInBytes() + RamUsageEstimator.sizeOf(chunk)))
            .recordStats()
            .build(
                key -> {
                  long startTime = System.nanoTime();
                  try {
                    TsFileSequenceReader reader =
                        FileReaderManager.getInstance().get(key.getFilePath(), true);
                    Chunk chunk = reader.readMemChunk(key.offsetOfChunkHeader);
                    // to save memory footprint, we don't save measurementId in ChunkHeader of Chunk
                    chunk.getHeader().setMeasurementID(null);
                    return chunk;
                  } finally {
                    SERIES_SCAN_COST_METRIC_SET.recordSeriesScanCost(
                        READ_CHUNK_FILE, System.nanoTime() - startTime);
                  }
                });

    // add metrics
    MetricService.getInstance().addMetricSet(new ChunkCacheMetrics(this));
  }

  public double getHitRate() {
    return lruCache.stats().hitRate() * 100;
  }

  public static ChunkCache getInstance() {
    return ChunkCacheHolder.INSTANCE;
  }

  public Chunk get(
      ChunkCacheKey chunkCacheKey,
      List<TimeRange> timeRangeList,
      Statistics chunkStatistic,
      boolean debug)
      throws IOException {
    long startTime = System.nanoTime();
    try {
      if (!CACHE_ENABLE) {
        TsFileSequenceReader reader =
            FileReaderManager.getInstance().get(chunkCacheKey.getFilePath(), true);
        Chunk chunk = reader.readMemChunk(chunkCacheKey.offsetOfChunkHeader);
        return new Chunk(
            chunk.getHeader(), chunk.getData().duplicate(), timeRangeList, chunkStatistic);
      }

      Chunk chunk = lruCache.get(chunkCacheKey);

      if (debug) {
        DEBUG_LOGGER.info("get chunk from cache whose key is: {}", chunkCacheKey);
      }

      return new Chunk(
          chunk.getHeader(), chunk.getData().duplicate(), timeRangeList, chunkStatistic);
    } finally {
      SERIES_SCAN_COST_METRIC_SET.recordSeriesScanCost(
          READ_CHUNK_ALL, System.nanoTime() - startTime);
    }
  }

  public double calculateChunkHitRatio() {
    return lruCache.stats().hitRate();
  }

  public long getEvictionCount() {
    return lruCache.stats().evictionCount();
  }

  public long getMaxMemory() {
    return MEMORY_THRESHOLD_IN_CHUNK_CACHE;
  }

  public double getAverageLoadPenalty() {
    return lruCache.stats().averageLoadPenalty();
  }

  /** clear LRUCache. */
  public void clear() {
    lruCache.invalidateAll();
    lruCache.cleanUp();
  }

  @TestOnly
  public boolean isEmpty() {
    return lruCache.asMap().isEmpty();
  }

  public static class ChunkCacheKey {

    private static final int INSTANCE_SIZE =
        ClassLayout.parseClass(ChunkCacheKey.class).instanceSize();

    // There is no need to add this field size while calculating the size of ChunkCacheKey,
    // because filePath is get from TsFileResource, different ChunkCacheKey of the same file
    // share this String.
    private final String filePath;
    private final int regionId;
    private final long timePartitionId;
    private final long tsFileVersion;
    // high 32 bit is compaction level, low 32 bit is merge count
    private final long compactionVersion;

    private final long offsetOfChunkHeader;

    public ChunkCacheKey(
        String filePath,
        int regionId,
        long timePartitionId,
        long tsFileVersion,
        long compactionVersion,
        long offsetOfChunkHeader) {
      this.filePath = filePath;
      this.regionId = regionId;
      this.timePartitionId = timePartitionId;
      this.tsFileVersion = tsFileVersion;
      this.compactionVersion = compactionVersion;
      this.offsetOfChunkHeader = offsetOfChunkHeader;
    }

    public ChunkCacheKey(String filePath, TsFileID tsfileId, long offsetOfChunkHeader) {
      this.filePath = filePath;
      this.regionId = tsfileId.regionId;
      this.timePartitionId = tsfileId.timePartitionId;
      this.tsFileVersion = tsfileId.fileVersion;
      this.compactionVersion = tsfileId.compactionVersion;
      this.offsetOfChunkHeader = offsetOfChunkHeader;
    }

    public long getRetainedSizeInBytes() {
      return INSTANCE_SIZE;
    }

    public String getFilePath() {
      return filePath;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      ChunkCacheKey that = (ChunkCacheKey) o;
      return regionId == that.regionId
          && timePartitionId == that.timePartitionId
          && tsFileVersion == that.tsFileVersion
          && compactionVersion == that.compactionVersion
          && offsetOfChunkHeader == that.offsetOfChunkHeader;
    }

    @Override
    public int hashCode() {
      return Objects.hash(
          regionId, timePartitionId, tsFileVersion, compactionVersion, offsetOfChunkHeader);
    }

    @Override
    public String toString() {
      return "ChunkCacheKey{"
          + "filePath='"
          + filePath
          + '\''
          + ", regionId="
          + regionId
          + ", timePartitionId="
          + timePartitionId
          + ", tsFileVersion="
          + tsFileVersion
          + ", compactionVersion="
          + compactionVersion
          + ", offsetOfChunkHeader="
          + offsetOfChunkHeader
          + '}';
    }
  }

  /** singleton pattern. */
  private static class ChunkCacheHolder {

    private static final ChunkCache INSTANCE = new ChunkCache();
  }
}
