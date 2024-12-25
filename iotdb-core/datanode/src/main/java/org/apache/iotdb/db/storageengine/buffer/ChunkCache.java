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

import org.apache.iotdb.commons.exception.IoTDBIORuntimeException;
import org.apache.iotdb.commons.service.metric.MetricService;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.queryengine.execution.fragment.QueryContext;
import org.apache.iotdb.db.queryengine.metric.ChunkCacheMetrics;
import org.apache.iotdb.db.queryengine.metric.SeriesScanCostMetricSet;
import org.apache.iotdb.db.storageengine.dataregion.read.control.FileReaderManager;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileID;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Weigher;
import org.apache.tsfile.file.metadata.statistics.Statistics;
import org.apache.tsfile.read.TsFileSequenceReader;
import org.apache.tsfile.read.common.Chunk;
import org.apache.tsfile.read.common.TimeRange;
import org.apache.tsfile.utils.RamUsageEstimator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;
import java.util.function.LongConsumer;

import static org.apache.iotdb.db.queryengine.metric.SeriesScanCostMetricSet.READ_CHUNK_CACHE;
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
  private final Cache<ChunkCacheKey, Chunk> lruCache;

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
                        (int) (key.getRetainedSizeInBytes() + chunk.getRetainedSizeInBytes()))
            .recordStats()
            .build();

    // add metrics
    MetricService.getInstance().addMetricSet(new ChunkCacheMetrics(this));
  }

  public double getHitRate() {
    return lruCache.stats().hitRate() * 100;
  }

  public static ChunkCache getInstance() {
    return ChunkCacheHolder.INSTANCE;
  }

  @TestOnly
  public Chunk get(
      ChunkCacheKey chunkCacheKey, List<TimeRange> timeRangeList, Statistics chunkStatistic)
      throws IOException {
    LongConsumer emptyConsumer = l -> {};
    return get(
        chunkCacheKey,
        timeRangeList,
        chunkStatistic,
        false,
        emptyConsumer,
        emptyConsumer,
        emptyConsumer);
  }

  public Chunk get(
      ChunkCacheKey chunkCacheKey,
      List<TimeRange> timeRangeList,
      Statistics chunkStatistic,
      QueryContext queryContext)
      throws IOException {
    LongConsumer ioSizeRecorder =
        queryContext.getQueryStatistics().getLoadChunkActualIOSize()::addAndGet;
    LongConsumer cacheHitAdder =
        queryContext.getQueryStatistics().getLoadChunkFromCacheCount()::addAndGet;
    LongConsumer cacheMissAdder =
        queryContext.getQueryStatistics().getLoadChunkFromDiskCount()::addAndGet;
    return get(
        chunkCacheKey,
        timeRangeList,
        chunkStatistic,
        queryContext.isDebug(),
        ioSizeRecorder,
        cacheHitAdder,
        cacheMissAdder);
  }

  private Chunk get(
      ChunkCacheKey chunkCacheKey,
      List<TimeRange> timeRangeList,
      Statistics chunkStatistic,
      boolean debug,
      LongConsumer ioSizeRecorder,
      LongConsumer cacheHitAdder,
      LongConsumer cacheMissAdder)
      throws IOException {
    long startTime = System.nanoTime();
    ChunkLoader chunkLoader = new ChunkLoader(ioSizeRecorder);
    try {
      if (!CACHE_ENABLE) {
        Chunk chunk = chunkLoader.apply(chunkCacheKey);
        return constructChunk(chunk, timeRangeList, chunkStatistic);
      }

      Chunk chunk = lruCache.get(chunkCacheKey, chunkLoader);

      if (debug) {
        DEBUG_LOGGER.info("get chunk from cache whose key is: {}", chunkCacheKey);
      }

      return constructChunk(chunk, timeRangeList, chunkStatistic);
    } catch (IoTDBIORuntimeException e) {
      throw e.getCause();
    } finally {
      if (chunkLoader.isCacheMiss()) {
        cacheMissAdder.accept(1);
        SERIES_SCAN_COST_METRIC_SET.recordSeriesScanCost(
            READ_CHUNK_FILE, System.nanoTime() - startTime);
      } else {
        cacheHitAdder.accept(1);
        SERIES_SCAN_COST_METRIC_SET.recordSeriesScanCost(
            READ_CHUNK_CACHE, System.nanoTime() - startTime);
      }
    }
  }

  private Chunk constructChunk(
      Chunk chunk, List<TimeRange> timeRangeList, Statistics chunkStatistic) {
    return new Chunk(
        chunk.getHeader(),
        chunk.getData().duplicate(),
        timeRangeList,
        chunkStatistic,
        chunk.getEncryptParam());
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

    private static final long INSTANCE_SIZE =
        RamUsageEstimator.shallowSizeOfInstance(ChunkCacheKey.class);

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

    // we don't need to compare this field, it's just used to correctly get TsFileSequenceReader
    // from FileReaderManager
    private final boolean closed;

    public ChunkCacheKey(
        String filePath, TsFileID tsfileId, long offsetOfChunkHeader, boolean closed) {
      this.filePath = filePath;
      this.regionId = tsfileId.regionId;
      this.timePartitionId = tsfileId.timePartitionId;
      this.tsFileVersion = tsfileId.fileVersion;
      this.compactionVersion = tsfileId.compactionVersion;
      this.offsetOfChunkHeader = offsetOfChunkHeader;
      this.closed = closed;
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

  private static class ChunkLoader implements Function<ChunkCacheKey, Chunk> {

    private boolean cacheMiss = false;
    private final LongConsumer ioSizeRecorder;

    private ChunkLoader(LongConsumer ioSizeRecorder) {
      this.ioSizeRecorder = ioSizeRecorder;
    }

    @Override
    public Chunk apply(ChunkCacheKey key) {

      long startTime = System.nanoTime();
      try {
        cacheMiss = true;
        TsFileSequenceReader reader =
            FileReaderManager.getInstance().get(key.getFilePath(), key.closed, ioSizeRecorder);
        Chunk chunk = reader.readMemChunk(key.offsetOfChunkHeader, ioSizeRecorder);
        // to save memory footprint, we don't save measurementId in ChunkHeader of Chunk
        chunk.getHeader().setMeasurementID(null);
        return chunk;
      } catch (IOException e) {
        throw new IoTDBIORuntimeException(e);
      } finally {
        SERIES_SCAN_COST_METRIC_SET.recordSeriesScanCost(
            READ_CHUNK_FILE, System.nanoTime() - startTime);
      }
    }

    public boolean isCacheMiss() {
      return cacheMiss;
    }
  }

  /** singleton pattern. */
  private static class ChunkCacheHolder {

    private static final ChunkCache INSTANCE = new ChunkCache();
  }
}
