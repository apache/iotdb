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

import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.memory.IMemoryBlock;
import org.apache.iotdb.commons.memory.MemoryBlockType;
import org.apache.iotdb.commons.service.metric.MetricService;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.db.conf.DataNodeMemoryConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.queryengine.execution.fragment.QueryContext;
import org.apache.iotdb.db.queryengine.metric.TimeSeriesMetadataCacheMetrics;
import org.apache.iotdb.db.storageengine.dataregion.read.control.FileReaderManager;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileID;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Weigher;
import org.apache.tsfile.common.constant.TsFileConstant;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.TimeseriesMetadata;
import org.apache.tsfile.read.TsFileSequenceReader;
import org.apache.tsfile.utils.BloomFilter;
import org.apache.tsfile.utils.RamUsageEstimator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.ref.WeakReference;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.WeakHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongConsumer;

import static org.apache.tsfile.utils.RamUsageEstimator.sizeOfCharArray;

/**
 * This class is used to cache <code>TimeSeriesMetadata</code> in IoTDB. The caching strategy is
 * LRU.
 */
@SuppressWarnings("squid:S6548")
public class TimeSeriesMetadataCache {

  private static final Logger logger = LoggerFactory.getLogger(TimeSeriesMetadataCache.class);
  private static final Logger DEBUG_LOGGER = LoggerFactory.getLogger("QUERY_DEBUG");
  private static final DataNodeMemoryConfig memoryConfig =
      IoTDBDescriptor.getInstance().getMemoryConfig();
  private static final IMemoryBlock CACHE_MEMORY_BLOCK;
  private static final boolean CACHE_ENABLE = memoryConfig.isMetaDataCacheEnable();

  private final Cache<TimeSeriesMetadataCacheKey, TimeseriesMetadata> lruCache;

  private final AtomicLong entryAverageSize = new AtomicLong(0);

  private final Map<String, WeakReference<String>> devices =
      Collections.synchronizedMap(new WeakHashMap<>());
  private static final String SEPARATOR = "$";

  static {
    CACHE_MEMORY_BLOCK =
        memoryConfig
            .getTimeSeriesMetaDataCacheMemoryManager()
            .exactAllocate("TimeSeriesMetadataCache", MemoryBlockType.STATIC);
    // TODO @spricoder find a better way to get the size of cache
    CACHE_MEMORY_BLOCK.allocate(CACHE_MEMORY_BLOCK.getTotalMemorySizeInBytes());
  }

  private TimeSeriesMetadataCache() {
    if (CACHE_ENABLE) {
      logger.info(
          "TimeSeriesMetadataCache size = {}", CACHE_MEMORY_BLOCK.getTotalMemorySizeInBytes());
    }
    lruCache =
        Caffeine.newBuilder()
            .maximumWeight(CACHE_MEMORY_BLOCK.getTotalMemorySizeInBytes())
            .weigher(
                (Weigher<TimeSeriesMetadataCacheKey, TimeseriesMetadata>)
                    (key, value) ->
                        (int) (key.getRetainedSizeInBytes() + value.getRetainedSizeInBytes()))
            .recordStats()
            .build();
    // add metrics
    MetricService.getInstance().addMetricSet(new TimeSeriesMetadataCacheMetrics(this));
  }

  public static TimeSeriesMetadataCache getInstance() {
    return TimeSeriesMetadataCache.TimeSeriesMetadataCacheHolder.INSTANCE;
  }

  @SuppressWarnings({"squid:S1860", "squid:S6541", "squid:S3776"}) // Suppress synchronize warning
  public TimeseriesMetadata get(
      String filePath,
      TimeSeriesMetadataCacheKey key,
      Set<String> allSensors,
      boolean ignoreNotExists,
      boolean debug,
      QueryContext queryContext)
      throws IOException {
    long startTime = System.nanoTime();
    long loadBloomFilterTime = 0;
    LongConsumer timeSeriesMetadataIoSizeRecorder =
        queryContext.getQueryStatistics().getLoadTimeSeriesMetadataActualIOSize()::addAndGet;
    LongConsumer bloomFilterIoSizeRecorder =
        queryContext.getQueryStatistics().getLoadBloomFilterActualIOSize()::addAndGet;
    boolean cacheHit = true;
    try {
      if (!CACHE_ENABLE) {
        String deviceStringFormat = key.device.toString();
        cacheHit = false;

        // bloom filter part
        TsFileSequenceReader reader =
            FileReaderManager.getInstance()
                .get(filePath, key.tsFileID, true, bloomFilterIoSizeRecorder);
        BloomFilter bloomFilter = reader.readBloomFilter(bloomFilterIoSizeRecorder);
        queryContext.getQueryStatistics().getLoadBloomFilterFromDiskCount().incrementAndGet();
        if (bloomFilter != null
            && !bloomFilter.contains(
                deviceStringFormat + IoTDBConstant.PATH_SEPARATOR + key.measurement)) {
          loadBloomFilterTime = System.nanoTime() - startTime;
          return null;
        }
        loadBloomFilterTime = System.nanoTime() - startTime;

        TimeseriesMetadata timeseriesMetadata =
            reader.readTimeseriesMetadata(
                key.device, key.measurement, ignoreNotExists, timeSeriesMetadataIoSizeRecorder);
        return (timeseriesMetadata == null || timeseriesMetadata.getStatistics().getCount() == 0)
            ? null
            : timeseriesMetadata;
      }

      TimeseriesMetadata timeseriesMetadata = lruCache.getIfPresent(key);

      if (timeseriesMetadata == null) {
        if (debug) {
          DEBUG_LOGGER.info("Cache miss: {}.{} in file: {}", key.device, key.measurement, filePath);
          DEBUG_LOGGER.info("Device: {}, all sensors: {}", key.device, allSensors);
        }
        String deviceStringFormat = key.device.toString();
        // allow for the parallelism of different devices
        synchronized (
            devices.computeIfAbsent(
                deviceStringFormat + SEPARATOR + filePath, WeakReference::new)) {
          // double check
          timeseriesMetadata = lruCache.getIfPresent(key);
          if (timeseriesMetadata == null) {
            cacheHit = false;

            long loadBloomFilterStartTime = System.nanoTime();
            // bloom filter part
            BloomFilter bloomFilter =
                BloomFilterCache.getInstance()
                    .get(
                        new BloomFilterCache.BloomFilterCacheKey(filePath, key.tsFileID),
                        debug,
                        bloomFilterIoSizeRecorder,
                        queryContext.getQueryStatistics().getLoadBloomFilterFromCacheCount()
                            ::addAndGet,
                        queryContext.getQueryStatistics().getLoadBloomFilterFromDiskCount()
                            ::addAndGet);
            if (bloomFilter != null
                && !bloomFilter.contains(
                    deviceStringFormat + TsFileConstant.PATH_SEPARATOR + key.measurement)) {
              if (debug) {
                DEBUG_LOGGER.info("TimeSeries meta data {} is filter by bloomFilter!", key);
              }
              loadBloomFilterTime = System.nanoTime() - loadBloomFilterStartTime;
              return null;
            }

            loadBloomFilterTime = System.nanoTime() - loadBloomFilterStartTime;
            TsFileSequenceReader reader =
                FileReaderManager.getInstance()
                    .get(filePath, key.tsFileID, true, timeSeriesMetadataIoSizeRecorder);
            List<TimeseriesMetadata> timeSeriesMetadataList =
                reader.readTimeseriesMetadata(
                    key.device,
                    key.measurement,
                    allSensors,
                    ignoreNotExists,
                    timeSeriesMetadataIoSizeRecorder);
            // put TimeSeriesMetadata of all sensors used in this read into cache
            for (TimeseriesMetadata metadata : timeSeriesMetadataList) {
              TimeSeriesMetadataCacheKey k =
                  new TimeSeriesMetadataCacheKey(
                      key.tsFileID, key.device, metadata.getMeasurementId());
              if (metadata.getStatistics().getCount() != 0) {
                lruCache.put(k, metadata);
              }
              if (metadata.getMeasurementId().equals(key.measurement)) {
                timeseriesMetadata = metadata.getStatistics().getCount() == 0 ? null : metadata;
              }
            }
          }
        }
      }
      if (timeseriesMetadata == null) {
        if (debug) {
          DEBUG_LOGGER.info("The file doesn't have this time series {}.", key);
        }
        return null;
      } else {
        if (debug) {
          DEBUG_LOGGER.info(
              "Get timeseries: {}.{}  metadata in file: {}  from cache: {}.",
              key.device,
              key.measurement,
              filePath,
              timeseriesMetadata);
        }
        return new TimeseriesMetadata(timeseriesMetadata);
      }
    } finally {
      queryContext.getQueryStatistics().getLoadBloomFilterTime().getAndAdd(loadBloomFilterTime);
      if (cacheHit) {
        queryContext
            .getQueryStatistics()
            .getLoadTimeSeriesMetadataFromCacheCount()
            .incrementAndGet();
        queryContext
            .getQueryStatistics()
            .getLoadTimeSeriesMetadataFromCacheTime()
            .getAndAdd(System.nanoTime() - startTime - loadBloomFilterTime);
      } else {
        queryContext
            .getQueryStatistics()
            .getLoadTimeSeriesMetadataFromDiskCount()
            .incrementAndGet();
        queryContext
            .getQueryStatistics()
            .getLoadTimeSeriesMetadataFromDiskTime()
            .getAndAdd(System.nanoTime() - startTime - loadBloomFilterTime);
      }
    }
  }

  public double calculateTimeSeriesMetadataHitRatio() {
    return lruCache.stats().hitRate();
  }

  public long getEvictionCount() {
    return lruCache.stats().evictionCount();
  }

  public long getMaxMemory() {
    return CACHE_MEMORY_BLOCK.getTotalMemorySizeInBytes();
  }

  public double getAverageLoadPenalty() {
    return lruCache.stats().averageLoadPenalty();
  }

  public long getAverageSize() {
    return entryAverageSize.get();
  }

  public double calculateBloomFilterHitRatio() {
    return BloomFilterCache.getInstance().calculateBloomFilterHitRatio();
  }

  /** clear LRUCache. */
  public void clear() {
    lruCache.invalidateAll();
    lruCache.cleanUp();
  }

  public void remove(TimeSeriesMetadataCacheKey key) {
    lruCache.invalidate(key);
  }

  @TestOnly
  public boolean isEmpty() {
    return lruCache.asMap().isEmpty();
  }

  public static class TimeSeriesMetadataCacheKey {

    private static final long INSTANCE_SIZE =
        RamUsageEstimator.shallowSizeOfInstance(TimeSeriesMetadataCacheKey.class)
            + RamUsageEstimator.shallowSizeOfInstance(String.class);

    private final TsFileID tsFileID;
    private final IDeviceID device;
    private final String measurement;

    public TimeSeriesMetadataCacheKey(TsFileID tsFileID, IDeviceID device, String measurement) {
      this.tsFileID = tsFileID;
      this.device = device;
      this.measurement = measurement;
    }

    public long getRetainedSizeInBytes() {
      return INSTANCE_SIZE + device.ramBytesUsed() + sizeOfCharArray(measurement.length());
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      TimeSeriesMetadataCacheKey that = (TimeSeriesMetadataCacheKey) o;
      return Objects.equals(tsFileID, that.tsFileID)
          && Objects.equals(device, that.device)
          && Objects.equals(measurement, that.measurement);
    }

    @Override
    public int hashCode() {
      return Objects.hash(tsFileID, device, measurement);
    }

    @Override
    public String toString() {
      return "TimeSeriesMetadataCacheKey{"
          + "regionId="
          + tsFileID.regionId
          + ", timePartitionId="
          + tsFileID.timePartitionId
          + ", tsFileVersion="
          + tsFileID.fileVersion
          + ", compactionVersion="
          + tsFileID.compactionVersion
          + ", device='"
          + device
          + '\''
          + ", measurement='"
          + measurement
          + '\''
          + '}';
    }
  }

  /** singleton pattern. */
  private static class TimeSeriesMetadataCacheHolder {

    private static final TimeSeriesMetadataCache INSTANCE = new TimeSeriesMetadataCache();
  }
}
