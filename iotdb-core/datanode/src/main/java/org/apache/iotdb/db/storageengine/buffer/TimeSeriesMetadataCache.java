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
import org.apache.iotdb.commons.service.metric.MetricService;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.queryengine.metric.SeriesScanCostMetricSet;
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

import static org.apache.iotdb.db.queryengine.metric.SeriesScanCostMetricSet.READ_TIMESERIES_METADATA_CACHE;
import static org.apache.iotdb.db.queryengine.metric.SeriesScanCostMetricSet.READ_TIMESERIES_METADATA_FILE;
import static org.apache.tsfile.utils.RamUsageEstimator.sizeOfCharArray;

/**
 * This class is used to cache <code>TimeSeriesMetadata</code> in IoTDB. The caching strategy is
 * LRU.
 */
@SuppressWarnings("squid:S6548")
public class TimeSeriesMetadataCache {

  private static final Logger logger = LoggerFactory.getLogger(TimeSeriesMetadataCache.class);
  private static final Logger DEBUG_LOGGER = LoggerFactory.getLogger("QUERY_DEBUG");
  private static final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
  private static final long MEMORY_THRESHOLD_IN_TIME_SERIES_METADATA_CACHE =
      config.getAllocateMemoryForTimeSeriesMetaDataCache();
  private static final boolean CACHE_ENABLE = config.isMetaDataCacheEnable();

  private static final SeriesScanCostMetricSet SERIES_SCAN_COST_METRIC_SET =
      SeriesScanCostMetricSet.getInstance();

  private final Cache<TimeSeriesMetadataCacheKey, TimeseriesMetadata> lruCache;

  private final AtomicLong entryAverageSize = new AtomicLong(0);

  private final Map<String, WeakReference<String>> devices =
      Collections.synchronizedMap(new WeakHashMap<>());
  private static final String SEPARATOR = "$";

  private TimeSeriesMetadataCache() {
    if (CACHE_ENABLE) {
      logger.info(
          "TimeSeriesMetadataCache size = {}", MEMORY_THRESHOLD_IN_TIME_SERIES_METADATA_CACHE);
    }
    lruCache =
        Caffeine.newBuilder()
            .maximumWeight(MEMORY_THRESHOLD_IN_TIME_SERIES_METADATA_CACHE)
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
      boolean debug)
      throws IOException {
    long startTime = System.nanoTime();
    boolean cacheHit = true;
    try {
      String deviceStringFormat = key.device.toString();
      if (!CACHE_ENABLE) {
        cacheHit = false;

        // bloom filter part
        TsFileSequenceReader reader = FileReaderManager.getInstance().get(filePath, true);
        BloomFilter bloomFilter = reader.readBloomFilter();
        if (bloomFilter != null
            && !bloomFilter.contains(
                deviceStringFormat + IoTDBConstant.PATH_SEPARATOR + key.measurement)) {
          return null;
        }
        TimeseriesMetadata timeseriesMetadata =
            reader.readTimeseriesMetadata(key.device, key.measurement, ignoreNotExists);
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
        // allow for the parallelism of different devices
        synchronized (
            devices.computeIfAbsent(
                deviceStringFormat + SEPARATOR + filePath, WeakReference::new)) {
          // double check
          timeseriesMetadata = lruCache.getIfPresent(key);
          if (timeseriesMetadata == null) {
            cacheHit = false;

            // bloom filter part
            BloomFilter bloomFilter =
                BloomFilterCache.getInstance()
                    .get(
                        new BloomFilterCache.BloomFilterCacheKey(
                            filePath,
                            key.regionId,
                            key.timePartitionId,
                            key.tsFileVersion,
                            key.compactionVersion),
                        debug);
            if (bloomFilter != null
                && !bloomFilter.contains(
                    deviceStringFormat + TsFileConstant.PATH_SEPARATOR + key.measurement)) {
              if (debug) {
                DEBUG_LOGGER.info("TimeSeries meta data {} is filter by bloomFilter!", key);
              }
              return null;
            }
            TsFileSequenceReader reader = FileReaderManager.getInstance().get(filePath, true);
            List<TimeseriesMetadata> timeSeriesMetadataList =
                reader.readTimeseriesMetadata(key.device, key.measurement, allSensors);
            // put TimeSeriesMetadata of all sensors used in this read into cache
            for (TimeseriesMetadata metadata : timeSeriesMetadataList) {
              TimeSeriesMetadataCacheKey k =
                  new TimeSeriesMetadataCacheKey(
                      key.regionId,
                      key.timePartitionId,
                      key.tsFileVersion,
                      key.compactionVersion,
                      key.device,
                      metadata.getMeasurementId());
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
      SERIES_SCAN_COST_METRIC_SET.recordSeriesScanCost(
          cacheHit ? READ_TIMESERIES_METADATA_CACHE : READ_TIMESERIES_METADATA_FILE,
          System.nanoTime() - startTime);
    }
  }

  public double calculateTimeSeriesMetadataHitRatio() {
    return lruCache.stats().hitRate();
  }

  public long getEvictionCount() {
    return lruCache.stats().evictionCount();
  }

  public long getMaxMemory() {
    return MEMORY_THRESHOLD_IN_TIME_SERIES_METADATA_CACHE;
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

    private final int regionId;
    private final long timePartitionId;
    private final long tsFileVersion;
    // high 32 bit is compaction level, low 32 bit is merge count
    private final long compactionVersion;
    private final IDeviceID device;
    private final String measurement;

    public TimeSeriesMetadataCacheKey(TsFileID tsFileID, IDeviceID device, String measurement) {
      this.regionId = tsFileID.regionId;
      this.timePartitionId = tsFileID.timePartitionId;
      this.tsFileVersion = tsFileID.fileVersion;
      this.compactionVersion = tsFileID.compactionVersion;
      this.device = device;
      this.measurement = measurement;
    }

    public TimeSeriesMetadataCacheKey(
        int regionId,
        long timePartitionId,
        long tsFileVersion,
        long compactionVersion,
        IDeviceID device,
        String measurement) {
      this.regionId = regionId;
      this.timePartitionId = timePartitionId;
      this.tsFileVersion = tsFileVersion;
      this.compactionVersion = compactionVersion;
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
      return regionId == that.regionId
          && timePartitionId == that.timePartitionId
          && tsFileVersion == that.tsFileVersion
          && compactionVersion == that.compactionVersion
          && Objects.equals(device, that.device)
          && Objects.equals(measurement, that.measurement);
    }

    @Override
    public int hashCode() {
      return Objects.hash(
          regionId, timePartitionId, tsFileVersion, compactionVersion, device, measurement);
    }

    @Override
    public String toString() {
      return "TimeSeriesMetadataCacheKey{"
          + "regionId="
          + regionId
          + ", timePartitionId="
          + timePartitionId
          + ", tsFileVersion="
          + tsFileVersion
          + ", compactionVersion="
          + compactionVersion
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
