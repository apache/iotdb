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

package org.apache.iotdb.db.engine.cache;

import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.query.control.FileReaderManager;
import org.apache.iotdb.db.utils.TestOnly;
import org.apache.iotdb.tsfile.file.metadata.TimeseriesMetadata;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.utils.BloomFilter;
import org.apache.iotdb.tsfile.utils.FilePathUtils;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.utils.RamUsageEstimator;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Weigher;
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

/**
 * This class is used to cache <code>TimeSeriesMetadata</code> in IoTDB. The caching strategy is
 * LRU.
 */
public class TimeSeriesMetadataCache {

  private static final Logger logger = LoggerFactory.getLogger(TimeSeriesMetadataCache.class);
  private static final Logger DEBUG_LOGGER = LoggerFactory.getLogger("QUERY_DEBUG");
  private static final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
  private static final long MEMORY_THRESHOLD_IN_TIME_SERIES_METADATA_CACHE =
      config.getAllocateMemoryForTimeSeriesMetaDataCache();
  private static final boolean CACHE_ENABLE = config.isMetaDataCacheEnable();

  private final Cache<TimeSeriesMetadataCacheKey, TimeseriesMetadata> lruCache;

  private final AtomicLong entryAverageSize = new AtomicLong(0);

  private final Map<String, WeakReference<String>> devices =
      Collections.synchronizedMap(new WeakHashMap<>());
  private static final String SEPARATOR = "$";

  private TimeSeriesMetadataCache() {
    if (CACHE_ENABLE) {
      logger.info(
          "TimeseriesMetadataCache size = " + MEMORY_THRESHOLD_IN_TIME_SERIES_METADATA_CACHE);
    }
    lruCache =
        Caffeine.newBuilder()
            .maximumWeight(MEMORY_THRESHOLD_IN_TIME_SERIES_METADATA_CACHE)
            .weigher(
                (Weigher<TimeSeriesMetadataCacheKey, TimeseriesMetadata>)
                    (key, value) ->
                        (int)
                            (RamUsageEstimator.shallowSizeOf(key)
                                + RamUsageEstimator.sizeOf(key.device)
                                + RamUsageEstimator.sizeOf(key.measurement)
                                + RamUsageEstimator.sizeOf(key.tsFilePrefixPath)
                                + RamUsageEstimator.sizeOf(key.tsFileVersion)
                                + RamUsageEstimator.shallowSizeOf(value)
                                + RamUsageEstimator.sizeOf(value.getMeasurementId())
                                + RamUsageEstimator.shallowSizeOf(value.getStatistics())
                                + (value.getChunkMetadataList().get(0).calculateRamSize()
                                        + RamUsageEstimator.NUM_BYTES_OBJECT_REF)
                                    * value.getChunkMetadataList().size()
                                + RamUsageEstimator.shallowSizeOf(value.getChunkMetadataList())))
            .recordStats()
            .build();
  }

  public static TimeSeriesMetadataCache getInstance() {
    return TimeSeriesMetadataCache.TimeSeriesMetadataCacheHolder.INSTANCE;
  }

  public TimeseriesMetadata get(TimeSeriesMetadataCacheKey key, Set<String> allSensors)
      throws IOException {
    return get(key, allSensors, false);
  }

  @SuppressWarnings("squid:S1860") // Suppress synchronize warning
  public TimeseriesMetadata get(
      TimeSeriesMetadataCacheKey key, Set<String> allSensors, boolean debug) throws IOException {
    if (!CACHE_ENABLE) {
      // bloom filter part
      TsFileSequenceReader reader = FileReaderManager.getInstance().get(key.filePath, true);
      BloomFilter bloomFilter = reader.readBloomFilter();
      if (bloomFilter != null
          && !bloomFilter.contains(key.device + IoTDBConstant.PATH_SEPARATOR + key.measurement)) {
        return null;
      }
      return reader.readTimeseriesMetadata(new Path(key.device, key.measurement), false);
    }

    TimeseriesMetadata timeseriesMetadata = lruCache.getIfPresent(key);

    if (timeseriesMetadata == null) {
      if (debug) {
        DEBUG_LOGGER.info(
            "Cache miss: {}.{} in file: {}", key.device, key.measurement, key.filePath);
        DEBUG_LOGGER.info("Device: {}, all sensors: {}", key.device, allSensors);
      }
      // allow for the parallelism of different devices
      synchronized (
          devices.computeIfAbsent(key.device + SEPARATOR + key.filePath, WeakReference::new)) {
        // double check
        timeseriesMetadata = lruCache.getIfPresent(key);
        if (timeseriesMetadata == null) {
          Path path = new Path(key.device, key.measurement);
          // bloom filter part
          TsFileSequenceReader reader = FileReaderManager.getInstance().get(key.filePath, true);
          BloomFilter bloomFilter = reader.readBloomFilter();
          if (bloomFilter != null && !bloomFilter.contains(path.getFullPath())) {
            if (debug) {
              DEBUG_LOGGER.info("TimeSeries meta data {} is filter by bloomFilter!", key);
            }
            return null;
          }
          List<TimeseriesMetadata> timeSeriesMetadataList =
              reader.readTimeseriesMetadata(path, allSensors);
          // put TimeSeriesMetadata of all sensors used in this query into cache
          for (TimeseriesMetadata metadata : timeSeriesMetadataList) {
            TimeSeriesMetadataCacheKey k =
                new TimeSeriesMetadataCacheKey(
                    key.filePath, key.device, metadata.getMeasurementId());
            lruCache.put(k, metadata);
            if (metadata.getMeasurementId().equals(key.measurement)) {
              timeseriesMetadata = metadata;
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
            key.filePath,
            timeseriesMetadata);
      }
      return new TimeseriesMetadata(timeseriesMetadata);
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
    private final String filePath;
    private final String tsFilePrefixPath;
    private final long tsFileVersion;
    private final String device;
    private final String measurement;

    public TimeSeriesMetadataCacheKey(String filePath, String device, String measurement) {
      this.filePath = filePath;
      Pair<String, Long> tsFilePrefixPathAndTsFileVersionPair =
          FilePathUtils.getTsFilePrefixPathAndTsFileVersionPair(filePath);
      this.tsFilePrefixPath = tsFilePrefixPathAndTsFileVersionPair.left;
      this.tsFileVersion = tsFilePrefixPathAndTsFileVersionPair.right;
      this.device = device;
      this.measurement = measurement;
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
      return device.equals(that.device)
          && measurement.equals(that.measurement)
          && tsFileVersion == that.tsFileVersion
          && tsFilePrefixPath.equals(that.tsFilePrefixPath);
    }

    @Override
    public int hashCode() {
      return Objects.hash(tsFilePrefixPath, tsFileVersion, device, measurement);
    }
  }

  /** singleton pattern. */
  private static class TimeSeriesMetadataCacheHolder {

    private static final TimeSeriesMetadataCache INSTANCE = new TimeSeriesMetadataCache();
  }
}
