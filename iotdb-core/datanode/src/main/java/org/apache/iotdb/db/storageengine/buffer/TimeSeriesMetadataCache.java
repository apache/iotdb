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
import org.apache.iotdb.db.i18n.StorageEngineMessages;
import org.apache.iotdb.db.queryengine.execution.fragment.QueryContext;
import org.apache.iotdb.db.queryengine.execution.fragment.QueryStatistics;
import org.apache.iotdb.db.queryengine.metric.TimeSeriesMetadataCacheMetrics;
import org.apache.iotdb.db.storageengine.dataregion.read.control.FileReaderManager;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileID;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Weigher;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.TimeseriesMetadata;
import org.apache.tsfile.file.metadata.statistics.Statistics;
import org.apache.tsfile.read.TsFileSequenceReader;
import org.apache.tsfile.utils.BloomFilter;
import org.apache.tsfile.utils.PublicBAOS;
import org.apache.tsfile.utils.RamUsageEstimator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongConsumer;

import static org.apache.tsfile.common.constant.TsFileConstant.TIME_COLUMN_ID;
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
  private static final TimeseriesMetadata ABSENT_SERIES_MARKER =
      new TimeseriesMetadata(
          (byte) 0,
          0,
          "",
          TSDataType.INT32,
          Statistics.getStatsByType(TSDataType.INT32),
          new PublicBAOS());

  private final Cache<TimeSeriesMetadataCacheKey, TimeseriesMetadata> lruCache;

  private final AtomicLong entryAverageSize = new AtomicLong(0);

  private static final int DEVICE_LOCK_STRIPES = 1024;
  private static final Object[] DEVICE_LOCKS = new Object[DEVICE_LOCK_STRIPES];

  static {
    for (int i = 0; i < DEVICE_LOCK_STRIPES; i++) {
      DEVICE_LOCKS[i] = new Object();
    }
  }

  private final AtomicLong evictedExistingEntryCount = new AtomicLong(0);
  private final AtomicLong evictedNonExistingEntryCount = new AtomicLong(0);

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
          StorageEngineMessages.STORAGE_LOG_TIMESERIESMETADATACACHE_SIZE_E31733D3,
          CACHE_MEMORY_BLOCK.getTotalMemorySizeInBytes());
    }
    lruCache =
        Caffeine.newBuilder()
            .maximumWeight(CACHE_MEMORY_BLOCK.getTotalMemorySizeInBytes())
            .weigher(
                (Weigher<TimeSeriesMetadataCacheKey, TimeseriesMetadata>)
                    (key, value) ->
                        (int)
                            (key.getRetainedSizeInBytes()
                                + (value == ABSENT_SERIES_MARKER
                                    ? 0
                                    : value.getRetainedSizeInBytes())))
            .recordStats()
            .evictionListener(
                (k, v, c) -> {
                  if (v == ABSENT_SERIES_MARKER) {
                    evictedNonExistingEntryCount.incrementAndGet();
                  } else {
                    evictedExistingEntryCount.incrementAndGet();
                  }
                })
            .build();
    // add metrics
    MetricService.getInstance().addMetricSet(new TimeSeriesMetadataCacheMetrics(this));
  }

  public static TimeSeriesMetadataCache getInstance() {
    return TimeSeriesMetadataCache.TimeSeriesMetadataCacheHolder.INSTANCE;
  }

  public TimeseriesMetadata get(
      String filePath,
      TimeSeriesMetadataCacheKey key,
      Set<String> allSensors,
      boolean ignoreMissingDevice,
      boolean debug,
      QueryContext queryContext)
      throws IOException {
    return get(filePath, key, allSensors, ignoreMissingDevice, debug, queryContext, null);
  }

  public TimeseriesMetadata get(
      String filePath,
      TimeSeriesMetadataCacheKey key,
      Set<String> allSensors,
      boolean ignoreMissingDevice,
      boolean debug,
      QueryContext queryContext,
      long[] deviceMetadataIndexNodeOffset)
      throws IOException {
    long startNanos = System.nanoTime();
    boolean useCache = CACHE_ENABLE && !queryContext.isExternalTsFileScan();
    long bloomFilterLoadNanos = 0;
    boolean cacheHit = false;
    try {
      if (useCache) {
        TimeseriesMetadata cached = lruCache.getIfPresent(key);
        if (cached != null) {
          cacheHit = true;
          return copyCachedMetadata(cached, debug, key, filePath);
        }
      }
      if (deviceMetadataIndexNodeOffset == null) {
        long bloomFilterStartNanos = System.nanoTime();
        BloomFilter bloomFilter = readBloomFilter(filePath, key.tsFileID, queryContext, debug);
        bloomFilterLoadNanos = System.nanoTime() - bloomFilterStartNanos;
        if (bloomFilter != null
            && !bloomFilter.contains(
                key.device.toString() + IoTDBConstant.PATH_SEPARATOR + key.measurement)) {
          if (debug) {
            DEBUG_LOGGER.info(StorageEngineMessages.TS_METADATA_FILTERED_BY_BLOOM_FILTER, key);
          }
          return null;
        }
      }
      DeviceMetadataReader fileReader =
          new DeviceMetadataReader(
              filePath,
              key.tsFileID,
              key.device,
              deviceMetadataIndexNodeOffset,
              ignoreMissingDevice,
              queryContext);
      Set<String> measurementToLocate = Collections.singleton(key.measurement);
      if (!useCache) {
        Map<String, TimeseriesMetadata> fileMetadata =
            fileReader.readMeasurements(measurementToLocate, measurementToLocate);
        return nonEmptyMetadata(fileMetadata.get(key.measurement), key, debug);
      }
      synchronized (deviceLock(key.device, filePath)) {
        TimeseriesMetadata cached = lruCache.getIfPresent(key);
        if (cached != null) {
          cacheHit = true;
          return copyCachedMetadata(cached, debug, key, filePath);
        }
        if (debug) {
          DEBUG_LOGGER.info(
              StorageEngineMessages.CACHE_MISS_IN_FILE, key.device, key.measurement, filePath);
          DEBUG_LOGGER.info(StorageEngineMessages.DEVICE_ALL_SENSORS, key.device, allSensors);
        }
        // A single-column read can warm other selected measurements in the same leaf group.
        Map<String, TimeseriesMetadata> fileMetadata =
            fileReader.readMeasurements(measurementToLocate, allSensors);
        for (TimeseriesMetadata metadata : fileMetadata.values()) {
          if (metadata.getStatistics().getCount() != 0) {
            lruCache.put(fileReader.cacheKey(metadata.getMeasurementId()), metadata);
          }
        }
        TimeseriesMetadata measurementMetadata =
            nonEmptyMetadata(fileMetadata.get(key.measurement), key, debug);
        if (measurementMetadata == null) {
          if (shouldCacheAbsentSeries(queryContext)) {
            lruCache.put(key, ABSENT_SERIES_MARKER);
          }
          return null;
        }
        return new TimeseriesMetadata(measurementMetadata);
      }
    } finally {
      long elapsed = System.nanoTime() - startNanos;
      QueryStatistics statistics = queryContext.getQueryStatistics();
      statistics.getLoadBloomFilterTime().getAndAdd(bloomFilterLoadNanos);
      if (cacheHit) {
        statistics.getLoadTimeSeriesMetadataFromCacheCount().getAndAdd(1);
        statistics
            .getLoadTimeSeriesMetadataFromCacheTime()
            .getAndAdd(elapsed - bloomFilterLoadNanos);
      } else {
        statistics.getLoadTimeSeriesMetadataFromDiskCount().getAndAdd(1);
        statistics
            .getLoadTimeSeriesMetadataFromDiskTime()
            .getAndAdd(elapsed - bloomFilterLoadNanos);
      }
    }
  }

  private static TimeseriesMetadata copyCachedMetadata(
      TimeseriesMetadata cached, boolean debug, TimeSeriesMetadataCacheKey key, String filePath) {
    if (cached == ABSENT_SERIES_MARKER) {
      if (debug) {
        DEBUG_LOGGER.info(StorageEngineMessages.FILE_NO_SUCH_TIME_SERIES, key);
      }
      return null;
    }
    if (debug) {
      DEBUG_LOGGER.info(
          StorageEngineMessages.STORAGE_LOG_GET_TIMESERIES_METADATA_IN_FILE_FROM_CACHE_36652729,
          key.device,
          key.measurement,
          filePath,
          cached);
    }
    return new TimeseriesMetadata(cached);
  }

  /**
   * Loads one aligned projection, including the time column ({@code ""}). A cache miss that passes
   * the file's BloomFilter is read together with the other misses. Only reader-confirmed absences
   * are eligible for negative caching, and only after recovery.
   *
   * <p>The caller may modify the returned metadata without changing cached entries.
   *
   * @param deviceMetadataIndexNodeOffset non-null for external tsfiles, which address the device
   *     metadata index by offset; bloom filtering is skipped in that case
   * @return null if the aligned time column is absent or empty; otherwise a map of the requested
   *     measurements, with null for each absent or empty value column
   */
  public Map<String, TimeseriesMetadata> getAlignedTimeseriesMetadata(
      String filePath,
      TsFileID tsFileID,
      IDeviceID device,
      Set<String> requestedMeasurements,
      boolean ignoreMissingDevice,
      QueryContext queryContext,
      long[] deviceMetadataIndexNodeOffset)
      throws IOException {
    long startNanos = System.nanoTime();
    long cacheLookupNanos = 0;
    long bloomFilterLoadNanos = 0;
    long cacheHitCount = 0;
    boolean debug = queryContext.isDebug();
    boolean useCache = CACHE_ENABLE && !queryContext.isExternalTsFileScan();
    try {
      TimeseriesMetadata cachedTimeColumn =
          useCache
              ? lruCache.getIfPresent(
                  new TimeSeriesMetadataCacheKey(tsFileID, device, TIME_COLUMN_ID))
              : null;
      if (cachedTimeColumn == ABSENT_SERIES_MARKER) {
        cacheHitCount = 1;
        cacheLookupNanos = System.nanoTime() - startNanos;
        return null;
      }

      BloomFilter bloomFilter = null;
      String devicePrefix = null;
      if (cachedTimeColumn == null && deviceMetadataIndexNodeOffset == null) {
        long bloomFilterStartNanos = System.nanoTime();
        bloomFilter = readBloomFilter(filePath, tsFileID, queryContext, debug);
        bloomFilterLoadNanos = System.nanoTime() - bloomFilterStartNanos;
        if (bloomFilter != null) {
          devicePrefix = device.toString() + IoTDBConstant.PATH_SEPARATOR;
          if (!bloomFilter.contains(devicePrefix)) {
            return null;
          }
        }
      }

      Map<String, TimeseriesMetadata> metadataByMeasurement =
          new HashMap<>(requestedMeasurements.size());
      Set<String> measurementsToRead = new HashSet<>(requestedMeasurements.size());
      if (useCache) {
        for (String measurement : requestedMeasurements) {
          TimeSeriesMetadataCacheKey key =
              new TimeSeriesMetadataCacheKey(tsFileID, device, measurement);
          TimeseriesMetadata cached =
              TIME_COLUMN_ID.equals(measurement) ? cachedTimeColumn : lruCache.getIfPresent(key);
          if (cached == null) {
            measurementsToRead.add(measurement);
          } else {
            metadataByMeasurement.put(
                measurement, copyCachedMetadata(cached, debug, key, filePath));
          }
        }
        cacheHitCount = metadataByMeasurement.size();
        cacheLookupNanos = System.nanoTime() - startNanos - bloomFilterLoadNanos;
        if (measurementsToRead.isEmpty()) {
          return metadataByMeasurement;
        }
        // A cached time column let us defer loading the filter until a value column missed.
        if (cachedTimeColumn != null && deviceMetadataIndexNodeOffset == null) {
          long bloomFilterStartNanos = System.nanoTime();
          bloomFilter = readBloomFilter(filePath, tsFileID, queryContext, debug);
          bloomFilterLoadNanos = System.nanoTime() - bloomFilterStartNanos;
          if (bloomFilter != null) {
            devicePrefix = device.toString() + IoTDBConstant.PATH_SEPARATOR;
          }
        }
      } else {
        measurementsToRead.addAll(requestedMeasurements);
      }

      DeviceMetadataReader fileReader =
          new DeviceMetadataReader(
              filePath,
              tsFileID,
              device,
              deviceMetadataIndexNodeOffset,
              ignoreMissingDevice,
              queryContext);
      if (bloomFilter != null) {
        fileReader.excludeAbsentValueColumns(
            bloomFilter, devicePrefix, measurementsToRead, metadataByMeasurement);
      }
      if (measurementsToRead.isEmpty()) {
        return metadataByMeasurement;
      }
      if (!useCache) {
        return loadAlignedMeasurements(
            fileReader, measurementsToRead, metadataByMeasurement, false);
      }
      // Only filter survivors need the locked recheck and the reader.
      synchronized (deviceLock(device, filePath)) {
        Iterator<String> iterator = measurementsToRead.iterator();
        while (iterator.hasNext()) {
          String measurement = iterator.next();
          TimeSeriesMetadataCacheKey key = fileReader.cacheKey(measurement);
          TimeseriesMetadata cached = lruCache.getIfPresent(key);
          if (cached != null) {
            cacheHitCount++;
            if (TIME_COLUMN_ID.equals(measurement) && cached == ABSENT_SERIES_MARKER) {
              return null;
            }
            metadataByMeasurement.put(
                measurement, copyCachedMetadata(cached, debug, key, filePath));
            iterator.remove();
          }
        }
        if (measurementsToRead.isEmpty()) {
          return metadataByMeasurement;
        }
        if (debug) {
          for (String measurement : measurementsToRead) {
            DEBUG_LOGGER.info(
                StorageEngineMessages.CACHE_MISS_IN_FILE, device, measurement, filePath);
          }
          DEBUG_LOGGER.info(
              StorageEngineMessages.DEVICE_ALL_SENSORS, device, requestedMeasurements);
        }
        return loadAlignedMeasurements(fileReader, measurementsToRead, metadataByMeasurement, true);
      }
    } finally {
      long elapsed = System.nanoTime() - startNanos - bloomFilterLoadNanos;
      long cacheMissCount = requestedMeasurements.size() - cacheHitCount;
      if (cacheHitCount == 0) {
        cacheLookupNanos = 0;
      } else if (cacheMissCount == 0) {
        cacheLookupNanos = elapsed;
      }
      long uncachedLookupNanos = elapsed - cacheLookupNanos;
      recordLoadStatistics(
          queryContext,
          cacheHitCount,
          cacheLookupNanos,
          cacheMissCount,
          uncachedLookupNanos,
          bloomFilterLoadNanos);
    }
  }

  /** Resolves the measurements left by the cache and BloomFilter in one TsFile read. */
  private Map<String, TimeseriesMetadata> loadAlignedMeasurements(
      DeviceMetadataReader fileReader,
      Set<String> measurementsToRead,
      Map<String, TimeseriesMetadata> metadataByMeasurement,
      boolean useCache)
      throws IOException {
    Map<String, TimeseriesMetadata> fileMetadata =
        fileReader.readMeasurements(measurementsToRead, measurementsToRead);
    boolean cacheAbsentSeries = useCache && shouldCacheAbsentSeries(fileReader.queryContext);
    if (measurementsToRead.contains(TIME_COLUMN_ID) && isAbsent(fileMetadata.get(TIME_COLUMN_ID))) {
      if (cacheAbsentSeries) {
        lruCache.put(fileReader.cacheKey(TIME_COLUMN_ID), ABSENT_SERIES_MARKER);
      }
      return null;
    }
    for (String measurement : measurementsToRead) {
      TimeseriesMetadata metadata = fileMetadata.get(measurement);
      if (isAbsent(metadata)) {
        if (cacheAbsentSeries) {
          lruCache.put(fileReader.cacheKey(measurement), ABSENT_SERIES_MARKER);
        }
        if (fileReader.queryContext.isDebug()) {
          DEBUG_LOGGER.info(
              StorageEngineMessages.FILE_NO_SUCH_TIME_SERIES, fileReader.cacheKey(measurement));
        }
        metadataByMeasurement.put(measurement, null);
      } else if (useCache) {
        lruCache.put(fileReader.cacheKey(measurement), metadata);
        metadataByMeasurement.put(measurement, new TimeseriesMetadata(metadata));
      } else {
        metadataByMeasurement.put(measurement, metadata);
      }
    }
    return metadataByMeasurement;
  }

  private static boolean isAbsent(TimeseriesMetadata metadata) {
    return metadata == null || metadata.getStatistics().getCount() == 0;
  }

  private static TimeseriesMetadata nonEmptyMetadata(
      TimeseriesMetadata metadata, TimeSeriesMetadataCacheKey key, boolean debug) {
    if (isAbsent(metadata)) {
      if (debug) {
        DEBUG_LOGGER.info(StorageEngineMessages.FILE_NO_SUCH_TIME_SERIES, key);
      }
      return null;
    }
    return metadata;
  }

  private static boolean shouldCacheAbsentSeries(QueryContext queryContext) {
    return !queryContext.startedDuringStorageEngineRecovery()
        && memoryConfig.isMayCacheNonExistSeries();
  }

  private static BloomFilter readBloomFilter(
      String filePath, TsFileID tsFileID, QueryContext queryContext, boolean debug)
      throws IOException {
    QueryStatistics statistics = queryContext.getQueryStatistics();
    return BloomFilterCache.getInstance()
        .get(
            new BloomFilterCache.BloomFilterCacheKey(filePath, tsFileID),
            debug,
            statistics.getLoadBloomFilterActualIOSize()::addAndGet,
            statistics.getLoadBloomFilterFromCacheCount()::addAndGet,
            statistics.getLoadBloomFilterFromDiskCount()::addAndGet,
            queryContext.isExternalTsFileScan());
  }

  /** Reads metadata for one device in one file and charges its I/O to the query. */
  private static final class DeviceMetadataReader {
    private final String filePath;
    private final TsFileID tsFileID;
    private final IDeviceID device;
    private final long[] deviceMetadataIndexNodeOffset;
    private final boolean ignoreMissingDevice;
    private final QueryContext queryContext;

    private DeviceMetadataReader(
        String filePath,
        TsFileID tsFileID,
        IDeviceID device,
        long[] deviceMetadataIndexNodeOffset,
        boolean ignoreMissingDevice,
        QueryContext queryContext) {
      this.filePath = filePath;
      this.tsFileID = tsFileID;
      this.device = device;
      this.deviceMetadataIndexNodeOffset = deviceMetadataIndexNodeOffset;
      this.ignoreMissingDevice = ignoreMissingDevice;
      this.queryContext = queryContext;
    }

    private TimeSeriesMetadataCacheKey cacheKey(String measurement) {
      return new TimeSeriesMetadataCacheKey(tsFileID, device, measurement);
    }

    private void excludeAbsentValueColumns(
        BloomFilter bloomFilter,
        String devicePrefix,
        Set<String> measurementsToRead,
        Map<String, TimeseriesMetadata> metadataByMeasurement) {
      Iterator<String> iterator = measurementsToRead.iterator();
      while (iterator.hasNext()) {
        String measurement = iterator.next();
        if (TIME_COLUMN_ID.equals(measurement)
            || bloomFilter.contains(devicePrefix + measurement)) {
          continue;
        }
        iterator.remove();
        metadataByMeasurement.put(measurement, null);
        if (queryContext.isDebug()) {
          DEBUG_LOGGER.info(
              StorageEngineMessages.TS_METADATA_FILTERED_BY_BLOOM_FILTER, cacheKey(measurement));
        }
      }
    }

    private Map<String, TimeseriesMetadata> readMeasurements(
        Set<String> measurementsToLocate, Set<String> measurementsToReturn) throws IOException {
      LongConsumer recordBytesRead =
          queryContext.getQueryStatistics().getLoadTimeSeriesMetadataActualIOSize()::addAndGet;
      TsFileSequenceReader reader =
          FileReaderManager.getInstance()
              .get(filePath, tsFileID, true, recordBytesRead, queryContext.isExternalTsFileScan());
      return reader.readTimeseriesMetadataForMeasurements(
          device,
          deviceMetadataIndexNodeOffset,
          measurementsToLocate,
          measurementsToReturn,
          ignoreMissingDevice,
          recordBytesRead);
    }
  }

  private static void recordLoadStatistics(
      QueryContext queryContext,
      long cacheHitCount,
      long cacheLookupNanos,
      long cacheMissCount,
      long uncachedLookupNanos,
      long bloomFilterLoadNanos) {
    QueryStatistics statistics = queryContext.getQueryStatistics();
    statistics.getLoadBloomFilterTime().getAndAdd(bloomFilterLoadNanos);
    if (cacheHitCount > 0) {
      statistics.getLoadTimeSeriesMetadataFromCacheCount().getAndAdd(cacheHitCount);
      statistics.getLoadTimeSeriesMetadataFromCacheTime().getAndAdd(cacheLookupNanos);
    }
    if (cacheMissCount > 0 || uncachedLookupNanos > 0) {
      statistics.getLoadTimeSeriesMetadataFromDiskCount().getAndAdd(cacheMissCount);
      statistics.getLoadTimeSeriesMetadataFromDiskTime().getAndAdd(uncachedLookupNanos);
    }
  }

  /**
   * Striped, strongly referenced monitor for one device+file. A WeakHashMap entry can be reclaimed
   * by GC between calls, which would let two threads hold "the same" lock simultaneously.
   */
  private static Object deviceLock(IDeviceID device, String filePath) {
    return DEVICE_LOCKS[
        Math.floorMod(31 * device.hashCode() + filePath.hashCode(), DEVICE_LOCK_STRIPES)];
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
    logger.info(
        StorageEngineMessages
            .MESSAGE_EVICTED_NON_EXISTING_EXISTING_SERIES_COUNT_ARG_ARG_ARG_TOTAL_REQUEST_ARG_3026ADBD,
        evictedNonExistingEntryCount.get(),
        evictedExistingEntryCount.get(),
        ((double) evictedNonExistingEntryCount.get()) / evictedExistingEntryCount.get(),
        lruCache.stats().requestCount());
    lruCache.invalidateAll();
    lruCache.cleanUp();
    evictedNonExistingEntryCount.set(0);
    evictedExistingEntryCount.set(0);
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
