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
import org.apache.tsfile.common.constant.TsFileConstant;
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
import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
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
  private static final TimeseriesMetadata NULL_EXISTS_CACHE_PLACE_HOLDER =
      new TimeseriesMetadata(
          (byte) 0,
          0,
          "",
          TSDataType.INT32,
          Statistics.getStatsByType(TSDataType.INT32),
          new PublicBAOS());

  private final Cache<TimeSeriesMetadataCacheKey, TimeseriesMetadata> lruCache;

  private final AtomicLong entryAverageSize = new AtomicLong(0);

  private final Map<String, WeakReference<String>> devices =
      Collections.synchronizedMap(new WeakHashMap<>());
  private static final String SEPARATOR = "$";

  /** A remainder this small is cheaper to load with per-column leaf-group lookups. */
  private static final int MAX_PER_COLUMN_FALLBACK = 2;

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
                                + (value == NULL_EXISTS_CACHE_PLACE_HOLDER
                                    ? 0
                                    : value.getRetainedSizeInBytes())))
            .recordStats()
            .evictionListener(
                (k, v, c) -> {
                  if (v == NULL_EXISTS_CACHE_PLACE_HOLDER) {
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

  @SuppressWarnings({"squid:S1860", "squid:S6541", "squid:S3776"}) // Suppress synchronize warning
  public TimeseriesMetadata get(
      String filePath,
      TimeSeriesMetadataCacheKey key,
      Set<String> allSensors,
      boolean ignoreNotExists,
      boolean debug,
      QueryContext queryContext)
      throws IOException {
    return get(filePath, key, allSensors, ignoreNotExists, debug, queryContext, null);
  }

  @SuppressWarnings({"squid:S1860", "squid:S6541", "squid:S3776"}) // Suppress synchronize warning
  public TimeseriesMetadata get(
      String filePath,
      TimeSeriesMetadataCacheKey key,
      Set<String> allSensors,
      boolean ignoreNotExists,
      boolean debug,
      QueryContext queryContext,
      long[] deviceMetadataIndexNodeOffset)
      throws IOException {
    long startTime = System.nanoTime();
    long loadBloomFilterTime = 0;
    LongConsumer timeSeriesMetadataIoSizeRecorder =
        queryContext.getQueryStatistics().getLoadTimeSeriesMetadataActualIOSize()::addAndGet;
    LongConsumer bloomFilterIoSizeRecorder =
        queryContext.getQueryStatistics().getLoadBloomFilterActualIOSize()::addAndGet;
    boolean cacheHit = true;
    boolean externalTsFile = queryContext.isExternalTsFileScan();
    try {
      if (!CACHE_ENABLE || externalTsFile) {
        String deviceStringFormat = key.device.toString();
        cacheHit = false;

        // bloom filter part
        TsFileSequenceReader reader =
            FileReaderManager.getInstance()
                .get(filePath, key.tsFileID, true, bloomFilterIoSizeRecorder, externalTsFile);
        if (deviceMetadataIndexNodeOffset == null) {
          BloomFilter bloomFilter = reader.readBloomFilter(bloomFilterIoSizeRecorder);
          queryContext.getQueryStatistics().getLoadBloomFilterFromDiskCount().incrementAndGet();
          if (bloomFilter != null
              && !bloomFilter.contains(
                  deviceStringFormat + IoTDBConstant.PATH_SEPARATOR + key.measurement)) {
            loadBloomFilterTime = System.nanoTime() - startTime;
            return null;
          }
          loadBloomFilterTime = System.nanoTime() - startTime;
        }

        TimeseriesMetadata timeseriesMetadata =
            reader.readTimeseriesMetadata(
                key.device,
                deviceMetadataIndexNodeOffset,
                key.measurement,
                ignoreNotExists,
                timeSeriesMetadataIoSizeRecorder);
        return (timeseriesMetadata == null || timeseriesMetadata.getStatistics().getCount() == 0)
            ? null
            : timeseriesMetadata;
      }

      TimeseriesMetadata timeseriesMetadata = lruCache.getIfPresent(key);

      if (timeseriesMetadata == null) {
        if (debug) {
          DEBUG_LOGGER.info(
              StorageEngineMessages.CACHE_MISS_IN_FILE, key.device, key.measurement, filePath);
          DEBUG_LOGGER.info(StorageEngineMessages.DEVICE_ALL_SENSORS, key.device, allSensors);
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

            if (deviceMetadataIndexNodeOffset == null) {
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
                              ::addAndGet,
                          false);
              if (bloomFilter != null
                  && !bloomFilter.contains(
                      deviceStringFormat + TsFileConstant.PATH_SEPARATOR + key.measurement)) {
                if (debug) {
                  DEBUG_LOGGER.info(
                      StorageEngineMessages.TS_METADATA_FILTERED_BY_BLOOM_FILTER, key);
                }
                loadBloomFilterTime = System.nanoTime() - loadBloomFilterStartTime;
                return null;
              }

              loadBloomFilterTime = System.nanoTime() - loadBloomFilterStartTime;
            }
            TsFileSequenceReader reader =
                FileReaderManager.getInstance()
                    .get(filePath, key.tsFileID, true, timeSeriesMetadataIoSizeRecorder, false);
            List<TimeseriesMetadata> timeSeriesMetadataList =
                reader.readTimeseriesMetadata(
                    key.device,
                    deviceMetadataIndexNodeOffset,
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
            if (timeseriesMetadata == null
                && !ignoreNotExists
                && memoryConfig.isMayCacheNonExistSeries()) {
              lruCache.put(key, NULL_EXISTS_CACHE_PLACE_HOLDER);
            }
          }
        }
      }
      if (timeseriesMetadata == null || timeseriesMetadata == NULL_EXISTS_CACHE_PLACE_HOLDER) {
        if (debug) {
          DEBUG_LOGGER.info(StorageEngineMessages.FILE_NO_SUCH_TIME_SERIES, key);
        }
        return null;
      } else {
        if (debug) {
          DEBUG_LOGGER.info(
              StorageEngineMessages.STORAGE_LOG_GET_TIMESERIES_METADATA_IN_FILE_FROM_CACHE_36652729,
              key.device,
              key.measurement,
              filePath,
              timeseriesMetadata);
        }
        return new TimeseriesMetadata(timeseriesMetadata);
      }
    } finally {
      long lookupTime = System.nanoTime() - startTime - loadBloomFilterTime;
      recordLoadStatistics(
          queryContext,
          cacheHit ? 1 : 0,
          cacheHit ? lookupTime : 0,
          cacheHit ? 0 : 1,
          cacheHit ? 0 : lookupTime,
          loadBloomFilterTime);
    }
  }

  /**
   * Load the TimeseriesMetadata of {@code allSensors} of a device, reusing the per-measurement
   * cache and reading the device in a single batch only for the measurements that are still
   * missing. This is used by wide-table scans (a device with many columns) to avoid walking the
   * device metadata index once per column / leaf group.
   *
   * <p>Measurements already cached are returned without touching the file. The remaining ones are
   * first checked against the file's cached bloom filter, so columns absent from this file do not
   * force a device read; only measurements that cannot be ruled out are loaded by the batch read.
   *
   * <p>The returned map always holds copies of the cached (or freshly read) metadata, so the caller
   * may apply {@code setModified} and {@code setChunkMetadataLoader} without corrupting cached
   * entries — matching {@link #get}.
   *
   * <p>Load statistics are counted per series (not per device read) so that they stay comparable
   * with {@link #get}; the bytes read by the batch read cannot be reported because {@link
   * TsFileSequenceReader#getDeviceTimeseriesMetadata(IDeviceID)} has no IO-size recorder.
   *
   * @param filePath path of the tsfile
   * @param tsFileID id of the tsfile
   * @param device target device
   * @param allSensors measurements the caller needs, including the time column keyed by ""
   * @param queryContext query context used for metrics and to detect external tsfiles
   * @param ignoreNotExists whether a device missing from a file that claims a device-level time
   *     index should be skipped instead of reported as an inconsistency
   * @return map from measurementId (including the time column, keyed by "") to TimeseriesMetadata;
   *     entries whose series is empty (count == 0) are mapped to null
   * @throws IOException if the metadata cannot be read from disk or the device is unexpectedly
   *     absent
   */
  public Map<String, TimeseriesMetadata> getDeviceTimeSeriesMetadata(
      String filePath,
      TsFileID tsFileID,
      IDeviceID device,
      Set<String> allSensors,
      QueryContext queryContext,
      boolean ignoreNotExists)
      throws IOException {
    long startTime = System.nanoTime();
    long cachePhaseEndTime = 0;
    long loadBloomFilterTime = 0;
    long cacheHitCount = 0;
    long diskLookupCount = 0;
    boolean delegated = false;
    boolean externalTsFile = queryContext.isExternalTsFileScan();
    LongConsumer timeSeriesMetadataIoSizeRecorder =
        queryContext.getQueryStatistics().getLoadTimeSeriesMetadataActualIOSize()::addAndGet;
    LongConsumer bloomFilterIoSizeRecorder =
        queryContext.getQueryStatistics().getLoadBloomFilterActualIOSize()::addAndGet;

    Map<String, TimeseriesMetadata> result = new HashMap<>(allSensors.size());
    List<String> missing = new ArrayList<>(allSensors.size());

    try {
      if (CACHE_ENABLE && !externalTsFile) {
        // fast path without locking: reuse everything the per-measurement cache already has
        collectCachedMetadata(tsFileID, device, allSensors, result, missing);
        cacheHitCount = result.size();
        if (missing.isEmpty()) {
          cachePhaseEndTime = System.nanoTime();
          return result;
        }
        // serialize reads of the same device+file (same lock key as get) so threads don't each
        // deserialize the whole device and race on the shared reader
        synchronized (
            devices.computeIfAbsent(device.toString() + SEPARATOR + filePath, WeakReference::new)) {
          List<String> stillMissing = new ArrayList<>(missing.size());
          collectCachedMetadata(tsFileID, device, missing, result, stillMissing);
          missing = stillMissing;
          cacheHitCount = result.size();
          cachePhaseEndTime = System.nanoTime();
          if (missing.isEmpty()) {
            return result;
          }

          // measurements the cached bloom filter already rejects are known absent in this file, so
          // they must not trigger a device read
          long bloomFilterStartTime = System.nanoTime();
          BloomFilter bloomFilter =
              loadBloomFilter(filePath, tsFileID, queryContext, bloomFilterIoSizeRecorder, false);
          List<String> possiblyPresent = new ArrayList<>(missing.size());
          for (String measurement : missing) {
            if (bloomFilterMayContain(bloomFilter, device, measurement)) {
              possiblyPresent.add(measurement);
            } else {
              result.put(measurement, null);
            }
          }
          loadBloomFilterTime = System.nanoTime() - bloomFilterStartTime;
          diskLookupCount = missing.size();
          if (possiblyPresent.isEmpty()) {
            return result;
          }

          // the time column is always written for an aligned device, so a known-absent time
          // column means the file will be skipped anyway; a bloom-filter false positive on a field
          // must not force a device read and a possible absent-device error
          if (allSensors.contains("") && result.get("") == null && !possiblyPresent.contains("")) {
            return result;
          }

          // for a small remainder, a per-column lookup reads one leaf group instead of the whole
          // device; those lookups record their own statistics
          if (possiblyPresent.size() <= MAX_PER_COLUMN_FALLBACK) {
            delegated = true;
            diskLookupCount = missing.size() - possiblyPresent.size();
            for (String measurement : possiblyPresent) {
              result.put(
                  measurement,
                  get(
                      filePath,
                      new TimeSeriesMetadataCacheKey(tsFileID, device, measurement),
                      allSensors,
                      ignoreNotExists,
                      false,
                      queryContext));
            }
            return result;
          }

          // TsFileSequenceReader has no selective batch-read API that accepts a measurement set,
          // so this call loads every measurement of the device and filters afterwards. A truly
          // selective read requires an upstream apache/tsfile change; the bloom filter and the
          // per-column fallback above keep this branch to measurements that are plausibly present.
          // The measurement-level readTimeseriesMetadata API is group-selective already and is what
          // the per-column path and the fallback use.
          TsFileSequenceReader reader =
              FileReaderManager.getInstance()
                  .get(filePath, tsFileID, true, timeSeriesMetadataIoSizeRecorder, false);
          result.putAll(
              readDeviceTimeSeriesMetadata(
                  reader,
                  filePath,
                  tsFileID,
                  device,
                  new HashSet<>(possiblyPresent),
                  true,
                  ignoreNotExists));
          return result;
        }
      }

      // cache disabled / external tsfiles: probe the time column first so an absent device does
      // not pay for the whole-device read
      long bloomFilterStartTime = System.nanoTime();
      BloomFilter bloomFilter =
          loadBloomFilter(
              filePath, tsFileID, queryContext, bloomFilterIoSizeRecorder, externalTsFile);
      loadBloomFilterTime = System.nanoTime() - bloomFilterStartTime;
      diskLookupCount = allSensors.size();
      if (!bloomFilterMayContain(bloomFilter, device, "")) {
        for (String measurement : allSensors) {
          result.put(measurement, null);
        }
        return result;
      }
      TsFileSequenceReader reader =
          FileReaderManager.getInstance()
              .get(filePath, tsFileID, true, timeSeriesMetadataIoSizeRecorder, externalTsFile);
      return readDeviceTimeSeriesMetadata(
          reader, filePath, tsFileID, device, allSensors, false, ignoreNotExists);
    } finally {
      long endTime = System.nanoTime();
      long cacheLookupTime;
      long diskLookupTime;
      if (delegated) {
        // the per-column fallbacks recorded their own disk time
        cacheLookupTime = cachePhaseEndTime == 0 ? 0 : cachePhaseEndTime - startTime;
        diskLookupTime = 0;
      } else if (cacheHitCount == 0 || cachePhaseEndTime == 0) {
        // cold or cache-disabled call: cache probes and the lock wait belong to the disk lookup
        cacheLookupTime = 0;
        diskLookupTime = endTime - startTime - loadBloomFilterTime;
      } else if (diskLookupCount == 0) {
        cacheLookupTime = endTime - startTime - loadBloomFilterTime;
        diskLookupTime = 0;
      } else {
        cacheLookupTime = cachePhaseEndTime - startTime;
        diskLookupTime = endTime - cachePhaseEndTime - loadBloomFilterTime;
      }
      recordLoadStatistics(
          queryContext,
          cacheHitCount,
          cacheLookupTime,
          diskLookupCount,
          diskLookupTime,
          loadBloomFilterTime);
    }
  }

  private BloomFilter loadBloomFilter(
      String filePath,
      TsFileID tsFileID,
      QueryContext queryContext,
      LongConsumer bloomFilterIoSizeRecorder,
      boolean externalTsFile)
      throws IOException {
    return BloomFilterCache.getInstance()
        .get(
            new BloomFilterCache.BloomFilterCacheKey(filePath, tsFileID),
            false,
            bloomFilterIoSizeRecorder,
            queryContext.getQueryStatistics().getLoadBloomFilterFromCacheCount()::addAndGet,
            queryContext.getQueryStatistics().getLoadBloomFilterFromDiskCount()::addAndGet,
            externalTsFile);
  }

  /** A null filter cannot rule the measurement out. */
  private static boolean bloomFilterMayContain(
      BloomFilter bloomFilter, IDeviceID device, String measurement) {
    return bloomFilter == null
        || bloomFilter.contains(device.toString() + IoTDBConstant.PATH_SEPARATOR + measurement);
  }

  private static void recordLoadStatistics(
      QueryContext queryContext,
      long cacheHitCount,
      long cacheLookupTime,
      long diskLookupCount,
      long diskLookupTime,
      long bloomFilterTime) {
    QueryStatistics statistics = queryContext.getQueryStatistics();
    statistics.getLoadBloomFilterTime().getAndAdd(bloomFilterTime);
    if (cacheHitCount > 0) {
      statistics.getLoadTimeSeriesMetadataFromCacheCount().getAndAdd(cacheHitCount);
      statistics.getLoadTimeSeriesMetadataFromCacheTime().getAndAdd(cacheLookupTime);
    }
    if (diskLookupCount > 0) {
      statistics.getLoadTimeSeriesMetadataFromDiskCount().getAndAdd(diskLookupCount);
      statistics.getLoadTimeSeriesMetadataFromDiskTime().getAndAdd(diskLookupTime);
    }
  }

  /**
   * Resolve cached measurements into {@code resolved} and collect the rest into {@code missing}.
   */
  private void collectCachedMetadata(
      TsFileID tsFileID,
      IDeviceID device,
      Collection<String> measurements,
      Map<String, TimeseriesMetadata> resolved,
      List<String> missing) {
    for (String measurement : measurements) {
      TimeseriesMetadata metadata =
          lruCache.getIfPresent(new TimeSeriesMetadataCacheKey(tsFileID, device, measurement));
      if (metadata == null) {
        missing.add(measurement);
      } else {
        resolved.put(
            measurement,
            metadata == NULL_EXISTS_CACHE_PLACE_HOLDER ? null : new TimeseriesMetadata(metadata));
      }
    }
  }

  private Map<String, TimeseriesMetadata> readDeviceTimeSeriesMetadata(
      TsFileSequenceReader reader,
      String filePath,
      TsFileID tsFileID,
      IDeviceID device,
      Set<String> allSensors,
      boolean cacheEnabled,
      boolean ignoreNotExists)
      throws IOException {
    // Use the chunk-metadata-including variant: the returned TimeseriesMetadata are passed to
    // setModifications and then to DiskAlignedChunkMetadataLoader, which dereferences the chunk
    // metadata list, so readDeviceMetadata (which skips chunk metadata) would NPE at query time.
    List<TimeseriesMetadata> deviceMetadataList = reader.getDeviceTimeseriesMetadata(device);
    if (deviceMetadataList.isEmpty()) {
      // the caller already consulted the bloom filter, so the device could not be ruled out:
      // missing metadata is an inconsistency unless ignoreNotExists is set
      if (ignoreNotExists) {
        return Collections.emptyMap();
      }
      throw new IOException(
          String.format(
              StorageEngineMessages
                  .EXCEPTION_DEVICE_ARG_IS_RECORDED_IN_THE_RESOURCE_BUT_MISSING_FROM_THE_METADATA_OF_TSFILE_ARG_DE00C374,
              device,
              filePath));
    }

    Map<String, TimeseriesMetadata> result = new HashMap<>(allSensors.size());
    for (TimeseriesMetadata metadata : deviceMetadataList) {
      String measurementId = metadata.getMeasurementId();
      if (!allSensors.contains(measurementId)) {
        continue;
      }
      if (metadata.getStatistics().getCount() == 0) {
        result.put(measurementId, null);
        if (cacheEnabled && !ignoreNotExists && memoryConfig.isMayCacheNonExistSeries()) {
          lruCache.put(
              new TimeSeriesMetadataCacheKey(tsFileID, device, measurementId),
              NULL_EXISTS_CACHE_PLACE_HOLDER);
        }
        continue;
      }
      if (cacheEnabled) {
        lruCache.put(new TimeSeriesMetadataCacheKey(tsFileID, device, measurementId), metadata);
      }
      result.put(measurementId, new TimeseriesMetadata(metadata));
    }
    // remember selected measurements that do not exist in this file, so a later lookup of the same
    // device can be served from the cache instead of reading the device again
    for (String measurement : allSensors) {
      if (result.containsKey(measurement)) {
        continue;
      }
      result.put(measurement, null);
      if (cacheEnabled && !ignoreNotExists && memoryConfig.isMayCacheNonExistSeries()) {
        lruCache.put(
            new TimeSeriesMetadataCacheKey(tsFileID, device, measurement),
            NULL_EXISTS_CACHE_PLACE_HOLDER);
      }
    }
    return result;
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
