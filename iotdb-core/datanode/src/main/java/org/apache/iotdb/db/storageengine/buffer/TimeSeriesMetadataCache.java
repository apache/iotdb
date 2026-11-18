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
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
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

  private static final String SEPARATOR = "$";

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

  public TimeseriesMetadata get(
      String filePath,
      TimeSeriesMetadataCacheKey key,
      Set<String> allSensors,
      boolean ignoreNotExists,
      boolean debug,
      QueryContext queryContext,
      long[] deviceMetadataIndexNodeOffset)
      throws IOException {
    return getTimeseriesMetadata(
            filePath,
            key.tsFileID,
            key.device,
            Collections.singleton(key.measurement),
            allSensors,
            ignoreNotExists,
            debug,
            queryContext,
            deviceMetadataIndexNodeOffset)
        .get(key.measurement);
  }

  /**
   * Resolve the metadata of {@code requested} measurements of a device in one file.
   *
   * <p>Cached measurements are returned directly. The remaining ones are checked against the file's
   * bloom filter and then loaded leaf group by leaf group: one pending measurement locates the
   * group it belongs to, that whole group is read and deserialized once, and every requested
   * measurement found in it is resolved. For files that expose a bloom filter, each group is read
   * at most once per call, independent of LRU cache pressure, and groups that contain no requested
   * measurement are never read. Without one (external tsfiles), a measurement the bloom filter
   * cannot rule out is read from its leaf, so a definitively absent measurement may read the leaf
   * that holds its floor entry again; this matches the previous per-column loader.
   *
   * <p>{@code allSensors} is the full selected set; it filters each group read and warms the cache,
   * so a later single-measurement {@link #get} can reuse the group. {@code requested} must be a
   * subset of {@code allSensors}.
   *
   * <p>The returned map holds copies of the cached (or freshly read) metadata, so the caller may
   * apply {@code setModified} and {@code setChunkMetadataLoader} without corrupting cached entries.
   * Load statistics are counted per requested measurement.
   *
   * @param deviceMetadataIndexNodeOffset non-null for external tsfiles, which address the device
   *     metadata index by offset; bloom filtering is skipped in that case
   * @return a map with an entry for every measurement in {@code requested}: a metadata copy when
   *     the series exists and is non-empty, null otherwise
   */
  public Map<String, TimeseriesMetadata> getTimeseriesMetadata(
      String filePath,
      TsFileID tsFileID,
      IDeviceID device,
      Set<String> requested,
      Set<String> allSensors,
      boolean ignoreNotExists,
      boolean debug,
      QueryContext queryContext,
      long[] deviceMetadataIndexNodeOffset)
      throws IOException {
    long startTime = System.nanoTime();
    long cachePhaseEndTime = 0;
    long loadBloomFilterTime = 0;
    long cacheHitCount = 0;
    long diskLookupCount = 0;
    boolean externalTsFile = queryContext.isExternalTsFileScan();
    LongConsumer timeSeriesMetadataIoSizeRecorder =
        queryContext.getQueryStatistics().getLoadTimeSeriesMetadataActualIOSize()::addAndGet;
    LongConsumer bloomFilterIoSizeRecorder =
        queryContext.getQueryStatistics().getLoadBloomFilterActualIOSize()::addAndGet;
    String devicePrefix = device.toString() + IoTDBConstant.PATH_SEPARATOR;

    Map<String, TimeseriesMetadata> result = new HashMap<>(requested.size());
    Set<String> pending = new HashSet<>(requested.size());

    try {
      if (CACHE_ENABLE && !externalTsFile) {
        collectCachedMetadata(tsFileID, device, requested, result, pending, filePath, debug);
        cacheHitCount = result.size();
        if (pending.isEmpty()) {
          cachePhaseEndTime = System.nanoTime();
          return result;
        }
        cachePhaseEndTime = System.nanoTime();
        // serialize reads of the same device+file (same lock key as get) so threads don't each
        // walk the device metadata index and race on the shared reader
        synchronized (deviceLock(device.toString(), filePath)) {
          if (debug) {
            for (String measurement : pending) {
              DEBUG_LOGGER.info(
                  StorageEngineMessages.CACHE_MISS_IN_FILE, device, measurement, filePath);
            }
            DEBUG_LOGGER.info(StorageEngineMessages.DEVICE_ALL_SENSORS, device, allSensors);
          }
          BloomFilter bloomFilter =
              loadBloomFilter(filePath, tsFileID, queryContext, bloomFilterIoSizeRecorder, false);
          if (deviceAbsent(requested, result, bloomFilter, devicePrefix)) {
            fillMissingWithNull(requested, result);
            return result;
          }
          diskLookupCount = requested.size() - cacheHitCount;
          TsFileSequenceReader reader =
              FileReaderManager.getInstance()
                  .get(filePath, tsFileID, true, timeSeriesMetadataIoSizeRecorder, false);
          readAndMerge(
              reader,
              tsFileID,
              device,
              deviceMetadataIndexNodeOffset,
              pending,
              requested,
              allSensors,
              ignoreNotExists,
              true,
              debug,
              result,
              timeSeriesMetadataIoSizeRecorder);
          return result;
        }
      }

      // cache disabled or external tsfiles: nothing was resolved from the cache
      pending.addAll(requested);
      BloomFilter bloomFilter =
          loadBloomFilter(
              filePath, tsFileID, queryContext, bloomFilterIoSizeRecorder, externalTsFile);
      if (deviceAbsent(requested, result, bloomFilter, devicePrefix)) {
        fillMissingWithNull(requested, result);
        return result;
      }
      diskLookupCount = requested.size() - cacheHitCount;
      TsFileSequenceReader reader =
          FileReaderManager.getInstance()
              .get(filePath, tsFileID, true, timeSeriesMetadataIoSizeRecorder, externalTsFile);
      readAndMerge(
          reader,
          tsFileID,
          device,
          deviceMetadataIndexNodeOffset,
          pending,
          requested,
          allSensors,
          ignoreNotExists,
          false,
          debug,
          result,
          timeSeriesMetadataIoSizeRecorder);
      return result;
    } finally {
      long endTime = System.nanoTime();
      long cacheLookupTime;
      long diskLookupTime;
      if (cacheHitCount == 0 || cachePhaseEndTime == 0) {
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

  /**
   * An aligned device always has its time column. When the cache or the bloom filter proves it
   * absent, no selected series of the device can be read from this file. For a single requested
   * series, the bloom filter can rule that series out directly.
   */
  private static boolean deviceAbsent(
      Set<String> requested,
      Map<String, TimeseriesMetadata> result,
      BloomFilter bloomFilter,
      String devicePrefix) {
    if (requested.contains("")) {
      if (result.containsKey("")) {
        return result.get("") == null;
      }
      return bloomFilter != null && !bloomFilter.contains(devicePrefix);
    }
    if (bloomFilter != null && requested.size() == 1) {
      return !bloomFilter.contains(devicePrefix + requested.iterator().next());
    }
    return false;
  }

  /**
   * Reads the pending measurements from the leaf groups that hold them. When the cache is enabled
   * the group siblings are returned as well, so a later single-column get() finds them in the LRU.
   */
  private void readAndMerge(
      TsFileSequenceReader reader,
      TsFileID tsFileID,
      IDeviceID device,
      long[] deviceMetadataIndexNodeOffset,
      Set<String> pending,
      Set<String> requested,
      Set<String> allSensors,
      boolean ignoreNotExists,
      boolean cacheEnabled,
      boolean debug,
      Map<String, TimeseriesMetadata> result,
      LongConsumer ioSizeRecorder)
      throws IOException {
    // cached measurements are already in the result: no need to deserialize them again
    Set<String> returned;
    if (!cacheEnabled || allSensors.equals(requested)) {
      // the aligned batch read requests exactly the set it caches, so only the misses remain
      returned = pending;
    } else {
      returned = new HashSet<>(allSensors);
      returned.removeAll(result.keySet());
    }
    Map<String, TimeseriesMetadata> loaded =
        reader.readTimeseriesMetadata(
            device,
            deviceMetadataIndexNodeOffset,
            pending,
            returned,
            ignoreNotExists,
            ioSizeRecorder);
    for (Map.Entry<String, TimeseriesMetadata> entry : loaded.entrySet()) {
      String measurement = entry.getKey();
      TimeseriesMetadata metadata = entry.getValue();
      if (metadata.getStatistics().getCount() == 0) {
        if (debug) {
          DEBUG_LOGGER.info(
              StorageEngineMessages.FILE_NO_SUCH_TIME_SERIES,
              new TimeSeriesMetadataCacheKey(tsFileID, device, measurement));
        }
        if (requested.contains(measurement)) {
          result.put(measurement, null);
        }
        cacheNonExistenceIfNeeded(tsFileID, device, measurement, cacheEnabled, ignoreNotExists);
      } else {
        if (cacheEnabled) {
          lruCache.put(new TimeSeriesMetadataCacheKey(tsFileID, device, measurement), metadata);
        }
        if (requested.contains(measurement)) {
          result.put(measurement, new TimeseriesMetadata(metadata));
        }
      }
    }
    fillMissingWithNull(requested, result);
  }

  private static void fillMissingWithNull(
      Set<String> requested, Map<String, TimeseriesMetadata> result) {
    for (String measurement : requested) {
      result.putIfAbsent(measurement, null);
    }
  }

  private void cacheNonExistenceIfNeeded(
      TsFileID tsFileID,
      IDeviceID device,
      String measurement,
      boolean cacheEnabled,
      boolean ignoreNotExists) {
    if (cacheEnabled && !ignoreNotExists && memoryConfig.isMayCacheNonExistSeries()) {
      lruCache.put(
          new TimeSeriesMetadataCacheKey(tsFileID, device, measurement),
          NULL_EXISTS_CACHE_PLACE_HOLDER);
    }
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
    if (diskLookupCount > 0 || diskLookupTime > 0) {
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
      Collection<String> missing,
      String filePath,
      boolean debug) {
    for (String measurement : measurements) {
      TimeSeriesMetadataCacheKey key =
          new TimeSeriesMetadataCacheKey(tsFileID, device, measurement);
      TimeseriesMetadata metadata = lruCache.getIfPresent(key);
      if (metadata == null) {
        missing.add(measurement);
      } else if (metadata == NULL_EXISTS_CACHE_PLACE_HOLDER) {
        if (debug) {
          DEBUG_LOGGER.info(StorageEngineMessages.FILE_NO_SUCH_TIME_SERIES, key);
        }
        resolved.put(measurement, null);
      } else {
        if (debug) {
          DEBUG_LOGGER.info(
              StorageEngineMessages.STORAGE_LOG_GET_TIMESERIES_METADATA_IN_FILE_FROM_CACHE_36652729,
              device,
              measurement,
              filePath,
              metadata);
        }
        resolved.put(measurement, new TimeseriesMetadata(metadata));
      }
    }
  }

  /**
   * Striped, strongly referenced monitor for one device+file. A WeakHashMap entry can be reclaimed
   * by GC between calls, which would let two threads hold "the same" lock simultaneously.
   */
  private static Object deviceLock(String deviceString, String filePath) {
    return DEVICE_LOCKS[
        Math.floorMod((deviceString + SEPARATOR + filePath).hashCode(), DEVICE_LOCK_STRIPES)];
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
    private final int hash;

    public TimeSeriesMetadataCacheKey(TsFileID tsFileID, IDeviceID device, String measurement) {
      this.tsFileID = tsFileID;
      this.device = device;
      this.measurement = measurement;
      // a key is built for every lookup and every write: compute the hash once instead of
      // allocating the Objects.hash argument array on each call
      this.hash = Objects.hash(tsFileID, device, measurement);
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
      return hash;
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
