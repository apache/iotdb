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

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.query.control.FileReaderManager;
import org.apache.iotdb.tsfile.common.cache.Accountable;
import org.apache.iotdb.tsfile.file.metadata.TimeseriesMetadata;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.utils.BloomFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is used to cache <code>TimeSeriesMetadata</code> in IoTDB. The caching strategy is
 * LRU.
 */
public class TimeSeriesMetadataCache {

  private static final Logger logger = LoggerFactory.getLogger(TimeSeriesMetadataCache.class);
  private static final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
  private static final long MEMORY_THRESHOLD_IN_TIME_SERIES_METADATA_CACHE = config
      .getAllocateMemoryForTimeSeriesMetaDataCache();
  private static boolean cacheEnable = config.isMetaDataCacheEnable();

  private final LRULinkedHashMap<TimeSeriesMetadataCacheKey, TimeseriesMetadata> lruCache;

  private AtomicLong cacheHitNum = new AtomicLong();
  private AtomicLong cacheRequestNum = new AtomicLong();

  private final ReadWriteLock lock = new ReentrantReadWriteLock();


  private TimeSeriesMetadataCache() {
    if (cacheEnable) {
      logger
          .info("TimeseriesMetadataCache size = " + MEMORY_THRESHOLD_IN_TIME_SERIES_METADATA_CACHE);
    }
    lruCache = new LRULinkedHashMap<TimeSeriesMetadataCacheKey, TimeseriesMetadata>(
        MEMORY_THRESHOLD_IN_TIME_SERIES_METADATA_CACHE) {

      @Override
      protected long calEntrySize(TimeSeriesMetadataCacheKey key, TimeseriesMetadata value) {
        long currentSize;
        if (count < 10) {
          currentSize = RamUsageEstimator.shallowSizeOf(key) + RamUsageEstimator.sizeOf(value);
          averageSize = ((averageSize * count) + currentSize) / (++count);
        } else if (count < 100000) {
          count++;
          currentSize = averageSize;
        } else {
          averageSize = RamUsageEstimator.shallowSizeOf(key) + RamUsageEstimator.sizeOf(value);
          count = 1;
          currentSize = averageSize;
        }
        key.setRAMSize(currentSize);
        return currentSize;
      }
    };
  }

  public static TimeSeriesMetadataCache getInstance() {
    return TimeSeriesMetadataCache.TimeSeriesMetadataCacheHolder.INSTANCE;
  }

  public TimeseriesMetadata get(TimeSeriesMetadataCacheKey key, Set<String> allSensors)
      throws IOException {
    if (!cacheEnable) {
      // bloom filter part
      TsFileSequenceReader reader = FileReaderManager.getInstance().get(key.filePath, true);
      BloomFilter bloomFilter = reader.readBloomFilter();
      if (bloomFilter != null && !bloomFilter
          .contains(key.device + IoTDBConstant.PATH_SEPARATOR + key.measurement)) {
        return null;
      }
      return reader.readTimeseriesMetadata(new Path(key.device, key.measurement));
    }

    cacheRequestNum.incrementAndGet();

    try {
      lock.readLock().lock();
      if (lruCache.containsKey(key)) {
        cacheHitNum.incrementAndGet();
        printCacheLog(true);
        return lruCache.get(key);
      }
    } finally {
      lock.readLock().unlock();
    }

    try {
      lock.writeLock().lock();
      if (lruCache.containsKey(key)) {
        cacheHitNum.incrementAndGet();
        printCacheLog(true);
        return lruCache.get(key);
      }
      printCacheLog(false);
      // bloom filter part
      TsFileSequenceReader reader = FileReaderManager.getInstance().get(key.filePath, true);
      BloomFilter bloomFilter = reader.readBloomFilter();
      if (bloomFilter != null && !bloomFilter
          .contains(key.device + IoTDBConstant.PATH_SEPARATOR + key.measurement)) {
        return null;
      }
      List<TimeseriesMetadata> timeSeriesMetadataList = reader
          .readTimeseriesMetadata(key.device, allSensors);
      // put TimeSeriesMetadata of all sensors used in this query into cache
      timeSeriesMetadataList.forEach(timeseriesMetadata ->
          lruCache.put(new TimeSeriesMetadataCacheKey(key.filePath, key.device,
              timeseriesMetadata.getMeasurementId()), timeseriesMetadata));
      return lruCache.get(key);
    } catch (IOException e) {
      logger.error("something wrong happened while reading {}", key.filePath);
      throw e;
    } finally {
      lock.writeLock().unlock();
    }

  }


  private void printCacheLog(boolean isHit) {
    if (!logger.isDebugEnabled()) {
      return;
    }
    logger.debug(
        "[TimeSeriesMetadata cache {}hit] The number of requests for cache is {}, hit rate is {}.",
        isHit ? "" : "didn't ", cacheRequestNum.get(),
        cacheHitNum.get() * 1.0 / cacheRequestNum.get());
  }

  public double calculateTimeSeriesMetadataHitRatio() {
    if (cacheRequestNum.get() != 0) {
      return cacheHitNum.get() * 1.0 / cacheRequestNum.get();
    } else {
      return 0;
    }
  }

  public long getUsedMemory() {
    return lruCache.getUsedMemory();
  }

  public long getMaxMemory() {
    return lruCache.getMaxMemory();
  }

  public double getUsedMemoryProportion() {
    return lruCache.getUsedMemoryProportion();
  }

  public long getAverageSize() {
    return lruCache.getAverageSize();
  }

  /**
   * clear LRUCache.
   */
  public void clear() {
    lock.writeLock().lock();
    if (lruCache != null) {
      lruCache.clear();
    }
    lock.writeLock().unlock();
  }

  public void remove(TimeSeriesMetadataCacheKey key) {
    lock.writeLock().lock();
    if (key != null) {
      lruCache.remove(key);
    }
    lock.writeLock().unlock();
  }

  public static class TimeSeriesMetadataCacheKey implements Accountable {

    private String filePath;
    private String device;
    private String measurement;

    private long RAMSize;


    public TimeSeriesMetadataCacheKey(String filePath, String device, String measurement) {
      this.filePath = filePath;
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
      return Objects.equals(filePath, that.filePath) &&
          Objects.equals(device, that.device) &&
          Objects.equals(measurement, that.measurement);
    }

    @Override
    public int hashCode() {
      return Objects.hash(filePath, device, measurement);
    }

    public void setRAMSize(long size) {
      this.RAMSize = size;
    }

    @Override
    public long getRAMSize() {
      return 0;
    }
  }

  /**
   * singleton pattern.
   */
  private static class TimeSeriesMetadataCacheHolder {

    private static final TimeSeriesMetadataCache INSTANCE = new TimeSeriesMetadataCache();
  }
}
