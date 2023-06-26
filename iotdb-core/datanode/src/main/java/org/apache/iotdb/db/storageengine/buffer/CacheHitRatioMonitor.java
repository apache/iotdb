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
import org.apache.iotdb.commons.exception.StartupException;
import org.apache.iotdb.commons.service.IService;
import org.apache.iotdb.commons.service.JMXService;
import org.apache.iotdb.commons.service.ServiceType;
import org.apache.iotdb.db.storageengine.dataregion.flush.FlushManager;
import org.apache.iotdb.db.storageengine.rescon.memory.MemTableManager;
import org.apache.iotdb.db.storageengine.rescon.memory.SystemInfo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("squid:S6548")
public class CacheHitRatioMonitor implements CacheHitRatioMonitorMXBean, IService {

  private static final Logger LOGGER = LoggerFactory.getLogger(CacheHitRatioMonitor.class);
  static final CacheHitRatioMonitor instance = AsyncCacheHitRatioHolder.DISPLAYER;

  @Override
  public void start() throws StartupException {
    try {
      JMXService.registerMBean(instance, ServiceType.CACHE_HIT_RATIO_DISPLAY_SERVICE.getJmxName());
    } catch (Exception e) {
      throw new StartupException(this.getID().getName(), e.getMessage());
    }
  }

  @Override
  public void stop() {
    JMXService.deregisterMBean(ServiceType.CACHE_HIT_RATIO_DISPLAY_SERVICE.getJmxName());
    LOGGER.info("{}: stop {}...", IoTDBConstant.GLOBAL_DB_NAME, this.getID().getName());
  }

  @Override
  public ServiceType getID() {
    return ServiceType.CACHE_HIT_RATIO_DISPLAY_SERVICE;
  }

  @Override
  public double getChunkHitRatio() {
    return ChunkCache.getInstance().calculateChunkHitRatio();
  }

  @Override
  public long getChunkEvictionCount() {
    return ChunkCache.getInstance().getEvictionCount();
  }

  @Override
  public long getChunkCacheMaxMemory() {
    return ChunkCache.getInstance().getMaxMemory();
  }

  @Override
  public double getChunkCacheAverageLoadPenalty() {
    return ChunkCache.getInstance().getAverageLoadPenalty();
  }

  @Override
  public long getChunkCacheAverageSize() {
    return ChunkCache.getInstance().getAverageSize();
  }

  @Override
  public double getTimeSeriesMetadataHitRatio() {
    return TimeSeriesMetadataCache.getInstance().calculateTimeSeriesMetadataHitRatio();
  }

  @Override
  public long getTimeSeriesMetadataCacheEvictionCount() {
    return TimeSeriesMetadataCache.getInstance().getEvictionCount();
  }

  @Override
  public long getTimeSeriesMetadataCacheMaxMemory() {
    return TimeSeriesMetadataCache.getInstance().getMaxMemory();
  }

  @Override
  public double getTimeSeriesCacheAverageLoadPenalty() {
    return TimeSeriesMetadataCache.getInstance().getAverageLoadPenalty();
  }

  @Override
  public long getTimeSeriesMetaDataCacheAverageSize() {
    return TimeSeriesMetadataCache.getInstance().getAverageSize();
  }

  @Override
  public double getBloomFilterHitRatio() {
    return BloomFilterCache.getInstance().calculateBloomFilterHitRatio();
  }

  @Override
  public long getBloomFilterCacheEvictionCount() {
    return BloomFilterCache.getInstance().getEvictionCount();
  }

  @Override
  public long getBloomFilterCacheMaxMemory() {
    return BloomFilterCache.getInstance().getMaxMemory();
  }

  @Override
  public double getBloomFilterCacheAverageLoadPenalty() {
    return BloomFilterCache.getInstance().getAverageLoadPenalty();
  }

  @Override
  public long getBloomFilterCacheAverageSize() {
    return BloomFilterCache.getInstance().getAverageSize();
  }

  public static CacheHitRatioMonitor getInstance() {
    return instance;
  }

  private static class AsyncCacheHitRatioHolder {

    private static final CacheHitRatioMonitor DISPLAYER = new CacheHitRatioMonitor();

    private AsyncCacheHitRatioHolder() {}
  }

  @Override
  public long getTotalMemTableSize() {
    return SystemInfo.getInstance().getTotalMemTableSize();
  }

  @Override
  public double getFlushThershold() {
    return SystemInfo.getInstance().getFlushThershold();
  }

  @Override
  public double getRejectThershold() {
    return SystemInfo.getInstance().getRejectThershold();
  }

  @Override
  public int flushingMemTableNum() {
    return FlushManager.getInstance().getNumberOfWorkingTasks();
  }

  @Override
  public int totalMemTableNum() {
    return MemTableManager.getInstance().getCurrentMemtableNumber();
  }
}
