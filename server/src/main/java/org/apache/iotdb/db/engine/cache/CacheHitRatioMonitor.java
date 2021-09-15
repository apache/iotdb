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

import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.engine.flush.FlushManager;
import org.apache.iotdb.db.exception.StartupException;
import org.apache.iotdb.db.rescon.MemTableManager;
import org.apache.iotdb.db.rescon.SystemInfo;
import org.apache.iotdb.db.service.IService;
import org.apache.iotdb.db.service.JMXService;
import org.apache.iotdb.db.service.ServiceType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CacheHitRatioMonitor implements CacheHitRatioMonitorMXBean, IService {

  private static Logger logger = LoggerFactory.getLogger(CacheHitRatioMonitor.class);
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
    logger.info("{}: stop {}...", IoTDBConstant.GLOBAL_DB_NAME, this.getID().getName());
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
