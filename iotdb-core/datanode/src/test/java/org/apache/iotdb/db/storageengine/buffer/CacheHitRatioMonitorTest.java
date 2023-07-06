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

import org.apache.iotdb.commons.exception.StartupException;
import org.apache.iotdb.commons.service.ServiceType;
import org.apache.iotdb.db.storageengine.dataregion.flush.FlushManager;
import org.apache.iotdb.db.storageengine.rescon.memory.MemTableManager;
import org.apache.iotdb.db.storageengine.rescon.memory.SystemInfo;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class CacheHitRatioMonitorTest {

  @Test
  public void testCacheHitRatioMonitor() {
    final double delta = 0.000001;
    ChunkCache.getInstance().clear();
    TimeSeriesMetadataCache.getInstance().clear();
    BloomFilterCache.getInstance().clear();

    CacheHitRatioMonitor cacheHitRatioMonitor = CacheHitRatioMonitor.getInstance();
    try {
      cacheHitRatioMonitor.start();
      assertEquals(ServiceType.CACHE_HIT_RATIO_DISPLAY_SERVICE, cacheHitRatioMonitor.getID());
      assertTrue(
          cacheHitRatioMonitor.getChunkHitRatio() >= 0.0d
              && cacheHitRatioMonitor.getChunkHitRatio() <= 1.0d);
      assertTrue(cacheHitRatioMonitor.getChunkEvictionCount() >= 0);
      assertEquals(
          ChunkCache.getInstance().getMaxMemory(), cacheHitRatioMonitor.getChunkCacheMaxMemory());
      assertTrue(cacheHitRatioMonitor.getChunkCacheAverageLoadPenalty() >= 0.0);
      assertTrue(cacheHitRatioMonitor.getChunkCacheAverageSize() >= 0);

      assertTrue(
          cacheHitRatioMonitor.getTimeSeriesMetadataHitRatio() >= 0.0d
              && cacheHitRatioMonitor.getTimeSeriesMetadataHitRatio() <= 1.0d);
      assertTrue(cacheHitRatioMonitor.getTimeSeriesMetadataCacheEvictionCount() >= 0);
      assertEquals(
          TimeSeriesMetadataCache.getInstance().getMaxMemory(),
          cacheHitRatioMonitor.getTimeSeriesMetadataCacheMaxMemory());
      assertTrue(cacheHitRatioMonitor.getTimeSeriesCacheAverageLoadPenalty() >= 0.0);
      assertTrue(cacheHitRatioMonitor.getTimeSeriesMetaDataCacheAverageSize() >= 0);

      assertTrue(
          cacheHitRatioMonitor.getBloomFilterHitRatio() >= 0.0d
              && cacheHitRatioMonitor.getBloomFilterHitRatio() <= 1.0d);
      assertTrue(cacheHitRatioMonitor.getBloomFilterCacheEvictionCount() >= 0);
      assertEquals(
          BloomFilterCache.getInstance().getMaxMemory(),
          cacheHitRatioMonitor.getBloomFilterCacheMaxMemory());
      assertTrue(cacheHitRatioMonitor.getBloomFilterCacheAverageLoadPenalty() >= 0.0);
      assertTrue(cacheHitRatioMonitor.getBloomFilterCacheAverageSize() >= 0);

      assertEquals(
          SystemInfo.getInstance().getTotalMemTableSize(),
          cacheHitRatioMonitor.getTotalMemTableSize());
      assertEquals(
          SystemInfo.getInstance().getFlushThershold(),
          cacheHitRatioMonitor.getFlushThershold(),
          delta);
      assertEquals(
          SystemInfo.getInstance().getRejectThershold(),
          cacheHitRatioMonitor.getRejectThershold(),
          delta);
      assertEquals(
          FlushManager.getInstance().getNumberOfWorkingTasks(),
          cacheHitRatioMonitor.flushingMemTableNum());
      assertEquals(
          MemTableManager.getInstance().getCurrentMemtableNumber(),
          cacheHitRatioMonitor.totalMemTableNum());

    } catch (StartupException e) {
      e.printStackTrace();
      fail(e.getMessage());
    } finally {
      cacheHitRatioMonitor.stop();
    }
  }
}
