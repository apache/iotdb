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

package org.apache.iotdb.db.pipe.core.collector.realtime;

import org.apache.iotdb.db.pipe.core.collector.realtime.cache.DataRegionChangeDataCache;
import org.apache.iotdb.db.pipe.core.collector.realtime.listener.PipeChangeDataCaptureListener;

import java.util.concurrent.ConcurrentHashMap;

public class PipeRealtimeCollectorManager {
  private ConcurrentHashMap<String, DataRegionChangeDataCache> id2Cache;

  public PipeRealtimeCollectorManager() {}

  private void setId2Cache(ConcurrentHashMap<String, DataRegionChangeDataCache> id2Cache) {
    this.id2Cache = id2Cache;
    PipeChangeDataCaptureListener.getInstance().setDataRegionChangeDataCaches(id2Cache);
  }

  public PipeRealtimeCollector createPipeRealtimeCollector(String pattern, String dataRegionId) {
    return new PipeRealtimeHybridCollector(pattern, dataRegionId, this);
  }

  public synchronized void register(PipeRealtimeCollector collector, String dataRegionId) {
    startListeningDataRegion(dataRegionId);
    id2Cache.get(dataRegionId).register(collector);
  }

  public synchronized void deregister(PipeRealtimeCollector collector, String dataRegionId) {
    if (id2Cache != null && id2Cache.containsKey(dataRegionId)) {
      DataRegionChangeDataCache cache = id2Cache.get(dataRegionId);
      cache.deregister(collector);
      if (cache.getRegisterCount() == 0) {
        stopListeningDataRegion(dataRegionId);
      }
    }
  }

  private void startListeningDataRegion(String dataRegion) {
    if (id2Cache == null) {
      setId2Cache(new ConcurrentHashMap<>());
    }

    id2Cache.putIfAbsent(dataRegion, new DataRegionChangeDataCache());
  }

  private void stopListeningDataRegion(String dataRegionId) {
    if (id2Cache != null) {
      id2Cache.remove(dataRegionId).clear();
    }
  }
}
