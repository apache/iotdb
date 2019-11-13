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
package org.apache.iotdb.db.conf.adapter;

import com.clearspring.analytics.stream.cardinality.HyperLogLog;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class ActiveTimeSeriesCounter implements IActiveTimeSeriesCounter {

  /**
   * Map[StorageGroup, HyperLogLogCounter]
   */
  private static Map<String, HyperLogLog> storageGroupHllMap = new ConcurrentHashMap<>();

  /**
   * Map[StorageGroup, ActiveTimeSeriesRatio]
   */
  private static Map<String, Double> activeRatioMap = new ConcurrentHashMap<>();

  /**
   * LOG2M decide the precision of the HyperLogLog algorithm
   */
  private static final int LOG2M = 13;

  @Override
  public void init(String storageGroup) {
    storageGroupHllMap.put(storageGroup, new HyperLogLog(LOG2M));
    activeRatioMap.put(storageGroup, 0D);
  }

  @Override
  public void offer(String storageGroup, String device, String measurement) {
    storageGroupHllMap.get(storageGroup).offer(device + measurement);
  }

  @Override
  public void updateActiveRatio(String storageGroup) {
    double totalActiveTsNum = 0;
    for (Map.Entry<String, HyperLogLog> entry : storageGroupHllMap.entrySet()) {
      totalActiveTsNum += entry.getValue().cardinality();
    }
    for (Map.Entry<String, HyperLogLog> entry : storageGroupHllMap.entrySet()) {
      double activeRatio = 0;
      if (totalActiveTsNum > 0) {
        activeRatio = entry.getValue().cardinality() / totalActiveTsNum;
      }
      activeRatioMap.put(entry.getKey(), activeRatio);
    }
    storageGroupHllMap.put(storageGroup, new HyperLogLog(LOG2M));
  }

  @Override
  public double getActiveRatio(String storageGroup) {
    return activeRatioMap.get(storageGroup);
  }

  @Override
  public void delete(String storageGroup) {
    storageGroupHllMap.remove(storageGroup);
    activeRatioMap.remove(storageGroup);
  }

  private static class ActiveTimeSeriesCounterHolder {
    private static final ActiveTimeSeriesCounter INSTANCE = new ActiveTimeSeriesCounter();
  }

  public static ActiveTimeSeriesCounter getInstance() {
    return ActiveTimeSeriesCounterHolder.INSTANCE;
  }
}