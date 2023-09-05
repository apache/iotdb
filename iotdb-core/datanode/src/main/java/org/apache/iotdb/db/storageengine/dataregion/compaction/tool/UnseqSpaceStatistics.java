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

package org.apache.iotdb.db.storageengine.dataregion.compaction.tool;

import java.util.HashMap;
import java.util.Map;

public class UnseqSpaceStatistics {
  public long unsequenceFileNum = 0;
  public long unsequenceFileSize = 0;

  public long unsequenceChunkNum = 0;
  public long unsequenceChunkGroupNum = 0;

  public long minStartTime = Long.MAX_VALUE;

  public long maxEndTime = Long.MIN_VALUE;
  private Map<String, Map<String, ITimeRange>> chunkStatisticMap = new HashMap<>();

  private Map<String, ITimeRange> chunkGroupStatisticMap = new HashMap<>();

  public void updateMeasurement(String device, String measurementUID, Interval interval) {
    chunkStatisticMap
        .computeIfAbsent(device, key -> new HashMap<>())
        .computeIfAbsent(measurementUID, key -> new ListTimeRangeImpl())
        .addInterval(interval);
  }

  public void updateDevice(String device, Interval interval) {
    chunkGroupStatisticMap
        .computeIfAbsent(device, key -> new ListTimeRangeImpl())
        .addInterval(interval);
  }

  public boolean chunkHasOverlap(String device, String measurementUID, Interval interval) {
    if (!chunkStatisticMap.containsKey(device)) {
      return false;
    }
    if (!chunkStatisticMap.get(device).containsKey(measurementUID)) {
      return false;
    }
    return chunkStatisticMap.get(device).get(measurementUID).isOverlapped(interval);
  }

  public boolean chunkGroupHasOverlap(String device, Interval interval) {
    if (!chunkGroupStatisticMap.containsKey(device)) {
      return false;
    }
    return chunkGroupStatisticMap.get(device).isOverlapped(interval);
  }

  public Map<String, Map<String, ITimeRange>> getChunkStatisticMap() {
    return chunkStatisticMap;
  }

  public Map<String, ITimeRange> getChunkGroupStatisticMap() {
    return chunkGroupStatisticMap;
  }

  public void setMaxEndTime(long maxEndTime) {
    this.maxEndTime = Math.max(this.maxEndTime, maxEndTime);
  }

  public void setMinStartTime(long minStartTime) {
    this.minStartTime = Math.min(this.minStartTime, minStartTime);
  }
}
