/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.storageengine.dataregion;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class HashLastFlushTimeMap implements ILastFlushTimeMap {

  private static final Logger logger = LoggerFactory.getLogger(HashLastFlushTimeMap.class);

  /**
   * String basic total, 40B
   *
   * <ul>
   *   <li>Object header: Mark Word + Classic Pointer, 12B
   *   <li>char[] reference 4B
   *   <li>hash code, 4B
   *   <li>padding 4B
   *   <li>char[] header + length 16B
   * </ul>
   */
  long STRING_BASE_SIZE = 40;

  long LONG_SIZE = 24;

  long HASHMAP_NODE_BASIC_SIZE = 14 + STRING_BASE_SIZE + LONG_SIZE;

  /**
   * time partition id -> map, which contains device -> largest timestamp of the latest memtable to
   * be submitted to asyncTryToFlush partitionLatestFlushedTimeForEachDevice determines whether a
   * data point should be put into a sequential file or an unsequential file. Data of some device
   * with timestamp less than or equals to the device's latestFlushedTime should go into an
   * unsequential file.
   *
   * <p>It is used to separate sequence and unsequence data.
   */
  private final Map<Long, ILastFlushTime> partitionLatestFlushedTime = new ConcurrentHashMap<>();

  /**
   * global mapping of device -> largest timestamp of the latest memtable to * be submitted to
   * asyncTryToFlush, globalLatestFlushedTimeForEachDevice is utilized to maintain global
   * latestFlushedTime of devices and will be updated along with
   * partitionLatestFlushedTimeForEachDevice
   *
   * <p>It is used to update last cache.
   */
  private final Map<String, Long> globalLatestFlushedTimeForEachDevice = new ConcurrentHashMap<>();

  /** record memory cost of map for each partitionId */
  private final Map<Long, Long> memCostForEachPartition = new ConcurrentHashMap<>();

  // For load
  @Override
  public void updateOneDeviceFlushedTime(long timePartitionId, String deviceId, long time) {
    ILastFlushTime flushTimeMapForPartition =
        partitionLatestFlushedTime.computeIfAbsent(
            timePartitionId, id -> new DeviceLastFlushTime());
    long lastFlushTime = flushTimeMapForPartition.getLastFlushTime(deviceId);
    if (lastFlushTime == Long.MIN_VALUE) {
      long memCost = HASHMAP_NODE_BASIC_SIZE + 2L * deviceId.length();
      memCostForEachPartition.compute(
          timePartitionId, (k1, v1) -> v1 == null ? memCost : v1 + memCost);
    }
    flushTimeMapForPartition.updateLastFlushTime(deviceId, time);
  }

  // For recover
  @Override
  public void updateMultiDeviceFlushedTime(long timePartitionId, Map<String, Long> flushedTimeMap) {
    ILastFlushTime flushTimeMapForPartition =
        partitionLatestFlushedTime.computeIfAbsent(
            timePartitionId, id -> new DeviceLastFlushTime());

    long memIncr = 0;
    for (Map.Entry<String, Long> entry : flushedTimeMap.entrySet()) {
      if (flushTimeMapForPartition.getLastFlushTime(entry.getKey()) == Long.MIN_VALUE) {
        memIncr += HASHMAP_NODE_BASIC_SIZE + 2L * entry.getKey().length();
      }
      flushTimeMapForPartition.updateLastFlushTime(entry.getKey(), entry.getValue());
    }
    long finalMemIncr = memIncr;
    memCostForEachPartition.compute(
        timePartitionId, (k1, v1) -> v1 == null ? finalMemIncr : v1 + finalMemIncr);
  }

  @Override
  public void updateOneDeviceGlobalFlushedTime(String path, long time) {
    globalLatestFlushedTimeForEachDevice.compute(
        path, (k, v) -> v == null ? time : Math.max(v, time));
  }

  @Override
  public void updateMultiDeviceGlobalFlushedTime(Map<String, Long> globalFlushedTimeMap) {
    for (Map.Entry<String, Long> entry : globalFlushedTimeMap.entrySet()) {
      globalLatestFlushedTimeForEachDevice.merge(entry.getKey(), entry.getValue(), Math::max);
    }
  }

  @Override
  public boolean checkAndCreateFlushedTimePartition(long timePartitionId) {
    if (!partitionLatestFlushedTime.containsKey(timePartitionId)) {
      partitionLatestFlushedTime.put(timePartitionId, new DeviceLastFlushTime());
      return false;
    }
    return true;
  }

  // For insert
  @Override
  public void updateLatestFlushTime(long partitionId, Map<String, Long> updateMap) {
    for (Map.Entry<String, Long> entry : updateMap.entrySet()) {
      partitionLatestFlushedTime
          .computeIfAbsent(partitionId, id -> new DeviceLastFlushTime())
          .updateLastFlushTime(entry.getKey(), entry.getValue());
      if (globalLatestFlushedTimeForEachDevice.getOrDefault(entry.getKey(), Long.MIN_VALUE)
          < entry.getValue()) {
        globalLatestFlushedTimeForEachDevice.put(entry.getKey(), entry.getValue());
      }
    }
  }

  @Override
  public long getFlushedTime(long timePartitionId, String deviceId) {
    return partitionLatestFlushedTime.get(timePartitionId).getLastFlushTime(deviceId);
  }

  @Override
  public long getGlobalFlushedTime(String path) {
    return globalLatestFlushedTimeForEachDevice.getOrDefault(path, Long.MIN_VALUE);
  }

  @Override
  public void clearFlushedTime() {
    partitionLatestFlushedTime.clear();
  }

  @Override
  public void clearGlobalFlushedTime() {
    globalLatestFlushedTimeForEachDevice.clear();
  }

  @Override
  public void degradeLastFlushTime(long partitionId) {
    partitionLatestFlushedTime.computeIfPresent(
        partitionId, (id, lastFlushTime) -> lastFlushTime.degradeLastFlushTime());
    memCostForEachPartition.put(partitionId, (long) Long.BYTES);
  }

  @Override
  public long getMemSize(long partitionId) {
    if (memCostForEachPartition.containsKey(partitionId)) {
      return memCostForEachPartition.get(partitionId);
    }
    return 0;
  }
}
