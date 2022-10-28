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

package org.apache.iotdb.db.engine.storagegroup;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

/**
 * This class manages last time and flush time for sequence and unsequence determination This class
 * This class is NOT thread safe, caller should ensure synchronization
 */
public class LastFlushTimeManager implements ILastFlushTimeManager {
  private static final Logger logger = LoggerFactory.getLogger(LastFlushTimeManager.class);

  /**
   * time partition id -> map, which contains device -> largest timestamp of the latest memtable to
   * be submitted to asyncTryToFlush partitionLatestFlushedTimeForEachDevice determines whether a
   * data point should be put into a sequential file or an unsequential file. Data of some device
   * with timestamp less than or equals to the device's latestFlushedTime should go into an
   * unsequential file.
   */
  private Map<Long, Map<String, Long>> partitionLatestFlushedTimeForEachDevice = new HashMap<>();
  /** used to record the latest flush time while upgrading and inserting */
  private Map<Long, Map<String, Long>> newlyFlushedPartitionLatestFlushedTimeForEachDevice =
      new HashMap<>();
  /**
   * global mapping of device -> largest timestamp of the latest memtable to * be submitted to
   * asyncTryToFlush, globalLatestFlushedTimeForEachDevice is utilized to maintain global
   * latestFlushedTime of devices and will be updated along with
   * partitionLatestFlushedTimeForEachDevice
   */
  private Map<String, Long> globalLatestFlushedTimeForEachDevice = new HashMap<>();

  // region set
  @Override
  public void setMultiDeviceFlushedTime(long timePartitionId, Map<String, Long> flushedTimeMap) {
    partitionLatestFlushedTimeForEachDevice
        .computeIfAbsent(timePartitionId, l -> new HashMap<>())
        .putAll(flushedTimeMap);
  }

  @Override
  public void setOneDeviceFlushedTime(long timePartitionId, String path, long time) {
    partitionLatestFlushedTimeForEachDevice
        .computeIfAbsent(timePartitionId, l -> new HashMap<>())
        .put(path, time);
  }

  @Override
  public void setMultiDeviceGlobalFlushedTime(Map<String, Long> globalFlushedTimeMap) {
    globalLatestFlushedTimeForEachDevice.putAll(globalFlushedTimeMap);
  }

  @Override
  public void setOneDeviceGlobalFlushedTime(String path, long time) {
    globalLatestFlushedTimeForEachDevice.put(path, time);
  }

  // endregion

  // region update

  @Override
  public void updateFlushedTime(long timePartitionId, String path, long time) {
    partitionLatestFlushedTimeForEachDevice
        .computeIfAbsent(timePartitionId, id -> new HashMap<>())
        .compute(path, (k, v) -> v == null ? time : Math.max(v, time));
  }

  @Override
  public void updateGlobalFlushedTime(String path, long time) {
    globalLatestFlushedTimeForEachDevice.compute(
        path, (k, v) -> v == null ? time : Math.max(v, time));
  }

  @Override
  public void updateNewlyFlushedPartitionLatestFlushedTimeForEachDevice(
      long partitionId, String deviceId, long time) {
    newlyFlushedPartitionLatestFlushedTimeForEachDevice
        .computeIfAbsent(partitionId, id -> new HashMap<>())
        .compute(deviceId, (k, v) -> v == null ? time : Math.max(v, time));
  }

  // endregion

  // region ensure
  @Override
  public void ensureFlushedTimePartition(long timePartitionId) {
    partitionLatestFlushedTimeForEachDevice.computeIfAbsent(timePartitionId, id -> new HashMap<>());
  }

  @Override
  public long ensureFlushedTimePartitionAndInit(long timePartitionId, String path, long initTime) {
    return partitionLatestFlushedTimeForEachDevice
        .computeIfAbsent(timePartitionId, id -> new HashMap<>())
        .computeIfAbsent(path, id -> initTime);
  }

  // endregion

  // region upgrade support methods

  @Override
  public void applyNewlyFlushedTimeToFlushedTime() {
    for (Entry<Long, Map<String, Long>> entry :
        newlyFlushedPartitionLatestFlushedTimeForEachDevice.entrySet()) {
      long timePartitionId = entry.getKey();
      Map<String, Long> latestFlushTimeForPartition =
          partitionLatestFlushedTimeForEachDevice.getOrDefault(timePartitionId, new HashMap<>());
      for (Entry<String, Long> endTimeMap : entry.getValue().entrySet()) {
        String device = endTimeMap.getKey();
        long endTime = endTimeMap.getValue();
        if (latestFlushTimeForPartition.getOrDefault(device, Long.MIN_VALUE) < endTime) {
          partitionLatestFlushedTimeForEachDevice
              .computeIfAbsent(timePartitionId, id -> new HashMap<>())
              .put(device, endTime);
        }
      }
    }
  }

  @Override
  public boolean updateLatestFlushTime(long partitionId, Map<String, Long> latestFlushTimeMap) {
    if (latestFlushTimeMap.isEmpty()) {
      return false;
    }
    for (Entry<String, Long> entry : latestFlushTimeMap.entrySet()) {
      partitionLatestFlushedTimeForEachDevice
          .computeIfAbsent(partitionId, id -> new HashMap<>())
          .put(entry.getKey(), entry.getValue());
      updateNewlyFlushedPartitionLatestFlushedTimeForEachDevice(
          partitionId, entry.getKey(), entry.getValue());
      if (globalLatestFlushedTimeForEachDevice.getOrDefault(entry.getKey(), Long.MIN_VALUE)
          < entry.getValue()) {
        globalLatestFlushedTimeForEachDevice.put(entry.getKey(), entry.getValue());
      }
    }
    return true;
  }

  // endregion

  // region query
  @Override
  public long getFlushedTime(long timePartitionId, String path) {
    return partitionLatestFlushedTimeForEachDevice
        .get(timePartitionId)
        .getOrDefault(path, Long.MIN_VALUE);
  }

  @Override
  public long getGlobalFlushedTime(String path) {
    return globalLatestFlushedTimeForEachDevice.getOrDefault(path, Long.MIN_VALUE);
  }

  // endregion

  // region deletion
  @Override
  public void removePartition(long partitionId) {
    partitionLatestFlushedTimeForEachDevice.remove(partitionId);
    newlyFlushedPartitionLatestFlushedTimeForEachDevice.remove(partitionId);
  }
  // endregion

  // region clear
  @Override
  public void clearFlushedTime() {
    partitionLatestFlushedTimeForEachDevice.clear();
  }

  @Override
  public void clearGlobalFlushedTime() {
    globalLatestFlushedTimeForEachDevice.clear();
  }
  // endregion
}
