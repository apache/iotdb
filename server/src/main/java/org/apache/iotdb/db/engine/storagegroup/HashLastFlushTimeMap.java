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
import java.util.List;
import java.util.Map;

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

  /** used for recovering flush time from tsfile resource */
  TsFileManager tsFileManager;

  /** record memory cost of map for each partitionId */
  private Map<Long, Long> memCostForEachPartition = new HashMap<>();

  public HashLastFlushTimeMap(TsFileManager tsFileManager) {
    this.tsFileManager = tsFileManager;
  }

  @Override
  public void setMultiDeviceFlushedTime(long timePartitionId, Map<String, Long> flushedTimeMap) {
    Map<String, Long> flushTimeMapForPartition =
        partitionLatestFlushedTimeForEachDevice.get(timePartitionId);
    if (flushTimeMapForPartition == null) {
      return;
    }
    long memIncr = 0;
    for (Map.Entry<String, Long> entry : flushedTimeMap.entrySet()) {
      if (flushTimeMapForPartition.put(entry.getKey(), entry.getValue()) == null) {
        memIncr += HASHMAP_NODE_BASIC_SIZE + 2L * entry.getKey().length();
      }
    }
    long finalMemIncr = memIncr;
    memCostForEachPartition.compute(
        timePartitionId, (k1, v1) -> v1 == null ? finalMemIncr : v1 + finalMemIncr);
  }

  @Override
  public void setOneDeviceFlushedTime(long timePartitionId, String path, long time) {
    Map<String, Long> flushTimeMapForPartition =
        partitionLatestFlushedTimeForEachDevice.get(timePartitionId);
    if (flushTimeMapForPartition == null) {
      return;
    }
    if (flushTimeMapForPartition.put(path, time) == null) {
      long memCost = HASHMAP_NODE_BASIC_SIZE + 2L * path.length();
      memCostForEachPartition.compute(
          timePartitionId, (k1, v1) -> v1 == null ? memCost : v1 + memCost);
    }
  }

  @Override
  public void setMultiDeviceGlobalFlushedTime(Map<String, Long> globalFlushedTimeMap) {
    globalLatestFlushedTimeForEachDevice.putAll(globalFlushedTimeMap);
  }

  @Override
  public void setOneDeviceGlobalFlushedTime(String path, long time) {
    globalLatestFlushedTimeForEachDevice.put(path, time);
  }

  @Override
  public void updateFlushedTime(long timePartitionId, String path, long time) {
    Map<String, Long> flushTimeMapForPartition =
        partitionLatestFlushedTimeForEachDevice.get(timePartitionId);
    if (flushTimeMapForPartition == null) {
      return;
    }
    flushTimeMapForPartition.compute(
        path,
        (k, v) -> {
          if (v == null) {
            long memCost = HASHMAP_NODE_BASIC_SIZE + 2L * path.length();
            memCostForEachPartition.compute(
                timePartitionId, (k1, v1) -> v1 == null ? memCost : v1 + memCost);
            return time;
          }
          return Math.max(v, time);
        });
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

  @Override
  public boolean checkAndCreateFlushedTimePartition(long timePartitionId) {
    if (!partitionLatestFlushedTimeForEachDevice.containsKey(timePartitionId)) {
      partitionLatestFlushedTimeForEachDevice.put(timePartitionId, new HashMap<>());
      return false;
    }
    return true;
  }

  @Override
  public void applyNewlyFlushedTimeToFlushedTime() {
    for (Map.Entry<Long, Map<String, Long>> entry :
        newlyFlushedPartitionLatestFlushedTimeForEachDevice.entrySet()) {
      long timePartitionId = entry.getKey();
      Map<String, Long> latestFlushTimeForPartition =
          partitionLatestFlushedTimeForEachDevice.getOrDefault(timePartitionId, new HashMap<>());
      for (Map.Entry<String, Long> endTimeMap : entry.getValue().entrySet()) {
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
  public void updateLatestFlushTime(long partitionId, Map<String, Long> updateMap) {
    for (Map.Entry<String, Long> entry : updateMap.entrySet()) {
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
  }

  @Override
  public long getFlushedTime(long timePartitionId, String path) {
    return partitionLatestFlushedTimeForEachDevice
        .get(timePartitionId)
        .computeIfAbsent(path, k -> recoverFlushTime(timePartitionId, path));
  }

  @Override
  public long getGlobalFlushedTime(String path) {
    return globalLatestFlushedTimeForEachDevice.getOrDefault(path, Long.MIN_VALUE);
  }

  @Override
  public void clearFlushedTime() {
    partitionLatestFlushedTimeForEachDevice.clear();
  }

  @Override
  public void clearGlobalFlushedTime() {
    globalLatestFlushedTimeForEachDevice.clear();
  }

  @Override
  public void removePartition(long partitionId) {
    partitionLatestFlushedTimeForEachDevice.remove(partitionId);
    memCostForEachPartition.remove(partitionId);
  }

  private long recoverFlushTime(long partitionId, String devicePath) {
    List<TsFileResource> tsFileResourceList =
        tsFileManager.getOrCreateSequenceListByTimePartition(partitionId);

    for (int i = tsFileResourceList.size() - 1; i >= 0; i--) {
      if (tsFileResourceList.get(i).timeIndex.mayContainsDevice(devicePath)) {
        return tsFileResourceList.get(i).timeIndex.getEndTime(devicePath);
      }
    }

    long memCost = HASHMAP_NODE_BASIC_SIZE + 2L * devicePath.length();
    memCostForEachPartition.compute(partitionId, (k, v) -> v == null ? memCost : v + memCost);
    return Long.MIN_VALUE;
  }

  @Override
  public long getMemSize(long partitionId) {
    if (memCostForEachPartition.containsKey(partitionId)) {
      return memCostForEachPartition.get(partitionId);
    }
    return 0;
  }
}
