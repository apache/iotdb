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

import org.apache.iotdb.db.metadata.idtable.IDTable;
import org.apache.iotdb.db.metadata.idtable.entry.DeviceEntry;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class IDTableLastFlushTimeMap implements ILastFlushTimeMap {

  IDTable idTable;

  TsFileManager tsFileManager;

  /** record memory cost of map for each partitionId */
  private Map<Long, Long> memCostForEachPartition = new HashMap<>();

  public IDTableLastFlushTimeMap(IDTable idTable, TsFileManager tsFileManager) {
    this.idTable = idTable;
    this.tsFileManager = tsFileManager;
  }

  @Override
  public void setMultiDeviceFlushedTime(long timePartitionId, Map<String, Long> flushedTimeMap) {
    for (Map.Entry<String, Long> entry : flushedTimeMap.entrySet()) {
      if (idTable.getDeviceEntry(entry.getKey()).putFlushTimeMap(timePartitionId, entry.getValue())
          == null) {
        long memCost = HASHMAP_NODE_BASIC_SIZE + 2L * entry.getKey().length();
        memCostForEachPartition.compute(
            timePartitionId, (k, v) -> v == null ? memCost : v + memCost);
      }
    }
  }

  @Override
  public void setOneDeviceFlushedTime(long timePartitionId, String path, long time) {
    if (idTable.getDeviceEntry(path).putFlushTimeMap(timePartitionId, time) != null) {
      long memCost = HASHMAP_NODE_BASIC_SIZE + 2L * path.length();
      memCostForEachPartition.compute(timePartitionId, (k, v) -> v == null ? memCost : v + memCost);
    }
  }

  @Override
  public void setMultiDeviceGlobalFlushedTime(Map<String, Long> globalFlushedTimeMap) {
    for (Map.Entry<String, Long> entry : globalFlushedTimeMap.entrySet()) {
      idTable.getDeviceEntry(entry.getKey()).setGlobalFlushTime(entry.getValue());
    }
  }

  @Override
  public void setOneDeviceGlobalFlushedTime(String path, long time) {
    idTable.getDeviceEntry(path).setGlobalFlushTime(time);
  }

  @Override
  public void updateFlushedTime(long timePartitionId, String path, long time) {
    idTable.getDeviceEntry(path).updateFlushTimeMap(timePartitionId, time);
  }

  @Override
  public void updateGlobalFlushedTime(String path, long time) {
    idTable.getDeviceEntry(path).updateGlobalFlushTime(time);
  }

  @Override
  public void updateNewlyFlushedPartitionLatestFlushedTimeForEachDevice(
      long partitionId, String deviceId, long time) {
    throw new UnsupportedOperationException("IDTableFlushTimeManager doesn't support upgrade");
  }

  @Override
  public boolean checkAndCreateFlushedTimePartition(long timePartitionId) {
    return false;
  }

  @Override
  public void applyNewlyFlushedTimeToFlushedTime() {
    throw new UnsupportedOperationException("IDTableFlushTimeManager doesn't support upgrade");
  }

  @Override
  public boolean updateLatestFlushTime(long partitionId, Map<String, Long> updateMap) {
    if (updateMap.isEmpty()) {
      return false;
    }

    for (Map.Entry<String, Long> entry : updateMap.entrySet()) {
      DeviceEntry deviceEntry = idTable.getDeviceEntry(entry.getKey());
      deviceEntry.updateFlushTimeMap(partitionId, entry.getValue());
      if (deviceEntry.getGlobalFlushTime() < entry.getValue()) {
        deviceEntry.setGlobalFlushTime(entry.getValue());
      }
    }
    return true;
  }

  @Override
  public long getFlushedTime(long timePartitionId, String path) {
    Long flushTime = idTable.getDeviceEntry(path).getFlushTime(timePartitionId);
    if (flushTime != null) {
      return flushTime;
    }
    long time = recoverFlushTime(timePartitionId, path);
    idTable.getDeviceEntry(path).updateFlushTimeMap(timePartitionId, time);
    return time;
  }

  @Override
  public long getGlobalFlushedTime(String path) {
    return idTable.getDeviceEntry(path).getGlobalFlushTime();
  }

  @Override
  public void clearFlushedTime() {
    for (DeviceEntry deviceEntry : idTable.getAllDeviceEntry()) {
      deviceEntry.clearFlushTime();
    }
  }

  @Override
  public void clearGlobalFlushedTime() {
    for (DeviceEntry deviceEntry : idTable.getAllDeviceEntry()) {
      deviceEntry.setGlobalFlushTime(Long.MIN_VALUE);
    }
  }

  @Override
  public void removePartition(long partitionId) {
    for (DeviceEntry deviceEntry : idTable.getAllDeviceEntry()) {
      deviceEntry.removePartition(partitionId);
    }
  }

  private long recoverFlushTime(long partitionId, String devicePath) {
    List<TsFileResource> tsFileResourceList =
        tsFileManager.getSequenceListByTimePartition(partitionId);

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
