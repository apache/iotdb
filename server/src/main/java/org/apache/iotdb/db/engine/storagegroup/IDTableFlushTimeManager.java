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

import java.util.Map;

/**
 * This class manages last time and flush time for sequence and unsequence determination This class
 * This class is NOT thread safe, caller should ensure synchronization This class not support
 * upgrade
 */
public class IDTableFlushTimeManager implements ILastFlushTimeManager {
  IDTable idTable;

  public IDTableFlushTimeManager(IDTable idTable) {
    this.idTable = idTable;
  }

  // region set
  @Override
  public void setLastTimeAll(long timePartitionId, Map<String, Long> lastTimeMap) {
    for (Map.Entry<String, Long> entry : lastTimeMap.entrySet()) {
      idTable.getDeviceEntry(entry.getKey()).putLastTimeMap(timePartitionId, entry.getValue());
    }
  }

  @Override
  public void setLastTime(long timePartitionId, String path, long time) {
    idTable.getDeviceEntry(path).putLastTimeMap(timePartitionId, time);
  }

  @Override
  public void setFlushedTimeAll(long timePartitionId, Map<String, Long> flushedTimeMap) {
    for (Map.Entry<String, Long> entry : flushedTimeMap.entrySet()) {
      idTable.getDeviceEntry(entry.getKey()).putFlushTimeMap(timePartitionId, entry.getValue());
    }
  }

  @Override
  public void setFlushedTime(long timePartitionId, String path, long time) {
    idTable.getDeviceEntry(path).putFlushTimeMap(timePartitionId, time);
  }

  @Override
  public void setGlobalFlushedTimeAll(Map<String, Long> globalFlushedTimeMap) {
    for (Map.Entry<String, Long> entry : globalFlushedTimeMap.entrySet()) {
      idTable.getDeviceEntry(entry.getKey()).setGlobalFlushTime(entry.getValue());
    }
  }

  @Override
  public void setGlobalFlushedTime(String path, long time) {
    idTable.getDeviceEntry(path).setGlobalFlushTime(time);
  }

  // endregion

  // region update

  @Override
  public void updateLastTime(long timePartitionId, String path, long time) {
    idTable.getDeviceEntry(path).updateLastTimeMap(timePartitionId, time);
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

  // endregion

  // region ensure

  @Override
  public void ensureLastTimePartition(long timePartitionId) {
    // do nothing is correct
  }

  @Override
  public void ensureFlushedTimePartition(long timePartitionId) {
    // do nothing is correct
  }

  @Override
  public long ensureFlushedTimePartitionAndInit(long timePartitionId, String path, long initTime) {
    return idTable.getDeviceEntry(path).updateFlushTimeMap(timePartitionId, initTime);
  }

  // endregion

  // region upgrade support methods

  @Override
  public void applyNewlyFlushedTimeToFlushedTime() {
    throw new UnsupportedOperationException("IDTableFlushTimeManager doesn't support upgrade");
  }

  /**
   * update latest flush time for partition id
   *
   * @param partitionId partition id
   * @param latestFlushTime lastest flush time
   * @return true if update latest flush time success
   */
  @Override
  public boolean updateLatestFlushTimeToPartition(long partitionId, long latestFlushTime) {
    for (DeviceEntry deviceEntry : idTable.getAllDeviceEntry()) {
      deviceEntry.putLastTimeMap(partitionId, latestFlushTime);
      deviceEntry.putFlushTimeMap(partitionId, latestFlushTime);
      deviceEntry.updateGlobalFlushTime(latestFlushTime);
    }

    return true;
  }

  @Override
  public boolean updateLatestFlushTime(long partitionId) {
    boolean updated = false;

    for (DeviceEntry deviceEntry : idTable.getAllDeviceEntry()) {
      Long lastTime = deviceEntry.getLastTime(partitionId);
      if (lastTime == null) {
        continue;
      }

      updated = true;
      deviceEntry.putFlushTimeMap(partitionId, lastTime);
      deviceEntry.updateGlobalFlushTime(lastTime);
    }

    return updated;
  }

  // endregion

  // region query
  @Override
  public long getFlushedTime(long timePartitionId, String path) {
    return idTable.getDeviceEntry(path).getFLushTimeWithDefaultValue(timePartitionId);
  }

  @Override
  public long getLastTime(long timePartitionId, String path) {
    return idTable.getDeviceEntry(path).getLastTimeWithDefaultValue(timePartitionId);
  }

  @Override
  public long getGlobalFlushedTime(String path) {
    return idTable.getDeviceEntry(path).getGlobalFlushTime();
  }

  // endregion

  // region clear
  @Override
  public void clearLastTime() {
    for (DeviceEntry deviceEntry : idTable.getAllDeviceEntry()) {
      deviceEntry.clearLastTime();
    }
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
  // endregion
}
