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

import org.apache.iotdb.commons.consensus.DataRegionId;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.StorageEngine;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;

/** Manage all the time partitions for all data regions and control the total memory of them */
public class TimePartitionManager {
  private static final Logger logger = LoggerFactory.getLogger(TimePartitionManager.class);
  final Map<DataRegionId, Map<Long, TimePartitionInfo>> timePartitionInfoMap;

  long memCost = 0;
  long timePartitionInfoMemoryThreshold =
      IoTDBDescriptor.getInstance().getConfig().getAllocateMemoryForTimePartitionInfo();

  private TimePartitionManager() {
    timePartitionInfoMap = new HashMap<>();
  }

  public void registerTimePartitionInfo(TimePartitionInfo timePartitionInfo) {
    synchronized (timePartitionInfoMap) {
      TreeMap<Long, TimePartitionInfo> timePartitionInfoMapForRegion =
          (TreeMap<Long, TimePartitionInfo>)
              timePartitionInfoMap.computeIfAbsent(
                  timePartitionInfo.dataRegionId, k -> new TreeMap<>());

      Map.Entry<Long, TimePartitionInfo> entry =
          timePartitionInfoMapForRegion.floorEntry(timePartitionInfo.partitionId);
      if (entry != null) {
        entry.getValue().isLatestPartition = false;
      }

      timePartitionInfoMapForRegion.put(timePartitionInfo.partitionId, timePartitionInfo);
    }
  }

  public void updateAfterFlushing(
      DataRegionId dataRegionId,
      long timePartitionId,
      long systemFlushTime,
      long memSize,
      boolean isActive) {
    synchronized (timePartitionInfoMap) {
      TimePartitionInfo timePartitionInfo =
          timePartitionInfoMap
              .computeIfAbsent(dataRegionId, k -> new TreeMap<>())
              .get(timePartitionId);
      if (timePartitionInfo != null) {
        timePartitionInfo.lastSystemFlushTime = systemFlushTime;
        memCost += memSize - timePartitionInfo.memSize;
        timePartitionInfo.memSize = memSize;
        timePartitionInfo.isActive = isActive;
        if (memCost > timePartitionInfoMemoryThreshold) {
          evictOldPartition();
        }
      }
    }
  }

  public void updateAfterOpeningTsFileProcessor(DataRegionId dataRegionId, long timePartitionId) {
    synchronized (timePartitionInfoMap) {
      TimePartitionInfo timePartitionInfo =
          timePartitionInfoMap
              .computeIfAbsent(dataRegionId, k -> new TreeMap<>())
              .get(timePartitionId);
      if (timePartitionInfo != null) {
        timePartitionInfo.isActive = true;
      }
    }
  }

  private void evictOldPartition() {
    TreeSet<TimePartitionInfo> treeSet = new TreeSet<>(TimePartitionInfo::comparePriority);
    synchronized (timePartitionInfoMap) {
      for (Map.Entry<DataRegionId, Map<Long, TimePartitionInfo>> entry :
          timePartitionInfoMap.entrySet()) {
        treeSet.addAll(entry.getValue().values());
      }

      while (memCost > timePartitionInfoMemoryThreshold) {
        TimePartitionInfo timePartitionInfo = treeSet.pollFirst();
        if (timePartitionInfo == null) {
          return;
        }
        memCost -= timePartitionInfo.memSize;
        DataRegion dataRegion =
            StorageEngine.getInstance().getDataRegion(timePartitionInfo.dataRegionId);
        if (dataRegion != null) {
          dataRegion.releaseFlushTimeMap(timePartitionInfo.partitionId);
        }
        timePartitionInfoMap
            .get(timePartitionInfo.dataRegionId)
            .remove(timePartitionInfo.partitionId);
      }
    }
  }

  public void removePartition(DataRegionId dataRegionId, long partitionId) {
    synchronized (timePartitionInfoMap) {
      Map<Long, TimePartitionInfo> timePartitionInfoMapForDataRegion =
          timePartitionInfoMap.get(dataRegionId);
      if (timePartitionInfoMapForDataRegion != null) {
        TimePartitionInfo timePartitionInfo = timePartitionInfoMapForDataRegion.get(partitionId);
        if (timePartitionInfo != null) {
          timePartitionInfoMapForDataRegion.remove(partitionId);
          memCost -= timePartitionInfo.memSize;
        }
      }
    }
  }

  public TimePartitionInfo getTimePartitionInfo(DataRegionId dataRegionId, long timePartitionId) {
    synchronized (timePartitionInfoMap) {
      Map<Long, TimePartitionInfo> timePartitionInfoMapForDataRegion =
          timePartitionInfoMap.get(dataRegionId);
      if (timePartitionInfoMapForDataRegion == null) {
        return null;
      }
      return timePartitionInfoMapForDataRegion.get(timePartitionId);
    }
  }

  public void clear() {
    synchronized (timePartitionInfoMap) {
      timePartitionInfoMap.clear();
      memCost = 0;
    }
  }

  @TestOnly
  public void setTimePartitionInfoMemoryThreshold(long timePartitionInfoMemoryThreshold) {
    this.timePartitionInfoMemoryThreshold = timePartitionInfoMemoryThreshold;
  }

  public static TimePartitionManager getInstance() {
    return TimePartitionManager.InstanceHolder.instance;
  }

  private static class InstanceHolder {
    private InstanceHolder() {}

    private static TimePartitionManager instance = new TimePartitionManager();
  }
}
