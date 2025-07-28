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

package org.apache.iotdb.db.storageengine.rescon.memory;

import org.apache.iotdb.commons.consensus.DataRegionId;
import org.apache.iotdb.commons.memory.IMemoryBlock;
import org.apache.iotdb.commons.memory.MemoryBlockType;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.pipe.source.dataregion.realtime.listener.PipeTimePartitionListener;
import org.apache.iotdb.db.storageengine.StorageEngine;
import org.apache.iotdb.db.storageengine.dataregion.DataRegion;

import org.apache.tsfile.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.TreeSet;

/** Manage all the time partitions for all data regions and control the total memory of them */
public class TimePartitionManager {
  private static final Logger logger = LoggerFactory.getLogger(TimePartitionManager.class);
  final Map<DataRegionId, Map<Long, TimePartitionInfo>> timePartitionInfoMap;

  IMemoryBlock timePartitionInfoMemoryBlock;

  private TimePartitionManager() {
    timePartitionInfoMap = new HashMap<>();
    timePartitionInfoMemoryBlock =
        IoTDBDescriptor.getInstance()
            .getMemoryConfig()
            .getTimePartitionInfoMemoryManager()
            .exactAllocate("TimePartitionInfoMemoryBlock", MemoryBlockType.DYNAMIC);
  }

  public void registerTimePartitionInfo(TimePartitionInfo timePartitionInfo) {
    synchronized (timePartitionInfoMap) {
      TreeMap<Long, TimePartitionInfo> timePartitionInfoMapForRegion =
          (TreeMap<Long, TimePartitionInfo>)
              timePartitionInfoMap.computeIfAbsent(
                  timePartitionInfo.dataRegionId, k -> new TreeMap<>());

      timePartitionInfoMapForRegion.put(timePartitionInfo.partitionId, timePartitionInfo);

      // We need to ensure that the following method is called before
      // PipeInsertionDataNodeListener.listenToInsertNode.
      PipeTimePartitionListener.getInstance()
          .listenToTimePartitionGrow(
              String.valueOf(timePartitionInfo.dataRegionId.getId()),
              new Pair<>(
                  timePartitionInfoMapForRegion.firstKey(),
                  timePartitionInfoMapForRegion.lastKey()));
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
        timePartitionInfoMemoryBlock.forceAllocateWithoutLimitation(
            memSize - timePartitionInfo.memSize);
        timePartitionInfo.memSize = memSize;
        timePartitionInfo.isActive = isActive;
        if (timePartitionInfoMemoryBlock.getUsedMemoryInBytes()
            > timePartitionInfoMemoryBlock.getTotalMemorySizeInBytes()) {
          degradeLastFlushTime();
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

  private void degradeLastFlushTime() {
    TreeSet<TimePartitionInfo> treeSet = new TreeSet<>(TimePartitionInfo::comparePriority);
    synchronized (timePartitionInfoMap) {
      for (Map.Entry<DataRegionId, Map<Long, TimePartitionInfo>> entry :
          timePartitionInfoMap.entrySet()) {
        treeSet.addAll(entry.getValue().values());
      }

      while (timePartitionInfoMemoryBlock.getUsedMemoryInBytes()
          > timePartitionInfoMemoryBlock.getTotalMemorySizeInBytes()) {
        TimePartitionInfo timePartitionInfo = treeSet.pollFirst();
        if (timePartitionInfo == null) {
          return;
        }
        timePartitionInfoMemoryBlock.release(timePartitionInfo.memSize);
        DataRegion dataRegion =
            StorageEngine.getInstance().getDataRegion(timePartitionInfo.dataRegionId);
        if (dataRegion != null) {
          dataRegion.degradeFlushTimeMap(timePartitionInfo.partitionId);
          logger.info(
              "[{}]degrade LastFlushTimeMap of old TimePartitionInfo-{}, mem size is {}, remaining mem cost is {}",
              timePartitionInfo.dataRegionId,
              timePartitionInfo.partitionId,
              timePartitionInfo.memSize,
              timePartitionInfoMemoryBlock.getUsedMemoryInBytes());
        }
        timePartitionInfoMap
            .get(timePartitionInfo.dataRegionId)
            .remove(timePartitionInfo.partitionId);
      }
    }
  }

  public void removeTimePartitionInfo(DataRegionId dataRegionId) {
    synchronized (timePartitionInfoMap) {
      Map<Long, TimePartitionInfo> timePartitionInfoMapForRegion =
          timePartitionInfoMap.remove(dataRegionId);
      if (timePartitionInfoMapForRegion != null) {
        for (TimePartitionInfo timePartitionInfo : timePartitionInfoMapForRegion.values()) {
          if (timePartitionInfo != null) {
            timePartitionInfoMemoryBlock.release(timePartitionInfo.memSize);
          }
        }
      }
    }
  }

  @TestOnly
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

  @TestOnly
  public void clear() {
    synchronized (timePartitionInfoMap) {
      timePartitionInfoMap.clear();
      timePartitionInfoMemoryBlock.setUsedMemoryInBytes(0L);
    }
  }

  @TestOnly
  public void setTimePartitionInfoMemoryThreshold(long timePartitionInfoMemoryThreshold) {
    timePartitionInfoMemoryBlock.setTotalMemorySizeInBytes(timePartitionInfoMemoryThreshold);
  }

  public static TimePartitionManager getInstance() {
    return TimePartitionManager.InstanceHolder.instance;
  }

  private static class InstanceHolder {
    private InstanceHolder() {}

    private static TimePartitionManager instance = new TimePartitionManager();
  }

  //////////////////////////// APIs provided for pipe engine ////////////////////////////

  public Pair<Long, Long> getTimePartitionIdBound(DataRegionId dataRegionId) {
    synchronized (timePartitionInfoMap) {
      Map<Long, TimePartitionInfo> timePartitionInfoMapForDataRegion =
          timePartitionInfoMap.get(dataRegionId);
      if (Objects.nonNull(timePartitionInfoMapForDataRegion)
          && !timePartitionInfoMapForDataRegion.isEmpty()
          && timePartitionInfoMapForDataRegion instanceof TreeMap) {
        return new Pair<>(
            ((TreeMap<Long, TimePartitionInfo>) timePartitionInfoMapForDataRegion).firstKey(),
            ((TreeMap<Long, TimePartitionInfo>) timePartitionInfoMapForDataRegion).lastKey());
      }
    }
    return null;
  }
}
