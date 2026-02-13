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

package org.apache.iotdb.db.storageengine.dataregion.utils.tableDiskUsageCache;

import org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceContext;
import org.apache.iotdb.db.queryengine.plan.planner.memory.MemoryReservationManager;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileID;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileManager;

import org.apache.tsfile.utils.RamUsageEstimator;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

public class DataRegionTableSizeQueryContext {

  private final boolean needAllData;

  private final Map<Long, TimePartitionTableSizeQueryContext>
      timePartitionTableSizeQueryContextMap = new LinkedHashMap<>();
  private int objectFileNum = 0;

  private long previousUsedTimePartition = Long.MIN_VALUE;
  private TimePartitionTableSizeQueryContext previousUsedTimePartitionContext = null;

  private final Optional<FragmentInstanceContext> fragmentInstanceContext;
  private long acquiredMemory;

  public DataRegionTableSizeQueryContext(boolean needAllData) {
    this(needAllData, null);
  }

  public DataRegionTableSizeQueryContext(
      boolean needAllData, FragmentInstanceContext fragmentInstanceContext) {
    this.needAllData = needAllData;
    this.fragmentInstanceContext = Optional.ofNullable(fragmentInstanceContext);
  }

  public Map<Long, TimePartitionTableSizeQueryContext> getTimePartitionTableSizeQueryContextMap() {
    return timePartitionTableSizeQueryContextMap;
  }

  public boolean isNeedAllData() {
    return needAllData;
  }

  public boolean isEmpty() {
    return timePartitionTableSizeQueryContextMap.isEmpty();
  }

  public Optional<FragmentInstanceContext> getFragmentInstanceContext() {
    return fragmentInstanceContext;
  }

  public void addCachedTsFileIDAndOffsetInValueFile(TsFileID tsFileID, long offset) {
    if (useTimePartition(tsFileID.timePartitionId)) {
      previousUsedTimePartitionContext.addCachedTsFileIDAndOffsetInValueFile(tsFileID, offset);
    }
  }

  public void replaceCachedTsFileID(TsFileID originTsFileID, TsFileID newTsFileID) {
    if (useTimePartition(originTsFileID.timePartitionId)) {
      previousUsedTimePartitionContext.replaceCachedTsFileID(originTsFileID, newTsFileID);
    }
  }

  public void updateResult(String table, long size, long currentTimePartition) {
    if (useTimePartition(currentTimePartition)) {
      previousUsedTimePartitionContext.updateResult(table, size, needAllData);
    }
  }

  /**
   * useTimePartition must be called before accessing previousUsedTimePartitionContext. When it
   * returns false, the caller must skip any operation on the context.
   */
  private boolean useTimePartition(long currentTimePartition) {
    if (currentTimePartition != previousUsedTimePartition
        || previousUsedTimePartitionContext == null) {
      TimePartitionTableSizeQueryContext currentTimePartitionContext =
          timePartitionTableSizeQueryContextMap.compute(
              currentTimePartition,
              (k, v) ->
                  (v == null && needAllData)
                      ? new TimePartitionTableSizeQueryContext(new HashMap<>())
                      : v);
      if (currentTimePartitionContext == null) {
        return false;
      }
      previousUsedTimePartition = currentTimePartition;
      previousUsedTimePartitionContext = currentTimePartitionContext;
    }
    return true;
  }

  public void addAllTimePartitionsInTsFileManager(TsFileManager tsFileManager) {
    for (Long timePartition : tsFileManager.getTimePartitions()) {
      addTimePartition(timePartition, new TimePartitionTableSizeQueryContext(new HashMap<>()));
    }
  }

  public void addTimePartition(
      long timePartition, TimePartitionTableSizeQueryContext timePartitionTableSizeQueryContext) {
    timePartitionTableSizeQueryContextMap.put(timePartition, timePartitionTableSizeQueryContext);
  }

  public int getObjectFileNum() {
    return objectFileNum;
  }

  public long getObjectFileSize() {
    long totalSize = 0;
    for (TimePartitionTableSizeQueryContext timePartitionContext :
        timePartitionTableSizeQueryContextMap.values()) {
      totalSize += timePartitionContext.getObjectFileSizeOfCurrentTimePartition();
    }
    return totalSize;
  }

  public void updateObjectFileNum(int delta) {
    this.objectFileNum += delta;
  }

  public void reserveMemoryForResultMap() {
    long cost =
        RamUsageEstimator.sizeOfMapWithKnownShallowSize(
            timePartitionTableSizeQueryContextMap,
            RamUsageEstimator.SHALLOW_SIZE_OF_HASHMAP,
            RamUsageEstimator.SHALLOW_SIZE_OF_HASHMAP_ENTRY);
    reserveMemory(cost);
  }

  public void reserveMemoryForTsFileIDs() {
    long cost =
        timePartitionTableSizeQueryContextMap.values().stream()
            .mapToLong(TimePartitionTableSizeQueryContext::ramBytesUsedOfTsFileIDOffsetMap)
            .sum();
    reserveMemory(cost);
  }

  public void reserveMemory(long size) {
    if (!fragmentInstanceContext.isPresent()) {
      return;
    }
    MemoryReservationManager memoryReservationContext =
        fragmentInstanceContext.get().getMemoryReservationContext();
    memoryReservationContext.reserveMemoryCumulatively(size);
    memoryReservationContext.reserveMemoryImmediately();
    acquiredMemory += size;
  }

  public void releaseMemory() {
    if (!fragmentInstanceContext.isPresent() || acquiredMemory <= 0) {
      return;
    }
    fragmentInstanceContext
        .get()
        .getMemoryReservationContext()
        .releaseMemoryCumulatively(acquiredMemory);
    acquiredMemory = 0;
  }
}
