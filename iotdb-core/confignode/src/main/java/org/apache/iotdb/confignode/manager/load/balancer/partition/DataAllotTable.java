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

package org.apache.iotdb.confignode.manager.load.balancer.partition;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TSeriesPartitionSlot;
import org.apache.iotdb.common.rpc.thrift.TTimePartitionSlot;
import org.apache.iotdb.commons.structure.BalanceTreeMap;
import org.apache.iotdb.confignode.conf.ConfigNodeConfig;
import org.apache.iotdb.confignode.conf.ConfigNodeDescriptor;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

public class DataAllotTable {

  private static final ConfigNodeConfig CONF = ConfigNodeDescriptor.getInstance().getConf();
  private static final int SERIES_SLOT_NUM = CONF.getSeriesSlotNum();

  private final ReentrantReadWriteLock dataAllotTableLock;
  private final AtomicReference<TTimePartitionSlot> currentTimePartition;
  // Map<TimePartitionSlot, DataPartitionCount>
  // Cache the number of DataPartitions in each future TimePartitionSlot
  private final TreeMap<TTimePartitionSlot, AtomicInteger> dataPartitionCounter;
  // Map<SeriesPartitionSlot, RegionGroupId>
  // The optimal allocation of SeriesSlots to RegionGroups in the currentTimePartition
  private final Map<TSeriesPartitionSlot, TConsensusGroupId> dataAllotTable;

  public DataAllotTable() {
    this.dataAllotTableLock = new ReentrantReadWriteLock();
    this.currentTimePartition = new AtomicReference<>(new TTimePartitionSlot(0));
    this.dataPartitionCounter = new TreeMap<>();
    this.dataAllotTable = new HashMap<>();
  }

  public boolean isEmpty() {
    dataAllotTableLock.readLock().lock();
    try {
      return dataAllotTable.isEmpty();
    } finally {
      dataAllotTableLock.readLock().unlock();
    }
  }

  /**
   * Update the DataAllotTable according to the current DataRegionGroups and future DataAllotTable.
   *
   * @param dataRegionGroups the current DataRegionGroups
   * @param allocatedTable the future DataAllotTable, i.e. some SeriesSlots have already allocated
   */
  public void updateDataAllotTable(
      List<TConsensusGroupId> dataRegionGroups,
      Map<TSeriesPartitionSlot, TConsensusGroupId> allocatedTable) {
    dataAllotTableLock.writeLock().lock();
    try {
      // mu is the average number of slots allocated to each regionGroup
      int mu = SERIES_SLOT_NUM / dataRegionGroups.size();
      // Decide all SeriesSlot randomly
      List<TSeriesPartitionSlot> seriesSlotList = new ArrayList<>();
      for (int i = 0; i < SERIES_SLOT_NUM; i++) {
        seriesSlotList.add(new TSeriesPartitionSlot(i));
      }
      Collections.shuffle(seriesSlotList);

      // The counter will maintain the number of slots allocated to each regionGroup
      BalanceTreeMap<TConsensusGroupId, Integer> counter = new BalanceTreeMap<>();
      Map<TConsensusGroupId, AtomicInteger> regionSlotCounter = new HashMap<>();
      allocatedTable.forEach(
          (seriesSlot, regionGroupId) ->
              regionSlotCounter
                  .computeIfAbsent(regionGroupId, empty -> new AtomicInteger(0))
                  .incrementAndGet());
      dataRegionGroups.forEach(
          regionGroupId -> regionSlotCounter.putIfAbsent(regionGroupId, new AtomicInteger(0)));
      regionSlotCounter.forEach(
          (regionGroupId, slotNum) -> counter.put(regionGroupId, slotNum.get()));

      Map<TSeriesPartitionSlot, TConsensusGroupId> newAllotTable = new HashMap<>();
      for (TSeriesPartitionSlot seriesPartitionSlot : seriesSlotList) {
        if (allocatedTable.containsKey(seriesPartitionSlot)) {
          // If the SeriesSlot has already been allocated, keep the allocation
          newAllotTable.put(seriesPartitionSlot, allocatedTable.get(seriesPartitionSlot));
          continue;
        }

        TConsensusGroupId oldRegionGroupId = dataAllotTable.get(seriesPartitionSlot);
        if (oldRegionGroupId != null
            && counter.containsKey(oldRegionGroupId)
            && counter.get(oldRegionGroupId) < mu) {
          // Inherit the oldRegionGroupId when the slotNum of oldRegionGroupId is less than average
          newAllotTable.put(seriesPartitionSlot, oldRegionGroupId);
          counter.put(oldRegionGroupId, counter.get(oldRegionGroupId) + 1);
          continue;
        }

        // Otherwise, choose the regionGroup with the least slotNum to keep load balance
        TConsensusGroupId newRegionGroupId = counter.getKeyWithMinValue();
        newAllotTable.put(seriesPartitionSlot, newRegionGroupId);
        counter.put(newRegionGroupId, counter.get(newRegionGroupId) + 1);
      }

      dataAllotTable.clear();
      dataAllotTable.putAll(newAllotTable);
    } finally {
      dataAllotTableLock.writeLock().unlock();
    }
  }

  /**
   * Update the current time partition and remove the useless time partitions.
   *
   * @param regionGroupNum the number of regionGroups
   * @return whether the current time partition is updated
   */
  public boolean updateCurrentTimePartition(int regionGroupNum) {
    int threshold = timePartitionThreshold(regionGroupNum);
    dataAllotTableLock.writeLock().lock();
    try {
      AtomicLong newStartTime = new AtomicLong(Long.MIN_VALUE);
      dataPartitionCounter.forEach(
          (timePartition, counter) -> {
            // Select the maximum TimePartition whose slotNum is greater than the following equation
            // Ensure that the remaining slots can be still distributed to new regionGroups
            if (counter.get() >= threshold && timePartition.getStartTime() > newStartTime.get()) {
              newStartTime.set(timePartition.getStartTime());
            }
          });

      if (newStartTime.get() > currentTimePartition.get().getStartTime()) {
        currentTimePartition.set(new TTimePartitionSlot(newStartTime.get()));
        List<TTimePartitionSlot> removeTimePartitionSlots =
            dataPartitionCounter.keySet().stream()
                .filter(timePartition -> timePartition.getStartTime() < newStartTime.get())
                .collect(Collectors.toList());
        removeTimePartitionSlots.forEach(dataPartitionCounter::remove);
        return true;
      }
    } finally {
      dataAllotTableLock.writeLock().unlock();
    }
    return false;
  }

  public void addTimePartitionCount(Map<TTimePartitionSlot, Integer> timePartitionCountMap) {
    dataAllotTableLock.writeLock().lock();
    try {
      timePartitionCountMap.forEach(
          (timePartition, count) ->
              dataPartitionCounter
                  .computeIfAbsent(timePartition, empty -> new AtomicInteger(0))
                  .addAndGet(count));
    } finally {
      dataAllotTableLock.writeLock().unlock();
    }
  }

  public TTimePartitionSlot getCurrentTimePartition() {
    return currentTimePartition.get();
  }

  public TConsensusGroupId getRegionGroupId(TSeriesPartitionSlot seriesPartitionSlot) {
    dataAllotTableLock.readLock().lock();
    try {
      return dataAllotTable.get(seriesPartitionSlot);
    } finally {
      dataAllotTableLock.readLock().unlock();
    }
  }

  /** Only use this interface when init PartitionBalancer. */
  public void setCurrentTimePartition(long startTime) {
    currentTimePartition.set(new TTimePartitionSlot(startTime));
  }

  /** Only use this interface when init PartitionBalancer. */
  public void setDataAllotTable(Map<TSeriesPartitionSlot, TConsensusGroupId> dataAllotTable) {
    this.dataAllotTable.putAll(dataAllotTable);
  }

  public static int timePartitionThreshold(int regionGroupNum) {
    return (int) (SERIES_SLOT_NUM * (1.0 - 2.0 / regionGroupNum));
  }

  public void acquireReadLock() {
    dataAllotTableLock.readLock().lock();
  }

  public void releaseReadLock() {
    dataAllotTableLock.readLock().unlock();
  }

  public void acquireWriteLock() {
    dataAllotTableLock.writeLock().lock();
  }

  public void releaseWriteLock() {
    dataAllotTableLock.writeLock().unlock();
  }
}
