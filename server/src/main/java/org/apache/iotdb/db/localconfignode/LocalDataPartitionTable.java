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

package org.apache.iotdb.db.localconfignode;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TConsensusGroupType;
import org.apache.iotdb.common.rpc.thrift.TTimePartitionSlot;
import org.apache.iotdb.commons.consensus.DataRegionId;
import org.apache.iotdb.commons.partition.SeriesPartitionTable;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.tsfile.utils.Pair;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class LocalDataPartitionTable {
  private static final Logger LOG = LoggerFactory.getLogger(LocalDataPartitionTable.class);

  private String storageGroupName;
  private Map<PartialPath, SeriesPartitionTable> partitionTableMap;
  private List<DataRegionId> regionList = new ArrayList<>();
  private Map<DataRegionId, AtomicInteger> regionSlotCountMap = new ConcurrentHashMap<>();

  public LocalDataPartitionTable() {}

  public LocalDataPartitionTable(String storageGroupName) {
    this.storageGroupName = storageGroupName;
    this.partitionTableMap = new ConcurrentHashMap<>();
  }

  public void init(ByteBuffer buffer) {
    // TODO: init from byte buffer
  }

  public void serialize(OutputStream outputStream) {
    // TODO: serialize the table to output stream
  }

  /**
   * Get the TimePartitionSlot to DataRegionId Map. The result is stored in param slotRegionMap.
   *
   * @param path The full path for the series.
   * @param timePartitionSlots The time partition slots for the series.
   * @param slotRegionMap The map that store the result.
   * @return Whether all the partitions exist.
   */
  public boolean getDataRegionId(
      PartialPath path,
      List<TTimePartitionSlot> timePartitionSlots,
      Map<TTimePartitionSlot, DataRegionId> slotRegionMap) {
    if (!partitionTableMap.containsKey(path)) {
      return false;
    }
    SeriesPartitionTable seriesPartitionTable = new SeriesPartitionTable();
    boolean allPartitionExists =
        partitionTableMap.get(path).getDataPartition(timePartitionSlots, seriesPartitionTable);
    for (Map.Entry<TTimePartitionSlot, List<TConsensusGroupId>> entry :
        seriesPartitionTable.getSeriesPartitionMap().entrySet()) {
      if (entry.getValue().size() > 0) {
        slotRegionMap.put(entry.getKey(), new DataRegionId(entry.getValue().get(0).getId()));
      }
    }
    return allPartitionExists;
  }

  /**
   * Get all data region id of current storage group
   *
   * @return data region id in list
   */
  public List<DataRegionId> getAllDataRegionId() {
    Set<DataRegionId> regionIdSet = new HashSet<>();
    for (SeriesPartitionTable partitionTable : partitionTableMap.values()) {
      Map<TTimePartitionSlot, List<TConsensusGroupId>> partitionGroupIdMap =
          partitionTable.getSeriesPartitionMap();
      for (List<TConsensusGroupId> ids : partitionGroupIdMap.values()) {
        ids.forEach(x -> regionIdSet.add(new DataRegionId(x.getId())));
      }
    }
    return new ArrayList<>(regionIdSet);
  }

  public int getTimeSlotNum() {
    int slotCount = 0;
    for (SeriesPartitionTable partitionTable : partitionTableMap.values()) {
      Map<TTimePartitionSlot, List<TConsensusGroupId>> partitionGroupIdMap =
          partitionTable.getSeriesPartitionMap();
      slotCount += partitionGroupIdMap.size();
    }
    return slotCount;
  }

  public DataRegionId getDataRegionWithAutoExtension(
      PartialPath path, TTimePartitionSlot timePartitionSlot) {
    DataRegionId regionId;
    if ((regionId = checkExpansion(path, timePartitionSlot)) != null) {
      // return the newly allocated region
      regionSlotCountMap.get(regionId).incrementAndGet();
      return regionId;
    }

    // select a region with min time partition slot
    List<Pair<DataRegionId, AtomicInteger>> slotCountList = new ArrayList<>();
    regionSlotCountMap.forEach((id, count) -> slotCountList.add(new Pair<>(id, count)));
    slotCountList.sort(Comparator.comparingInt(o -> o.right.get()));
    DataRegionId chosenId = slotCountList.get(0).left;
    regionSlotCountMap.get(chosenId).incrementAndGet();
    return chosenId;
  }

  public DataRegionId checkExpansion(PartialPath path, TTimePartitionSlot timePartitionSlot) {
    if (regionList.size() == 0) {
      // there is no data region for this storage group, create one
      return carryOutExpansion(path, timePartitionSlot);
    }

    // check the load and carry out the expansion if necessary
    double allocatedSlotNum = getTimeSlotNum();
    double maxRegionNum = IoTDBDescriptor.getInstance().getConfig().getDataRegionNum();
    double maxSlotNum = IoTDBDescriptor.getInstance().getConfig().getSeriesPartitionSlotNum();
    if (regionList.size() < maxRegionNum
        && allocatedSlotNum / (double) regionList.size() > maxSlotNum / maxRegionNum) {
      return carryOutExpansion(path, timePartitionSlot);
    }
    return null;
  }

  private DataRegionId carryOutExpansion(PartialPath path, TTimePartitionSlot timePartitionSlot) {
    if (!partitionTableMap.containsKey(path)) {
      partitionTableMap.put(path, new SeriesPartitionTable());
    }
    SeriesPartitionTable seriesPartitionTable = partitionTableMap.get(path);
    Map<TTimePartitionSlot, List<TConsensusGroupId>> allotmentMap = new HashMap<>();
    int nextRegionId = DataRegionIdGenerator.getInstance().getNextId();
    allotmentMap.put(
        timePartitionSlot,
        Collections.singletonList(
            new TConsensusGroupId(TConsensusGroupType.DataRegion, nextRegionId)));
    SeriesPartitionTable requestTable = new SeriesPartitionTable(allotmentMap);
    Map<TConsensusGroupId, AtomicInteger> deltaMap = new HashMap<>();
    seriesPartitionTable.createDataPartition(requestTable, deltaMap);
    regionList.add(new DataRegionId(nextRegionId));
    regionSlotCountMap.put(new DataRegionId(nextRegionId), new AtomicInteger(0));
    return new DataRegionId(nextRegionId);
  }

  public void clear() {
    // TODO: clear the table
  }
}
