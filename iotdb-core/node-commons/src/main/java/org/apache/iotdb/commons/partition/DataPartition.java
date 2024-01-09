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
package org.apache.iotdb.commons.partition;

import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.common.rpc.thrift.TSeriesPartitionSlot;
import org.apache.iotdb.common.rpc.thrift.TTimePartitionSlot;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.utils.PathUtils;
import org.apache.iotdb.commons.utils.TimePartitionUtils;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.stream.Collectors.toList;

// TODO: Remove this class
public class DataPartition extends Partition {
  private static long timePartitionInterval =
      CommonDescriptor.getInstance().getConfig().getTimePartitionInterval();
  public static final TRegionReplicaSet NOT_ASSIGNED = new TRegionReplicaSet();
  // Map<StorageGroup, Map<TSeriesPartitionSlot, Map<TTimePartitionSlot, List<TRegionMessage>>>>
  private Map<String, Map<TSeriesPartitionSlot, Map<TTimePartitionSlot, List<TRegionReplicaSet>>>>
      dataPartitionMap;

  public DataPartition(String seriesSlotExecutorName, int seriesPartitionSlotNum) {
    super(seriesSlotExecutorName, seriesPartitionSlotNum);
  }

  @Override
  public boolean isEmpty() {
    return dataPartitionMap == null || dataPartitionMap.isEmpty();
  }

  public DataPartition(
      Map<String, Map<TSeriesPartitionSlot, Map<TTimePartitionSlot, List<TRegionReplicaSet>>>>
          dataPartitionMap,
      String seriesSlotExecutorName,
      int seriesPartitionSlotNum) {
    this(seriesSlotExecutorName, seriesPartitionSlotNum);
    this.dataPartitionMap = dataPartitionMap;
  }

  public Map<String, Map<TSeriesPartitionSlot, Map<TTimePartitionSlot, List<TRegionReplicaSet>>>>
      getDataPartitionMap() {
    return dataPartitionMap;
  }

  public void setDataPartitionMap(
      Map<String, Map<TSeriesPartitionSlot, Map<TTimePartitionSlot, List<TRegionReplicaSet>>>>
          dataPartitionMap) {
    this.dataPartitionMap = dataPartitionMap;
  }

  public List<List<TTimePartitionSlot>> getTimePartitionRange(
      String deviceName, Filter timeFilter) {
    String storageGroup = getStorageGroupByDevice(deviceName);
    TSeriesPartitionSlot seriesPartitionSlot = calculateDeviceGroupId(deviceName);
    if (!dataPartitionMap.containsKey(storageGroup)
        || !dataPartitionMap.get(storageGroup).containsKey(seriesPartitionSlot)) {
      return Collections.emptyList();
    }

    List<List<TTimePartitionSlot>> res = new ArrayList<>();
    Map<TTimePartitionSlot, List<TRegionReplicaSet>> map =
        dataPartitionMap.get(storageGroup).get(seriesPartitionSlot);
    List<TTimePartitionSlot> timePartitionSlotList =
        map.keySet().stream()
            .filter(key -> TimePartitionUtils.satisfyPartitionStartTime(timeFilter, key.startTime))
            .sorted(Comparator.comparingLong(TTimePartitionSlot::getStartTime))
            .collect(toList());

    if (timePartitionSlotList.isEmpty()) {
      return res;
    }

    int previousRegionId = map.get(timePartitionSlotList.get(0)).get(0).regionId.id;
    int previousIndex = 0;
    res.add(new ArrayList<>());
    res.get(previousIndex).add(timePartitionSlotList.get(0));

    for (int i = 1, size = timePartitionSlotList.size(); i < size; i++) {
      int currentRegionId = map.get(timePartitionSlotList.get(i)).get(0).regionId.id;
      // region id of current time partition is same as previous
      if (currentRegionId == previousRegionId) {
        res.get(previousIndex).add(timePartitionSlotList.get(i));
      } else {
        previousIndex++;
        previousRegionId = currentRegionId;
        res.add(new ArrayList<>());
        res.get(previousIndex).add(timePartitionSlotList.get(i));
      }
    }

    return res;
  }

  public List<TRegionReplicaSet> getDataRegionReplicaSetWithTimeFilter(
      String deviceName, Filter timeFilter) {
    String storageGroup = getStorageGroupByDevice(deviceName);
    TSeriesPartitionSlot seriesPartitionSlot = calculateDeviceGroupId(deviceName);
    if (!dataPartitionMap.containsKey(storageGroup)
        || !dataPartitionMap.get(storageGroup).containsKey(seriesPartitionSlot)) {
      return Collections.singletonList(NOT_ASSIGNED);
    }
    return dataPartitionMap.get(storageGroup).get(seriesPartitionSlot).entrySet().stream()
        .filter(
            entry ->
                TimePartitionUtils.satisfyPartitionStartTime(timeFilter, entry.getKey().startTime))
        .flatMap(entry -> entry.getValue().stream())
        .distinct()
        .collect(toList());
  }

  public List<TRegionReplicaSet> getDataRegionReplicaSet(
      String deviceName, TTimePartitionSlot tTimePartitionSlot) {
    String storageGroup = getStorageGroupByDevice(deviceName);
    Map<TSeriesPartitionSlot, Map<TTimePartitionSlot, List<TRegionReplicaSet>>> dbMap =
        dataPartitionMap.get(storageGroup);
    if (dbMap == null) {
      return Collections.singletonList(NOT_ASSIGNED);
    }
    TSeriesPartitionSlot seriesPartitionSlot = calculateDeviceGroupId(deviceName);
    Map<TTimePartitionSlot, List<TRegionReplicaSet>> seriesSlotMap = dbMap.get(seriesPartitionSlot);
    if (seriesSlotMap == null) {
      return Collections.singletonList(NOT_ASSIGNED);
    }

    List<TRegionReplicaSet> regionReplicaSets = seriesSlotMap.get(tTimePartitionSlot);

    if (regionReplicaSets == null) {
      return Collections.singletonList(NOT_ASSIGNED);
    }

    return regionReplicaSets;
  }

  public List<TRegionReplicaSet> getDataRegionReplicaSetForWriting(
      String deviceName, List<TTimePartitionSlot> timePartitionSlotList) {
    // A list of data region replica sets will store data in a same time partition.
    // We will insert data to the last set in the list.
    // TODO return the latest dataRegionReplicaSet for each time partition
    String storageGroup = getStorageGroupByDevice(deviceName);
    TSeriesPartitionSlot seriesPartitionSlot = calculateDeviceGroupId(deviceName);
    // IMPORTANT TODO: (xingtanzjr) need to handle the situation for write operation that there are
    // more than 1 Regions for one timeSlot
    List<TRegionReplicaSet> dataRegionReplicaSets = new ArrayList<>();
    Map<TSeriesPartitionSlot, Map<TTimePartitionSlot, List<TRegionReplicaSet>>>
        dataBasePartitionMap = dataPartitionMap.get(storageGroup);
    Map<TTimePartitionSlot, List<TRegionReplicaSet>> slotReplicaSetMap =
        dataBasePartitionMap.get(seriesPartitionSlot);
    for (TTimePartitionSlot timePartitionSlot : timePartitionSlotList) {
      List<TRegionReplicaSet> targetRegionList = slotReplicaSetMap.get(timePartitionSlot);
      if (targetRegionList == null || targetRegionList.isEmpty()) {
        throw new RuntimeException(
            String.format(
                "targetRegionList is empty. device: %s, timeSlot: %s",
                deviceName, timePartitionSlot));
      } else {
        dataRegionReplicaSets.add(targetRegionList.get(targetRegionList.size() - 1));
      }
    }
    return dataRegionReplicaSets;
  }

  public TRegionReplicaSet getDataRegionReplicaSetForWriting(
      String deviceName, TTimePartitionSlot timePartitionSlot) {
    // A list of data region replica sets will store data in a same time partition.
    // We will insert data to the last set in the list.
    // TODO return the latest dataRegionReplicaSet for each time partition
    String storageGroup = getStorageGroupByDevice(deviceName);
    TSeriesPartitionSlot seriesPartitionSlot = calculateDeviceGroupId(deviceName);
    Map<TSeriesPartitionSlot, Map<TTimePartitionSlot, List<TRegionReplicaSet>>>
        databasePartitionMap = dataPartitionMap.get(storageGroup);
    if (databasePartitionMap == null) {
      throw new RuntimeException(
          "Database not exists and failed to create automatically because enable_auto_create_schema is FALSE.");
    }
    List<TRegionReplicaSet> regions =
        databasePartitionMap.get(seriesPartitionSlot).get(timePartitionSlot);
    // IMPORTANT TODO: (xingtanzjr) need to handle the situation for write operation that there
    // are more than 1 Regions for one timeSlot
    return regions.get(0);
  }

  private String getStorageGroupByDevice(String deviceName) {
    for (String storageGroup : dataPartitionMap.keySet()) {
      if (PathUtils.isStartWith(deviceName, storageGroup)) {
        return storageGroup;
      }
    }
    // TODO: (xingtanzjr) how to handle this exception in IoTDB
    return null;
  }

  @Override
  public List<RegionReplicaSetInfo> getDistributionInfo() {
    Map<TRegionReplicaSet, RegionReplicaSetInfo> distributionMap = new HashMap<>();

    dataPartitionMap.forEach(
        (storageGroup, partition) -> {
          List<TRegionReplicaSet> ret =
              partition.entrySet().stream()
                  .flatMap(
                      s -> s.getValue().entrySet().stream().flatMap(e -> e.getValue().stream()))
                  .collect(toList());
          for (TRegionReplicaSet regionReplicaSet : ret) {
            distributionMap
                .computeIfAbsent(regionReplicaSet, RegionReplicaSetInfo::new)
                .setStorageGroup(storageGroup);
          }
        });
    return new ArrayList<>(distributionMap.values());
  }
}
