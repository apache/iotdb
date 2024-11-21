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
import org.apache.iotdb.commons.utils.PathUtils;
import org.apache.iotdb.commons.utils.TimePartitionUtils;

import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.read.filter.basic.Filter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

public class DataPartition extends Partition {

  private static final Logger LOGGER = LoggerFactory.getLogger(DataPartition.class);
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

  public Set<TRegionReplicaSet> getAllReplicaSets() {
    Set<TRegionReplicaSet> replicaSets = new HashSet<>();
    for (Entry<String, Map<TSeriesPartitionSlot, Map<TTimePartitionSlot, List<TRegionReplicaSet>>>>
        dbEntry : dataPartitionMap.entrySet()) {
      for (Entry<TSeriesPartitionSlot, Map<TTimePartitionSlot, List<TRegionReplicaSet>>>
          seriesEntry : dbEntry.getValue().entrySet()) {
        for (Entry<TTimePartitionSlot, List<TRegionReplicaSet>> timeSlotEntry :
            seriesEntry.getValue().entrySet()) {
          replicaSets.addAll(timeSlotEntry.getValue());
        }
      }
    }
    return replicaSets;
  }

  public void setDataPartitionMap(
      Map<String, Map<TSeriesPartitionSlot, Map<TTimePartitionSlot, List<TRegionReplicaSet>>>>
          dataPartitionMap) {
    this.dataPartitionMap = dataPartitionMap;
  }

  public List<List<TTimePartitionSlot>> getTimePartitionRange(
      IDeviceID deviceID, Filter timeFilter) {
    String storageGroup = getDatabaseNameByDevice(deviceID);
    TSeriesPartitionSlot seriesPartitionSlot = calculateDeviceGroupId(deviceID);
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
      IDeviceID deviceId, Filter timeFilter) {
    String storageGroup = getDatabaseNameByDevice(deviceId);
    TSeriesPartitionSlot seriesPartitionSlot = calculateDeviceGroupId(deviceId);
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

  /**
   * For table model usage.
   *
   * <p>The database shall start with "root.". Concat this to a user-provided db name if necessary.
   *
   * <p>The device id shall be [table, seg1, ....]
   */
  public List<TRegionReplicaSet> getDataRegionReplicaSetWithTimeFilter(
      String database, IDeviceID deviceId, Filter timeFilter) {
    // TODO perfect this interface, @Potato
    database = PathUtils.qualifyDatabaseName(database);
    TSeriesPartitionSlot seriesPartitionSlot = calculateDeviceGroupId(deviceId);
    if (!dataPartitionMap.containsKey(database)
        || !dataPartitionMap.get(database).containsKey(seriesPartitionSlot)) {
      return Collections.singletonList(NOT_ASSIGNED);
    }
    return dataPartitionMap.get(database).get(seriesPartitionSlot).entrySet().stream()
        .filter(
            entry ->
                TimePartitionUtils.satisfyPartitionStartTime(timeFilter, entry.getKey().startTime))
        .flatMap(entry -> entry.getValue().stream())
        .distinct()
        .collect(toList());
  }

  public List<TRegionReplicaSet> getDataRegionReplicaSet(
      IDeviceID deviceID, TTimePartitionSlot tTimePartitionSlot) {
    String storageGroup = getDatabaseNameByDevice(deviceID);
    Map<TSeriesPartitionSlot, Map<TTimePartitionSlot, List<TRegionReplicaSet>>> dbMap =
        dataPartitionMap.get(storageGroup);
    if (dbMap == null) {
      return Collections.singletonList(NOT_ASSIGNED);
    }
    TSeriesPartitionSlot seriesPartitionSlot = calculateDeviceGroupId(deviceID);
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
      IDeviceID deviceID, List<TTimePartitionSlot> timePartitionSlotList, String databaseName) {
    if (databaseName == null) {
      databaseName = getDatabaseNameByDevice(deviceID);
    }
    // A list of data region replica sets will store data in a same time partition.
    // We will insert data to the last set in the list.
    // TODO return the latest dataRegionReplicaSet for each time partition
    TSeriesPartitionSlot seriesPartitionSlot = calculateDeviceGroupId(deviceID);
    // IMPORTANT TODO: (xingtanzjr) need to handle the situation for write operation that there are
    // more than 1 Regions for one timeSlot
    List<TRegionReplicaSet> dataRegionReplicaSets = new ArrayList<>();
    Map<TSeriesPartitionSlot, Map<TTimePartitionSlot, List<TRegionReplicaSet>>>
        dataBasePartitionMap = dataPartitionMap.get(PathUtils.qualifyDatabaseName(databaseName));
    Map<TTimePartitionSlot, List<TRegionReplicaSet>> slotReplicaSetMap =
        dataBasePartitionMap.get(seriesPartitionSlot);
    for (TTimePartitionSlot timePartitionSlot : timePartitionSlotList) {
      List<TRegionReplicaSet> targetRegionList = slotReplicaSetMap.get(timePartitionSlot);
      if (targetRegionList == null || targetRegionList.isEmpty()) {
        throw new RuntimeException(
            String.format(
                "targetRegionList is empty. device: %s, timeSlot: %s",
                deviceID, timePartitionSlot));
      } else {
        dataRegionReplicaSets.add(targetRegionList.get(targetRegionList.size() - 1));
      }
    }
    return dataRegionReplicaSets;
  }

  public TRegionReplicaSet getDataRegionReplicaSetForWriting(
      IDeviceID deviceID, TTimePartitionSlot timePartitionSlot, String databaseName) {
    // A list of data region replica sets will store data in a same time partition.
    // We will insert data to the last set in the list.
    // TODO return the latest dataRegionReplicaSet for each time partition
    TSeriesPartitionSlot seriesPartitionSlot = calculateDeviceGroupId(deviceID);
    if (databaseName != null) {
      databaseName = PathUtils.qualifyDatabaseName(databaseName);
    } else {
      databaseName = getDatabaseNameByDevice(deviceID);
    }
    Map<TSeriesPartitionSlot, Map<TTimePartitionSlot, List<TRegionReplicaSet>>>
        databasePartitionMap = dataPartitionMap.get(databaseName);
    if (databasePartitionMap == null) {
      throw new RuntimeException(
          "Database "
              + databaseName
              + " not exists and failed to create automatically because enable_auto_create_schema is FALSE.");
    }
    List<TRegionReplicaSet> regions =
        databasePartitionMap.get(seriesPartitionSlot).get(timePartitionSlot);
    // IMPORTANT TODO: (xingtanzjr) need to handle the situation for write operation that there
    // are more than 1 Regions for one timeSlot
    return regions.get(0);
  }

  public TRegionReplicaSet getDataRegionReplicaSetForWriting(
      IDeviceID deviceID, TTimePartitionSlot timePartitionSlot) {
    return getDataRegionReplicaSetForWriting(
        deviceID, timePartitionSlot, getDatabaseNameByDevice(deviceID));
  }

  private String getDatabaseNameByDevice(IDeviceID deviceID) {
    for (String storageGroup : dataPartitionMap.keySet()) {
      if (PathUtils.isStartWith(deviceID, storageGroup)) {
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

  // TODO(beyyes) if join queries more than one table, may trigger the unmodication error in
  // Collections.singletonMap
  public void upsertDataPartition(DataPartition targetDataPartition) {
    requireNonNull(this.dataPartitionMap, "dataPartitionMap is null");

    for (Map.Entry<
            String, Map<TSeriesPartitionSlot, Map<TTimePartitionSlot, List<TRegionReplicaSet>>>>
        targetDbEntry : targetDataPartition.getDataPartitionMap().entrySet()) {
      String database = targetDbEntry.getKey();
      if (dataPartitionMap.containsKey(database)) {
        Map<TSeriesPartitionSlot, Map<TTimePartitionSlot, List<TRegionReplicaSet>>>
            sourceSeriesPartitionMap = dataPartitionMap.get(database);

        for (Map.Entry<TSeriesPartitionSlot, Map<TTimePartitionSlot, List<TRegionReplicaSet>>>
            targetSeriesSlotEntry : targetDbEntry.getValue().entrySet()) {

          TSeriesPartitionSlot targetSeriesSlot = targetSeriesSlotEntry.getKey();
          if (sourceSeriesPartitionMap.containsKey(targetSeriesSlot)) {
            Map<TTimePartitionSlot, List<TRegionReplicaSet>> sourceTimePartitionMap =
                sourceSeriesPartitionMap.get(targetSeriesSlot);
            Map<TTimePartitionSlot, List<TRegionReplicaSet>> targetTimePartionMap =
                targetSeriesSlotEntry.getValue();
            for (Map.Entry<TTimePartitionSlot, List<TRegionReplicaSet>> targetEntry :
                targetTimePartionMap.entrySet()) {
              if (!sourceTimePartitionMap.containsKey(targetEntry.getKey())) {
                sourceTimePartitionMap.put(targetEntry.getKey(), targetEntry.getValue());
              }
            }
          } else {
            sourceSeriesPartitionMap.put(targetSeriesSlot, targetSeriesSlotEntry.getValue());
          }
        }

      } else {
        dataPartitionMap.put(database, targetDbEntry.getValue());
      }
    }
  }
}
