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

import org.apache.iotdb.commons.partition.executor.SeriesPartitionExecutor;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class DataPartition {

  private String seriesSlotExecutorName;
  private int seriesPartitionSlotNum;

  // Map<StorageGroup, Map<SeriesPartitionSlot, Map<TimePartitionSlot, List<RegionMessage>>>>
  private Map<String, Map<SeriesPartitionSlot, Map<TimePartitionSlot, List<RegionReplicaSet>>>>
      dataPartitionMap;

  public DataPartition(String seriesSlotExecutorName, int seriesPartitionSlotNum) {
    this.seriesSlotExecutorName = seriesSlotExecutorName;
    this.seriesPartitionSlotNum = seriesPartitionSlotNum;
  }

  public DataPartition(
      Map<String, Map<SeriesPartitionSlot, Map<TimePartitionSlot, List<RegionReplicaSet>>>>
          dataPartitionMap,
      String seriesSlotExecutorName,
      int seriesPartitionSlotNum) {
    this(seriesSlotExecutorName, seriesPartitionSlotNum);
    this.dataPartitionMap = dataPartitionMap;
  }

  public Map<String, Map<SeriesPartitionSlot, Map<TimePartitionSlot, List<RegionReplicaSet>>>>
      getDataPartitionMap() {
    return dataPartitionMap;
  }

  public void setDataPartitionMap(
      Map<String, Map<SeriesPartitionSlot, Map<TimePartitionSlot, List<RegionReplicaSet>>>>
          dataPartitionMap) {
    this.dataPartitionMap = dataPartitionMap;
  }

  public List<RegionReplicaSet> getDataRegionReplicaSet(
      String deviceName, List<TimePartitionSlot> timePartitionSlotList) {
    String storageGroup = getStorageGroupByDevice(deviceName);
    SeriesPartitionSlot seriesPartitionSlot = calculateDeviceGroupId(deviceName);
    // TODO: (xingtanzjr) the timePartitionIdList is ignored
    return dataPartitionMap.get(storageGroup).get(seriesPartitionSlot).values().stream()
        .flatMap(Collection::stream)
        .collect(Collectors.toList());
  }

  public List<RegionReplicaSet> getDataRegionReplicaSetForWriting(
      String deviceName, List<TimePartitionSlot> timePartitionSlotList) {
    // A list of data region replica sets will store data in a same time partition.
    // We will insert data to the last set in the list.
    // TODO return the latest dataRegionReplicaSet for each time partition
    String storageGroup = getStorageGroupByDevice(deviceName);
    SeriesPartitionSlot seriesPartitionSlot = calculateDeviceGroupId(deviceName);
    // IMPORTANT TODO: (xingtanzjr) need to handle the situation for write operation that there are
    // more than 1 Regions for one timeSlot
    return dataPartitionMap.get(storageGroup).get(seriesPartitionSlot).entrySet().stream()
        .filter(entry -> timePartitionSlotList.contains(entry.getKey()))
        .flatMap(entry -> entry.getValue().stream())
        .collect(Collectors.toList());
  }

  public RegionReplicaSet getDataRegionReplicaSetForWriting(
      String deviceName, TimePartitionSlot timePartitionSlot) {
    // A list of data region replica sets will store data in a same time partition.
    // We will insert data to the last set in the list.
    // TODO return the latest dataRegionReplicaSet for each time partition
    String storageGroup = getStorageGroupByDevice(deviceName);
    SeriesPartitionSlot seriesPartitionSlot = calculateDeviceGroupId(deviceName);
    List<RegionReplicaSet> regions =
        dataPartitionMap.get(storageGroup).get(seriesPartitionSlot).entrySet().stream()
            .filter(entry -> entry.getKey().equals(timePartitionSlot))
            .flatMap(entry -> entry.getValue().stream())
            .collect(Collectors.toList());
    // IMPORTANT TODO: (xingtanzjr) need to handle the situation for write operation that there are
    // more than 1 Regions for one timeSlot
    return regions.get(0);
  }

  private SeriesPartitionSlot calculateDeviceGroupId(String deviceName) {
    SeriesPartitionExecutor executor =
        SeriesPartitionExecutor.getSeriesPartitionExecutor(
            seriesSlotExecutorName, seriesPartitionSlotNum);
    return executor.getSeriesPartitionSlot(deviceName);
  }

  private String getStorageGroupByDevice(String deviceName) {
    for (String storageGroup : dataPartitionMap.keySet()) {
      if (deviceName.startsWith(storageGroup)) {
        return storageGroup;
      }
    }
    // TODO: (xingtanzjr) how to handle this exception in IoTDB
    return null;
  }

  /* Interfaces for ConfigNode */

  /**
   * Get DataPartition by partitionSlotsMap
   *
   * @param partitionSlotsMap Map<StorageGroupName, Map<SeriesPartitionSlot,
   *     List<TimePartitionSlot>>>
   * @return Subset of current DataPartition, including Map<StorageGroupName,
   *     Map<SeriesPartitionSlot, Map<TimePartitionSlot, List<RegionReplicaSet>>>>
   */
  public DataPartition getDataPartition(
      Map<String, Map<SeriesPartitionSlot, List<TimePartitionSlot>>> partitionSlotsMap,
      String seriesSlotExecutorName,
      int seriesPartitionSlotNum) {
    Map<String, Map<SeriesPartitionSlot, Map<TimePartitionSlot, List<RegionReplicaSet>>>> result =
        new HashMap<>();

    for (String storageGroupName : partitionSlotsMap.keySet()) {
      // Compare StorageGroupName
      if (dataPartitionMap.containsKey(storageGroupName)) {
        Map<SeriesPartitionSlot, Map<TimePartitionSlot, List<RegionReplicaSet>>>
            seriesTimePartitionSlotMap = dataPartitionMap.get(storageGroupName);
        for (SeriesPartitionSlot seriesPartitionSlot :
            partitionSlotsMap.get(storageGroupName).keySet()) {
          // Compare SeriesPartitionSlot
          if (seriesTimePartitionSlotMap.containsKey(seriesPartitionSlot)) {
            Map<TimePartitionSlot, List<RegionReplicaSet>> timePartitionSlotMap =
                seriesTimePartitionSlotMap.get(seriesPartitionSlot);
            // TODO: (xingtanzjr) optimize if timeSlotPartition is empty
            if (partitionSlotsMap.get(storageGroupName).get(seriesPartitionSlot).size() == 0) {
              result
                  .computeIfAbsent(storageGroupName, key -> new HashMap<>())
                  .computeIfAbsent(seriesPartitionSlot, key -> new HashMap<>())
                  .putAll(new HashMap<>(timePartitionSlotMap));
            } else {
              for (TimePartitionSlot timePartitionSlot :
                  partitionSlotsMap.get(storageGroupName).get(seriesPartitionSlot)) {
                // Compare TimePartitionSlot
                if (timePartitionSlotMap.containsKey(timePartitionSlot)) {
                  result
                      .computeIfAbsent(storageGroupName, key -> new HashMap<>())
                      .computeIfAbsent(seriesPartitionSlot, key -> new HashMap<>())
                      .put(
                          timePartitionSlot,
                          new ArrayList<>(timePartitionSlotMap.get(timePartitionSlot)));
                }
              }
            }
          }
        }
      }
    }

    return new DataPartition(result, seriesSlotExecutorName, seriesPartitionSlotNum);
  }

  /**
   * Filter out unassigned PartitionSlots
   *
   * @param partitionSlotsMap Map<StorageGroupName, Map<SeriesPartitionSlot,
   *     List<TimePartitionSlot>>>
   * @return Map<StorageGroupName, Map < SeriesPartitionSlot, List < TimePartitionSlot>>>,
   *     unassigned PartitionSlots
   */
  public Map<String, Map<SeriesPartitionSlot, List<TimePartitionSlot>>>
      filterNoAssignedDataPartitionSlots(
          Map<String, Map<SeriesPartitionSlot, List<TimePartitionSlot>>> partitionSlotsMap) {
    Map<String, Map<SeriesPartitionSlot, List<TimePartitionSlot>>> result = new HashMap<>();

    for (String storageGroupName : partitionSlotsMap.keySet()) {
      // Compare StorageGroupName
      if (dataPartitionMap.containsKey(storageGroupName)) {
        Map<SeriesPartitionSlot, Map<TimePartitionSlot, List<RegionReplicaSet>>>
            seriesTimePartitionSlotMap = dataPartitionMap.get(storageGroupName);
        for (SeriesPartitionSlot seriesPartitionSlot :
            partitionSlotsMap.get(storageGroupName).keySet()) {
          // Compare SeriesPartitionSlot
          if (seriesTimePartitionSlotMap.containsKey(seriesPartitionSlot)) {
            Map<TimePartitionSlot, List<RegionReplicaSet>> timePartitionSlotMap =
                seriesTimePartitionSlotMap.get(seriesPartitionSlot);
            for (TimePartitionSlot timePartitionSlot :
                partitionSlotsMap.get(storageGroupName).get(seriesPartitionSlot)) {
              // Compare TimePartitionSlot
              if (!timePartitionSlotMap.containsKey(timePartitionSlot)) {
                result
                    .computeIfAbsent(storageGroupName, key -> new HashMap<>())
                    .computeIfAbsent(seriesPartitionSlot, key -> new ArrayList<>())
                    .add(timePartitionSlot);
              }
            }
          } else {
            // Clone all if SeriesPartitionSlot not assigned
            result
                .computeIfAbsent(storageGroupName, key -> new HashMap<>())
                .put(
                    seriesPartitionSlot,
                    new ArrayList<>(
                        partitionSlotsMap.get(storageGroupName).get(seriesPartitionSlot)));
          }
        }
      } else {
        // Clone all if StorageGroupName not assigned
        result.put(storageGroupName, new HashMap<>(partitionSlotsMap.get(storageGroupName)));
      }
    }

    return result;
  }

  /** Create a DataPartition by ConfigNode */
  public void createDataPartition(
      String storageGroup,
      SeriesPartitionSlot seriesPartitionSlot,
      TimePartitionSlot timePartitionSlot,
      RegionReplicaSet regionReplicaSet) {
    dataPartitionMap
        .computeIfAbsent(storageGroup, key -> new HashMap<>())
        .computeIfAbsent(seriesPartitionSlot, key -> new HashMap<>())
        .put(timePartitionSlot, Collections.singletonList(regionReplicaSet));
  }
}
