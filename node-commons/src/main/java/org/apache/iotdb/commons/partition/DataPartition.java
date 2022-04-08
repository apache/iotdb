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

import java.util.*;
import java.util.stream.Collectors;

public class DataPartition {

  // Map<StorageGroup, Map<DeviceGroupID, Map<TimePartitionId, List<DataRegionPlaceInfo>>>>
  private Map<String, Map<SeriesPartitionSlot, Map<TimePartitionSlot, List<RegionReplicaSet>>>>
      dataPartitionMap;

  public Map<String, Map<SeriesPartitionSlot, Map<TimePartitionSlot, List<RegionReplicaSet>>>>
      getDataPartitionMap() {
    return dataPartitionMap;
  }

  public void setDataPartitionMap(
      Map<String, Map<SeriesPartitionSlot, Map<TimePartitionSlot, List<RegionReplicaSet>>>>
          dataPartitionMap) {
    this.dataPartitionMap = dataPartitionMap;
  }

  public Map<String, Map<SeriesPartitionSlot, Map<TimePartitionSlot, List<RegionReplicaSet>>>> getDataPartition(String storageGroup, Map<Integer, List<Long>> partitionSlots) {
    Map<String, Map<SeriesPartitionSlot, Map<TimePartitionSlot, List<RegionReplicaSet>>>> result = new HashMap<>();
    Map<SeriesPartitionSlot, RegionReplicaSet> deviceGroupMap = new HashMap<>();
    partitionSlots.forEach(
            seriesPartitionSlot -> {
              if (dataPartitionMap.get(storageGroup) != null
                      && dataPartitionMap
                      .get(storageGroup)
                      .containsKey(new SeriesPartitionSlot(seriesPartitionSlot))) {
                deviceGroupMap.put(
                        new SeriesPartitionSlot(seriesPartitionSlot),
                        schemaPartitionMap.get(storageGroup).get(new SeriesPartitionSlot(seriesPartitionSlot)));
              }
            });
    storageGroupMap.put(storageGroup, deviceGroupMap);
    return result;
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
      String deviceName, List<TimePartitionSlot> timePartitionIdList) {
    // A list of data region replica sets will store data in a same time partition.
    // We will insert data to the last set in the list.
    // TODO return the latest dataRegionReplicaSet for each time partition
    return Collections.emptyList();
  }

  public RegionReplicaSet getDataRegionReplicaSetForWriting(
      String deviceName, TimePartitionSlot timePartitionIdList) {
    // A list of data region replica sets will store data in a same time partition.
    // We will insert data to the last set in the list.
    // TODO return the latest dataRegionReplicaSet for each time partition
    return null;
  }

  private SeriesPartitionSlot calculateDeviceGroupId(String deviceName) {
    // TODO: (xingtanzjr) implement the real algorithm for calculation of DeviceGroupId
    return new SeriesPartitionSlot(deviceName.length());
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

  /**
   * Filter out unassigned SeriesPartitionSlots and TimePartitionSlots
   *
   * @param storageGroup storage group name
   * @param seriesPartitionTimePartitionSlots SeriesPartitionSlotIds and TimePartitionSlotIds
   * @return not assigned seriesPartitionSlots and TimePartitionSlots
   */
  public Map<Integer, List<Long>> filterDataRegionNoAssignedPartitionSlots(String storageGroup, Map<Integer, List<Long>> seriesPartitionTimePartitionSlots) {
    if (!dataPartitionMap.containsKey(storageGroup)) {
      return seriesPartitionTimePartitionSlots;
    }

    Map<Integer, List<Long>> result = new HashMap<>();
    for (int seriesPartitionSlotId : seriesPartitionTimePartitionSlots.keySet()) {
      SeriesPartitionSlot seriesPartitionSlot = new SeriesPartitionSlot(seriesPartitionSlotId);
      if (!dataPartitionMap.get(storageGroup).containsKey(seriesPartitionSlot)) {
        result.put(seriesPartitionSlotId, seriesPartitionTimePartitionSlots.get(seriesPartitionSlotId));
      } else {
        for (long timePartitionSlotId : seriesPartitionTimePartitionSlots.get(seriesPartitionSlotId)) {
          TimePartitionSlot timePartitionSlot = new TimePartitionSlot(timePartitionSlotId);
          if (!dataPartitionMap.get(storageGroup).get(seriesPartitionSlot).containsKey(timePartitionSlot)) {
            result.computeIfAbsent(seriesPartitionSlotId, key -> new ArrayList<>()).add(timePartitionSlotId);
          }
        }
      }
    }

    return result;
  }
}
