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
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TProtocol;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;

public class DataPartition extends Partition {

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

  public List<TRegionReplicaSet> getDataRegionReplicaSet(
      String deviceName, List<TTimePartitionSlot> timePartitionSlotList) {
    String storageGroup = getStorageGroupByDevice(deviceName);
    TSeriesPartitionSlot seriesPartitionSlot = calculateDeviceGroupId(deviceName);
    // TODO: (xingtanzjr) the timePartitionIdList is ignored
    return dataPartitionMap.get(storageGroup).get(seriesPartitionSlot).values().stream()
        .flatMap(Collection::stream)
        .collect(Collectors.toList());
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
    return dataPartitionMap.get(storageGroup).get(seriesPartitionSlot).entrySet().stream()
        .filter(entry -> timePartitionSlotList.contains(entry.getKey()))
        .flatMap(entry -> entry.getValue().stream())
        .collect(Collectors.toList());
  }

  public TRegionReplicaSet getDataRegionReplicaSetForWriting(
      String deviceName, TTimePartitionSlot timePartitionSlot) {
    // A list of data region replica sets will store data in a same time partition.
    // We will insert data to the last set in the list.
    // TODO return the latest dataRegionReplicaSet for each time partition
    String storageGroup = getStorageGroupByDevice(deviceName);
    TSeriesPartitionSlot seriesPartitionSlot = calculateDeviceGroupId(deviceName);
    List<TRegionReplicaSet> regions =
        dataPartitionMap.get(storageGroup).get(seriesPartitionSlot).entrySet().stream()
            .filter(entry -> entry.getKey().equals(timePartitionSlot))
            .flatMap(entry -> entry.getValue().stream())
            .collect(Collectors.toList());
    // IMPORTANT TODO: (xingtanzjr) need to handle the situation for write operation that there are
    // more than 1 Regions for one timeSlot
    return regions.get(0);
  }

  private String getStorageGroupByDevice(String deviceName) {
    for (String storageGroup : dataPartitionMap.keySet()) {
      if (deviceName.startsWith(storageGroup + ".")) {
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
      Map<String, Map<TSeriesPartitionSlot, List<TTimePartitionSlot>>> partitionSlotsMap,
      String seriesSlotExecutorName,
      int seriesPartitionSlotNum,
      Set<String> preDeletedStorageGroup) {
    Map<String, Map<TSeriesPartitionSlot, Map<TTimePartitionSlot, List<TRegionReplicaSet>>>>
        result = new HashMap<>();

    for (String storageGroupName : partitionSlotsMap.keySet()) {
      // Compare StorageGroupName
      if (dataPartitionMap.containsKey(storageGroupName)
          && !preDeletedStorageGroup.contains(storageGroupName)) {
        Map<TSeriesPartitionSlot, Map<TTimePartitionSlot, List<TRegionReplicaSet>>>
            seriesTimePartitionSlotMap = dataPartitionMap.get(storageGroupName);
        for (TSeriesPartitionSlot seriesPartitionSlot :
            partitionSlotsMap.get(storageGroupName).keySet()) {
          // Compare SeriesPartitionSlot
          if (seriesTimePartitionSlotMap.containsKey(seriesPartitionSlot)) {
            Map<TTimePartitionSlot, List<TRegionReplicaSet>> timePartitionSlotMap =
                seriesTimePartitionSlotMap.get(seriesPartitionSlot);
            if (partitionSlotsMap.get(storageGroupName).get(seriesPartitionSlot).size() == 0) {
              result
                  .computeIfAbsent(storageGroupName, key -> new HashMap<>())
                  .computeIfAbsent(seriesPartitionSlot, key -> new HashMap<>())
                  .putAll(new HashMap<>(timePartitionSlotMap));
            } else {
              for (TTimePartitionSlot timePartitionSlot :
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
  public Map<String, Map<TSeriesPartitionSlot, List<TTimePartitionSlot>>>
      filterNoAssignedDataPartitionSlots(
          Map<String, Map<TSeriesPartitionSlot, List<TTimePartitionSlot>>> partitionSlotsMap) {
    Map<String, Map<TSeriesPartitionSlot, List<TTimePartitionSlot>>> result = new HashMap<>();

    for (String storageGroupName : partitionSlotsMap.keySet()) {
      // Compare StorageGroupName
      if (dataPartitionMap.containsKey(storageGroupName)) {
        Map<TSeriesPartitionSlot, Map<TTimePartitionSlot, List<TRegionReplicaSet>>>
            seriesTimePartitionSlotMap = dataPartitionMap.get(storageGroupName);
        for (TSeriesPartitionSlot seriesPartitionSlot :
            partitionSlotsMap.get(storageGroupName).keySet()) {
          // Compare SeriesPartitionSlot
          if (seriesTimePartitionSlotMap.containsKey(seriesPartitionSlot)) {
            Map<TTimePartitionSlot, List<TRegionReplicaSet>> timePartitionSlotMap =
                seriesTimePartitionSlotMap.get(seriesPartitionSlot);
            for (TTimePartitionSlot timePartitionSlot :
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
      TSeriesPartitionSlot seriesPartitionSlot,
      TTimePartitionSlot timePartitionSlot,
      TRegionReplicaSet regionReplicaSet) {
    dataPartitionMap
        .computeIfAbsent(storageGroup, key -> new HashMap<>())
        .computeIfAbsent(seriesPartitionSlot, key -> new HashMap<>())
        .put(timePartitionSlot, Collections.singletonList(regionReplicaSet));
  }

  public void serialize(OutputStream outputStream, TProtocol protocol)
      throws IOException, TException {
    // Map<StorageGroup, Map<TSeriesPartitionSlot, Map<TTimePartitionSlot, List<TRegionMessage>>>>
    ReadWriteIOUtils.write(dataPartitionMap.size(), outputStream);
    for (Entry<String, Map<TSeriesPartitionSlot, Map<TTimePartitionSlot, List<TRegionReplicaSet>>>>
        entry : dataPartitionMap.entrySet()) {
      ReadWriteIOUtils.write(entry.getKey(), outputStream);
      ReadWriteIOUtils.write(entry.getValue().size(), outputStream);
      for (Entry<TSeriesPartitionSlot, Map<TTimePartitionSlot, List<TRegionReplicaSet>>>
          seriesPartitionSlotMapEntry : entry.getValue().entrySet()) {
        seriesPartitionSlotMapEntry.getKey().write(protocol);
        ReadWriteIOUtils.write(seriesPartitionSlotMapEntry.getValue().size(), outputStream);
        for (Entry<TTimePartitionSlot, List<TRegionReplicaSet>> timePartitionSlotListEntry :
            seriesPartitionSlotMapEntry.getValue().entrySet()) {
          timePartitionSlotListEntry.getKey().write(protocol);
          ReadWriteIOUtils.write(timePartitionSlotListEntry.getValue().size(), outputStream);
          for (TRegionReplicaSet tRegionReplicaSet : timePartitionSlotListEntry.getValue()) {
            tRegionReplicaSet.write(protocol);
          }
        }
      }
    }
  }

  public void deserialize(InputStream inputStream, TProtocol protocol)
      throws TException, IOException {
    int storageGroupNum = ReadWriteIOUtils.readInt(inputStream);
    // Map<StorageGroup, Map<TSeriesPartitionSlot, Map<TTimePartitionSlot, List<TRegionMessage>>>>
    while (storageGroupNum > 0) {
      String storageGroup = ReadWriteIOUtils.readString(inputStream);
      int tSeriesPartitionSlotNum = ReadWriteIOUtils.readInt(inputStream);

      Map<TSeriesPartitionSlot, Map<TTimePartitionSlot, List<TRegionReplicaSet>>>
          seriesPartitionSlotMapHashMap = new HashMap<>();
      while (tSeriesPartitionSlotNum > 0) {
        TSeriesPartitionSlot tSeriesPartitionSlot = new TSeriesPartitionSlot();
        tSeriesPartitionSlot.read(protocol);

        int tTimePartitionSlotNum = ReadWriteIOUtils.readInt(inputStream);

        Map<TTimePartitionSlot, List<TRegionReplicaSet>> timePartitionSlotListHashMap =
            new HashMap<>();
        while (tTimePartitionSlotNum > 0) {
          TTimePartitionSlot tTimePartitionSlot = new TTimePartitionSlot();
          tTimePartitionSlot.read(protocol);
          int size = ReadWriteIOUtils.readInt(inputStream);

          List<TRegionReplicaSet> tRegionMessageList = new ArrayList<>();
          while (size > 0) {
            TRegionReplicaSet tRegionReplicaSet = new TRegionReplicaSet();
            tRegionReplicaSet.read(protocol);
            tRegionMessageList.add(tRegionReplicaSet);
            size--;
          }
          timePartitionSlotListHashMap.put(tTimePartitionSlot, tRegionMessageList);
          tTimePartitionSlotNum--;
        }

        seriesPartitionSlotMapHashMap.put(tSeriesPartitionSlot, timePartitionSlotListHashMap);
        tSeriesPartitionSlotNum--;
      }
      dataPartitionMap.put(storageGroup, seriesPartitionSlotMapHashMap);
      storageGroupNum--;
    }
  }

  public List<RegionReplicaSetInfo> getDistributionInfo() {
    Map<TRegionReplicaSet, RegionReplicaSetInfo> distributionMap = new HashMap<>();

    dataPartitionMap.forEach(
        (storageGroup, partition) -> {
          List<TRegionReplicaSet> ret =
              partition.entrySet().stream()
                  .flatMap(
                      s -> s.getValue().entrySet().stream().flatMap(e -> e.getValue().stream()))
                  .collect(Collectors.toList());
          for (TRegionReplicaSet regionReplicaSet : ret) {
            distributionMap
                .computeIfAbsent(regionReplicaSet, RegionReplicaSetInfo::new)
                .setStorageGroup(storageGroup);
          }
        });
    return new ArrayList<>(distributionMap.values());
  }
}
