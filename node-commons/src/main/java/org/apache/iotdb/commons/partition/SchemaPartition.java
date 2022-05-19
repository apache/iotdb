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
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TProtocol;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

public class SchemaPartition extends Partition {

  // Map<StorageGroup, Map<TSeriesPartitionSlot, TSchemaRegionPlaceInfo>>
  private Map<String, Map<TSeriesPartitionSlot, TRegionReplicaSet>> schemaPartitionMap;

  public SchemaPartition(String seriesSlotExecutorName, int seriesPartitionSlotNum) {
    super(seriesSlotExecutorName, seriesPartitionSlotNum);
  }

  @Override
  public boolean isEmpty() {
    return schemaPartitionMap == null || schemaPartitionMap.isEmpty();
  }

  public SchemaPartition(
      Map<String, Map<TSeriesPartitionSlot, TRegionReplicaSet>> schemaPartitionMap,
      String seriesSlotExecutorName,
      int seriesPartitionSlotNum) {
    this(seriesSlotExecutorName, seriesPartitionSlotNum);
    this.schemaPartitionMap = schemaPartitionMap;
  }

  public Map<String, Map<TSeriesPartitionSlot, TRegionReplicaSet>> getSchemaPartitionMap() {
    return schemaPartitionMap;
  }

  public void setSchemaPartitionMap(
      Map<String, Map<TSeriesPartitionSlot, TRegionReplicaSet>> schemaPartitionMap) {
    this.schemaPartitionMap = schemaPartitionMap;
  }

  public TRegionReplicaSet getSchemaRegionReplicaSet(String deviceName) {
    // A list of data region replica sets will store data in a same time partition.
    // We will insert data to the last set in the list.
    // TODO return the latest dataRegionReplicaSet for each time partition
    String storageGroup = getStorageGroupByDevice(deviceName);
    TSeriesPartitionSlot seriesPartitionSlot = calculateDeviceGroupId(deviceName);
    return schemaPartitionMap.get(storageGroup).get(seriesPartitionSlot);
  }

  private String getStorageGroupByDevice(String deviceName) {
    for (String storageGroup : schemaPartitionMap.keySet()) {
      if (deviceName.startsWith(storageGroup + ".")) {
        return storageGroup;
      }
    }
    // TODO: (xingtanzjr) how to handle this exception in IoTDB
    return null;
  }

  /* Interfaces for ConfigNode */

  /**
   * Get SchemaPartition by partitionSlotsMap
   *
   * @param partitionSlotsMap Map<StorageGroup, List<SeriesPartitionSlot>>
   * @return Subset of current SchemaPartition, including Map<StorageGroup, Map<SeriesPartitionSlot,
   *     RegionReplicaSet>>
   */
  public SchemaPartition getSchemaPartition(
      Map<String, List<TSeriesPartitionSlot>> partitionSlotsMap) {
    if (partitionSlotsMap.isEmpty()) {
      // Return all SchemaPartitions when the partitionSlotsMap is empty
      return new SchemaPartition(
          new HashMap<>(schemaPartitionMap), seriesSlotExecutorName, seriesPartitionSlotNum);
    } else {
      Map<String, Map<TSeriesPartitionSlot, TRegionReplicaSet>> result = new HashMap<>();

      partitionSlotsMap.forEach(
          (storageGroup, seriesPartitionSlots) -> {
            if (schemaPartitionMap.containsKey(storageGroup)) {
              if (seriesPartitionSlots.isEmpty()) {
                // Return all SchemaPartitions in one StorageGroup when the queried
                // SeriesPartitionSlots is empty
                result.put(storageGroup, new HashMap<>(schemaPartitionMap.get(storageGroup)));
              } else {
                // Return the specific SchemaPartition
                seriesPartitionSlots.forEach(
                    seriesPartitionSlot -> {
                      if (schemaPartitionMap.get(storageGroup).containsKey(seriesPartitionSlot)) {
                        result
                            .computeIfAbsent(storageGroup, key -> new HashMap<>())
                            .put(
                                seriesPartitionSlot,
                                schemaPartitionMap.get(storageGroup).get(seriesPartitionSlot));
                      }
                    });
              }
            }
          });

      return new SchemaPartition(result, seriesSlotExecutorName, seriesPartitionSlotNum);
    }
  }

  /**
   * Get SchemaPartition by storageGroup name
   *
   * @param matchedStorageGroup List<String>
   * @return Subset of current SchemaPartition which contains matchedStorageGroup
   */
  public SchemaPartition getSchemaPartition(List<String> matchedStorageGroup) {
    Map<String, Map<TSeriesPartitionSlot, TRegionReplicaSet>> result = new HashMap<>();
    matchedStorageGroup.forEach(
        (storageGroup) -> {
          if (schemaPartitionMap.containsKey(storageGroup)) {
            result.put(storageGroup, new HashMap<>(schemaPartitionMap.get(storageGroup)));
          }
        });
    return new SchemaPartition(result, seriesSlotExecutorName, seriesPartitionSlotNum);
  }

  /**
   * Filter out unassigned PartitionSlots
   *
   * @param partitionSlotsMap Map<StorageGroupName, List<SeriesPartitionSlot>>
   * @return Map<String, List < SeriesPartitionSlot>>, unassigned PartitionSlots
   */
  public Map<String, List<TSeriesPartitionSlot>> filterNoAssignedSchemaPartitionSlot(
      Map<String, List<TSeriesPartitionSlot>> partitionSlotsMap) {
    Map<String, List<TSeriesPartitionSlot>> result = new HashMap<>();

    partitionSlotsMap.forEach(
        (storageGroup, seriesPartitionSlots) -> {
          // Compare StorageGroup
          if (!schemaPartitionMap.containsKey(storageGroup)) {
            result.put(storageGroup, partitionSlotsMap.get(storageGroup));
          } else {
            seriesPartitionSlots.forEach(
                seriesPartitionSlot -> {
                  // Compare SeriesPartitionSlot
                  if (!schemaPartitionMap.get(storageGroup).containsKey(seriesPartitionSlot)) {
                    result
                        .computeIfAbsent(storageGroup, key -> new ArrayList<>())
                        .add(seriesPartitionSlot);
                  }
                });
          }
        });

    return result;
  }

  /** Create a SchemaPartition by ConfigNode */
  public void createSchemaPartition(
      String storageGroup,
      TSeriesPartitionSlot seriesPartitionSlot,
      TRegionReplicaSet regionReplicaSet) {
    schemaPartitionMap
        .computeIfAbsent(storageGroup, key -> new HashMap<>())
        .put(seriesPartitionSlot, regionReplicaSet);
  }

  public void serialize(DataOutputStream dataOutputStream, TProtocol protocol)
      throws IOException, TException {
    dataOutputStream.writeInt(schemaPartitionMap.size());
    for (Entry<String, Map<TSeriesPartitionSlot, TRegionReplicaSet>> entry :
        schemaPartitionMap.entrySet()) {
      ReadWriteIOUtils.write(entry.getKey(), dataOutputStream);
      writeMap(entry.getValue(), dataOutputStream, protocol);
    }
  }

  public void deserialize(DataInputStream dataInputStream, TProtocol protocol)
      throws TException, IOException {
    int size = dataInputStream.readInt();
    while (size > 0) {
      String key = ReadWriteIOUtils.readString(dataInputStream);
      Map<TSeriesPartitionSlot, TRegionReplicaSet> value = readMap(dataInputStream, protocol);
      schemaPartitionMap.put(key, value);
      size--;
    }
  }

  private Map<TSeriesPartitionSlot, TRegionReplicaSet> readMap(
      DataInputStream dataInputStream, TProtocol protocol) throws TException, IOException {
    int size = dataInputStream.readInt();
    Map<TSeriesPartitionSlot, TRegionReplicaSet> result = new HashMap<>();
    while (size > 0) {
      TSeriesPartitionSlot tSeriesPartitionSlot = new TSeriesPartitionSlot();
      tSeriesPartitionSlot.read(protocol);
      TRegionReplicaSet tRegionReplicaSet = new TRegionReplicaSet();
      tRegionReplicaSet.read(protocol);
      result.put(tSeriesPartitionSlot, tRegionReplicaSet);
      size--;
    }
    return result;
  }

  private void writeMap(
      Map<TSeriesPartitionSlot, TRegionReplicaSet> valueMap,
      DataOutputStream dataOutputStream,
      TProtocol protocol)
      throws TException, IOException {
    dataOutputStream.writeInt(valueMap.size());
    for (Entry<TSeriesPartitionSlot, TRegionReplicaSet> entry : valueMap.entrySet()) {
      entry.getKey().write(protocol);
      entry.getValue().write(protocol);
    }
  }
}
