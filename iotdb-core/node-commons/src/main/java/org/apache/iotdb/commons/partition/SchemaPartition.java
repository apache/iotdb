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
import org.apache.iotdb.commons.exception.IoTDBException;
import org.apache.iotdb.commons.utils.PathUtils;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.tsfile.file.metadata.IDeviceID;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

// TODO: Remove this class
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

  // table model usage

  /**
   * For table model usage.
   *
   * <p>The database shall start with "root.". Concat this to a user-provided db name if necessary.
   *
   * <p>The device id shall be [table, seg1, ....]
   */
  public TRegionReplicaSet getSchemaRegionReplicaSet(String database, IDeviceID deviceID) {
    TSeriesPartitionSlot seriesPartitionSlot = calculateDeviceGroupId(deviceID);
    return schemaPartitionMap.get(database).get(seriesPartitionSlot);
  }

  // [root, db, ....]
  public TRegionReplicaSet getSchemaRegionReplicaSet(IDeviceID deviceID) {
    // A list of data region replica sets will store data in a same time partition.
    // We will insert data to the last set in the list.
    // TODO return the latest dataRegionReplicaSet for each time partition
    String storageGroup = getStorageGroupByDevice(deviceID);
    TSeriesPartitionSlot seriesPartitionSlot = calculateDeviceGroupId(deviceID);
    if (schemaPartitionMap.get(storageGroup) == null) {
      throw new RuntimeException(
          new IoTDBException("Path does not exist. ", TSStatusCode.PATH_NOT_EXIST.getStatusCode()));
    }
    return schemaPartitionMap.get(storageGroup).get(seriesPartitionSlot);
  }

  private String getStorageGroupByDevice(IDeviceID deviceID) {
    for (String storageGroup : schemaPartitionMap.keySet()) {
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
    schemaPartitionMap.forEach(
        (storageGroup, partition) -> {
          for (TRegionReplicaSet regionReplicaSet : partition.values()) {
            distributionMap
                .computeIfAbsent(regionReplicaSet, RegionReplicaSetInfo::new)
                .setStorageGroup(storageGroup);
          }
        });
    return new ArrayList<>(distributionMap.values());
  }

  @Override
  public String toString() {
    return "SchemaPartition{" + "schemaPartitionMap=" + schemaPartitionMap + '}';
  }
}
