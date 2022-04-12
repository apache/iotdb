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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class SchemaPartition {

  // Map<StorageGroup, Map<DeviceGroupID, SchemaRegionPlaceInfo>>
  private Map<String, Map<SeriesPartitionSlot, RegionReplicaSet>> schemaPartitionMap;

  public SchemaPartition() {
    schemaPartitionMap = new HashMap<>();
  }

  public Map<String, Map<SeriesPartitionSlot, RegionReplicaSet>> getSchemaPartitionMap() {
    return schemaPartitionMap;
  }

  public void setSchemaPartitionMap(
      Map<String, Map<SeriesPartitionSlot, RegionReplicaSet>> schemaPartitionMap) {
    this.schemaPartitionMap = schemaPartitionMap;
  }

  public Map<String, Map<SeriesPartitionSlot, RegionReplicaSet>> getSchemaPartition(
      String storageGroup, List<Integer> seriesPartitionSlots) {
    Map<String, Map<SeriesPartitionSlot, RegionReplicaSet>> storageGroupMap = new HashMap<>();
    Map<SeriesPartitionSlot, RegionReplicaSet> deviceGroupMap = new HashMap<>();
    seriesPartitionSlots.forEach(
        deviceGroupID -> {
          if (schemaPartitionMap.get(storageGroup) != null
              && schemaPartitionMap
                  .get(storageGroup)
                  .containsKey(new SeriesPartitionSlot(deviceGroupID))) {
            deviceGroupMap.put(
                new SeriesPartitionSlot(deviceGroupID),
                schemaPartitionMap.get(storageGroup).get(new SeriesPartitionSlot(deviceGroupID)));
          }
        });
    storageGroupMap.put(storageGroup, deviceGroupMap);
    return storageGroupMap;
  }

  /**
   * Filter out unassigned SeriesPartitionSlots
   *
   * @param storageGroup storage group name
   * @param seriesPartitionSlots SeriesPartitionSlotIds
   * @return not assigned seriesPartitionSlots
   */
  public List<Integer> filterNoAssignedSeriesPartitionSlot(
      String storageGroup, List<Integer> seriesPartitionSlots) {
    if (!schemaPartitionMap.containsKey(storageGroup)) {
      return seriesPartitionSlots;
    }
    return seriesPartitionSlots.stream()
        .filter(
            id -> !schemaPartitionMap.get(storageGroup).containsKey(new SeriesPartitionSlot(id)))
        .collect(Collectors.toList());
  }

  public void setSchemaRegionReplicaSet(
      String storageGroup, int deviceGroupId, RegionReplicaSet regionReplicaSet) {
    schemaPartitionMap
        .computeIfAbsent(storageGroup, value -> new HashMap<>())
        .put(new SeriesPartitionSlot(deviceGroupId), regionReplicaSet);
  }
}
