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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SchemaPartition {

  // Map<StorageGroup, Map<SeriesPartitionSlot, SchemaRegionPlaceInfo>>
  private Map<String, Map<SeriesPartitionSlot, RegionReplicaSet>> schemaPartitionMap;

  public SchemaPartition() {
    // Empty constructor
  }

  public SchemaPartition(
      Map<String, Map<SeriesPartitionSlot, RegionReplicaSet>> schemaPartitionMap) {
    this.schemaPartitionMap = schemaPartitionMap;
  }

  public Map<String, Map<SeriesPartitionSlot, RegionReplicaSet>> getSchemaPartitionMap() {
    return schemaPartitionMap;
  }

  public void setSchemaPartitionMap(
      Map<String, Map<SeriesPartitionSlot, RegionReplicaSet>> schemaPartitionMap) {
    this.schemaPartitionMap = schemaPartitionMap;
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
      Map<String, List<SeriesPartitionSlot>> partitionSlotsMap) {
    if (partitionSlotsMap.isEmpty()) {
      // Return all SchemaPartitions when the partitionSlotsMap is empty
      return new SchemaPartition(new HashMap<>(schemaPartitionMap));
    } else {
      Map<String, Map<SeriesPartitionSlot, RegionReplicaSet>> result = new HashMap<>();

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

      return new SchemaPartition(result);
    }
  }

  /**
   * Filter out unassigned PartitionSlots
   *
   * @param partitionSlotsMap Map<StorageGroupName, List<SeriesPartitionSlot>>
   * @return Map<String, List<SeriesPartitionSlot>>, unassigned PartitionSlots
   */
  public Map<String, List<SeriesPartitionSlot>> filterNoAssignedSchemaPartitionSlot(
      Map<String, List<SeriesPartitionSlot>> partitionSlotsMap) {
    Map<String, List<SeriesPartitionSlot>> result = new HashMap<>();

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
      SeriesPartitionSlot seriesPartitionSlot,
      RegionReplicaSet regionReplicaSet) {
    schemaPartitionMap
        .computeIfAbsent(storageGroup, key -> new HashMap<>())
        .put(seriesPartitionSlot, regionReplicaSet);
  }
}
