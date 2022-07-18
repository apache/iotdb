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
package org.apache.iotdb.confignode.manager.load.balancer.partition;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TConsensusGroupType;
import org.apache.iotdb.common.rpc.thrift.TSeriesPartitionSlot;
import org.apache.iotdb.common.rpc.thrift.TTimePartitionSlot;
import org.apache.iotdb.commons.partition.DataPartitionTable;
import org.apache.iotdb.commons.partition.SchemaPartitionTable;
import org.apache.iotdb.commons.partition.SeriesPartitionTable;
import org.apache.iotdb.confignode.manager.IManager;
import org.apache.iotdb.confignode.manager.PartitionManager;
import org.apache.iotdb.tsfile.utils.Pair;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/** Allocating new Partitions by greedy algorithm */
public class GreedyPartitionAllocator implements IPartitionAllocator {

  private final IManager configManager;

  public GreedyPartitionAllocator(IManager configManager) {
    this.configManager = configManager;
  }

  @Override
  public Map<String, SchemaPartitionTable> allocateSchemaPartition(
      Map<String, List<TSeriesPartitionSlot>> unassignedSchemaPartitionSlotsMap) {
    Map<String, SchemaPartitionTable> result = new ConcurrentHashMap<>();

    unassignedSchemaPartitionSlotsMap.forEach(
        (storageGroup, unassignedPartitionSlots) -> {
          // List<Pair<allocatedSlotsNum, TConsensusGroupId>>
          List<Pair<Long, TConsensusGroupId>> regionSlotsCounter =
              getPartitionManager()
                  .getSortedRegionSlotsCounter(storageGroup, TConsensusGroupType.SchemaRegion);

          // Enumerate SeriesPartitionSlot
          Map<TSeriesPartitionSlot, TConsensusGroupId> schemaPartitionMap =
              new ConcurrentHashMap<>();
          for (TSeriesPartitionSlot seriesPartitionSlot : unassignedPartitionSlots) {
            // Greedy allocation
            schemaPartitionMap.put(seriesPartitionSlot, regionSlotsCounter.get(0).getRight());
            // Bubble sort
            bubbleSort(regionSlotsCounter);
          }
          result.put(storageGroup, new SchemaPartitionTable(schemaPartitionMap));
        });

    return result;
  }

  @Override
  public Map<String, DataPartitionTable> allocateDataPartition(
      Map<String, Map<TSeriesPartitionSlot, List<TTimePartitionSlot>>>
          unassignedDataPartitionSlotsMap) {
    Map<String, DataPartitionTable> result = new ConcurrentHashMap<>();

    unassignedDataPartitionSlotsMap.forEach(
        (storageGroup, unassignedPartitionSlotsMap) -> {
          // List<Pair<allocatedSlotsNum, TConsensusGroupId>>
          List<Pair<Long, TConsensusGroupId>> regionSlotsCounter =
              getPartitionManager()
                  .getSortedRegionSlotsCounter(storageGroup, TConsensusGroupType.DataRegion);

          // Enumerate SeriesPartitionSlot
          Map<TSeriesPartitionSlot, SeriesPartitionTable> dataPartitionMap =
              new ConcurrentHashMap<>();
          for (Map.Entry<TSeriesPartitionSlot, List<TTimePartitionSlot>> seriesPartitionEntry :
              unassignedPartitionSlotsMap.entrySet()) {
            // Enumerate TimePartitionSlot
            Map<TTimePartitionSlot, List<TConsensusGroupId>> seriesPartitionMap =
                new ConcurrentHashMap<>();
            for (TTimePartitionSlot timePartitionSlot : seriesPartitionEntry.getValue()) {
              // Greedy allocation
              seriesPartitionMap.put(
                  timePartitionSlot,
                  Collections.singletonList(regionSlotsCounter.get(0).getRight()));
              // Bubble sort
              bubbleSort(regionSlotsCounter);
            }
            dataPartitionMap.put(
                seriesPartitionEntry.getKey(), new SeriesPartitionTable(seriesPartitionMap));
          }
          result.put(storageGroup, new DataPartitionTable(dataPartitionMap));
        });

    return result;
  }

  private void bubbleSort(List<Pair<Long, TConsensusGroupId>> regionSlotsCounter) {
    int index = 0;
    regionSlotsCounter.get(0).setLeft(regionSlotsCounter.get(0).getLeft() + 1);
    while (index < regionSlotsCounter.size() - 1
        && regionSlotsCounter.get(index).getLeft() > regionSlotsCounter.get(index + 1).getLeft()) {
      Collections.swap(regionSlotsCounter, index, index + 1);
      index += 1;
    }
  }

  private PartitionManager getPartitionManager() {
    return configManager.getPartitionManager();
  }
}
