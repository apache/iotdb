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
import org.apache.iotdb.confignode.conf.ConfigNodeDescriptor;
import org.apache.iotdb.confignode.manager.IManager;
import org.apache.iotdb.confignode.manager.PartitionManager;
import org.apache.iotdb.tsfile.utils.Pair;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/** Allocating new Partitions by greedy algorithm */
public class GreedyPartitionAllocator implements IPartitionAllocator {

  private static final long TIME_PARTITION_INTERVAL =
      ConfigNodeDescriptor.getInstance().getConf().getTimePartitionInterval();

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
            bubbleSort(regionSlotsCounter.get(0).getRight(), regionSlotsCounter);
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

          DataPartitionTable dataPartitionTable = new DataPartitionTable();

          // Enumerate SeriesPartitionSlot
          for (Map.Entry<TSeriesPartitionSlot, List<TTimePartitionSlot>> seriesPartitionEntry :
              unassignedPartitionSlotsMap.entrySet()) {
            SeriesPartitionTable seriesPartitionTable = new SeriesPartitionTable();

            // Enumerate TimePartitionSlot in ascending order
            List<TTimePartitionSlot> timePartitionSlots = seriesPartitionEntry.getValue();
            timePartitionSlots.sort(Comparator.comparingLong(TTimePartitionSlot::getStartTime));
            for (TTimePartitionSlot timePartitionSlot : timePartitionSlots) {

              /* Check if the current DataPartition has predecessor firstly, and inherit it if exists */

              // Check if the current Partition's predecessor is allocated
              // in the same batch of Partition creation
              TConsensusGroupId predecessor =
                  seriesPartitionTable.getPrecededDataPartition(
                      timePartitionSlot, TIME_PARTITION_INTERVAL);
              if (predecessor != null) {
                seriesPartitionTable
                    .getSeriesPartitionMap()
                    .put(timePartitionSlot, Collections.singletonList(predecessor));
                bubbleSort(predecessor, regionSlotsCounter);
                continue;
              }

              // Check if the current Partition's predecessor was allocated
              // in the former Partition creation
              predecessor =
                  getPartitionManager()
                      .getPrecededDataPartition(
                          storageGroup,
                          seriesPartitionEntry.getKey(),
                          timePartitionSlot,
                          TIME_PARTITION_INTERVAL);
              if (predecessor != null) {
                seriesPartitionTable
                    .getSeriesPartitionMap()
                    .put(timePartitionSlot, Collections.singletonList(predecessor));
                bubbleSort(predecessor, regionSlotsCounter);
                continue;
              }

              /* Greedy allocation */
              seriesPartitionTable
                  .getSeriesPartitionMap()
                  .put(
                      timePartitionSlot,
                      Collections.singletonList(regionSlotsCounter.get(0).getRight()));
              bubbleSort(regionSlotsCounter.get(0).getRight(), regionSlotsCounter);
            }
            dataPartitionTable
                .getDataPartitionMap()
                .put(seriesPartitionEntry.getKey(), seriesPartitionTable);
          }
          result.put(storageGroup, dataPartitionTable);
        });

    return result;
  }

  /**
   * Bubble sort the regionSlotsCounter from the specified consensus group
   *
   * <p>Notice: Here we use bubble sort instead of other sorting algorithm is because that, there is
   * only one Partition allocated in each loop. Therefore, only consider one consensus group weight
   * change is enough
   *
   * @param consensusGroupId The consensus group where the new Partition is allocated
   * @param regionSlotsCounter List<Pair<Allocated Partition num, TConsensusGroupId>>
   */
  private void bubbleSort(
      TConsensusGroupId consensusGroupId, List<Pair<Long, TConsensusGroupId>> regionSlotsCounter) {
    // Find the corresponding consensus group
    int index = 0;
    for (int i = 0; i < regionSlotsCounter.size(); i++) {
      if (regionSlotsCounter.get(i).getRight().equals(consensusGroupId)) {
        index = i;
        break;
      }
    }

    // Do bubble sort
    regionSlotsCounter.get(index).setLeft(regionSlotsCounter.get(index).getLeft() + 1);
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
