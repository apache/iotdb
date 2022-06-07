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
package org.apache.iotdb.confignode.persistence.partition;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TSeriesPartitionSlot;
import org.apache.iotdb.common.rpc.thrift.TTimePartitionSlot;

import java.util.List;
import java.util.Map;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class DataPartitionTable {

  private final Map<TSeriesPartitionSlot, DataSlice> dataPartitionMap;

  public DataPartitionTable() {
    this.dataPartitionMap = new ConcurrentHashMap<>();
  }

  public DataPartitionTable(Map<TSeriesPartitionSlot, DataSlice> dataPartitionMap) {
    this.dataPartitionMap = dataPartitionMap;
  }

  public Map<TSeriesPartitionSlot, DataSlice> getDataPartitionMap() {
    return dataPartitionMap;
  }

  /**
   * Thread-safely get DataPartition within the specific StorageGroup
   *
   * @param partitionSlots SeriesPartitionSlots and TimePartitionSlots
   * @return RegionIds of the PartitionSlots
   */
  public DataPartitionTable getDataPartition(Map<TSeriesPartitionSlot, List<TTimePartitionSlot>> partitionSlots) {
    if (partitionSlots.isEmpty()) {
      // Return all DataPartitions in one StorageGroup when the queried
      // PartitionSlots are empty
      return new DataPartitionTable(new ConcurrentHashMap<>(dataPartitionMap));
    } else {
      // Return the DataPartition for each SeriesPartitionSlot
      Map<TSeriesPartitionSlot, DataSlice> result = new ConcurrentHashMap<>();

      partitionSlots.forEach((seriesPartitionSlot, timePartitionSlots) -> {
        if (dataPartitionMap.containsKey(seriesPartitionSlot)) {
          result.put(seriesPartitionSlot, dataPartitionMap.get(seriesPartitionSlot).getDataPartition(timePartitionSlots));
        }
        }
        );

      return new DataPartitionTable(result);
    }
  }

  /**
   * Thread-safely create DataPartition within the specific StorageGroup
   *
   * @param assignedDataPartition assigned result
   * @return Number of DataPartitions added to each Region
   */
  public Map<TConsensusGroupId, AtomicInteger> createDataPartition(Map<TSeriesPartitionSlot, Map<TTimePartitionSlot, TConsensusGroupId>> assignedDataPartition) {
    Map<TConsensusGroupId, AtomicInteger> deltaMap = new ConcurrentHashMap<>();

    assignedDataPartition.forEach(((seriesPartitionSlot, consensusGroupId) ->
      dataPartitionMap.computeIfAbsent(seriesPartitionSlot, empty -> new DataSlice())
      .createDataPartition(assignedDataPartition.get(seriesPartitionSlot), deltaMap)));

    return deltaMap;
  }

  /**
   * Only Leader use this interface
   * Thread-safely filter no assigned DataPartitionSlots within the specific StorageGroup
   *
   * @param partitionSlots SeriesPartitionSlots and TimePartitionSlots
   * @return Un-assigned PartitionSlots
   */
  public Map<TSeriesPartitionSlot, List<TTimePartitionSlot>> filterNoAssignedSchemaPartitionSlots(Map<TSeriesPartitionSlot, List<TTimePartitionSlot>> partitionSlots) {
    Map<TSeriesPartitionSlot, List<TTimePartitionSlot>> result = new ConcurrentHashMap<>();

    partitionSlots.forEach((seriesPartitionSlot, timePartitionSlots) -> {
      if (!dataPartitionMap.containsKey(seriesPartitionSlot)) {
        result.put(seriesPartitionSlot, timePartitionSlots);
      } else {
        result.put(seriesPartitionSlot, dataPartitionMap.get(seriesPartitionSlot).filterNoAssignedSchemaPartitionSlots(timePartitionSlots));
      }
    });

    return result;
  }
}
