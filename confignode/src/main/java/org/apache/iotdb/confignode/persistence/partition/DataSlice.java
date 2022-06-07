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
import org.apache.iotdb.common.rpc.thrift.TTimePartitionSlot;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class DataSlice {

  private final Map<TTimePartitionSlot, List<TConsensusGroupId>> dataSliceMap;

  public DataSlice() {
    this.dataSliceMap = new ConcurrentHashMap<>();
  }

  public DataSlice(Map<TTimePartitionSlot, List<TConsensusGroupId>> dataSliceMap) {
    this.dataSliceMap = dataSliceMap;
  }

  public Map<TTimePartitionSlot, List<TConsensusGroupId>> getDataSliceMap() {
    return dataSliceMap;
  }

  /**
   * Thread-safely get DataPartition within the specific StorageGroup
   *
   * @param partitionSlots TimePartitionSlots
   * @return RegionIds of the PartitionSlots
   */
  public DataSlice getDataPartition(List<TTimePartitionSlot> partitionSlots) {
    if (partitionSlots.isEmpty()) {
      // Return all DataPartitions in one SeriesPartitionSlot when the queried
      // PartitionSlots are empty
      return new DataSlice(new ConcurrentHashMap<>(dataSliceMap));
    } else {
      // Return the DataPartition for each TimePartitionSlot
      Map<TTimePartitionSlot, List<TConsensusGroupId>> result = new ConcurrentHashMap<>();
      partitionSlots.forEach(timePartitionSlot -> {
        if (dataSliceMap.containsKey(timePartitionSlot)) {
          result.put(timePartitionSlot, new ArrayList<>(dataSliceMap.get(timePartitionSlot)));
        }
        }
        );
      return new DataSlice(result);
    }
  }

  /**
   * Thread-safely create DataPartition within the specific SeriesPartitionSlot
   *
   * @param assignedDataPartition assigned result
   * @param deltaMap Number of DataPartitions added to each Region
   */
  public void createDataPartition(Map<TTimePartitionSlot, TConsensusGroupId> assignedDataPartition, Map<TConsensusGroupId, AtomicInteger> deltaMap) {
    assignedDataPartition.forEach(((timePartitionSlot, consensusGroupId) -> {
      // In concurrent scenarios, some DataPartitions may have already been created by another thread.
      // Therefore, we could just ignore them.
      if (!dataSliceMap.containsKey(timePartitionSlot)) {
        List<TConsensusGroupId> consensusGroupIds = Collections.synchronizedList(new ArrayList<>());
        consensusGroupIds.add(consensusGroupId);
        dataSliceMap.put(timePartitionSlot, consensusGroupIds);
        deltaMap.computeIfAbsent(consensusGroupId, empty -> new AtomicInteger(0)).getAndIncrement();
      }
    }));
  }

  /**
   * Only Leader use this interface
   * Thread-safely filter no assigned DataPartitionSlots within the specific SeriesPartitionSlot
   *
   * @param partitionSlots TimePartitionSlots
   * @return Un-assigned PartitionSlots
   */
  public List<TTimePartitionSlot> filterNoAssignedSchemaPartitionSlots(List<TTimePartitionSlot> partitionSlots) {
    List<TTimePartitionSlot> result = new Vector<>();

    partitionSlots.forEach(timePartitionSlot -> {
      if (!dataSliceMap.containsKey(timePartitionSlot)) {
        result.add(timePartitionSlot);
      }
    });

    return result;
  }
}
