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

import java.util.List;
import java.util.Map;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class SchemaPartitionTable {

  private final Map<TSeriesPartitionSlot, TConsensusGroupId> schemaPartitionMap;

  public SchemaPartitionTable() {
    this.schemaPartitionMap = new ConcurrentHashMap<>();
  }

  public SchemaPartitionTable(Map<TSeriesPartitionSlot, TConsensusGroupId> schemaPartitionMap) {
    this.schemaPartitionMap = schemaPartitionMap;
  }

  public Map<TSeriesPartitionSlot, TConsensusGroupId> getSchemaPartitionMap() {
    return schemaPartitionMap;
  }

  /**
   * Thread-safely get SchemaPartition within the specific StorageGroup
   *
   * @param partitionSlots SeriesPartitionSlots
   * @return SchemaPartitionTable
   */
  public SchemaPartitionTable getSchemaPartition(List<TSeriesPartitionSlot> partitionSlots) {
    if (partitionSlots.isEmpty()) {
      // Return all DataPartitions in one StorageGroup when the queried
      // PartitionSlots are empty
      return new SchemaPartitionTable(new ConcurrentHashMap<>(schemaPartitionMap));
    } else {
      // Return the DataPartition for each SeriesPartitionSlot
      Map<TSeriesPartitionSlot, TConsensusGroupId> result = new ConcurrentHashMap<>();
      partitionSlots.forEach(seriesPartitionSlot -> {
        if (schemaPartitionMap.containsKey(seriesPartitionSlot)) {
          result.put(seriesPartitionSlot, schemaPartitionMap.get(seriesPartitionSlot));
        }
      });
      return new SchemaPartitionTable(result);
    }
  }

  /**
   * Thread-safely create SchemaPartition within the specific StorageGroup
   *
   * @param assignedSchemaPartition assigned result
   * @return Number of SchemaPartitions added to each Region
   */
  public Map<TConsensusGroupId, AtomicInteger> createSchemaPartition(SchemaPartitionTable assignedSchemaPartition) {
    Map<TConsensusGroupId, AtomicInteger> deltaMap = new ConcurrentHashMap<>();

    assignedSchemaPartition.getSchemaPartitionMap().forEach(((seriesPartitionSlot, consensusGroupId) -> {
      // In concurrent scenarios, some SchemaPartitions may have already been created by another thread.
      // Therefore, we could just ignore them.
      if (!schemaPartitionMap.containsKey(seriesPartitionSlot)) {
        schemaPartitionMap.put(seriesPartitionSlot, consensusGroupId);
        deltaMap.computeIfAbsent(consensusGroupId, empty -> new AtomicInteger(0)).getAndIncrement();
      }
    }));

    return deltaMap;
  }

  /**
   * Only Leader use this interface
   * Thread-safely filter no assigned SchemaPartitionSlots within the specific StorageGroup
   *
   * @param partitionSlots List<TSeriesPartitionSlot>
   * @return Un-assigned PartitionSlots
   */
  public List<TSeriesPartitionSlot> filterNoAssignedSchemaPartitionSlots(List<TSeriesPartitionSlot> partitionSlots) {
    List<TSeriesPartitionSlot> result = new Vector<>();

    partitionSlots.forEach(seriesPartitionSlot -> {
      if (!schemaPartitionMap.containsKey(seriesPartitionSlot)) {
        result.add(seriesPartitionSlot);
      }
    });

    return result;
  }
}
