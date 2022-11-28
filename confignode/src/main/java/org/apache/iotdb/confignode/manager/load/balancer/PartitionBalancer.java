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
package org.apache.iotdb.confignode.manager.load.balancer;

import org.apache.iotdb.common.rpc.thrift.TSeriesPartitionSlot;
import org.apache.iotdb.commons.partition.DataPartitionTable;
import org.apache.iotdb.commons.partition.SchemaPartitionTable;
import org.apache.iotdb.confignode.exception.NoAvailableRegionGroupException;
import org.apache.iotdb.confignode.manager.IManager;
import org.apache.iotdb.confignode.manager.load.balancer.partition.GreedyPartitionAllocator;
import org.apache.iotdb.confignode.manager.load.balancer.partition.IPartitionAllocator;
import org.apache.iotdb.confignode.rpc.thrift.TTimeSlotList;

import java.util.List;
import java.util.Map;

/**
 * The SeriesPartitionSlotBalancer provides interfaces to generate optimal Partition allocation and
 * migration plans
 */
public class PartitionBalancer {

  private final IManager configManager;

  public PartitionBalancer(IManager configManager) {
    this.configManager = configManager;
  }

  /**
   * Allocate SchemaPartitions
   *
   * @param unassignedSchemaPartitionSlotsMap SchemaPartitionSlots that should be assigned
   * @return Map<StorageGroupName, SchemaPartitionTable>, the allocating result
   */
  public Map<String, SchemaPartitionTable> allocateSchemaPartition(
      Map<String, List<TSeriesPartitionSlot>> unassignedSchemaPartitionSlotsMap)
      throws NoAvailableRegionGroupException {
    return genPartitionAllocator().allocateSchemaPartition(unassignedSchemaPartitionSlotsMap);
  }

  /**
   * Allocate DataPartitions
   *
   * @param unassignedDataPartitionSlotsMap DataPartitionSlots that should be assigned
   * @return Map<StorageGroupName, DataPartitionTable>, the allocating result
   */
  public Map<String, DataPartitionTable> allocateDataPartition(
      Map<String, Map<TSeriesPartitionSlot, TTimeSlotList>> unassignedDataPartitionSlotsMap)
      throws NoAvailableRegionGroupException {
    return genPartitionAllocator().allocateDataPartition(unassignedDataPartitionSlotsMap);
  }

  private IPartitionAllocator genPartitionAllocator() {
    // TODO: The type of PartitionAllocator should be configurable
    return new GreedyPartitionAllocator(configManager);
  }
}
