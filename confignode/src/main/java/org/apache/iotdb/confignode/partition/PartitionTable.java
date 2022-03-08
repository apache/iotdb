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
package org.apache.iotdb.confignode.partition;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * PartitionTable stores metadata partition table, data partition table, and real-time write load
 * allocation rules
 */
public class PartitionTable {

  // Map<StorageGroup, Map<DeviceGroupID, MManagerGroupID>>
  private final Map<String, Map<Integer, Integer>> metadataPartitionTable;
  // Map<MManagerGroupID, List<PhysicalNodeIDs>>
  private final Map<Integer, List<Integer>> mManagerGroupPhysicalNodesMap;

  // Map<StorageGroup, Map<DeviceGroupID, Map<TimeInterval, List<VSGGroupID>>>>
  private final Map<String, Map<Integer, Map<Long, List<Integer>>>> dataPartitionTable;
  // Map<VSGGroupID, List<PhysicalNodeIDs>>
  private final Map<Integer, List<Integer>> vsgGroupPhysicalNodesMap;

  // Map<StorageGroup, Map<DeviceGroupID, DataPartitionRule>>
  private final Map<String, Map<Integer, DataPartitionRule>> dataPartitionRuleTable;

  public PartitionTable() {
    this.metadataPartitionTable = new HashMap<>();
    this.mManagerGroupPhysicalNodesMap = new HashMap<>();

    this.dataPartitionTable = new HashMap<>();
    this.vsgGroupPhysicalNodesMap = new HashMap<>();

    this.dataPartitionRuleTable = new HashMap<>();
  }

  // TODO: Interfaces for metadata operations

  // TODO: Interfaces for data operations

  // TODO: Interfaces for data partition rules
}
