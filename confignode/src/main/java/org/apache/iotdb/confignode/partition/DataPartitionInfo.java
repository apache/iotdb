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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DataPartitionInfo {

  // TODO: Serialize and Deserialize
  // Map<StorageGroup, Map<DeviceGroupID, Map<TimeInterval, List<DataRegionID>>>>
  private final Map<String, Map<Integer, Map<Long, List<Integer>>>> dataPartitionTable;
  // Map<DataRegionID, List<DataNodeID>>
  private final Map<Integer, List<Integer>> dataRegionDataNodesMap;

  // Map<StorageGroup, Map<DeviceGroupID, DataPartitionRule>>
  private final Map<String, Map<Integer, DataPartitionRule>> dataPartitionRuleTable;

  public DataPartitionInfo() {
    this.dataPartitionTable = new HashMap<>();
    this.dataRegionDataNodesMap = new HashMap<>();

    this.dataPartitionRuleTable = new HashMap<>();
  }

  public void createDataPartition(
      String storageGroup, int deviceGroup, long timeInterval, int dataRegionGroup) {
    if (!dataPartitionTable.containsKey(storageGroup)) {
      dataPartitionTable.put(storageGroup, new HashMap<>());
    }
    if (!dataPartitionTable.get(storageGroup).containsKey(deviceGroup)) {
      dataPartitionTable.get(storageGroup).put(deviceGroup, new HashMap<>());
    }
    if (!dataPartitionTable.get(storageGroup).get(deviceGroup).containsKey(timeInterval)) {
      dataPartitionTable.get(storageGroup).get(deviceGroup).put(timeInterval, new ArrayList<>());
    }
    dataPartitionTable.get(storageGroup).get(deviceGroup).get(timeInterval).add(dataRegionGroup);
  }

  public void createDataRegion(int dataRegionGroup, List<Integer> dataNodeList) {
    if (!dataRegionDataNodesMap.containsKey(dataRegionGroup)) {
      dataRegionDataNodesMap.put(dataRegionGroup, dataNodeList);
    }
  }

  public void updateDataPartitionRule(
      String StorageGroup, int deviceGroup, DataPartitionRule rule) {}
}
