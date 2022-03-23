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

public class SchemaPartitionInfo {

  // TODO: Serialize and Deserialize
  // Map<StorageGroup, Map<DeviceGroupID, SchemaRegionID>>
  private final Map<String, Map<Integer, Integer>> schemaPartitionTable;
  // TODO: Serialize and Deserialize
  // Map<SchemaRegionID, List<DataNodeID>>
  private final Map<Integer, List<Integer>> schemaRegionDataNodesMap;

  public SchemaPartitionInfo() {
    this.schemaPartitionTable = new HashMap<>();
    this.schemaRegionDataNodesMap = new HashMap<>();
  }

  public void createSchemaPartition(String storageGroup, int deviceGroup, int schemaRegion) {
    if (!schemaPartitionTable.containsKey(storageGroup)) {
      schemaPartitionTable.put(storageGroup, new HashMap<>());
    }
    schemaPartitionTable.get(storageGroup).put(deviceGroup, schemaRegion);
  }

  public Integer getSchemaPartition(String storageGroup, int deviceGroup) {
    if (schemaPartitionTable.containsKey(storageGroup)) {
      return schemaPartitionTable.get(storageGroup).get(deviceGroup);
    }
    return null;
  }

  public void createSchemaRegion(int schemaRegion, List<Integer> dataNode) {
    if (!schemaRegionDataNodesMap.containsKey(schemaRegion)) {
      schemaRegionDataNodesMap.put(schemaRegion, dataNode);
    }
  }

  public List<Integer> getSchemaRegionLocation(int schemaRegionGroup) {
    return schemaRegionDataNodesMap.get(schemaRegionGroup);
  }
}
