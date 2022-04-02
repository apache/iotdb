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

import org.apache.iotdb.confignode.util.SerializeDeserializeUtil;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DataRegionInfo {

  // TODO: Serialize and Deserialize
  // Map<DataRegionID, List<DataNodeID>>
  private Map<Integer, List<Integer>> dataRegionDataNodesMap;

  // Map<StorageGroup, Map<DeviceGroupID, DataPartitionRule>>
  private final Map<String, Map<Integer, DataPartitionRule>> dataPartitionRuleTable;

  public DataRegionInfo() {
    this.dataRegionDataNodesMap = new HashMap<>();
    this.dataPartitionRuleTable = new HashMap<>();
  }

  public Map<Integer, List<Integer>> getDataRegionDataNodesMap() {
    return dataRegionDataNodesMap;
  }

  public void createDataRegion(int dataRegionGroup, List<Integer> dataNodeList) {
    dataRegionDataNodesMap.put(dataRegionGroup, dataNodeList);
  }

  public List<Integer> getDataRegionLocation(int dataRegionGroup) {
    return dataRegionDataNodesMap.get(dataRegionGroup);
  }

  public void updateDataPartitionRule(
      String StorageGroup, int deviceGroup, DataPartitionRule rule) {
    // TODO: Data partition policy by @YongzaoDan
  }

  public void serializeImpl(ByteBuffer buffer) {
    SerializeDeserializeUtil.writeIntMapLists(dataRegionDataNodesMap, buffer);
  }

  public void deserializeImpl(ByteBuffer buffer) {
    dataRegionDataNodesMap = SerializeDeserializeUtil.readIntMapLists(buffer);
  }
}
