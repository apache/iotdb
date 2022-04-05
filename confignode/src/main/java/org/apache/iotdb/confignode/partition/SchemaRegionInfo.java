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

public class SchemaRegionInfo {

  // TODO: Serialize and Deserialize
  // Map<SchemaRegionID, List<DataNodeID>>
  private Map<Integer, List<Integer>> schemaRegionDataNodesMap;

  public SchemaRegionInfo() {
    this.schemaRegionDataNodesMap = new HashMap<>();
  }

  public void addSchemaRegion(int schemaRegion, List<Integer> dataNode) {
    if (!schemaRegionDataNodesMap.containsKey(schemaRegion)) {
      schemaRegionDataNodesMap.put(schemaRegion, dataNode);
    }
  }

  public List<Integer> getSchemaRegionLocation(int schemaRegionGroup) {
    return schemaRegionDataNodesMap.get(schemaRegionGroup);
  }

  public Map<Integer, List<Integer>> getSchemaRegionDataNodesMap() {
    return schemaRegionDataNodesMap;
  }

  public void serializeImpl(ByteBuffer buffer) {
    SerializeDeserializeUtil.writeIntMapLists(schemaRegionDataNodesMap, buffer);
  }

  public void deserializeImpl(ByteBuffer buffer) {
    schemaRegionDataNodesMap = SerializeDeserializeUtil.readIntMapLists(buffer);
  }
}
