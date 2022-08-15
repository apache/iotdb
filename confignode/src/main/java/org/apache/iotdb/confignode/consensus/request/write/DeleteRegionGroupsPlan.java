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
package org.apache.iotdb.confignode.consensus.request.write;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.commons.utils.BasicStructureSerDeUtil;
import org.apache.iotdb.commons.utils.ThriftCommonsSerDeUtils;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlan;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlanType;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class DeleteRegionGroupsPlan extends ConfigPhysicalPlan {

  // Map<StorageGroupName, List<TRegionReplicaSet>>
  private final Map<String, List<TConsensusGroupId>> regionGroupMap;

  public DeleteRegionGroupsPlan() {
    super(ConfigPhysicalPlanType.DeleteRegionGroups);
    this.regionGroupMap = new HashMap<>();
  }

  public void addDeleteRegion(String name, TConsensusGroupId consensusGroupId) {
    regionGroupMap.computeIfAbsent(name, empty -> new ArrayList<>()).add(consensusGroupId);
  }

  public Map<String, List<TConsensusGroupId>> getRegionGroupMap() {
    return regionGroupMap;
  }

  @Override
  protected void serializeImpl(DataOutputStream stream) throws IOException {
    stream.writeInt(ConfigPhysicalPlanType.DeleteRegionGroups.ordinal());

    stream.writeInt(regionGroupMap.size());
    for (Map.Entry<String, List<TConsensusGroupId>> consensusGroupIdsEntry :
        regionGroupMap.entrySet()) {
      BasicStructureSerDeUtil.write(consensusGroupIdsEntry.getKey(), stream);
      stream.writeInt(consensusGroupIdsEntry.getValue().size());
      for (TConsensusGroupId consensusGroupId : consensusGroupIdsEntry.getValue()) {
        ThriftCommonsSerDeUtils.serializeTConsensusGroupId(consensusGroupId, stream);
      }
    }
  }

  @Override
  protected void deserializeImpl(ByteBuffer buffer) throws IOException {
    int length = buffer.getInt();
    for (int i = 0; i < length; i++) {
      String name = BasicStructureSerDeUtil.readString(buffer);
      regionGroupMap.put(name, new ArrayList<>());
      int regionNum = buffer.getInt();
      for (int j = 0; j < regionNum; j++) {
        regionGroupMap.get(name).add(ThriftCommonsSerDeUtils.deserializeTConsensusGroupId(buffer));
      }
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    DeleteRegionGroupsPlan that = (DeleteRegionGroupsPlan) o;
    return regionGroupMap.equals(that.regionGroupMap);
  }

  @Override
  public int hashCode() {
    return Objects.hash(regionGroupMap);
  }
}
