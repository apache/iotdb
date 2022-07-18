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

public class DeleteRegionsPlan extends ConfigPhysicalPlan {

  private final Map<String, List<TConsensusGroupId>> deleteRegionMap;

  public DeleteRegionsPlan() {
    super(ConfigPhysicalPlanType.DeleteRegions);
    this.deleteRegionMap = new HashMap<>();
  }

  public void addDeleteRegion(String name, TConsensusGroupId consensusGroupId) {
    deleteRegionMap.computeIfAbsent(name, empty -> new ArrayList<>()).add(consensusGroupId);
  }

  public Map<String, List<TConsensusGroupId>> getDeleteRegionMap() {
    return deleteRegionMap;
  }

  @Override
  protected void serializeImpl(DataOutputStream stream) throws IOException {
    stream.writeInt(ConfigPhysicalPlanType.DeleteRegions.ordinal());

    stream.writeInt(deleteRegionMap.size());
    for (Map.Entry<String, List<TConsensusGroupId>> consensusGroupIdsEntry :
        deleteRegionMap.entrySet()) {
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
      deleteRegionMap.put(name, new ArrayList<>());
      int regionNum = buffer.getInt();
      for (int j = 0; j < regionNum; j++) {
        deleteRegionMap.get(name).add(ThriftCommonsSerDeUtils.deserializeTConsensusGroupId(buffer));
      }
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    DeleteRegionsPlan that = (DeleteRegionsPlan) o;
    return deleteRegionMap.equals(that.deleteRegionMap);
  }

  @Override
  public int hashCode() {
    return Objects.hash(deleteRegionMap);
  }
}
