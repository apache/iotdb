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

package org.apache.iotdb.confignode.consensus.request.write.region;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.commons.utils.BasicStructureSerDeUtil;
import org.apache.iotdb.commons.utils.ThriftCommonsSerDeUtils;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlan;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlanType;

import org.slf4j.Logger;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.stream.Collectors;

/** Create regions for specified Databases. */
public class CreateRegionGroupsPlan extends ConfigPhysicalPlan {

  // Map<Database, List<TRegionReplicaSet>>
  protected final Map<String, List<TRegionReplicaSet>> regionGroupMap;
  protected final Map<TConsensusGroupId, Long> regionGroupCreateTimeMap;

  public CreateRegionGroupsPlan() {
    super(ConfigPhysicalPlanType.CreateRegionGroups);
    this.regionGroupMap = new HashMap<>();
    this.regionGroupCreateTimeMap = new HashMap<>();
  }

  public CreateRegionGroupsPlan(ConfigPhysicalPlanType type) {
    super(type);
    this.regionGroupMap = new HashMap<>();
    this.regionGroupCreateTimeMap = new HashMap<>();
  }

  public Map<String, List<TRegionReplicaSet>> getRegionGroupMap() {
    return regionGroupMap;
  }

  public Map<TConsensusGroupId, Long> getRegionGroupCreateTimeMap() {
    return regionGroupCreateTimeMap;
  }

  public void addRegionGroup(String database, TRegionReplicaSet regionReplicaSet, long createTime) {
    regionGroupMap
        .computeIfAbsent(database, regionReplicaSets -> new ArrayList<>())
        .add(regionReplicaSet);
    regionGroupCreateTimeMap.put(regionReplicaSet.getRegionId(), createTime);
  }

  public void planLog(Logger logger) {
    for (Map.Entry<String, List<TRegionReplicaSet>> regionGroupEntry : regionGroupMap.entrySet()) {
      String database = regionGroupEntry.getKey();
      for (TRegionReplicaSet regionReplicaSet : regionGroupEntry.getValue()) {
        logger.info(
            "[CreateRegionGroups] RegionGroup: {}, belonged database: {}, on DataNodes: {}",
            regionReplicaSet.getRegionId(),
            database,
            regionReplicaSet.getDataNodeLocations().stream()
                .map(TDataNodeLocation::getDataNodeId)
                .collect(Collectors.toList()));
      }
    }
  }

  public void serializeForProcedure(DataOutputStream stream) throws IOException {
    this.serializeImpl(stream);
  }

  public void deserializeForProcedure(ByteBuffer buffer) throws IOException {
    // to remove the planType of ConfigPhysicalPlanType
    buffer.getShort();
    this.deserializeImpl(buffer);
  }

  @Override
  protected void serializeImpl(DataOutputStream stream) throws IOException {
    stream.writeShort(getType().getPlanType());

    stream.writeInt(regionGroupMap.size());
    for (Entry<String, List<TRegionReplicaSet>> entry : regionGroupMap.entrySet()) {
      String database = entry.getKey();
      List<TRegionReplicaSet> regionReplicaSets = entry.getValue();
      BasicStructureSerDeUtil.write(database, stream);
      stream.writeInt(regionReplicaSets.size());
      regionReplicaSets.forEach(
          regionReplicaSet ->
              ThriftCommonsSerDeUtils.serializeTRegionReplicaSet(regionReplicaSet, stream));
    }

    stream.writeInt(regionGroupCreateTimeMap.size());
    for (Entry<TConsensusGroupId, Long> entry : regionGroupCreateTimeMap.entrySet()) {
      TConsensusGroupId regionId = entry.getKey();
      long createTime = entry.getValue();
      ThriftCommonsSerDeUtils.serializeTConsensusGroupId(regionId, stream);
      stream.writeLong(createTime);
    }
  }

  @Override
  protected void deserializeImpl(ByteBuffer buffer) throws IOException {
    int databaseNum = buffer.getInt();
    for (int i = 0; i < databaseNum; i++) {
      String database = BasicStructureSerDeUtil.readString(buffer);
      regionGroupMap.put(database, new ArrayList<>());

      int regionReplicaSetNum = buffer.getInt();
      for (int j = 0; j < regionReplicaSetNum; j++) {
        TRegionReplicaSet regionReplicaSet =
            ThriftCommonsSerDeUtils.deserializeTRegionReplicaSet(buffer);
        regionGroupMap.get(database).add(regionReplicaSet);
      }
    }

    if (buffer.hasRemaining()) {
      // For compatibility
      int regionGroupCreateTimeMapSize = buffer.getInt();
      for (int i = 0; i < regionGroupCreateTimeMapSize; i++) {
        TConsensusGroupId regionId = ThriftCommonsSerDeUtils.deserializeTConsensusGroupId(buffer);
        long createTime = buffer.getLong();
        regionGroupCreateTimeMap.put(regionId, createTime);
      }
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }
    CreateRegionGroupsPlan that = (CreateRegionGroupsPlan) o;
    return Objects.equals(regionGroupMap, that.regionGroupMap)
        && Objects.equals(regionGroupCreateTimeMap, that.regionGroupCreateTimeMap);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), regionGroupMap, regionGroupCreateTimeMap);
  }
}
