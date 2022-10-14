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
package org.apache.iotdb.confignode.consensus.request.write.statistics;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.commons.utils.ThriftCommonsSerDeUtils;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlan;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlanType;
import org.apache.iotdb.confignode.persistence.node.NodeStatistics;
import org.apache.iotdb.confignode.persistence.partition.statistics.RegionGroupStatistics;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class UpdateLoadStatisticsPlan extends ConfigPhysicalPlan {

  // Map<NodeId, newNodeStatistics>
  private final Map<Integer, NodeStatistics> nodeStatisticsMap;

  // Map<TConsensusGroupId, newRegionGroupStatistics>
  private final Map<TConsensusGroupId, RegionGroupStatistics> regionGroupStatisticsMap;

  public UpdateLoadStatisticsPlan() {
    super(ConfigPhysicalPlanType.UpdateLoadStatistics);
    this.nodeStatisticsMap = new HashMap<>();
    this.regionGroupStatisticsMap = new HashMap<>();
  }

  /** We should update and consensus the newer statistics information if exists */
  public boolean isNeedUpdate() {
    return !nodeStatisticsMap.isEmpty() || !regionGroupStatisticsMap.isEmpty();
  }

  public void putNodeStatistics(int nodeId, NodeStatistics nodeStatistics) {
    this.nodeStatisticsMap.put(nodeId, nodeStatistics);
  }

  public Map<Integer, NodeStatistics> getNodeStatisticsMap() {
    return nodeStatisticsMap;
  }

  public void putRegionGroupStatistics(
      TConsensusGroupId consensusGroupId, RegionGroupStatistics regionGroupStatistics) {
    this.regionGroupStatisticsMap.put(consensusGroupId, regionGroupStatistics);
  }

  public Map<TConsensusGroupId, RegionGroupStatistics> getRegionGroupStatisticsMap() {
    return regionGroupStatisticsMap;
  }

  @Override
  protected void serializeImpl(DataOutputStream stream) throws IOException {
    ReadWriteIOUtils.write(nodeStatisticsMap.size(), stream);
    for (Map.Entry<Integer, NodeStatistics> nodeStatisticsEntry : nodeStatisticsMap.entrySet()) {
      ReadWriteIOUtils.write(nodeStatisticsEntry.getKey(), stream);
      nodeStatisticsEntry.getValue().serialize(stream);
    }

    ReadWriteIOUtils.write(regionGroupStatisticsMap.size(), stream);
    for (Map.Entry<TConsensusGroupId, RegionGroupStatistics> regionGroupStatisticsEntry :
        regionGroupStatisticsMap.entrySet()) {
      ThriftCommonsSerDeUtils.serializeTConsensusGroupId(
          regionGroupStatisticsEntry.getKey(), stream);
      regionGroupStatisticsEntry.getValue().serialize(stream);
    }
  }

  @Override
  protected void deserializeImpl(ByteBuffer buffer) throws IOException {
    int nodeNum = buffer.getInt();
    for (int i = 0; i < nodeNum; i++) {
      int nodeId = buffer.getInt();
      NodeStatistics nodeStatistics = new NodeStatistics();
      nodeStatistics.deserialize(buffer);
      nodeStatisticsMap.put(nodeId, nodeStatistics);
    }

    int regionGroupNum = buffer.getInt();
    for (int i = 0; i < regionGroupNum; i++) {
      TConsensusGroupId consensusGroupId =
          ThriftCommonsSerDeUtils.deserializeTConsensusGroupId(buffer);
      RegionGroupStatistics regionGroupStatistics = new RegionGroupStatistics();
      regionGroupStatistics.deserialize(buffer);
      regionGroupStatisticsMap.put(consensusGroupId, regionGroupStatistics);
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    if (!super.equals(o)) return false;
    UpdateLoadStatisticsPlan that = (UpdateLoadStatisticsPlan) o;
    return nodeStatisticsMap.equals(that.nodeStatisticsMap)
        && regionGroupStatisticsMap.equals(that.regionGroupStatisticsMap);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), nodeStatisticsMap, regionGroupStatisticsMap);
  }
}
