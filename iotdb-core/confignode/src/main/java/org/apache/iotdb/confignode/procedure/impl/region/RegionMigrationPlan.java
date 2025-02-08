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

package org.apache.iotdb.confignode.procedure.impl.region;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.commons.utils.ThriftCommonsSerDeUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;

public class RegionMigrationPlan {
  private TConsensusGroupId regionId;
  private TDataNodeLocation fromDataNode;
  private TDataNodeLocation toDataNode;

  public RegionMigrationPlan(TConsensusGroupId regionId, TDataNodeLocation fromDataNode) {
    this.regionId = regionId;
    this.fromDataNode = fromDataNode;
    // default value is fromDataNode, which means no migration
    this.toDataNode = fromDataNode;
  }

  public static RegionMigrationPlan create(
      TConsensusGroupId regionId, TDataNodeLocation fromDataNode) {
    return new RegionMigrationPlan(regionId, fromDataNode);
  }

  public TConsensusGroupId getRegionId() {
    return regionId;
  }

  public TDataNodeLocation getFromDataNode() {
    return fromDataNode;
  }

  public TDataNodeLocation getToDataNode() {
    return toDataNode;
  }

  public void setToDataNode(TDataNodeLocation toDataNode) {
    this.toDataNode = toDataNode;
  }

  public void serialize(DataOutputStream stream) throws IOException {
    ThriftCommonsSerDeUtils.serializeTConsensusGroupId(regionId, stream);
    ThriftCommonsSerDeUtils.serializeTDataNodeLocation(fromDataNode, stream);
    ThriftCommonsSerDeUtils.serializeTDataNodeLocation(toDataNode, stream);
  }

  public static RegionMigrationPlan deserialize(ByteBuffer byteBuffer) {
    TConsensusGroupId regionId = ThriftCommonsSerDeUtils.deserializeTConsensusGroupId(byteBuffer);
    TDataNodeLocation fromDataNode =
        ThriftCommonsSerDeUtils.deserializeTDataNodeLocation(byteBuffer);
    RegionMigrationPlan plan = RegionMigrationPlan.create(regionId, fromDataNode);
    plan.setToDataNode(ThriftCommonsSerDeUtils.deserializeTDataNodeLocation(byteBuffer));
    return plan;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    RegionMigrationPlan that = (RegionMigrationPlan) obj;
    return regionId.equals(that.regionId)
        && fromDataNode.equals(that.fromDataNode)
        && toDataNode.equals(that.toDataNode);
  }

  @Override
  public int hashCode() {
    return Objects.hash(regionId, fromDataNode, toDataNode);
  }

  @Override
  public String toString() {
    return "RegionMigrationPlan{"
        + "regionId="
        + regionId
        + ", fromDataNode="
        + fromDataNode
        + ", toDataNode="
        + toDataNode
        + '}';
  }
}
