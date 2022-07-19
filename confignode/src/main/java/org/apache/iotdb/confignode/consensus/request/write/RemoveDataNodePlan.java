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
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.commons.enums.DataNodeRemoveState;
import org.apache.iotdb.commons.enums.RegionMigrateState;
import org.apache.iotdb.commons.utils.ThriftCommonsSerDeUtils;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlan;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlanType;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class RemoveDataNodePlan extends ConfigPhysicalPlan {
  private List<TDataNodeLocation> dataNodeLocations = new ArrayList<>();

  // if true, means update the  request exec  state.
  private boolean isUpdate;

  // [0, dataNodeLocations.size), -1 means No Data Node exec remove action
  private int execDataNodeIndex = -1;

  private DataNodeRemoveState execDataNodeState = DataNodeRemoveState.NORMAL;

  private List<TConsensusGroupId> execDataNodeRegionIds = new ArrayList<>();

  // [0, execDataNodeRegionIds), -1 means No Region exec migrate action
  private int execRegionIndex = -1;

  // if the request finished
  private boolean finished = false;

  private RegionMigrateState execRegionState = RegionMigrateState.ONLINE;

  public RemoveDataNodePlan() {
    super(ConfigPhysicalPlanType.RemoveDataNode);
  }

  public RemoveDataNodePlan(List<TDataNodeLocation> dataNodeLocations) {
    this();
    this.dataNodeLocations = dataNodeLocations;
    isUpdate = false;
  }

  @Override
  protected void serializeImpl(DataOutputStream stream) throws IOException {
    stream.writeInt(ConfigPhysicalPlanType.RemoveDataNode.ordinal());
    stream.writeInt(dataNodeLocations.size());
    dataNodeLocations.forEach(
        location -> ThriftCommonsSerDeUtils.serializeTDataNodeLocation(location, stream));
    stream.writeInt(isUpdate ? 1 : 0);
    if (isUpdate) {
      stream.writeInt(execDataNodeIndex);
      stream.writeInt(execDataNodeState.getCode());
      stream.writeInt(execDataNodeRegionIds.size());
      execDataNodeRegionIds.forEach(
          tid -> ThriftCommonsSerDeUtils.serializeTConsensusGroupId(tid, stream));
      stream.writeInt(execRegionIndex);
      stream.writeInt(execRegionState.getCode());
      stream.writeInt(finished ? 1 : 0);
    }
  }

  @Override
  protected void deserializeImpl(ByteBuffer buffer) throws IOException {
    int dataNodeLocationSize = buffer.getInt();
    for (int i = 0; i < dataNodeLocationSize; i++) {
      dataNodeLocations.add(ThriftCommonsSerDeUtils.deserializeTDataNodeLocation(buffer));
    }
    isUpdate = buffer.getInt() == 1;
    if (isUpdate) {
      execDataNodeIndex = buffer.getInt();
      execDataNodeState = DataNodeRemoveState.getStateByCode(buffer.getInt());
      int regionSize = buffer.getInt();
      execDataNodeRegionIds = new ArrayList<>(regionSize);
      for (int i = 0; i < regionSize; i++) {
        execDataNodeRegionIds.add(ThriftCommonsSerDeUtils.deserializeTConsensusGroupId(buffer));
      }
      execRegionIndex = buffer.getInt();
      execRegionState = RegionMigrateState.getStateByCode(buffer.getInt());
      finished = buffer.getInt() == 1;
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

    RemoveDataNodePlan other = (RemoveDataNodePlan) o;
    if (getDataNodeLocationSize() != other.getDataNodeLocationSize()) {
      return false;
    }

    return dataNodeLocations.containsAll(other.getDataNodeLocations());
  }

  @Override
  public int hashCode() {
    return Objects.hash(dataNodeLocations);
  }

  public List<TDataNodeLocation> getDataNodeLocations() {
    return dataNodeLocations;
  }

  public int getDataNodeLocationSize() {
    return dataNodeLocations.size();
  }

  @Override
  public String toString() {
    return "{RemoveDataNodeReq: "
        + "TDataNodeLocations: "
        + this.getDataNodeLocations()
        + ", is update: "
        + this.isUpdate()
        + ", is finished: "
        + this.isFinished()
        + ", exec node index: "
        + this.getExecDataNodeIndex()
        + ", exec node state: "
        + this.getExecDataNodeState()
        + ", exec region index: "
        + this.getExecRegionIndex()
        + ", exec region state: "
        + this.getExecRegionState()
        + ", exec node region ids: "
        + this.getExecDataNodeRegionIds().stream()
            .map(TConsensusGroupId::getId)
            .collect(Collectors.toList())
        + "}";
  }

  public boolean isUpdate() {
    return isUpdate;
  }

  public void setUpdate(boolean update) {
    isUpdate = update;
  }

  public int getExecDataNodeIndex() {
    return execDataNodeIndex;
  }

  public void setExecDataNodeIndex(int execDataNodeIndex) {
    this.execDataNodeIndex = execDataNodeIndex;
  }

  public DataNodeRemoveState getExecDataNodeState() {
    return execDataNodeState;
  }

  public void setExecDataNodeState(DataNodeRemoveState execDataNodeState) {
    this.execDataNodeState = execDataNodeState;
  }

  public List<TConsensusGroupId> getExecDataNodeRegionIds() {
    return execDataNodeRegionIds;
  }

  public void setExecDataNodeRegionIds(List<TConsensusGroupId> execDataNodeRegionIds) {
    this.execDataNodeRegionIds = execDataNodeRegionIds;
  }

  public int getExecRegionIndex() {
    return execRegionIndex;
  }

  public void setExecRegionIndex(int execRegionIndex) {
    this.execRegionIndex = execRegionIndex;
  }

  public RegionMigrateState getExecRegionState() {
    return execRegionState;
  }

  public void setExecRegionState(RegionMigrateState execRegionState) {
    this.execRegionState = execRegionState;
  }

  public void setFinished(boolean finished) {
    this.finished = finished;
  }

  public boolean isFinished() {
    return this.finished;
  }
}
