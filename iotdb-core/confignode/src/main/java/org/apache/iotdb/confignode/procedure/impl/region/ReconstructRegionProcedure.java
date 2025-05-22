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
import org.apache.iotdb.commons.exception.runtime.ThriftSerDeException;
import org.apache.iotdb.commons.utils.CommonDateTimeUtils;
import org.apache.iotdb.commons.utils.ThriftCommonsSerDeUtils;
import org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv;
import org.apache.iotdb.confignode.procedure.exception.ProcedureException;
import org.apache.iotdb.confignode.procedure.state.ReconstructRegionState;
import org.apache.iotdb.confignode.procedure.store.ProcedureType;
import org.apache.iotdb.db.utils.DateTimeUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public class ReconstructRegionProcedure extends RegionOperationProcedure<ReconstructRegionState> {
  private static final Logger LOGGER = LoggerFactory.getLogger(ReconstructRegionProcedure.class);

  private TDataNodeLocation targetDataNode;
  private TDataNodeLocation coordinator;

  public ReconstructRegionProcedure() {}
  ;

  public ReconstructRegionProcedure(
      TConsensusGroupId regionId, TDataNodeLocation targetDataNode, TDataNodeLocation coordinator) {
    super(regionId);
    this.targetDataNode = targetDataNode;
    this.coordinator = coordinator;
  }

  @Override
  protected Flow executeFromState(ConfigNodeProcedureEnv env, ReconstructRegionState state)
      throws InterruptedException {
    try {
      switch (state) {
        case RECONSTRUCT_REGION_PREPARE:
          LOGGER.info(
              "[pid{}][ReconstructRegion] started, region {} on DataNode {}({}) will be reconstructed.",
              getProcId(),
              regionId.getId(),
              targetDataNode.getDataNodeId(),
              targetDataNode.getInternalEndPoint());
          setNextState(ReconstructRegionState.REMOVE_REGION_PEER);
          break;
        case REMOVE_REGION_PEER:
          addChildProcedure(new RemoveRegionPeerProcedure(regionId, coordinator, targetDataNode));
          setNextState(ReconstructRegionState.CHECK_REMOVE_REGION_PEER);
          break;
        case CHECK_REMOVE_REGION_PEER:
          if (env.getConfigManager()
              .getPartitionManager()
              .isDataNodeContainsRegion(targetDataNode.getDataNodeId(), regionId)) {
            LOGGER.warn(
                "[pid{}][ReconstructRegion] sub-procedure RemoveRegionPeerProcedure failed, ReconstructRegionProcedure will not continue",
                getProcId());
            return Flow.NO_MORE_STATE;
          }
          setNextState(ReconstructRegionState.ADD_REGION_PEER);
          break;
        case ADD_REGION_PEER:
          addChildProcedure(new AddRegionPeerProcedure(regionId, coordinator, targetDataNode));
          setNextState(ReconstructRegionState.CHECK_ADD_REGION_PEER);
          break;
        case CHECK_ADD_REGION_PEER:
          if (!env.getConfigManager()
              .getPartitionManager()
              .isDataNodeContainsRegion(targetDataNode.getDataNodeId(), regionId)) {
            LOGGER.warn(
                "[pid{}][ReconstructRegion] failed, but the region {} has been removed from DataNode {}. Use 'extend region' to fix this.",
                getProcId(),
                regionId.getId(),
                targetDataNode.getDataNodeId());
          } else {
            LOGGER.info(
                "[pid{}][ReconstructRegion] success, region {} has been reconstructed on DataNode {}. Procedure took {} (started at {})",
                getProcId(),
                regionId.getId(),
                targetDataNode.getDataNodeId(),
                CommonDateTimeUtils.convertMillisecondToDurationStr(
                    System.currentTimeMillis() - getSubmittedTime()),
                DateTimeUtils.convertLongToDate(getSubmittedTime(), "ms"));
          }
          return Flow.NO_MORE_STATE;
        default:
          throw new ProcedureException("Unsupported state: " + state.name());
      }
    } catch (Exception e) {
      LOGGER.error("[pid{}][ReconstructRegion] state {} fail", getProcId(), state, e);
      return Flow.NO_MORE_STATE;
    }
    LOGGER.info("[pid{}][ReconstructRegion] state {} complete", getProcId(), state);
    return Flow.HAS_MORE_STATE;
  }

  @Override
  protected void rollbackState(
      ConfigNodeProcedureEnv configNodeProcedureEnv, ReconstructRegionState reconstructRegionState)
      throws IOException, InterruptedException, ProcedureException {}

  @Override
  public void serialize(DataOutputStream stream) throws IOException {
    stream.writeShort(ProcedureType.RECONSTRUCT_REGION_PROCEDURE.getTypeCode());
    super.serialize(stream);
    ThriftCommonsSerDeUtils.serializeTConsensusGroupId(regionId, stream);
    ThriftCommonsSerDeUtils.serializeTDataNodeLocation(targetDataNode, stream);
    ThriftCommonsSerDeUtils.serializeTDataNodeLocation(coordinator, stream);
  }

  @Override
  public void deserialize(ByteBuffer byteBuffer) {
    super.deserialize(byteBuffer);
    try {
      regionId = ThriftCommonsSerDeUtils.deserializeTConsensusGroupId(byteBuffer);
      targetDataNode = ThriftCommonsSerDeUtils.deserializeTDataNodeLocation(byteBuffer);
      coordinator = ThriftCommonsSerDeUtils.deserializeTDataNodeLocation(byteBuffer);
    } catch (ThriftSerDeException e) {
      LOGGER.warn(
          "Error in deserialize {} (procID {}). This procedure will be ignored. It may belong to old version and cannot be used now.",
          this.getClass(),
          this.getProcId(),
          e);
      throw e;
    }
  }

  @Override
  protected ReconstructRegionState getState(int stateId) {
    return ReconstructRegionState.values()[stateId];
  }

  @Override
  protected int getStateId(ReconstructRegionState reconstructRegionState) {
    return reconstructRegionState.ordinal();
  }

  @Override
  protected ReconstructRegionState getInitialState() {
    return ReconstructRegionState.RECONSTRUCT_REGION_PREPARE;
  }

  @Override
  public String toString() {
    return super.toString()
        + ", targetDataNode="
        + targetDataNode.getDataNodeId()
        + ", coordinator="
        + coordinator.getDataNodeId();
  }
}
