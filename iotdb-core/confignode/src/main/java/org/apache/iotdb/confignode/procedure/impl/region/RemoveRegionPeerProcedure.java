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
import org.apache.iotdb.common.rpc.thrift.TRegionMaintainTaskStatus;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.cluster.RegionStatus;
import org.apache.iotdb.commons.exception.runtime.ThriftSerDeException;
import org.apache.iotdb.commons.utils.CommonDateTimeUtils;
import org.apache.iotdb.commons.utils.ThriftCommonsSerDeUtils;
import org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv;
import org.apache.iotdb.confignode.procedure.env.RegionMaintainHandler;
import org.apache.iotdb.confignode.procedure.exception.ProcedureException;
import org.apache.iotdb.confignode.procedure.exception.ProcedureSuspendedException;
import org.apache.iotdb.confignode.procedure.exception.ProcedureYieldException;
import org.apache.iotdb.confignode.procedure.impl.StateMachineProcedure;
import org.apache.iotdb.confignode.procedure.state.RemoveRegionPeerState;
import org.apache.iotdb.confignode.procedure.store.ProcedureType;
import org.apache.iotdb.db.utils.DateTimeUtils;
import org.apache.iotdb.mpp.rpc.thrift.TRegionMigrateResult;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;

import static org.apache.iotdb.commons.utils.KillPoint.KillPoint.setKillPoint;
import static org.apache.iotdb.confignode.procedure.state.RemoveRegionPeerState.DELETE_OLD_REGION_PEER;
import static org.apache.iotdb.confignode.procedure.state.RemoveRegionPeerState.REMOVE_REGION_LOCATION_CACHE;
import static org.apache.iotdb.confignode.procedure.state.RemoveRegionPeerState.REMOVE_REGION_PEER;
import static org.apache.iotdb.rpc.TSStatusCode.SUCCESS_STATUS;

public class RemoveRegionPeerProcedure
    extends StateMachineProcedure<ConfigNodeProcedureEnv, RemoveRegionPeerState> {
  private static final Logger LOGGER = LoggerFactory.getLogger(RemoveRegionPeerProcedure.class);
  private TConsensusGroupId consensusGroupId;
  private TDataNodeLocation coordinator;
  private TDataNodeLocation targetDataNode;

  public RemoveRegionPeerProcedure() {
    super();
  }

  public RemoveRegionPeerProcedure(
      TConsensusGroupId consensusGroupId,
      TDataNodeLocation coordinator,
      TDataNodeLocation targetDataNode) {
    this.consensusGroupId = consensusGroupId;
    this.coordinator = coordinator;
    this.targetDataNode = targetDataNode;
  }

  private void handleTransferLeader(RegionMaintainHandler handler)
      throws ProcedureException, InterruptedException {
    LOGGER.info(
        "[pid{}][RemoveRegion] started, region {} will be removed from DataNode {}.",
        getProcId(),
        consensusGroupId.getId(),
        targetDataNode.getDataNodeId());
    handler.forceUpdateRegionCache(consensusGroupId, targetDataNode, RegionStatus.Removing);
    handler.transferRegionLeader(consensusGroupId, targetDataNode, coordinator);
  }

  @Override
  protected Flow executeFromState(ConfigNodeProcedureEnv env, RemoveRegionPeerState state)
      throws ProcedureSuspendedException, ProcedureYieldException, InterruptedException {
    if (consensusGroupId == null) {
      return Flow.NO_MORE_STATE;
    }
    TSStatus tsStatus;
    RegionMaintainHandler handler = env.getRegionMaintainHandler();
    try {
      switch (state) {
        case TRANSFER_REGION_LEADER:
          handleTransferLeader(handler);
          setKillPoint(state);
          setNextState(REMOVE_REGION_PEER);
          break;
        case REMOVE_REGION_PEER:
          handler.forceUpdateRegionCache(consensusGroupId, targetDataNode, RegionStatus.Removing);
          tsStatus =
              handler.submitRemoveRegionPeerTask(
                  this.getProcId(), targetDataNode, consensusGroupId, coordinator);
          setKillPoint(state);
          if (tsStatus.getCode() != SUCCESS_STATUS.getStatusCode()) {
            LOGGER.warn(
                "[pid{}][RemoveRegion] {} task submitted failed, procedure will continue. You should manually clear peer list.",
                getProcId(),
                state);
            setNextState(DELETE_OLD_REGION_PEER);
            return Flow.HAS_MORE_STATE;
          }
          TRegionMigrateResult removeRegionPeerResult =
              handler.waitTaskFinish(this.getProcId(), coordinator);
          if (removeRegionPeerResult.getTaskStatus() != TRegionMaintainTaskStatus.SUCCESS) {
            LOGGER.warn(
                "[pid{}][RemoveRegion] {} executed failed, procedure will continue. You should manually clear peer list.",
                getProcId(),
                state);
            setNextState(DELETE_OLD_REGION_PEER);
            return Flow.HAS_MORE_STATE;
          }
          setNextState(DELETE_OLD_REGION_PEER);
          break;
        case DELETE_OLD_REGION_PEER:
          handler.forceUpdateRegionCache(consensusGroupId, targetDataNode, RegionStatus.Removing);
          tsStatus =
              handler.submitDeleteOldRegionPeerTask(
                  this.getProcId(), targetDataNode, consensusGroupId);
          setKillPoint(state);
          if (tsStatus.getCode() != SUCCESS_STATUS.getStatusCode()) {
            LOGGER.warn(
                "[pid{}][RemoveRegion] DELETE_OLD_REGION_PEER task submitted failed, procedure will continue. You should manually delete region file.",
                getProcId());
            setNextState(REMOVE_REGION_LOCATION_CACHE);
            return Flow.HAS_MORE_STATE;
          }
          TRegionMigrateResult deleteOldRegionPeerResult =
              handler.waitTaskFinish(this.getProcId(), targetDataNode);
          if (deleteOldRegionPeerResult.getTaskStatus() != TRegionMaintainTaskStatus.SUCCESS) {
            LOGGER.warn(
                "[pid{}][RemoveRegion] DELETE_OLD_REGION_PEER executed failed, procedure will continue. You should manually delete region file.",
                getProcId());
            setNextState(REMOVE_REGION_LOCATION_CACHE);
            return Flow.HAS_MORE_STATE;
          }
          setNextState(REMOVE_REGION_LOCATION_CACHE);
          break;
        case REMOVE_REGION_LOCATION_CACHE:
          handler.removeRegionLocation(consensusGroupId, targetDataNode);
          setKillPoint(state);
          LOGGER.info("RemoveRegionPeer state {} success", state);
          LOGGER.info(
              "[pid{}][RemoveRegion] success, region {} has been removed from DataNode {}. Procedure took {} (started at {})",
              getProcId(),
              consensusGroupId.getId(),
              targetDataNode.getDataNodeId(),
              CommonDateTimeUtils.convertMillisecondToDurationStr(
                  System.currentTimeMillis() - getSubmittedTime()),
              DateTimeUtils.convertLongToDate(getSubmittedTime(), "ms"));
          return Flow.NO_MORE_STATE;
        default:
          throw new ProcedureException("Unsupported state: " + state.name());
      }
    } catch (Exception e) {
      LOGGER.error("RemoveRegionPeer state {} failed", state, e);
      return Flow.NO_MORE_STATE;
    }
    LOGGER.info("[pid{}][RemoveRegion] state {} success", getProcId(), state);
    return Flow.HAS_MORE_STATE;
  }

  @Override
  protected void rollbackState(ConfigNodeProcedureEnv env, RemoveRegionPeerState state)
      throws IOException, InterruptedException, ProcedureException {}

  @Override
  protected RemoveRegionPeerState getState(int stateId) {
    return RemoveRegionPeerState.values()[stateId];
  }

  @Override
  protected int getStateId(RemoveRegionPeerState RemoveRegionPeerState) {
    return RemoveRegionPeerState.ordinal();
  }

  @Override
  protected RemoveRegionPeerState getInitialState() {
    return RemoveRegionPeerState.TRANSFER_REGION_LEADER;
  }

  @Override
  public void serialize(DataOutputStream stream) throws IOException {
    stream.writeShort(ProcedureType.REMOVE_REGION_PEER_PROCEDURE.getTypeCode());
    super.serialize(stream);
    ThriftCommonsSerDeUtils.serializeTConsensusGroupId(consensusGroupId, stream);
    ThriftCommonsSerDeUtils.serializeTDataNodeLocation(targetDataNode, stream);
    ThriftCommonsSerDeUtils.serializeTDataNodeLocation(coordinator, stream);
  }

  @Override
  public void deserialize(ByteBuffer byteBuffer) {
    super.deserialize(byteBuffer);
    try {
      consensusGroupId = ThriftCommonsSerDeUtils.deserializeTConsensusGroupId(byteBuffer);
      targetDataNode = ThriftCommonsSerDeUtils.deserializeTDataNodeLocation(byteBuffer);
      coordinator = ThriftCommonsSerDeUtils.deserializeTDataNodeLocation(byteBuffer);
    } catch (ThriftSerDeException e) {
      LOGGER.error("Error in deserialize {}", this.getClass(), e);
    }
  }

  public TConsensusGroupId getConsensusGroupId() {
    return consensusGroupId;
  }

  public TDataNodeLocation getCoordinator() {
    return coordinator;
  }

  public TDataNodeLocation getTargetDataNode() {
    return targetDataNode;
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof RemoveRegionPeerProcedure)) {
      return false;
    }
    RemoveRegionPeerProcedure procedure = (RemoveRegionPeerProcedure) obj;
    return this.consensusGroupId.equals(procedure.consensusGroupId)
        && this.targetDataNode.equals(procedure.targetDataNode)
        && this.coordinator.equals(procedure.coordinator);
  }

  @Override
  public int hashCode() {
    return Objects.hash(consensusGroupId, targetDataNode, coordinator);
  }
}
