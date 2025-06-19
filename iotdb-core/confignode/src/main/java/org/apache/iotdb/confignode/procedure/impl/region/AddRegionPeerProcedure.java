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
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.cluster.RegionStatus;
import org.apache.iotdb.commons.exception.runtime.ThriftSerDeException;
import org.apache.iotdb.commons.utils.CommonDateTimeUtils;
import org.apache.iotdb.commons.utils.ThriftCommonsSerDeUtils;
import org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv;
import org.apache.iotdb.confignode.procedure.env.RegionMaintainHandler;
import org.apache.iotdb.confignode.procedure.exception.ProcedureException;
import org.apache.iotdb.confignode.procedure.state.AddRegionPeerState;
import org.apache.iotdb.confignode.procedure.store.ProcedureType;
import org.apache.iotdb.db.utils.DateTimeUtils;
import org.apache.iotdb.mpp.rpc.thrift.TRegionMigrateResult;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.apache.iotdb.commons.utils.KillPoint.KillPoint.setKillPoint;
import static org.apache.iotdb.confignode.procedure.env.RegionMaintainHandler.simplifiedLocation;
import static org.apache.iotdb.confignode.procedure.state.AddRegionPeerState.UPDATE_REGION_LOCATION_CACHE;
import static org.apache.iotdb.rpc.TSStatusCode.SUCCESS_STATUS;

public class AddRegionPeerProcedure extends RegionOperationProcedure<AddRegionPeerState> {
  private static final Logger LOGGER = LoggerFactory.getLogger(AddRegionPeerProcedure.class);

  private TDataNodeLocation coordinator;

  private TDataNodeLocation targetDataNode;

  public AddRegionPeerProcedure() {
    super();
  }

  public AddRegionPeerProcedure(
      TConsensusGroupId consensusGroupId,
      TDataNodeLocation coordinator,
      TDataNodeLocation targetDataNode) {
    super(consensusGroupId);
    this.coordinator = coordinator;
    this.targetDataNode = targetDataNode;
  }

  @Override
  protected Flow executeFromState(ConfigNodeProcedureEnv env, AddRegionPeerState state)
      throws InterruptedException {
    if (regionId == null) {
      return Flow.NO_MORE_STATE;
    }
    RegionMaintainHandler handler = env.getRegionMaintainHandler();
    try {
      outerSwitch:
      switch (state) {
        case CREATE_NEW_REGION_PEER:
          LOGGER.info(
              "[pid{}][AddRegion] started, {} will be added to DataNode {}.",
              getProcId(),
              regionId,
              simplifiedLocation(targetDataNode));
          handler.addRegionLocation(regionId, targetDataNode);
          handler.forceUpdateRegionCache(regionId, targetDataNode, RegionStatus.Adding);
          TSStatus status = handler.createNewRegionPeer(regionId, targetDataNode);
          setKillPoint(state);
          if (status.getCode() != SUCCESS_STATUS.getStatusCode()) {
            return warnAndRollBackAndNoMoreState(env, handler, "CREATE_NEW_REGION_PEER fail");
          }
          setNextState(AddRegionPeerState.DO_ADD_REGION_PEER);
          break;
        case DO_ADD_REGION_PEER:
          handler.forceUpdateRegionCache(regionId, targetDataNode, RegionStatus.Adding);
          // We don't want to re-submit AddRegionPeerTask when leader change or ConfigNode reboot
          if (!this.isStateDeserialized()) {
            TSStatus tsStatus =
                handler.submitAddRegionPeerTask(
                    this.getProcId(), targetDataNode, regionId, coordinator);
            setKillPoint(state);
            if (tsStatus.getCode() != SUCCESS_STATUS.getStatusCode()) {
              return warnAndRollBackAndNoMoreState(
                  env, handler, "submit DO_ADD_REGION_PEER task fail");
            }
          }
          TRegionMigrateResult result = handler.waitTaskFinish(this.getProcId(), coordinator);
          switch (result.getTaskStatus()) {
            case TASK_NOT_EXIST:
              // coordinator crashed and lost its task table
            case FAIL:
              // maybe some DataNode crash
              return warnAndRollBackAndNoMoreState(
                  env, handler, String.format("%s result is %s", state, result.getTaskStatus()));
            case PROCESSING:
              LOGGER.info(
                  "waitTaskFinish() returns PROCESSING, which means the waiting has been interrupted, this procedure will end without rollback");
              return Flow.NO_MORE_STATE;
            case SUCCESS:
              setNextState(UPDATE_REGION_LOCATION_CACHE);
              break outerSwitch;
            default:
              return warnAndRollBackAndNoMoreState(
                  env, handler, String.format("status %s is unsupported", result.getTaskStatus()));
          }
        case UPDATE_REGION_LOCATION_CACHE:
          handler.forceUpdateRegionCache(regionId, targetDataNode, RegionStatus.Running);
          setKillPoint(state);
          LOGGER.info("[pid{}][AddRegion] state {} complete", getProcId(), state);
          LOGGER.info(
              "[pid{}][AddRegion] success, {} has been added to DataNode {}. Procedure took {} (start at {}).",
              getProcId(),
              regionId,
              simplifiedLocation(targetDataNode),
              CommonDateTimeUtils.convertMillisecondToDurationStr(
                  System.currentTimeMillis() - getSubmittedTime()),
              DateTimeUtils.convertLongToDate(getSubmittedTime(), "ms"));
          return Flow.NO_MORE_STATE;
        default:
          throw new ProcedureException("Unsupported state: " + state.name());
      }
    } catch (Exception e) {
      LOGGER.error("[pid{}][AddRegion] state {} failed", getProcId(), state, e);
      return Flow.NO_MORE_STATE;
    }
    LOGGER.info("[pid{}][AddRegion] state {} complete", getProcId(), state);
    return Flow.HAS_MORE_STATE;
  }

  private Flow warnAndRollBackAndNoMoreState(
      ConfigNodeProcedureEnv env, RegionMaintainHandler handler, String reason)
      throws ProcedureException {
    return warnAndRollBackAndNoMoreState(env, handler, reason, null);
  }

  private Flow warnAndRollBackAndNoMoreState(
      ConfigNodeProcedureEnv env, RegionMaintainHandler handler, String reason, Exception e)
      throws ProcedureException {
    if (e != null) {
      LOGGER.warn("[pid{}][AddRegion] Start to roll back, because: {}", getProcId(), reason, e);
    } else {
      LOGGER.warn("[pid{}][AddRegion] Start to roll back, because: {}", getProcId(), reason);
    }
    handler.removeRegionLocation(regionId, targetDataNode);

    List<TDataNodeLocation> correctDataNodeLocations =
        env.getConfigManager().getPartitionManager().getAllReplicaSets().stream()
            .filter(tRegionReplicaSet -> tRegionReplicaSet.getRegionId().equals(regionId))
            .findAny()
            .orElseThrow(
                () ->
                    new ProcedureException(
                        "[pid{}][AddRegion] Cannot roll back, because cannot find the correct locations"))
            .getDataNodeLocations();
    if (correctDataNodeLocations.remove(targetDataNode)) {
      LOGGER.warn(
          "[pid{}][AddRegion] It appears that consensus write has not modified the local partition table. "
              + "Please verify whether a leader change has occurred during this stage. "
              + "If this log is triggered without a leader change, it indicates a potential bug in the partition table.",
          getProcId());
    }
    String correctStr =
        correctDataNodeLocations.stream()
            .map(TDataNodeLocation::getDataNodeId)
            .collect(Collectors.toList())
            .toString();
    List<TDataNodeLocation> relatedDataNodeLocations = new ArrayList<>(correctDataNodeLocations);
    relatedDataNodeLocations.add(targetDataNode);
    Map<Integer, TDataNodeLocation> relatedDataNodeLocationMap =
        relatedDataNodeLocations.stream()
            .collect(
                Collectors.toMap(
                    TDataNodeLocation::getDataNodeId, dataNodeLocation -> dataNodeLocation));
    LOGGER.info(
        "[pid{}][AddRegion] reset peer list: peer list of consensus group {} on DataNode {} will be reset to {}",
        getProcId(),
        regionId,
        relatedDataNodeLocationMap.values().stream()
            .map(TDataNodeLocation::getDataNodeId)
            .collect(Collectors.toList()),
        correctStr);

    Map<Integer, TSStatus> resultMap =
        handler.resetPeerList(regionId, correctDataNodeLocations, relatedDataNodeLocationMap);

    resultMap.forEach(
        (dataNodeId, resetResult) -> {
          if (resetResult.getCode() == SUCCESS_STATUS.getStatusCode()) {
            LOGGER.info(
                "[pid{}][AddRegion] reset peer list: peer list of consensus group {} on DataNode {} has been successfully reset to {}",
                getProcId(),
                regionId,
                dataNodeId,
                correctStr);
          } else {
            // TODO: more precise
            LOGGER.warn(
                "[pid{}][AddRegion] reset peer list: peer list of consensus group {} on DataNode {} failed to reset to {}, you may manually reset it",
                getProcId(),
                regionId,
                dataNodeId,
                correctStr);
          }
        });
    return Flow.NO_MORE_STATE;
  }

  @Override
  protected void rollbackState(
      ConfigNodeProcedureEnv configNodeProcedureEnv, AddRegionPeerState addRegionPeerState)
      throws IOException, InterruptedException, ProcedureException {}

  @Override
  protected AddRegionPeerState getState(int stateId) {
    return AddRegionPeerState.values()[stateId];
  }

  @Override
  protected int getStateId(AddRegionPeerState addRegionPeerState) {
    return addRegionPeerState.ordinal();
  }

  @Override
  protected AddRegionPeerState getInitialState() {
    return AddRegionPeerState.CREATE_NEW_REGION_PEER;
  }

  @Override
  public void serialize(DataOutputStream stream) throws IOException {
    stream.writeShort(ProcedureType.ADD_REGION_PEER_PROCEDURE.getTypeCode());
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
      LOGGER.error("Error in deserialize {}", this.getClass(), e);
    }
  }

  public TDataNodeLocation getCoordinator() {
    return coordinator;
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof AddRegionPeerProcedure)) {
      return false;
    }
    AddRegionPeerProcedure procedure = (AddRegionPeerProcedure) obj;
    return this.regionId.equals(procedure.regionId)
        && this.targetDataNode.equals(procedure.targetDataNode)
        && this.coordinator.equals(procedure.coordinator);
  }

  @Override
  public int hashCode() {
    return Objects.hash(regionId, targetDataNode, coordinator);
  }

  @Override
  public String toString() {
    return "AddRegionPeerProcedure{"
        + "regionId="
        + regionId
        + ", coordinator="
        + simplifiedLocation(coordinator)
        + ", targetDataNode="
        + simplifiedLocation(targetDataNode)
        + '}';
  }
}
