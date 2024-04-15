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
import org.apache.iotdb.commons.exception.runtime.ThriftSerDeException;
import org.apache.iotdb.commons.utils.ThriftCommonsSerDeUtils;
import org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv;
import org.apache.iotdb.confignode.procedure.env.RegionMaintainHandler;
import org.apache.iotdb.confignode.procedure.exception.ProcedureException;
import org.apache.iotdb.confignode.procedure.exception.ProcedureSuspendedException;
import org.apache.iotdb.confignode.procedure.exception.ProcedureYieldException;
import org.apache.iotdb.confignode.procedure.impl.StateMachineProcedure;
import org.apache.iotdb.confignode.procedure.state.AddRegionPeerState;
import org.apache.iotdb.confignode.procedure.store.ProcedureType;
import org.apache.iotdb.mpp.rpc.thrift.TRegionMigrateResult;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.iotdb.commons.utils.KillPoint.KillPoint.setKillPoint;
import static org.apache.iotdb.confignode.procedure.state.AddRegionPeerState.UPDATE_REGION_LOCATION_CACHE;
import static org.apache.iotdb.rpc.TSStatusCode.SUCCESS_STATUS;

public class AddRegionPeerProcedure
    extends StateMachineProcedure<ConfigNodeProcedureEnv, AddRegionPeerState> {
  private static final Logger LOGGER = LoggerFactory.getLogger(AddRegionPeerProcedure.class);
  private TConsensusGroupId consensusGroupId;

  private TDataNodeLocation coordinator;

  private TDataNodeLocation destDataNode;

  public AddRegionPeerProcedure() {
    super();
  }

  public AddRegionPeerProcedure(
      TConsensusGroupId consensusGroupId,
      TDataNodeLocation coordinator,
      TDataNodeLocation destDataNode) {
    super();
    this.consensusGroupId = consensusGroupId;
    this.coordinator = coordinator;
    this.destDataNode = destDataNode;
  }

  @Override
  protected Flow executeFromState(ConfigNodeProcedureEnv env, AddRegionPeerState state)
      throws ProcedureSuspendedException, ProcedureYieldException, InterruptedException {
    if (consensusGroupId == null) {
      return Flow.NO_MORE_STATE;
    }
    RegionMaintainHandler handler = env.getRegionMaintainHandler();
    try {
      outerSwitch:
      switch (state) {
        case CREATE_NEW_REGION_PEER:
          TSStatus status = handler.createNewRegionPeer(consensusGroupId, destDataNode);
          setKillPoint(state);
          if (status.getCode() != SUCCESS_STATUS.getStatusCode()) {
            rollback(env, handler);
          }
          setNextState(AddRegionPeerState.DO_ADD_REGION_PEER);
          break;
        case DO_ADD_REGION_PEER:
          // We don't want to re-submit AddRegionPeerTask when leader change or ConfigNode reboot
          if (!this.isStateDeserialized()) {
            TSStatus tsStatus =
                handler.submitAddRegionPeerTask(
                    this.getProcId(), destDataNode, consensusGroupId, coordinator);
            setKillPoint(state);
            if (tsStatus.getCode() != SUCCESS_STATUS.getStatusCode()) {
              throw new ProcedureException("ADD_REGION_PEER executed failed in DataNode");
            }
          }
          TRegionMigrateResult result = handler.waitTaskFinish(this.getProcId(), coordinator);
          switch (result.getTaskStatus()) {
            case TASK_NOT_EXIST:
              // coordinator crashed and lost its task table
            case FAIL:
              // maybe some DataNode crash
              LOGGER.warn(
                  "{} result is {}, procedure failed. Will try to reset peer list automatically...",
                  state,
                  result.getTaskStatus());
              rollback(env, handler);
              return Flow.NO_MORE_STATE;
            case PROCESSING:
              // should never happen
              LOGGER.error("should never happen");
              throw new UnsupportedOperationException("should never happen");
            case SUCCESS:
              setNextState(UPDATE_REGION_LOCATION_CACHE);
              break outerSwitch;
            default:
              String msg = String.format("status %s is unsupported", result.getTaskStatus());
              LOGGER.error(msg);
              throw new UnsupportedOperationException(msg);
          }
        case UPDATE_REGION_LOCATION_CACHE:
          handler.addRegionLocation(consensusGroupId, destDataNode);
          setKillPoint(state);
          LOGGER.info("AddRegionPeer state {} complete", state);
          LOGGER.info(
              "AddRegionPeerProcedure success, region {} has been added to DataNode {}",
              consensusGroupId.getId(),
              destDataNode.getDataNodeId());
          return Flow.NO_MORE_STATE;
        default:
          throw new ProcedureException("Unsupported state: " + state.name());
      }
    } catch (Exception e) {
      LOGGER.error("AddRegionPeer state {} failed", state, e);
      return Flow.NO_MORE_STATE;
    }
    LOGGER.info("AddRegionPeer state {} complete", state);
    return Flow.HAS_MORE_STATE;
  }

  private void rollback(ConfigNodeProcedureEnv env, RegionMaintainHandler handler) {
    List<TDataNodeLocation> correctDataNodeLocations =
        env.getConfigManager().getPartitionManager().getAllReplicaSets().stream()
            .filter(tRegionReplicaSet -> tRegionReplicaSet.getRegionId().equals(consensusGroupId))
            .findAny()
            .get()
            .getDataNodeLocations();

    String correctStr =
        correctDataNodeLocations.stream()
            .map(TDataNodeLocation::getDataNodeId)
            .collect(Collectors.toList())
            .toString();
    List<TDataNodeLocation> relatedDataNodeLocations = new ArrayList<>(correctDataNodeLocations);
    relatedDataNodeLocations.add(destDataNode);
    Map<Integer, TDataNodeLocation> relatedDataNodeLocationMap = new HashMap<>();
    relatedDataNodeLocations.forEach(
        location -> relatedDataNodeLocationMap.put(location.dataNodeId, location));
    LOGGER.info(
        "Will reset peer list of consensus group {} on DataNode {}",
        consensusGroupId,
        relatedDataNodeLocations.stream()
            .map(TDataNodeLocation::getDataNodeId)
            .collect(Collectors.toList()));

    Map<Integer, TSStatus> resultMap =
        handler.resetPeerList(
            consensusGroupId, correctDataNodeLocations, relatedDataNodeLocationMap);

    resultMap.forEach(
        (dataNodeId, resetResult) -> {
          if (resetResult.getCode() == SUCCESS_STATUS.getStatusCode()) {
            LOGGER.info(
                "reset peer list: peer list of consensus group {} on DataNode {} has been successfully to {}",
                consensusGroupId,
                dataNodeId,
                correctStr);
          } else {
            // TODO: more precise
            LOGGER.warn(
                "reset peer list: peer list of consensus group {} on DataNode {} failed to reset to {}, you may manually reset it",
                consensusGroupId,
                dataNodeId,
                correctStr);
          }
        });
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
    ThriftCommonsSerDeUtils.serializeTConsensusGroupId(consensusGroupId, stream);
    ThriftCommonsSerDeUtils.serializeTDataNodeLocation(destDataNode, stream);
    ThriftCommonsSerDeUtils.serializeTDataNodeLocation(coordinator, stream);
  }

  @Override
  public void deserialize(ByteBuffer byteBuffer) {
    super.deserialize(byteBuffer);
    try {
      consensusGroupId = ThriftCommonsSerDeUtils.deserializeTConsensusGroupId(byteBuffer);
      destDataNode = ThriftCommonsSerDeUtils.deserializeTDataNodeLocation(byteBuffer);
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

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof AddRegionPeerProcedure)) {
      return false;
    }
    AddRegionPeerProcedure procedure = (AddRegionPeerProcedure) obj;
    return this.consensusGroupId.equals(procedure.consensusGroupId)
        && this.destDataNode.equals(procedure.destDataNode)
        && this.coordinator.equals(procedure.coordinator);
  }
}
