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
import org.apache.iotdb.commons.queryengine.utils.DateTimeUtils;
import org.apache.iotdb.commons.utils.CommonDateTimeUtils;
import org.apache.iotdb.commons.utils.KillPoint.RegionMaintainKillPoints;
import org.apache.iotdb.commons.utils.ThriftCommonsSerDeUtils;
import org.apache.iotdb.confignode.i18n.ProcedureMessages;
import org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv;
import org.apache.iotdb.confignode.procedure.env.RegionMaintainHandler;
import org.apache.iotdb.confignode.procedure.exception.ProcedureException;
import org.apache.iotdb.confignode.procedure.state.AddRegionPeerState;
import org.apache.iotdb.confignode.procedure.store.ProcedureType;
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
              ProcedureMessages.PID_ADDREGION_STARTED_WILL_BE_ADDED_TO_DATANODE,
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
          setNextState(AddRegionPeerState.CREATE_CONSENSUS_PIPES);
          break;
        case CREATE_CONSENSUS_PIPES:
          handler.createConsensusPipesForAddPeer(regionId, targetDataNode);
          setKillPoint(state);
          setNextState(AddRegionPeerState.DO_ADD_REGION_PEER);
          break;
        case DO_ADD_REGION_PEER:
          handler.forceUpdateRegionCache(regionId, targetDataNode, RegionStatus.Adding);
          // Only submit the AddRegionPeerTask on the very first entry of this state. We must NOT
          // re-submit when:
          //   - the state was restored from disk after a leader change / ConfigNode reboot
          //     (isStateDeserialized()), or
          //   - this state is being re-entered in place because a previous attempt parked here on
          //     PROCESSING (getCycles() > 0, see the PROCESSING branch below).
          // The coordinator DataNode also dedups by taskId, so a duplicate submit would be a no-op,
          // but skipping it here avoids the useless RPC and keeps the re-poll cheap.
          if (!this.isStateDeserialized() && getCycles() == 0) {
            TSStatus tsStatus =
                handler.submitAddRegionPeerTask(
                    this.getProcId(), targetDataNode, regionId, coordinator);
            setKillPoint(state);
            if (tsStatus.getCode() != SUCCESS_STATUS.getStatusCode()) {
              return warnAndRollBackAndNoMoreState(
                  env, handler, "submit DO_ADD_REGION_PEER task fail");
            }
          }
          TRegionMigrateResult result =
              handler.waitTaskFinish(
                  this.getProcId(), coordinator, RegionMaintainKillPoints.WAIT_TASK_FINISH_POLLING);
          switch (result.getTaskStatus()) {
            case TASK_NOT_EXIST:
            // coordinator crashed and lost its task table
            case FAIL:
              // maybe some DataNode crash
              return warnAndRollBackAndNoMoreState(
                  env, handler, String.format("%s result is %s", state, result.getTaskStatus()));
            case PROCESSING:
              // waitTaskFinish() only returns PROCESSING when its polling loop was interrupted by
              // an InterruptedException, i.e. this ConfigNode is shutting down / losing leadership
              // (a user CANCEL or a coordinator disconnection both go through the FAIL branch
              // above). The AddRegionPeerTask is still running on the coordinator DataNode, so we
              // must NOT silently end here: doing so would let the parent RegionMigrateProcedure
              // proceed to CHECK_ADD_REGION_PEER / REMOVE_REGION_PEER and remove the source replica
              // before the destination replica has actually finished receiving the snapshot.
              // Instead, stay in DO_ADD_REGION_PEER and persist it; after recovery the new leader
              // re-enters this state and re-polls the still-running coordinator task (the
              // isStateDeserialized() guard above prevents re-submitting the task) until it really
              // reaches SUCCESS or FAIL.
              LOGGER.info(
                  ProcedureMessages
                      .WAITTASKFINISH_RETURNS_PROCESSING_WHICH_MEANS_THE_WAITING_HAS_BEEN_INTERRUPTED);
              setNextState(AddRegionPeerState.DO_ADD_REGION_PEER);
              break outerSwitch;
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
          LOGGER.info(ProcedureMessages.PID_ADDREGION_STATE_COMPLETE, getProcId(), state);
          LOGGER.info(
              ProcedureMessages.PID_ADDREGION_SUCCESS_HAS_BEEN_ADDED_TO_DATANODE_PROCEDURE_TOOK,
              getProcId(),
              regionId,
              simplifiedLocation(targetDataNode),
              CommonDateTimeUtils.convertMillisecondToDurationStr(
                  System.currentTimeMillis() - getSubmittedTime()),
              DateTimeUtils.convertLongToDate(getSubmittedTime(), "ms"));
          return Flow.NO_MORE_STATE;
        default:
          throw new ProcedureException(ProcedureMessages.UNSUPPORTED_STATE + state.name());
      }
    } catch (Exception e) {
      LOGGER.error(ProcedureMessages.PID_ADDREGION_STATE_FAILED, getProcId(), state, e);
      return Flow.NO_MORE_STATE;
    }
    LOGGER.info(ProcedureMessages.PID_ADDREGION_STATE_COMPLETE, getProcId(), state);
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
      LOGGER.warn(
          ProcedureMessages.PID_ADDREGION_START_TO_ROLL_BACK_BECAUSE, getProcId(), reason, e);
    } else {
      LOGGER.warn(ProcedureMessages.PID_ADDREGION_START_TO_ROLL_BACK_BECAUSE, getProcId(), reason);
    }
    handler.removeRegionLocation(regionId, targetDataNode);

    List<TDataNodeLocation> correctDataNodeLocations =
        env.getConfigManager().getPartitionManager().getAllReplicaSets().stream()
            .filter(tRegionReplicaSet -> tRegionReplicaSet.getRegionId().equals(regionId))
            .findAny()
            .orElseThrow(
                () ->
                    new ProcedureException(
                        ProcedureMessages
                            .PID_ADDREGION_CANNOT_ROLL_BACK_BECAUSE_CANNOT_FIND_THE_CORRECT))
            .getDataNodeLocations();
    if (correctDataNodeLocations.remove(targetDataNode)) {
      LOGGER.warn(
          ProcedureMessages.PID_ADDREGION_IT_APPEARS_THAT_CONSENSUS_WRITE_HAS_NOT_MODIFIED
              + ProcedureMessages
                  .LOG_PLEASE_VERIFY_WHETHER_LEADER_CHANGE_HAS_OCCURRED_DURING_STAGE_9FE68EE3
              + ProcedureMessages
                  .LOG_IF_LOG_TRIGGERED_WITHOUT_LEADER_CHANGE_IT_INDICATES_POTENTIAL_BUG_32AE71FD,
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
        ProcedureMessages.PID_ADDREGION_RESET_PEER_LIST_PEER_LIST_OF_CONSENSUS_GROUP_3,
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
                ProcedureMessages.PID_ADDREGION_RESET_PEER_LIST_PEER_LIST_OF_CONSENSUS_GROUP_2,
                getProcId(),
                regionId,
                dataNodeId,
                correctStr);
          } else {
            // TODO: more precise
            LOGGER.warn(
                ProcedureMessages.PID_ADDREGION_RESET_PEER_LIST_PEER_LIST_OF_CONSENSUS_GROUP,
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
      LOGGER.error(ProcedureMessages.ERROR_IN_DESERIALIZE, this.getClass(), e);
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
