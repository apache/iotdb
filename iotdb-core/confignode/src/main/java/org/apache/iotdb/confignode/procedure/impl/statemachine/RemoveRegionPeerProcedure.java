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

package org.apache.iotdb.confignode.procedure.impl.statemachine;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TRegionMaintainTaskStatus;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.exception.runtime.ThriftSerDeException;
import org.apache.iotdb.commons.utils.ThriftCommonsSerDeUtils;
import org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv;
import org.apache.iotdb.confignode.procedure.env.RegionMaintainHandler;
import org.apache.iotdb.confignode.procedure.exception.ProcedureException;
import org.apache.iotdb.confignode.procedure.exception.ProcedureSuspendedException;
import org.apache.iotdb.confignode.procedure.exception.ProcedureYieldException;
import org.apache.iotdb.confignode.procedure.state.RemoveRegionPeerState;
import org.apache.iotdb.confignode.procedure.store.ProcedureType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import static org.apache.iotdb.commons.utils.FileUtils.logBreakpoint;
import static org.apache.iotdb.confignode.procedure.state.RemoveRegionPeerState.DELETE_OLD_REGION_PEER;
import static org.apache.iotdb.confignode.procedure.state.RemoveRegionPeerState.REMOVE_REGION_LOCATION_CACHE;
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
    super();
    this.consensusGroupId = consensusGroupId;
    this.coordinator = coordinator;
    this.targetDataNode = targetDataNode;
  }

  @Override
  protected Flow executeFromState(ConfigNodeProcedureEnv env, RemoveRegionPeerState state)
      throws ProcedureSuspendedException, ProcedureYieldException, InterruptedException {
    if (consensusGroupId == null) {
      return Flow.NO_MORE_STATE;
    }
    TSStatus tsStatus;
    RegionMaintainHandler handler = env.getDataNodeRemoveHandler();
    try {
      switch (state) {
        case REMOVE_REGION_PEER:
          tsStatus =
              handler.removeRegionPeer(
                  targetDataNode, consensusGroupId, coordinator, this.getProcId());
          TRegionMaintainTaskStatus result;
          if (tsStatus.getCode() == SUCCESS_STATUS.getStatusCode()) {
            result = handler.waitTaskFinish(this.getProcId(), coordinator);
          } else {
            throw new ProcedureException("REMOVE_REGION_PEER executed failed in DataNode");
          }
          if (result != TRegionMaintainTaskStatus.SUCCESS) {
            throw new ProcedureException("REMOVE_REGION_PEER executed failed in DataNode");
          }
          setNextState(DELETE_OLD_REGION_PEER);
          break;
        case DELETE_OLD_REGION_PEER:
          tsStatus =
              handler.deleteOldRegionPeer(targetDataNode, consensusGroupId, this.getProcId());
          if (tsStatus.getCode() == SUCCESS_STATUS.getStatusCode()) {
            result = handler.waitTaskFinish(this.getProcId(), targetDataNode);
          } else {
            throw new ProcedureException("DELETE_OLD_REGION_PEER executed failed in DataNode");
          }
          if (result != TRegionMaintainTaskStatus.SUCCESS) {
            throw new ProcedureException("DELETE_OLD_REGION_PEER executed failed in DataNode");
          }
          setNextState(REMOVE_REGION_LOCATION_CACHE);
          break;
        case REMOVE_REGION_LOCATION_CACHE:
          handler.removeRegionLocation(consensusGroupId, targetDataNode);
          logBreakpoint(state.name());
          return Flow.NO_MORE_STATE;
        default:
          throw new ProcedureException("Unsupported state: " + state.name());
      }
    } catch (Exception e) {
      return Flow.NO_MORE_STATE;
    }
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
    return RemoveRegionPeerState.REMOVE_REGION_PEER;
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
}
