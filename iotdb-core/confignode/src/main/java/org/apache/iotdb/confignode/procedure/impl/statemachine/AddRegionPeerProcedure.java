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
import org.apache.iotdb.common.rpc.thrift.TRegionMigrateResultReportReq;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.exception.runtime.ThriftSerDeException;
import org.apache.iotdb.commons.utils.ThriftCommonsSerDeUtils;
import org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv;
import org.apache.iotdb.confignode.procedure.env.RegionMaintainHandler;
import org.apache.iotdb.confignode.procedure.exception.ProcedureException;
import org.apache.iotdb.confignode.procedure.exception.ProcedureSuspendedException;
import org.apache.iotdb.confignode.procedure.exception.ProcedureYieldException;
import org.apache.iotdb.confignode.procedure.state.AddRegionPeerState;
import org.apache.iotdb.confignode.procedure.store.ProcedureType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import static org.apache.iotdb.commons.utils.FileUtils.logBreakpoint;
import static org.apache.iotdb.confignode.conf.ConfigNodeConstant.ADD_REGION_PEER_PROGRESS;
import static org.apache.iotdb.confignode.procedure.state.AddRegionPeerState.UPDATE_REGION_LOCATION_CACHE;
import static org.apache.iotdb.rpc.TSStatusCode.SUCCESS_STATUS;

public class AddRegionPeerProcedure
    extends StateMachineProcedure<ConfigNodeProcedureEnv, AddRegionPeerState> {
  private static final Logger LOGGER = LoggerFactory.getLogger(AddRegionPeerProcedure.class);
  private TConsensusGroupId consensusGroupId;

  private TDataNodeLocation coordinator;

  private TDataNodeLocation destDataNode;

  private boolean addRegionPeerSuccess = true;
  private String addRegionPeerResult;

  private final Object addRegionPeerLock = new Object();

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
    RegionMaintainHandler handler = env.getDataNodeRemoveHandler();
    try {
      switch (state) {
        case CREATE_NEW_REGION_PEER:
          handler.createNewRegionPeer(consensusGroupId, destDataNode);
          logBreakpoint(state.name());
          setNextState(AddRegionPeerState.DO_ADD_REGION_PEER);
          break;
        case DO_ADD_REGION_PEER:
          TSStatus tsStatus =
              handler.addRegionPeer(this.getProcId(), destDataNode, consensusGroupId, coordinator);
          TRegionMaintainTaskStatus result;
          if (tsStatus.getCode() == SUCCESS_STATUS.getStatusCode()) {
            result = handler.waitTaskFinish(this.getProcId(), coordinator);
          } else {
            throw new ProcedureException("ADD_REGION_PEER executed failed in DataNode");
          }
          if (result == TRegionMaintainTaskStatus.SUCCESS) {
            setNextState(UPDATE_REGION_LOCATION_CACHE);
            break;
          }
          throw new ProcedureException("ADD_REGION_PEER executed failed in DataNode");
        case UPDATE_REGION_LOCATION_CACHE:
          handler.addRegionLocation(consensusGroupId, destDataNode);
          return Flow.NO_MORE_STATE;
        default:
          throw new ProcedureException("Unsupported state: " + state.name());
      }
    } catch (Exception e) {
      return Flow.NO_MORE_STATE;
    }
    return Flow.HAS_MORE_STATE;
  }

  // TODO: Clear all remaining information related to 'migrate' and 'migration'

  public void notifyAddPeerFinished(TRegionMigrateResultReportReq req) {

    LOGGER.info(
        "{}, ConfigNode received region migration result reported by DataNode: {}",
        ADD_REGION_PEER_PROGRESS,
        req);

    // TODO the req is used in roll back
    synchronized (addRegionPeerLock) {
      TSStatus migrateStatus = req.getMigrateResult();
      // Migration failed
      if (migrateStatus.getCode() != SUCCESS_STATUS.getStatusCode()) {
        LOGGER.info(
            "{}, Region migration failed in DataNode, migrateStatus: {}",
            ADD_REGION_PEER_PROGRESS,
            migrateStatus);
        addRegionPeerSuccess = false;
        addRegionPeerResult = migrateStatus.toString();
      }
      addRegionPeerLock.notifyAll();
    }
  }

  @Override
  protected boolean isRollbackSupported(AddRegionPeerState state) {
    return false;
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

  public TDataNodeLocation getDestDataNode() {
    return destDataNode;
  }
}
