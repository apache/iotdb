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
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.exception.runtime.ThriftSerDeException;
import org.apache.iotdb.commons.utils.ThriftCommonsSerDeUtils;
import org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv;
import org.apache.iotdb.confignode.procedure.env.DataNodeRemoveHandler;
import org.apache.iotdb.confignode.procedure.exception.ProcedureException;
import org.apache.iotdb.confignode.procedure.exception.ProcedureSuspendedException;
import org.apache.iotdb.confignode.procedure.exception.ProcedureYieldException;
import org.apache.iotdb.confignode.procedure.state.AddRegionPeerState;
import org.apache.iotdb.confignode.procedure.store.ProcedureType;
import org.apache.iotdb.rpc.TSStatusCode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import static org.apache.iotdb.commons.utils.FileUtils.logBreakpoint;
import static org.apache.iotdb.confignode.conf.ConfigNodeConstant.REGION_MIGRATE_PROCESS;
import static org.apache.iotdb.confignode.procedure.state.AddRegionPeerState.UPDATE_REGION_LOCATION_CACHE;
import static org.apache.iotdb.rpc.TSStatusCode.SUCCESS_STATUS;

public class AddRegionPeerProcedure
    extends StateMachineProcedure<ConfigNodeProcedureEnv, AddRegionPeerState> {
  private static final Logger LOGGER = LoggerFactory.getLogger(AddRegionPeerProcedure.class);
  private TConsensusGroupId consensusGroupId;

  private TDataNodeLocation coordinator;

  private TDataNodeLocation destDataNode;

  private boolean addRegionPeerSuccess = true;

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
    DataNodeRemoveHandler handler = env.getDataNodeRemoveHandler();
    try {
      switch (state) {
        case CREATE_NEW_REGION_PEER:
          handler.createNewRegionPeer(consensusGroupId, destDataNode);
          logBreakpoint(state.name());
          setNextState(AddRegionPeerState.CREATE_NEW_REGION_PEER);
          break;
        case DO_ADD_REGION_PEER:
          TSStatus tsStatus = handler.addRegionPeer(destDataNode, consensusGroupId, coordinator);
          if (tsStatus.getCode() == SUCCESS_STATUS.getStatusCode()) {
            waitForOneMigrationStepFinished(consensusGroupId, state);
          } else {
            throw new ProcedureException("ADD_REGION_PEER executed failed in DataNode");
          }
          logBreakpoint(state.name());
          setNextState(UPDATE_REGION_LOCATION_CACHE);
          break;
        case UPDATE_REGION_LOCATION_CACHE:
          handler.addRegionLocation(consensusGroupId, destDataNode);
          return Flow.NO_MORE_STATE;
      }
    } catch (Exception e) {
      return Flow.NO_MORE_STATE;
    }
    return Flow.HAS_MORE_STATE;
  }

  public TSStatus waitForOneMigrationStepFinished(
      TConsensusGroupId consensusGroupId, AddRegionPeerState state) throws Exception {
    LOGGER.info(
        "{}, Wait for state {} finished, regionId: {}",
        REGION_MIGRATE_PROCESS,
        state,
        consensusGroupId);

    TSStatus status = new TSStatus(SUCCESS_STATUS.getStatusCode());
    synchronized (addRegionPeerLock) {
      try {
        addRegionPeerLock.wait();

        if (!addRegionPeerSuccess) {
          throw new ProcedureException(
              String.format("Region migration failed, regionId: %s", consensusGroupId));
        }
      } catch (InterruptedException e) {
        LOGGER.error(
            "{}, region migration {} interrupt", REGION_MIGRATE_PROCESS, consensusGroupId, e);
        Thread.currentThread().interrupt();
        status.setCode(TSStatusCode.MIGRATE_REGION_ERROR.getStatusCode());
        status.setMessage("Waiting for region migration interruption," + e.getMessage());
      }
    }
    return status;
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
      LOGGER.error("Error in deserialize RemoveConfigNodeProcedure", e);
    }
  }
}
