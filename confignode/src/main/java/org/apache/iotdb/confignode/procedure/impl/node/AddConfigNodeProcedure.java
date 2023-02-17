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

package org.apache.iotdb.confignode.procedure.impl.node;

import org.apache.iotdb.common.rpc.thrift.TConfigNodeLocation;
import org.apache.iotdb.commons.exception.runtime.ThriftSerDeException;
import org.apache.iotdb.commons.utils.ThriftConfigNodeSerDeUtils;
import org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv;
import org.apache.iotdb.confignode.procedure.exception.ProcedureException;
import org.apache.iotdb.confignode.procedure.state.AddConfigNodeState;
import org.apache.iotdb.confignode.procedure.store.ProcedureType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

/** add config node procedure */
public class AddConfigNodeProcedure extends AbstractNodeProcedure<AddConfigNodeState> {
  private static final Logger LOG = LoggerFactory.getLogger(AddConfigNodeProcedure.class);
  private static final int RETRY_THRESHOLD = 5;

  private TConfigNodeLocation tConfigNodeLocation;

  public AddConfigNodeProcedure() {
    super();
  }

  public AddConfigNodeProcedure(TConfigNodeLocation tConfigNodeLocation) {
    super();
    this.tConfigNodeLocation = tConfigNodeLocation;
  }

  @Override
  protected Flow executeFromState(ConfigNodeProcedureEnv env, AddConfigNodeState state) {
    if (tConfigNodeLocation == null) {
      return Flow.NO_MORE_STATE;
    }
    try {
      switch (state) {
        case ADD_CONFIG_NODE_PREPARE:
          setNextState(AddConfigNodeState.CREATE_PEER);
          break;
        case CREATE_PEER:
          LOG.info("Executing CREATE_PEER on {}...", tConfigNodeLocation);
          env.addConsensusGroup(tConfigNodeLocation);
          setNextState(AddConfigNodeState.ADD_PEER);
          LOG.info("Successfully CREATE_PEER on {}", tConfigNodeLocation);
          break;
        case ADD_PEER:
          LOG.info("Executing ADD_PEER {}...", tConfigNodeLocation);
          env.addConfigNodePeer(tConfigNodeLocation);
          setNextState(AddConfigNodeState.REGISTER_SUCCESS);
          LOG.info("Successfully ADD_PEER {}", tConfigNodeLocation);
          break;
        case REGISTER_SUCCESS:
          env.notifyRegisterSuccess(tConfigNodeLocation);
          env.applyConfigNode(tConfigNodeLocation);
          env.broadCastTheLatestConfigNodeGroup();
          LOG.info("The ConfigNode: {} is successfully added to the cluster", tConfigNodeLocation);
          return Flow.NO_MORE_STATE;
      }
    } catch (Exception e) {
      if (isRollbackSupported(state)) {
        setFailure(new ProcedureException("Add ConfigNode failed " + state));
      } else {
        LOG.error(
            "Retrievable error trying to add config node {}, state {}",
            tConfigNodeLocation,
            state,
            e);
        if (getCycles() > RETRY_THRESHOLD) {
          setFailure(new ProcedureException("State stuck at " + state));
        }
      }
    }
    return Flow.HAS_MORE_STATE;
  }

  @Override
  protected void rollbackState(ConfigNodeProcedureEnv env, AddConfigNodeState state)
      throws ProcedureException {
    switch (state) {
      case CREATE_PEER:
        env.deleteConfigNodePeer(tConfigNodeLocation);
        LOG.info("Rollback CREATE_PEER for: {}", tConfigNodeLocation);
        break;
      case ADD_PEER:
        env.removeConfigNodePeer(tConfigNodeLocation);
        LOG.info("Rollback ADD_PEER for: {}", tConfigNodeLocation);
        break;
      default:
        break;
    }
  }

  @Override
  protected boolean isRollbackSupported(AddConfigNodeState state) {
    switch (state) {
      case CREATE_PEER:
      case ADD_PEER:
        return true;
      default:
        return false;
    }
  }

  @Override
  protected AddConfigNodeState getState(int stateId) {
    return AddConfigNodeState.values()[stateId];
  }

  @Override
  protected int getStateId(AddConfigNodeState deleteStorageGroupState) {
    return deleteStorageGroupState.ordinal();
  }

  @Override
  protected AddConfigNodeState getInitialState() {
    return AddConfigNodeState.ADD_CONFIG_NODE_PREPARE;
  }

  @Override
  public void serialize(DataOutputStream stream) throws IOException {
    stream.writeShort(ProcedureType.ADD_CONFIG_NODE_PROCEDURE.getTypeCode());
    super.serialize(stream);
    ThriftConfigNodeSerDeUtils.serializeTConfigNodeLocation(tConfigNodeLocation, stream);
  }

  @Override
  public void deserialize(ByteBuffer byteBuffer) {
    super.deserialize(byteBuffer);
    try {
      tConfigNodeLocation = ThriftConfigNodeSerDeUtils.deserializeTConfigNodeLocation(byteBuffer);
    } catch (ThriftSerDeException e) {
      LOG.error("Error in deserialize AddConfigNodeProcedure", e);
    }
  }

  @Override
  public boolean equals(Object that) {
    if (that instanceof AddConfigNodeProcedure) {
      AddConfigNodeProcedure thatProc = (AddConfigNodeProcedure) that;
      return thatProc.getProcId() == this.getProcId()
          && thatProc.getState() == this.getState()
          && thatProc.tConfigNodeLocation.equals(this.tConfigNodeLocation);
    }
    return false;
  }
}
