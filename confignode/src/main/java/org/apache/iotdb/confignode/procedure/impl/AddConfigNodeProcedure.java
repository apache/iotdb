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

package org.apache.iotdb.confignode.procedure.impl;

import org.apache.iotdb.common.rpc.thrift.TConfigNodeLocation;
import org.apache.iotdb.commons.exception.runtime.ThriftSerDeException;
import org.apache.iotdb.commons.utils.ThriftConfigNodeSerDeUtils;
import org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv;
import org.apache.iotdb.confignode.procedure.exception.ProcedureException;
import org.apache.iotdb.confignode.procedure.state.AddConfigNodeState;
import org.apache.iotdb.confignode.procedure.store.ProcedureFactory;

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
          setNextState(AddConfigNodeState.ADD_CONSENSUS_GROUP);
          break;
        case ADD_CONSENSUS_GROUP:
          env.addConsensusGroup(tConfigNodeLocation);
          setNextState(AddConfigNodeState.ADD_PEER);
          LOG.info("Add consensus group {}", tConfigNodeLocation);
          break;
        case ADD_PEER:
          env.addConfigNodePeer(tConfigNodeLocation);
          setNextState(AddConfigNodeState.REGISTER_SUCCESS);
          LOG.info("Add Peer of {}", tConfigNodeLocation);
          break;
        case REGISTER_SUCCESS:
          env.notifyRegisterSuccess(tConfigNodeLocation);
          env.applyConfigNode(tConfigNodeLocation);
          env.broadCastTheLatestConfigNodeGroup();
          return Flow.NO_MORE_STATE;
      }
    } catch (Exception e) {
      if (isRollbackSupported(state)) {
        setFailure(new ProcedureException("Add Config Node failed " + state));
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
      case ADD_CONSENSUS_GROUP:
        env.removeConsensusGroup(tConfigNodeLocation);
        LOG.info("Rollback add consensus group:{}", tConfigNodeLocation);
        break;
      case ADD_PEER:
        env.removeConfigNodePeer(tConfigNodeLocation);
        LOG.info("Rollback remove peer:{}", tConfigNodeLocation);
        break;
    }
  }

  @Override
  protected boolean isRollbackSupported(AddConfigNodeState state) {
    switch (state) {
      case ADD_CONSENSUS_GROUP:
      case ADD_PEER:
        return true;
    }
    return false;
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
    stream.writeInt(ProcedureFactory.ProcedureType.ADD_CONFIG_NODE_PROCEDURE.ordinal());
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
