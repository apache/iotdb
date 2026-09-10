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
import org.apache.iotdb.confignode.i18n.ProcedureMessages;
import org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv;
import org.apache.iotdb.confignode.procedure.exception.ProcedureException;
import org.apache.iotdb.confignode.procedure.state.RemoveConfigNodeState;
import org.apache.iotdb.confignode.procedure.store.ProcedureType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;

/** remove config node procedure */
public class RemoveConfigNodeProcedure extends AbstractNodeProcedure<RemoveConfigNodeState> {
  private static final Logger LOG = LoggerFactory.getLogger(RemoveConfigNodeProcedure.class);
  private static final int RETRY_THRESHOLD = 5;

  private TConfigNodeLocation removedConfigNode;

  public RemoveConfigNodeProcedure() {
    super();
  }

  public RemoveConfigNodeProcedure(TConfigNodeLocation removedConfigNode) {
    super();
    this.removedConfigNode = removedConfigNode;
  }

  @Override
  protected Flow executeFromState(ConfigNodeProcedureEnv env, RemoveConfigNodeState state) {
    if (removedConfigNode == null) {
      return Flow.NO_MORE_STATE;
    }
    try {
      switch (state) {
        case REMOVE_PEER:
          env.removeConfigNodePeer(removedConfigNode);
          setNextState(RemoveConfigNodeState.DELETE_PEER);
          LOG.info(ProcedureMessages.REMOVE_PEER_FOR_CONFIGNODE, removedConfigNode);
          break;
        case DELETE_PEER:
          env.deleteConfigNodePeer(removedConfigNode);
          setNextState(RemoveConfigNodeState.STOP_AND_CLEAR_CONFIG_NODE);
          LOG.info(ProcedureMessages.DELETE_PEER_FOR_CONFIGNODE, removedConfigNode);
          break;
        case STOP_AND_CLEAR_CONFIG_NODE:
          env.stopAndClearConfigNode(removedConfigNode);
          LOG.info(ProcedureMessages.STOP_AND_CLEAR_CONFIGNODE, removedConfigNode);
          return Flow.NO_MORE_STATE;
      }
    } catch (Exception e) {
      if (isRollbackSupported(state)) {
        setFailure(
            new ProcedureException(
                ProcedureMessages.REMOVE_CONFIG_NODE
                    + removedConfigNode
                    + ProcedureMessages.EXCEPTION_FAILED_DAA6EA2F
                    + state));
      } else {
        LOG.error(
            ProcedureMessages
                .LOG_RETRIEVABLE_ERROR_TRYING_REMOVE_CONFIG_NODE_ARG_STATE_ARG_3754EBA1,
            removedConfigNode,
            state,
            e);
        if (getCycles() > RETRY_THRESHOLD) {
          setFailure(new ProcedureException(ProcedureMessages.STATE_STUCK_AT + state));
        }
      }
    }
    return Flow.HAS_MORE_STATE;
  }

  @Override
  protected void rollbackState(ConfigNodeProcedureEnv env, RemoveConfigNodeState state)
      throws IOException, InterruptedException, ProcedureException {}

  @Override
  protected boolean isRollbackSupported(RemoveConfigNodeState state) {
    return true;
  }

  @Override
  protected RemoveConfigNodeState getState(int stateId) {
    return RemoveConfigNodeState.values()[stateId];
  }

  @Override
  protected int getStateId(RemoveConfigNodeState deleteStorageGroupState) {
    return deleteStorageGroupState.ordinal();
  }

  @Override
  protected RemoveConfigNodeState getInitialState() {
    return RemoveConfigNodeState.REMOVE_PEER;
  }

  @Override
  public void serialize(DataOutputStream stream) throws IOException {
    stream.writeShort(ProcedureType.REMOVE_CONFIG_NODE_PROCEDURE.getTypeCode());
    super.serialize(stream);
    ThriftConfigNodeSerDeUtils.serializeTConfigNodeLocation(removedConfigNode, stream);
  }

  @Override
  public void deserialize(ByteBuffer byteBuffer) {
    super.deserialize(byteBuffer);
    try {
      removedConfigNode = ThriftConfigNodeSerDeUtils.deserializeTConfigNodeLocation(byteBuffer);
    } catch (ThriftSerDeException e) {
      LOG.error(ProcedureMessages.ERROR_IN_DESERIALIZE_REMOVECONFIGNODEPROCEDURE, e);
    }
  }

  @Override
  public boolean equals(Object that) {
    if (that instanceof RemoveConfigNodeProcedure) {
      RemoveConfigNodeProcedure thatProc = (RemoveConfigNodeProcedure) that;
      return thatProc.getProcId() == this.getProcId()
          && thatProc.getState() == this.getState()
          && thatProc.removedConfigNode.equals(this.removedConfigNode);
    }
    return false;
  }

  @Override
  public int hashCode() {
    return Objects.hash(this.removedConfigNode);
  }
}
