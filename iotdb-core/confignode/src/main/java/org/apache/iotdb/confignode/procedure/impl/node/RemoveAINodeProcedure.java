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

import org.apache.iotdb.common.rpc.thrift.TAINodeLocation;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.utils.ThriftCommonsSerDeUtils;
import org.apache.iotdb.confignode.consensus.request.write.ainode.RemoveAINodePlan;
import org.apache.iotdb.confignode.consensus.request.write.model.DropModelInNodePlan;
import org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv;
import org.apache.iotdb.confignode.procedure.exception.ProcedureException;
import org.apache.iotdb.confignode.procedure.exception.ProcedureSuspendedException;
import org.apache.iotdb.confignode.procedure.exception.ProcedureYieldException;
import org.apache.iotdb.confignode.procedure.state.RemoveAINodeState;
import org.apache.iotdb.confignode.procedure.store.ProcedureType;
import org.apache.iotdb.rpc.TSStatusCode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;

public class RemoveAINodeProcedure extends AbstractNodeProcedure<RemoveAINodeState> {

  private static final Logger LOGGER = LoggerFactory.getLogger(RemoveAINodeProcedure.class);
  private static final int RETRY_THRESHOLD = 5;

  private TAINodeLocation removedAINode;

  public RemoveAINodeProcedure(TAINodeLocation removedAINode) {
    super();
    this.removedAINode = removedAINode;
  }

  public RemoveAINodeProcedure() {
    super();
  }

  @Override
  protected Flow executeFromState(ConfigNodeProcedureEnv env, RemoveAINodeState state)
      throws ProcedureSuspendedException, ProcedureYieldException, InterruptedException {
    if (removedAINode == null) {
      return Flow.NO_MORE_STATE;
    }

    try {
      switch (state) {
        case MODEL_DELETE:
          env.getConfigManager()
              .getConsensusManager()
              .write(new DropModelInNodePlan(removedAINode.aiNodeId));
          // Cause the AINode is removed, so we don't need to remove the model file.
          setNextState(RemoveAINodeState.NODE_REMOVE);
          break;
        case NODE_REMOVE:
          TSStatus response =
              env.getConfigManager()
                  .getConsensusManager()
                  .write(new RemoveAINodePlan(removedAINode));

          if (response.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
            throw new ProcedureException(
                String.format(
                    "Fail to remove [%s] AINode on Config Nodes [%s]",
                    removedAINode, response.getMessage()));
          }
          return Flow.NO_MORE_STATE;
        default:
          throw new UnsupportedOperationException(
              String.format("Unknown state during executing removeAINodeProcedure, %s", state));
      }
    } catch (Exception e) {
      if (isRollbackSupported(state)) {
        setFailure(new ProcedureException(e.getMessage()));
      } else {
        LOGGER.error(
            "Retrievable error trying to remove AINode [{}], state [{}]", removedAINode, state, e);
        if (getCycles() > RETRY_THRESHOLD) {
          setFailure(
              new ProcedureException(
                  String.format(
                      "Fail to remove AINode [%s] at STATE [%s], %s",
                      removedAINode, state, e.getMessage())));
        }
      }
    }
    return Flow.HAS_MORE_STATE;
  }

  @Override
  protected void rollbackState(
      ConfigNodeProcedureEnv configNodeProcedureEnv, RemoveAINodeState removeAINodeState)
      throws IOException, InterruptedException, ProcedureException {
    // no need to rollback
  }

  @Override
  protected RemoveAINodeState getState(int stateId) {
    return RemoveAINodeState.values()[stateId];
  }

  @Override
  protected int getStateId(RemoveAINodeState removeAINodeState) {
    return removeAINodeState.ordinal();
  }

  @Override
  protected RemoveAINodeState getInitialState() {
    return RemoveAINodeState.MODEL_DELETE;
  }

  @Override
  public void serialize(DataOutputStream stream) throws IOException {
    stream.writeShort(ProcedureType.REMOVE_AI_NODE_PROCEDURE.getTypeCode());
    super.serialize(stream);
    ThriftCommonsSerDeUtils.serializeTAINodeLocation(removedAINode, stream);
  }

  @Override
  public void deserialize(ByteBuffer byteBuffer) {
    super.deserialize(byteBuffer);
    removedAINode = ThriftCommonsSerDeUtils.deserializeTAINodeLocation(byteBuffer);
  }

  @Override
  public boolean equals(Object that) {
    if (that instanceof RemoveAINodeProcedure) {
      RemoveAINodeProcedure thatProc = (RemoveAINodeProcedure) that;
      return thatProc.getProcId() == this.getProcId()
          && thatProc.getState() == this.getState()
          && (thatProc.removedAINode).equals(this.removedAINode);
    }
    return false;
  }

  @Override
  public int hashCode() {
    return Objects.hash(getProcId(), getState(), removedAINode);
  }
}
