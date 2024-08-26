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

package org.apache.iotdb.confignode.procedure.impl.model;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.client.ainode.AINodeClient;
import org.apache.iotdb.commons.client.ainode.AINodeClientManager;
import org.apache.iotdb.commons.model.exception.ModelManagementException;
import org.apache.iotdb.confignode.consensus.request.write.model.DropModelPlan;
import org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv;
import org.apache.iotdb.confignode.procedure.exception.ProcedureException;
import org.apache.iotdb.confignode.procedure.impl.node.AbstractNodeProcedure;
import org.apache.iotdb.confignode.procedure.state.model.DropModelState;
import org.apache.iotdb.confignode.procedure.store.ProcedureType;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.thrift.TException;
import org.apache.tsfile.utils.ReadWriteIOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Objects;

import static org.apache.iotdb.confignode.procedure.state.model.DropModelState.CONFIG_NODE_DROPPED;

public class DropModelProcedure extends AbstractNodeProcedure<DropModelState> {

  private static final Logger LOGGER = LoggerFactory.getLogger(DropModelProcedure.class);
  private static final int RETRY_THRESHOLD = 1;

  private String modelName;

  public DropModelProcedure() {
    super();
  }

  public DropModelProcedure(String modelName) {
    super();
    this.modelName = modelName;
  }

  @Override
  protected Flow executeFromState(ConfigNodeProcedureEnv env, DropModelState state) {
    if (modelName == null) {
      return Flow.NO_MORE_STATE;
    }
    try {
      switch (state) {
        case AI_NODE_DROPPED:
          LOGGER.info("Start to drop model [{}] on AI Nodes", modelName);
          dropModelOnAINode(env);
          setNextState(CONFIG_NODE_DROPPED);
          break;
        case CONFIG_NODE_DROPPED:
          dropModelOnConfigNode(env);
          return Flow.NO_MORE_STATE;
        default:
          throw new UnsupportedOperationException(
              String.format("Unknown state during executing dropModelProcedure, %s", state));
      }
    } catch (Exception e) {
      if (isRollbackSupported(state)) {
        LOGGER.error("Fail in DropModelProcedure", e);
        setFailure(new ProcedureException(e.getMessage()));
      } else {
        LOGGER.error(
            "Retrievable error trying to drop model [{}], state [{}]", modelName, state, e);
        if (getCycles() > RETRY_THRESHOLD) {
          setFailure(
              new ProcedureException(
                  String.format(
                      "Fail to drop model [%s] at STATE [%s], %s",
                      modelName, state, e.getMessage())));
        }
      }
    }
    return Flow.HAS_MORE_STATE;
  }

  private void dropModelOnAINode(ConfigNodeProcedureEnv env) {
    LOGGER.info("Start to drop model file [{}] on AI Node", modelName);

    List<Integer> nodeIds =
        env.getConfigManager().getModelManager().getModelDistributions(modelName);
    for (Integer nodeId : nodeIds) {
      try (AINodeClient client =
          AINodeClientManager.getInstance()
              .borrowClient(
                  env.getConfigManager()
                      .getNodeManager()
                      .getRegisteredAINode(nodeId)
                      .getLocation()
                      .getInternalEndPoint())) {
        TSStatus status = client.deleteModel(modelName);
        if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
          LOGGER.warn(
              "Failed to drop model [{}] on AINode [{}], status: {}",
              modelName,
              nodeId,
              status.getMessage());
        }
      } catch (Exception e) {
        LOGGER.warn(
            "Failed to drop model [{}] on AINode [{}], status: {}",
            modelName,
            nodeId,
            e.getMessage());
      }
    }
  }

  private void dropModelOnConfigNode(ConfigNodeProcedureEnv env) {
    try {
      TSStatus response =
          env.getConfigManager().getConsensusManager().write(new DropModelPlan(modelName));
      if (response.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        throw new TException(response.getMessage());
      }
    } catch (Exception e) {
      throw new ModelManagementException(
          String.format(
              "Fail to start training model [%s] on AI Node: %s", modelName, e.getMessage()));
    }
  }

  @Override
  protected void rollbackState(ConfigNodeProcedureEnv env, DropModelState state)
      throws IOException, InterruptedException, ProcedureException {
    // no need to rollback
  }

  @Override
  protected DropModelState getState(int stateId) {
    return DropModelState.values()[stateId];
  }

  @Override
  protected int getStateId(DropModelState dropModelState) {
    return dropModelState.ordinal();
  }

  @Override
  protected DropModelState getInitialState() {
    return DropModelState.AI_NODE_DROPPED;
  }

  @Override
  public void serialize(DataOutputStream stream) throws IOException {
    stream.writeShort(ProcedureType.DROP_MODEL_PROCEDURE.getTypeCode());
    super.serialize(stream);
    ReadWriteIOUtils.write(modelName, stream);
  }

  @Override
  public void deserialize(ByteBuffer byteBuffer) {
    super.deserialize(byteBuffer);
    modelName = ReadWriteIOUtils.readString(byteBuffer);
  }

  @Override
  public boolean equals(Object that) {
    if (that instanceof DropModelProcedure) {
      DropModelProcedure thatProc = (DropModelProcedure) that;
      return thatProc.getProcId() == this.getProcId()
          && thatProc.getState() == this.getState()
          && (thatProc.modelName).equals(this.modelName);
    }
    return false;
  }

  @Override
  public int hashCode() {
    return Objects.hash(getProcId(), getState(), modelName);
  }
}
