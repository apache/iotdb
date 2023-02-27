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

import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.common.rpc.thrift.TrainingState;
import org.apache.iotdb.commons.model.exception.ModelManagementException;
import org.apache.iotdb.confignode.client.DataNodeRequestType;
import org.apache.iotdb.confignode.client.sync.SyncDataNodeClientPool;
import org.apache.iotdb.confignode.consensus.request.write.model.DropModelPlan;
import org.apache.iotdb.confignode.consensus.request.write.model.UpdateModelStatePlan;
import org.apache.iotdb.confignode.persistence.ModelInfo;
import org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv;
import org.apache.iotdb.confignode.procedure.exception.ProcedureException;
import org.apache.iotdb.confignode.procedure.impl.node.AbstractNodeProcedure;
import org.apache.iotdb.confignode.procedure.state.model.DropModelState;
import org.apache.iotdb.confignode.procedure.store.ProcedureType;
import org.apache.iotdb.confignode.rpc.thrift.TUpdateModelStateReq;
import org.apache.iotdb.consensus.common.response.ConsensusWriteResponse;
import org.apache.iotdb.db.client.MLNodeClient;
import org.apache.iotdb.mpp.rpc.thrift.TDeleteModelMetricsReq;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.Optional;

public class DropModelProcedure extends AbstractNodeProcedure<DropModelState> {

  private static final Logger LOGGER = LoggerFactory.getLogger(DropModelProcedure.class);
  private static final int RETRY_THRESHOLD = 5;

  private String modelId;

  public DropModelProcedure() {
    super();
  }

  public DropModelProcedure(String modelId) {
    super();
    this.modelId = modelId;
  }

  @Override
  protected Flow executeFromState(ConfigNodeProcedureEnv env, DropModelState state) {
    if (modelId == null) {
      return Flow.NO_MORE_STATE;
    }
    try {
      switch (state) {
        case INIT:
          LOGGER.info("Start to drop model [{}]", modelId);

          ModelInfo modelInfo = env.getConfigManager().getModelManager().getModelInfo();
          modelInfo.acquireModelTableLock();
          if (!modelInfo.isModelExist(modelId)) {
            throw new ModelManagementException(
                String.format(
                    "Failed to drop model [%s], this model has not been created", modelId));
          }
          setNextState(DropModelState.VALIDATED);
          break;

        case VALIDATED:
          LOGGER.info("Change state of model [{}] to DROPPING", modelId);

          ConsensusWriteResponse response =
              env.getConfigManager()
                  .getConsensusManager()
                  .write(
                      new UpdateModelStatePlan(
                          new TUpdateModelStateReq(modelId, TrainingState.DROPPING)));
          if (!response.isSuccessful()) {
            throw new ModelManagementException(
                String.format(
                    "Failed to drop model [%s], fail to modify model state: %s",
                    modelId, response.getErrorMessage()));
          }

          setNextState(DropModelState.CONFIG_NODE_DROPPING);
          break;

        case CONFIG_NODE_DROPPING:
          LOGGER.info("Start to drop model metrics [{}] on Data Nodes", modelId);

          Optional<TDataNodeLocation> targetDataNode =
              env.getConfigManager().getNodeManager().getLowestLoadDataNode();
          if (!targetDataNode.isPresent()) {
            // no usable DataNode
            throw new ModelManagementException(
                String.format("Failed to drop model [%s], there is no RUNNING DataNode", modelId));
          }

          TSStatus status =
              SyncDataNodeClientPool.getInstance()
                  .sendSyncRequestToDataNodeWithRetry(
                      targetDataNode.get().getInternalEndPoint(),
                      new TDeleteModelMetricsReq(modelId),
                      DataNodeRequestType.DELETE_MODEL_METRICS);
          if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
            throw new ModelManagementException(
                String.format(
                    "Failed to drop model [%s], fail to delete metrics: %s",
                    modelId, status.getMessage()));
          }

          setNextState(DropModelState.DATA_NODE_DROPPED);
          break;

        case DATA_NODE_DROPPED:
          LOGGER.info("Start to drop model file [{}] on Ml Node", modelId);

          try (MLNodeClient client = new MLNodeClient()) {
            status = client.deleteModel(modelId);
            if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
              throw new TException(status.getMessage());
            }
          } catch (TException e) {
            throw new ModelManagementException(
                String.format(
                    "Failed to drop model [%s], fail to delete model on MLNode: %s",
                    modelId, e.getMessage()));
          }

          setNextState(DropModelState.ML_NODE_DROPPED);
          break;

        case ML_NODE_DROPPED:
          LOGGER.info("Start to drop model [{}] on Config Nodes", modelId);

          response = env.getConfigManager().getConsensusManager().write(new DropModelPlan(modelId));
          if (!response.isSuccessful()) {
            throw new ModelManagementException(
                String.format(
                    "Failed to drop model [%s], fail to drop model on Config Nodes: %s",
                    modelId, response.getErrorMessage()));
          }

          setNextState(DropModelState.CONFIG_NODE_DROPPED);
          break;

        case CONFIG_NODE_DROPPED:
          env.getConfigManager().getModelManager().getModelInfo().releaseModelTableLock();
          return Flow.NO_MORE_STATE;
      }
    } catch (Exception e) {
      if (isRollbackSupported(state)) {
        LOGGER.error("Fail in DropModelProcedure", e);
        setFailure(new ProcedureException(e.getMessage()));
      } else {
        LOGGER.error("Retrievable error trying to drop model [{}], state [{}]", modelId, state, e);
        if (getCycles() > RETRY_THRESHOLD) {
          setFailure(
              new ProcedureException(
                  String.format("Fail to drop model [%s] at STATE [%s]", modelId, state)));
        }
      }
    }
    return Flow.HAS_MORE_STATE;
  }

  @Override
  protected void rollbackState(ConfigNodeProcedureEnv env, DropModelState state)
      throws IOException, InterruptedException, ProcedureException {
    if (state == DropModelState.INIT) {
      LOGGER.info("Start [INIT] rollback of model [{}]", modelId);

      env.getConfigManager().getModelManager().getModelInfo().releaseModelTableLock();
    }
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
    return DropModelState.INIT;
  }

  @Override
  public void serialize(DataOutputStream stream) throws IOException {
    stream.writeShort(ProcedureType.DROP_MODEL_PROCEDURE.getTypeCode());
    super.serialize(stream);
    ReadWriteIOUtils.write(modelId, stream);
  }

  @Override
  public void deserialize(ByteBuffer byteBuffer) {
    super.deserialize(byteBuffer);
    modelId = ReadWriteIOUtils.readString(byteBuffer);
  }

  @Override
  public boolean equals(Object that) {
    if (that instanceof DropModelProcedure) {
      DropModelProcedure thatProc = (DropModelProcedure) that;
      return thatProc.getProcId() == this.getProcId()
          && thatProc.getState() == this.getState()
          && (thatProc.modelId).equals(this.modelId);
    }
    return false;
  }

  @Override
  public int hashCode() {
    return Objects.hash(getProcId(), getState(), modelId);
  }
}
