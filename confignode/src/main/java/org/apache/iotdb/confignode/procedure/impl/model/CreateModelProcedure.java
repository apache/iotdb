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
import org.apache.iotdb.commons.model.ModelInformation;
import org.apache.iotdb.commons.model.exception.ModelManagementException;
import org.apache.iotdb.confignode.consensus.request.write.model.CreateModelPlan;
import org.apache.iotdb.confignode.consensus.request.write.model.DropModelPlan;
import org.apache.iotdb.confignode.manager.ConfigManager;
import org.apache.iotdb.confignode.persistence.ModelInfo;
import org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv;
import org.apache.iotdb.confignode.procedure.exception.ProcedureException;
import org.apache.iotdb.confignode.procedure.impl.node.AbstractNodeProcedure;
import org.apache.iotdb.confignode.procedure.state.model.CreateModelState;
import org.apache.iotdb.confignode.procedure.store.ProcedureType;
import org.apache.iotdb.consensus.common.response.ConsensusWriteResponse;
import org.apache.iotdb.db.client.MLNodeClient;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Objects;

public class CreateModelProcedure extends AbstractNodeProcedure<CreateModelState> {

  private static final Logger LOGGER = LoggerFactory.getLogger(CreateModelProcedure.class);
  private static final int RETRY_THRESHOLD = 5;

  private ModelInformation modelInformation;
  private Map<String, String> modelConfigs;

  public CreateModelProcedure() {
    super();
  }

  public CreateModelProcedure(ModelInformation modelInformation, Map<String, String> modelConfigs) {
    super();
    this.modelInformation = modelInformation;
    this.modelConfigs = modelConfigs;
  }

  @Override
  protected Flow executeFromState(ConfigNodeProcedureEnv env, CreateModelState state) {
    if (modelInformation == null) {
      return Flow.NO_MORE_STATE;
    }
    try {
      switch (state) {
        case INIT:
          LOGGER.info("Start to create model [{}]", modelInformation.getModelId());

          ModelInfo modelInfo = env.getConfigManager().getModelManager().getModelInfo();
          modelInfo.acquireModelTableLock();
          String modelId = modelInformation.getModelId();
          if (modelInfo.isModelExist(modelId)) {
            throw new ModelManagementException(
                String.format(
                    "Failed to create model [%s], the same name model has been created", modelId));
          }
          setNextState(CreateModelState.VALIDATED);
          break;

        case VALIDATED:
          ConfigManager configManager = env.getConfigManager();
          modelId = modelInformation.getModelId();

          LOGGER.info("Start to add model [{}] in ModelTable on Config Nodes", modelId);

          ConsensusWriteResponse response =
              configManager.getConsensusManager().write(new CreateModelPlan(modelInformation));
          if (!response.isSuccessful()) {
            throw new ModelManagementException(
                String.format(
                    "Failed to add model [%s] in ModelTable on Config Nodes: %s",
                    modelId, response.getErrorMessage()));
          }

          setNextState(CreateModelState.CONFIG_NODE_ACTIVE);
          break;

        case CONFIG_NODE_ACTIVE:
          LOGGER.info("Start to train model [{}] on ML Node", modelInformation.getModelId());

          try (MLNodeClient client = new MLNodeClient()) {
            TSStatus status = client.createTrainingTask(modelInformation, modelConfigs);
            if (status.code != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
              throw new TException(status.getMessage());
            }

            setNextState(CreateModelState.ML_NODE_ACTIVE);
          } catch (TException e) {
            throw new ModelManagementException(
                String.format(
                    "Fail to start training model [%s] on ML Node: %s",
                    modelInformation.getModelId(), e.getMessage()));
          }
          break;

        case ML_NODE_ACTIVE:
          env.getConfigManager().getModelManager().getModelInfo().releaseModelTableLock();
          return Flow.NO_MORE_STATE;
      }
    } catch (Exception e) {
      if (isRollbackSupported(state)) {
        LOGGER.error("Fail in CreateModelProcedure", e);
        setFailure(new ProcedureException(e.getMessage()));
      } else {
        LOGGER.error(
            "Retrievable error trying to create model [{}], state [{}]",
            modelInformation.getModelId(),
            state,
            e);
        if (getCycles() > RETRY_THRESHOLD) {
          setFailure(
              new ProcedureException(
                  String.format(
                      "Fail to create model [%s] at STATE [%s]",
                      modelInformation.getModelId(), state)));
        }
      }
    }
    return Flow.HAS_MORE_STATE;
  }

  @Override
  protected void rollbackState(ConfigNodeProcedureEnv env, CreateModelState state)
      throws IOException, InterruptedException, ProcedureException {
    switch (state) {
      case INIT:
        LOGGER.info("Start [INIT] rollback of model [{}]", modelInformation.getModelId());

        env.getConfigManager().getModelManager().getModelInfo().releaseModelTableLock();
        break;

      case VALIDATED:
        LOGGER.info("Start [VALIDATED] rollback of model [{}]", modelInformation.getModelId());

        env.getConfigManager()
            .getConsensusManager()
            .write(new DropModelPlan(modelInformation.getModelId()));
        break;

      default:
        break;
    }
  }

  @Override
  protected boolean isRollbackSupported(CreateModelState state) {
    return true;
  }

  @Override
  protected CreateModelState getState(int stateId) {
    return CreateModelState.values()[stateId];
  }

  @Override
  protected int getStateId(CreateModelState createModelState) {
    return createModelState.ordinal();
  }

  @Override
  protected CreateModelState getInitialState() {
    return CreateModelState.INIT;
  }

  @Override
  public void serialize(DataOutputStream stream) throws IOException {
    stream.writeShort(ProcedureType.CREATE_MODEL_PROCEDURE.getTypeCode());
    super.serialize(stream);
    modelInformation.serialize(stream);
    ReadWriteIOUtils.write(modelConfigs, stream);
  }

  @Override
  public void deserialize(ByteBuffer byteBuffer) {
    super.deserialize(byteBuffer);
    modelInformation = ModelInformation.deserialize(byteBuffer);
    modelConfigs = ReadWriteIOUtils.readMap(byteBuffer);
  }

  @Override
  public boolean equals(Object that) {
    if (that instanceof CreateModelProcedure) {
      CreateModelProcedure thatProc = (CreateModelProcedure) that;
      return thatProc.getProcId() == this.getProcId()
          && thatProc.getState() == this.getState()
          && thatProc.modelInformation.equals(this.modelInformation)
          && thatProc.modelConfigs.equals(this.modelConfigs);
    }
    return false;
  }

  @Override
  public int hashCode() {
    return Objects.hash(getProcId(), getState(), modelInformation, modelConfigs);
  }
}
