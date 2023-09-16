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
import org.apache.iotdb.commons.client.mlnode.MLNodeClient;
import org.apache.iotdb.commons.client.mlnode.MLNodeClientManager;
import org.apache.iotdb.commons.client.mlnode.MLNodeInfo;
import org.apache.iotdb.commons.model.ModelInformation;
import org.apache.iotdb.commons.model.exception.ModelManagementException;
import org.apache.iotdb.confignode.consensus.request.write.model.CreateModelPlan;
import org.apache.iotdb.confignode.consensus.request.write.model.DropModelPlan;
import org.apache.iotdb.confignode.manager.ConfigManager;
import org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv;
import org.apache.iotdb.confignode.procedure.exception.ProcedureException;
import org.apache.iotdb.confignode.procedure.impl.node.AbstractNodeProcedure;
import org.apache.iotdb.confignode.procedure.state.model.CreateModelState;
import org.apache.iotdb.confignode.procedure.store.ProcedureType;
import org.apache.iotdb.consensus.exception.ConsensusException;
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
  private static final int RETRY_THRESHOLD = 1;

  private ModelInformation modelInformation;
  private Map<String, String> hyperparameters;

  public CreateModelProcedure() {
    super();
  }

  public CreateModelProcedure(
      ModelInformation modelInformation, Map<String, String> hyperparameters) {
    super();
    this.modelInformation = modelInformation;
    this.hyperparameters = hyperparameters;
  }

  @Override
  protected Flow executeFromState(ConfigNodeProcedureEnv env, CreateModelState state) {
    if (modelInformation == null) {
      return Flow.NO_MORE_STATE;
    }
    try {
      switch (state) {
        case INIT:
          createModel(env);
          break;
        case ML_NODE_ACTIVE:
          trainModel();
          return Flow.NO_MORE_STATE;
        default:
          throw new UnsupportedOperationException(
              String.format("Unknown state during executing createModelProcedure, %s", state));
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

  private void createModel(ConfigNodeProcedureEnv env) throws ConsensusException {
    LOGGER.info("Start to create model [{}]", modelInformation.getModelId());

    ConfigManager configManager = env.getConfigManager();
    TSStatus response =
        configManager.getConsensusManager().write(new CreateModelPlan(modelInformation));
    if (response.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      throw new ModelManagementException(
          String.format(
              "Failed to add model [%s] in ModelTable on Config Nodes: %s",
              modelInformation.getModelId(), response.getMessage()));
    }
    setNextState(CreateModelState.ML_NODE_ACTIVE);
  }

  private void trainModel() {
    LOGGER.info("Start to train model [{}] on ML Node", modelInformation.getModelId());

    try (MLNodeClient client =
        MLNodeClientManager.getInstance().borrowClient(MLNodeInfo.endPoint)) {
      TSStatus status = client.createTrainingTask(modelInformation, hyperparameters);
      if (status.code != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        throw new TException(status.getMessage());
      }
    } catch (Exception e) {
      throw new ModelManagementException(
          String.format(
              "Fail to start training model [%s] on ML Node: %s",
              modelInformation.getModelId(), e.getMessage()));
    }
  }

  @Override
  protected void rollbackState(ConfigNodeProcedureEnv env, CreateModelState state)
      throws IOException, InterruptedException, ProcedureException {
    switch (state) {
      case INIT:
        // do nothing
        break;
      case ML_NODE_ACTIVE:
        LOGGER.info("Start [VALIDATED] rollback of model [{}]", modelInformation.getModelId());
        try {
          env.getConfigManager()
              .getConsensusManager()
              .write(new DropModelPlan(modelInformation.getModelId()));
        } catch (ConsensusException e) {
          LOGGER.warn("Failed in the write API executing the consensus layer due to: ", e);
        }
        break;
      default:
        break;
    }
    if (Objects.requireNonNull(state) == CreateModelState.INIT) {
      LOGGER.info("Start [INIT] rollback of model [{}]", modelInformation.getModelId());
      try {
        env.getConfigManager()
            .getConsensusManager()
            .write(new DropModelPlan(modelInformation.getModelId()));
      } catch (ConsensusException e) {
        LOGGER.warn("Failed in the write API executing the consensus layer due to: ", e);
      }
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
    ReadWriteIOUtils.write(hyperparameters, stream);
  }

  @Override
  public void deserialize(ByteBuffer byteBuffer) {
    super.deserialize(byteBuffer);
    modelInformation = ModelInformation.deserialize(byteBuffer);
    hyperparameters = ReadWriteIOUtils.readMap(byteBuffer);
  }

  @Override
  public boolean equals(Object that) {
    if (that instanceof CreateModelProcedure) {
      CreateModelProcedure thatProc = (CreateModelProcedure) that;
      return thatProc.getProcId() == this.getProcId()
          && thatProc.getState() == this.getState()
          && thatProc.modelInformation.equals(this.modelInformation)
          && thatProc.hyperparameters.equals(this.hyperparameters);
    }
    return false;
  }

  @Override
  public int hashCode() {
    return Objects.hash(getProcId(), getState(), modelInformation, hyperparameters);
  }
}
