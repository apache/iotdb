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

import org.apache.iotdb.common.rpc.thrift.TAINodeConfiguration;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.client.ainode.AINodeClient;
import org.apache.iotdb.commons.client.ainode.AINodeClientManager;
import org.apache.iotdb.commons.exception.ainode.LoadModelException;
import org.apache.iotdb.commons.model.ModelInformation;
import org.apache.iotdb.commons.model.ModelStatus;
import org.apache.iotdb.commons.model.exception.ModelManagementException;
import org.apache.iotdb.confignode.consensus.request.write.model.CreateModelPlan;
import org.apache.iotdb.confignode.consensus.request.write.model.UpdateModelInfoPlan;
import org.apache.iotdb.confignode.manager.ConfigManager;
import org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv;
import org.apache.iotdb.confignode.procedure.exception.ProcedureException;
import org.apache.iotdb.confignode.procedure.impl.node.AbstractNodeProcedure;
import org.apache.iotdb.confignode.procedure.state.model.CreateModelState;
import org.apache.iotdb.confignode.procedure.store.ProcedureType;
import org.apache.iotdb.consensus.exception.ConsensusException;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.tsfile.utils.ReadWriteIOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class CreateModelProcedure extends AbstractNodeProcedure<CreateModelState> {

  private static final Logger LOGGER = LoggerFactory.getLogger(CreateModelProcedure.class);
  private static final int RETRY_THRESHOLD = 0;

  private String modelName;

  private String uri;

  private ModelInformation modelInformation = null;

  private List<Integer> aiNodeIds;

  private String loadErrorMsg = "";

  public CreateModelProcedure() {
    super();
  }

  public CreateModelProcedure(String modelName, String uri) {
    super();
    this.modelName = modelName;
    this.uri = uri;
    this.aiNodeIds = new ArrayList<>();
  }

  @Override
  protected Flow executeFromState(ConfigNodeProcedureEnv env, CreateModelState state) {
    if (modelName == null || uri == null) {
      return Flow.NO_MORE_STATE;
    }
    try {
      switch (state) {
        case LOADING:
          initModel(env);
          loadModel(env);
          setNextState(CreateModelState.ACTIVE);
          break;
        case ACTIVE:
          modelInformation.updateStatus(ModelStatus.ACTIVE);
          updateModel(env);
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
            "Retrievable error trying to create model [{}], state [{}]", modelName, state, e);
        if (getCycles() > RETRY_THRESHOLD) {
          modelInformation = new ModelInformation(modelName, ModelStatus.UNAVAILABLE);
          modelInformation.setAttribute(loadErrorMsg);
          updateModel(env);
          setFailure(
              new ProcedureException(
                  String.format("Fail to create model [%s] at STATE [%s]", modelName, state)));
        }
      }
    }
    return Flow.HAS_MORE_STATE;
  }

  private void initModel(ConfigNodeProcedureEnv env) throws ConsensusException {
    LOGGER.info("Start to add model [{}]", modelName);

    ConfigManager configManager = env.getConfigManager();
    TSStatus response = configManager.getConsensusManager().write(new CreateModelPlan(modelName));
    if (response.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      throw new ModelManagementException(
          String.format(
              "Failed to add model [%s] in ModelTable on Config Nodes: %s",
              modelName, response.getMessage()));
    }
  }

  private void checkModelInformationEquals(ModelInformation receiveModelInfo) {
    if (modelInformation == null) {
      modelInformation = receiveModelInfo;
    } else {
      if (!modelInformation.equals(receiveModelInfo)) {
        throw new ModelManagementException(
            String.format(
                "Failed to load model [%s] on AI Nodes, model information is not equal in different nodes",
                modelName));
      }
    }
  }

  private void loadModel(ConfigNodeProcedureEnv env) {
    for (TAINodeConfiguration curNodeConfig :
        env.getConfigManager().getNodeManager().getRegisteredAINodes()) {
      try (AINodeClient client =
          AINodeClientManager.getInstance()
              .borrowClient(curNodeConfig.getLocation().getInternalEndPoint())) {
        ModelInformation resp = client.registerModel(modelName, uri);
        checkModelInformationEquals(resp);
        aiNodeIds.add(curNodeConfig.getLocation().aiNodeId);
      } catch (LoadModelException e) {
        LOGGER.warn(e.getMessage());
        loadErrorMsg = e.getMessage();
      } catch (Exception e) {
        LOGGER.warn(
            "Failed to load model on AINode {} from ConfigNode",
            curNodeConfig.getLocation().getInternalEndPoint());
        loadErrorMsg = e.getMessage();
      }
    }

    if (aiNodeIds.isEmpty()) {
      throw new ModelManagementException(
          String.format("CREATE MODEL [%s] failed on all AINodes:[%s]", modelName, loadErrorMsg));
    }
  }

  private void updateModel(ConfigNodeProcedureEnv env) {
    LOGGER.info("Start to update model [{}]", modelName);

    ConfigManager configManager = env.getConfigManager();
    try {
      TSStatus response =
          configManager
              .getConsensusManager()
              .write(new UpdateModelInfoPlan(modelName, modelInformation, aiNodeIds));
      if (response.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        throw new ModelManagementException(
            String.format(
                "Failed to update model [%s] in ModelTable on Config Nodes: %s",
                modelName, response.getMessage()));
      }
    } catch (Exception e) {
      throw new ModelManagementException(
          String.format(
              "Failed to update model [%s] in ModelTable on Config Nodes: %s",
              modelName, e.getMessage()));
    }
  }

  @Override
  protected void rollbackState(ConfigNodeProcedureEnv env, CreateModelState state)
      throws IOException, InterruptedException, ProcedureException {
    // do nothing
  }

  @Override
  protected boolean isRollbackSupported(CreateModelState state) {
    return false;
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
    return CreateModelState.LOADING;
  }

  @Override
  public void serialize(DataOutputStream stream) throws IOException {
    stream.writeShort(ProcedureType.CREATE_MODEL_PROCEDURE.getTypeCode());
    super.serialize(stream);
    ReadWriteIOUtils.write(modelName, stream);
    ReadWriteIOUtils.write(uri, stream);
  }

  @Override
  public void deserialize(ByteBuffer byteBuffer) {
    super.deserialize(byteBuffer);
    modelName = ReadWriteIOUtils.readString(byteBuffer);
    uri = ReadWriteIOUtils.readString(byteBuffer);
  }

  @Override
  public boolean equals(Object that) {
    if (that instanceof CreateModelProcedure) {
      CreateModelProcedure thatProc = (CreateModelProcedure) that;
      return thatProc.getProcId() == this.getProcId()
          && thatProc.getState() == this.getState()
          && Objects.equals(thatProc.modelName, this.modelName)
          && Objects.equals(thatProc.uri, this.uri);
    }
    return false;
  }

  @Override
  public int hashCode() {
    return Objects.hash(getProcId(), getState(), modelName, uri);
  }
}
