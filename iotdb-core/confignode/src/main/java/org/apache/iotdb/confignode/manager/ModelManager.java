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

package org.apache.iotdb.confignode.manager;

import org.apache.iotdb.ainode.rpc.thrift.TLoadModelReq;
import org.apache.iotdb.ainode.rpc.thrift.TShowAIDevicesResp;
import org.apache.iotdb.ainode.rpc.thrift.TShowLoadedModelsReq;
import org.apache.iotdb.ainode.rpc.thrift.TShowLoadedModelsResp;
import org.apache.iotdb.ainode.rpc.thrift.TShowModelsReq;
import org.apache.iotdb.ainode.rpc.thrift.TShowModelsResp;
import org.apache.iotdb.ainode.rpc.thrift.TUnloadModelReq;
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.client.exception.ClientManagerException;
import org.apache.iotdb.commons.model.ModelInformation;
import org.apache.iotdb.commons.model.ModelStatus;
import org.apache.iotdb.commons.model.ModelType;
import org.apache.iotdb.confignode.consensus.request.write.model.CreateModelPlan;
import org.apache.iotdb.confignode.consensus.request.write.model.UpdateModelInfoPlan;
import org.apache.iotdb.confignode.exception.NoAvailableAINodeException;
import org.apache.iotdb.confignode.persistence.ModelInfo;
import org.apache.iotdb.confignode.rpc.thrift.TAINodeInfo;
import org.apache.iotdb.confignode.rpc.thrift.TCreateModelReq;
import org.apache.iotdb.confignode.rpc.thrift.TDropModelReq;
import org.apache.iotdb.confignode.rpc.thrift.TGetModelInfoReq;
import org.apache.iotdb.confignode.rpc.thrift.TGetModelInfoResp;
import org.apache.iotdb.confignode.rpc.thrift.TUpdateModelInfoReq;
import org.apache.iotdb.consensus.exception.ConsensusException;
import org.apache.iotdb.db.protocol.client.ainode.AINodeClient;
import org.apache.iotdb.db.protocol.client.ainode.AINodeClientManager;
import org.apache.iotdb.rpc.TSStatusCode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class ModelManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(ModelManager.class);

  private final ConfigManager configManager;
  private final ModelInfo modelInfo;

  public ModelManager(ConfigManager configManager, ModelInfo modelInfo) {
    this.configManager = configManager;
    this.modelInfo = modelInfo;
  }

  public TSStatus createModel(TCreateModelReq req) {
    if (modelInfo.contain(req.modelName)) {
      return new TSStatus(TSStatusCode.MODEL_EXIST_ERROR.getStatusCode())
          .setMessage(String.format("Model name %s already exists", req.modelName));
    }
    try {
      if (req.uri.isEmpty()) {
        return configManager.getConsensusManager().write(new CreateModelPlan(req.modelName));
      }
      return configManager.getProcedureManager().createModel(req.modelName, req.uri);
    } catch (ConsensusException e) {
      LOGGER.warn("Unexpected error happened while getting model: ", e);
      // consensus layer related errors
      TSStatus res = new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode());
      res.setMessage(e.getMessage());
      return res;
    }
  }

  public TSStatus dropModel(TDropModelReq req) {
    if (modelInfo.checkModelType(req.getModelId()) != ModelType.USER_DEFINED) {
      return new TSStatus(TSStatusCode.DROP_MODEL_ERROR.getStatusCode())
          .setMessage(String.format("Built-in model %s can't be removed", req.modelId));
    }
    if (!modelInfo.contain(req.modelId)) {
      return new TSStatus(TSStatusCode.MODEL_EXIST_ERROR.getStatusCode())
          .setMessage(String.format("Model name %s doesn't exists", req.modelId));
    }
    return configManager.getProcedureManager().dropModel(req.getModelId());
  }

  public TSStatus loadModel(TLoadModelReq req) {
    try (AINodeClient client = getAINodeClient()) {
      org.apache.iotdb.ainode.rpc.thrift.TLoadModelReq loadModelReq =
          new org.apache.iotdb.ainode.rpc.thrift.TLoadModelReq(
              req.existingModelId, req.deviceIdList);
      return client.loadModel(loadModelReq);
    } catch (Exception e) {
      LOGGER.warn("Failed to load model due to", e);
      return new TSStatus(TSStatusCode.AI_NODE_INTERNAL_ERROR.getStatusCode())
          .setMessage(e.getMessage());
    }
  }

  public TSStatus unloadModel(TUnloadModelReq req) {
    try (AINodeClient client = getAINodeClient()) {
      org.apache.iotdb.ainode.rpc.thrift.TUnloadModelReq unloadModelReq =
          new org.apache.iotdb.ainode.rpc.thrift.TUnloadModelReq(req.modelId, req.deviceIdList);
      return client.unloadModel(unloadModelReq);
    } catch (Exception e) {
      LOGGER.warn("Failed to unload model due to", e);
      return new TSStatus(TSStatusCode.AI_NODE_INTERNAL_ERROR.getStatusCode())
          .setMessage(e.getMessage());
    }
  }

  public TShowModelsResp showModel(final TShowModelsReq req) {
    try (AINodeClient client = getAINodeClient()) {
      TShowModelsReq showModelsReq = new TShowModelsReq();
      if (req.isSetModelId()) {
        showModelsReq.setModelId(req.getModelId());
      }
      TShowModelsResp resp = client.showModels(showModelsReq);
      TShowModelsResp res =
          new TShowModelsResp()
              .setStatus(new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode()));
      res.setModelIdList(resp.getModelIdList());
      res.setModelTypeMap(resp.getModelTypeMap());
      res.setCategoryMap(resp.getCategoryMap());
      res.setStateMap(resp.getStateMap());
      return res;
    } catch (Exception e) {
      LOGGER.warn("Failed to show models due to", e);
      return new TShowModelsResp()
          .setStatus(
              new TSStatus(TSStatusCode.AI_NODE_INTERNAL_ERROR.getStatusCode())
                  .setMessage(e.getMessage()));
    }
  }

  public TShowLoadedModelsResp showLoadedModel(final TShowLoadedModelsReq req) {
    try (AINodeClient client = getAINodeClient()) {
      TShowLoadedModelsReq showModelsReq =
          new TShowLoadedModelsReq().setDeviceIdList(req.getDeviceIdList());
      TShowLoadedModelsResp resp = client.showLoadedModels(showModelsReq);
      TShowLoadedModelsResp res =
          new TShowLoadedModelsResp()
              .setStatus(new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode()));
      res.setDeviceLoadedModelsMap(resp.getDeviceLoadedModelsMap());
      return res;
    } catch (Exception e) {
      LOGGER.warn("Failed to show loaded models due to", e);
      return new TShowLoadedModelsResp()
          .setStatus(
              new TSStatus(TSStatusCode.AI_NODE_INTERNAL_ERROR.getStatusCode())
                  .setMessage(e.getMessage()));
    }
  }

  public TShowAIDevicesResp showAIDevices() {
    try (AINodeClient client = getAINodeClient()) {
      org.apache.iotdb.ainode.rpc.thrift.TShowAIDevicesResp resp = client.showAIDevices();
      TShowAIDevicesResp res =
          new TShowAIDevicesResp()
              .setStatus(new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode()));
      res.setDeviceIdList(resp.getDeviceIdList());
      return res;
    } catch (Exception e) {
      LOGGER.warn("Failed to show AI devices due to", e);
      return new TShowAIDevicesResp()
          .setStatus(
              new TSStatus(TSStatusCode.AI_NODE_INTERNAL_ERROR.getStatusCode())
                  .setMessage(e.getMessage()));
    }
  }

  public TGetModelInfoResp getModelInfo(TGetModelInfoReq req) {
    return new TGetModelInfoResp()
        .setStatus(new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode()))
        .setAiNodeAddress(
            configManager
                .getNodeManager()
                .getRegisteredAINodes()
                .get(0)
                .getLocation()
                .getInternalEndPoint());
  }

  // Currently this method is only used by built-in timer_xl
  public TSStatus updateModelInfo(TUpdateModelInfoReq req) {
    if (!modelInfo.contain(req.getModelId())) {
      return new TSStatus(TSStatusCode.MODEL_NOT_FOUND_ERROR.getStatusCode())
          .setMessage(String.format("Model %s doesn't exists", req.getModelId()));
    }
    try {
      ModelInformation modelInformation =
          new ModelInformation(ModelType.USER_DEFINED, req.getModelId());
      modelInformation.updateStatus(ModelStatus.values()[req.getModelStatus()]);
      modelInformation.setAttribute(req.getAttributes());
      modelInformation.setInputColumnSize(1);
      if (req.isSetOutputLength()) {
        modelInformation.setOutputLength(req.getOutputLength());
      }
      if (req.isSetInputLength()) {
        modelInformation.setInputLength(req.getInputLength());
      }
      UpdateModelInfoPlan updateModelInfoPlan =
          new UpdateModelInfoPlan(req.getModelId(), modelInformation);
      if (req.isSetAiNodeIds()) {
        updateModelInfoPlan.setNodeIds(req.getAiNodeIds());
      }
      return configManager.getConsensusManager().write(updateModelInfoPlan);
    } catch (ConsensusException e) {
      LOGGER.warn("Unexpected error happened while updating model info: ", e);
      // consensus layer related errors
      TSStatus res = new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode());
      res.setMessage(e.getMessage());
      return res;
    }
  }

  private AINodeClient getAINodeClient() throws NoAvailableAINodeException, ClientManagerException {
    List<TAINodeInfo> aiNodeInfo = configManager.getNodeManager().getRegisteredAINodeInfoList();
    if (aiNodeInfo.isEmpty()) {
      throw new NoAvailableAINodeException();
    }
    TEndPoint targetAINodeEndPoint =
        new TEndPoint(aiNodeInfo.get(0).getInternalAddress(), aiNodeInfo.get(0).getInternalPort());
    try {
      return AINodeClientManager.getInstance().borrowClient(targetAINodeEndPoint);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public List<Integer> getModelDistributions(String modelName) {
    return modelInfo.getNodeIds(modelName);
  }
}
