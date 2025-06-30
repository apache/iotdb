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

import org.apache.iotdb.ainode.rpc.thrift.TShowModelsResp;
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.client.ainode.AINodeClient;
import org.apache.iotdb.commons.client.ainode.AINodeClientManager;
import org.apache.iotdb.commons.model.ModelInformation;
import org.apache.iotdb.commons.model.ModelStatus;
import org.apache.iotdb.commons.model.ModelType;
import org.apache.iotdb.confignode.consensus.request.read.model.GetModelInfoPlan;
import org.apache.iotdb.confignode.consensus.request.write.model.CreateModelPlan;
import org.apache.iotdb.confignode.consensus.request.write.model.UpdateModelInfoPlan;
import org.apache.iotdb.confignode.consensus.response.model.GetModelInfoResp;
import org.apache.iotdb.confignode.persistence.ModelInfo;
import org.apache.iotdb.confignode.rpc.thrift.TAINodeInfo;
import org.apache.iotdb.confignode.rpc.thrift.TCreateModelReq;
import org.apache.iotdb.confignode.rpc.thrift.TDropModelReq;
import org.apache.iotdb.confignode.rpc.thrift.TGetModelInfoReq;
import org.apache.iotdb.confignode.rpc.thrift.TGetModelInfoResp;
import org.apache.iotdb.confignode.rpc.thrift.TShowModelReq;
import org.apache.iotdb.confignode.rpc.thrift.TShowModelResp;
import org.apache.iotdb.confignode.rpc.thrift.TUpdateModelInfoReq;
import org.apache.iotdb.consensus.exception.ConsensusException;
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
      return new TSStatus(TSStatusCode.MODEL_EXIST_ERROR.getStatusCode())
          .setMessage(String.format("Built-in model %s can't be removed", req.modelId));
    }
    if (!modelInfo.contain(req.modelId)) {
      return new TSStatus(TSStatusCode.MODEL_EXIST_ERROR.getStatusCode())
          .setMessage(String.format("Model name %s doesn't exists", req.modelId));
    }
    return configManager.getProcedureManager().dropModel(req.getModelId());
  }

  public TShowModelResp showModel(final TShowModelReq req) {
    List<TAINodeInfo> registeredAINodes =
        configManager.getNodeManager().getRegisteredAINodeInfoList();
    if (registeredAINodes.isEmpty()) {
      return new TShowModelResp()
          .setStatus(
              new TSStatus(TSStatusCode.NO_AVAILABLE_AINODE.getStatusCode())
                  .setMessage("Show models failed due to there is no AINode available"));
    }
    TAINodeInfo registeredAINode = registeredAINodes.get(0);
    TEndPoint targetAINodeEndPoint =
        new TEndPoint(registeredAINode.getInternalAddress(), registeredAINode.getInternalPort());
    try (AINodeClient client =
        AINodeClientManager.getInstance().borrowClient(targetAINodeEndPoint)) {
      TShowModelsResp resp = client.showModels();
      TShowModelResp res =
          new TShowModelResp().setStatus(new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode()));
      res.setModelIdList(resp.getModelIdList());
      res.setModelTypeMap(resp.getModelTypeMap());
      res.setCategoryMap(resp.getCategoryMap());
      res.setStateMap(resp.getStateMap());
      return res;
    } catch (Exception e) {
      LOGGER.warn("Failed to show models due to", e);
      return new TShowModelResp()
          .setStatus(
              new TSStatus(TSStatusCode.CAN_NOT_CONNECT_AINODE.getStatusCode())
                  .setMessage(e.getMessage()));
    }
  }

  public TGetModelInfoResp getModelInfo(TGetModelInfoReq req) {
    try {
      GetModelInfoResp response =
          (GetModelInfoResp) configManager.getConsensusManager().read(new GetModelInfoPlan(req));
      if (response.getStatus().getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        return new TGetModelInfoResp(response.getStatus());
      }
      int aiNodeId = response.getTargetAINodeId();
      if (aiNodeId != 0) {
        response.setTargetAINodeAddress(
            configManager.getNodeManager().getRegisteredAINode(aiNodeId));
      } else {
        if (configManager.getNodeManager().getRegisteredAINodes().isEmpty()) {
          return new TGetModelInfoResp(
              new TSStatus(TSStatusCode.GET_MODEL_INFO_ERROR.getStatusCode())
                  .setMessage("There is no AINode available"));
        }
        response.setTargetAINodeAddress(
            configManager.getNodeManager().getRegisteredAINodes().get(0));
      }
      return response.convertToThriftResponse();
    } catch (ConsensusException e) {
      LOGGER.warn("Unexpected error happened while getting model: ", e);
      // consensus layer related errors
      TSStatus res = new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode());
      res.setMessage(e.getMessage());
      return new TGetModelInfoResp(res);
    }
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

  public List<Integer> getModelDistributions(String modelName) {
    return modelInfo.getNodeIds(modelName);
  }
}
