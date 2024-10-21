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

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.confignode.consensus.request.read.model.GetModelInfoPlan;
import org.apache.iotdb.confignode.consensus.request.read.model.ShowModelPlan;
import org.apache.iotdb.confignode.consensus.response.model.GetModelInfoResp;
import org.apache.iotdb.confignode.consensus.response.model.ModelTableResp;
import org.apache.iotdb.confignode.persistence.ModelInfo;
import org.apache.iotdb.confignode.rpc.thrift.TCreateModelReq;
import org.apache.iotdb.confignode.rpc.thrift.TDropModelReq;
import org.apache.iotdb.confignode.rpc.thrift.TGetModelInfoReq;
import org.apache.iotdb.confignode.rpc.thrift.TGetModelInfoResp;
import org.apache.iotdb.confignode.rpc.thrift.TShowModelReq;
import org.apache.iotdb.confignode.rpc.thrift.TShowModelResp;
import org.apache.iotdb.consensus.common.DataSet;
import org.apache.iotdb.consensus.exception.ConsensusException;
import org.apache.iotdb.rpc.TSStatusCode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
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
    return configManager.getProcedureManager().createModel(req.modelName, req.uri);
  }

  public TSStatus dropModel(TDropModelReq req) {
    if (!modelInfo.contain(req.modelId)) {
      return new TSStatus(TSStatusCode.MODEL_EXIST_ERROR.getStatusCode())
          .setMessage(String.format("Model name %s doesn't exists", req.modelId));
    }
    return configManager.getProcedureManager().dropModel(req.getModelId());
  }

  public TShowModelResp showModel(final TShowModelReq req) {
    try {
      final DataSet response = configManager.getConsensusManager().read(new ShowModelPlan(req));
      return ((ModelTableResp) response).convertToThriftResponse();
    } catch (final ConsensusException e) {
      LOGGER.warn(
          String.format("Unexpected error happened while showing model %s: ", req.getModelId()), e);
      // consensus layer related errors
      final TSStatus res = new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode());
      res.setMessage(e.getMessage());
      return new TShowModelResp(res, Collections.emptyList());
    } catch (final IOException e) {
      LOGGER.warn("Fail to get ModelTable", e);
      return new TShowModelResp(
          new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode())
              .setMessage(e.getMessage()),
          Collections.emptyList());
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

  public List<Integer> getModelDistributions(String modelName) {
    return modelInfo.getNodeIds(modelName);
  }
}
