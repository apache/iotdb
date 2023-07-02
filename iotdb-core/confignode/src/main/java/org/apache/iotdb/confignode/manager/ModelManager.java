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
import org.apache.iotdb.common.rpc.thrift.TaskType;
import org.apache.iotdb.commons.model.ForecastModeInformation;
import org.apache.iotdb.commons.model.ModelInformation;
import org.apache.iotdb.confignode.consensus.request.read.model.GetModelInfoPlan;
import org.apache.iotdb.confignode.consensus.request.read.model.ShowModelPlan;
import org.apache.iotdb.confignode.consensus.request.read.model.ShowTrailPlan;
import org.apache.iotdb.confignode.consensus.request.write.model.UpdateModelInfoPlan;
import org.apache.iotdb.confignode.consensus.request.write.model.UpdateModelStatePlan;
import org.apache.iotdb.confignode.consensus.response.model.GetModelInfoResp;
import org.apache.iotdb.confignode.consensus.response.model.ModelTableResp;
import org.apache.iotdb.confignode.consensus.response.model.TrailTableResp;
import org.apache.iotdb.confignode.persistence.ModelInfo;
import org.apache.iotdb.confignode.rpc.thrift.TCreateModelReq;
import org.apache.iotdb.confignode.rpc.thrift.TDropModelReq;
import org.apache.iotdb.confignode.rpc.thrift.TGetModelInfoReq;
import org.apache.iotdb.confignode.rpc.thrift.TGetModelInfoResp;
import org.apache.iotdb.confignode.rpc.thrift.TShowModelReq;
import org.apache.iotdb.confignode.rpc.thrift.TShowModelResp;
import org.apache.iotdb.confignode.rpc.thrift.TShowTrailReq;
import org.apache.iotdb.confignode.rpc.thrift.TShowTrailResp;
import org.apache.iotdb.confignode.rpc.thrift.TUpdateModelInfoReq;
import org.apache.iotdb.confignode.rpc.thrift.TUpdateModelStateReq;
import org.apache.iotdb.consensus.common.response.ConsensusReadResponse;
import org.apache.iotdb.consensus.common.response.ConsensusWriteResponse;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.iotdb.commons.model.ForecastModeInformation.DEFAULT_INPUT_LENGTH;
import static org.apache.iotdb.commons.model.ForecastModeInformation.DEFAULT_PREDICT_LENGTH;
import static org.apache.iotdb.commons.model.ForecastModeInformation.INPUT_LENGTH;
import static org.apache.iotdb.commons.model.ForecastModeInformation.INPUT_TYPE_LIST;
import static org.apache.iotdb.commons.model.ForecastModeInformation.PREDICT_INDEX_LIST;
import static org.apache.iotdb.commons.model.ForecastModeInformation.PREDICT_LENGTH;

public class ModelManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(ModelManager.class);

  private final ConfigManager configManager;
  private final ModelInfo modelInfo;

  public ModelManager(ConfigManager configManager, ModelInfo modelInfo) {
    this.configManager = configManager;
    this.modelInfo = modelInfo;
  }

  public ModelInfo getModelInfo() {
    return modelInfo;
  }

  public TSStatus createModel(TCreateModelReq req) {
    TaskType taskType = req.getTaskType();

    Map<String, String> options = req.getOptions();
    ModelInformation modelInformation;
    if (taskType == TaskType.FORECAST) {
      String inputTypeListStr = options.get(INPUT_TYPE_LIST);
      List<TSDataType> inputTypeList =
          Arrays.stream(inputTypeListStr.split(","))
              .sequential()
              .map(s -> TSDataType.valueOf(s.toUpperCase()))
              .collect(Collectors.toList());

      String predictIndexListStr = options.get(PREDICT_INDEX_LIST);
      List<Integer> predictIndexList =
          Arrays.stream(predictIndexListStr.split(","))
              .sequential()
              .map(Integer::valueOf)
              .collect(Collectors.toList());

      modelInformation =
          new ForecastModeInformation(
              req.getModelId(),
              req.getOptions(),
              req.getDatasetFetchSQL(),
              inputTypeList,
              predictIndexList,
              Integer.parseInt(options.getOrDefault(INPUT_LENGTH, DEFAULT_INPUT_LENGTH)),
              Integer.parseInt(options.getOrDefault(PREDICT_LENGTH, DEFAULT_PREDICT_LENGTH)));
    } else {
      throw new IllegalArgumentException("Invalid task type: " + taskType);
    }
    return configManager
        .getProcedureManager()
        .createModel(modelInformation, req.getHyperparameters());
  }

  public TSStatus dropModel(TDropModelReq req) {
    return configManager.getProcedureManager().dropModel(req.getModelId());
  }

  public TSStatus updateModelInfo(TUpdateModelInfoReq req) {
    ConsensusWriteResponse response =
        configManager.getConsensusManager().write(new UpdateModelInfoPlan(req));
    if (response.getStatus() != null) {
      return response.getStatus();
    } else {
      LOGGER.warn(
          "Unexpected error happened while updating model {}: ",
          req.getModelId(),
          response.getException());
      // consensus layer related errors
      TSStatus res = new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode());
      res.setMessage(response.getErrorMessage());
      return res;
    }
  }

  public TSStatus updateModelState(TUpdateModelStateReq req) {
    ConsensusWriteResponse response =
        configManager.getConsensusManager().write(new UpdateModelStatePlan(req));
    if (response.getStatus() != null) {
      return response.getStatus();
    } else {
      LOGGER.warn(
          "Unexpected error happened while updating state of model {}: ",
          req.getModelId(),
          response.getException());
      // consensus layer related errors
      TSStatus res = new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode());
      res.setMessage(response.getErrorMessage());
      return res;
    }
  }

  public TShowModelResp showModel(TShowModelReq req) {
    try {
      ConsensusReadResponse response =
          configManager.getConsensusManager().read(new ShowModelPlan(req));
      if (response.getDataset() != null) {
        return ((ModelTableResp) response.getDataset()).convertToThriftResponse();
      } else {
        LOGGER.warn("Unexpected error happened while showing model: ", response.getException());
        // consensus layer related errors
        TSStatus res = new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode());
        res.setMessage(response.getException().toString());
        return new TShowModelResp(res, Collections.emptyList());
      }
    } catch (IOException e) {
      LOGGER.warn("Fail to get ModelTable", e);
      return new TShowModelResp(
          new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode())
              .setMessage(e.getMessage()),
          Collections.emptyList());
    }
  }

  public TShowTrailResp showTrail(TShowTrailReq req) {
    try {
      ConsensusReadResponse response =
          configManager.getConsensusManager().read(new ShowTrailPlan(req));
      if (response.getDataset() != null) {
        return ((TrailTableResp) response.getDataset()).convertToThriftResponse();
      } else {
        LOGGER.warn("Unexpected error happened while showing trail: ", response.getException());
        // consensus layer related errors
        TSStatus res = new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode());
        res.setMessage(response.getException().toString());
        return new TShowTrailResp(res, Collections.emptyList());
      }
    } catch (IOException e) {
      LOGGER.warn("Fail to get TrailTable", e);
      return new TShowTrailResp(
          new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode())
              .setMessage(e.getMessage()),
          Collections.emptyList());
    }
  }

  public TGetModelInfoResp getModelInfo(TGetModelInfoReq req) {
    ConsensusReadResponse response =
        configManager.getConsensusManager().read(new GetModelInfoPlan(req));
    if (response.getDataset() != null) {
      return ((GetModelInfoResp) response.getDataset()).convertToThriftResponse();
    } else {
      LOGGER.warn("Unexpected error happened while getting model: ", response.getException());
      // consensus layer related errors
      TSStatus res = new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode());
      res.setMessage(response.getException().toString());
      return new TGetModelInfoResp(res, null);
    }
  }
}
