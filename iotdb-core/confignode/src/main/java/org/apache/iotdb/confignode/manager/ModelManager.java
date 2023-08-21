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
import org.apache.iotdb.commons.model.ModelInformation;
import org.apache.iotdb.confignode.consensus.request.read.model.ShowModelPlan;
import org.apache.iotdb.confignode.consensus.request.read.model.ShowTrailPlan;
import org.apache.iotdb.confignode.consensus.request.write.model.UpdateModelInfoPlan;
import org.apache.iotdb.confignode.consensus.request.write.model.UpdateModelStatePlan;
import org.apache.iotdb.confignode.consensus.response.ModelTableResp;
import org.apache.iotdb.confignode.consensus.response.TrailTableResp;
import org.apache.iotdb.confignode.persistence.ModelInfo;
import org.apache.iotdb.confignode.rpc.thrift.TCreateModelReq;
import org.apache.iotdb.confignode.rpc.thrift.TDropModelReq;
import org.apache.iotdb.confignode.rpc.thrift.TShowModelReq;
import org.apache.iotdb.confignode.rpc.thrift.TShowModelResp;
import org.apache.iotdb.confignode.rpc.thrift.TShowTrailReq;
import org.apache.iotdb.confignode.rpc.thrift.TShowTrailResp;
import org.apache.iotdb.confignode.rpc.thrift.TUpdateModelInfoReq;
import org.apache.iotdb.confignode.rpc.thrift.TUpdateModelStateReq;
import org.apache.iotdb.consensus.common.DataSet;
import org.apache.iotdb.consensus.exception.ConsensusException;
import org.apache.iotdb.rpc.TSStatusCode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;

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
    ModelInformation modelInformation =
        new ModelInformation(
            req.getModelId(),
            req.getModelTask(),
            req.getModelType(),
            req.isIsAuto(),
            req.getQueryExpressions(),
            req.getQueryFilter());
    return configManager.getProcedureManager().createModel(modelInformation, req.getModelConfigs());
  }

  public TSStatus dropModel(TDropModelReq req) {
    return configManager.getProcedureManager().dropModel(req.getModelId());
  }

  public TSStatus updateModelInfo(TUpdateModelInfoReq req) {
    try {
      return configManager.getConsensusManager().write(new UpdateModelInfoPlan(req));
    } catch (ConsensusException e) {
      LOGGER.warn(
          String.format("Unexpected error happened while updating model %s: ", req.getModelId()),
          e);
      // consensus layer related errors
      TSStatus res = new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode());
      res.setMessage(e.getMessage());
      return res;
    }
  }

  public TSStatus updateModelState(TUpdateModelStateReq req) {
    try {
      return configManager.getConsensusManager().write(new UpdateModelStatePlan(req));
    } catch (ConsensusException e) {
      LOGGER.warn(
          String.format(
              "Unexpected error happened while updating state of model %s: ", req.getModelId()),
          e);
      // consensus layer related errors
      TSStatus res = new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode());
      res.setMessage(e.getMessage());
      return res;
    }
  }

  public TShowModelResp showModel(TShowModelReq req) {
    try {
      DataSet response = configManager.getConsensusManager().read(new ShowModelPlan(req));
      return ((ModelTableResp) response).convertToThriftResponse();
    } catch (ConsensusException e) {
      LOGGER.warn(
          String.format("Unexpected error happened while showing model %s: ", req.getModelId()), e);
      // consensus layer related errors
      TSStatus res = new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode());
      res.setMessage(e.getMessage());
      return new TShowModelResp(res, Collections.emptyList());
    } catch (IOException e) {
      LOGGER.error("Fail to get ModelTable", e);
      return new TShowModelResp(
          new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode())
              .setMessage(e.getMessage()),
          Collections.emptyList());
    }
  }

  public TShowTrailResp showTrail(TShowTrailReq req) {
    try {
      DataSet response = configManager.getConsensusManager().read(new ShowTrailPlan(req));
      return ((TrailTableResp) response).convertToThriftResponse();
    } catch (ConsensusException e) {
      LOGGER.warn(
          String.format("Unexpected error happened while showing trail %s: ", req.getModelId()), e);
      // consensus layer related errors
      TSStatus res = new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode());
      res.setMessage(e.getMessage());
      return new TShowTrailResp(res, Collections.emptyList());
    } catch (IOException e) {
      LOGGER.error("Fail to get TrailTable", e);
      return new TShowTrailResp(
          new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode())
              .setMessage(e.getMessage()),
          Collections.emptyList());
    }
  }
}
