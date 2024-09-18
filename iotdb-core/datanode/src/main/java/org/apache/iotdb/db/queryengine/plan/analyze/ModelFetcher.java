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

package org.apache.iotdb.db.queryengine.plan.analyze;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.client.IClientManager;
import org.apache.iotdb.commons.client.exception.ClientManagerException;
import org.apache.iotdb.commons.consensus.ConfigRegionId;
import org.apache.iotdb.commons.model.ModelInformation;
import org.apache.iotdb.confignode.rpc.thrift.TGetModelInfoReq;
import org.apache.iotdb.confignode.rpc.thrift.TGetModelInfoResp;
import org.apache.iotdb.db.exception.ainode.ModelNotFoundException;
import org.apache.iotdb.db.exception.sql.StatementAnalyzeException;
import org.apache.iotdb.db.protocol.client.ConfigNodeClient;
import org.apache.iotdb.db.protocol.client.ConfigNodeClientManager;
import org.apache.iotdb.db.protocol.client.ConfigNodeInfo;
import org.apache.iotdb.db.queryengine.plan.planner.plan.parameter.model.ModelInferenceDescriptor;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.thrift.TException;

public class ModelFetcher implements IModelFetcher {

  private final IClientManager<ConfigRegionId, ConfigNodeClient> configNodeClientManager =
      ConfigNodeClientManager.getInstance();

  private static final class ModelFetcherHolder {

    private static final ModelFetcher INSTANCE = new ModelFetcher();

    private ModelFetcherHolder() {}
  }

  public static ModelFetcher getInstance() {
    return ModelFetcherHolder.INSTANCE;
  }

  private ModelFetcher() {}

  @Override
  public TSStatus fetchModel(String modelName, Analysis analysis) {
    try (ConfigNodeClient client =
        configNodeClientManager.borrowClient(ConfigNodeInfo.CONFIG_REGION_ID)) {
      TGetModelInfoResp getModelInfoResp = client.getModelInfo(new TGetModelInfoReq(modelName));
      if (getModelInfoResp.getStatus().getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        if (getModelInfoResp.modelInfo != null && getModelInfoResp.isSetAiNodeAddress()) {
          analysis.setModelInferenceDescriptor(
              new ModelInferenceDescriptor(
                  getModelInfoResp.aiNodeAddress,
                  ModelInformation.deserialize(getModelInfoResp.modelInfo)));
          return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
        } else {
          TSStatus status = new TSStatus(TSStatusCode.GET_MODEL_INFO_ERROR.getStatusCode());
          status.setMessage(String.format("model [%s] is not available", modelName));
          return status;
        }
      } else {
        throw new ModelNotFoundException(getModelInfoResp.getStatus().getMessage());
      }
    } catch (ClientManagerException | TException e) {
      throw new StatementAnalyzeException(e.getMessage());
    }
  }
}
