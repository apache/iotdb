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

package org.apache.iotdb.confignode.manager.externalservice;

import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TExternalServiceEntry;
import org.apache.iotdb.common.rpc.thrift.TExternalServiceListResp;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.externalservice.ServiceInfo;
import org.apache.iotdb.confignode.client.async.CnToDnAsyncRequestType;
import org.apache.iotdb.confignode.client.async.CnToDnInternalServiceAsyncRequestManager;
import org.apache.iotdb.confignode.client.async.handlers.DataNodeAsyncRequestContext;
import org.apache.iotdb.confignode.consensus.request.read.exernalservice.ShowExternalServicePlan;
import org.apache.iotdb.confignode.consensus.request.write.externalservice.CreateExternalServicePlan;
import org.apache.iotdb.confignode.consensus.request.write.externalservice.DropExternalServicePlan;
import org.apache.iotdb.confignode.consensus.request.write.externalservice.StartExternalServicePlan;
import org.apache.iotdb.confignode.consensus.request.write.externalservice.StopExternalServicePlan;
import org.apache.iotdb.confignode.consensus.response.externalservice.ShowExternalServiceResp;
import org.apache.iotdb.confignode.manager.ConfigManager;
import org.apache.iotdb.confignode.rpc.thrift.TCreateExternalServiceReq;
import org.apache.iotdb.consensus.exception.ConsensusException;
import org.apache.iotdb.mpp.rpc.thrift.TCreateFunctionInstanceReq;
import org.apache.iotdb.rpc.TSStatusCode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkState;

public class ExternalServiceManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(ExternalServiceManager.class);

  private final ConfigManager configManager;

  public ExternalServiceManager(ConfigManager configManager) {
    this.configManager = configManager;
  }

  public TSStatus createService(TCreateExternalServiceReq req) {
    try {
      return configManager
          .getConsensusManager()
          .write(
              new CreateExternalServicePlan(
                  req.getDataNodeId(),
                  new ServiceInfo(
                      req.getServiceName(),
                      req.getClassName(),
                      ServiceInfo.ServiceType.USER_DEFINED)));
    } catch (ConsensusException e) {
      LOGGER.warn(
          "Unexpected error happened while creating Service {} on DataNode {}: ",
          req.getServiceName(),
          req.getDataNodeId(),
          e);
      // consensus layer related errors
      TSStatus res = new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode());
      res.setMessage(e.getMessage());
      return res;
    }
  }

  public TSStatus startService(int dataNodeId, String serviceName) {
    try {
      return configManager
          .getConsensusManager()
          .write(new StartExternalServicePlan(dataNodeId, serviceName));
    } catch (ConsensusException e) {
      LOGGER.warn(
          "Unexpected error happened while starting Service {} on DataNode {}: ",
          serviceName,
          dataNodeId,
          e);
      // consensus layer related errors
      TSStatus res = new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode());
      res.setMessage(e.getMessage());
      return res;
    }
  }

  public TSStatus stopService(int dataNodeId, String serviceName) {
    try {
      return configManager
          .getConsensusManager()
          .write(new StopExternalServicePlan(dataNodeId, serviceName));
    } catch (ConsensusException e) {
      LOGGER.warn(
          "Unexpected error happened while stopping Service {} on DataNode {}: ",
          serviceName,
          dataNodeId,
          e);
      // consensus layer related errors
      TSStatus res = new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode());
      res.setMessage(e.getMessage());
      return res;
    }
  }

  public TSStatus dropService(int dataNodeId, String serviceName) {
    try {
      return configManager
          .getConsensusManager()
          .write(new DropExternalServicePlan(dataNodeId, serviceName));
    } catch (ConsensusException e) {
      LOGGER.warn(
          "Unexpected error happened while dropping Service {} on DataNode {}: ",
          serviceName,
          dataNodeId,
          e);
      // consensus layer related errors
      TSStatus res = new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode());
      res.setMessage(e.getMessage());
      return res;
    }
  }

  public TExternalServiceListResp showService(int dataNodeId) {
    Map<Integer, TDataNodeLocation> targetDataNodes =
        configManager.getReadableDataNodeLocationMap();

    if (targetDataNodes.isEmpty()) {
      // no readable DN, return directly
      return new TExternalServiceListResp(
          new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode()), Collections.emptyList());
    }

    if (dataNodeId != -1) {
      if (!targetDataNodes.containsKey(dataNodeId)) {
        // target DN is not readable, return directly
        return new TExternalServiceListResp(
            new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode()), Collections.emptyList());
      } else {
        targetDataNodes = Collections.singletonMap(dataNodeId, targetDataNodes.get(dataNodeId));
      }
    }

    // 1. get built-in services info from DN
    Map<Integer, TExternalServiceListResp> builtInServiceInfos =
        getBuiltInServiceInfosFromDataNodes(targetDataNodes);
    if (builtInServiceInfos.isEmpty()) {
      return new TExternalServiceListResp(
          new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode()), Collections.emptyList());
    }

    try {
      // 2. get user-defined services info from CN consensus
      ShowExternalServiceResp response =
          (ShowExternalServiceResp)
              configManager
                  .getConsensusManager()
                  .read(new ShowExternalServicePlan(builtInServiceInfos.keySet()));

      // 3. combined built-in services info and user-defined services info
      builtInServiceInfos
          .values()
          .forEach(
              builtInResp ->
                  response.getServiceInfoEntryList().addAll(builtInResp.getExternalServiceInfos()));
      return response.convertToRpcShowExternalServiceResp();
    } catch (ConsensusException e) {
      LOGGER.warn("Unexpected error happened while showing Service: ", e);
      // consensus layer related errors
      TSStatus res = new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode());
      res.setMessage(e.getMessage());
      return new TExternalServiceListResp(res, Collections.emptyList());
    }
  }

  public List<TExternalServiceEntry> getUserDefinedService(int dataNodeId) {
    try {
      checkState(dataNodeId != -1, "dataNodeId should not be -1 here");

      ShowExternalServiceResp response =
          (ShowExternalServiceResp)
              configManager
                  .getConsensusManager()
                  .read(new ShowExternalServicePlan(Collections.singleton(dataNodeId)));
      return response.getServiceInfoEntryList();
    } catch (ConsensusException e) {
      LOGGER.warn("Unexpected error happened while getting user-defined Service: ", e);
      return Collections.emptyList();
    }
  }

  private Map<Integer, TExternalServiceListResp> getBuiltInServiceInfosFromDataNodes(
      Map<Integer, TDataNodeLocation> dataNodeLocationMap) {
    DataNodeAsyncRequestContext<TCreateFunctionInstanceReq, TExternalServiceListResp> context =
        new DataNodeAsyncRequestContext<>(
            CnToDnAsyncRequestType.GET_BUILTIN_SERVICE, dataNodeLocationMap);
    CnToDnInternalServiceAsyncRequestManager.getInstance().sendAsyncRequestWithRetry(context);
    return context.getResponseMap();
  }
}
