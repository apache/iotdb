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

import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.confignode.client.DataNodeRequestType;
import org.apache.iotdb.confignode.client.async.AsyncDataNodeClientPool;
import org.apache.iotdb.confignode.consensus.request.write.CreateFunctionPlan;
import org.apache.iotdb.confignode.consensus.request.write.DropFunctionPlan;
import org.apache.iotdb.confignode.persistence.UDFInfo;
import org.apache.iotdb.mpp.rpc.thrift.TCreateFunctionRequest;
import org.apache.iotdb.mpp.rpc.thrift.TDropFunctionRequest;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class UDFManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(UDFManager.class);

  private final ConfigManager configManager;
  private final UDFInfo udfInfo;

  public UDFManager(ConfigManager configManager, UDFInfo udfInfo) {
    this.configManager = configManager;
    this.udfInfo = udfInfo;
  }

  public TSStatus createFunction(String functionName, String className, List<String> uris) {
    try {
      udfInfo.validateBeforeRegistration(functionName, className, uris);

      final TSStatus configNodeStatus =
          configManager
              .getConsensusManager()
              .write(new CreateFunctionPlan(functionName, className, uris))
              .getStatus();
      if (configNodeStatus.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        return configNodeStatus;
      }

      return RpcUtils.squashResponseStatusList(
          createFunctionOnDataNodes(functionName, className, uris));
    } catch (Exception e) {
      final String errorMessage =
          String.format(
              "Failed to register UDF %s(class name: %s, uris: %s), because of exception: %s",
              functionName, className, uris, e);
      LOGGER.warn(errorMessage, e);
      return new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode())
          .setMessage(errorMessage);
    }
  }

  private List<TSStatus> createFunctionOnDataNodes(
      String functionName, String className, List<String> uris) {
    final Map<Integer, TDataNodeLocation> dataNodeLocationMap =
        configManager.getNodeManager().getRegisteredDataNodeLocations();
    final List<TSStatus> dataNodeResponseStatus =
        Collections.synchronizedList(new ArrayList<>(dataNodeLocationMap.size()));
    final TCreateFunctionRequest request =
        new TCreateFunctionRequest(functionName, className, uris);
    AsyncDataNodeClientPool.getInstance()
        .sendAsyncRequestToDataNodeWithRetry(
            request, dataNodeLocationMap, DataNodeRequestType.CREATE_FUNCTION);
    return dataNodeResponseStatus;
  }

  public TSStatus dropFunction(String functionName) {
    try {
      final List<TSStatus> nodeResponseList = dropFunctionOnDataNodes(functionName);
      final TSStatus configNodeStatus =
          configManager.getConsensusManager().write(new DropFunctionPlan(functionName)).getStatus();
      nodeResponseList.add(configNodeStatus);
      return RpcUtils.squashResponseStatusList(nodeResponseList);
    } catch (Exception e) {
      final String errorMessage =
          String.format("Failed to deregister UDF %s, because of exception: %s", functionName, e);
      LOGGER.warn(errorMessage, e);
      return new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode())
          .setMessage(errorMessage);
    }
  }

  private List<TSStatus> dropFunctionOnDataNodes(String functionName) {
    final Map<Integer, TDataNodeLocation> dataNodeLocationMap =
        configManager.getNodeManager().getRegisteredDataNodeLocations();
    final List<TSStatus> dataNodeResponseStatus =
        Collections.synchronizedList(new ArrayList<>(dataNodeLocationMap.size()));
    final TDropFunctionRequest request = new TDropFunctionRequest(functionName);
    AsyncDataNodeClientPool.getInstance()
        .sendAsyncRequestToDataNodeWithRetry(
            request, dataNodeLocationMap, DataNodeRequestType.DROP_FUNCTION);
    return dataNodeResponseStatus;
  }
}
