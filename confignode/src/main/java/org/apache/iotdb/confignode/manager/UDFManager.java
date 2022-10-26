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
import org.apache.iotdb.commons.udf.UDFInformation;
import org.apache.iotdb.confignode.client.DataNodeRequestType;
import org.apache.iotdb.confignode.client.async.AsyncDataNodeClientPool;
import org.apache.iotdb.confignode.client.async.handlers.AsyncClientHandler;
import org.apache.iotdb.confignode.consensus.request.write.function.CreateFunctionPlan;
import org.apache.iotdb.confignode.consensus.request.write.function.DropFunctionPlan;
import org.apache.iotdb.confignode.persistence.UDFInfo;
import org.apache.iotdb.confignode.rpc.thrift.TCreateFunctionReq;
import org.apache.iotdb.mpp.rpc.thrift.TCreateFunctionInstanceReq;
import org.apache.iotdb.mpp.rpc.thrift.TDropFunctionInstanceReq;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.tsfile.utils.Binary;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
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

  public TSStatus createFunction(TCreateFunctionReq req) {
    udfInfo.acquireUDFTableLock();
    try {
      final String udfName = req.udfName.toUpperCase(),
          jarName = req.getJarName(),
          jarMD5 = req.jarMD5;
      final byte[] jarFile = req.getJarFile();
      udfInfo.validate(udfName, jarName, jarMD5);

      final UDFInformation udfInformation =
          new UDFInformation(udfName, req.getClassName(), false, jarName, jarMD5);

      LOGGER.info("Start to create UDF [{}] on Data Nodes", udfName);

      final TSStatus dataNodesStatus =
          RpcUtils.squashResponseStatusList(
              createFunctionOnDataNodes(udfInformation, req.getJarFile()));
      if (dataNodesStatus.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        return dataNodesStatus;
      }

      final boolean needToSaveJar = udfInfo.needToSaveJar(jarName);

      LOGGER.info(
          "Start to add UDF [{}] in UDF_Table on Config Nodes, needToSaveJar[{}]",
          udfName,
          needToSaveJar);

      return configManager
          .getConsensusManager()
          .write(new CreateFunctionPlan(udfInformation, needToSaveJar ? new Binary(jarFile) : null))
          .getStatus();
    } catch (Exception e) {
      LOGGER.warn(e.getMessage(), e);
      return new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode())
          .setMessage(e.getMessage());
    } finally {
      udfInfo.releaseUDFTableLock();
    }
  }

  private List<TSStatus> createFunctionOnDataNodes(UDFInformation udfInformation, byte[] jarFile)
      throws IOException {
    final Map<Integer, TDataNodeLocation> dataNodeLocationMap =
        configManager.getNodeManager().getRegisteredDataNodeLocations();
    final TCreateFunctionInstanceReq req =
        new TCreateFunctionInstanceReq(udfInformation.serialize(), ByteBuffer.wrap(jarFile));
    AsyncClientHandler<TCreateFunctionInstanceReq, TSStatus> clientHandler =
        new AsyncClientHandler<>(DataNodeRequestType.CREATE_FUNCTION, req, dataNodeLocationMap);
    AsyncDataNodeClientPool.getInstance().sendAsyncRequestToDataNodeWithRetry(clientHandler);
    return clientHandler.getResponseList();
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
    final TDropFunctionInstanceReq request = new TDropFunctionInstanceReq(functionName);

    AsyncClientHandler<TDropFunctionInstanceReq, TSStatus> clientHandler =
        new AsyncClientHandler<>(DataNodeRequestType.DROP_FUNCTION, request, dataNodeLocationMap);
    AsyncDataNodeClientPool.getInstance().sendAsyncRequestToDataNodeWithRetry(clientHandler);
    return clientHandler.getResponseList();
  }
}
