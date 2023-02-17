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
import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.udf.UDFInformation;
import org.apache.iotdb.confignode.client.DataNodeRequestType;
import org.apache.iotdb.confignode.client.async.AsyncDataNodeClientPool;
import org.apache.iotdb.confignode.client.async.handlers.AsyncClientHandler;
import org.apache.iotdb.confignode.conf.ConfigNodeDescriptor;
import org.apache.iotdb.confignode.consensus.request.read.function.GetFunctionTablePlan;
import org.apache.iotdb.confignode.consensus.request.read.udf.GetUDFJarPlan;
import org.apache.iotdb.confignode.consensus.request.write.function.CreateFunctionPlan;
import org.apache.iotdb.confignode.consensus.request.write.function.DropFunctionPlan;
import org.apache.iotdb.confignode.consensus.response.function.FunctionTableResp;
import org.apache.iotdb.confignode.consensus.response.udf.JarResp;
import org.apache.iotdb.confignode.persistence.UDFInfo;
import org.apache.iotdb.confignode.rpc.thrift.TCreateFunctionReq;
import org.apache.iotdb.confignode.rpc.thrift.TGetJarInListReq;
import org.apache.iotdb.confignode.rpc.thrift.TGetJarInListResp;
import org.apache.iotdb.confignode.rpc.thrift.TGetUDFTableResp;
import org.apache.iotdb.mpp.rpc.thrift.TCreateFunctionInstanceReq;
import org.apache.iotdb.mpp.rpc.thrift.TDropFunctionInstanceReq;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.tsfile.utils.Binary;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class UDFManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(UDFManager.class);

  private final ConfigManager configManager;
  private final UDFInfo udfInfo;

  private final long planSizeLimit =
      ConfigNodeDescriptor.getInstance()
              .getConf()
              .getConfigNodeRatisConsensusLogAppenderBufferSize()
          - IoTDBConstant.RAFT_LOG_BASIC_SIZE;

  public UDFManager(ConfigManager configManager, UDFInfo udfInfo) {
    this.configManager = configManager;
    this.udfInfo = udfInfo;
  }

  public UDFInfo getUdfInfo() {
    return udfInfo;
  }

  public TSStatus createFunction(TCreateFunctionReq req) {
    udfInfo.acquireUDFTableLock();
    try {
      final boolean isUsingURI = req.isIsUsingURI();
      final String udfName = req.udfName.toUpperCase(),
          jarMD5 = req.getJarMD5(),
          jarName = req.getJarName();
      final byte[] jarFile = req.getJarFile();
      udfInfo.validate(udfName, jarName, jarMD5);

      final UDFInformation udfInformation =
          new UDFInformation(udfName, req.getClassName(), false, isUsingURI, jarName, jarMD5);
      final boolean needToSaveJar = isUsingURI && udfInfo.needToSaveJar(jarName);

      LOGGER.info(
          "Start to create UDF [{}] on Data Nodes, needToSaveJar[{}]", udfName, needToSaveJar);

      final TSStatus dataNodesStatus =
          RpcUtils.squashResponseStatusList(
              createFunctionOnDataNodes(udfInformation, needToSaveJar ? jarFile : null));
      if (dataNodesStatus.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        return dataNodesStatus;
      }

      CreateFunctionPlan createFunctionPlan =
          new CreateFunctionPlan(udfInformation, needToSaveJar ? new Binary(jarFile) : null);
      if (needToSaveJar && createFunctionPlan.getSerializedSize() > planSizeLimit) {
        return new TSStatus(TSStatusCode.CREATE_TRIGGER_ERROR.getStatusCode())
            .setMessage(
                String.format(
                    "Fail to create UDF[%s], the size of Jar is too large, you can increase the value of property 'config_node_ratis_log_appender_buffer_size_max' on ConfigNode",
                    udfName));
      }

      LOGGER.info("Start to add UDF [{}] in UDF_Table on Config Nodes", udfName);

      return configManager.getConsensusManager().write(createFunctionPlan).getStatus();
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
        new TCreateFunctionInstanceReq(udfInformation.serialize()).setJarFile(jarFile);
    AsyncClientHandler<TCreateFunctionInstanceReq, TSStatus> clientHandler =
        new AsyncClientHandler<>(DataNodeRequestType.CREATE_FUNCTION, req, dataNodeLocationMap);
    AsyncDataNodeClientPool.getInstance().sendAsyncRequestToDataNodeWithRetry(clientHandler);
    return clientHandler.getResponseList();
  }

  public TSStatus dropFunction(String functionName) {
    functionName = functionName.toUpperCase();
    udfInfo.acquireUDFTableLock();
    try {
      udfInfo.validate(functionName);

      TSStatus result = RpcUtils.squashResponseStatusList(dropFunctionOnDataNodes(functionName));
      if (result.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        return result;
      }

      return configManager
          .getConsensusManager()
          .write(new DropFunctionPlan(functionName))
          .getStatus();
    } catch (Exception e) {
      LOGGER.warn(e.getMessage(), e);
      return new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode())
          .setMessage(e.getMessage());
    } finally {
      udfInfo.releaseUDFTableLock();
    }
  }

  private List<TSStatus> dropFunctionOnDataNodes(String functionName) {
    final Map<Integer, TDataNodeLocation> dataNodeLocationMap =
        configManager.getNodeManager().getRegisteredDataNodeLocations();

    final TDropFunctionInstanceReq request = new TDropFunctionInstanceReq(functionName, false);

    AsyncClientHandler<TDropFunctionInstanceReq, TSStatus> clientHandler =
        new AsyncClientHandler<>(DataNodeRequestType.DROP_FUNCTION, request, dataNodeLocationMap);
    AsyncDataNodeClientPool.getInstance().sendAsyncRequestToDataNodeWithRetry(clientHandler);
    return clientHandler.getResponseList();
  }

  public TGetUDFTableResp getUDFTable() {
    try {
      return ((FunctionTableResp)
              configManager.getConsensusManager().read(new GetFunctionTablePlan()).getDataset())
          .convertToThriftResponse();
    } catch (IOException e) {
      LOGGER.error("Fail to get TriggerTable", e);
      return new TGetUDFTableResp(
          new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode())
              .setMessage(e.getMessage()),
          Collections.emptyList());
    }
  }

  public TGetJarInListResp getUDFJar(TGetJarInListReq req) {
    try {
      return ((JarResp)
              configManager
                  .getConsensusManager()
                  .read(new GetUDFJarPlan(req.getJarNameList()))
                  .getDataset())
          .convertToThriftResponse();
    } catch (IOException e) {
      LOGGER.error("Fail to get TriggerJar", e);
      return new TGetJarInListResp(
          new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode())
              .setMessage(e.getMessage()),
          Collections.emptyList());
    }
  }
}
