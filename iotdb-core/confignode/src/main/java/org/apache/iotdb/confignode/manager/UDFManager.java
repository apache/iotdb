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

import org.apache.iotdb.common.rpc.thrift.FunctionType;
import org.apache.iotdb.common.rpc.thrift.Model;
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.udf.UDFInformation;
import org.apache.iotdb.commons.udf.UDFType;
import org.apache.iotdb.confignode.client.async.CnToDnAsyncRequestType;
import org.apache.iotdb.confignode.client.async.CnToDnInternalServiceAsyncRequestManager;
import org.apache.iotdb.confignode.client.async.handlers.DataNodeAsyncRequestContext;
import org.apache.iotdb.confignode.conf.ConfigNodeDescriptor;
import org.apache.iotdb.confignode.consensus.request.read.function.GetAllFunctionTablePlan;
import org.apache.iotdb.confignode.consensus.request.read.function.GetFunctionTablePlan;
import org.apache.iotdb.confignode.consensus.request.read.function.GetUDFJarPlan;
import org.apache.iotdb.confignode.consensus.request.write.function.CreateFunctionPlan;
import org.apache.iotdb.confignode.consensus.request.write.function.DropTableModelFunctionPlan;
import org.apache.iotdb.confignode.consensus.request.write.function.DropTreeModelFunctionPlan;
import org.apache.iotdb.confignode.consensus.request.write.function.UpdateFunctionPlan;
import org.apache.iotdb.confignode.consensus.response.JarResp;
import org.apache.iotdb.confignode.consensus.response.function.FunctionTableResp;
import org.apache.iotdb.confignode.persistence.UDFInfo;
import org.apache.iotdb.confignode.rpc.thrift.TCreateFunctionReq;
import org.apache.iotdb.confignode.rpc.thrift.TGetJarInListReq;
import org.apache.iotdb.confignode.rpc.thrift.TGetJarInListResp;
import org.apache.iotdb.confignode.rpc.thrift.TGetUDFTableResp;
import org.apache.iotdb.consensus.exception.ConsensusException;
import org.apache.iotdb.mpp.rpc.thrift.TCreateFunctionInstanceReq;
import org.apache.iotdb.mpp.rpc.thrift.TDropFunctionInstanceReq;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.tsfile.utils.Binary;
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
      final String udfName = req.udfName.toUpperCase();
      final String jarMD5 = req.getJarMD5();
      final String jarName = req.getJarName();
      final byte[] jarFile = req.getJarFile();
      final Model model = req.getModel();
      final FunctionType functionType = req.getFunctionType();
      udfInfo.validate(model, udfName, jarName, jarMD5);

      UDFInformation udfInformation =
          new UDFInformation(
              udfName,
              req.getClassName(),
              UDFType.of(model, functionType, false),
              isUsingURI,
              jarName,
              jarMD5);

      final boolean needToSaveJar = isUsingURI && udfInfo.needToSaveJar(jarName);

      LOGGER.info("Start to add UDF [{}] in UDF_Table on Config Nodes", udfName);
      CreateFunctionPlan createFunctionPlan =
          new CreateFunctionPlan(udfInformation, needToSaveJar ? new Binary(jarFile) : null);
      if (needToSaveJar && createFunctionPlan.getSerializedSize() > planSizeLimit) {
        return new TSStatus(TSStatusCode.CREATE_UDF_ERROR.getStatusCode())
            .setMessage(
                String.format(
                    "Fail to create UDF[%s], the size of Jar is too large, you can increase the value of property 'config_node_ratis_log_appender_buffer_size_max' on ConfigNode",
                    udfName));
      }
      TSStatus preCreateStatus = configManager.getConsensusManager().write(createFunctionPlan);
      if (preCreateStatus.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        return preCreateStatus;
      }
      udfInformation =
          new UDFInformation(
              udfName,
              req.getClassName(),
              UDFType.of(model, functionType, true),
              isUsingURI,
              jarName,
              jarMD5);
      LOGGER.info(
          "Start to create UDF [{}] on Data Nodes, needToSaveJar[{}]", udfName, needToSaveJar);
      final TSStatus dataNodesStatus =
          RpcUtils.squashResponseStatusList(
              createFunctionOnDataNodes(udfInformation, needToSaveJar ? jarFile : null));
      if (dataNodesStatus.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        return dataNodesStatus;
      }

      LOGGER.info("Start to activate UDF [{}] in UDF_Table on Config Nodes", udfName);
      return configManager.getConsensusManager().write(new UpdateFunctionPlan(udfInformation));
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
    DataNodeAsyncRequestContext<TCreateFunctionInstanceReq, TSStatus> clientHandler =
        new DataNodeAsyncRequestContext<>(
            CnToDnAsyncRequestType.CREATE_FUNCTION, req, dataNodeLocationMap);
    CnToDnInternalServiceAsyncRequestManager.getInstance().sendAsyncRequestWithRetry(clientHandler);
    return clientHandler.getResponseList();
  }

  public TSStatus dropFunction(Model model, String functionName) {
    functionName = functionName.toUpperCase();
    udfInfo.acquireUDFTableLock();
    try {
      UDFInformation information = udfInfo.getUDFInformation(model, functionName);
      information.setAvailable(false);
      TSStatus preDropStatus =
          configManager.getConsensusManager().write(new UpdateFunctionPlan(information));
      if (preDropStatus.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        return preDropStatus;
      }

      TSStatus result =
          RpcUtils.squashResponseStatusList(dropFunctionOnDataNodes(model, functionName));
      if (result.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        return result;
      }

      if (Model.TREE.equals(model)) {
        return configManager
            .getConsensusManager()
            .write(new DropTreeModelFunctionPlan(functionName));
      } else {
        return configManager
            .getConsensusManager()
            .write(new DropTableModelFunctionPlan(functionName));
      }
    } catch (Exception e) {
      LOGGER.warn(e.getMessage(), e);
      return new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode())
          .setMessage(e.getMessage());
    } finally {
      udfInfo.releaseUDFTableLock();
    }
  }

  private List<TSStatus> dropFunctionOnDataNodes(Model model, String functionName) {
    final Map<Integer, TDataNodeLocation> dataNodeLocationMap =
        configManager.getNodeManager().getRegisteredDataNodeLocations();

    final TDropFunctionInstanceReq request =
        new TDropFunctionInstanceReq(functionName, false).setModel(model);

    DataNodeAsyncRequestContext<TDropFunctionInstanceReq, TSStatus> clientHandler =
        new DataNodeAsyncRequestContext<>(
            CnToDnAsyncRequestType.DROP_FUNCTION, request, dataNodeLocationMap);
    CnToDnInternalServiceAsyncRequestManager.getInstance().sendAsyncRequestWithRetry(clientHandler);
    return clientHandler.getResponseList();
  }

  public TGetUDFTableResp getUDFTable(Model model) {
    try {
      return ((FunctionTableResp)
              configManager.getConsensusManager().read(new GetFunctionTablePlan(model)))
          .convertToThriftResponse();
    } catch (IOException | ConsensusException e) {
      LOGGER.error("Fail to get UDFTable", e);
      return new TGetUDFTableResp(
          new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode())
              .setMessage(e.getMessage()),
          Collections.emptyList());
    }
  }

  public TGetUDFTableResp getAllUDFTable() {
    try {
      return ((FunctionTableResp)
              configManager.getConsensusManager().read(new GetAllFunctionTablePlan()))
          .convertToThriftResponse();
    } catch (IOException | ConsensusException e) {
      LOGGER.error("Fail to get AllUDFTable", e);
      return new TGetUDFTableResp(
          new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode())
              .setMessage(e.getMessage()),
          Collections.emptyList());
    }
  }

  public TGetJarInListResp getUDFJar(TGetJarInListReq req) {
    try {
      return ((JarResp)
              configManager.getConsensusManager().read(new GetUDFJarPlan(req.getJarNameList())))
          .convertToThriftResponse();
    } catch (ConsensusException e) {
      LOGGER.warn("Failed in the read API executing the consensus layer due to: ", e);
      TSStatus res = new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode());
      res.setMessage(e.getMessage());
      return new JarResp(res, Collections.emptyList()).convertToThriftResponse();
    }
  }
}
