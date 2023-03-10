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

package org.apache.iotdb.confignode.manager.pipe;

import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.pipe.plugin.meta.PipePluginMeta;
import org.apache.iotdb.confignode.client.DataNodeRequestType;
import org.apache.iotdb.confignode.client.async.AsyncDataNodeClientPool;
import org.apache.iotdb.confignode.client.async.handlers.AsyncClientHandler;
import org.apache.iotdb.confignode.conf.ConfigNodeDescriptor;
import org.apache.iotdb.confignode.consensus.request.read.pipe.plugin.GetPipePluginJarPlan;
import org.apache.iotdb.confignode.consensus.request.read.pipe.plugin.GetPipePluginTablePlan;
import org.apache.iotdb.confignode.consensus.request.write.pipe.plugin.CreatePipePluginPlan;
import org.apache.iotdb.confignode.consensus.request.write.pipe.plugin.DropPipePluginPlan;
import org.apache.iotdb.confignode.consensus.response.pipe.plugin.PipePluginTableResp;
import org.apache.iotdb.confignode.consensus.response.udf.JarResp;
import org.apache.iotdb.confignode.manager.ConfigManager;
import org.apache.iotdb.confignode.persistence.pipe.PipePluginInfo;
import org.apache.iotdb.confignode.rpc.thrift.TCreatePipePluginReq;
import org.apache.iotdb.confignode.rpc.thrift.TGetJarInListReq;
import org.apache.iotdb.confignode.rpc.thrift.TGetJarInListResp;
import org.apache.iotdb.confignode.rpc.thrift.TGetPipePluginTableResp;
import org.apache.iotdb.mpp.rpc.thrift.TCreatePipePluginInstanceReq;
import org.apache.iotdb.mpp.rpc.thrift.TDropPipePluginInstanceReq;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.tsfile.utils.Binary;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class PipePluginCoordinator {

  private static final Logger LOGGER = LoggerFactory.getLogger(PipePluginCoordinator.class);

  private final long planSizeLimit =
      ConfigNodeDescriptor.getInstance()
              .getConf()
              .getConfigNodeRatisConsensusLogAppenderBufferSize()
          - IoTDBConstant.RAFT_LOG_BASIC_SIZE;

  private final ConfigManager configManager;
  private final PipePluginInfo pipePluginInfo;

  public PipePluginCoordinator(ConfigManager configManager, PipePluginInfo pipePluginInfo) {
    this.configManager = configManager;
    this.pipePluginInfo = pipePluginInfo;
  }

  public TSStatus createPipePlugin(TCreatePipePluginReq req) {
    pipePluginInfo.acquirePipePluginInfoLock();
    try {
      return doCreatePipePlugin(req);
    } catch (Exception e) {
      LOGGER.warn(e.getMessage(), e);
      return new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode())
          .setMessage(e.getMessage());
    } finally {
      pipePluginInfo.releasePipePluginInfoLock();
    }
  }

  private TSStatus doCreatePipePlugin(TCreatePipePluginReq req) throws IOException {
    final String pluginName = req.getPluginName().toUpperCase();
    final String jarName = req.getJarName();
    final String jarMD5 = req.getJarMD5();

    pipePluginInfo.validateBeforeCreatingPipePlugin(pluginName, jarName, jarMD5);
    final boolean needToSaveJar =
        pipePluginInfo.isJarNeededToBeSavedWhenCreatingPipePlugin(jarName);
    LOGGER.info(
        "Start to create PipePlugin [{}] on Data Nodes, needToSaveJar[{}]",
        pluginName,
        needToSaveJar);

    final byte[] jarFile = req.getJarFile();
    final PipePluginMeta pipePluginMeta =
        new PipePluginMeta(pluginName, req.getClassName(), jarName, jarMD5);

    // data nodes
    final TSStatus dataNodesStatus =
        RpcUtils.squashResponseStatusList(
            createPipePluginOnDataNodes(pipePluginMeta, needToSaveJar ? jarFile : null));
    if (dataNodesStatus.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      return dataNodesStatus;
    }

    // config nodes
    final CreatePipePluginPlan createPluginPlan =
        new CreatePipePluginPlan(pipePluginMeta, needToSaveJar ? new Binary(jarFile) : null);
    if (needToSaveJar && createPluginPlan.getSerializedSize() > planSizeLimit) {
      return new TSStatus(TSStatusCode.CREATE_PIPE_PLUGIN_ERROR.getStatusCode())
          .setMessage(
              String.format(
                  "Fail to create PipePlugin[%s], the size of Jar is too large, you should increase the value of property 'config_node_ratis_log_appender_buffer_size_max' on ConfigNode",
                  pluginName));
    }
    LOGGER.info("Start to add PipePlugin [{}] to PipePluginTable", pluginName);
    return configManager.getConsensusManager().write(createPluginPlan).getStatus();
  }

  private List<TSStatus> createPipePluginOnDataNodes(PipePluginMeta pipePluginMeta, byte[] jarFile)
      throws IOException {
    final Map<Integer, TDataNodeLocation> dataNodeLocationMap =
        configManager.getNodeManager().getRegisteredDataNodeLocations();
    final TCreatePipePluginInstanceReq req =
        new TCreatePipePluginInstanceReq(pipePluginMeta.serialize(), ByteBuffer.wrap(jarFile));

    final AsyncClientHandler<TCreatePipePluginInstanceReq, TSStatus> clientHandler =
        new AsyncClientHandler<>(DataNodeRequestType.CREATE_PIPE_PLUGIN, req, dataNodeLocationMap);
    AsyncDataNodeClientPool.getInstance().sendAsyncRequestToDataNodeWithRetry(clientHandler);
    return clientHandler.getResponseList();
  }

  public TSStatus dropPipePlugin(String pluginName) {
    pipePluginInfo.acquirePipePluginInfoLock();
    try {
      return doDropPipePlugin(pluginName);
    } catch (Exception e) {
      LOGGER.warn(e.getMessage(), e);
      return new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode())
          .setMessage(e.getMessage());
    } finally {
      pipePluginInfo.releasePipePluginInfoLock();
    }
  }

  private TSStatus doDropPipePlugin(String pluginName) {
    pluginName = pluginName.toUpperCase();
    pipePluginInfo.validateBeforeDroppingPipePlugin(pluginName);

    TSStatus result = RpcUtils.squashResponseStatusList(dropPipePluginOnDataNodes(pluginName));
    if (result.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      return result;
    }

    return configManager
        .getConsensusManager()
        .write(new DropPipePluginPlan(pluginName))
        .getStatus();
  }

  private List<TSStatus> dropPipePluginOnDataNodes(String pluginName) {
    final Map<Integer, TDataNodeLocation> dataNodeLocationMap =
        configManager.getNodeManager().getRegisteredDataNodeLocations();
    final TDropPipePluginInstanceReq req = new TDropPipePluginInstanceReq(pluginName, false);

    final AsyncClientHandler<TDropPipePluginInstanceReq, TSStatus> clientHandler =
        new AsyncClientHandler<>(DataNodeRequestType.DROP_PIPE_PLUGIN, req, dataNodeLocationMap);
    AsyncDataNodeClientPool.getInstance().sendAsyncRequestToDataNodeWithRetry(clientHandler);
    return clientHandler.getResponseList();
  }

  public TGetPipePluginTableResp getPipePluginTable() {
    try {
      return ((PipePluginTableResp)
              configManager.getConsensusManager().read(new GetPipePluginTablePlan()).getDataset())
          .convertToThriftResponse();
    } catch (IOException e) {
      LOGGER.error("Fail to get PipePluginTable", e);
      return new TGetPipePluginTableResp(
          new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode())
              .setMessage(e.getMessage()),
          Collections.emptyList());
    }
  }

  public TGetJarInListResp getPipePluginJar(TGetJarInListReq req) {
    try {
      return ((JarResp)
              configManager
                  .getConsensusManager()
                  .read(new GetPipePluginJarPlan(req.getJarNameList()))
                  .getDataset())
          .convertToThriftResponse();
    } catch (IOException e) {
      LOGGER.error("Fail to get PipePluginJar", e);
      return new TGetJarInListResp(
          new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode())
              .setMessage(e.getMessage()),
          Collections.emptyList());
    }
  }
}
