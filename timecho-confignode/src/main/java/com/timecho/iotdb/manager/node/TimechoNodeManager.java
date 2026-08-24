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

package com.timecho.iotdb.manager.node;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.confignode.manager.node.NodeManager;
import org.apache.iotdb.confignode.persistence.node.NodeInfo;
import org.apache.iotdb.confignode.rpc.thrift.TAINodeRegisterReq;
import org.apache.iotdb.confignode.rpc.thrift.TDataNodeRegisterReq;
import org.apache.iotdb.confignode.rpc.thrift.TDataNodeRestartReq;
import org.apache.iotdb.confignode.rpc.thrift.TDataNodeRestartResp;
import org.apache.iotdb.rpc.TSStatusCode;

import com.timecho.iotdb.commons.commission.Lottery;
import com.timecho.iotdb.manager.ITimechoManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TimechoNodeManager extends NodeManager {
  private static final Logger LOGGER = LoggerFactory.getLogger(TimechoNodeManager.class);
  private final ITimechoManager configManager;

  public TimechoNodeManager(ITimechoManager configManager, NodeInfo nodeInfo) {
    super(configManager, nodeInfo);
    this.configManager = configManager;
  }

  @Override
  protected TSStatus registerDataNodeActivationCheck(TDataNodeRegisterReq req) {
    Lottery lottery = configManager.getActivationManager().getLicense();
    // check if no license
    if (lottery.noLicense()) {
      LOGGER.info(
          "There is no license in cluster, allow DataNode start: {}",
          req.getDataNodeConfiguration().getLocation().getInternalEndPoint());
      return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
    }
    // check if unactivated
    if (!configManager.getActivationManager().isActivated()) {
      final String message =
          "Deny DataNode registration: Cluster is unactivated now, DataNode is not allowed to join";
      LOGGER.warn(message);
      return new TSStatus(TSStatusCode.LICENSE_ERROR.getStatusCode()).setMessage(message);
    }
    // check DataNode num limit
    if (nodeInfo.getRegisteredDataNodeCount() + 1 > lottery.getDataNodeNumLimit()) {
      final String message =
          String.format(
              "Deny DataNode registration: DataNodes number limit exceeded, %d + 1 = %d, greater than %d",
              nodeInfo.getRegisteredDataNodeCount(),
              nodeInfo.getRegisteredDataNodeCount() + 1,
              lottery.getDataNodeNumLimit());
      LOGGER.warn(message);
      return new TSStatus(TSStatusCode.LICENSE_ERROR.getStatusCode()).setMessage(message);
    } else {
      String message =
          String.format(
              "DataNode register node num check pass. "
                  + "After the successful register of this datanode, "
                  + "the remaining quota for node num will be set to %d.",
              lottery.getDataNodeNumLimit() - nodeInfo.getRegisteredDataNodeCount() - 1);
      LOGGER.info(message);
    }
    // check DataNode's cpu core num limit
    int clusterCpuCores = nodeInfo.getDataNodeTotalCpuCoreCount();
    int newNodeCpuCores = req.getDataNodeConfiguration().getResource().getCpuCoreNum();
    int cpuCoreLimit = lottery.getDataNodeCpuCoreNumLimit();
    if (clusterCpuCores + newNodeCpuCores > cpuCoreLimit) {
      String message =
          String.format(
              "Deny DataNode's registration: DataNodes' CPU cores number limit exceeded, %d + %d = %d, greater than %d (clusterCores + newDataNodeCores = allCores, greater than limit)",
              clusterCpuCores, newNodeCpuCores, clusterCpuCores + newNodeCpuCores, cpuCoreLimit);
      LOGGER.warn(message);
      return new TSStatus(TSStatusCode.LICENSE_ERROR.getStatusCode()).setMessage(message);
    } else {
      String message =
          String.format(
              "DataNode register cpu core num check pass. "
                  + "After the successful register of this datanode, "
                  + "the remaining quota for cpu cores will be set to (%d - %d - %d = %d)",
              cpuCoreLimit,
              clusterCpuCores,
              newNodeCpuCores,
              cpuCoreLimit - clusterCpuCores - newNodeCpuCores);
      LOGGER.info(message);
    }
    return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
  }

  @Override
  protected TSStatus registerAINodeActivationCheck(TAINodeRegisterReq req) {
    final Lottery lottery = configManager.getActivationManager().getLicense();
    // check if unactivated
    if (!configManager.getActivationManager().isActivated()) {
      final String message =
          "Deny AINode registration: Cluster is unactivated now, AINode is not allowed to join";
      LOGGER.warn(message);
      return new TSStatus(TSStatusCode.LICENSE_ERROR.getStatusCode()).setMessage(message);
    }
    // check AINode num limit
    lottery.getAINodeNumLimit();
    if (nodeInfo.getRegisteredAINodes().size() + 1 > lottery.getAINodeNumLimit()) {
      final String message =
          String.format(
              "Deny AINode registration: AINodes number limit exceeded, %d + 1 = %d, greater than %d",
              nodeInfo.getRegisteredAINodeCount(),
              nodeInfo.getRegisteredAINodeCount() + 1,
              lottery.getAINodeNumLimit());
      LOGGER.warn(message);
      return new TSStatus(TSStatusCode.LICENSE_ERROR.getStatusCode()).setMessage(message);
    }
    return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
  }

  @Override
  protected void printDataNodeRegistrationResult() {
    Lottery lottery = configManager.getActivationManager().getLicense();
    LOGGER.info(
        "Accept DataNode registration, node quota remain {}, cpu core quota remain {}",
        lottery.getDataNodeNumLimit() - nodeInfo.getRegisteredDataNodeCount(),
        lottery.getDataNodeCpuCoreNumLimit() - nodeInfo.getDataNodeTotalCpuCoreCount());
  }

  @Override
  protected TDataNodeRestartResp updateDataNodeActivationCheck(TDataNodeRestartReq req) {
    Lottery lottery = configManager.getActivationManager().getLicense();
    TDataNodeRestartResp resp = new TDataNodeRestartResp();
    resp.setConfigNodeList(getRegisteredConfigNodes());
    // Keep the restart path available while the cluster is unactivated so operators can install or
    // replace the license. New DataNode registration keeps its existing activation policy.
    if (!configManager.getActivationManager().isActivated()) {
      resp.setStatus(new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode()));
      return resp;
    }
    // check DataNode's cpu core num limit
    final int previousNodeCpuCores =
        nodeInfo.getDataNodeCpuCoreCount(
            req.getDataNodeConfiguration().getLocation().getDataNodeId());
    final int restartNodeCpuCores = req.getDataNodeConfiguration().getResource().getCpuCoreNum();
    if (restartNodeCpuCores <= previousNodeCpuCores) {
      LOGGER.info(
          "cpu core num is less or equal, {} <= {}, check pass.",
          restartNodeCpuCores,
          previousNodeCpuCores);
      resp.setStatus(new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode()));
      return resp;
    }
    final int clusterCpuCoresExceptThisNode =
        nodeInfo.getDataNodeTotalCpuCoreCount() - previousNodeCpuCores;
    final int cpuCoreLimit = lottery.getDataNodeCpuCoreNumLimit();
    if (clusterCpuCoresExceptThisNode + restartNodeCpuCores > cpuCoreLimit) {
      String message =
          String.format(
              "Deny DataNode's restart: DataNodes' CPU cores number limit exceeded, %d + %d = %d, greater than %d (clusterCores + restartDataNodeCores = allCores, greater than limit)",
              clusterCpuCoresExceptThisNode,
              restartNodeCpuCores,
              clusterCpuCoresExceptThisNode + restartNodeCpuCores,
              cpuCoreLimit);
      LOGGER.warn(message);
      resp.setStatus(new TSStatus(TSStatusCode.LICENSE_ERROR.getStatusCode()).setMessage(message));
      return resp;
    } else {
      String message =
          String.format(
              "DataNode restart cpu core num check pass. "
                  + "After the successful restart of this datanode, "
                  + "the remaining quota for cpu cores will be set to (%d - %d - %d = %d)",
              cpuCoreLimit,
              clusterCpuCoresExceptThisNode,
              restartNodeCpuCores,
              cpuCoreLimit - clusterCpuCoresExceptThisNode - restartNodeCpuCores);
      LOGGER.info(message);
    }
    resp.setStatus(new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode()));
    return resp;
  }
}
