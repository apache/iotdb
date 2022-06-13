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

import org.apache.iotdb.common.rpc.thrift.TConfigNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TDataNodeInfo;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.confignode.client.AsyncConfigNodeToDataNodeClientPool;
import org.apache.iotdb.confignode.conf.ConfigNodeConf;
import org.apache.iotdb.confignode.conf.ConfigNodeDescriptor;
import org.apache.iotdb.confignode.consensus.request.read.GetDataNodeInfoReq;
import org.apache.iotdb.confignode.consensus.request.write.RegisterConfigNodeReq;
import org.apache.iotdb.confignode.consensus.request.write.RegisterDataNodeReq;
import org.apache.iotdb.confignode.consensus.response.DataNodeConfigurationResp;
import org.apache.iotdb.confignode.consensus.response.DataNodeInfosResp;
import org.apache.iotdb.confignode.persistence.NodeInfo;
import org.apache.iotdb.confignode.rpc.thrift.TConfigNodeRegisterReq;
import org.apache.iotdb.confignode.rpc.thrift.TConfigNodeRegisterResp;
import org.apache.iotdb.confignode.rpc.thrift.TGlobalConfig;
import org.apache.iotdb.consensus.common.DataSet;
import org.apache.iotdb.consensus.common.response.ConsensusWriteResponse;
import org.apache.iotdb.rpc.TSStatusCode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.locks.ReentrantLock;

/** NodeManager manages cluster node addition and removal requests */
public class NodeManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(NodeManager.class);

  private final Manager configManager;

  private final NodeInfo nodeInfo;

  private final ReentrantLock registerConfigNodeLock;

  public NodeManager(Manager configManager, NodeInfo nodeInfo) {
    this.configManager = configManager;
    this.nodeInfo = nodeInfo;
    this.registerConfigNodeLock = new ReentrantLock();
  }

  private void setGlobalConfig(DataNodeConfigurationResp dataSet) {
    // Set TGlobalConfig
    TGlobalConfig globalConfig = new TGlobalConfig();
    globalConfig.setDataRegionConsensusProtocolClass(
        ConfigNodeDescriptor.getInstance().getConf().getDataRegionConsensusProtocolClass());
    globalConfig.setSchemaRegionConsensusProtocolClass(
        ConfigNodeDescriptor.getInstance().getConf().getSchemaRegionConsensusProtocolClass());
    globalConfig.setSeriesPartitionSlotNum(
        ConfigNodeDescriptor.getInstance().getConf().getSeriesPartitionSlotNum());
    globalConfig.setSeriesPartitionExecutorClass(
        ConfigNodeDescriptor.getInstance().getConf().getSeriesPartitionExecutorClass());
    globalConfig.setTimePartitionInterval(
        ConfigNodeDescriptor.getInstance().getConf().getTimePartitionInterval());
    dataSet.setGlobalConfig(globalConfig);
  }

  /**
   * Register DataNode
   *
   * @param req RegisterDataNodeReq
   * @return DataNodeConfigurationDataSet. The TSStatus will be set to SUCCESS_STATUS when register
   *     success, and DATANODE_ALREADY_REGISTERED when the DataNode is already exist.
   */
  public DataSet registerDataNode(RegisterDataNodeReq req) {
    DataNodeConfigurationResp dataSet = new DataNodeConfigurationResp();

    if (nodeInfo.isOnlineDataNode(req.getInfo().getLocation())) {
      // Reset client
      AsyncConfigNodeToDataNodeClientPool.getInstance()
          .resetClient(req.getInfo().getLocation().getInternalEndPoint());

      TSStatus status = new TSStatus(TSStatusCode.DATANODE_ALREADY_REGISTERED.getStatusCode());
      status.setMessage("DataNode already registered.");
      dataSet.setStatus(status);
    } else {
      // Persist DataNodeInfo
      req.getInfo().getLocation().setDataNodeId(nodeInfo.generateNextDataNodeId());
      ConsensusWriteResponse resp = getConsensusManager().write(req);
      dataSet.setStatus(resp.getStatus());
    }

    dataSet.setDataNodeId(req.getInfo().getLocation().getDataNodeId());
    dataSet.setConfigNodeList(nodeInfo.getOnlineConfigNodes());
    setGlobalConfig(dataSet);
    return dataSet;
  }

  /**
   * Get DataNode info
   *
   * @param req QueryDataNodeInfoPlan
   * @return The specific DataNode's info or all DataNode info if dataNodeId in
   *     QueryDataNodeInfoPlan is -1
   */
  public DataNodeInfosResp getDataNodeInfo(GetDataNodeInfoReq req) {
    return (DataNodeInfosResp) getConsensusManager().read(req).getDataset();
  }

  /**
   * Only leader use this interface
   *
   * @return The number of online DataNodes
   */
  public int getOnlineDataNodeCount() {
    return nodeInfo.getOnlineDataNodeCount();
  }

  /**
   * Only leader use this interface
   *
   * @return The number of total cpu cores in online DataNodes
   */
  public int getTotalCpuCoreCount() {
    return nodeInfo.getTotalCpuCoreCount();
  }

  /**
   * Only leader use this interface
   *
   * @param dataNodeId Specific DataNodeId
   * @return All online DataNodes if dataNodeId equals -1. And return the specific DataNode
   *     otherwise.
   */
  public List<TDataNodeInfo> getOnlineDataNodes(int dataNodeId) {
    return nodeInfo.getOnlineDataNodes(dataNodeId);
  }

  /**
   * Provides ConfigNodeGroup information for the newly registered ConfigNode
   *
   * @param req TConfigNodeRegisterReq
   * @return TConfigNodeRegisterResp with PartitionRegionId and online ConfigNodes
   */
  public TConfigNodeRegisterResp registerConfigNode(TConfigNodeRegisterReq req) {
    // Check system configuration
    TSStatus status = checkSystemConfig(req);
    if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      return new TConfigNodeRegisterResp(status, getOnlineConfigNodes());
    }

    // If there is any error in the following process,
    // the new ConfigNode must retry and the online ConfigNodes should remain the same
    TConfigNodeRegisterResp errorResp =
        new TConfigNodeRegisterResp(
            new TSStatus(TSStatusCode.NEED_RETRY.getStatusCode()), getOnlineConfigNodes());
    if (registerConfigNodeLock.tryLock()) {
      // We use a ReentrantLock here so that the register ConfigNode process is serialized,
      // and the concurrent register request will be rejected directly.
      try {
        // Skip register process if it's done before
        if (!getOnlineConfigNodes().contains(req.getConfigNodeLocation())) {
          // Do register
          RegisterConfigNodeReq registerConfigNodeReq =
              new RegisterConfigNodeReq(req.getConfigNodeLocation());
          if (!getConsensusManager().addConfigNodePeer(registerConfigNodeReq)) {
            LOGGER.error("Error when addPeer: {}", registerConfigNodeReq.getConfigNodeLocation());
            return errorResp;
          }
          if (getConsensusManager().write(registerConfigNodeReq).getStatus().getCode()
              != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
            LOGGER.error(
                "Error when cache ConfigNode: {}", registerConfigNodeReq.getConfigNodeLocation());
            return errorResp;
          }
        }

        // Return SUCCESS_STATUS and new configNodeList
        return new TConfigNodeRegisterResp(
            new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode()), getOnlineConfigNodes());
      } finally {
        registerConfigNodeLock.unlock();
      }
    } else {
      return errorResp;
    }
  }

  /**
   * Ensure the system parameters are consistent.
   *
   * @return SUCCESS_STATUS when consistent, INCONSISTENT_SYSTEM_CONFIG otherwise.
   */
  private TSStatus checkSystemConfig(TConfigNodeRegisterReq req) {
    ConfigNodeConf conf = ConfigNodeDescriptor.getInstance().getConf();
    TSStatus errorStatus = new TSStatus(TSStatusCode.INCONSISTENT_SYSTEM_CONFIG.getStatusCode());

    if (!req.getPartitionRegionId().equals(conf.getPartitionRegionId())) {
      errorStatus.setMessage(
          "Reject register, please ensure that the cluster_id " + "are consistent.");
      return errorStatus;
    }

    if (!req.getGlobalConfig()
        .getDataRegionConsensusProtocolClass()
        .equals(conf.getDataRegionConsensusProtocolClass())) {
      errorStatus.setMessage(
          "Reject register, please ensure that the data_region_consensus_protocol_class "
              + "are consistent.");
      return errorStatus;
    }

    if (!req.getGlobalConfig()
        .getSchemaRegionConsensusProtocolClass()
        .equals(conf.getSchemaRegionConsensusProtocolClass())) {
      errorStatus.setMessage(
          "Reject register, please ensure that the schema_region_consensus_protocol_class "
              + "are consistent.");
      return errorStatus;
    }

    if (req.getGlobalConfig().getSeriesPartitionSlotNum() != conf.getSeriesPartitionSlotNum()) {
      errorStatus.setMessage(
          "Reject register, please ensure that the series_partition_slot_num are consistent.");
      return errorStatus;
    }

    if (!req.getGlobalConfig()
        .getSeriesPartitionExecutorClass()
        .equals(conf.getSeriesPartitionExecutorClass())) {
      errorStatus.setMessage(
          "Reject register, please ensure that the series_partition_executor_class are consistent.");
      return errorStatus;
    }

    if (req.getGlobalConfig().getTimePartitionInterval() != conf.getTimePartitionInterval()) {
      errorStatus.setMessage(
          "Reject register, please ensure that the time_partition_interval are consistent.");
      return errorStatus;
    }

    if (req.getDefaultTTL() != CommonDescriptor.getInstance().getConfig().getDefaultTTL()) {
      errorStatus.setMessage("Reject register, please ensure that the default_ttl are consistent.");
      return errorStatus;
    }

    if (req.getSchemaReplicationFactor() != conf.getSchemaReplicationFactor()) {
      errorStatus.setMessage(
          "Reject register, please ensure that the schema_replication_factor are consistent.");
      return errorStatus;
    }

    if (req.getDataReplicationFactor() != conf.getDataReplicationFactor()) {
      errorStatus.setMessage(
          "Reject register, please ensure that the data_replication_factor are consistent.");
      return errorStatus;
    }

    return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
  }

  public List<TConfigNodeLocation> getOnlineConfigNodes() {
    return nodeInfo.getOnlineConfigNodes();
  }

  private ConsensusManager getConsensusManager() {
    return configManager.getConsensusManager();
  }
}
