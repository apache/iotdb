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
package org.apache.iotdb.confignode.service.thrift;

import org.apache.iotdb.common.rpc.thrift.TConfigNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TDataNodeInfo;
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TFlushReq;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.auth.AuthException;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.consensus.ConsensusGroupId;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.confignode.client.SyncConfigNodeClientPool;
import org.apache.iotdb.confignode.conf.ConfigNodeDescriptor;
import org.apache.iotdb.confignode.consensus.request.ConfigRequestType;
import org.apache.iotdb.confignode.consensus.request.auth.AuthorReq;
import org.apache.iotdb.confignode.consensus.request.read.CountStorageGroupReq;
import org.apache.iotdb.confignode.consensus.request.read.GetConfigNodeConfigurationReq;
import org.apache.iotdb.confignode.consensus.request.read.GetDataNodeInfoReq;
import org.apache.iotdb.confignode.consensus.request.read.GetDataPartitionReq;
import org.apache.iotdb.confignode.consensus.request.read.GetOrCreateDataPartitionReq;
import org.apache.iotdb.confignode.consensus.request.read.GetRegionInfoListReq;
import org.apache.iotdb.confignode.consensus.request.read.GetStorageGroupReq;
import org.apache.iotdb.confignode.consensus.request.write.RegisterDataNodeReq;
import org.apache.iotdb.confignode.consensus.request.write.RemoveConfigNodeReq;
import org.apache.iotdb.confignode.consensus.request.write.SetDataReplicationFactorReq;
import org.apache.iotdb.confignode.consensus.request.write.SetSchemaReplicationFactorReq;
import org.apache.iotdb.confignode.consensus.request.write.SetStorageGroupReq;
import org.apache.iotdb.confignode.consensus.request.write.SetTTLReq;
import org.apache.iotdb.confignode.consensus.request.write.SetTimePartitionIntervalReq;
import org.apache.iotdb.confignode.consensus.response.ConfigNodeConfigurationResp;
import org.apache.iotdb.confignode.consensus.response.CountStorageGroupResp;
import org.apache.iotdb.confignode.consensus.response.DataNodeConfigurationResp;
import org.apache.iotdb.confignode.consensus.response.DataNodeInfosResp;
import org.apache.iotdb.confignode.consensus.response.PermissionInfoResp;
import org.apache.iotdb.confignode.consensus.response.RegionInfoListResp;
import org.apache.iotdb.confignode.consensus.response.StorageGroupSchemaResp;
import org.apache.iotdb.confignode.manager.ConfigManager;
import org.apache.iotdb.confignode.manager.ConsensusManager;
import org.apache.iotdb.confignode.rpc.thrift.ConfigIService;
import org.apache.iotdb.confignode.rpc.thrift.TAuthorizerReq;
import org.apache.iotdb.confignode.rpc.thrift.TAuthorizerResp;
import org.apache.iotdb.confignode.rpc.thrift.TCheckUserPrivilegesReq;
import org.apache.iotdb.confignode.rpc.thrift.TClusterNodeInfos;
import org.apache.iotdb.confignode.rpc.thrift.TConfigNodeConfigurationResp;
import org.apache.iotdb.confignode.rpc.thrift.TConfigNodeRegisterReq;
import org.apache.iotdb.confignode.rpc.thrift.TConfigNodeRegisterResp;
import org.apache.iotdb.confignode.rpc.thrift.TCountStorageGroupResp;
import org.apache.iotdb.confignode.rpc.thrift.TCreateFunctionReq;
import org.apache.iotdb.confignode.rpc.thrift.TDataNodeActiveReq;
import org.apache.iotdb.confignode.rpc.thrift.TDataNodeInfoResp;
import org.apache.iotdb.confignode.rpc.thrift.TDataNodeRegisterReq;
import org.apache.iotdb.confignode.rpc.thrift.TDataNodeRegisterResp;
import org.apache.iotdb.confignode.rpc.thrift.TDataPartitionReq;
import org.apache.iotdb.confignode.rpc.thrift.TDataPartitionResp;
import org.apache.iotdb.confignode.rpc.thrift.TDeleteStorageGroupReq;
import org.apache.iotdb.confignode.rpc.thrift.TDeleteStorageGroupsReq;
import org.apache.iotdb.confignode.rpc.thrift.TDropFunctionReq;
import org.apache.iotdb.confignode.rpc.thrift.TLoginReq;
import org.apache.iotdb.confignode.rpc.thrift.TPermissionInfoResp;
import org.apache.iotdb.confignode.rpc.thrift.TSchemaNodeManagementReq;
import org.apache.iotdb.confignode.rpc.thrift.TSchemaNodeManagementResp;
import org.apache.iotdb.confignode.rpc.thrift.TSchemaPartitionReq;
import org.apache.iotdb.confignode.rpc.thrift.TSchemaPartitionResp;
import org.apache.iotdb.confignode.rpc.thrift.TSetDataReplicationFactorReq;
import org.apache.iotdb.confignode.rpc.thrift.TSetSchemaReplicationFactorReq;
import org.apache.iotdb.confignode.rpc.thrift.TSetStorageGroupReq;
import org.apache.iotdb.confignode.rpc.thrift.TSetTTLReq;
import org.apache.iotdb.confignode.rpc.thrift.TSetTimePartitionIntervalReq;
import org.apache.iotdb.confignode.rpc.thrift.TShowRegionReq;
import org.apache.iotdb.confignode.rpc.thrift.TShowRegionResp;
import org.apache.iotdb.confignode.rpc.thrift.TStorageGroupSchema;
import org.apache.iotdb.confignode.rpc.thrift.TStorageGroupSchemaResp;
import org.apache.iotdb.confignode.service.ConfigNode;
import org.apache.iotdb.consensus.common.response.ConsensusGenericResponse;
import org.apache.iotdb.db.mpp.common.schematree.PathPatternTree;
import org.apache.iotdb.db.qp.logical.sys.AuthorOperator;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/** ConfigNodeRPCServer exposes the interface that interacts with the DataNode */
public class ConfigNodeRPCServiceProcessor implements ConfigIService.Iface {

  private static final Logger LOGGER = LoggerFactory.getLogger(ConfigNodeRPCServiceProcessor.class);

  private final ConfigManager configManager;

  public ConfigNodeRPCServiceProcessor(ConfigManager configManager) {
    this.configManager = configManager;
  }

  @TestOnly
  public void close() throws IOException {
    configManager.close();
  }

  @TestOnly
  public ConsensusManager getConsensusManager() {
    return configManager.getConsensusManager();
  }

  @Override
  public TDataNodeRegisterResp registerDataNode(TDataNodeRegisterReq req) throws TException {
    RegisterDataNodeReq registerReq = new RegisterDataNodeReq(req.getDataNodeInfo());
    DataNodeConfigurationResp registerResp =
        (DataNodeConfigurationResp) configManager.registerDataNode(registerReq);

    TDataNodeRegisterResp resp = new TDataNodeRegisterResp();
    registerResp.convertToRpcDataNodeRegisterResp(resp);

    // Print log to record the ConfigNode that performs the RegisterDatanodeRequest
    LOGGER.info("Execute RegisterDatanodeRequest {} with result {}", req, resp);

    return resp;
  }

  @Override
  public TSStatus activeDataNode(TDataNodeActiveReq req) throws TException {
    // TODO: implement active data node
    return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
  }

  @Override
  public TDataNodeInfoResp getDataNodeInfo(int dataNodeID) throws TException {
    GetDataNodeInfoReq queryReq = new GetDataNodeInfoReq(dataNodeID);
    DataNodeInfosResp queryResp = (DataNodeInfosResp) configManager.getDataNodeInfo(queryReq);

    TDataNodeInfoResp resp = new TDataNodeInfoResp();
    queryResp.convertToRpcDataNodeLocationResp(resp);
    return resp;
  }

  @Override
  public TClusterNodeInfos getAllClusterNodeInfos() throws TException {
    List<TConfigNodeLocation> configNodeLocations =
        configManager.getNodeManager().getOnlineConfigNodes();
    List<TDataNodeLocation> dataNodeInfoLocations =
        configManager.getNodeManager().getOnlineDataNodes(-1).stream()
            .map(TDataNodeInfo::getLocation)
            .collect(Collectors.toList());

    return new TClusterNodeInfos(
        new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode()),
        configNodeLocations,
        dataNodeInfoLocations);
  }

  @Override
  public TSStatus setStorageGroup(TSetStorageGroupReq req) throws TException {
    TStorageGroupSchema storageGroupSchema = req.getStorageGroup();

    // Set default configurations if necessary
    if (!storageGroupSchema.isSetTTL()) {
      storageGroupSchema.setTTL(CommonDescriptor.getInstance().getConfig().getDefaultTTL());
    }
    if (!storageGroupSchema.isSetSchemaReplicationFactor()) {
      storageGroupSchema.setSchemaReplicationFactor(
          ConfigNodeDescriptor.getInstance().getConf().getSchemaReplicationFactor());
    }
    if (!storageGroupSchema.isSetDataReplicationFactor()) {
      storageGroupSchema.setDataReplicationFactor(
          ConfigNodeDescriptor.getInstance().getConf().getDataReplicationFactor());
    }
    if (!storageGroupSchema.isSetTimePartitionInterval()) {
      storageGroupSchema.setTimePartitionInterval(
          ConfigNodeDescriptor.getInstance().getConf().getTimePartitionInterval());
    }

    // Initialize the maxSchemaRegionGroupCount and maxDataRegionGroupCount as 0
    storageGroupSchema.setMaxSchemaRegionGroupCount(0);
    storageGroupSchema.setMaxDataRegionGroupCount(0);

    SetStorageGroupReq setReq = new SetStorageGroupReq(storageGroupSchema);
    TSStatus resp = configManager.setStorageGroup(setReq);

    // Print log to record the ConfigNode that performs the set SetStorageGroupRequest
    LOGGER.info("Execute SetStorageGroupRequest {} with result {}", req, resp);

    return resp;
  }

  @Override
  public TSStatus deleteStorageGroup(TDeleteStorageGroupReq tDeleteReq) throws TException {
    String prefixPath = tDeleteReq.getPrefixPath();
    return configManager.deleteStorageGroups(Collections.singletonList(prefixPath));
  }

  @Override
  public TSStatus deleteStorageGroups(TDeleteStorageGroupsReq tDeleteReq) throws TException {
    List<String> prefixList = tDeleteReq.getPrefixPathList();
    return configManager.deleteStorageGroups(prefixList);
  }

  @Override
  public TSStatus setTTL(TSetTTLReq req) throws TException {
    return configManager.setTTL(new SetTTLReq(req.getStorageGroup(), req.getTTL()));
  }

  @Override
  public TSStatus setSchemaReplicationFactor(TSetSchemaReplicationFactorReq req) throws TException {
    return configManager.setSchemaReplicationFactor(
        new SetSchemaReplicationFactorReq(req.getStorageGroup(), req.getSchemaReplicationFactor()));
  }

  @Override
  public TSStatus setDataReplicationFactor(TSetDataReplicationFactorReq req) throws TException {
    return configManager.setDataReplicationFactor(
        new SetDataReplicationFactorReq(req.getStorageGroup(), req.getDataReplicationFactor()));
  }

  @Override
  public TSStatus setTimePartitionInterval(TSetTimePartitionIntervalReq req) throws TException {
    return configManager.setTimePartitionInterval(
        new SetTimePartitionIntervalReq(req.getStorageGroup(), req.getTimePartitionInterval()));
  }

  @Override
  public TCountStorageGroupResp countMatchedStorageGroups(List<String> storageGroupPathPattern)
      throws TException {
    CountStorageGroupResp countStorageGroupResp =
        (CountStorageGroupResp)
            configManager.countMatchedStorageGroups(
                new CountStorageGroupReq(storageGroupPathPattern));

    TCountStorageGroupResp resp = new TCountStorageGroupResp();
    countStorageGroupResp.convertToRPCCountStorageGroupResp(resp);
    return resp;
  }

  @Override
  public TStorageGroupSchemaResp getMatchedStorageGroupSchemas(List<String> storageGroupPathPattern)
      throws TException {
    StorageGroupSchemaResp storageGroupSchemaResp =
        (StorageGroupSchemaResp)
            configManager.getMatchedStorageGroupSchemas(
                new GetStorageGroupReq(storageGroupPathPattern));

    TStorageGroupSchemaResp resp = new TStorageGroupSchemaResp();
    storageGroupSchemaResp.convertToRPCStorageGroupSchemaResp(resp);
    return resp;
  }

  @Override
  public TSchemaPartitionResp getSchemaPartition(TSchemaPartitionReq req) throws TException {
    PathPatternTree patternTree =
        PathPatternTree.deserialize(ByteBuffer.wrap(req.getPathPatternTree()));
    return configManager.getSchemaPartition(patternTree);
  }

  @Override
  public TSchemaPartitionResp getOrCreateSchemaPartition(TSchemaPartitionReq req)
      throws TException {
    PathPatternTree patternTree =
        PathPatternTree.deserialize(ByteBuffer.wrap(req.getPathPatternTree()));
    return configManager.getOrCreateSchemaPartition(patternTree);
  }

  @Override
  public TSchemaNodeManagementResp getSchemaNodeManagementPartition(TSchemaNodeManagementReq req)
      throws TException {
    PathPatternTree patternTree =
        PathPatternTree.deserialize(ByteBuffer.wrap(req.getPathPatternTree()));
    PartialPath partialPath = patternTree.getAllPathPatterns().get(0);
    return configManager.getNodePathsPartition(partialPath, req.getLevel());
  }

  @Override
  public TDataPartitionResp getDataPartition(TDataPartitionReq req) throws TException {
    GetDataPartitionReq getDataPartitionReq = new GetDataPartitionReq();
    getDataPartitionReq.convertFromRpcTDataPartitionReq(req);
    return configManager.getDataPartition(getDataPartitionReq);
  }

  @Override
  public TDataPartitionResp getOrCreateDataPartition(TDataPartitionReq req) throws TException {
    GetOrCreateDataPartitionReq getOrCreateDataPartitionReq = new GetOrCreateDataPartitionReq();
    getOrCreateDataPartitionReq.convertFromRpcTDataPartitionReq(req);
    return configManager.getOrCreateDataPartition(getOrCreateDataPartitionReq);
  }

  @Override
  public TSStatus operatePermission(TAuthorizerReq req) throws TException {
    if (req.getAuthorType() < 0
        || req.getAuthorType() >= AuthorOperator.AuthorType.values().length) {
      throw new IndexOutOfBoundsException("Invalid Author Type ordinal");
    }
    AuthorReq plan = null;
    try {
      plan =
          new AuthorReq(
              ConfigRequestType.values()[
                  req.getAuthorType() + ConfigRequestType.Author.ordinal() + 1],
              req.getUserName(),
              req.getRoleName(),
              req.getPassword(),
              req.getNewPassword(),
              req.getPermissions(),
              req.getNodeName());
    } catch (AuthException e) {
      LOGGER.error(e.getMessage());
    }
    return configManager.operatePermission(plan);
  }

  @Override
  public TAuthorizerResp queryPermission(TAuthorizerReq req) throws TException {
    if (req.getAuthorType() < 0
        || req.getAuthorType() >= AuthorOperator.AuthorType.values().length) {
      throw new IndexOutOfBoundsException("Invalid Author Type ordinal");
    }
    AuthorReq plan = null;
    try {
      plan =
          new AuthorReq(
              ConfigRequestType.values()[
                  req.getAuthorType() + ConfigRequestType.Author.ordinal() + 1],
              req.getUserName(),
              req.getRoleName(),
              req.getPassword(),
              req.getNewPassword(),
              req.getPermissions(),
              req.getNodeName());
    } catch (AuthException e) {
      LOGGER.error(e.getMessage());
    }
    PermissionInfoResp dataSet = (PermissionInfoResp) configManager.queryPermission(plan);
    TAuthorizerResp resp = new TAuthorizerResp(dataSet.getStatus());
    resp.setAuthorizerInfo(dataSet.getPermissionInfo());
    return resp;
  }

  @Override
  public TPermissionInfoResp login(TLoginReq req) throws TException {
    return configManager.login(req.getUserrname(), req.getPassword());
  }

  @Override
  public TPermissionInfoResp checkUserPrivileges(TCheckUserPrivilegesReq req) throws TException {
    return configManager.checkUserPrivileges(
        req.getUsername(), req.getPaths(), req.getPermission());
  }

  @Override
  public TConfigNodeConfigurationResp getConfigNodeConfiguration(
      TConfigNodeLocation configNodeLocation) throws TException {
    GetConfigNodeConfigurationReq getConfigurationReq =
        new GetConfigNodeConfigurationReq(configNodeLocation);
    ConfigNodeConfigurationResp configurationResp =
        (ConfigNodeConfigurationResp) configManager.getConfigNodeConfiguration(getConfigurationReq);

    TConfigNodeConfigurationResp resp = new TConfigNodeConfigurationResp();
    configurationResp.convertToRPCConfigNodeConfigurationResp(resp);

    // Print log to record the ConfigNode that performs the GetConfigNodeConfigurationRequest
    LOGGER.info(
        "Execute GetConfigNodeConfigurationReq {} with result {}", getConfigurationReq, resp);

    return resp;
  }

  @Override
  public TConfigNodeRegisterResp registerConfigNode(TConfigNodeRegisterReq req) throws TException {
    TConfigNodeRegisterResp resp = configManager.registerConfigNode(req);

    // Print log to record the ConfigNode that performs the RegisterConfigNodeRequest
    LOGGER.info("Execute RegisterConfigNodeRequest {} with result {}", req, resp);

    return resp;
  }

  @Override
  public TSStatus addConsensusGroup(TConfigNodeRegisterResp registerResp) {
    return configManager.addConsensusGroup(registerResp.getConfigNodeList());
  }

  /**
   * For leader to remove ConfigNode configuration in consensus layer
   *
   * @param configNodeLocation
   * @return
   */
  @Override
  public TSStatus removeConfigNode(TConfigNodeLocation configNodeLocation) throws TException {
    RemoveConfigNodeReq removeConfigNodeReq = new RemoveConfigNodeReq(configNodeLocation);

    TSStatus status = configManager.removeConfigNode(removeConfigNodeReq);
    if (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      status = SyncConfigNodeClientPool.getInstance().stopConfigNode(configNodeLocation);
    }

    // Print log to record the ConfigNode that performs the RemoveConfigNodeRequest
    LOGGER.info("Execute RemoveConfigNodeRequest {} with result {}", configNodeLocation, status);

    return status;
  }

  /**
   * For leader to stop ConfigNode
   *
   * @param configNodeLocation
   * @return
   */
  @Override
  public TSStatus stopConfigNode(TConfigNodeLocation configNodeLocation) throws TException {
    if (!configManager.getNodeManager().getOnlineConfigNodes().contains(configNodeLocation)) {
      return new TSStatus(TSStatusCode.REMOVE_CONFIGNODE_FAILED.getStatusCode())
          .setMessage("Stop ConfigNode failed because the ConfigNode not in current Cluster.");
    }

    ConsensusGroupId groupId = configManager.getConsensusManager().getConsensusGroupId();
    ConsensusGenericResponse resp =
        configManager.getConsensusManager().getConsensusImpl().removeConsensusGroup(groupId);
    if (!resp.isSuccess()) {
      return new TSStatus(TSStatusCode.REMOVE_CONFIGNODE_FAILED.getStatusCode())
          .setMessage("Stop ConfigNode failed because remove ConsensusGroup failed.");
    }

    new Thread(
            () -> {
              try {
                ConfigNode.getInstance().stop();
                System.exit(0);
              } catch (IOException e) {
                LOGGER.error("Meet error when stop ConfigNode!", e);
              }
            })
        .start();
    return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode())
        .setMessage("Stop ConfigNode success.");
  }

  @Override
  public TSStatus createFunction(TCreateFunctionReq req) {
    return configManager.createFunction(req.getUdfName(), req.getClassName(), req.getUris());
  }

  @Override
  public TSStatus dropFunction(TDropFunctionReq req) throws TException {
    return configManager.dropFunction(req.getUdfName());
  }

  @Override
  public TSStatus flush(TFlushReq req) throws TException {
    if (req.storageGroups != null) {
      List<PartialPath> noExistSg =
          configManager.checkStorageGroupExist(PartialPath.fromStringList(req.storageGroups));
      if (!noExistSg.isEmpty()) {
        StringBuilder sb = new StringBuilder();
        noExistSg.forEach(storageGroup -> sb.append(storageGroup.getFullPath()).append(","));
        return RpcUtils.getStatus(
            TSStatusCode.STORAGE_GROUP_NOT_EXIST,
            "storageGroup " + sb.subSequence(0, sb.length() - 1) + " does not exist");
      }
    }
    return configManager.flush(req);
  }

  @Override
  public TShowRegionResp showRegion(TShowRegionReq showRegionReq) throws TException {
    GetRegionInfoListReq getRegionsinfoReq =
        new GetRegionInfoListReq(showRegionReq.getConsensusGroupType());
    RegionInfoListResp dataSet = (RegionInfoListResp) configManager.showRegion(getRegionsinfoReq);
    TShowRegionResp showRegionResp = new TShowRegionResp();
    showRegionResp.setStatus(dataSet.getStatus());
    showRegionResp.setRegionInfoList(dataSet.getRegionInfoList());
    return showRegionResp;
  }

  public void handleClientExit() {}

  // TODO: Interfaces for data operations
}
