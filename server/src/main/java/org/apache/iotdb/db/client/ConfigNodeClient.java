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

package org.apache.iotdb.db.client;

import org.apache.iotdb.common.rpc.thrift.TConfigNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TFlushReq;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.common.rpc.thrift.TSetTTLReq;
import org.apache.iotdb.commons.client.BaseClientFactory;
import org.apache.iotdb.commons.client.ClientFactoryProperty;
import org.apache.iotdb.commons.client.ClientManager;
import org.apache.iotdb.commons.client.ClientPoolProperty;
import org.apache.iotdb.commons.client.sync.SyncThriftClient;
import org.apache.iotdb.commons.client.sync.SyncThriftClientWithErrorHandler;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.consensus.PartitionRegionId;
import org.apache.iotdb.confignode.rpc.thrift.IConfigNodeRPCService;
import org.apache.iotdb.confignode.rpc.thrift.TAddConsensusGroupReq;
import org.apache.iotdb.confignode.rpc.thrift.TAuthorizerReq;
import org.apache.iotdb.confignode.rpc.thrift.TAuthorizerResp;
import org.apache.iotdb.confignode.rpc.thrift.TCheckUserPrivilegesReq;
import org.apache.iotdb.confignode.rpc.thrift.TConfigNodeRegisterReq;
import org.apache.iotdb.confignode.rpc.thrift.TCountStorageGroupResp;
import org.apache.iotdb.confignode.rpc.thrift.TCreateFunctionReq;
import org.apache.iotdb.confignode.rpc.thrift.TCreateSchemaTemplateReq;
import org.apache.iotdb.confignode.rpc.thrift.TDataNodeConfigurationResp;
import org.apache.iotdb.confignode.rpc.thrift.TDataNodeRegisterReq;
import org.apache.iotdb.confignode.rpc.thrift.TDataNodeRegisterResp;
import org.apache.iotdb.confignode.rpc.thrift.TDataNodeRemoveReq;
import org.apache.iotdb.confignode.rpc.thrift.TDataNodeRemoveResp;
import org.apache.iotdb.confignode.rpc.thrift.TDataPartitionReq;
import org.apache.iotdb.confignode.rpc.thrift.TDataPartitionResp;
import org.apache.iotdb.confignode.rpc.thrift.TDataPartitionTableResp;
import org.apache.iotdb.confignode.rpc.thrift.TDeleteStorageGroupReq;
import org.apache.iotdb.confignode.rpc.thrift.TDeleteStorageGroupsReq;
import org.apache.iotdb.confignode.rpc.thrift.TDropFunctionReq;
import org.apache.iotdb.confignode.rpc.thrift.TGetAllTemplatesResp;
import org.apache.iotdb.confignode.rpc.thrift.TGetPathsSetTemplatesResp;
import org.apache.iotdb.confignode.rpc.thrift.TGetTemplateResp;
import org.apache.iotdb.confignode.rpc.thrift.TLoginReq;
import org.apache.iotdb.confignode.rpc.thrift.TPermissionInfoResp;
import org.apache.iotdb.confignode.rpc.thrift.TRegionMigrateResultReportReq;
import org.apache.iotdb.confignode.rpc.thrift.TRegionRouteMapResp;
import org.apache.iotdb.confignode.rpc.thrift.TSchemaNodeManagementReq;
import org.apache.iotdb.confignode.rpc.thrift.TSchemaNodeManagementResp;
import org.apache.iotdb.confignode.rpc.thrift.TSchemaPartitionReq;
import org.apache.iotdb.confignode.rpc.thrift.TSchemaPartitionResp;
import org.apache.iotdb.confignode.rpc.thrift.TSchemaPartitionTableResp;
import org.apache.iotdb.confignode.rpc.thrift.TSetDataReplicationFactorReq;
import org.apache.iotdb.confignode.rpc.thrift.TSetSchemaReplicationFactorReq;
import org.apache.iotdb.confignode.rpc.thrift.TSetSchemaTemplateReq;
import org.apache.iotdb.confignode.rpc.thrift.TSetStorageGroupReq;
import org.apache.iotdb.confignode.rpc.thrift.TSetTimePartitionIntervalReq;
import org.apache.iotdb.confignode.rpc.thrift.TShowClusterResp;
import org.apache.iotdb.confignode.rpc.thrift.TShowConfigNodesResp;
import org.apache.iotdb.confignode.rpc.thrift.TShowDataNodesResp;
import org.apache.iotdb.confignode.rpc.thrift.TShowRegionReq;
import org.apache.iotdb.confignode.rpc.thrift.TShowRegionResp;
import org.apache.iotdb.confignode.rpc.thrift.TStorageGroupSchemaResp;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.rpc.RpcTransportFactory;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.List;

public class ConfigNodeClient
    implements IConfigNodeRPCService.Iface, SyncThriftClient, AutoCloseable {
  private static final Logger logger = LoggerFactory.getLogger(ConfigNodeClient.class);

  private static final int RETRY_NUM = 5;

  public static final String MSG_RECONNECTION_FAIL =
      "Fail to connect to any config node. Please check server it";

  private long connectionTimeout = ClientPoolProperty.DefaultProperty.WAIT_CLIENT_TIMEOUT_MS;

  private IConfigNodeRPCService.Iface client;

  private TTransport transport;

  private TEndPoint configLeader;

  private List<TEndPoint> configNodes;

  private TEndPoint configNode;

  private int cursor = 0;

  private IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();

  ClientManager<PartitionRegionId, ConfigNodeClient> clientManager;

  PartitionRegionId partitionRegionId = ConfigNodeInfo.partitionRegionId;

  TProtocolFactory protocolFactory;

  public ConfigNodeClient() throws TException {
    // Read config nodes from configuration
    configNodes = ConfigNodeInfo.getInstance().getLatestConfigNodes();
    protocolFactory =
        CommonDescriptor.getInstance().getConfig().isRpcThriftCompressionEnabled()
            ? new TCompactProtocol.Factory()
            : new TBinaryProtocol.Factory();

    init();
  }

  public ConfigNodeClient(
      TProtocolFactory protocolFactory,
      long connectionTimeout,
      ClientManager<PartitionRegionId, ConfigNodeClient> clientManager)
      throws TException {
    configNodes = ConfigNodeInfo.getInstance().getLatestConfigNodes();
    this.protocolFactory = protocolFactory;
    this.connectionTimeout = connectionTimeout;
    this.clientManager = clientManager;

    init();
  }

  public void init() throws TException {
    reconnect();
  }

  public void connect(TEndPoint endpoint) throws TException {
    try {
      transport =
          RpcTransportFactory.INSTANCE.getTransport(
              // as there is a try-catch already, we do not need to use TSocket.wrap
              endpoint.getIp(), endpoint.getPort(), (int) connectionTimeout);
      transport.open();
      configNode = endpoint;
    } catch (TTransportException e) {
      throw new TException(e);
    }

    client = new IConfigNodeRPCService.Client(protocolFactory.getProtocol(transport));
  }

  private void reconnect() throws TException {
    try {
      tryToConnect();
    } catch (TException e) {
      // can not connect to each config node
      syncLatestConfigNodeList();
      tryToConnect();
    }
  }

  private void tryToConnect() throws TException {
    if (configLeader != null) {
      try {
        connect(configLeader);
        return;
      } catch (TException e) {
        logger.warn("The current node may have been down {},try next node", configLeader);
        configLeader = null;
      }
    }

    if (transport != null) {
      transport.close();
    }

    for (int tryHostNum = 0; tryHostNum < configNodes.size(); tryHostNum++) {
      cursor = (cursor + 1) % configNodes.size();
      TEndPoint tryEndpoint = configNodes.get(cursor);

      try {
        connect(tryEndpoint);
        return;
      } catch (TException e) {
        logger.warn("The current node may have been down {},try next node", tryEndpoint);
      }
    }

    throw new TException(MSG_RECONNECTION_FAIL);
  }

  public TTransport getTransport() {
    return transport;
  }

  public void syncLatestConfigNodeList() {
    configNodes = ConfigNodeInfo.getInstance().getLatestConfigNodes();
    cursor = 0;
  }

  @Override
  public void close() {
    if (clientManager != null) {
      clientManager.returnClient(partitionRegionId, this);
    } else {
      invalidate();
    }
  }

  @Override
  public void invalidate() {
    transport.close();
  }

  @Override
  public void invalidateAll() {
    clientManager.clear(ConfigNodeInfo.partitionRegionId);
  }

  private boolean updateConfigNodeLeader(TSStatus status) {
    if (status.getCode() == TSStatusCode.NEED_REDIRECTION.getStatusCode()) {
      if (status.isSetRedirectNode()) {
        configLeader =
            new TEndPoint(status.getRedirectNode().getIp(), status.getRedirectNode().getPort());
      } else {
        configLeader = null;
      }
      logger.warn(
          "Failed to connect to ConfigNode {} from DataNode {},because the current node is not leader,try next node",
          configNode,
          config.getAddressAndPort());
      return true;
    }
    return false;
  }

  @Override
  public TDataNodeRegisterResp registerDataNode(TDataNodeRegisterReq req) throws TException {
    for (int i = 0; i < RETRY_NUM; i++) {
      try {
        TDataNodeRegisterResp resp = client.registerDataNode(req);

        if (!updateConfigNodeLeader(resp.status)) {
          return resp;
        }

        // set latest config node list
        List<TEndPoint> newConfigNodes = new ArrayList<>();
        for (TConfigNodeLocation configNodeLocation : resp.getConfigNodeList()) {
          newConfigNodes.add(configNodeLocation.getInternalEndPoint());
        }
        configNodes = newConfigNodes;
      } catch (TException e) {
        configLeader = null;
      }
      reconnect();
    }
    throw new TException(MSG_RECONNECTION_FAIL);
  }

  @Override
  public TDataNodeRemoveResp removeDataNode(TDataNodeRemoveReq req) throws TException {
    for (int i = 0; i < RETRY_NUM; i++) {
      try {
        TDataNodeRemoveResp resp = client.removeDataNode(req);
        if (!updateConfigNodeLeader(resp.status)) {
          return resp;
        }
      } catch (TException e) {
        configLeader = null;
      }
      reconnect();
    }
    throw new TException(MSG_RECONNECTION_FAIL);
  }

  @Override
  public TDataNodeConfigurationResp getDataNodeConfiguration(int dataNodeId) throws TException {
    for (int i = 0; i < RETRY_NUM; i++) {
      try {
        TDataNodeConfigurationResp resp = client.getDataNodeConfiguration(dataNodeId);
        if (!updateConfigNodeLeader(resp.status)) {
          return resp;
        }
      } catch (TException e) {
        configLeader = null;
      }
      reconnect();
    }
    throw new TException(MSG_RECONNECTION_FAIL);
  }

  @Override
  public TSStatus reportRegionMigrateResult(TRegionMigrateResultReportReq req) throws TException {
    for (int i = 0; i < RETRY_NUM; i++) {
      try {
        TSStatus status = client.reportRegionMigrateResult(req);
        if (!updateConfigNodeLeader(status)) {
          return status;
        }
      } catch (TException e) {
        configLeader = null;
      }
      reconnect();
    }
    throw new TException(MSG_RECONNECTION_FAIL);
  }

  @Override
  public TShowClusterResp showCluster() throws TException {
    for (int i = 0; i < RETRY_NUM; i++) {
      try {
        TShowClusterResp resp = client.showCluster();
        if (!updateConfigNodeLeader(resp.status)) {
          return resp;
        }
      } catch (TException e) {
        configLeader = null;
      }
      reconnect();
    }
    throw new TException(MSG_RECONNECTION_FAIL);
  }

  @Override
  public TSStatus setStorageGroup(TSetStorageGroupReq req) throws TException {
    for (int i = 0; i < RETRY_NUM; i++) {
      try {
        TSStatus status = client.setStorageGroup(req);
        if (!updateConfigNodeLeader(status)) {
          return status;
        }
      } catch (TException e) {
        configLeader = null;
      }
      reconnect();
    }
    throw new TException(MSG_RECONNECTION_FAIL);
  }

  @Override
  public TSStatus deleteStorageGroup(TDeleteStorageGroupReq req) throws TException {
    for (int i = 0; i < RETRY_NUM; i++) {
      try {
        TSStatus status = client.deleteStorageGroup(req);
        if (!updateConfigNodeLeader(status)) {
          return status;
        }
      } catch (TException e) {
        configLeader = null;
      }
      reconnect();
    }
    throw new TException(MSG_RECONNECTION_FAIL);
  }

  @Override
  public TSStatus deleteStorageGroups(TDeleteStorageGroupsReq req) throws TException {
    for (int i = 0; i < RETRY_NUM; i++) {
      try {
        TSStatus status = client.deleteStorageGroups(req);
        if (!updateConfigNodeLeader(status)) {
          return status;
        }
      } catch (TException e) {
        configLeader = null;
      }
      reconnect();
    }
    throw new TException(MSG_RECONNECTION_FAIL);
  }

  @Override
  public TCountStorageGroupResp countMatchedStorageGroups(List<String> storageGroupPathPattern)
      throws TException {
    for (int i = 0; i < RETRY_NUM; i++) {
      try {
        TCountStorageGroupResp resp = client.countMatchedStorageGroups(storageGroupPathPattern);
        if (!updateConfigNodeLeader(resp.status)) {
          return resp;
        }
      } catch (TException e) {
        configLeader = null;
      }
      reconnect();
    }
    throw new TException(MSG_RECONNECTION_FAIL);
  }

  @Override
  public TStorageGroupSchemaResp getMatchedStorageGroupSchemas(List<String> storageGroupPathPattern)
      throws TException {
    for (int i = 0; i < RETRY_NUM; i++) {
      try {
        TStorageGroupSchemaResp resp =
            client.getMatchedStorageGroupSchemas(storageGroupPathPattern);
        if (!updateConfigNodeLeader(resp.status)) {
          return resp;
        }
      } catch (TException e) {
        configLeader = null;
      }
      reconnect();
    }
    throw new TException(MSG_RECONNECTION_FAIL);
  }

  @Override
  public TSStatus setTTL(TSetTTLReq setTTLReq) throws TException {
    for (int i = 0; i < RETRY_NUM; i++) {
      try {
        TSStatus status = client.setTTL(setTTLReq);
        if (!updateConfigNodeLeader(status)) {
          return status;
        }
      } catch (TException e) {
        configLeader = null;
      }
      reconnect();
    }
    throw new TException(MSG_RECONNECTION_FAIL);
  }

  @Override
  public TSStatus setSchemaReplicationFactor(TSetSchemaReplicationFactorReq req) throws TException {
    for (int i = 0; i < RETRY_NUM; i++) {
      try {
        TSStatus status = client.setSchemaReplicationFactor(req);
        if (!updateConfigNodeLeader(status)) {
          return status;
        }
      } catch (TException e) {
        configLeader = null;
      }
      reconnect();
    }
    throw new TException(MSG_RECONNECTION_FAIL);
  }

  @Override
  public TSStatus setDataReplicationFactor(TSetDataReplicationFactorReq req) throws TException {
    for (int i = 0; i < RETRY_NUM; i++) {
      try {
        TSStatus status = client.setDataReplicationFactor(req);
        if (!updateConfigNodeLeader(status)) {
          return status;
        }
      } catch (TException e) {
        configLeader = null;
      }
      reconnect();
    }
    throw new TException(MSG_RECONNECTION_FAIL);
  }

  @Override
  public TSStatus setTimePartitionInterval(TSetTimePartitionIntervalReq req) throws TException {
    for (int i = 0; i < RETRY_NUM; i++) {
      try {
        TSStatus status = client.setTimePartitionInterval(req);
        if (!updateConfigNodeLeader(status)) {
          return status;
        }
      } catch (TException e) {
        configLeader = null;
      }
      reconnect();
    }
    throw new TException(MSG_RECONNECTION_FAIL);
  }

  @Override
  public TSchemaPartitionResp getSchemaPartition(TSchemaPartitionReq req) throws TException {
    for (int i = 0; i < RETRY_NUM; i++) {
      try {
        TSchemaPartitionResp resp = client.getSchemaPartition(req);
        if (!updateConfigNodeLeader(resp.status)) {
          return resp;
        }
      } catch (TException e) {
        configLeader = null;
      }
      reconnect();
    }
    throw new TException(MSG_RECONNECTION_FAIL);
  }

  @Override
  public TSchemaPartitionTableResp getSchemaPartitionTable(TSchemaPartitionReq req)
      throws TException {
    for (int i = 0; i < RETRY_NUM; i++) {
      try {
        TSchemaPartitionTableResp resp = client.getSchemaPartitionTable(req);
        if (!updateConfigNodeLeader(resp.status)) {
          return resp;
        }
      } catch (TException e) {
        configLeader = null;
      }
      reconnect();
    }
    throw new TException(MSG_RECONNECTION_FAIL);
  }

  @Override
  public TSchemaPartitionResp getOrCreateSchemaPartition(TSchemaPartitionReq req)
      throws TException {
    for (int i = 0; i < RETRY_NUM; i++) {
      try {
        TSchemaPartitionResp resp = client.getOrCreateSchemaPartition(req);
        if (!updateConfigNodeLeader(resp.status)) {
          return resp;
        }
      } catch (TException e) {
        configLeader = null;
      }
      reconnect();
    }
    throw new TException(MSG_RECONNECTION_FAIL);
  }

  @Override
  public TSchemaPartitionTableResp getOrCreateSchemaPartitionTable(TSchemaPartitionReq req)
      throws TException {
    for (int i = 0; i < RETRY_NUM; i++) {
      try {
        TSchemaPartitionTableResp resp = client.getOrCreateSchemaPartitionTable(req);
        if (!updateConfigNodeLeader(resp.status)) {
          return resp;
        }
      } catch (TException e) {
        configLeader = null;
      }
      reconnect();
    }
    throw new TException(MSG_RECONNECTION_FAIL);
  }

  @Override
  public TSchemaNodeManagementResp getSchemaNodeManagementPartition(TSchemaNodeManagementReq req)
      throws TException {
    for (int i = 0; i < RETRY_NUM; i++) {
      try {
        TSchemaNodeManagementResp resp = client.getSchemaNodeManagementPartition(req);
        if (!updateConfigNodeLeader(resp.status)) {
          return resp;
        }
      } catch (TException e) {
        configLeader = null;
      }
      reconnect();
    }
    throw new TException(MSG_RECONNECTION_FAIL);
  }

  @Override
  public TDataPartitionResp getDataPartition(TDataPartitionReq req) throws TException {
    for (int i = 0; i < RETRY_NUM; i++) {
      try {
        TDataPartitionResp resp = client.getDataPartition(req);
        if (!updateConfigNodeLeader(resp.status)) {
          return resp;
        }
      } catch (TException e) {
        configLeader = null;
      }
      reconnect();
    }
    throw new TException(MSG_RECONNECTION_FAIL);
  }

  @Override
  public TDataPartitionTableResp getDataPartitionTable(TDataPartitionReq req) throws TException {
    for (int i = 0; i < RETRY_NUM; i++) {
      try {
        TDataPartitionTableResp resp = client.getDataPartitionTable(req);
        if (!updateConfigNodeLeader(resp.status)) {
          return resp;
        }
      } catch (TException e) {
        configLeader = null;
      }
      reconnect();
    }
    throw new TException(MSG_RECONNECTION_FAIL);
  }

  @Override
  public TDataPartitionResp getOrCreateDataPartition(TDataPartitionReq req) throws TException {
    for (int i = 0; i < RETRY_NUM; i++) {
      try {
        TDataPartitionResp resp = client.getOrCreateDataPartition(req);
        if (!updateConfigNodeLeader(resp.status)) {
          return resp;
        }
      } catch (TException e) {
        configLeader = null;
      }
      reconnect();
    }
    throw new TException(MSG_RECONNECTION_FAIL);
  }

  @Override
  public TDataPartitionTableResp getOrCreateDataPartitionTable(TDataPartitionReq req)
      throws TException {
    for (int i = 0; i < RETRY_NUM; i++) {
      try {
        TDataPartitionTableResp resp = client.getOrCreateDataPartitionTable(req);
        if (!updateConfigNodeLeader(resp.status)) {
          return resp;
        }
      } catch (TException e) {
        configLeader = null;
      }
      reconnect();
    }
    throw new TException(MSG_RECONNECTION_FAIL);
  }

  @Override
  public TSStatus operatePermission(TAuthorizerReq req) throws TException {
    for (int i = 0; i < RETRY_NUM; i++) {
      try {
        TSStatus status = client.operatePermission(req);
        if (!updateConfigNodeLeader(status)) {
          return status;
        }
      } catch (TException e) {
        configLeader = null;
      }
      reconnect();
    }
    throw new TException(MSG_RECONNECTION_FAIL);
  }

  @Override
  public TAuthorizerResp queryPermission(TAuthorizerReq req) throws TException {
    for (int i = 0; i < RETRY_NUM; i++) {
      try {
        TAuthorizerResp resp = client.queryPermission(req);
        if (!updateConfigNodeLeader(resp.status)) {
          return resp;
        }
      } catch (TException e) {
        configLeader = null;
      }
      reconnect();
    }
    throw new TException(MSG_RECONNECTION_FAIL);
  }

  @Override
  public TPermissionInfoResp login(TLoginReq req) throws TException {
    for (int i = 0; i < RETRY_NUM; i++) {
      try {
        TPermissionInfoResp status = client.login(req);
        if (!updateConfigNodeLeader(status.getStatus())) {
          return status;
        }
      } catch (TException e) {
        configLeader = null;
      }
      reconnect();
    }
    throw new TException(MSG_RECONNECTION_FAIL);
  }

  @Override
  public TPermissionInfoResp checkUserPrivileges(TCheckUserPrivilegesReq req) throws TException {
    for (int i = 0; i < RETRY_NUM; i++) {
      try {
        TPermissionInfoResp status = client.checkUserPrivileges(req);
        if (!updateConfigNodeLeader(status.getStatus())) {
          return status;
        }
      } catch (TException e) {
        configLeader = null;
      }
      reconnect();
    }
    throw new TException(MSG_RECONNECTION_FAIL);
  }

  @Override
  public TSStatus registerConfigNode(TConfigNodeRegisterReq req) throws TException {
    throw new TException("DataNode to ConfigNode client doesn't support registerConfigNode.");
  }

  @Override
  public TSStatus addConsensusGroup(TAddConsensusGroupReq registerResp) throws TException {
    throw new TException("DataNode to ConfigNode client doesn't support addConsensusGroup.");
  }

  @Override
  public TSStatus notifyRegisterSuccess() throws TException {
    throw new TException("DataNode to ConfigNode client doesn't support notifyRegisterSuccess.");
  }

  @Override
  public TSStatus removeConfigNode(TConfigNodeLocation configNodeLocation) throws TException {
    throw new TException("DataNode to ConfigNode client doesn't support removeConfigNode.");
  }

  @Override
  public TSStatus removeConsensusGroup(TConfigNodeLocation configNodeLocation) throws TException {
    throw new TException("DataNode to ConfigNode client doesn't support removeConsensusGroup.");
  }

  @Override
  public TSStatus stopConfigNode(TConfigNodeLocation configNodeLocation) throws TException {
    throw new TException("DataNode to ConfigNode client doesn't support stopConfigNode.");
  }

  @Override
  public TSStatus createFunction(TCreateFunctionReq req) throws TException {
    for (int i = 0; i < RETRY_NUM; i++) {
      try {
        TSStatus status = client.createFunction(req);
        if (!updateConfigNodeLeader(status)) {
          return status;
        }
      } catch (TException e) {
        configLeader = null;
      }
      reconnect();
    }
    throw new TException(MSG_RECONNECTION_FAIL);
  }

  @Override
  public TSStatus merge() throws TException {
    for (int i = 0; i < RETRY_NUM; i++) {
      try {
        TSStatus status = client.merge();
        if (!updateConfigNodeLeader(status)) {
          return status;
        }
      } catch (TException e) {
        configLeader = null;
      }
      reconnect();
    }
    throw new TException(MSG_RECONNECTION_FAIL);
  }

  @Override
  public TSStatus flush(TFlushReq req) throws TException {
    for (int i = 0; i < RETRY_NUM; i++) {
      try {
        TSStatus status = client.flush(req);
        if (!updateConfigNodeLeader(status)) {
          return status;
        }
      } catch (TException e) {
        configLeader = null;
      }
      reconnect();
    }
    throw new TException(MSG_RECONNECTION_FAIL);
  }

  @Override
  public TSStatus clearCache() throws TException {
    for (int i = 0; i < RETRY_NUM; i++) {
      try {
        TSStatus status = client.clearCache();
        if (!updateConfigNodeLeader(status)) {
          return status;
        }
      } catch (TException e) {
        configLeader = null;
      }
      reconnect();
    }
    throw new TException(MSG_RECONNECTION_FAIL);
  }

  @Override
  public TSStatus loadConfiguration() throws TException {
    for (int i = 0; i < RETRY_NUM; i++) {
      try {
        TSStatus status = client.loadConfiguration();
        if (!updateConfigNodeLeader(status)) {
          return status;
        }
      } catch (TException e) {
        configLeader = null;
      }
      reconnect();
    }
    throw new TException(MSG_RECONNECTION_FAIL);
  }

  @Override
  public TShowRegionResp showRegion(TShowRegionReq req) throws TException {
    for (int i = 0; i < RETRY_NUM; i++) {
      try {
        TShowRegionResp showRegionResp = client.showRegion(req);
        if (!updateConfigNodeLeader(showRegionResp.getStatus())) {
          return showRegionResp;
        }
      } catch (TException e) {
        configLeader = null;
      }
      reconnect();
    }
    throw new TException(MSG_RECONNECTION_FAIL);
  }

  @Override
  public TShowDataNodesResp showDataNodes() throws TException {
    for (int i = 0; i < RETRY_NUM; i++) {
      try {
        TShowDataNodesResp showDataNodesResp = client.showDataNodes();
        showDataNodesResp.setStatus(showDataNodesResp.getStatus());
        if (!updateConfigNodeLeader(showDataNodesResp.getStatus())) {
          return showDataNodesResp;
        }
      } catch (TException e) {
        configLeader = null;
      }
      reconnect();
    }
    throw new TException(MSG_RECONNECTION_FAIL);
  }

  @Override
  public TShowConfigNodesResp showConfigNodes() throws TException {
    for (int i = 0; i < RETRY_NUM; i++) {
      try {
        TShowConfigNodesResp showConfigNodesResp = client.showConfigNodes();
        if (!updateConfigNodeLeader(showConfigNodesResp.getStatus())) {
          return showConfigNodesResp;
        }
      } catch (TException e) {
        configLeader = null;
      }
      reconnect();
    }
    throw new TException(MSG_RECONNECTION_FAIL);
  }

  @Override
  public TRegionRouteMapResp getLatestRegionRouteMap() throws TException {
    for (int i = 0; i < RETRY_NUM; i++) {
      try {
        TRegionRouteMapResp regionRouteMapResp = client.getLatestRegionRouteMap();
        if (!updateConfigNodeLeader(regionRouteMapResp.getStatus())) {
          return regionRouteMapResp;
        }
      } catch (TException e) {
        configLeader = null;
      }
      reconnect();
    }
    throw new TException(MSG_RECONNECTION_FAIL);
  }

  @Override
  public long getConfigNodeHeartBeat(long timestamp) throws TException {
    throw new TException("DataNode to ConfigNode client doesn't support getConfigNodeHeartBeat.");
  }

  @Override
  public TSStatus dropFunction(TDropFunctionReq req) throws TException {
    for (int i = 0; i < RETRY_NUM; i++) {
      try {
        TSStatus status = client.dropFunction(req);
        if (!updateConfigNodeLeader(status)) {
          return status;
        }
      } catch (TException e) {
        configLeader = null;
      }
      reconnect();
    }
    throw new TException(MSG_RECONNECTION_FAIL);
  }

  @Override
  public TSStatus createSchemaTemplate(TCreateSchemaTemplateReq req) throws TException {
    for (int i = 0; i < RETRY_NUM; i++) {
      try {
        TSStatus tsStatus = client.createSchemaTemplate(req);
        if (!updateConfigNodeLeader(tsStatus)) {
          return tsStatus;
        }
      } catch (TException e) {
        configLeader = null;
      }
      reconnect();
    }
    throw new TException(MSG_RECONNECTION_FAIL);
  }

  @Override
  public TGetAllTemplatesResp getAllTemplates() throws TException {
    for (int i = 0; i < RETRY_NUM; i++) {
      try {
        TGetAllTemplatesResp resp = client.getAllTemplates();
        if (!updateConfigNodeLeader(resp.getStatus())) {
          return resp;
        }
      } catch (TException e) {
        configLeader = null;
      }
      reconnect();
    }
    throw new TException(MSG_RECONNECTION_FAIL);
  }

  @Override
  public TGetTemplateResp getTemplate(String req) throws TException {
    for (int i = 0; i < RETRY_NUM; i++) {
      try {
        TGetTemplateResp resp = client.getTemplate(req);
        if (!updateConfigNodeLeader(resp.getStatus())) {
          return resp;
        }
      } catch (TException e) {
        configLeader = null;
      }
      reconnect();
    }
    throw new TException(MSG_RECONNECTION_FAIL);
  }

  @Override
  public TSStatus setSchemaTemplate(TSetSchemaTemplateReq req) throws TException {
    for (int i = 0; i < RETRY_NUM; i++) {
      try {
        TSStatus tsStatus = client.setSchemaTemplate(req);
        if (!updateConfigNodeLeader(tsStatus)) {
          return tsStatus;
        }
      } catch (TException e) {
        configLeader = null;
      }
      reconnect();
    }
    throw new TException(MSG_RECONNECTION_FAIL);
  }

  @Override
  public TGetPathsSetTemplatesResp getPathsSetTemplate(String req) throws TException {
    for (int i = 0; i < RETRY_NUM; i++) {
      try {
        TGetPathsSetTemplatesResp tGetPathsSetTemplatesResp = client.getPathsSetTemplate(req);
        if (!updateConfigNodeLeader(tGetPathsSetTemplatesResp.getStatus())) {
          return tGetPathsSetTemplatesResp;
        }
      } catch (TException e) {
        configLeader = null;
      }
      reconnect();
    }
    throw new TException(MSG_RECONNECTION_FAIL);
  }

  public static class Factory extends BaseClientFactory<PartitionRegionId, ConfigNodeClient> {

    public Factory(
        ClientManager<PartitionRegionId, ConfigNodeClient> clientManager,
        ClientFactoryProperty clientFactoryProperty) {
      super(clientManager, clientFactoryProperty);
    }

    @Override
    public void destroyObject(
        PartitionRegionId partitionRegionId, PooledObject<ConfigNodeClient> pooledObject) {
      pooledObject.getObject().invalidate();
    }

    @Override
    public PooledObject<ConfigNodeClient> makeObject(PartitionRegionId partitionRegionId)
        throws Exception {
      Constructor<ConfigNodeClient> constructor =
          ConfigNodeClient.class.getConstructor(
              TProtocolFactory.class, long.class, clientManager.getClass());
      return new DefaultPooledObject<>(
          SyncThriftClientWithErrorHandler.newErrorHandler(
              ConfigNodeClient.class,
              constructor,
              clientFactoryProperty.getProtocolFactory(),
              clientFactoryProperty.getConnectionTimeoutMs(),
              clientManager));
    }

    @Override
    public boolean validateObject(
        PartitionRegionId partitionRegionId, PooledObject<ConfigNodeClient> pooledObject) {
      return pooledObject.getObject() != null && pooledObject.getObject().getTransport().isOpen();
    }
  }
}
