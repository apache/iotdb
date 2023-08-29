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

package org.apache.iotdb.db.protocol.client;

import org.apache.iotdb.common.rpc.thrift.TConfigNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TFlushReq;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.common.rpc.thrift.TSetSpaceQuotaReq;
import org.apache.iotdb.common.rpc.thrift.TSetTTLReq;
import org.apache.iotdb.common.rpc.thrift.TSetThrottleQuotaReq;
import org.apache.iotdb.commons.client.ClientManager;
import org.apache.iotdb.commons.client.ThriftClient;
import org.apache.iotdb.commons.client.factory.ThriftClientFactory;
import org.apache.iotdb.commons.client.property.ThriftClientProperty;
import org.apache.iotdb.commons.client.sync.SyncThriftClientWithErrorHandler;
import org.apache.iotdb.commons.consensus.ConfigRegionId;
import org.apache.iotdb.confignode.rpc.thrift.IConfigNodeRPCService;
import org.apache.iotdb.confignode.rpc.thrift.TAddConsensusGroupReq;
import org.apache.iotdb.confignode.rpc.thrift.TAlterLogicalViewReq;
import org.apache.iotdb.confignode.rpc.thrift.TAlterSchemaTemplateReq;
import org.apache.iotdb.confignode.rpc.thrift.TAuthizedPatternTreeResp;
import org.apache.iotdb.confignode.rpc.thrift.TAuthorizerReq;
import org.apache.iotdb.confignode.rpc.thrift.TAuthorizerResp;
import org.apache.iotdb.confignode.rpc.thrift.TCheckUserPrivilegesReq;
import org.apache.iotdb.confignode.rpc.thrift.TConfigNodeRegisterReq;
import org.apache.iotdb.confignode.rpc.thrift.TConfigNodeRegisterResp;
import org.apache.iotdb.confignode.rpc.thrift.TConfigNodeRestartReq;
import org.apache.iotdb.confignode.rpc.thrift.TCountDatabaseResp;
import org.apache.iotdb.confignode.rpc.thrift.TCountTimeSlotListReq;
import org.apache.iotdb.confignode.rpc.thrift.TCountTimeSlotListResp;
import org.apache.iotdb.confignode.rpc.thrift.TCreateCQReq;
import org.apache.iotdb.confignode.rpc.thrift.TCreateFunctionReq;
import org.apache.iotdb.confignode.rpc.thrift.TCreateModelReq;
import org.apache.iotdb.confignode.rpc.thrift.TCreatePipePluginReq;
import org.apache.iotdb.confignode.rpc.thrift.TCreatePipeReq;
import org.apache.iotdb.confignode.rpc.thrift.TCreateSchemaTemplateReq;
import org.apache.iotdb.confignode.rpc.thrift.TCreateTriggerReq;
import org.apache.iotdb.confignode.rpc.thrift.TDataNodeConfigurationResp;
import org.apache.iotdb.confignode.rpc.thrift.TDataNodeRegisterReq;
import org.apache.iotdb.confignode.rpc.thrift.TDataNodeRegisterResp;
import org.apache.iotdb.confignode.rpc.thrift.TDataNodeRemoveReq;
import org.apache.iotdb.confignode.rpc.thrift.TDataNodeRemoveResp;
import org.apache.iotdb.confignode.rpc.thrift.TDataNodeRestartReq;
import org.apache.iotdb.confignode.rpc.thrift.TDataNodeRestartResp;
import org.apache.iotdb.confignode.rpc.thrift.TDataPartitionReq;
import org.apache.iotdb.confignode.rpc.thrift.TDataPartitionTableResp;
import org.apache.iotdb.confignode.rpc.thrift.TDatabaseSchema;
import org.apache.iotdb.confignode.rpc.thrift.TDatabaseSchemaResp;
import org.apache.iotdb.confignode.rpc.thrift.TDeactivateSchemaTemplateReq;
import org.apache.iotdb.confignode.rpc.thrift.TDeleteDatabaseReq;
import org.apache.iotdb.confignode.rpc.thrift.TDeleteDatabasesReq;
import org.apache.iotdb.confignode.rpc.thrift.TDeleteLogicalViewReq;
import org.apache.iotdb.confignode.rpc.thrift.TDeleteTimeSeriesReq;
import org.apache.iotdb.confignode.rpc.thrift.TDropCQReq;
import org.apache.iotdb.confignode.rpc.thrift.TDropFunctionReq;
import org.apache.iotdb.confignode.rpc.thrift.TDropModelReq;
import org.apache.iotdb.confignode.rpc.thrift.TDropPipePluginReq;
import org.apache.iotdb.confignode.rpc.thrift.TDropPipeSinkReq;
import org.apache.iotdb.confignode.rpc.thrift.TDropTriggerReq;
import org.apache.iotdb.confignode.rpc.thrift.TGetAllPipeInfoResp;
import org.apache.iotdb.confignode.rpc.thrift.TGetAllTemplatesResp;
import org.apache.iotdb.confignode.rpc.thrift.TGetDataNodeLocationsResp;
import org.apache.iotdb.confignode.rpc.thrift.TGetDatabaseReq;
import org.apache.iotdb.confignode.rpc.thrift.TGetJarInListReq;
import org.apache.iotdb.confignode.rpc.thrift.TGetJarInListResp;
import org.apache.iotdb.confignode.rpc.thrift.TGetLocationForTriggerResp;
import org.apache.iotdb.confignode.rpc.thrift.TGetPathsSetTemplatesReq;
import org.apache.iotdb.confignode.rpc.thrift.TGetPathsSetTemplatesResp;
import org.apache.iotdb.confignode.rpc.thrift.TGetPipePluginTableResp;
import org.apache.iotdb.confignode.rpc.thrift.TGetPipeSinkReq;
import org.apache.iotdb.confignode.rpc.thrift.TGetPipeSinkResp;
import org.apache.iotdb.confignode.rpc.thrift.TGetRegionIdReq;
import org.apache.iotdb.confignode.rpc.thrift.TGetRegionIdResp;
import org.apache.iotdb.confignode.rpc.thrift.TGetSeriesSlotListReq;
import org.apache.iotdb.confignode.rpc.thrift.TGetSeriesSlotListResp;
import org.apache.iotdb.confignode.rpc.thrift.TGetTemplateResp;
import org.apache.iotdb.confignode.rpc.thrift.TGetTimeSlotListReq;
import org.apache.iotdb.confignode.rpc.thrift.TGetTimeSlotListResp;
import org.apache.iotdb.confignode.rpc.thrift.TGetTriggerTableResp;
import org.apache.iotdb.confignode.rpc.thrift.TGetUDFTableResp;
import org.apache.iotdb.confignode.rpc.thrift.TLoginReq;
import org.apache.iotdb.confignode.rpc.thrift.TMigrateRegionReq;
import org.apache.iotdb.confignode.rpc.thrift.TPermissionInfoResp;
import org.apache.iotdb.confignode.rpc.thrift.TPipeSinkInfo;
import org.apache.iotdb.confignode.rpc.thrift.TRegionMigrateResultReportReq;
import org.apache.iotdb.confignode.rpc.thrift.TRegionRouteMapResp;
import org.apache.iotdb.confignode.rpc.thrift.TSchemaNodeManagementReq;
import org.apache.iotdb.confignode.rpc.thrift.TSchemaNodeManagementResp;
import org.apache.iotdb.confignode.rpc.thrift.TSchemaPartitionReq;
import org.apache.iotdb.confignode.rpc.thrift.TSchemaPartitionTableResp;
import org.apache.iotdb.confignode.rpc.thrift.TSetDataNodeStatusReq;
import org.apache.iotdb.confignode.rpc.thrift.TSetDataReplicationFactorReq;
import org.apache.iotdb.confignode.rpc.thrift.TSetSchemaReplicationFactorReq;
import org.apache.iotdb.confignode.rpc.thrift.TSetSchemaTemplateReq;
import org.apache.iotdb.confignode.rpc.thrift.TSetTimePartitionIntervalReq;
import org.apache.iotdb.confignode.rpc.thrift.TShowCQResp;
import org.apache.iotdb.confignode.rpc.thrift.TShowClusterResp;
import org.apache.iotdb.confignode.rpc.thrift.TShowConfigNodesResp;
import org.apache.iotdb.confignode.rpc.thrift.TShowDataNodesResp;
import org.apache.iotdb.confignode.rpc.thrift.TShowDatabaseResp;
import org.apache.iotdb.confignode.rpc.thrift.TShowModelReq;
import org.apache.iotdb.confignode.rpc.thrift.TShowModelResp;
import org.apache.iotdb.confignode.rpc.thrift.TShowPipeReq;
import org.apache.iotdb.confignode.rpc.thrift.TShowPipeResp;
import org.apache.iotdb.confignode.rpc.thrift.TShowRegionReq;
import org.apache.iotdb.confignode.rpc.thrift.TShowRegionResp;
import org.apache.iotdb.confignode.rpc.thrift.TShowThrottleReq;
import org.apache.iotdb.confignode.rpc.thrift.TShowTrailReq;
import org.apache.iotdb.confignode.rpc.thrift.TShowTrailResp;
import org.apache.iotdb.confignode.rpc.thrift.TShowVariablesResp;
import org.apache.iotdb.confignode.rpc.thrift.TSpaceQuotaResp;
import org.apache.iotdb.confignode.rpc.thrift.TSystemConfigurationResp;
import org.apache.iotdb.confignode.rpc.thrift.TThrottleQuotaResp;
import org.apache.iotdb.confignode.rpc.thrift.TUnsetSchemaTemplateReq;
import org.apache.iotdb.confignode.rpc.thrift.TUpdateModelInfoReq;
import org.apache.iotdb.confignode.rpc.thrift.TUpdateModelStateReq;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.rpc.RpcTransportFactory;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Predicate;

public class ConfigNodeClient implements IConfigNodeRPCService.Iface, ThriftClient, AutoCloseable {

  private static final Logger logger = LoggerFactory.getLogger(ConfigNodeClient.class);

  private static final int RETRY_NUM = 5;

  public static final String MSG_RECONNECTION_FAIL =
      "Fail to connect to any config node. Please check status of ConfigNodes or logs of connected DataNode";

  private static final String MSG_RECONNECTION_DATANODE_FAIL =
      "Failed to connect to ConfigNode %s from DataNode %s when executing %s, Exception:";
  private static final int RETRY_INTERVAL_MS = 2000;

  private final ThriftClientProperty property;

  private IConfigNodeRPCService.Iface client;

  private TTransport transport;

  private TEndPoint configLeader;

  private List<TEndPoint> configNodes;

  private TEndPoint configNode;

  private int cursor = 0;

  private final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();

  ClientManager<ConfigRegionId, ConfigNodeClient> clientManager;

  ConfigRegionId configRegionId = ConfigNodeInfo.CONFIG_REGION_ID;

  public ConfigNodeClient(
      List<TEndPoint> configNodes,
      ThriftClientProperty property,
      ClientManager<ConfigRegionId, ConfigNodeClient> clientManager)
      throws TException {
    this.configNodes = configNodes;
    this.property = property;
    this.clientManager = clientManager;

    init();
  }

  public void init() throws TException {
    try {
      tryToConnect();
    } catch (TException e) {
      // Can not connect to each config node
      syncLatestConfigNodeList();
      tryToConnect();
    }
  }

  public void connect(TEndPoint endpoint) throws TException {
    try {
      transport =
          RpcTransportFactory.INSTANCE.getTransport(
              // As there is a try-catch already, we do not need to use TSocket.wrap
              endpoint.getIp(), endpoint.getPort(), property.getConnectionTimeoutMs());
      if (!transport.isOpen()) {
        transport.open();
      }
      configNode = endpoint;
    } catch (TTransportException e) {
      throw new TException(e);
    }

    client = new IConfigNodeRPCService.Client(property.getProtocolFactory().getProtocol(transport));
  }

  private void waitAndReconnect() throws TException {
    try {
      // Wait to start the next try
      Thread.sleep(RETRY_INTERVAL_MS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new TException(
          "Unexpected interruption when waiting to retry to connect to ConfigNode");
    }

    try {
      tryToConnect();
    } catch (TException e) {
      // Can not connect to each config node
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
    clientManager.returnClient(configRegionId, this);
  }

  @Override
  public void invalidate() {
    Optional.ofNullable(transport).ifPresent(TTransport::close);
  }

  @Override
  public void invalidateAll() {
    clientManager.clear(ConfigNodeInfo.CONFIG_REGION_ID);
  }

  @Override
  public boolean printLogWhenEncounterException() {
    return property.isPrintLogWhenEncounterException();
  }

  private boolean updateConfigNodeLeader(TSStatus status) {
    if (status.getCode() == TSStatusCode.REDIRECTION_RECOMMEND.getStatusCode()) {
      if (status.isSetRedirectNode()) {
        configLeader =
            new TEndPoint(status.getRedirectNode().getIp(), status.getRedirectNode().getPort());
      } else {
        configLeader = null;
      }
      logger.warn(
          "Failed to connect to ConfigNode {} from DataNode {}, because the current node is not leader, try next node",
          configNode,
          config.getAddressAndPort());
      return true;
    }
    return false;
  }

  /**
   * The frame of execute RPC, include logic of retry and exception handling.
   *
   * @param call which rpc should call
   * @param check check the rpc's result
   * @return rpc's result
   * @param <T> the type of rpc result
   * @throws TException if fails more than RETRY_NUM times, throw TException(MSG_RECONNECTION_FAIL)
   */
  private <T> T executeRemoteCallWithRetry(Operation<T> call, Predicate<T> check)
      throws TException {
    for (int i = 0; i < RETRY_NUM; i++) {
      try {
        T result = call.execute();
        if (check.test(result)) {
          return result;
        }
      } catch (TException e) {
        String message =
            String.format(
                MSG_RECONNECTION_DATANODE_FAIL,
                configNode,
                config.getAddressAndPort(),
                Thread.currentThread().getStackTrace()[2].getMethodName());
        logger.warn(message, e);
        configLeader = null;
      }
      waitAndReconnect();
    }
    throw new TException(MSG_RECONNECTION_FAIL);
  }

  @FunctionalInterface
  private interface Operation<T> {
    T execute() throws TException;
  }

  @Override
  public TSystemConfigurationResp getSystemConfiguration() throws TException {
    return executeRemoteCallWithRetry(
        () -> client.getSystemConfiguration(), resp -> !updateConfigNodeLeader(resp.status));
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
        String message =
            String.format(
                MSG_RECONNECTION_DATANODE_FAIL,
                configNode,
                config.getAddressAndPort(),
                Thread.currentThread().getStackTrace()[1].getMethodName());
        logger.warn(message, e);
        configLeader = null;
      }
      waitAndReconnect();
    }
    throw new TException(MSG_RECONNECTION_FAIL);
  }

  @Override
  public TDataNodeRestartResp restartDataNode(TDataNodeRestartReq req) throws TException {
    return executeRemoteCallWithRetry(
        () -> client.restartDataNode(req), resp -> !updateConfigNodeLeader(resp.status));
  }

  @Override
  public TDataNodeRemoveResp removeDataNode(TDataNodeRemoveReq req) throws TException {
    return executeRemoteCallWithRetry(
        () -> client.removeDataNode(req), resp -> !updateConfigNodeLeader(resp.status));
  }

  @Override
  public TSStatus reportDataNodeShutdown(TDataNodeLocation dataNodeLocation) throws TException {
    return executeRemoteCallWithRetry(
        () -> client.reportDataNodeShutdown(dataNodeLocation),
        status -> !updateConfigNodeLeader(status));
  }

  @Override
  public TDataNodeConfigurationResp getDataNodeConfiguration(int dataNodeId) throws TException {
    return executeRemoteCallWithRetry(
        () -> client.getDataNodeConfiguration(dataNodeId),
        resp -> !updateConfigNodeLeader(resp.status));
  }

  @Override
  public TSStatus reportRegionMigrateResult(TRegionMigrateResultReportReq req) throws TException {
    return executeRemoteCallWithRetry(
        () -> client.reportRegionMigrateResult(req), status -> !updateConfigNodeLeader(status));
  }

  @Override
  public TShowClusterResp showCluster() throws TException {
    return executeRemoteCallWithRetry(
        () -> client.showCluster(), resp -> !updateConfigNodeLeader(resp.status));
  }

  @Override
  public TShowVariablesResp showVariables() throws TException {
    return executeRemoteCallWithRetry(
        () -> client.showVariables(), resp -> !updateConfigNodeLeader(resp.status));
  }

  @Override
  public TSStatus setDatabase(TDatabaseSchema databaseSchema) throws TException {
    return executeRemoteCallWithRetry(
        () -> client.setDatabase(databaseSchema), status -> !updateConfigNodeLeader(status));
  }

  @Override
  public TSStatus alterDatabase(TDatabaseSchema databaseSchema) throws TException {
    return executeRemoteCallWithRetry(
        () -> client.alterDatabase(databaseSchema), status -> !updateConfigNodeLeader(status));
  }

  @Override
  public TSStatus deleteDatabase(TDeleteDatabaseReq req) throws TException {
    return executeRemoteCallWithRetry(
        () -> client.deleteDatabase(req), status -> !updateConfigNodeLeader(status));
  }

  @Override
  public TSStatus deleteDatabases(TDeleteDatabasesReq req) throws TException {
    return executeRemoteCallWithRetry(
        () -> client.deleteDatabases(req), status -> !updateConfigNodeLeader(status));
  }

  @Override
  public TCountDatabaseResp countMatchedDatabases(TGetDatabaseReq req) throws TException {
    return executeRemoteCallWithRetry(
        () -> client.countMatchedDatabases(req), resp -> !updateConfigNodeLeader(resp.status));
  }

  @Override
  public TDatabaseSchemaResp getMatchedDatabaseSchemas(TGetDatabaseReq req) throws TException {
    return executeRemoteCallWithRetry(
        () -> client.getMatchedDatabaseSchemas(req), resp -> !updateConfigNodeLeader(resp.status));
  }

  @Override
  public TSStatus setTTL(TSetTTLReq setTTLReq) throws TException {
    return executeRemoteCallWithRetry(
        () -> client.setTTL(setTTLReq), status -> !updateConfigNodeLeader(status));
  }

  @Override
  public TSStatus setSchemaReplicationFactor(TSetSchemaReplicationFactorReq req) throws TException {
    return executeRemoteCallWithRetry(
        () -> client.setSchemaReplicationFactor(req), status -> !updateConfigNodeLeader(status));
  }

  @Override
  public TSStatus setDataReplicationFactor(TSetDataReplicationFactorReq req) throws TException {
    return executeRemoteCallWithRetry(
        () -> client.setDataReplicationFactor(req), status -> !updateConfigNodeLeader(status));
  }

  @Override
  public TSStatus setTimePartitionInterval(TSetTimePartitionIntervalReq req) throws TException {
    return executeRemoteCallWithRetry(
        () -> client.setTimePartitionInterval(req), status -> !updateConfigNodeLeader(status));
  }

  @Override
  public TSchemaPartitionTableResp getSchemaPartitionTable(TSchemaPartitionReq req)
      throws TException {
    return executeRemoteCallWithRetry(
        () -> client.getSchemaPartitionTable(req), resp -> !updateConfigNodeLeader(resp.status));
  }

  @Override
  public TSchemaPartitionTableResp getOrCreateSchemaPartitionTable(TSchemaPartitionReq req)
      throws TException {
    return executeRemoteCallWithRetry(
        () -> client.getOrCreateSchemaPartitionTable(req),
        resp -> !updateConfigNodeLeader(resp.status));
  }

  @Override
  public TSchemaNodeManagementResp getSchemaNodeManagementPartition(TSchemaNodeManagementReq req)
      throws TException {
    return executeRemoteCallWithRetry(
        () -> client.getSchemaNodeManagementPartition(req),
        resp -> !updateConfigNodeLeader(resp.status));
  }

  @Override
  public TDataPartitionTableResp getDataPartitionTable(TDataPartitionReq req) throws TException {
    return executeRemoteCallWithRetry(
        () -> client.getDataPartitionTable(req), resp -> !updateConfigNodeLeader(resp.status));
  }

  @Override
  public TDataPartitionTableResp getOrCreateDataPartitionTable(TDataPartitionReq req)
      throws TException {
    return executeRemoteCallWithRetry(
        () -> client.getOrCreateDataPartitionTable(req),
        resp -> !updateConfigNodeLeader(resp.status));
  }

  @Override
  public TSStatus operatePermission(TAuthorizerReq req) throws TException {
    return executeRemoteCallWithRetry(
        () -> client.operatePermission(req), status -> !updateConfigNodeLeader(status));
  }

  @Override
  public TAuthorizerResp queryPermission(TAuthorizerReq req) throws TException {
    return executeRemoteCallWithRetry(
        () -> client.queryPermission(req), resp -> !updateConfigNodeLeader(resp.status));
  }

  @Override
  public TPermissionInfoResp login(TLoginReq req) throws TException {
    return executeRemoteCallWithRetry(
        () -> client.login(req), resp -> !updateConfigNodeLeader(resp.status));
  }

  @Override
  public TPermissionInfoResp checkUserPrivileges(TCheckUserPrivilegesReq req) throws TException {
    return executeRemoteCallWithRetry(
        () -> client.checkUserPrivileges(req), resp -> !updateConfigNodeLeader(resp.status));
  }

  @Override
  public TAuthizedPatternTreeResp fetchAuthizedPatternTree(TCheckUserPrivilegesReq req)
      throws TException {
    return executeRemoteCallWithRetry(
        () -> client.fetchAuthizedPatternTree(req), resp -> !updateConfigNodeLeader(resp.status));
  }

  @Override
  public TConfigNodeRegisterResp registerConfigNode(TConfigNodeRegisterReq req) throws TException {
    throw new TException("DataNode to ConfigNode client doesn't support registerConfigNode.");
  }

  @Override
  public TSStatus restartConfigNode(TConfigNodeRestartReq req) throws TException {
    throw new TException("DataNode to ConfigNode client doesn't support restartConfigNode.");
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
  public TSStatus deleteConfigNodePeer(TConfigNodeLocation configNodeLocation) throws TException {
    throw new TException("DataNode to ConfigNode client doesn't support removeConsensusGroup.");
  }

  @Override
  public TSStatus reportConfigNodeShutdown(TConfigNodeLocation configNodeLocation)
      throws TException {
    throw new TException("DataNode to ConfigNode client doesn't support reportConfigNodeShutdown.");
  }

  @Override
  public TSStatus stopConfigNode(TConfigNodeLocation configNodeLocation) throws TException {
    throw new TException("DataNode to ConfigNode client doesn't support stopConfigNode.");
  }

  @Override
  public TSStatus merge() throws TException {
    return executeRemoteCallWithRetry(
        () -> client.merge(), status -> !updateConfigNodeLeader(status));
  }

  @Override
  public TSStatus flush(TFlushReq req) throws TException {
    return executeRemoteCallWithRetry(
        () -> client.flush(req), status -> !updateConfigNodeLeader(status));
  }

  @Override
  public TSStatus clearCache() throws TException {
    return executeRemoteCallWithRetry(
        () -> client.clearCache(), status -> !updateConfigNodeLeader(status));
  }

  @Override
  public TSStatus loadConfiguration() throws TException {
    return executeRemoteCallWithRetry(
        () -> client.loadConfiguration(), status -> !updateConfigNodeLeader(status));
  }

  @Override
  public TSStatus setSystemStatus(String systemStatus) throws TException {
    return executeRemoteCallWithRetry(
        () -> client.setSystemStatus(systemStatus), status -> !updateConfigNodeLeader(status));
  }

  @Override
  public TSStatus setDataNodeStatus(TSetDataNodeStatusReq req) throws TException {
    throw new TException("DataNode to ConfigNode client doesn't support setDataNodeStatus.");
  }

  @Override
  public TSStatus killQuery(String queryId, int dataNodeId) throws TException {
    return executeRemoteCallWithRetry(
        () -> client.killQuery(queryId, dataNodeId), status -> !updateConfigNodeLeader(status));
  }

  @Override
  public TGetDataNodeLocationsResp getRunningDataNodeLocations() throws TException {
    return executeRemoteCallWithRetry(
        () -> client.getRunningDataNodeLocations(), resp -> !updateConfigNodeLeader(resp.status));
  }

  @Override
  public TShowRegionResp showRegion(TShowRegionReq req) throws TException {
    return executeRemoteCallWithRetry(
        () -> client.showRegion(req), resp -> !updateConfigNodeLeader(resp.status));
  }

  @Override
  public TShowDataNodesResp showDataNodes() throws TException {
    return executeRemoteCallWithRetry(
        () -> client.showDataNodes(), resp -> !updateConfigNodeLeader(resp.status));
  }

  @Override
  public TShowConfigNodesResp showConfigNodes() throws TException {
    return executeRemoteCallWithRetry(
        () -> client.showConfigNodes(), resp -> !updateConfigNodeLeader(resp.status));
  }

  @Override
  public TShowDatabaseResp showDatabase(TGetDatabaseReq req) throws TException {
    return executeRemoteCallWithRetry(
        () -> client.showDatabase(req), resp -> !updateConfigNodeLeader(resp.status));
  }

  @Override
  public TRegionRouteMapResp getLatestRegionRouteMap() throws TException {
    return executeRemoteCallWithRetry(
        () -> client.getLatestRegionRouteMap(), resp -> !updateConfigNodeLeader(resp.status));
  }

  @Override
  public long getConfigNodeHeartBeat(long timestamp) throws TException {
    throw new TException("DataNode to ConfigNode client doesn't support getConfigNodeHeartBeat.");
  }

  @Override
  public TSStatus createFunction(TCreateFunctionReq req) throws TException {
    return executeRemoteCallWithRetry(
        () -> client.createFunction(req), status -> !updateConfigNodeLeader(status));
  }

  @Override
  public TSStatus dropFunction(TDropFunctionReq req) throws TException {
    return executeRemoteCallWithRetry(
        () -> client.dropFunction(req), status -> !updateConfigNodeLeader(status));
  }

  @Override
  public TGetUDFTableResp getUDFTable() throws TException {
    return executeRemoteCallWithRetry(
        () -> client.getUDFTable(), resp -> !updateConfigNodeLeader(resp.status));
  }

  @Override
  public TGetJarInListResp getUDFJar(TGetJarInListReq req) throws TException {
    return executeRemoteCallWithRetry(
        () -> client.getUDFJar(req), resp -> !updateConfigNodeLeader(resp.status));
  }

  @Override
  public TSStatus createTrigger(TCreateTriggerReq req) throws TException {
    return executeRemoteCallWithRetry(
        () -> client.createTrigger(req), status -> !updateConfigNodeLeader(status));
  }

  @Override
  public TSStatus dropTrigger(TDropTriggerReq req) throws TException {
    return executeRemoteCallWithRetry(
        () -> client.dropTrigger(req), status -> !updateConfigNodeLeader(status));
  }

  @Override
  public TGetLocationForTriggerResp getLocationOfStatefulTrigger(String triggerName)
      throws TException {
    return executeRemoteCallWithRetry(
        () -> client.getLocationOfStatefulTrigger(triggerName),
        resp -> !updateConfigNodeLeader(resp.status));
  }

  public TGetTriggerTableResp getTriggerTable() throws TException {
    return executeRemoteCallWithRetry(
        () -> client.getTriggerTable(), resp -> !updateConfigNodeLeader(resp.status));
  }

  @Override
  public TGetTriggerTableResp getStatefulTriggerTable() throws TException {
    return executeRemoteCallWithRetry(
        () -> client.getStatefulTriggerTable(), resp -> !updateConfigNodeLeader(resp.status));
  }

  @Override
  public TGetJarInListResp getTriggerJar(TGetJarInListReq req) throws TException {
    return executeRemoteCallWithRetry(
        () -> client.getTriggerJar(req), resp -> !updateConfigNodeLeader(resp.status));
  }

  @Override
  public TSStatus createPipePlugin(TCreatePipePluginReq req) throws TException {
    return executeRemoteCallWithRetry(
        () -> client.createPipePlugin(req), status -> !updateConfigNodeLeader(status));
  }

  @Override
  public TSStatus dropPipePlugin(TDropPipePluginReq req) throws TException {
    return executeRemoteCallWithRetry(
        () -> client.dropPipePlugin(req), status -> !updateConfigNodeLeader(status));
  }

  @Override
  public TGetPipePluginTableResp getPipePluginTable() throws TException {
    return executeRemoteCallWithRetry(
        () -> client.getPipePluginTable(), resp -> !updateConfigNodeLeader(resp.status));
  }

  @Override
  public TGetJarInListResp getPipePluginJar(TGetJarInListReq req) throws TException {
    return executeRemoteCallWithRetry(
        () -> client.getPipePluginJar(req), resp -> !updateConfigNodeLeader(resp.status));
  }

  @Override
  public TSStatus createSchemaTemplate(TCreateSchemaTemplateReq req) throws TException {
    return executeRemoteCallWithRetry(
        () -> client.createSchemaTemplate(req), status -> !updateConfigNodeLeader(status));
  }

  @Override
  public TGetAllTemplatesResp getAllTemplates() throws TException {
    return executeRemoteCallWithRetry(
        () -> client.getAllTemplates(), resp -> !updateConfigNodeLeader(resp.status));
  }

  @Override
  public TGetTemplateResp getTemplate(String req) throws TException {
    return executeRemoteCallWithRetry(
        () -> client.getTemplate(req), resp -> !updateConfigNodeLeader(resp.status));
  }

  @Override
  public TSStatus setSchemaTemplate(TSetSchemaTemplateReq req) throws TException {
    return executeRemoteCallWithRetry(
        () -> client.setSchemaTemplate(req), status -> !updateConfigNodeLeader(status));
  }

  @Override
  public TGetPathsSetTemplatesResp getPathsSetTemplate(TGetPathsSetTemplatesReq req)
      throws TException {
    return executeRemoteCallWithRetry(
        () -> client.getPathsSetTemplate(req), resp -> !updateConfigNodeLeader(resp.status));
  }

  @Override
  public TSStatus deactivateSchemaTemplate(TDeactivateSchemaTemplateReq req) throws TException {
    return executeRemoteCallWithRetry(
        () -> client.deactivateSchemaTemplate(req), status -> !updateConfigNodeLeader(status));
  }

  @Override
  public TSStatus unsetSchemaTemplate(TUnsetSchemaTemplateReq req) throws TException {
    return executeRemoteCallWithRetry(
        () -> client.unsetSchemaTemplate(req), status -> !updateConfigNodeLeader(status));
  }

  @Override
  public TSStatus dropSchemaTemplate(String req) throws TException {
    return executeRemoteCallWithRetry(
        () -> client.dropSchemaTemplate(req), status -> !updateConfigNodeLeader(status));
  }

  @Override
  public TSStatus alterSchemaTemplate(TAlterSchemaTemplateReq req) throws TException {
    return executeRemoteCallWithRetry(
        () -> client.alterSchemaTemplate(req), status -> !updateConfigNodeLeader(status));
  }

  @Override
  public TSStatus deleteTimeSeries(TDeleteTimeSeriesReq req) throws TException {
    return executeRemoteCallWithRetry(
        () -> client.deleteTimeSeries(req), status -> !updateConfigNodeLeader(status));
  }

  @Override
  public TSStatus deleteLogicalView(TDeleteLogicalViewReq req) throws TException {
    return executeRemoteCallWithRetry(
        () -> client.deleteLogicalView(req), status -> !updateConfigNodeLeader(status));
  }

  @Override
  public TSStatus alterLogicalView(TAlterLogicalViewReq req) throws TException {
    return executeRemoteCallWithRetry(
        () -> client.alterLogicalView(req), status -> !updateConfigNodeLeader(status));
  }

  @Override
  public TSStatus createPipeSink(TPipeSinkInfo req) throws TException {
    return executeRemoteCallWithRetry(
        () -> client.createPipeSink(req), status -> !updateConfigNodeLeader(status));
  }

  @Override
  public TSStatus dropPipeSink(TDropPipeSinkReq req) throws TException {
    return executeRemoteCallWithRetry(
        () -> client.dropPipeSink(req), status -> !updateConfigNodeLeader(status));
  }

  @Override
  public TGetPipeSinkResp getPipeSink(TGetPipeSinkReq req) throws TException {
    return executeRemoteCallWithRetry(
        () -> client.getPipeSink(req), resp -> !updateConfigNodeLeader(resp.status));
  }

  @Override
  public TSStatus createPipe(TCreatePipeReq req) throws TException {
    return executeRemoteCallWithRetry(
        () -> client.createPipe(req), status -> !updateConfigNodeLeader(status));
  }

  @Override
  public TSStatus startPipe(String pipeName) throws TException {
    return executeRemoteCallWithRetry(
        () -> client.startPipe(pipeName), status -> !updateConfigNodeLeader(status));
  }

  @Override
  public TSStatus stopPipe(String pipeName) throws TException {
    return executeRemoteCallWithRetry(
        () -> client.stopPipe(pipeName), status -> !updateConfigNodeLeader(status));
  }

  @Override
  public TSStatus dropPipe(String pipeName) throws TException {
    return executeRemoteCallWithRetry(
        () -> client.dropPipe(pipeName), status -> !updateConfigNodeLeader(status));
  }

  @Override
  public TShowPipeResp showPipe(TShowPipeReq req) throws TException {
    return executeRemoteCallWithRetry(
        () -> client.showPipe(req), resp -> !updateConfigNodeLeader(resp.status));
  }

  @Override
  public TGetAllPipeInfoResp getAllPipeInfo() throws TException {
    return executeRemoteCallWithRetry(
        () -> client.getAllPipeInfo(), resp -> !updateConfigNodeLeader(resp.status));
  }

  @Override
  public TGetRegionIdResp getRegionId(TGetRegionIdReq req) throws TException {
    return executeRemoteCallWithRetry(
        () -> client.getRegionId(req), resp -> !updateConfigNodeLeader(resp.status));
  }

  @Override
  public TGetTimeSlotListResp getTimeSlotList(TGetTimeSlotListReq req) throws TException {
    return executeRemoteCallWithRetry(
        () -> client.getTimeSlotList(req), resp -> !updateConfigNodeLeader(resp.status));
  }

  public TCountTimeSlotListResp countTimeSlotList(TCountTimeSlotListReq req) throws TException {
    return executeRemoteCallWithRetry(
        () -> client.countTimeSlotList(req), resp -> !updateConfigNodeLeader(resp.status));
  }

  @Override
  public TGetSeriesSlotListResp getSeriesSlotList(TGetSeriesSlotListReq req) throws TException {
    return executeRemoteCallWithRetry(
        () -> client.getSeriesSlotList(req), resp -> !updateConfigNodeLeader(resp.status));
  }

  @Override
  public TSStatus migrateRegion(TMigrateRegionReq req) throws TException {
    return executeRemoteCallWithRetry(
        () -> client.migrateRegion(req), status -> !updateConfigNodeLeader(status));
  }

  @Override
  public TSStatus createCQ(TCreateCQReq req) throws TException {
    return executeRemoteCallWithRetry(
        () -> client.createCQ(req), status -> !updateConfigNodeLeader(status));
  }

  @Override
  public TSStatus dropCQ(TDropCQReq req) throws TException {
    return executeRemoteCallWithRetry(
        () -> client.dropCQ(req), status -> !updateConfigNodeLeader(status));
  }

  @Override
  public TShowCQResp showCQ() throws TException {
    return executeRemoteCallWithRetry(
        () -> client.showCQ(), resp -> !updateConfigNodeLeader(resp.status));
  }

  @Override
  public TSStatus createModel(TCreateModelReq req) throws TException {
    return executeRemoteCallWithRetry(
        () -> client.createModel(req), status -> !updateConfigNodeLeader(status));
  }

  @Override
  public TSStatus dropModel(TDropModelReq req) throws TException {
    return executeRemoteCallWithRetry(
        () -> client.dropModel(req), status -> !updateConfigNodeLeader(status));
  }

  @Override
  public TShowModelResp showModel(TShowModelReq req) throws TException {
    return executeRemoteCallWithRetry(
        () -> client.showModel(req), resp -> !updateConfigNodeLeader(resp.status));
  }

  @Override
  public TShowTrailResp showTrail(TShowTrailReq req) throws TException {
    return executeRemoteCallWithRetry(
        () -> client.showTrail(req), resp -> !updateConfigNodeLeader(resp.status));
  }

  @Override
  public TSStatus updateModelInfo(TUpdateModelInfoReq req) throws TException {
    throw new TException(new UnsupportedOperationException().getCause());
  }

  @Override
  public TSStatus updateModelState(TUpdateModelStateReq req) throws TException {
    throw new TException(new UnsupportedOperationException().getCause());
  }

  @Override
  public TSStatus setSpaceQuota(TSetSpaceQuotaReq req) throws TException {
    return executeRemoteCallWithRetry(
        () -> client.setSpaceQuota(req), status -> !updateConfigNodeLeader(status));
  }

  @Override
  public TSpaceQuotaResp showSpaceQuota(List<String> databases) throws TException {
    return executeRemoteCallWithRetry(
        () -> client.showSpaceQuota(databases), resp -> !updateConfigNodeLeader(resp.status));
  }

  @Override
  public TSpaceQuotaResp getSpaceQuota() throws TException {
    return executeRemoteCallWithRetry(
        () -> client.getSpaceQuota(), resp -> !updateConfigNodeLeader(resp.status));
  }

  @Override
  public TSStatus setThrottleQuota(TSetThrottleQuotaReq req) throws TException {
    return executeRemoteCallWithRetry(
        () -> client.setThrottleQuota(req), status -> !updateConfigNodeLeader(status));
  }

  @Override
  public TThrottleQuotaResp showThrottleQuota(TShowThrottleReq req) throws TException {
    return executeRemoteCallWithRetry(
        () -> client.showThrottleQuota(req), resp -> !updateConfigNodeLeader(resp.status));
  }

  @Override
  public TThrottleQuotaResp getThrottleQuota() throws TException {
    return executeRemoteCallWithRetry(
        () -> client.getThrottleQuota(), resp -> !updateConfigNodeLeader(resp.status));
  }

  public static class Factory extends ThriftClientFactory<ConfigRegionId, ConfigNodeClient> {

    public Factory(
        ClientManager<ConfigRegionId, ConfigNodeClient> clientManager,
        ThriftClientProperty thriftClientProperty) {
      super(clientManager, thriftClientProperty);
    }

    @Override
    public void destroyObject(
        ConfigRegionId configRegionId, PooledObject<ConfigNodeClient> pooledObject) {
      pooledObject.getObject().invalidate();
    }

    @Override
    public PooledObject<ConfigNodeClient> makeObject(ConfigRegionId configRegionId)
        throws Exception {
      return new DefaultPooledObject<>(
          SyncThriftClientWithErrorHandler.newErrorHandler(
              ConfigNodeClient.class,
              ConfigNodeClient.class.getConstructor(
                  List.class, thriftClientProperty.getClass(), clientManager.getClass()),
              ConfigNodeInfo.getInstance().getLatestConfigNodes(),
              thriftClientProperty,
              clientManager));
    }

    @Override
    public boolean validateObject(
        ConfigRegionId configRegionId, PooledObject<ConfigNodeClient> pooledObject) {
      return Optional.ofNullable(pooledObject.getObject().getTransport())
          .map(TTransport::isOpen)
          .orElse(false);
    }
  }
}
