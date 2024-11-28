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
import org.apache.iotdb.common.rpc.thrift.TNodeLocations;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.common.rpc.thrift.TSeriesPartitionSlot;
import org.apache.iotdb.common.rpc.thrift.TSetConfigurationReq;
import org.apache.iotdb.common.rpc.thrift.TSetSpaceQuotaReq;
import org.apache.iotdb.common.rpc.thrift.TSetTTLReq;
import org.apache.iotdb.common.rpc.thrift.TSetThrottleQuotaReq;
import org.apache.iotdb.common.rpc.thrift.TShowConfigurationResp;
import org.apache.iotdb.common.rpc.thrift.TShowTTLReq;
import org.apache.iotdb.common.rpc.thrift.TTestConnectionResp;
import org.apache.iotdb.commons.client.ClientManager;
import org.apache.iotdb.commons.client.ThriftClient;
import org.apache.iotdb.commons.client.factory.ThriftClientFactory;
import org.apache.iotdb.commons.client.property.ThriftClientProperty;
import org.apache.iotdb.commons.client.request.TestConnectionUtils;
import org.apache.iotdb.commons.client.sync.SyncThriftClientWithErrorHandler;
import org.apache.iotdb.commons.consensus.ConfigRegionId;
import org.apache.iotdb.confignode.rpc.thrift.IConfigNodeRPCService;
import org.apache.iotdb.confignode.rpc.thrift.TAINodeConfigurationResp;
import org.apache.iotdb.confignode.rpc.thrift.TAINodeRegisterReq;
import org.apache.iotdb.confignode.rpc.thrift.TAINodeRegisterResp;
import org.apache.iotdb.confignode.rpc.thrift.TAINodeRemoveReq;
import org.apache.iotdb.confignode.rpc.thrift.TAINodeRestartReq;
import org.apache.iotdb.confignode.rpc.thrift.TAINodeRestartResp;
import org.apache.iotdb.confignode.rpc.thrift.TAddConsensusGroupReq;
import org.apache.iotdb.confignode.rpc.thrift.TAlterLogicalViewReq;
import org.apache.iotdb.confignode.rpc.thrift.TAlterOrDropTableReq;
import org.apache.iotdb.confignode.rpc.thrift.TAlterPipeReq;
import org.apache.iotdb.confignode.rpc.thrift.TAlterSchemaTemplateReq;
import org.apache.iotdb.confignode.rpc.thrift.TAuthizedPatternTreeResp;
import org.apache.iotdb.confignode.rpc.thrift.TAuthorizerReq;
import org.apache.iotdb.confignode.rpc.thrift.TAuthorizerResp;
import org.apache.iotdb.confignode.rpc.thrift.TCheckUserPrivilegesReq;
import org.apache.iotdb.confignode.rpc.thrift.TCloseConsumerReq;
import org.apache.iotdb.confignode.rpc.thrift.TConfigNodeHeartbeatReq;
import org.apache.iotdb.confignode.rpc.thrift.TConfigNodeHeartbeatResp;
import org.apache.iotdb.confignode.rpc.thrift.TConfigNodeRegisterReq;
import org.apache.iotdb.confignode.rpc.thrift.TConfigNodeRegisterResp;
import org.apache.iotdb.confignode.rpc.thrift.TCountDatabaseResp;
import org.apache.iotdb.confignode.rpc.thrift.TCountTimeSlotListReq;
import org.apache.iotdb.confignode.rpc.thrift.TCountTimeSlotListResp;
import org.apache.iotdb.confignode.rpc.thrift.TCreateCQReq;
import org.apache.iotdb.confignode.rpc.thrift.TCreateConsumerReq;
import org.apache.iotdb.confignode.rpc.thrift.TCreateFunctionReq;
import org.apache.iotdb.confignode.rpc.thrift.TCreateModelReq;
import org.apache.iotdb.confignode.rpc.thrift.TCreatePipePluginReq;
import org.apache.iotdb.confignode.rpc.thrift.TCreatePipeReq;
import org.apache.iotdb.confignode.rpc.thrift.TCreateSchemaTemplateReq;
import org.apache.iotdb.confignode.rpc.thrift.TCreateTopicReq;
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
import org.apache.iotdb.confignode.rpc.thrift.TDeleteTableDeviceReq;
import org.apache.iotdb.confignode.rpc.thrift.TDeleteTableDeviceResp;
import org.apache.iotdb.confignode.rpc.thrift.TDeleteTimeSeriesReq;
import org.apache.iotdb.confignode.rpc.thrift.TDescTableResp;
import org.apache.iotdb.confignode.rpc.thrift.TDropCQReq;
import org.apache.iotdb.confignode.rpc.thrift.TDropFunctionReq;
import org.apache.iotdb.confignode.rpc.thrift.TDropModelReq;
import org.apache.iotdb.confignode.rpc.thrift.TDropPipePluginReq;
import org.apache.iotdb.confignode.rpc.thrift.TDropPipeReq;
import org.apache.iotdb.confignode.rpc.thrift.TDropTopicReq;
import org.apache.iotdb.confignode.rpc.thrift.TDropTriggerReq;
import org.apache.iotdb.confignode.rpc.thrift.TFetchTableResp;
import org.apache.iotdb.confignode.rpc.thrift.TGetAllPipeInfoResp;
import org.apache.iotdb.confignode.rpc.thrift.TGetAllSubscriptionInfoResp;
import org.apache.iotdb.confignode.rpc.thrift.TGetAllTemplatesResp;
import org.apache.iotdb.confignode.rpc.thrift.TGetAllTopicInfoResp;
import org.apache.iotdb.confignode.rpc.thrift.TGetClusterIdResp;
import org.apache.iotdb.confignode.rpc.thrift.TGetDataNodeLocationsResp;
import org.apache.iotdb.confignode.rpc.thrift.TGetDatabaseReq;
import org.apache.iotdb.confignode.rpc.thrift.TGetJarInListReq;
import org.apache.iotdb.confignode.rpc.thrift.TGetJarInListResp;
import org.apache.iotdb.confignode.rpc.thrift.TGetLocationForTriggerResp;
import org.apache.iotdb.confignode.rpc.thrift.TGetModelInfoReq;
import org.apache.iotdb.confignode.rpc.thrift.TGetModelInfoResp;
import org.apache.iotdb.confignode.rpc.thrift.TGetPathsSetTemplatesReq;
import org.apache.iotdb.confignode.rpc.thrift.TGetPathsSetTemplatesResp;
import org.apache.iotdb.confignode.rpc.thrift.TGetPipePluginTableResp;
import org.apache.iotdb.confignode.rpc.thrift.TGetRegionIdReq;
import org.apache.iotdb.confignode.rpc.thrift.TGetRegionIdResp;
import org.apache.iotdb.confignode.rpc.thrift.TGetSeriesSlotListReq;
import org.apache.iotdb.confignode.rpc.thrift.TGetSeriesSlotListResp;
import org.apache.iotdb.confignode.rpc.thrift.TGetTemplateResp;
import org.apache.iotdb.confignode.rpc.thrift.TGetTimeSlotListReq;
import org.apache.iotdb.confignode.rpc.thrift.TGetTimeSlotListResp;
import org.apache.iotdb.confignode.rpc.thrift.TGetTriggerTableResp;
import org.apache.iotdb.confignode.rpc.thrift.TGetUDFTableResp;
import org.apache.iotdb.confignode.rpc.thrift.TGetUdfTableReq;
import org.apache.iotdb.confignode.rpc.thrift.TLoginReq;
import org.apache.iotdb.confignode.rpc.thrift.TMigrateRegionReq;
import org.apache.iotdb.confignode.rpc.thrift.TPermissionInfoResp;
import org.apache.iotdb.confignode.rpc.thrift.TPipeConfigTransferReq;
import org.apache.iotdb.confignode.rpc.thrift.TPipeConfigTransferResp;
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
import org.apache.iotdb.confignode.rpc.thrift.TShowAINodesResp;
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
import org.apache.iotdb.confignode.rpc.thrift.TShowSubscriptionReq;
import org.apache.iotdb.confignode.rpc.thrift.TShowSubscriptionResp;
import org.apache.iotdb.confignode.rpc.thrift.TShowTTLResp;
import org.apache.iotdb.confignode.rpc.thrift.TShowTableResp;
import org.apache.iotdb.confignode.rpc.thrift.TShowThrottleReq;
import org.apache.iotdb.confignode.rpc.thrift.TShowTopicReq;
import org.apache.iotdb.confignode.rpc.thrift.TShowTopicResp;
import org.apache.iotdb.confignode.rpc.thrift.TShowVariablesResp;
import org.apache.iotdb.confignode.rpc.thrift.TSpaceQuotaResp;
import org.apache.iotdb.confignode.rpc.thrift.TSubscribeReq;
import org.apache.iotdb.confignode.rpc.thrift.TSystemConfigurationResp;
import org.apache.iotdb.confignode.rpc.thrift.TTestOperation;
import org.apache.iotdb.confignode.rpc.thrift.TThrottleQuotaResp;
import org.apache.iotdb.confignode.rpc.thrift.TUnsetSchemaTemplateReq;
import org.apache.iotdb.confignode.rpc.thrift.TUnsubscribeReq;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.rpc.DeepCopyRpcTransportFactory;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;

public class ConfigNodeClient implements IConfigNodeRPCService.Iface, ThriftClient, AutoCloseable {

  private static final Logger logger = LoggerFactory.getLogger(ConfigNodeClient.class);

  private static final int RETRY_NUM = 15;

  public static final String MSG_RECONNECTION_FAIL =
      "Fail to connect to any config node. Please check status of ConfigNodes or logs of connected DataNode";

  private static final String MSG_RECONNECTION_DATANODE_FAIL =
      "Failed to connect to ConfigNode %s from DataNode %s when executing %s, Exception:";
  private static final long RETRY_INTERVAL_MS = 1000L;
  private static final long WAIT_CN_LEADER_ELECTION_INTERVAL_MS = 2000L;

  private static final String UNSUPPORTED_INVOCATION =
      "This method is not supported for invocation by DataNode";

  private final ThriftClientProperty property;

  private IConfigNodeRPCService.Iface client;

  private TTransport transport;

  private TEndPoint configLeader;

  private List<TEndPoint> configNodes;

  private TEndPoint configNode;

  private int cursor = 0;

  private boolean isFirstInitiated;

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
    // Set the first configNode as configLeader for a tentative connection
    this.configLeader = this.configNodes.get(0);
    this.isFirstInitiated = true;

    connectAndSync();
  }

  public void connect(TEndPoint endpoint, int timeoutMs) throws TException {
    try {
      transport =
          DeepCopyRpcTransportFactory.INSTANCE.getTransport(
              // As there is a try-catch already, we do not need to use TSocket.wrap
              endpoint.getIp(), endpoint.getPort(), timeoutMs);
      if (!transport.isOpen()) {
        transport.open();
      }
      configNode = endpoint;
    } catch (TTransportException e) {
      throw new TException(e);
    }

    client = new IConfigNodeRPCService.Client(property.getProtocolFactory().getProtocol(transport));
  }

  private void connectAndSync() throws TException {
    try {
      tryToConnect(property.getConnectionTimeoutMs());
    } catch (TException e) {
      // Can not connect to each config node
      syncLatestConfigNodeList();
      tryToConnect(property.getConnectionTimeoutMs());
    }
  }

  private void connectAndSync(int timeoutMs) throws TException {
    try {
      tryToConnect(timeoutMs);
    } catch (TException e) {
      // Can not connect to each config node
      syncLatestConfigNodeList();
      tryToConnect(timeoutMs);
    }
  }

  private void tryToConnect(int timeoutMs) throws TException {
    if (configLeader != null) {
      try {
        connect(configLeader, timeoutMs);
        return;
      } catch (TException ignore) {
        logger.warn("The current node leader may have been down {}, try next node", configLeader);
        configLeader = null;
      }
    } else {
      try {
        // Wait to start the next try
        Thread.sleep(RETRY_INTERVAL_MS);
      } catch (InterruptedException ignore) {
        Thread.currentThread().interrupt();
        logger.warn("Unexpected interruption when waiting to try to connect to ConfigNode");
      }
    }

    if (transport != null) {
      transport.close();
    }

    for (int tryHostNum = 0; tryHostNum < configNodes.size(); tryHostNum++) {
      cursor = (cursor + 1) % configNodes.size();
      TEndPoint tryEndpoint = configNodes.get(cursor);

      try {
        connect(tryEndpoint, timeoutMs);
        return;
      } catch (TException ignore) {
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
    try {
      if (status.getCode() == TSStatusCode.REDIRECTION_RECOMMEND.getStatusCode()) {
        if (status.isSetRedirectNode()) {
          configLeader =
              new TEndPoint(status.getRedirectNode().getIp(), status.getRedirectNode().getPort());
        } else {
          configLeader = null;
        }
        if (!isFirstInitiated) {
          logger.info(
              "Failed to connect to ConfigNode {} from DataNode {}, because the current node is not "
                  + "leader or not ready yet, will try again later",
              configNode,
              config.getAddressAndPort());
        }
        return true;
      }
      return false;
    } finally {
      isFirstInitiated = false;
    }
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
  private <T> T executeRemoteCallWithRetry(final Operation<T> call, final Predicate<T> check)
      throws TException {
    int detectedNodeNum = 0;
    for (int i = 0; i < RETRY_NUM; i++) {
      try {
        final T result = call.execute();
        if (check.test(result)) {
          return result;
        }
        detectedNodeNum++;
      } catch (TException e) {
        final String message =
            String.format(
                MSG_RECONNECTION_DATANODE_FAIL,
                configNode,
                config.getAddressAndPort(),
                Thread.currentThread().getStackTrace()[2].getMethodName());
        logger.warn(message, e);
        configLeader = null;
      }

      // If we have detected all configNodes and still not return
      if (detectedNodeNum >= configNodes.size()) {
        // Clear count
        detectedNodeNum = 0;
        // Wait to start the next try
        try {
          Thread.sleep(WAIT_CN_LEADER_ELECTION_INTERVAL_MS);
        } catch (InterruptedException ignore) {
          Thread.currentThread().interrupt();
          logger.warn(
              "Unexpected interruption when waiting to try to connect to ConfigNode, may because current node has been down. Will break current execution process to avoid meaningless wait.");
          break;
        }
      }

      connectAndSync();
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
  public TGetClusterIdResp getClusterId() throws TException {
    return executeRemoteCallWithRetry(
        () -> client.getClusterId(), resp -> !updateConfigNodeLeader(resp.status));
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
      connectAndSync();
    }
    throw new TException(MSG_RECONNECTION_FAIL);
  }

  @Override
  public TDataNodeRestartResp restartDataNode(TDataNodeRestartReq req) throws TException {
    return executeRemoteCallWithRetry(
        () -> client.restartDataNode(req), resp -> !updateConfigNodeLeader(resp.status));
  }

  @Override
  public TAINodeRegisterResp registerAINode(TAINodeRegisterReq req) throws TException {
    throw new UnsupportedOperationException(UNSUPPORTED_INVOCATION);
  }

  @Override
  public TAINodeRestartResp restartAINode(TAINodeRestartReq req) throws TException {
    throw new UnsupportedOperationException(UNSUPPORTED_INVOCATION);
  }

  @Override
  public TSStatus removeAINode(TAINodeRemoveReq req) throws TException {
    throw new UnsupportedOperationException(UNSUPPORTED_INVOCATION);
  }

  @Override
  public TShowAINodesResp showAINodes() throws TException {
    return executeRemoteCallWithRetry(
        () -> client.showAINodes(), resp -> !updateConfigNodeLeader(resp.status));
  }

  @Override
  public TAINodeConfigurationResp getAINodeConfiguration(int aiNodeId) throws TException {
    throw new UnsupportedOperationException(UNSUPPORTED_INVOCATION);
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
  public TSStatus deleteDatabases(final TDeleteDatabasesReq req) throws TException {
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
  public TShowTTLResp showTTL(TShowTTLReq req) throws TException {
    return executeRemoteCallWithRetry(
        () -> client.showTTL(req), resp -> !updateConfigNodeLeader(resp.status));
  }

  public TSStatus callSpecialProcedure(TTestOperation operation) throws TException {
    return executeRemoteCallWithRetry(
        () -> client.callSpecialProcedure(operation), status -> !updateConfigNodeLeader(status));
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
  public TSchemaPartitionTableResp getSchemaPartitionTableWithSlots(
      Map<String, List<TSeriesPartitionSlot>> dbSlotMap) throws TException {
    return executeRemoteCallWithRetry(
        () -> client.getSchemaPartitionTableWithSlots(dbSlotMap),
        resp -> !updateConfigNodeLeader(resp.status));
  }

  @Override
  public TSchemaPartitionTableResp getOrCreateSchemaPartitionTable(TSchemaPartitionReq req)
      throws TException {
    return executeRemoteCallWithRetry(
        () -> client.getOrCreateSchemaPartitionTable(req),
        resp -> !updateConfigNodeLeader(resp.status));
  }

  @Override
  public TSchemaPartitionTableResp getOrCreateSchemaPartitionTableWithSlots(
      final Map<String, List<TSeriesPartitionSlot>> dbSlotMap) throws TException {
    return executeRemoteCallWithRetry(
        () -> client.getOrCreateSchemaPartitionTableWithSlots(dbSlotMap),
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
  public TPermissionInfoResp checkUserPrivilegeGrantOpt(TCheckUserPrivilegesReq req)
      throws TException {
    return executeRemoteCallWithRetry(
        () -> client.checkUserPrivilegeGrantOpt(req), resp -> !updateConfigNodeLeader(resp.status));
  }

  public TPermissionInfoResp checkRoleOfUser(TAuthorizerReq req) throws TException {
    return executeRemoteCallWithRetry(
        () -> client.checkRoleOfUser(req), resp -> !updateConfigNodeLeader(resp.status));
  }

  @Override
  public TConfigNodeRegisterResp registerConfigNode(TConfigNodeRegisterReq req) throws TException {
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
  public TSStatus clearCache(final Set<Integer> clearCacheOptions) throws TException {
    return executeRemoteCallWithRetry(
        () -> client.clearCache(clearCacheOptions), status -> !updateConfigNodeLeader(status));
  }

  @Override
  public TSStatus setConfiguration(TSetConfigurationReq req) throws TException {
    return executeRemoteCallWithRetry(
        () -> client.setConfiguration(req), status -> !updateConfigNodeLeader(status));
  }

  @Override
  public TSStatus startRepairData() throws TException {
    return executeRemoteCallWithRetry(
        () -> client.startRepairData(), status -> !updateConfigNodeLeader(status));
  }

  @Override
  public TSStatus stopRepairData() throws TException {
    return executeRemoteCallWithRetry(
        () -> client.stopRepairData(), status -> !updateConfigNodeLeader(status));
  }

  @Override
  public TSStatus submitLoadConfigurationTask() throws TException {
    return executeRemoteCallWithRetry(
        () -> client.submitLoadConfigurationTask(), status -> !updateConfigNodeLeader(status));
  }

  @Override
  public TSStatus loadConfiguration() throws TException {
    throw new UnsupportedOperationException(
        UNSUPPORTED_INVOCATION + ", please call submitLoadConfigurationTask instead");
  }

  @Override
  public TShowConfigurationResp showConfiguration(int nodeId) throws TException {
    return executeRemoteCallWithRetry(
        () -> client.showConfiguration(nodeId), resp -> !updateConfigNodeLeader(resp.getStatus()));
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
  public TTestConnectionResp submitTestConnectionTask(TNodeLocations nodeLocations)
      throws TException {
    return executeRemoteCallWithRetry(
        () -> client.submitTestConnectionTask(nodeLocations),
        resp -> !updateConfigNodeLeader(resp.getStatus()));
  }

  @Override
  public TTestConnectionResp submitTestConnectionTaskToLeader() throws TException {
    try {
      // this rpc need special timeout
      connectAndSync(TestConnectionUtils.calculateDnToCnLeaderMaxTime());
      return executeRemoteCallWithRetry(
          () -> client.submitTestConnectionTaskToLeader(),
          resp -> !updateConfigNodeLeader(resp.getStatus()));
    } finally {
      // reset timeout to default
      connectAndSync();
    }
  }

  @Override
  public TSStatus testConnectionEmptyRPC() throws TException {
    return executeRemoteCallWithRetry(
        () -> client.testConnectionEmptyRPC(), resp -> !updateConfigNodeLeader(resp));
  }

  @Override
  public TRegionRouteMapResp getLatestRegionRouteMap() throws TException {
    return executeRemoteCallWithRetry(
        () -> client.getLatestRegionRouteMap(), resp -> !updateConfigNodeLeader(resp.status));
  }

  @Override
  public TConfigNodeHeartbeatResp getConfigNodeHeartBeat(TConfigNodeHeartbeatReq req)
      throws TException {
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
  public TGetUDFTableResp getUDFTable(TGetUdfTableReq req) throws TException {
    return executeRemoteCallWithRetry(
        () -> client.getUDFTable(req), resp -> !updateConfigNodeLeader(resp.status));
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
  public TSStatus createPipe(TCreatePipeReq req) throws TException {
    return executeRemoteCallWithRetry(
        () -> client.createPipe(req), status -> !updateConfigNodeLeader(status));
  }

  @Override
  public TSStatus alterPipe(TAlterPipeReq req) throws TException {
    return executeRemoteCallWithRetry(
        () -> client.alterPipe(req), status -> !updateConfigNodeLeader(status));
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
  public TSStatus dropPipeExtended(TDropPipeReq req) throws TException {
    return executeRemoteCallWithRetry(
        () -> client.dropPipeExtended(req), status -> !updateConfigNodeLeader(status));
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
  public TSStatus createTopic(TCreateTopicReq req) throws TException {
    return executeRemoteCallWithRetry(
        () -> client.createTopic(req), status -> !updateConfigNodeLeader(status));
  }

  @Override
  public TSStatus dropTopic(String topicName) throws TException {
    return executeRemoteCallWithRetry(
        () -> client.dropTopic(topicName), status -> !updateConfigNodeLeader(status));
  }

  @Override
  public TSStatus dropTopicExtended(TDropTopicReq req) throws TException {
    return executeRemoteCallWithRetry(
        () -> client.dropTopicExtended(req), status -> !updateConfigNodeLeader(status));
  }

  @Override
  public TShowTopicResp showTopic(TShowTopicReq req) throws TException {
    return executeRemoteCallWithRetry(
        () -> client.showTopic(req), resp -> !updateConfigNodeLeader(resp.status));
  }

  @Override
  public TGetAllTopicInfoResp getAllTopicInfo() throws TException {
    return executeRemoteCallWithRetry(
        () -> client.getAllTopicInfo(), resp -> !updateConfigNodeLeader(resp.status));
  }

  @Override
  public TSStatus createConsumer(TCreateConsumerReq req) throws TException {
    return executeRemoteCallWithRetry(
        () -> client.createConsumer(req), status -> !updateConfigNodeLeader(status));
  }

  @Override
  public TSStatus closeConsumer(TCloseConsumerReq req) throws TException {
    return executeRemoteCallWithRetry(
        () -> client.closeConsumer(req), status -> !updateConfigNodeLeader(status));
  }

  @Override
  public TSStatus createSubscription(TSubscribeReq req) throws TException {
    return executeRemoteCallWithRetry(
        () -> client.createSubscription(req), status -> !updateConfigNodeLeader(status));
  }

  @Override
  public TSStatus dropSubscription(TUnsubscribeReq req) throws TException {
    return executeRemoteCallWithRetry(
        () -> client.dropSubscription(req), status -> !updateConfigNodeLeader(status));
  }

  @Override
  public TShowSubscriptionResp showSubscription(TShowSubscriptionReq req) throws TException {
    return executeRemoteCallWithRetry(
        () -> client.showSubscription(req), resp -> !updateConfigNodeLeader(resp.status));
  }

  @Override
  public TGetAllSubscriptionInfoResp getAllSubscriptionInfo() throws TException {
    return executeRemoteCallWithRetry(
        () -> client.getAllSubscriptionInfo(), resp -> !updateConfigNodeLeader(resp.status));
  }

  @Override
  public TPipeConfigTransferResp handleTransferConfigPlan(TPipeConfigTransferReq req)
      throws TException {
    return executeRemoteCallWithRetry(
        () -> client.handleTransferConfigPlan(req), resp -> !updateConfigNodeLeader(resp.status));
  }

  @Override
  public TSStatus handlePipeConfigClientExit(String clientId) throws TException {
    return executeRemoteCallWithRetry(
        () -> client.handlePipeConfigClientExit(clientId),
        status -> !updateConfigNodeLeader(status));
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
  public TGetModelInfoResp getModelInfo(TGetModelInfoReq req) throws TException {
    return executeRemoteCallWithRetry(
        () -> client.getModelInfo(req), resp -> !updateConfigNodeLeader(resp.getStatus()));
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

  @Override
  public TSStatus createTable(final ByteBuffer tableInfo) throws TException {
    return executeRemoteCallWithRetry(
        () -> client.createTable(tableInfo), status -> !updateConfigNodeLeader(status));
  }

  @Override
  public TSStatus alterOrDropTable(final TAlterOrDropTableReq req) throws TException {
    return executeRemoteCallWithRetry(
        () -> client.alterOrDropTable(req), status -> !updateConfigNodeLeader(status));
  }

  @Override
  public TShowTableResp showTables(final String database, final boolean isDetails)
      throws TException {
    return executeRemoteCallWithRetry(
        () -> client.showTables(database, isDetails), resp -> !updateConfigNodeLeader(resp.status));
  }

  @Override
  public TDescTableResp describeTable(
      final String database, final String tableName, final boolean isDetails) throws TException {
    return executeRemoteCallWithRetry(
        () -> client.describeTable(database, tableName, isDetails),
        resp -> !updateConfigNodeLeader(resp.status));
  }

  @Override
  public TFetchTableResp fetchTables(final Map<String, Set<String>> fetchTableMap)
      throws TException {
    return executeRemoteCallWithRetry(
        () -> client.fetchTables(fetchTableMap), resp -> !updateConfigNodeLeader(resp.status));
  }

  @Override
  public TDeleteTableDeviceResp deleteDevice(final TDeleteTableDeviceReq req) throws TException {
    return executeRemoteCallWithRetry(
        () -> client.deleteDevice(req), resp -> !updateConfigNodeLeader(resp.status));
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
