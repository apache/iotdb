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

package org.apache.iotdb.commons.client;

import org.apache.iotdb.common.rpc.thrift.TConfigNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TExternalServiceListResp;
import org.apache.iotdb.common.rpc.thrift.TFlushReq;
import org.apache.iotdb.common.rpc.thrift.TNodeLocations;
import org.apache.iotdb.common.rpc.thrift.TPipeHeartbeatResp;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.common.rpc.thrift.TSeriesPartitionSlot;
import org.apache.iotdb.common.rpc.thrift.TSetConfigurationReq;
import org.apache.iotdb.common.rpc.thrift.TSetSpaceQuotaReq;
import org.apache.iotdb.common.rpc.thrift.TSetTTLReq;
import org.apache.iotdb.common.rpc.thrift.TSetThrottleQuotaReq;
import org.apache.iotdb.common.rpc.thrift.TShowAppliedConfigurationsResp;
import org.apache.iotdb.common.rpc.thrift.TShowConfigurationResp;
import org.apache.iotdb.common.rpc.thrift.TShowTTLReq;
import org.apache.iotdb.common.rpc.thrift.TTestConnectionResp;
import org.apache.iotdb.commons.client.property.ThriftClientProperty;
import org.apache.iotdb.commons.conf.CommonConfig;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.consensus.ConfigRegionId;
import org.apache.iotdb.commons.i18n.ConfigMessages;
import org.apache.iotdb.confignode.rpc.thrift.IConfigNodeRPCService;
import org.apache.iotdb.confignode.rpc.thrift.TAINodeConfigurationResp;
import org.apache.iotdb.confignode.rpc.thrift.TAINodeRegisterReq;
import org.apache.iotdb.confignode.rpc.thrift.TAINodeRegisterResp;
import org.apache.iotdb.confignode.rpc.thrift.TAINodeRemoveReq;
import org.apache.iotdb.confignode.rpc.thrift.TAINodeRestartReq;
import org.apache.iotdb.confignode.rpc.thrift.TAINodeRestartResp;
import org.apache.iotdb.confignode.rpc.thrift.TAddConsensusGroupReq;
import org.apache.iotdb.confignode.rpc.thrift.TAlterEncodingCompressorReq;
import org.apache.iotdb.confignode.rpc.thrift.TAlterLogicalViewReq;
import org.apache.iotdb.confignode.rpc.thrift.TAlterOrDropTableReq;
import org.apache.iotdb.confignode.rpc.thrift.TAlterPipeReq;
import org.apache.iotdb.confignode.rpc.thrift.TAlterSchemaTemplateReq;
import org.apache.iotdb.confignode.rpc.thrift.TAlterTimeSeriesReq;
import org.apache.iotdb.confignode.rpc.thrift.TAlterTopicReq;
import org.apache.iotdb.confignode.rpc.thrift.TAuthizedPatternTreeResp;
import org.apache.iotdb.confignode.rpc.thrift.TAuthorizerRelationalReq;
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
import org.apache.iotdb.confignode.rpc.thrift.TCreateExternalServiceReq;
import org.apache.iotdb.confignode.rpc.thrift.TCreateFunctionReq;
import org.apache.iotdb.confignode.rpc.thrift.TCreatePipePluginReq;
import org.apache.iotdb.confignode.rpc.thrift.TCreatePipeReq;
import org.apache.iotdb.confignode.rpc.thrift.TCreateSchemaTemplateReq;
import org.apache.iotdb.confignode.rpc.thrift.TCreateTableViewReq;
import org.apache.iotdb.confignode.rpc.thrift.TCreateTopicReq;
import org.apache.iotdb.confignode.rpc.thrift.TCreateTriggerReq;
import org.apache.iotdb.confignode.rpc.thrift.TDataNodeConfigurationResp;
import org.apache.iotdb.confignode.rpc.thrift.TDataNodeLeaseRecoveryResp;
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
import org.apache.iotdb.confignode.rpc.thrift.TDescTable4InformationSchemaResp;
import org.apache.iotdb.confignode.rpc.thrift.TDescTableResp;
import org.apache.iotdb.confignode.rpc.thrift.TDropCQReq;
import org.apache.iotdb.confignode.rpc.thrift.TDropFunctionReq;
import org.apache.iotdb.confignode.rpc.thrift.TDropPipePluginReq;
import org.apache.iotdb.confignode.rpc.thrift.TDropPipeReq;
import org.apache.iotdb.confignode.rpc.thrift.TDropSubscriptionReq;
import org.apache.iotdb.confignode.rpc.thrift.TDropTopicReq;
import org.apache.iotdb.confignode.rpc.thrift.TDropTriggerReq;
import org.apache.iotdb.confignode.rpc.thrift.TExtendRegionReq;
import org.apache.iotdb.confignode.rpc.thrift.TFetchTableResp;
import org.apache.iotdb.confignode.rpc.thrift.TGetAINodeLocationResp;
import org.apache.iotdb.confignode.rpc.thrift.TGetAllPipeInfoResp;
import org.apache.iotdb.confignode.rpc.thrift.TGetAllSubscriptionInfoResp;
import org.apache.iotdb.confignode.rpc.thrift.TGetAllTemplatesResp;
import org.apache.iotdb.confignode.rpc.thrift.TGetAllTopicInfoResp;
import org.apache.iotdb.confignode.rpc.thrift.TGetClusterIdResp;
import org.apache.iotdb.confignode.rpc.thrift.TGetCommitProgressReq;
import org.apache.iotdb.confignode.rpc.thrift.TGetCommitProgressResp;
import org.apache.iotdb.confignode.rpc.thrift.TGetDataNodeLocationsResp;
import org.apache.iotdb.confignode.rpc.thrift.TGetDatabaseReq;
import org.apache.iotdb.confignode.rpc.thrift.TGetJarInListReq;
import org.apache.iotdb.confignode.rpc.thrift.TGetJarInListResp;
import org.apache.iotdb.confignode.rpc.thrift.TGetLocationForTriggerResp;
import org.apache.iotdb.confignode.rpc.thrift.TGetPathsSetTemplatesReq;
import org.apache.iotdb.confignode.rpc.thrift.TGetPathsSetTemplatesResp;
import org.apache.iotdb.confignode.rpc.thrift.TGetPipePluginTableResp;
import org.apache.iotdb.confignode.rpc.thrift.TGetRegionGroupsByTimeReq;
import org.apache.iotdb.confignode.rpc.thrift.TGetRegionGroupsByTimeResp;
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
import org.apache.iotdb.confignode.rpc.thrift.TReconstructRegionReq;
import org.apache.iotdb.confignode.rpc.thrift.TRegionRouteMapResp;
import org.apache.iotdb.confignode.rpc.thrift.TRemoveRegionReq;
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
import org.apache.iotdb.confignode.rpc.thrift.TShowConfigNodes4InformationSchemaResp;
import org.apache.iotdb.confignode.rpc.thrift.TShowConfigNodesResp;
import org.apache.iotdb.confignode.rpc.thrift.TShowDataNodes4InformationSchemaResp;
import org.apache.iotdb.confignode.rpc.thrift.TShowDataNodesResp;
import org.apache.iotdb.confignode.rpc.thrift.TShowDatabaseResp;
import org.apache.iotdb.confignode.rpc.thrift.TShowPipePluginReq;
import org.apache.iotdb.confignode.rpc.thrift.TShowPipeReq;
import org.apache.iotdb.confignode.rpc.thrift.TShowPipeResp;
import org.apache.iotdb.confignode.rpc.thrift.TShowRegionReq;
import org.apache.iotdb.confignode.rpc.thrift.TShowRegionResp;
import org.apache.iotdb.confignode.rpc.thrift.TShowRepairDataPartitionTableProgressResp;
import org.apache.iotdb.confignode.rpc.thrift.TShowSubscriptionReq;
import org.apache.iotdb.confignode.rpc.thrift.TShowSubscriptionResp;
import org.apache.iotdb.confignode.rpc.thrift.TShowTTLResp;
import org.apache.iotdb.confignode.rpc.thrift.TShowTable4InformationSchemaResp;
import org.apache.iotdb.confignode.rpc.thrift.TShowTableResp;
import org.apache.iotdb.confignode.rpc.thrift.TShowThrottleReq;
import org.apache.iotdb.confignode.rpc.thrift.TShowTopicReq;
import org.apache.iotdb.confignode.rpc.thrift.TShowTopicResp;
import org.apache.iotdb.confignode.rpc.thrift.TShowVariablesResp;
import org.apache.iotdb.confignode.rpc.thrift.TSpaceQuotaResp;
import org.apache.iotdb.confignode.rpc.thrift.TStartPipeReq;
import org.apache.iotdb.confignode.rpc.thrift.TStopPipeReq;
import org.apache.iotdb.confignode.rpc.thrift.TSubscribeReq;
import org.apache.iotdb.confignode.rpc.thrift.TSystemConfigurationResp;
import org.apache.iotdb.confignode.rpc.thrift.TTestOperation;
import org.apache.iotdb.confignode.rpc.thrift.TThrottleQuotaResp;
import org.apache.iotdb.confignode.rpc.thrift.TUnsetSchemaTemplateReq;
import org.apache.iotdb.confignode.rpc.thrift.TUnsubscribeReq;
import org.apache.iotdb.rpc.DeepCopyRpcTransportFactory;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLHandshakeException;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;

public abstract class AbstractConfigNodeClient
    implements IConfigNodeRPCService.Iface, ThriftClient, AutoCloseable {

  private static final Logger LOGGER = LoggerFactory.getLogger(AbstractConfigNodeClient.class);

  protected static final int RETRY_NUM = 15;
  protected static final long RETRY_INTERVAL_MS = 1000L;
  protected static final long WAIT_CN_LEADER_ELECTION_INTERVAL_MS = 2000L;

  protected final ThriftClientProperty property;

  protected IConfigNodeRPCService.Iface client;

  protected TTransport transport;

  protected TEndPoint configLeader;

  protected List<TEndPoint> configNodes;

  protected TEndPoint configNode;

  protected int cursor = 0;

  protected boolean isFirstInitiated;

  protected final CommonConfig commonConfig = CommonDescriptor.getInstance().getConfig();

  protected final ClientManager<ConfigRegionId, ? super AbstractConfigNodeClient> clientManager;

  protected final ConfigRegionId configRegionId = AbstractConfigNodeInfo.CONFIG_REGION_ID;

  protected AbstractConfigNodeClient(
      List<TEndPoint> configNodes,
      ThriftClientProperty property,
      ClientManager<ConfigRegionId, ? super AbstractConfigNodeClient> clientManager)
      throws TException {
    this.configNodes = configNodes;
    this.property = property;
    this.clientManager = clientManager;
    // Set the first configNode as configLeader for a tentative connection
    this.configLeader = this.configNodes.get(0);
    this.isFirstInitiated = true;

    connectAndSync();
  }

  public TTransport getTransport() {
    return transport;
  }

  public void connect(TEndPoint endpoint, int timeoutMs) throws TException {
    // Close existing transport before reassigning to prevent connection leaks.
    if (transport != null) {
      transport.close();
    }
    transport =
        commonConfig.isEnableInternalSSL()
            ? DeepCopyRpcTransportFactory.INSTANCE.getTransport(
                endpoint.getIp(),
                endpoint.getPort(),
                timeoutMs,
                commonConfig.getTrustStorePath(),
                commonConfig.getTrustStorePwd(),
                commonConfig.getKeyStorePath(),
                commonConfig.getKeyStorePwd())
            : DeepCopyRpcTransportFactory.INSTANCE.getTransport(
                // As there is a try-catch already, we do not need to use TSocket.wrap
                endpoint.getIp(), endpoint.getPort(), timeoutMs);
    if (!transport.isOpen()) {
      transport.open();
    }
    configNode = endpoint;

    client = new IConfigNodeRPCService.Client(property.getProtocolFactory().getProtocol(transport));
  }

  protected void connectAndSync() throws TException {
    try {
      tryToConnect(property.getConnectionTimeoutMs());
    } catch (TException e) {
      // Can not connect to each config node
      syncLatestConfigNodeList();
      tryToConnect(property.getConnectionTimeoutMs());
    }
  }

  protected void connectAndSync(int timeoutMs) throws TException {
    try {
      tryToConnect(timeoutMs);
    } catch (TException e) {
      // Can not connect to each config node
      syncLatestConfigNodeList();
      tryToConnect(timeoutMs);
    }
  }

  private void tryToConnect(int timeoutMs) throws TException {
    TException exception = null;
    if (configLeader != null) {
      try {
        connect(configLeader, timeoutMs);
        return;
      } catch (TException e) {
        LOGGER.warn(ConfigMessages.NODE_LEADER_MAY_DOWN_TRY_NEXT, configLeader);
        configLeader = null;
        exception = e;
      }
    } else {
      try {
        // Wait to start the next try
        Thread.sleep(RETRY_INTERVAL_MS);
      } catch (InterruptedException ignore) {
        Thread.currentThread().interrupt();
        LOGGER.warn(ConfigMessages.UNEXPECTED_INTERRUPTION_CONNECT_CONFIG_NODE);
      }
    }

    for (int tryHostNum = 0; tryHostNum < configNodes.size(); tryHostNum++) {
      cursor = (cursor + 1) % configNodes.size();
      TEndPoint tryEndpoint = configNodes.get(cursor);

      try {
        connect(tryEndpoint, timeoutMs);
        return;
      } catch (TException e) {
        LOGGER.warn(ConfigMessages.NODE_MAY_DOWN_TRY_NEXT, tryEndpoint);
        exception = e;
      }
    }
    if (exception != null
        && exception.getCause() != null
        && exception.getCause().getCause() != null
        && exception.getCause().getCause() instanceof IOException) {
      throw new TException(exception.getCause().getCause());
    }

    throw new TException(String.format(ConfigMessages.MSG_RECONNECTION_FAIL, getNodeTypeName()));
  }

  public void syncLatestConfigNodeList() {
    configNodes = getConfigNodeInfo().getLatestConfigNodes();
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
    clientManager.clear(AbstractConfigNodeInfo.CONFIG_REGION_ID);
  }

  @Override
  public boolean printLogWhenEncounterException() {
    return property.isPrintLogWhenEncounterException();
  }

  protected boolean updateConfigNodeLeader(TSStatus status) {
    try {
      if (status.getCode() == TSStatusCode.REDIRECTION_RECOMMEND.getStatusCode()) {
        if (status.isSetRedirectNode()) {
          configLeader =
              new TEndPoint(status.getRedirectNode().getIp(), status.getRedirectNode().getPort());
        } else {
          configLeader = null;
        }
        if (!isFirstInitiated) {
          LOGGER.info(
              ConfigMessages.FAILED_CONNECT_CONFIG_NODE_NOT_LEADER,
              configNode,
              getNodeTypeName(),
              getAddressAndPort());
        }
        return true;
      }
      if (status.getCode() == TSStatusCode.CONFIG_NODE_LEADER_WARMING_UP.getStatusCode()) {
        if (!isFirstInitiated) {
          LOGGER.info(
              ConfigMessages
                  .MESSAGE_CONFIGNODE_LEADER_ARG_IS_WARMING_UP_BEFORE_SERVING_DATANODE_ARG_WILL_WAIT_AND_RETRY_REASON_ARG_3A2A4163,
              configNode,
              getNodeTypeName(),
              getAddressAndPort(),
              status.getMessage());
        }
        try {
          Thread.sleep(WAIT_CN_LEADER_ELECTION_INTERVAL_MS);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          LOGGER.warn(ConfigMessages.UNEXPECTED_INTERRUPTION_CONNECT_CONFIG_NODE_BREAK);
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
   * @param <R> the type of rpc result
   * @throws TException if fails more than RETRY_NUM times, throw TException(MSG_RECONNECTION_FAIL)
   */
  protected <R> R executeRemoteCallWithRetry(final Operation<R> call, final Predicate<R> check)
      throws TException {
    int detectedNodeNum = 0;
    for (int i = 0; i < RETRY_NUM; i++) {
      try {
        final R result = call.execute();
        if (check.test(result)) {
          return result;
        }
        detectedNodeNum++;
      } catch (TException e) {
        final String message =
            String.format(
                ConfigMessages.MSG_RECONNECTION_NODE_FAIL,
                configNode,
                getNodeTypeName(),
                getAddressAndPort(),
                Thread.currentThread().getStackTrace()[2].getMethodName());
        LOGGER.warn(message, e);
        configLeader = null;
        if (e.getCause() != null && e.getCause() instanceof SSLHandshakeException) {
          throw e;
        }
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
          LOGGER.warn(ConfigMessages.UNEXPECTED_INTERRUPTION_CONNECT_CONFIG_NODE_BREAK);
          break;
        }
      }

      connectAndSync();
    }
    throw new TException(String.format(ConfigMessages.MSG_RECONNECTION_FAIL, getNodeTypeName()));
  }

  @Override
  public TSystemConfigurationResp getSystemConfiguration() throws TException {
    return executeRemoteCallWithRetry(
        () -> client.getSystemConfiguration(), resp -> !updateConfigNodeLeader(resp.status));
  }

  @Override
  public TGetDataNodeLocationsResp getReadableDataNodeLocations() throws TException {
    return executeRemoteCallWithRetry(
        () -> client.getReadableDataNodeLocations(), resp -> !updateConfigNodeLeader(resp.status));
  }

  @FunctionalInterface
  protected interface Operation<R> {
    R execute() throws TException;
  }

  /** The node type name used in log messages, e.g. DataNode, StreamNode. */
  protected abstract String getNodeTypeName();

  /** The address and port of the current node, used in log messages. */
  protected abstract TEndPoint getAddressAndPort();

  /** The ConfigNodeInfo singleton of the current node. */
  protected abstract AbstractConfigNodeInfo getConfigNodeInfo();

  @Override
  public TGetClusterIdResp getClusterId() throws TException {
    return null;
  }

  @Override
  public TDataNodeRegisterResp registerDataNode(TDataNodeRegisterReq req) throws TException {
    return null;
  }

  @Override
  public TDataNodeRestartResp restartDataNode(TDataNodeRestartReq req) throws TException {
    return null;
  }

  @Override
  public TDataNodeLeaseRecoveryResp reloadCacheAfterLeaseRecovery() throws TException {
    return null;
  }

  @Override
  public TAINodeRegisterResp registerAINode(TAINodeRegisterReq req) throws TException {
    return null;
  }

  @Override
  public TAINodeRestartResp restartAINode(TAINodeRestartReq req) throws TException {
    return null;
  }

  @Override
  public TSStatus removeAINode(TAINodeRemoveReq req) throws TException {
    return null;
  }

  @Override
  public TShowAINodesResp showAINodes() throws TException {
    return null;
  }

  @Override
  public TAINodeConfigurationResp getAINodeConfiguration(int aiNodeId) throws TException {
    return null;
  }

  @Override
  public TGetAINodeLocationResp getAINodeLocation() throws TException {
    return null;
  }

  @Override
  public TDataNodeRemoveResp removeDataNode(TDataNodeRemoveReq req) throws TException {
    return null;
  }

  @Override
  public TSStatus reportDataNodeShutdown(TDataNodeLocation dataNodeLocation) throws TException {
    return null;
  }

  @Override
  public TDataNodeConfigurationResp getDataNodeConfiguration(int dataNodeId) throws TException {
    return null;
  }

  @Override
  public TSStatus setDatabase(TDatabaseSchema databaseSchema) throws TException {
    return null;
  }

  @Override
  public TSStatus alterDatabase(TDatabaseSchema databaseSchema) throws TException {
    return null;
  }

  @Override
  public TSStatus deleteDatabase(TDeleteDatabaseReq req) throws TException {
    return null;
  }

  @Override
  public TSStatus deleteDatabases(TDeleteDatabasesReq req) throws TException {
    return null;
  }

  @Override
  public TSStatus setSchemaReplicationFactor(TSetSchemaReplicationFactorReq req) throws TException {
    return null;
  }

  @Override
  public TSStatus setDataReplicationFactor(TSetDataReplicationFactorReq req) throws TException {
    return null;
  }

  @Override
  public TSStatus setTimePartitionInterval(TSetTimePartitionIntervalReq req) throws TException {
    return null;
  }

  @Override
  public TCountDatabaseResp countMatchedDatabases(TGetDatabaseReq req) throws TException {
    return null;
  }

  @Override
  public TDatabaseSchemaResp getMatchedDatabaseSchemas(TGetDatabaseReq req) throws TException {
    return null;
  }

  @Override
  public TSStatus callSpecialProcedure(TTestOperation operation) throws TException {
    return null;
  }

  @Override
  public TSchemaPartitionTableResp getSchemaPartitionTable(TSchemaPartitionReq req)
      throws TException {
    return null;
  }

  @Override
  public TSchemaPartitionTableResp getSchemaPartitionTableWithSlots(
      java.util.Map<String, java.util.List<TSeriesPartitionSlot>> dbSlotMap) throws TException {
    return null;
  }

  @Override
  public TSchemaPartitionTableResp getOrCreateSchemaPartitionTable(TSchemaPartitionReq req)
      throws TException {
    return null;
  }

  @Override
  public TSchemaPartitionTableResp getOrCreateSchemaPartitionTableWithSlots(
      java.util.Map<String, java.util.List<TSeriesPartitionSlot>> dbSlotMap) throws TException {
    return null;
  }

  @Override
  public TSchemaNodeManagementResp getSchemaNodeManagementPartition(TSchemaNodeManagementReq req)
      throws TException {
    return null;
  }

  @Override
  public TDataPartitionTableResp getDataPartitionTable(TDataPartitionReq req) throws TException {
    return null;
  }

  @Override
  public TDataPartitionTableResp getOrCreateDataPartitionTable(TDataPartitionReq req)
      throws TException {
    return null;
  }

  @Override
  public TSStatus dataPartitionTableIntegrityCheck() throws TException {
    return null;
  }

  @Override
  public TShowRepairDataPartitionTableProgressResp showRepairDataPartitionTableProgress()
      throws TException {
    return null;
  }

  @Override
  public TSStatus operatePermission(TAuthorizerReq req) throws TException {
    return null;
  }

  @Override
  public TSStatus operateRPermission(TAuthorizerRelationalReq req) throws TException {
    return null;
  }

  @Override
  public TAuthorizerResp queryPermission(TAuthorizerReq req) throws TException {
    return null;
  }

  @Override
  public TAuthorizerResp queryRPermission(TAuthorizerRelationalReq req) throws TException {
    return null;
  }

  @Override
  public TPermissionInfoResp login(TLoginReq req) throws TException {
    return null;
  }

  @Override
  public TPermissionInfoResp checkUserPrivileges(TCheckUserPrivilegesReq req) throws TException {
    return null;
  }

  @Override
  public TAuthizedPatternTreeResp fetchAuthizedPatternTree(TCheckUserPrivilegesReq req)
      throws TException {
    return null;
  }

  @Override
  public TPermissionInfoResp checkRoleOfUser(TAuthorizerReq req) throws TException {
    return null;
  }

  @Override
  public TPermissionInfoResp getUser(String userName) throws TException {
    return null;
  }

  @Override
  public TConfigNodeRegisterResp registerConfigNode(TConfigNodeRegisterReq req) throws TException {
    return null;
  }

  @Override
  public TSStatus addConsensusGroup(TAddConsensusGroupReq req) throws TException {
    return null;
  }

  @Override
  public TSStatus notifyRegisterSuccess() throws TException {
    return null;
  }

  @Override
  public TSStatus removeConfigNode(TConfigNodeLocation configNodeLocation) throws TException {
    return null;
  }

  @Override
  public TSStatus deleteConfigNodePeer(TConfigNodeLocation configNodeLocation) throws TException {
    return null;
  }

  @Override
  public TSStatus reportConfigNodeShutdown(TConfigNodeLocation configNodeLocation)
      throws TException {
    return null;
  }

  @Override
  public TSStatus stopAndClearConfigNode(TConfigNodeLocation configNodeLocation) throws TException {
    return null;
  }

  @Override
  public TConfigNodeHeartbeatResp getConfigNodeHeartBeat(TConfigNodeHeartbeatReq req)
      throws TException {
    return null;
  }

  @Override
  public TSStatus createFunction(TCreateFunctionReq req) throws TException {
    return null;
  }

  @Override
  public TSStatus dropFunction(TDropFunctionReq req) throws TException {
    return null;
  }

  @Override
  public TGetUDFTableResp getUDFTable(TGetUdfTableReq req) throws TException {
    return null;
  }

  @Override
  public TGetJarInListResp getUDFJar(TGetJarInListReq req) throws TException {
    return null;
  }

  @Override
  public TSStatus createTrigger(TCreateTriggerReq req) throws TException {
    return null;
  }

  @Override
  public TSStatus dropTrigger(TDropTriggerReq req) throws TException {
    return null;
  }

  @Override
  public TGetLocationForTriggerResp getLocationOfStatefulTrigger(String triggerName)
      throws TException {
    return null;
  }

  @Override
  public TGetTriggerTableResp getTriggerTable() throws TException {
    return null;
  }

  @Override
  public TGetTriggerTableResp getStatefulTriggerTable() throws TException {
    return null;
  }

  @Override
  public TGetJarInListResp getTriggerJar(TGetJarInListReq req) throws TException {
    return null;
  }

  @Override
  public TSStatus createPipePlugin(TCreatePipePluginReq req) throws TException {
    return null;
  }

  @Override
  public TSStatus dropPipePlugin(TDropPipePluginReq req) throws TException {
    return null;
  }

  @Override
  public TGetPipePluginTableResp getPipePluginTable() throws TException {
    return null;
  }

  @Override
  public TGetPipePluginTableResp getPipePluginTableExtended(TShowPipePluginReq req)
      throws TException {
    return null;
  }

  @Override
  public TGetJarInListResp getPipePluginJar(TGetJarInListReq req) throws TException {
    return null;
  }

  @Override
  public TShowTTLResp showTTL(TShowTTLReq req) throws TException {
    return null;
  }

  @Override
  public TSStatus setTTL(TSetTTLReq req) throws TException {
    return null;
  }

  @Override
  public TSStatus merge() throws TException {
    return null;
  }

  @Override
  public TSStatus flush(TFlushReq req) throws TException {
    return null;
  }

  @Override
  public TSStatus clearCache(java.util.Set<Integer> cacheClearOptions) throws TException {
    return null;
  }

  @Override
  public TSStatus setConfiguration(TSetConfigurationReq req) throws TException {
    return null;
  }

  @Override
  public TShowConfigurationResp showConfiguration(int nodeId) throws TException {
    return null;
  }

  @Override
  public TShowAppliedConfigurationsResp showAppliedConfigurations(int nodeId) throws TException {
    return null;
  }

  @Override
  public TSStatus startRepairData() throws TException {
    return null;
  }

  @Override
  public TSStatus stopRepairData() throws TException {
    return null;
  }

  @Override
  public TSStatus submitLoadConfigurationTask() throws TException {
    return null;
  }

  @Override
  public TSStatus loadConfiguration() throws TException {
    return null;
  }

  @Override
  public TSStatus setSystemStatus(String status) throws TException {
    return null;
  }

  @Override
  public TSStatus setDataNodeStatus(TSetDataNodeStatusReq req) throws TException {
    return null;
  }

  @Override
  public TSStatus migrateRegion(TMigrateRegionReq req) throws TException {
    return null;
  }

  @Override
  public TSStatus reconstructRegion(TReconstructRegionReq req) throws TException {
    return null;
  }

  @Override
  public TSStatus extendRegion(TExtendRegionReq req) throws TException {
    return null;
  }

  @Override
  public TSStatus removeRegion(TRemoveRegionReq req) throws TException {
    return null;
  }

  @Override
  public TSStatus killQuery(String queryId, int dataNodeId, String allowedUsername)
      throws TException {
    return null;
  }

  @Override
  public TShowClusterResp showCluster() throws TException {
    return null;
  }

  @Override
  public TShowVariablesResp showVariables() throws TException {
    return null;
  }

  @Override
  public TShowDataNodesResp showDataNodes() throws TException {
    return null;
  }

  @Override
  public TShowDataNodes4InformationSchemaResp showDataNodes4InformationSchema() throws TException {
    return null;
  }

  @Override
  public TShowConfigNodesResp showConfigNodes() throws TException {
    return null;
  }

  @Override
  public TShowConfigNodes4InformationSchemaResp showConfigNodes4InformationSchema()
      throws TException {
    return null;
  }

  @Override
  public TShowDatabaseResp showDatabase(TGetDatabaseReq req) throws TException {
    return null;
  }

  @Override
  public TTestConnectionResp submitTestConnectionTask(TNodeLocations nodeLocations)
      throws TException {
    return null;
  }

  @Override
  public TTestConnectionResp submitTestConnectionTaskToLeader() throws TException {
    return null;
  }

  @Override
  public TSStatus testConnectionEmptyRPC() throws TException {
    return null;
  }

  @Override
  public TShowRegionResp showRegion(TShowRegionReq req) throws TException {
    return null;
  }

  @Override
  public TRegionRouteMapResp getLatestRegionRouteMap() throws TException {
    return null;
  }

  @Override
  public TSStatus createSchemaTemplate(TCreateSchemaTemplateReq req) throws TException {
    return null;
  }

  @Override
  public TGetAllTemplatesResp getAllTemplates() throws TException {
    return null;
  }

  @Override
  public TGetTemplateResp getTemplate(String req) throws TException {
    return null;
  }

  @Override
  public TSStatus setSchemaTemplate(TSetSchemaTemplateReq req) throws TException {
    return null;
  }

  @Override
  public TGetPathsSetTemplatesResp getPathsSetTemplate(TGetPathsSetTemplatesReq req)
      throws TException {
    return null;
  }

  @Override
  public TSStatus deactivateSchemaTemplate(TDeactivateSchemaTemplateReq req) throws TException {
    return null;
  }

  @Override
  public TSStatus unsetSchemaTemplate(TUnsetSchemaTemplateReq req) throws TException {
    return null;
  }

  @Override
  public TSStatus dropSchemaTemplate(String req) throws TException {
    return null;
  }

  @Override
  public TSStatus alterSchemaTemplate(TAlterSchemaTemplateReq req) throws TException {
    return null;
  }

  @Override
  public TSStatus alterEncodingCompressor(TAlterEncodingCompressorReq req) throws TException {
    return null;
  }

  @Override
  public TSStatus deleteTimeSeries(TDeleteTimeSeriesReq req) throws TException {
    return null;
  }

  @Override
  public TSStatus alterTimeSeriesDataType(TAlterTimeSeriesReq req) throws TException {
    return null;
  }

  @Override
  public TSStatus deleteLogicalView(TDeleteLogicalViewReq req) throws TException {
    return null;
  }

  @Override
  public TSStatus alterLogicalView(TAlterLogicalViewReq req) throws TException {
    return null;
  }

  @Override
  public TSStatus createPipe(TCreatePipeReq req) throws TException {
    return null;
  }

  @Override
  public TSStatus alterPipe(TAlterPipeReq req) throws TException {
    return null;
  }

  @Override
  public TSStatus startPipe(String pipeName) throws TException {
    return null;
  }

  @Override
  public TSStatus startPipeExtended(TStartPipeReq req) throws TException {
    return null;
  }

  @Override
  public TSStatus stopPipe(String pipeName) throws TException {
    return null;
  }

  @Override
  public TSStatus stopPipeExtended(TStopPipeReq req) throws TException {
    return null;
  }

  @Override
  public TSStatus dropPipe(String pipeName) throws TException {
    return null;
  }

  @Override
  public TSStatus dropPipeExtended(TDropPipeReq req) throws TException {
    return null;
  }

  @Override
  public TShowPipeResp showPipe(TShowPipeReq req) throws TException {
    return null;
  }

  @Override
  public TGetAllPipeInfoResp getAllPipeInfo() throws TException {
    return null;
  }

  @Override
  public TPipeConfigTransferResp handleTransferConfigPlan(TPipeConfigTransferReq req)
      throws TException {
    return null;
  }

  @Override
  public TSStatus handlePipeConfigClientExit(String clientId) throws TException {
    return null;
  }

  @Override
  public TSStatus createTopic(TCreateTopicReq req) throws TException {
    return null;
  }

  @Override
  public TSStatus alterTopic(TAlterTopicReq req) throws TException {
    return null;
  }

  @Override
  public TSStatus dropTopic(String topicName) throws TException {
    return null;
  }

  @Override
  public TSStatus dropTopicExtended(TDropTopicReq req) throws TException {
    return null;
  }

  @Override
  public TShowTopicResp showTopic(TShowTopicReq req) throws TException {
    return null;
  }

  @Override
  public TGetAllTopicInfoResp getAllTopicInfo() throws TException {
    return null;
  }

  @Override
  public TSStatus createConsumer(TCreateConsumerReq req) throws TException {
    return null;
  }

  @Override
  public TSStatus closeConsumer(TCloseConsumerReq req) throws TException {
    return null;
  }

  @Override
  public TSStatus createSubscription(TSubscribeReq req) throws TException {
    return null;
  }

  @Override
  public TSStatus dropSubscription(TUnsubscribeReq req) throws TException {
    return null;
  }

  @Override
  public TSStatus dropSubscriptionById(TDropSubscriptionReq req) throws TException {
    return null;
  }

  @Override
  public TShowSubscriptionResp showSubscription(TShowSubscriptionReq req) throws TException {
    return null;
  }

  @Override
  public TGetAllSubscriptionInfoResp getAllSubscriptionInfo() throws TException {
    return null;
  }

  @Override
  public TGetCommitProgressResp getCommitProgress(TGetCommitProgressReq req) throws TException {
    return null;
  }

  @Override
  public TGetRegionIdResp getRegionId(TGetRegionIdReq req) throws TException {
    return null;
  }

  @Override
  public TGetTimeSlotListResp getTimeSlotList(TGetTimeSlotListReq req) throws TException {
    return null;
  }

  @Override
  public TCountTimeSlotListResp countTimeSlotList(TCountTimeSlotListReq req) throws TException {
    return null;
  }

  @Override
  public TGetSeriesSlotListResp getSeriesSlotList(TGetSeriesSlotListReq req) throws TException {
    return null;
  }

  @Override
  public TGetRegionGroupsByTimeResp getRegionGroupsByTime(TGetRegionGroupsByTimeReq req)
      throws TException {
    return null;
  }

  @Override
  public TSStatus createCQ(TCreateCQReq req) throws TException {
    return null;
  }

  @Override
  public TSStatus dropCQ(TDropCQReq req) throws TException {
    return null;
  }

  @Override
  public TShowCQResp showCQ() throws TException {
    return null;
  }

  @Override
  public TSStatus createExternalService(TCreateExternalServiceReq req) throws TException {
    return null;
  }

  @Override
  public TSStatus startExternalService(int dataNodeId, String serviceName) throws TException {
    return null;
  }

  @Override
  public TSStatus stopExternalService(int dataNodeId, String serviceName) throws TException {
    return null;
  }

  @Override
  public TSStatus dropExternalService(int dataNodeId, String serviceName) throws TException {
    return null;
  }

  @Override
  public TExternalServiceListResp showExternalService(int dataNodeId) throws TException {
    return null;
  }

  @Override
  public TSStatus setSpaceQuota(TSetSpaceQuotaReq req) throws TException {
    return null;
  }

  @Override
  public TSpaceQuotaResp showSpaceQuota(java.util.List<String> databases) throws TException {
    return null;
  }

  @Override
  public TSpaceQuotaResp getSpaceQuota() throws TException {
    return null;
  }

  @Override
  public TSStatus setThrottleQuota(TSetThrottleQuotaReq req) throws TException {
    return null;
  }

  @Override
  public TThrottleQuotaResp showThrottleQuota(TShowThrottleReq req) throws TException {
    return null;
  }

  @Override
  public TThrottleQuotaResp getThrottleQuota() throws TException {
    return null;
  }

  @Override
  public TSStatus pushHeartbeat(int dataNodeId, TPipeHeartbeatResp resp) throws TException {
    return null;
  }

  @Override
  public TSStatus createTable(ByteBuffer tableInfo) throws TException {
    return null;
  }

  @Override
  public TSStatus alterOrDropTable(TAlterOrDropTableReq req) throws TException {
    return null;
  }

  @Override
  public TShowTableResp showTables(String database, boolean isDetails) throws TException {
    return null;
  }

  @Override
  public TShowTable4InformationSchemaResp showTables4InformationSchema() throws TException {
    return null;
  }

  @Override
  public TDescTableResp describeTable(String database, String tableName, boolean isDetails)
      throws TException {
    return null;
  }

  @Override
  public TDescTable4InformationSchemaResp descTables4InformationSchema() throws TException {
    return null;
  }

  @Override
  public TFetchTableResp fetchTables(Map<String, Set<String>> fetchTableMap, byte tableNodeStatus)
      throws TException {
    return null;
  }

  @Override
  public TDeleteTableDeviceResp deleteDevice(TDeleteTableDeviceReq req) throws TException {
    return null;
  }

  @Override
  public TSStatus createTableView(TCreateTableViewReq req) throws TException {
    return null;
  }
}
