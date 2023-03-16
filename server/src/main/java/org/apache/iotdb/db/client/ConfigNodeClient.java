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
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TFlushReq;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.common.rpc.thrift.TSetTTLReq;
import org.apache.iotdb.commons.client.ClientManager;
import org.apache.iotdb.commons.client.ThriftClient;
import org.apache.iotdb.commons.client.factory.ThriftClientFactory;
import org.apache.iotdb.commons.client.property.ThriftClientProperty;
import org.apache.iotdb.commons.client.sync.SyncThriftClientWithErrorHandler;
import org.apache.iotdb.commons.consensus.ConfigRegionId;
import org.apache.iotdb.confignode.rpc.thrift.IConfigNodeRPCService;
import org.apache.iotdb.confignode.rpc.thrift.TAddConsensusGroupReq;
import org.apache.iotdb.confignode.rpc.thrift.TAuthorizerReq;
import org.apache.iotdb.confignode.rpc.thrift.TAuthorizerResp;
import org.apache.iotdb.confignode.rpc.thrift.TCheckUserPrivilegesReq;
import org.apache.iotdb.confignode.rpc.thrift.TConfigNodeRegisterReq;
import org.apache.iotdb.confignode.rpc.thrift.TConfigNodeRegisterResp;
import org.apache.iotdb.confignode.rpc.thrift.TConfigNodeRestartReq;
import org.apache.iotdb.confignode.rpc.thrift.TCountDatabaseResp;
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
import org.apache.iotdb.confignode.rpc.thrift.TGetJarInListReq;
import org.apache.iotdb.confignode.rpc.thrift.TGetJarInListResp;
import org.apache.iotdb.confignode.rpc.thrift.TGetLocationForTriggerResp;
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
import org.apache.iotdb.confignode.rpc.thrift.TRecordPipeMessageReq;
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
import org.apache.iotdb.confignode.rpc.thrift.TShowTrailReq;
import org.apache.iotdb.confignode.rpc.thrift.TShowTrailResp;
import org.apache.iotdb.confignode.rpc.thrift.TShowVariablesResp;
import org.apache.iotdb.confignode.rpc.thrift.TSystemConfigurationResp;
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

public class ConfigNodeClient implements IConfigNodeRPCService.Iface, ThriftClient, AutoCloseable {

  private static final Logger logger = LoggerFactory.getLogger(ConfigNodeClient.class);

  private static final int RETRY_NUM = 5;

  public static final String MSG_RECONNECTION_FAIL =
      "Fail to connect to any config node. Please check status of ConfigNodes";

  private static final int RETRY_INTERVAL_MS = 1000;

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
      // wait to start the next try
      Thread.sleep(RETRY_INTERVAL_MS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new TException(
          "Unexpected interruption when waiting to retry to connect to ConfigNode");
    }

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

  @Override
  public TSystemConfigurationResp getSystemConfiguration() throws TException {
    for (int i = 0; i < RETRY_NUM; i++) {
      try {
        TSystemConfigurationResp resp = client.getSystemConfiguration();
        if (!updateConfigNodeLeader(resp.status)) {
          return resp;
        }
      } catch (TException e) {
        logger.warn(
            "Failed to connect to ConfigNode {} from DataNode {} when executing {}",
            configNode,
            config.getAddressAndPort(),
            Thread.currentThread().getStackTrace()[1].getMethodName());
        configLeader = null;
      }
      waitAndReconnect();
    }
    throw new TException(MSG_RECONNECTION_FAIL);
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
        logger.warn(
            "Failed to connect to ConfigNode {} from DataNode {} when executing {}",
            configNode,
            config.getAddressAndPort(),
            Thread.currentThread().getStackTrace()[1].getMethodName());
        configLeader = null;
      }
      waitAndReconnect();
    }
    throw new TException(MSG_RECONNECTION_FAIL);
  }

  @Override
  public TDataNodeRestartResp restartDataNode(TDataNodeRestartReq req) throws TException {
    for (int i = 0; i < RETRY_NUM; i++) {
      try {
        TDataNodeRestartResp resp = client.restartDataNode(req);
        if (!updateConfigNodeLeader(resp.status)) {
          return resp;
        }
      } catch (TException e) {
        logger.warn(
            "Failed to connect to ConfigNode {} from DataNode {} when executing {}",
            configNode,
            config.getAddressAndPort(),
            Thread.currentThread().getStackTrace()[1].getMethodName());
        configLeader = null;
      }
      waitAndReconnect();
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
        logger.warn(
            "Failed to connect to ConfigNode {} from DataNode {} when executing {}",
            configNode,
            config.getAddressAndPort(),
            Thread.currentThread().getStackTrace()[1].getMethodName());
        configLeader = null;
      }
      waitAndReconnect();
    }
    throw new TException(MSG_RECONNECTION_FAIL);
  }

  @Override
  public TSStatus reportDataNodeShutdown(TDataNodeLocation dataNodeLocation) throws TException {
    for (int i = 0; i < RETRY_NUM; i++) {
      try {
        TSStatus status = client.reportDataNodeShutdown(dataNodeLocation);
        if (!updateConfigNodeLeader(status)) {
          return status;
        }
      } catch (TException e) {
        configLeader = null;
      }
      waitAndReconnect();
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
        logger.warn(
            "Failed to connect to ConfigNode {} from DataNode {} when executing {}",
            configNode,
            config.getAddressAndPort(),
            Thread.currentThread().getStackTrace()[1].getMethodName());
        configLeader = null;
      }
      waitAndReconnect();
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
        logger.warn(
            "Failed to connect to ConfigNode {} from DataNode {} when executing {}",
            configNode,
            config.getAddressAndPort(),
            Thread.currentThread().getStackTrace()[1].getMethodName());
        configLeader = null;
      }
      waitAndReconnect();
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
        logger.warn(
            "Failed to connect to ConfigNode {} from DataNode {} when executing {}",
            configNode,
            config.getAddressAndPort(),
            Thread.currentThread().getStackTrace()[1].getMethodName());
        configLeader = null;
      }
      waitAndReconnect();
    }
    throw new TException(MSG_RECONNECTION_FAIL);
  }

  @Override
  public TShowVariablesResp showVariables() throws TException {
    for (int i = 0; i < RETRY_NUM; i++) {
      try {
        TShowVariablesResp resp = client.showVariables();
        if (!updateConfigNodeLeader(resp.status)) {
          return resp;
        }
      } catch (TException e) {
        logger.warn(
            "Failed to connect to ConfigNode {} from DataNode {} when executing {}",
            configNode,
            config.getAddressAndPort(),
            Thread.currentThread().getStackTrace()[1].getMethodName());
        configLeader = null;
      }
      waitAndReconnect();
    }
    throw new TException(MSG_RECONNECTION_FAIL);
  }

  @Override
  public TSStatus setDatabase(TDatabaseSchema databaseSchema) throws TException {
    for (int i = 0; i < RETRY_NUM; i++) {
      try {
        TSStatus status = client.setDatabase(databaseSchema);
        if (!updateConfigNodeLeader(status)) {
          return status;
        }
      } catch (TException e) {
        logger.warn(
            "Failed to connect to ConfigNode {} from DataNode {} when executing {}",
            configNode,
            config.getAddressAndPort(),
            Thread.currentThread().getStackTrace()[1].getMethodName());
        configLeader = null;
      }
      waitAndReconnect();
    }
    throw new TException(MSG_RECONNECTION_FAIL);
  }

  @Override
  public TSStatus alterDatabase(TDatabaseSchema databaseSchema) throws TException {
    for (int i = 0; i < RETRY_NUM; i++) {
      try {
        TSStatus status = client.alterDatabase(databaseSchema);
        if (!updateConfigNodeLeader(status)) {
          return status;
        }
      } catch (TException e) {
        logger.warn(
            "Failed to connect to ConfigNode {} from DataNode {} when executing {}",
            configNode,
            config.getAddressAndPort(),
            Thread.currentThread().getStackTrace()[1].getMethodName());
        configLeader = null;
      }
      waitAndReconnect();
    }
    throw new TException(MSG_RECONNECTION_FAIL);
  }

  @Override
  public TSStatus deleteDatabase(TDeleteDatabaseReq req) throws TException {
    for (int i = 0; i < RETRY_NUM; i++) {
      try {
        TSStatus status = client.deleteDatabase(req);
        if (!updateConfigNodeLeader(status)) {
          return status;
        }
      } catch (TException e) {
        logger.warn(
            "Failed to connect to ConfigNode {} from DataNode {} when executing {}",
            configNode,
            config.getAddressAndPort(),
            Thread.currentThread().getStackTrace()[1].getMethodName());
        configLeader = null;
      }
      waitAndReconnect();
    }
    throw new TException(MSG_RECONNECTION_FAIL);
  }

  @Override
  public TSStatus deleteDatabases(TDeleteDatabasesReq req) throws TException {
    for (int i = 0; i < RETRY_NUM; i++) {
      try {
        TSStatus status = client.deleteDatabases(req);
        if (!updateConfigNodeLeader(status)) {
          return status;
        }
      } catch (TException e) {
        logger.warn(
            "Failed to connect to ConfigNode {} from DataNode {} when executing {}",
            configNode,
            config.getAddressAndPort(),
            Thread.currentThread().getStackTrace()[1].getMethodName());
        configLeader = null;
      }
      waitAndReconnect();
    }
    throw new TException(MSG_RECONNECTION_FAIL);
  }

  @Override
  public TCountDatabaseResp countMatchedDatabases(List<String> storageGroupPathPattern)
      throws TException {
    for (int i = 0; i < RETRY_NUM; i++) {
      try {
        TCountDatabaseResp resp = client.countMatchedDatabases(storageGroupPathPattern);
        if (!updateConfigNodeLeader(resp.status)) {
          return resp;
        }
      } catch (TException e) {
        logger.warn(
            "Failed to connect to ConfigNode {} from DataNode {} when executing {}",
            configNode,
            config.getAddressAndPort(),
            Thread.currentThread().getStackTrace()[1].getMethodName());
        configLeader = null;
      }
      waitAndReconnect();
    }
    throw new TException(MSG_RECONNECTION_FAIL);
  }

  @Override
  public TDatabaseSchemaResp getMatchedDatabaseSchemas(List<String> storageGroupPathPattern)
      throws TException {
    for (int i = 0; i < RETRY_NUM; i++) {
      try {
        TDatabaseSchemaResp resp = client.getMatchedDatabaseSchemas(storageGroupPathPattern);
        if (!updateConfigNodeLeader(resp.status)) {
          return resp;
        }
      } catch (TException e) {
        logger.warn(
            "Failed to connect to ConfigNode {} from DataNode {} when executing {}",
            configNode,
            config.getAddressAndPort(),
            Thread.currentThread().getStackTrace()[1].getMethodName());
        configLeader = null;
      }
      waitAndReconnect();
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
        logger.warn(
            "Failed to connect to ConfigNode {} from DataNode {} when executing {}",
            configNode,
            config.getAddressAndPort(),
            Thread.currentThread().getStackTrace()[1].getMethodName());
        configLeader = null;
      }
      waitAndReconnect();
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
        logger.warn(
            "Failed to connect to ConfigNode {} from DataNode {} when executing {}",
            configNode,
            config.getAddressAndPort(),
            Thread.currentThread().getStackTrace()[1].getMethodName());
        configLeader = null;
      }
      waitAndReconnect();
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
        logger.warn(
            "Failed to connect to ConfigNode {} from DataNode {} when executing {}",
            configNode,
            config.getAddressAndPort(),
            Thread.currentThread().getStackTrace()[1].getMethodName());
        configLeader = null;
      }
      waitAndReconnect();
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
        logger.warn(
            "Failed to connect to ConfigNode {} from DataNode {} when executing {}",
            configNode,
            config.getAddressAndPort(),
            Thread.currentThread().getStackTrace()[1].getMethodName());
        configLeader = null;
      }
      waitAndReconnect();
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
        logger.warn(
            "Failed to connect to ConfigNode {} from DataNode {} when executing {}",
            configNode,
            config.getAddressAndPort(),
            Thread.currentThread().getStackTrace()[1].getMethodName());
        configLeader = null;
      }
      waitAndReconnect();
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
        logger.warn(
            "Failed to connect to ConfigNode {} from DataNode {} when executing {}",
            configNode,
            config.getAddressAndPort(),
            Thread.currentThread().getStackTrace()[1].getMethodName());
        configLeader = null;
      }
      waitAndReconnect();
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
        logger.warn(
            "Failed to connect to ConfigNode {} from DataNode {} when executing {}",
            configNode,
            config.getAddressAndPort(),
            Thread.currentThread().getStackTrace()[1].getMethodName());
        configLeader = null;
      }
      waitAndReconnect();
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
        logger.warn(
            "Failed to connect to ConfigNode {} from DataNode {} when executing {}",
            configNode,
            config.getAddressAndPort(),
            Thread.currentThread().getStackTrace()[1].getMethodName());
        configLeader = null;
      }
      waitAndReconnect();
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
        logger.warn(
            "Failed to connect to ConfigNode {} from DataNode {} when executing {}",
            configNode,
            config.getAddressAndPort(),
            Thread.currentThread().getStackTrace()[1].getMethodName());
        configLeader = null;
      }
      waitAndReconnect();
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
        logger.warn(
            "Failed to connect to ConfigNode {} from DataNode {} when executing {}",
            configNode,
            config.getAddressAndPort(),
            Thread.currentThread().getStackTrace()[1].getMethodName());
        configLeader = null;
      }
      waitAndReconnect();
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
        logger.warn(
            "Failed to connect to ConfigNode {} from DataNode {} when executing {}",
            configNode,
            config.getAddressAndPort(),
            Thread.currentThread().getStackTrace()[1].getMethodName());
        configLeader = null;
      }
      waitAndReconnect();
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
        logger.warn(
            "Failed to connect to ConfigNode {} from DataNode {} when executing {}",
            configNode,
            config.getAddressAndPort(),
            Thread.currentThread().getStackTrace()[1].getMethodName());
        configLeader = null;
      }
      waitAndReconnect();
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
        logger.warn(
            "Failed to connect to ConfigNode {} from DataNode {} when executing {}",
            configNode,
            config.getAddressAndPort(),
            Thread.currentThread().getStackTrace()[1].getMethodName());
        configLeader = null;
      }
      waitAndReconnect();
    }
    throw new TException(MSG_RECONNECTION_FAIL);
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
    for (int i = 0; i < RETRY_NUM; i++) {
      try {
        TSStatus status = client.merge();
        if (!updateConfigNodeLeader(status)) {
          return status;
        }
      } catch (TException e) {
        logger.warn(
            "Failed to connect to ConfigNode {} from DataNode {} when executing {}",
            configNode,
            config.getAddressAndPort(),
            Thread.currentThread().getStackTrace()[1].getMethodName());
        configLeader = null;
      }
      waitAndReconnect();
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
        logger.warn(
            "Failed to connect to ConfigNode {} from DataNode {} when executing {}",
            configNode,
            config.getAddressAndPort(),
            Thread.currentThread().getStackTrace()[1].getMethodName());
        configLeader = null;
      }
      waitAndReconnect();
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
        logger.warn(
            "Failed to connect to ConfigNode {} from DataNode {} when executing {}",
            configNode,
            config.getAddressAndPort(),
            Thread.currentThread().getStackTrace()[1].getMethodName());
        configLeader = null;
      }
      waitAndReconnect();
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
        logger.warn(
            "Failed to connect to ConfigNode {} from DataNode {} when executing {}",
            configNode,
            config.getAddressAndPort(),
            Thread.currentThread().getStackTrace()[1].getMethodName());
        configLeader = null;
      }
      waitAndReconnect();
    }
    throw new TException(MSG_RECONNECTION_FAIL);
  }

  @Override
  public TSStatus setSystemStatus(String systemStatus) throws TException {
    for (int i = 0; i < RETRY_NUM; i++) {
      try {
        TSStatus status = client.setSystemStatus(systemStatus);
        if (!updateConfigNodeLeader(status)) {
          return status;
        }
      } catch (TException e) {
        logger.warn(
            "Failed to connect to ConfigNode {} from DataNode {} when executing {}",
            configNode,
            config.getAddressAndPort(),
            Thread.currentThread().getStackTrace()[1].getMethodName());
        configLeader = null;
      }
      waitAndReconnect();
    }
    throw new TException(MSG_RECONNECTION_FAIL);
  }

  @Override
  public TSStatus setDataNodeStatus(TSetDataNodeStatusReq req) throws TException {
    throw new TException("DataNode to ConfigNode client doesn't support setDataNodeStatus.");
  }

  @Override
  public TSStatus killQuery(String queryId, int dataNodeId) throws TException {
    for (int i = 0; i < RETRY_NUM; i++) {
      try {
        TSStatus status = client.killQuery(queryId, dataNodeId);
        if (!updateConfigNodeLeader(status)) {
          return status;
        }
      } catch (TException e) {
        logger.warn(
            "Failed to connect to ConfigNode {} from DataNode {} when executing {}",
            configNode,
            config.getAddressAndPort(),
            Thread.currentThread().getStackTrace()[1].getMethodName());
        configLeader = null;
      }
      waitAndReconnect();
    }
    throw new TException(MSG_RECONNECTION_FAIL);
  }

  @Override
  public TGetDataNodeLocationsResp getRunningDataNodeLocations() throws TException {
    for (int i = 0; i < RETRY_NUM; i++) {
      try {
        TGetDataNodeLocationsResp resp = client.getRunningDataNodeLocations();
        if (!updateConfigNodeLeader(resp.status)) {
          return resp;
        }
      } catch (TException e) {
        logger.warn(
            "Failed to connect to ConfigNode {} from DataNode {} when executing {}",
            configNode,
            config.getAddressAndPort(),
            Thread.currentThread().getStackTrace()[1].getMethodName());
        configLeader = null;
      }
      waitAndReconnect();
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
        logger.warn(
            "Failed to connect to ConfigNode {} from DataNode {} when executing {}",
            configNode,
            config.getAddressAndPort(),
            Thread.currentThread().getStackTrace()[1].getMethodName());
        configLeader = null;
      }
      waitAndReconnect();
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
        logger.warn(
            "Failed to connect to ConfigNode {} from DataNode {} when executing {}",
            configNode,
            config.getAddressAndPort(),
            Thread.currentThread().getStackTrace()[1].getMethodName());
        configLeader = null;
      }
      waitAndReconnect();
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
        logger.warn(
            "Failed to connect to ConfigNode {} from DataNode {} when executing {}",
            configNode,
            config.getAddressAndPort(),
            Thread.currentThread().getStackTrace()[1].getMethodName());
        configLeader = null;
      }
      waitAndReconnect();
    }
    throw new TException(MSG_RECONNECTION_FAIL);
  }

  @Override
  public TShowDatabaseResp showDatabase(List<String> storageGroupPathPattern) throws TException {
    for (int i = 0; i < RETRY_NUM; i++) {
      try {
        TShowDatabaseResp showStorageGroupResp = client.showDatabase(storageGroupPathPattern);
        if (!updateConfigNodeLeader(showStorageGroupResp.getStatus())) {
          return showStorageGroupResp;
        }
      } catch (TException e) {
        logger.warn(
            "Failed to connect to ConfigNode {} from DataNode {} when executing {}",
            configNode,
            config.getAddressAndPort(),
            Thread.currentThread().getStackTrace()[1].getMethodName());
        configLeader = null;
      }
      waitAndReconnect();
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
        logger.warn(
            "Failed to connect to ConfigNode {} from DataNode {} when executing {}",
            configNode,
            config.getAddressAndPort(),
            Thread.currentThread().getStackTrace()[1].getMethodName());
        configLeader = null;
      }
      waitAndReconnect();
    }
    throw new TException(MSG_RECONNECTION_FAIL);
  }

  @Override
  public long getConfigNodeHeartBeat(long timestamp) throws TException {
    throw new TException("DataNode to ConfigNode client doesn't support getConfigNodeHeartBeat.");
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
        logger.warn(
            "Failed to connect to ConfigNode {} from DataNode {} when executing {}",
            configNode,
            config.getAddressAndPort(),
            Thread.currentThread().getStackTrace()[1].getMethodName());
        configLeader = null;
      }
      waitAndReconnect();
    }
    throw new TException(MSG_RECONNECTION_FAIL);
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
        logger.warn(
            "Failed to connect to ConfigNode {} from DataNode {} when executing {}",
            configNode,
            config.getAddressAndPort(),
            Thread.currentThread().getStackTrace()[1].getMethodName());
        configLeader = null;
      }
      waitAndReconnect();
    }
    throw new TException(MSG_RECONNECTION_FAIL);
  }

  @Override
  public TGetUDFTableResp getUDFTable() throws TException {
    for (int i = 0; i < RETRY_NUM; i++) {
      try {
        TGetUDFTableResp resp = client.getUDFTable();
        if (!updateConfigNodeLeader(resp.getStatus())) {
          return resp;
        }
      } catch (TException e) {
        configLeader = null;
      }
      waitAndReconnect();
    }
    throw new TException(MSG_RECONNECTION_FAIL);
  }

  @Override
  public TGetJarInListResp getUDFJar(TGetJarInListReq req) throws TException {
    for (int i = 0; i < RETRY_NUM; i++) {
      try {
        TGetJarInListResp resp = client.getUDFJar(req);
        if (!updateConfigNodeLeader(resp.getStatus())) {
          return resp;
        }
      } catch (TException e) {
        configLeader = null;
      }
      waitAndReconnect();
    }
    throw new TException(MSG_RECONNECTION_FAIL);
  }

  @Override
  public TSStatus createTrigger(TCreateTriggerReq req) throws TException {
    for (int i = 0; i < 5; i++) {
      try {
        TSStatus status = client.createTrigger(req);
        if (!updateConfigNodeLeader(status)) {
          return status;
        }
      } catch (TException e) {
        logger.warn(
            "Failed to connect to ConfigNode {} from DataNode {} when executing {}",
            configNode,
            config.getAddressAndPort(),
            Thread.currentThread().getStackTrace()[1].getMethodName());
        configLeader = null;
      }
      waitAndReconnect();
    }
    throw new TException(MSG_RECONNECTION_FAIL);
  }

  @Override
  public TSStatus dropTrigger(TDropTriggerReq req) throws TException {
    for (int i = 0; i < RETRY_NUM; i++) {
      try {
        TSStatus status = client.dropTrigger(req);
        if (!updateConfigNodeLeader(status)) {
          return status;
        }
      } catch (TException e) {
        logger.warn(
            "Failed to connect to ConfigNode {} from DataNode {} when executing {}",
            configNode,
            config.getAddressAndPort(),
            Thread.currentThread().getStackTrace()[1].getMethodName());
        configLeader = null;
      }
      waitAndReconnect();
    }
    throw new TException(MSG_RECONNECTION_FAIL);
  }

  @Override
  public TGetLocationForTriggerResp getLocationOfStatefulTrigger(String triggerName)
      throws TException {
    for (int i = 0; i < RETRY_NUM; i++) {
      try {
        TGetLocationForTriggerResp resp = client.getLocationOfStatefulTrigger(triggerName);
        if (!updateConfigNodeLeader(resp.getStatus())) {
          return resp;
        }
      } catch (TException e) {
        logger.warn(
            "Failed to connect to ConfigNode {} from DataNode {} when executing {}",
            configNode,
            config.getAddressAndPort(),
            Thread.currentThread().getStackTrace()[1].getMethodName());
        configLeader = null;
      }
      waitAndReconnect();
    }
    throw new TException(MSG_RECONNECTION_FAIL);
  }

  public TGetTriggerTableResp getTriggerTable() throws TException {
    for (int i = 0; i < RETRY_NUM; i++) {
      try {
        TGetTriggerTableResp resp = client.getTriggerTable();
        if (!updateConfigNodeLeader(resp.getStatus())) {
          return resp;
        }
      } catch (TException e) {
        logger.warn(
            "Failed to connect to ConfigNode {} from DataNode {} when executing {}",
            configNode,
            config.getAddressAndPort(),
            Thread.currentThread().getStackTrace()[1].getMethodName());
        configLeader = null;
      }
      waitAndReconnect();
    }
    throw new TException(MSG_RECONNECTION_FAIL);
  }

  @Override
  public TGetTriggerTableResp getStatefulTriggerTable() throws TException {
    for (int i = 0; i < RETRY_NUM; i++) {
      try {
        TGetTriggerTableResp resp = client.getStatefulTriggerTable();
        if (!updateConfigNodeLeader(resp.getStatus())) {
          return resp;
        }
      } catch (TException e) {
        logger.warn(
            "Failed to connect to ConfigNode {} from DataNode {} when executing {}",
            configNode,
            config.getAddressAndPort(),
            Thread.currentThread().getStackTrace()[1].getMethodName());
        configLeader = null;
      }
      waitAndReconnect();
    }
    throw new TException(MSG_RECONNECTION_FAIL);
  }

  @Override
  public TGetJarInListResp getTriggerJar(TGetJarInListReq req) throws TException {
    for (int i = 0; i < RETRY_NUM; i++) {
      try {
        TGetJarInListResp resp = client.getTriggerJar(req);
        if (!updateConfigNodeLeader(resp.getStatus())) {
          return resp;
        }
      } catch (TException e) {
        logger.warn(
            "Failed to connect to ConfigNode {} from DataNode {} when executing {}",
            configNode,
            config.getAddressAndPort(),
            Thread.currentThread().getStackTrace()[1].getMethodName());
        configLeader = null;
      }
      waitAndReconnect();
    }
    throw new TException(MSG_RECONNECTION_FAIL);
  }

  @Override
  public TSStatus createPipePlugin(TCreatePipePluginReq req) throws TException {
    for (int i = 0; i < RETRY_NUM; i++) {
      try {
        TSStatus status = client.createPipePlugin(req);
        if (!updateConfigNodeLeader(status)) {
          return status;
        }
      } catch (TException e) {
        logger.warn(
            "Failed to connect to ConfigNode {} from DataNode {} when executing {}",
            configNode,
            config.getAddressAndPort(),
            Thread.currentThread().getStackTrace()[1].getMethodName());
        configLeader = null;
      }
      waitAndReconnect();
    }
    throw new TException(MSG_RECONNECTION_FAIL);
  }

  @Override
  public TSStatus dropPipePlugin(TDropPipePluginReq req) throws TException {
    for (int i = 0; i < RETRY_NUM; i++) {
      try {
        TSStatus status = client.dropPipePlugin(req);
        if (!updateConfigNodeLeader(status)) {
          return status;
        }
      } catch (TException e) {
        logger.warn(
            "Failed to connect to ConfigNode {} from DataNode {} when executing {}",
            configNode,
            config.getAddressAndPort(),
            Thread.currentThread().getStackTrace()[1].getMethodName());
        configLeader = null;
      }
      waitAndReconnect();
    }
    throw new TException(MSG_RECONNECTION_FAIL);
  }

  @Override
  public TGetPipePluginTableResp getPipePluginTable() throws TException {
    for (int i = 0; i < RETRY_NUM; i++) {
      try {
        TGetPipePluginTableResp resp = client.getPipePluginTable();
        if (!updateConfigNodeLeader(resp.getStatus())) {
          return resp;
        }
      } catch (TException e) {
        logger.warn(
            "Failed to connect to ConfigNode {} from DataNode {} when executing {}",
            configNode,
            config.getAddressAndPort(),
            Thread.currentThread().getStackTrace()[1].getMethodName());
        configLeader = null;
      }
      waitAndReconnect();
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
        logger.warn(
            "Failed to connect to ConfigNode {} from DataNode {} when executing {}",
            configNode,
            config.getAddressAndPort(),
            Thread.currentThread().getStackTrace()[1].getMethodName());
        configLeader = null;
      }
      waitAndReconnect();
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
        logger.warn(
            "Failed to connect to ConfigNode {} from DataNode {} when executing {}",
            configNode,
            config.getAddressAndPort(),
            Thread.currentThread().getStackTrace()[1].getMethodName());
        configLeader = null;
      }
      waitAndReconnect();
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
        logger.warn(
            "Failed to connect to ConfigNode {} from DataNode {} when executing {}",
            configNode,
            config.getAddressAndPort(),
            Thread.currentThread().getStackTrace()[1].getMethodName());
        configLeader = null;
      }
      waitAndReconnect();
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
        logger.warn(
            "Failed to connect to ConfigNode {} from DataNode {} when executing {}",
            configNode,
            config.getAddressAndPort(),
            Thread.currentThread().getStackTrace()[1].getMethodName());
        configLeader = null;
      }
      waitAndReconnect();
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
        logger.warn(
            "Failed to connect to ConfigNode {} from DataNode {} when executing {}",
            configNode,
            config.getAddressAndPort(),
            Thread.currentThread().getStackTrace()[1].getMethodName());
        configLeader = null;
      }
      waitAndReconnect();
    }
    throw new TException(MSG_RECONNECTION_FAIL);
  }

  @Override
  public TSStatus deactivateSchemaTemplate(TDeactivateSchemaTemplateReq req) throws TException {
    for (int i = 0; i < RETRY_NUM; i++) {
      try {
        TSStatus status = client.deactivateSchemaTemplate(req);
        if (!updateConfigNodeLeader(status)) {
          return status;
        }
      } catch (TException e) {
        logger.warn(
            "Failed to connect to ConfigNode {} from DataNode {} when executing {}",
            configNode,
            config.getAddressAndPort(),
            Thread.currentThread().getStackTrace()[1].getMethodName());
        configLeader = null;
      }
      waitAndReconnect();
    }
    throw new TException(MSG_RECONNECTION_FAIL);
  }

  @Override
  public TSStatus unsetSchemaTemplate(TUnsetSchemaTemplateReq req) throws TException {
    for (int i = 0; i < RETRY_NUM; i++) {
      try {
        TSStatus status = client.unsetSchemaTemplate(req);
        if (!updateConfigNodeLeader(status)) {
          return status;
        }
      } catch (TException e) {
        configLeader = null;
      }
      waitAndReconnect();
    }
    throw new TException(MSG_RECONNECTION_FAIL);
  }

  @Override
  public TSStatus dropSchemaTemplate(String req) throws TException {
    for (int i = 0; i < RETRY_NUM; i++) {
      try {
        TSStatus status = client.dropSchemaTemplate(req);
        if (!updateConfigNodeLeader(status)) {
          return status;
        }
      } catch (TException e) {
        logger.warn(
            "Failed to connect to ConfigNode {} from DataNode {} when executing {}",
            configNode,
            config.getAddressAndPort(),
            Thread.currentThread().getStackTrace()[1].getMethodName());
        configLeader = null;
      }
      waitAndReconnect();
    }
    throw new TException(MSG_RECONNECTION_FAIL);
  }

  @Override
  public TSStatus deleteTimeSeries(TDeleteTimeSeriesReq req) throws TException {
    for (int i = 0; i < RETRY_NUM; i++) {
      try {
        TSStatus status = client.deleteTimeSeries(req);
        if (!updateConfigNodeLeader(status)) {
          return status;
        }
      } catch (TException e) {
        logger.warn(
            "Failed to connect to ConfigNode {} from DataNode {} when executing {}",
            configNode,
            config.getAddressAndPort(),
            Thread.currentThread().getStackTrace()[1].getMethodName());
        configLeader = null;
      }
      waitAndReconnect();
    }
    throw new TException(MSG_RECONNECTION_FAIL);
  }

  @Override
  public TSStatus createPipeSink(TPipeSinkInfo req) throws TException {
    for (int i = 0; i < RETRY_NUM; i++) {
      try {
        TSStatus status = client.createPipeSink(req);
        if (!updateConfigNodeLeader(status)) {
          return status;
        }
      } catch (TException e) {
        logger.warn(
            "Failed to connect to ConfigNode {} from DataNode {} when executing {}",
            configNode,
            config.getAddressAndPort(),
            Thread.currentThread().getStackTrace()[1].getMethodName());
        configLeader = null;
      }
      waitAndReconnect();
    }
    throw new TException(MSG_RECONNECTION_FAIL);
  }

  @Override
  public TSStatus dropPipeSink(TDropPipeSinkReq req) throws TException {
    for (int i = 0; i < RETRY_NUM; i++) {
      try {
        TSStatus status = client.dropPipeSink(req);
        if (!updateConfigNodeLeader(status)) {
          return status;
        }
      } catch (TException e) {
        logger.warn(
            "Failed to connect to ConfigNode {} from DataNode {} when executing {}",
            configNode,
            config.getAddressAndPort(),
            Thread.currentThread().getStackTrace()[1].getMethodName());
        configLeader = null;
      }
      waitAndReconnect();
    }
    throw new TException(MSG_RECONNECTION_FAIL);
  }

  @Override
  public TGetPipeSinkResp getPipeSink(TGetPipeSinkReq req) throws TException {
    for (int i = 0; i < RETRY_NUM; i++) {
      try {
        TGetPipeSinkResp resp = client.getPipeSink(req);
        if (!updateConfigNodeLeader(resp.getStatus())) {
          return resp;
        }
      } catch (TException e) {
        logger.warn(
            "Failed to connect to ConfigNode {} from DataNode {} when executing {}",
            configNode,
            config.getAddressAndPort(),
            Thread.currentThread().getStackTrace()[1].getMethodName());
        configLeader = null;
      }
      waitAndReconnect();
    }
    throw new TException(MSG_RECONNECTION_FAIL);
  }

  @Override
  public TSStatus createPipe(TCreatePipeReq req) throws TException {
    for (int i = 0; i < RETRY_NUM; i++) {
      try {
        TSStatus status = client.createPipe(req);
        if (!updateConfigNodeLeader(status)) {
          return status;
        }
      } catch (TException e) {
        logger.warn(
            "Failed to connect to ConfigNode {} from DataNode {} when executing {}",
            configNode,
            config.getAddressAndPort(),
            Thread.currentThread().getStackTrace()[1].getMethodName());
        configLeader = null;
      }
      waitAndReconnect();
    }
    throw new TException(MSG_RECONNECTION_FAIL);
  }

  @Override
  public TSStatus startPipe(String pipeName) throws TException {
    for (int i = 0; i < RETRY_NUM; i++) {
      try {
        TSStatus status = client.startPipe(pipeName);
        if (!updateConfigNodeLeader(status)) {
          return status;
        }
      } catch (TException e) {
        logger.warn(
            "Failed to connect to ConfigNode {} from DataNode {} when executing {}",
            configNode,
            config.getAddressAndPort(),
            Thread.currentThread().getStackTrace()[1].getMethodName());
        configLeader = null;
      }
      waitAndReconnect();
    }
    throw new TException(MSG_RECONNECTION_FAIL);
  }

  @Override
  public TSStatus stopPipe(String pipeName) throws TException {
    for (int i = 0; i < RETRY_NUM; i++) {
      try {
        TSStatus status = client.stopPipe(pipeName);
        if (!updateConfigNodeLeader(status)) {
          return status;
        }
      } catch (TException e) {
        logger.warn(
            "Failed to connect to ConfigNode {} from DataNode {} when executing {}",
            configNode,
            config.getAddressAndPort(),
            Thread.currentThread().getStackTrace()[1].getMethodName());
        configLeader = null;
      }
      waitAndReconnect();
    }
    throw new TException(MSG_RECONNECTION_FAIL);
  }

  @Override
  public TSStatus dropPipe(String pipeName) throws TException {
    for (int i = 0; i < RETRY_NUM; i++) {
      try {
        TSStatus status = client.dropPipe(pipeName);
        if (!updateConfigNodeLeader(status)) {
          return status;
        }
      } catch (TException e) {
        logger.warn(
            "Failed to connect to ConfigNode {} from DataNode {} when executing {}",
            configNode,
            config.getAddressAndPort(),
            Thread.currentThread().getStackTrace()[1].getMethodName());
        configLeader = null;
      }
      waitAndReconnect();
    }
    throw new TException(MSG_RECONNECTION_FAIL);
  }

  @Override
  public TShowPipeResp showPipe(TShowPipeReq req) throws TException {
    for (int i = 0; i < RETRY_NUM; i++) {
      try {
        TShowPipeResp resp = client.showPipe(req);
        if (!updateConfigNodeLeader(resp.getStatus())) {
          return resp;
        }
      } catch (TException e) {
        logger.warn(
            "Failed to connect to ConfigNode {} from DataNode {} when executing {}",
            configNode,
            config.getAddressAndPort(),
            Thread.currentThread().getStackTrace()[1].getMethodName());
        configLeader = null;
      }
      waitAndReconnect();
    }
    throw new TException(MSG_RECONNECTION_FAIL);
  }

  @Override
  public TGetAllPipeInfoResp getAllPipeInfo() throws TException {
    for (int i = 0; i < RETRY_NUM; i++) {
      try {
        TGetAllPipeInfoResp resp = client.getAllPipeInfo();
        if (!updateConfigNodeLeader(resp.getStatus())) {
          return resp;
        }
      } catch (TException e) {
        configLeader = null;
      }
      waitAndReconnect();
    }
    throw new TException(MSG_RECONNECTION_FAIL);
  }

  @Override
  public TSStatus recordPipeMessage(TRecordPipeMessageReq req) throws TException {
    for (int i = 0; i < RETRY_NUM; i++) {
      try {
        TSStatus status = client.recordPipeMessage(req);
        if (!updateConfigNodeLeader(status)) {
          return status;
        }
      } catch (TException e) {
        configLeader = null;
      }
      waitAndReconnect();
    }
    throw new TException(MSG_RECONNECTION_FAIL);
  }

  @Override
  public TGetRegionIdResp getRegionId(TGetRegionIdReq req) throws TException {
    for (int i = 0; i < RETRY_NUM; i++) {
      try {
        TGetRegionIdResp resp = client.getRegionId(req);
        if (!updateConfigNodeLeader(resp.getStatus())) {
          return resp;
        }
      } catch (TException e) {
        logger.warn(
            "Failed to connect to ConfigNode {} from DataNode {} when executing {}",
            configNode,
            config.getAddressAndPort(),
            Thread.currentThread().getStackTrace()[1].getMethodName());
        configLeader = null;
      }
      waitAndReconnect();
    }
    throw new TException(MSG_RECONNECTION_FAIL);
  }

  @Override
  public TGetTimeSlotListResp getTimeSlotList(TGetTimeSlotListReq req) throws TException {
    for (int i = 0; i < RETRY_NUM; i++) {
      try {
        TGetTimeSlotListResp resp = client.getTimeSlotList(req);
        if (!updateConfigNodeLeader(resp.getStatus())) {
          return resp;
        }
      } catch (TException e) {
        logger.warn(
            "Failed to connect to ConfigNode {} from DataNode {} when executing {}",
            configNode,
            config.getAddressAndPort(),
            Thread.currentThread().getStackTrace()[1].getMethodName());
        configLeader = null;
      }
      waitAndReconnect();
    }
    throw new TException(MSG_RECONNECTION_FAIL);
  }

  @Override
  public TGetSeriesSlotListResp getSeriesSlotList(TGetSeriesSlotListReq req) throws TException {
    for (int i = 0; i < RETRY_NUM; i++) {
      try {
        TGetSeriesSlotListResp resp = client.getSeriesSlotList(req);
        if (!updateConfigNodeLeader(resp.getStatus())) {
          return resp;
        }
      } catch (TException e) {
        logger.warn(
            "Failed to connect to ConfigNode {} from DataNode {} when executing {}",
            configNode,
            config.getAddressAndPort(),
            Thread.currentThread().getStackTrace()[1].getMethodName());
        configLeader = null;
      }
      waitAndReconnect();
    }
    throw new TException(MSG_RECONNECTION_FAIL);
  }

  @Override
  public TSStatus migrateRegion(TMigrateRegionReq req) throws TException {
    for (int i = 0; i < RETRY_NUM; i++) {
      try {
        TSStatus status = client.migrateRegion(req);
        if (!updateConfigNodeLeader(status)) {
          return status;
        }
      } catch (TException e) {
        logger.warn(
            "Failed to connect to ConfigNode {} from DataNode {} when executing {}",
            configNode,
            config.getAddressAndPort(),
            Thread.currentThread().getStackTrace()[1].getMethodName());
        configLeader = null;
      }
      waitAndReconnect();
    }
    throw new TException(MSG_RECONNECTION_FAIL);
  }

  @Override
  public TSStatus createCQ(TCreateCQReq req) throws TException {
    for (int i = 0; i < RETRY_NUM; i++) {
      try {
        TSStatus status = client.createCQ(req);
        if (!updateConfigNodeLeader(status)) {
          return status;
        }
      } catch (TException e) {
        configLeader = null;
      }
      waitAndReconnect();
    }
    throw new TException(MSG_RECONNECTION_FAIL);
  }

  @Override
  public TSStatus dropCQ(TDropCQReq req) throws TException {
    for (int i = 0; i < RETRY_NUM; i++) {
      try {
        TSStatus status = client.dropCQ(req);
        if (!updateConfigNodeLeader(status)) {
          return status;
        }
      } catch (TException e) {
        configLeader = null;
      }
      waitAndReconnect();
    }
    throw new TException(MSG_RECONNECTION_FAIL);
  }

  @Override
  public TShowCQResp showCQ() throws TException {
    for (int i = 0; i < RETRY_NUM; i++) {
      try {
        TShowCQResp resp = client.showCQ();
        if (!updateConfigNodeLeader(resp.getStatus())) {
          return resp;
        }
      } catch (TException e) {
        configLeader = null;
      }
      waitAndReconnect();
    }
    throw new TException(MSG_RECONNECTION_FAIL);
  }

  @Override
  public TSStatus createModel(TCreateModelReq req) throws TException {
    // TODO
    throw new TException(new UnsupportedOperationException().getCause());
  }

  @Override
  public TSStatus dropModel(TDropModelReq req) throws TException {
    // TODO
    throw new TException(new UnsupportedOperationException().getCause());
  }

  @Override
  public TShowModelResp showModel(TShowModelReq req) throws TException {
    // TODO
    throw new TException(new UnsupportedOperationException().getCause());
  }

  @Override
  public TShowTrailResp showTrail(TShowTrailReq req) throws TException {
    // TODO
    throw new TException(new UnsupportedOperationException().getCause());
  }

  @Override
  public TSStatus updateModelInfo(TUpdateModelInfoReq req) throws TException {
    // TODO
    throw new TException(new UnsupportedOperationException().getCause());
  }

  @Override
  public TSStatus updateModelState(TUpdateModelStateReq req) throws TException {
    // TODO
    throw new TException(new UnsupportedOperationException().getCause());
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
