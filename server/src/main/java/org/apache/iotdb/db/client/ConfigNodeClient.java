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

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.confignode.rpc.thrift.ConfigIService;
import org.apache.iotdb.confignode.rpc.thrift.TAuthorizerReq;
import org.apache.iotdb.confignode.rpc.thrift.TAuthorizerResp;
import org.apache.iotdb.confignode.rpc.thrift.TCheckUserPrivilegesReq;
import org.apache.iotdb.confignode.rpc.thrift.TConfigNodeLocation;
import org.apache.iotdb.confignode.rpc.thrift.TConfigNodeRegisterReq;
import org.apache.iotdb.confignode.rpc.thrift.TConfigNodeRegisterResp;
import org.apache.iotdb.confignode.rpc.thrift.TCountStorageGroupResp;
import org.apache.iotdb.confignode.rpc.thrift.TDataNodeLocationResp;
import org.apache.iotdb.confignode.rpc.thrift.TDataNodeRegisterReq;
import org.apache.iotdb.confignode.rpc.thrift.TDataNodeRegisterResp;
import org.apache.iotdb.confignode.rpc.thrift.TDataPartitionReq;
import org.apache.iotdb.confignode.rpc.thrift.TDataPartitionResp;
import org.apache.iotdb.confignode.rpc.thrift.TDeleteStorageGroupReq;
import org.apache.iotdb.confignode.rpc.thrift.TLoginReq;
import org.apache.iotdb.confignode.rpc.thrift.TSchemaPartitionReq;
import org.apache.iotdb.confignode.rpc.thrift.TSchemaPartitionResp;
import org.apache.iotdb.confignode.rpc.thrift.TSetDataReplicationFactorReq;
import org.apache.iotdb.confignode.rpc.thrift.TSetSchemaReplicationFactorReq;
import org.apache.iotdb.confignode.rpc.thrift.TSetStorageGroupReq;
import org.apache.iotdb.confignode.rpc.thrift.TSetTTLReq;
import org.apache.iotdb.confignode.rpc.thrift.TSetTimePartitionIntervalReq;
import org.apache.iotdb.confignode.rpc.thrift.TStorageGroupSchemaResp;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.rpc.RpcTransportFactory;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class ConfigNodeClient implements ConfigIService.Iface {
  private static final Logger logger = LoggerFactory.getLogger(ConfigNodeClient.class);

  private static final int TIMEOUT_MS = 10000;

  private static final int RETRY_NUM = 5;

  public static final String MSG_RECONNECTION_FAIL =
      "Fail to connect to any config node. Please check server it";

  private ConfigIService.Iface client;

  private TTransport transport;

  private TEndPoint configLeader;

  private List<TEndPoint> configNodes;

  private int cursor = 0;

  public ConfigNodeClient() throws TException {
    // Read config nodes from configuration
    configNodes = IoTDBDescriptor.getInstance().getConfig().getConfigNodeList();
    init();
  }

  public ConfigNodeClient(List<TEndPoint> configNodes) throws TException {
    this.configNodes = configNodes;
    init();
  }

  public ConfigNodeClient(List<TEndPoint> configNodes, TEndPoint configLeader) throws TException {
    this.configNodes = configNodes;
    this.configLeader = configLeader;
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
              endpoint.getIp(), endpoint.getPort(), TIMEOUT_MS);
      transport.open();
    } catch (TTransportException e) {
      throw new TException(e);
    }

    if (IoTDBDescriptor.getInstance().getConfig().isRpcThriftCompressionEnable()) {
      client = new ConfigIService.Client(new TCompactProtocol(transport));
    } else {
      client = new ConfigIService.Client(new TBinaryProtocol(transport));
    }
  }

  private void reconnect() throws TException {
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

  public void close() {
    transport.close();
  }

  private boolean updateConfigNodeLeader(TSStatus status) {
    if (status.getCode() == TSStatusCode.NEED_REDIRECTION.getStatusCode()) {
      if (status.isSetRedirectNode()) {
        configLeader =
            new TEndPoint(status.getRedirectNode().getIp(), status.getRedirectNode().getPort());
      } else {
        configLeader = null;
      }
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
  public TDataNodeLocationResp getDataNodeLocations(int dataNodeID) throws TException {
    for (int i = 0; i < RETRY_NUM; i++) {
      try {
        TDataNodeLocationResp resp = client.getDataNodeLocations(dataNodeID);
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
  public TSStatus login(TLoginReq req) throws TException {
    for (int i = 0; i < RETRY_NUM; i++) {
      try {
        TSStatus status = client.login(req);
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
  public TSStatus checkUserPrivileges(TCheckUserPrivilegesReq req) throws TException {
    for (int i = 0; i < RETRY_NUM; i++) {
      try {
        TSStatus status = client.checkUserPrivileges(req);
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
  public TConfigNodeRegisterResp registerConfigNode(TConfigNodeRegisterReq req) throws TException {
    for (int i = 0; i < RETRY_NUM; i++) {
      try {
        TConfigNodeRegisterResp resp = client.registerConfigNode(req);
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
  public TSStatus applyConfigNode(TConfigNodeLocation configNodeLocation) throws TException {
    for (int i = 0; i < RETRY_NUM; i++) {
      try {
        TSStatus status = client.applyConfigNode(configNodeLocation);
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
}
