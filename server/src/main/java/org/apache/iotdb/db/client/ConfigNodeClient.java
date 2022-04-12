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

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.cluster.Endpoint;
import org.apache.iotdb.commons.exception.BadNodeUrlException;
import org.apache.iotdb.commons.utils.CommonUtils;
import org.apache.iotdb.confignode.rpc.thrift.*;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.RpcTransportFactory;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.service.rpc.thrift.*;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Random;

public class ConfigNodeClient {
  private static final Logger logger = LoggerFactory.getLogger(ConfigNodeClient.class);

  private static final int TIMEOUT_MS = 2000;

  private static final int RETRY_NUM = 5;

  public static final String MSG_RECONNECTION_FAIL =
      "Fail to connect to any config node. Please check server it";

  private ConfigIService.Iface client;

  private TTransport transport;

  private Endpoint configLeader;

  private List<Endpoint> configNodes;

  public ConfigNodeClient() throws BadNodeUrlException, IoTDBConnectionException {
    // Read config nodes from configuration
    configNodes =
        CommonUtils.parseNodeUrls(IoTDBDescriptor.getInstance().getConfig().getConfigNodeUrls());
    init();
  }

  public ConfigNodeClient(List<Endpoint> configNodes) throws IoTDBConnectionException {
    this.configNodes = configNodes;
    init();
  }

  public ConfigNodeClient(List<Endpoint> configNodes, Endpoint configLeader)
      throws IoTDBConnectionException {
    this.configNodes = configNodes;
    this.configLeader = configLeader;
    init();
  }

  public void init() throws IoTDBConnectionException {
    reconnect();
  }

  public void connect(Endpoint endpoint) throws IoTDBConnectionException {
    try {
      transport =
          RpcTransportFactory.INSTANCE.getTransport(
              // as there is a try-catch already, we do not need to use TSocket.wrap
              endpoint.getIp(), endpoint.getPort(), TIMEOUT_MS);
      transport.open();
    } catch (TTransportException e) {
      throw new IoTDBConnectionException(e);
    }

    if (IoTDBDescriptor.getInstance().getConfig().isRpcThriftCompressionEnable()) {
      client = new ConfigIService.Client(new TCompactProtocol(transport));
    } else {
      client = new ConfigIService.Client(new TBinaryProtocol(transport));
    }
  }

  private void reconnect() throws IoTDBConnectionException {
    if (configLeader != null) {
      try {
        connect(configLeader);
        return;
      } catch (IoTDBConnectionException e) {
        logger.warn("The current node may have been down {},try next node", configLeader);
        configLeader = null;
      }
    }

    Random random = new Random();
    if (transport != null) {
      transport.close();
    }
    int currHostIndex = random.nextInt(configNodes.size());
    int tryHostNum = 0;
    for (int j = currHostIndex; j < configNodes.size(); j++) {
      if (tryHostNum == configNodes.size()) {
        break;
      }
      Endpoint tryEndpoint = configNodes.get(j);
      if (j == configNodes.size() - 1) {
        j = -1;
      }
      tryHostNum++;
      try {
        connect(tryEndpoint);
        return;
      } catch (IoTDBConnectionException e) {
        logger.warn("The current node may have been down {},try next node", tryEndpoint);
      }
    }
    throw new IoTDBConnectionException(MSG_RECONNECTION_FAIL);
  }

  public void close() {
    transport.close();
  }

  private boolean verifyNeedRedirect(TSStatus status) throws StatementExecutionException {
    if (status.getCode() == TSStatusCode.NEED_REDIRECTION.getStatusCode()) {
      if (status.isSetRedirectNode()) {
        configLeader =
            new Endpoint(status.getRedirectNode().getIp(), status.getRedirectNode().getPort());
      } else {
        configLeader = null;
      }
      return true;
    }
    if (status.code == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      return false;
    }
    throw new StatementExecutionException(status);
  }

  public TDataNodeRegisterResp registerDataNode(TDataNodeRegisterReq req)
      throws StatementExecutionException, IoTDBConnectionException {
    for (int i = 0; i < RETRY_NUM; i++) {
      try {
        TDataNodeRegisterResp resp = client.registerDataNode(req);
        if (!verifyNeedRedirect(resp.status)) {
          return resp;
        }
      } catch (TException e) {
        configLeader = null;
      }
      reconnect();
    }
    throw new IoTDBConnectionException(MSG_RECONNECTION_FAIL);
  }

  public TDataNodeMessageResp getDataNodesMessage(int dataNodeID)
      throws StatementExecutionException, IoTDBConnectionException {
    for (int i = 0; i < RETRY_NUM; i++) {
      try {
        TDataNodeMessageResp resp = client.getDataNodesMessage(dataNodeID);
        if (!verifyNeedRedirect(resp.status)) {
          return resp;
        }
      } catch (TException e) {
        configLeader = null;
      }
      reconnect();
    }
    throw new IoTDBConnectionException(MSG_RECONNECTION_FAIL);
  }

  public TSStatus setStorageGroup(TSetStorageGroupReq req)
      throws StatementExecutionException, IoTDBConnectionException {
    for (int i = 0; i < RETRY_NUM; i++) {
      try {
        TSStatus status = client.setStorageGroup(req);
        if (!verifyNeedRedirect(status)) {
          return status;
        }
      } catch (TException e) {
        configLeader = null;
      }
      reconnect();
    }
    throw new IoTDBConnectionException(MSG_RECONNECTION_FAIL);
  }

  public TSStatus deleteStorageGroup(TDeleteStorageGroupReq req)
      throws StatementExecutionException, IoTDBConnectionException {
    for (int i = 0; i < RETRY_NUM; i++) {
      try {
        TSStatus status = client.deleteStorageGroup(req);
        if (!verifyNeedRedirect(status)) {
          return status;
        }
      } catch (TException e) {
        configLeader = null;
      }
      reconnect();
    }
    throw new IoTDBConnectionException(MSG_RECONNECTION_FAIL);
  }

  public TStorageGroupMessageResp getStorageGroupsMessage()
      throws StatementExecutionException, IoTDBConnectionException {
    for (int i = 0; i < RETRY_NUM; i++) {
      try {
        TStorageGroupMessageResp resp = client.getStorageGroupsMessage();
        if (!verifyNeedRedirect(resp.status)) {
          return resp;
        }
      } catch (TException e) {
        configLeader = null;
      }
      reconnect();
    }
    throw new IoTDBConnectionException(MSG_RECONNECTION_FAIL);
  }

  public TSchemaPartitionResp getSchemaPartition(TSchemaPartitionReq req)
      throws StatementExecutionException, IoTDBConnectionException {
    for (int i = 0; i < RETRY_NUM; i++) {
      try {
        TSchemaPartitionResp resp = client.getSchemaPartition(req);
        if (!verifyNeedRedirect(resp.status)) {
          return resp;
        }
      } catch (TException e) {
        configLeader = null;
      }
      reconnect();
    }
    throw new IoTDBConnectionException(MSG_RECONNECTION_FAIL);
  }

  public TSchemaPartitionResp getOrCreateSchemaPartition(TSchemaPartitionReq req)
      throws StatementExecutionException, IoTDBConnectionException {
    for (int i = 0; i < RETRY_NUM; i++) {
      try {
        TSchemaPartitionResp resp = client.getOrCreateSchemaPartition(req);
        if (!verifyNeedRedirect(resp.status)) {
          return resp;
        }
      } catch (TException e) {
        configLeader = null;
      }
      reconnect();
    }
    throw new IoTDBConnectionException(MSG_RECONNECTION_FAIL);
  }

  public TDataPartitionResp getDataPartition(TDataPartitionReq req)
      throws StatementExecutionException, IoTDBConnectionException {
    for (int i = 0; i < RETRY_NUM; i++) {
      try {
        TDataPartitionResp resp = client.getDataPartition(req);
        if (!verifyNeedRedirect(resp.status)) {
          return resp;
        }
      } catch (TException e) {
        configLeader = null;
      }
      reconnect();
    }
    throw new IoTDBConnectionException(MSG_RECONNECTION_FAIL);
  }

  public TDataPartitionResp getOrCreateDataPartition(TDataPartitionReq req)
      throws StatementExecutionException, IoTDBConnectionException {
    for (int i = 0; i < RETRY_NUM; i++) {
      try {
        TDataPartitionResp resp = client.getOrCreateDataPartition(req);
        if (!verifyNeedRedirect(resp.status)) {
          return resp;
        }
      } catch (TException e) {
        configLeader = null;
      }
      reconnect();
    }
    throw new IoTDBConnectionException(MSG_RECONNECTION_FAIL);
  }

  public TSStatus operatePermission(TAuthorizerReq req)
      throws StatementExecutionException, IoTDBConnectionException {
    for (int i = 0; i < RETRY_NUM; i++) {
      try {
        TSStatus status = client.operatePermission(req);
        if (!verifyNeedRedirect(status)) {
          return status;
        }
      } catch (TException e) {
        configLeader = null;
      }
      reconnect();
    }
    throw new IoTDBConnectionException(MSG_RECONNECTION_FAIL);
  }
}
