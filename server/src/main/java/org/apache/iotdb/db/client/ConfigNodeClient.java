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
import org.apache.iotdb.commons.exception.BadNodeUrlException;
import org.apache.iotdb.commons.utils.CommonUtils;
import org.apache.iotdb.confignode.rpc.thrift.ConfigIService;
import org.apache.iotdb.confignode.rpc.thrift.TAuthorizerReq;
import org.apache.iotdb.confignode.rpc.thrift.TAuthorizerResp;
import org.apache.iotdb.confignode.rpc.thrift.TCheckUserPrivilegesReq;
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
import org.apache.iotdb.confignode.rpc.thrift.TSetStorageGroupReq;
import org.apache.iotdb.confignode.rpc.thrift.TStorageGroupSchemaResp;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.rpc.IoTDBConnectionException;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TCompactProtocol;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class ConfigNodeClient extends ConsensusClient {
  private static final Logger logger = LoggerFactory.getLogger(ConfigNodeClient.class);

  private static final int RETRY_NUM = 5;

  public static final String MSG_RECONNECTION_FAIL =
      "Fail to connect to any config node. Please check server it";

  private ConfigIService.Iface client;

  public ConfigNodeClient() throws BadNodeUrlException, IoTDBConnectionException {
    // Read config nodes from configuration
    super(CommonUtils.parseNodeUrls(IoTDBDescriptor.getInstance().getConfig().getConfigNodeUrls()));
    init();
  }

  public ConfigNodeClient(List<TEndPoint> configNodes) throws IoTDBConnectionException {
    super(configNodes);
    init();
  }

  public void init() throws IoTDBConnectionException {
    reconnect();
  }

  @Override
  protected void reconnect() throws IoTDBConnectionException {
    super.reconnect();
    if (IoTDBDescriptor.getInstance().getConfig().isRpcThriftCompressionEnable()) {
      client = new ConfigIService.Client(new TCompactProtocol(transport));
    } else {
      client = new ConfigIService.Client(new TBinaryProtocol(transport));
    }
  }

  public TDataNodeRegisterResp registerDataNode(TDataNodeRegisterReq req)
      throws IoTDBConnectionException {
    for (int i = 0; i < RETRY_NUM; i++) {
      try {
        TDataNodeRegisterResp resp = client.registerDataNode(req);
        if (!processResponse(resp.status)) {
          return resp;
        }
        logger.info("Register current node using request {} with response {}", req, resp);
      } catch (TException e) {
        logger.warn("Can not connect to leader {}", consensusLeader);
        consensusLeader = null;
      }
      reconnect();
    }
    throw new IoTDBConnectionException(MSG_RECONNECTION_FAIL);
  }

  public TDataNodeLocationResp getDataNodeLocations(int dataNodeID)
      throws IoTDBConnectionException {
    for (int i = 0; i < RETRY_NUM; i++) {
      try {
        TDataNodeLocationResp resp = client.getDataNodeLocations(dataNodeID);
        if (!processResponse(resp.status)) {
          return resp;
        }
      } catch (TException e) {
        logger.warn("Can not connect to leader {}", consensusLeader);
        consensusLeader = null;
      }
      reconnect();
    }
    throw new IoTDBConnectionException(MSG_RECONNECTION_FAIL);
  }

  public TSStatus setStorageGroup(TSetStorageGroupReq req) throws IoTDBConnectionException {
    for (int i = 0; i < RETRY_NUM; i++) {
      try {
        TSStatus status = client.setStorageGroup(req);
        if (!processResponse(status)) {
          return status;
        }
      } catch (TException e) {
        logger.warn("Can not connect to leader {}", consensusLeader);
        consensusLeader = null;
      }
      reconnect();
    }
    throw new IoTDBConnectionException(MSG_RECONNECTION_FAIL);
  }

  public TSStatus deleteStorageGroup(TDeleteStorageGroupReq req) throws IoTDBConnectionException {
    for (int i = 0; i < RETRY_NUM; i++) {
      try {
        TSStatus status = client.deleteStorageGroup(req);
        if (!processResponse(status)) {
          return status;
        }
      } catch (TException e) {
        logger.warn("Can not connect to leader {}", consensusLeader);
        consensusLeader = null;
      }
      reconnect();
    }
    throw new IoTDBConnectionException(MSG_RECONNECTION_FAIL);
  }

  public TCountStorageGroupResp countMatchedStorageGroups(List<String> storageGroupPathPattern)
      throws IoTDBConnectionException {
    for (int i = 0; i < RETRY_NUM; i++) {
      try {
        TCountStorageGroupResp resp = client.countMatchedStorageGroups(storageGroupPathPattern);
        if (!processResponse(resp.status)) {
          return resp;
        }
      } catch (TException e) {
        logger.warn("Can not connect to leader {}", consensusLeader);
        consensusLeader = null;
      }
      reconnect();
    }
    throw new IoTDBConnectionException(MSG_RECONNECTION_FAIL);
  }

  public TStorageGroupSchemaResp getMatchedStorageGroupSchemas(List<String> storageGroupPathPattern)
      throws IoTDBConnectionException {
    for (int i = 0; i < RETRY_NUM; i++) {
      try {
        TStorageGroupSchemaResp resp =
            client.getMatchedStorageGroupSchemas(storageGroupPathPattern);
        if (!processResponse(resp.status)) {
          return resp;
        }
      } catch (TException e) {
        logger.warn("Can not connect to leader {}", consensusLeader);
        consensusLeader = null;
      }
      reconnect();
    }
    throw new IoTDBConnectionException(MSG_RECONNECTION_FAIL);
  }

  public TSchemaPartitionResp getSchemaPartition(TSchemaPartitionReq req)
      throws IoTDBConnectionException {
    for (int i = 0; i < RETRY_NUM; i++) {
      try {
        TSchemaPartitionResp resp = client.getSchemaPartition(req);
        if (!processResponse(resp.status)) {
          return resp;
        }
      } catch (TException e) {
        logger.warn("Can not connect to leader {}", consensusLeader);
        consensusLeader = null;
      }
      reconnect();
    }
    throw new IoTDBConnectionException(MSG_RECONNECTION_FAIL);
  }

  public TSchemaPartitionResp getOrCreateSchemaPartition(TSchemaPartitionReq req)
      throws IoTDBConnectionException {
    for (int i = 0; i < RETRY_NUM; i++) {
      try {
        TSchemaPartitionResp resp = client.getOrCreateSchemaPartition(req);
        if (!processResponse(resp.status)) {
          return resp;
        }
      } catch (TException e) {
        logger.warn("Can not connect to leader {}", consensusLeader);
        consensusLeader = null;
      }
      reconnect();
    }
    throw new IoTDBConnectionException(MSG_RECONNECTION_FAIL);
  }

  public TDataPartitionResp getDataPartition(TDataPartitionReq req)
      throws IoTDBConnectionException {
    for (int i = 0; i < RETRY_NUM; i++) {
      try {
        TDataPartitionResp resp = client.getDataPartition(req);
        if (!processResponse(resp.status)) {
          return resp;
        }
      } catch (TException e) {
        logger.warn("Can not connect to leader {}", consensusLeader);
        consensusLeader = null;
      }
      reconnect();
    }
    throw new IoTDBConnectionException(MSG_RECONNECTION_FAIL);
  }

  public TDataPartitionResp getOrCreateDataPartition(TDataPartitionReq req)
      throws IoTDBConnectionException {
    for (int i = 0; i < RETRY_NUM; i++) {
      try {
        TDataPartitionResp resp = client.getOrCreateDataPartition(req);
        if (!processResponse(resp.status)) {
          return resp;
        }
      } catch (TException e) {
        logger.warn("Can not connect to leader {}", consensusLeader);
        consensusLeader = null;
      }
      reconnect();
    }
    throw new IoTDBConnectionException(MSG_RECONNECTION_FAIL);
  }

  public TSStatus operatePermission(TAuthorizerReq req) throws IoTDBConnectionException {
    for (int i = 0; i < RETRY_NUM; i++) {
      try {
        TSStatus status = client.operatePermission(req);
        if (!processResponse(status)) {
          return status;
        }
      } catch (TException e) {
        logger.warn("Can not connect to leader {}", consensusLeader);
        consensusLeader = null;
      }
      reconnect();
    }
    throw new IoTDBConnectionException(MSG_RECONNECTION_FAIL);
  }

  public TAuthorizerResp queryPermission(TAuthorizerReq req) throws IoTDBConnectionException {
    for (int i = 0; i < RETRY_NUM; i++) {
      try {
        TAuthorizerResp resp = client.queryPermission(req);
        if (!processResponse(resp.status)) {
          return resp;
        }
      } catch (TException e) {
        logger.warn("Can not connect to leader {}", consensusLeader);
        consensusLeader = null;
      }
      reconnect();
    }
    throw new IoTDBConnectionException(MSG_RECONNECTION_FAIL);
  }

  public TSStatus login(TLoginReq req) throws IoTDBConnectionException {
    for (int i = 0; i < RETRY_NUM; i++) {
      try {
        TSStatus status = client.login(req);
        if (!processResponse(status)) {
          return status;
        }
      } catch (TException e) {
        logger.warn("Can not connect to leader {}", consensusLeader);
        consensusLeader = null;
      }
      reconnect();
    }
    throw new IoTDBConnectionException(MSG_RECONNECTION_FAIL);
  }

  public TSStatus checkUserPrivileges(TCheckUserPrivilegesReq req) throws IoTDBConnectionException {
    for (int i = 0; i < RETRY_NUM; i++) {
      try {
        TSStatus status = client.checkUserPrivileges(req);
        if (!processResponse(status)) {
          return status;
        }
      } catch (TException e) {
        logger.warn("Can not connect to leader {}", consensusLeader);
        consensusLeader = null;
      }
      reconnect();
    }
    throw new IoTDBConnectionException(MSG_RECONNECTION_FAIL);
  }
}
