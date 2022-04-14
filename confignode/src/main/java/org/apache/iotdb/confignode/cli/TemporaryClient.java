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
package org.apache.iotdb.confignode.cli;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.cluster.Endpoint;
import org.apache.iotdb.commons.partition.RegionReplicaSet;
import org.apache.iotdb.confignode.persistence.DataNodeInfoPersistence;
import org.apache.iotdb.rpc.RpcTransportFactory;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.service.rpc.thrift.CreateDataRegionReq;
import org.apache.iotdb.service.rpc.thrift.CreateSchemaRegionReq;
import org.apache.iotdb.service.rpc.thrift.ManagementIService;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * TODO: Replace TemporaryCli with ClientPool A temporary client for ConfigNode to send RPC request
 * to DataNode
 */
public class TemporaryClient {

  private static final Logger LOGGER = LoggerFactory.getLogger(TemporaryClient.class);

  private static final int timeOutInMS = 2000;
  private static final int retryWait = 10;
  private static final int retryNum = 3;

  // Map<DataNodeId, ManagementIService.Client>
  private final Map<Integer, ManagementIService.Client> clients;

  private TemporaryClient() {
    this.clients = new HashMap<>();
  }

  public void buildClient(int dataNodeId, Endpoint endpoint) {
    for (int i = 0; i < retryNum; i++) {
      try {
        TTransport transport =
            RpcTransportFactory.INSTANCE.getTransport(
                endpoint.getIp(), endpoint.getPort(), timeOutInMS);
        transport.open();
        clients.put(dataNodeId, new ManagementIService.Client(new TBinaryProtocol(transport)));
        LOGGER.info("Build client to DataNode: {} success", endpoint);
        return;
      } catch (TTransportException e) {
        LOGGER.error(
            "Build client to DataNode: {} failed, {}. Retrying...", endpoint, e.toString());
        try {
          TimeUnit.MILLISECONDS.sleep(retryWait);
        } catch (InterruptedException ignore) {
          // Ignored
        }
      }
    }
    LOGGER.error("Build client to DataNode: {} failed.", endpoint);
  }

  private CreateSchemaRegionReq genCreateSchemaRegionReq(
      String storageGroup, RegionReplicaSet regionReplicaSet) {
    CreateSchemaRegionReq req = new CreateSchemaRegionReq();
    req.setStorageGroup(storageGroup);
    req.setRegionReplicaSet(regionReplicaSet.convertToRPCTRegionReplicaSet());
    return req;
  }

  public void createSchemaRegion(
      int dataNodeId, String storageGroup, RegionReplicaSet regionReplicaSet) {

    if (clients.get(dataNodeId) == null) {
      buildClient(
          dataNodeId,
          DataNodeInfoPersistence.getInstance().getOnlineDataNode(dataNodeId).getEndPoint());
    }

    CreateSchemaRegionReq req = genCreateSchemaRegionReq(storageGroup, regionReplicaSet);
    if (clients.get(dataNodeId) != null) {
      for (int i = 0; i < retryNum; i++) {
        try {
          TSStatus status = clients.get(dataNodeId).createSchemaRegion(req);
          if (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
            LOGGER.info(
                "Create SchemaRegion on DataNode: {} success",
                DataNodeInfoPersistence.getInstance().getOnlineDataNode(dataNodeId).getEndPoint());
            return;
          } else {
            LOGGER.error(
                "Create SchemaRegion on DataNode: {} failed, {}. Retrying...",
                DataNodeInfoPersistence.getInstance().getOnlineDataNode(dataNodeId).getEndPoint(),
                status);
          }
        } catch (TException e) {
          LOGGER.error(
              "Create SchemaRegion on DataNode: {} failed, {}. Retrying...",
              DataNodeInfoPersistence.getInstance().getOnlineDataNode(dataNodeId).getEndPoint(),
              e.toString());
          try {
            TimeUnit.MILLISECONDS.sleep(retryWait);
          } catch (InterruptedException ignore) {
            // Ignored
          }
        }
      }
    }
    LOGGER.error(
        "Create SchemaRegion on DataNode: {} failed.",
        DataNodeInfoPersistence.getInstance().getOnlineDataNode(dataNodeId).getEndPoint());
  }

  private CreateDataRegionReq genCreateDataRegionReq(
      String storageGroup, RegionReplicaSet regionReplicaSet, long TTL) {
    CreateDataRegionReq req = new CreateDataRegionReq();
    req.setStorageGroup(storageGroup);
    req.setRegionReplicaSet(regionReplicaSet.convertToRPCTRegionReplicaSet());
    req.setTtl(TTL);
    return req;
  }

  public void createDataRegion(
      int dataNodeId, String storageGroup, RegionReplicaSet regionReplicaSet, long TTL) {

    if (clients.get(dataNodeId) == null) {
      buildClient(
          dataNodeId,
          DataNodeInfoPersistence.getInstance().getOnlineDataNode(dataNodeId).getEndPoint());
    }

    CreateDataRegionReq req = genCreateDataRegionReq(storageGroup, regionReplicaSet, TTL);
    if (clients.get(dataNodeId) != null) {
      for (int i = 0; i < retryNum; i++) {
        try {
          TSStatus status = clients.get(dataNodeId).createDataRegion(req);
          if (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
            LOGGER.info(
                "Create DataRegion on DataNode: {} success",
                DataNodeInfoPersistence.getInstance().getOnlineDataNode(dataNodeId).getEndPoint());
            return;
          } else {
            LOGGER.error(
                "Create DataRegion on DataNode: {} failed, {}. Retrying...",
                DataNodeInfoPersistence.getInstance().getOnlineDataNode(dataNodeId).getEndPoint(),
                status);
          }
        } catch (TException e) {
          LOGGER.error(
              "Create DataRegion on DataNode: {} failed, {}. Retrying...",
              DataNodeInfoPersistence.getInstance().getOnlineDataNode(dataNodeId).getEndPoint(),
              e.toString());
          try {
            TimeUnit.MILLISECONDS.sleep(retryWait);
          } catch (InterruptedException ignore) {
            // Ignored
          }
        }
      }
    }
    LOGGER.error(
        "Create DataRegion on DataNode: {} failed.",
        DataNodeInfoPersistence.getInstance().getOnlineDataNode(dataNodeId).getEndPoint());
  }

  private static class TemporaryClientHolder {

    private static final TemporaryClient INSTANCE = new TemporaryClient();

    private TemporaryClientHolder() {
      // Empty constructor
    }
  }

  public static TemporaryClient getInstance() {
    return TemporaryClientHolder.INSTANCE;
  }
}
