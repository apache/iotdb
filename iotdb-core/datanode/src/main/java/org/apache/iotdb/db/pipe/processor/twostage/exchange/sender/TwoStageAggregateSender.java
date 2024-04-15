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

package org.apache.iotdb.db.pipe.processor.twostage.exchange.sender;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.commons.client.exception.ClientManagerException;
import org.apache.iotdb.commons.client.property.ThriftClientProperty;
import org.apache.iotdb.commons.pipe.config.PipeConfig;
import org.apache.iotdb.commons.pipe.connector.client.IoTDBSyncClient;
import org.apache.iotdb.confignode.rpc.thrift.TDataNodeInfo;
import org.apache.iotdb.confignode.rpc.thrift.TShowDataNodesResp;
import org.apache.iotdb.db.protocol.client.ConfigNodeClient;
import org.apache.iotdb.db.protocol.client.ConfigNodeClientManager;
import org.apache.iotdb.db.protocol.client.ConfigNodeInfo;
import org.apache.iotdb.pipe.api.exception.PipeException;
import org.apache.iotdb.service.rpc.thrift.TPipeTransferReq;
import org.apache.iotdb.service.rpc.thrift.TPipeTransferResp;

import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class TwoStageAggregateSender implements AutoCloseable {

  private static final Logger LOGGER = LoggerFactory.getLogger(TwoStageAggregateSender.class);

  private static final PipeConfig PIPE_CONFIG = PipeConfig.getInstance();

  // TODO: Update the map periodically
  private static final Map<TEndPoint, IoTDBSyncClient> ENDPOINT_SYNC_CLIENT_MAP =
      new ConcurrentHashMap<>();
  private static final AtomicInteger REFERENCE_COUNT = new AtomicInteger(0);

  // TODO: Update the end points periodically
  private final TEndPoint[] endPoints;

  public TwoStageAggregateSender() {
    synchronized (ENDPOINT_SYNC_CLIENT_MAP) {
      REFERENCE_COUNT.incrementAndGet();

      if (ENDPOINT_SYNC_CLIENT_MAP.isEmpty()) {
        try (final ConfigNodeClient configNodeClient =
            ConfigNodeClientManager.getInstance().borrowClient(ConfigNodeInfo.CONFIG_REGION_ID)) {
          final TShowDataNodesResp showDataNodesResp = configNodeClient.showDataNodes();
          if (showDataNodesResp == null || showDataNodesResp.getDataNodesInfoList() == null) {
            throw new PipeException("Failed to fetch data nodes");
          }
          // TODO: We assume that different data nodes will get the same data nodes' endpoints for
          // now. Finally, we have to periodically update the data nodes' endpoints.
          for (final TDataNodeInfo dataNodeInfo : showDataNodesResp.getDataNodesInfoList()) {
            final TEndPoint endPoint =
                new TEndPoint(dataNodeInfo.getRpcAddresss(), dataNodeInfo.getRpcPort());
            ENDPOINT_SYNC_CLIENT_MAP.put(endPoint, constructIoTDBSyncClient(endPoint));
          }
        } catch (ClientManagerException | TException e) {
          throw new PipeException("Failed to fetch data nodes", e);
        }

        LOGGER.info(
            "Data nodes' endpoints for two-stage aggregation: {}",
            ENDPOINT_SYNC_CLIENT_MAP.keySet());
      }

      endPoints = ENDPOINT_SYNC_CLIENT_MAP.keySet().stream().sorted().toArray(TEndPoint[]::new);
    }
  }

  public TPipeTransferResp request(long watermark, TPipeTransferReq req) throws TException {
    final TEndPoint endPoint = endPoints[(int) watermark % endPoints.length];
    IoTDBSyncClient client = ENDPOINT_SYNC_CLIENT_MAP.get(endPoint);
    if (client == null) {
      client = reconstructIoTDBSyncClient(endPoint);
    }
    try {
      return client.pipeTransfer(req);
    } catch (Exception e) {
      reconstructIoTDBSyncClient(endPoint);
      throw e;
    }
  }

  private IoTDBSyncClient reconstructIoTDBSyncClient(TEndPoint endPoint)
      throws TTransportException {
    synchronized (ENDPOINT_SYNC_CLIENT_MAP) {
      final IoTDBSyncClient oldClient = ENDPOINT_SYNC_CLIENT_MAP.remove(endPoint);
      if (oldClient != null) {
        try {
          oldClient.close();
        } catch (Exception e) {
          LOGGER.warn("Failed to close old IoTDBSyncClient", e);
        }
      }
      final IoTDBSyncClient newClient = constructIoTDBSyncClient(endPoint);
      ENDPOINT_SYNC_CLIENT_MAP.put(endPoint, newClient);
      return newClient;
    }
  }

  private IoTDBSyncClient constructIoTDBSyncClient(TEndPoint endPoint) throws TTransportException {
    return new IoTDBSyncClient(
        new ThriftClientProperty.Builder()
            .setConnectionTimeoutMs((int) PIPE_CONFIG.getPipeConnectorHandshakeTimeoutMs())
            .setRpcThriftCompressionEnabled(
                PIPE_CONFIG.isPipeConnectorRPCThriftCompressionEnabled())
            .build(),
        endPoint.getIp(),
        endPoint.getPort(),
        false,
        null,
        null);
  }

  @Override
  public void close() {
    synchronized (ENDPOINT_SYNC_CLIENT_MAP) {
      if (REFERENCE_COUNT.decrementAndGet() > 0) {
        return;
      }

      for (final IoTDBSyncClient client : ENDPOINT_SYNC_CLIENT_MAP.values()) {
        try {
          client.close();
        } catch (Exception e) {
          LOGGER.warn("Failed to close IoTDBSyncClient", e);
        }
      }

      ENDPOINT_SYNC_CLIENT_MAP.clear();
    }
  }
}
