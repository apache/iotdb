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

import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

public class TwoStageAggregateSender implements AutoCloseable {

  private static final Logger LOGGER = LoggerFactory.getLogger(TwoStageAggregateSender.class);

  private static final PipeConfig PIPE_CONFIG = PipeConfig.getInstance();

  // TODO: Update the endpoints periodically to handle node changes
  private static final AtomicReference<TEndPoint[]> END_POINTS = new AtomicReference<>();

  private TEndPoint[] endPoints;
  private final Map<TEndPoint, IoTDBSyncClient> endPointIoTDBSyncClientMap =
      new ConcurrentHashMap<>();

  public synchronized TPipeTransferResp request(long watermark, TPipeTransferReq req)
      throws TException {
    tryFetchEndPointsIfNecessary();
    tryConstructClients();

    final TEndPoint endPoint = endPoints[(int) watermark % endPoints.length];
    IoTDBSyncClient client = endPointIoTDBSyncClientMap.get(endPoint);
    if (client == null) {
      client = reconstructIoTDBSyncClient(endPoint);
    }

    try {
      return client.pipeTransfer(req);
    } catch (Exception e) {
      LOGGER.warn("Failed to send request {} (watermark = {}) to {}", req, watermark, endPoint, e);
      try {
        reconstructIoTDBSyncClient(endPoint);
      } catch (Exception ex) {
        LOGGER.warn(
            "Failed to reconstruct IoTDBSyncClient {} after failure to send request {} (watermark = {})",
            endPoint,
            req,
            watermark,
            ex);
      }
      throw e;
    }
  }

  private void tryFetchEndPointsIfNecessary() {
    if (END_POINTS.get() != null) {
      return;
    }

    synchronized (END_POINTS) {
      if (END_POINTS.get() != null) {
        return;
      }

      final Set<TEndPoint> endPointSet = new HashSet<>();
      try (final ConfigNodeClient configNodeClient =
          ConfigNodeClientManager.getInstance().borrowClient(ConfigNodeInfo.CONFIG_REGION_ID)) {
        // TODO: We assume that different data nodes will get the same data nodes' endpoints
        final TShowDataNodesResp showDataNodesResp = configNodeClient.showDataNodes();
        if (showDataNodesResp == null || showDataNodesResp.getDataNodesInfoList() == null) {
          throw new PipeException("Failed to fetch data nodes");
        }
        for (final TDataNodeInfo dataNodeInfo : showDataNodesResp.getDataNodesInfoList()) {
          endPointSet.add(new TEndPoint(dataNodeInfo.getRpcAddresss(), dataNodeInfo.getRpcPort()));
        }
      } catch (ClientManagerException | TException e) {
        throw new PipeException("Failed to fetch data nodes", e);
      }

      if (endPointSet.isEmpty()) {
        throw new PipeException("No data nodes' endpoints fetched");
      }

      END_POINTS.set(endPointSet.toArray(new TEndPoint[0]));
    }

    LOGGER.info(
        "Data nodes' endpoints for two-stage aggregation: {}", Arrays.toString(END_POINTS.get()));
  }

  private void tryConstructClients() {
    if (Objects.isNull(endPoints)) {
      endPoints = END_POINTS.get();
    }

    if (endPointIoTDBSyncClientMap.isEmpty()) {
      for (final TEndPoint endPoint : endPoints) {
        try {
          endPointIoTDBSyncClientMap.put(endPoint, constructIoTDBSyncClient(endPoint));
        } catch (TTransportException e) {
          LOGGER.warn("Failed to construct IoTDBSyncClient", e);
        }
      }
    }
  }

  private IoTDBSyncClient reconstructIoTDBSyncClient(TEndPoint endPoint)
      throws TTransportException {
    final IoTDBSyncClient oldClient = endPointIoTDBSyncClientMap.remove(endPoint);
    if (oldClient != null) {
      try {
        oldClient.close();
      } catch (Exception e) {
        LOGGER.warn("Failed to close old IoTDBSyncClient", e);
      }
    }
    final IoTDBSyncClient newClient = constructIoTDBSyncClient(endPoint);
    endPointIoTDBSyncClientMap.put(endPoint, newClient);
    return newClient;
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
    for (final IoTDBSyncClient client : endPointIoTDBSyncClientMap.values()) {
      try {
        client.close();
      } catch (Exception e) {
        LOGGER.warn("Failed to close IoTDBSyncClient", e);
      }
    }

    endPointIoTDBSyncClientMap.clear();
    endPoints = null;
  }
}
