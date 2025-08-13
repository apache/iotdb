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
import org.apache.iotdb.commons.pipe.sink.client.IoTDBSyncClient;
import org.apache.iotdb.confignode.rpc.thrift.TDataNodeInfo;
import org.apache.iotdb.confignode.rpc.thrift.TShowDataNodesResp;
import org.apache.iotdb.db.pipe.processor.twostage.combiner.PipeCombineHandlerManager;
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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

public class TwoStageAggregateSender implements AutoCloseable {

  private static final Logger LOGGER = LoggerFactory.getLogger(TwoStageAggregateSender.class);

  private static final PipeConfig PIPE_CONFIG = PipeConfig.getInstance();

  private final String pipeName;
  private final long creationTime;

  private static final AtomicLong DATANODE_ID_2_END_POINTS_LAST_UPDATE_TIME = new AtomicLong(0);
  private static final AtomicReference<Map<Integer, TEndPoint>> DATANODE_ID_2_END_POINTS =
      new AtomicReference<>();

  private TEndPoint[] endPoints;
  private final Map<TEndPoint, IoTDBSyncClient> endPointIoTDBSyncClientMap =
      new ConcurrentHashMap<>();

  public TwoStageAggregateSender(String pipeName, long creationTime) {
    this.pipeName = pipeName;
    this.creationTime = creationTime;
  }

  public synchronized TPipeTransferResp request(long watermark, TPipeTransferReq req)
      throws TException {
    final boolean endPointsChanged = tryFetchEndPointsIfNecessary();
    tryConstructClients(endPointsChanged);

    final TEndPoint endPoint = endPoints[(int) watermark % endPoints.length];
    IoTDBSyncClient client = endPointIoTDBSyncClientMap.get(endPoint);
    if (client == null) {
      client = reconstructIoTDBSyncClient(endPoint);
    }

    LOGGER.info("Sending request {} (watermark = {}) to {}", req, watermark, endPoint);

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

  private static boolean tryFetchEndPointsIfNecessary() {
    final long currentTime = System.currentTimeMillis();

    if (DATANODE_ID_2_END_POINTS.get() != null
        && currentTime - DATANODE_ID_2_END_POINTS_LAST_UPDATE_TIME.get()
            < PIPE_CONFIG.getTwoStageAggregateSenderEndPointsCacheInMs()) {
      return false;
    }

    synchronized (DATANODE_ID_2_END_POINTS) {
      if (DATANODE_ID_2_END_POINTS.get() != null
          && currentTime - DATANODE_ID_2_END_POINTS_LAST_UPDATE_TIME.get()
              < PIPE_CONFIG.getTwoStageAggregateSenderEndPointsCacheInMs()) {
        return false;
      }

      final Map<Integer, TEndPoint> dataNodeId2EndPointMap = new HashMap<>();
      try (final ConfigNodeClient configNodeClient =
          ConfigNodeClientManager.getInstance().borrowClient(ConfigNodeInfo.CONFIG_REGION_ID)) {
        final TShowDataNodesResp showDataNodesResp = configNodeClient.showDataNodes();
        if (showDataNodesResp == null || showDataNodesResp.getDataNodesInfoList() == null) {
          throw new PipeException("Failed to fetch data nodes");
        }
        for (final TDataNodeInfo dataNodeInfo : showDataNodesResp.getDataNodesInfoList()) {
          dataNodeId2EndPointMap.put(
              dataNodeInfo.getDataNodeId(),
              new TEndPoint(dataNodeInfo.getRpcAddresss(), dataNodeInfo.getRpcPort()));
        }
      } catch (ClientManagerException | TException e) {
        throw new PipeException("Failed to fetch data nodes", e);
      }

      if (dataNodeId2EndPointMap.isEmpty()) {
        throw new PipeException("No data nodes' endpoints fetched");
      }

      DATANODE_ID_2_END_POINTS.set(dataNodeId2EndPointMap);
      DATANODE_ID_2_END_POINTS_LAST_UPDATE_TIME.set(currentTime);
    }

    LOGGER.info("Data nodes' endpoints for two-stage aggregation: {}", DATANODE_ID_2_END_POINTS);
    return true;
  }

  private void tryConstructClients(boolean endPointsChanged) {
    if (Objects.nonNull(endPoints) && !endPointsChanged) {
      return;
    }

    final Set<Integer> expectedDataNodeIdSet =
        PipeCombineHandlerManager.getInstance().getExpectedDataNodeIdSet(pipeName, creationTime);
    if (expectedDataNodeIdSet.isEmpty()) {
      throw new PipeException("No expected region id set fetched");
    }

    endPoints =
        DATANODE_ID_2_END_POINTS.get().entrySet().stream()
            .filter(entry -> expectedDataNodeIdSet.contains(entry.getKey()))
            .map(Map.Entry::getValue)
            .toArray(TEndPoint[]::new);
    LOGGER.info(
        "End points for two-stage aggregation pipe (pipeName={}, creationTime={}) were updated to {}",
        pipeName,
        creationTime,
        endPoints);

    for (final TEndPoint endPoint : endPoints) {
      if (endPointIoTDBSyncClientMap.containsKey(endPoint)) {
        continue;
      }

      try {
        endPointIoTDBSyncClientMap.put(endPoint, constructIoTDBSyncClient(endPoint));
      } catch (TTransportException e) {
        LOGGER.warn("Failed to construct IoTDBSyncClient", e);
      }
    }

    for (final TEndPoint endPoint : new HashSet<>(endPointIoTDBSyncClientMap.keySet())) {
      if (!DATANODE_ID_2_END_POINTS.get().containsValue(endPoint)) {
        try {
          endPointIoTDBSyncClientMap.remove(endPoint).close();
        } catch (Exception e) {
          LOGGER.warn("Failed to close IoTDBSyncClient", e);
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
            .setConnectionTimeoutMs(PIPE_CONFIG.getPipeConnectorHandshakeTimeoutMs())
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
