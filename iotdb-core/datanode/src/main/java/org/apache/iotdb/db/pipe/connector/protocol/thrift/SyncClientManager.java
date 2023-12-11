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

package org.apache.iotdb.db.pipe.connector.protocol.thrift;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.commons.client.property.ThriftClientProperty;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.pipe.config.PipeConfig;
import org.apache.iotdb.db.pipe.connector.payload.evolvable.request.PipeTransferHandshakeReq;
import org.apache.iotdb.db.pipe.connector.protocol.thrift.sync.IoTDBThriftSyncConnectorClient;
import org.apache.iotdb.db.pipe.event.common.tablet.PipeInsertNodeTabletInsertionEvent;
import org.apache.iotdb.db.pipe.event.common.tablet.PipeRawTabletInsertionEvent;
import org.apache.iotdb.db.pipe.event.common.tsfile.PipeTsFileInsertionEvent;
import org.apache.iotdb.pipe.api.exception.PipeConnectionException;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.service.rpc.thrift.TPipeTransferResp;
import org.apache.iotdb.tsfile.utils.Pair;

import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class SyncClientManager {
  private static final Logger LOGGER = LoggerFactory.getLogger(SyncClientManager.class);

  private static final PipeConfig PIPE_CONFIG = PipeConfig.getInstance();

  private final List<TEndPoint> endPoints;
  private final boolean useLeaderCache;

  private final Map<TEndPoint, Pair<IoTDBThriftSyncConnectorClient, Boolean>> endPoint2client =
      new ConcurrentHashMap<>();
  private final LeaderCacheManager leaderCacheManager = new LeaderCacheManager();

  private long currentClientIndex = 0;

  public SyncClientManager(List<TEndPoint> endPoints, boolean useLeaderCache) {
    this.endPoints = endPoints;
    this.useLeaderCache = useLeaderCache;

    for (TEndPoint endPoint : endPoints) {
      endPoint2client.putIfAbsent(endPoint, new Pair<>(null, false));
    }
  }

  public void initClients(boolean useSSL, String trustStore, String trustStorePwd)
      throws IOException, TTransportException {
    for (TEndPoint nodeUrl : endPoint2client.keySet()) {
      Pair<IoTDBThriftSyncConnectorClient, Boolean> clientAndStatus = endPoint2client.get(nodeUrl);

      if (Boolean.TRUE.equals(clientAndStatus.right)) {
        continue;
      }

      final String ip = nodeUrl.getIp();
      final int port = nodeUrl.getPort();

      // Close the client if necessary
      if (clientAndStatus.getLeft() != null) {
        try {
          clientAndStatus.getLeft().close();
          clientAndStatus.setLeft(null);
        } catch (Exception e) {
          LOGGER.warn(
              "Failed to close client with target server ip: {}, port: {}, because: {}. Ignore it.",
              ip,
              port,
              e.getMessage());
        }
      }

      clientAndStatus.setLeft(
          new IoTDBThriftSyncConnectorClient(
              new ThriftClientProperty.Builder()
                  .setConnectionTimeoutMs((int) PIPE_CONFIG.getPipeConnectorTimeoutMs())
                  .setRpcThriftCompressionEnabled(
                      PIPE_CONFIG.isPipeConnectorRPCThriftCompressionEnabled())
                  .build(),
              ip,
              port,
              useSSL,
              trustStore,
              trustStorePwd));

      try {
        final TPipeTransferResp resp =
            clientAndStatus
                .getLeft()
                .pipeTransfer(
                    PipeTransferHandshakeReq.toTPipeTransferReq(
                        CommonDescriptor.getInstance().getConfig().getTimestampPrecision()));
        if (resp.getStatus().getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
          LOGGER.warn(
              "Handshake error with target server ip: {}, port: {}, because: {}.",
              ip,
              port,
              resp.status);
        } else {
          clientAndStatus.setRight(true);
          LOGGER.info("Handshake success. Target server ip: {}, port: {}", ip, port);
        }
      } catch (TException e) {
        LOGGER.warn(
            "Handshake error with target server ip: {}, port: {}, because: {}.",
            ip,
            port,
            e.getMessage());
      }
    }

    for (TEndPoint nodeUrl : endPoint2client.keySet()) {
      if (Boolean.TRUE.equals(endPoint2client.get(nodeUrl).getRight())) {
        return;
      }
    }

    throw new PipeConnectionException(
        String.format("All target servers %s are not available.", endPoint2client.keySet()));
  }

  public Pair<IoTDBThriftSyncConnectorClient, Boolean> getOneConnectedClientAndStatus() {
    final int clientSize = endPoints.size();
    // Round-robin, find the next alive client
    for (int tryCount = 0; tryCount < clientSize; ++tryCount) {
      final int clientIndex = (int) (currentClientIndex++ % clientSize);
      Pair<IoTDBThriftSyncConnectorClient, Boolean> clientAndStatus =
          endPoint2client.get(endPoints.get(clientIndex));
      if (Boolean.TRUE.equals(clientAndStatus.getRight())) {
        return clientAndStatus;
      }
    }
    throw new PipeConnectionException(
        "All clients are dead, please check the connection to the receiver.");
  }

  public Pair<IoTDBThriftSyncConnectorClient, Boolean> getClientAndStatusByEvent(
      PipeInsertNodeTabletInsertionEvent event) {
    TEndPoint endPoint = leaderCacheManager.parseEndPoint(event);
    return endPoint == null || endPoint2client.containsKey(endPoint) || !useLeaderCache
        ? getOneConnectedClientAndStatus()
        : endPoint2client.get(endPoint);
  }

  public Pair<IoTDBThriftSyncConnectorClient, Boolean> getClientAndStatusByEvent(
      PipeRawTabletInsertionEvent event) {
    TEndPoint endPoint = leaderCacheManager.parseEndPoint(event);
    return endPoint == null || endPoint2client.containsKey(endPoint) || !useLeaderCache
        ? getOneConnectedClientAndStatus()
        : endPoint2client.get(endPoint);
  }

  public Pair<IoTDBThriftSyncConnectorClient, Boolean> getClientAndStatusByEvent(
      PipeTsFileInsertionEvent event) {
    return getOneConnectedClientAndStatus();
  }

  public void updateOrCreate() {}

  public void clean() {
    for (TEndPoint endPoint : endPoint2client.keySet()) {
      try {
        if (endPoint2client.get(endPoint).getLeft() != null) {
          endPoint2client.get(endPoint).getLeft().close();
          endPoint2client.get(endPoint).setLeft(null);
          endPoint2client.get(endPoint).setRight(null);
        }
      } catch (Exception e) {
        LOGGER.warn("Failed to close client {}:{}.", endPoint.getIp(), endPoint.getPort(), e);
      } finally {
        endPoint2client.get(endPoint).setRight(false);
      }
    }
  }
}
