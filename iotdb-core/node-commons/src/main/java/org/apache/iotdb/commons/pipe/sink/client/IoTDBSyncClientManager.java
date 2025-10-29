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

package org.apache.iotdb.commons.pipe.sink.client;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.commons.audit.UserEntity;
import org.apache.iotdb.commons.client.property.ThriftClientProperty;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.pipe.config.PipeConfig;
import org.apache.iotdb.commons.pipe.resource.log.PipeLogger;
import org.apache.iotdb.commons.pipe.sink.payload.thrift.common.PipeTransferHandshakeConstant;
import org.apache.iotdb.commons.pipe.sink.payload.thrift.request.PipeTransferHandshakeV1Req;
import org.apache.iotdb.commons.pipe.sink.payload.thrift.request.PipeTransferHandshakeV2Req;
import org.apache.iotdb.pipe.api.exception.PipeConnectionException;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.service.rpc.thrift.TPipeTransferResp;

import org.apache.tsfile.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.CONNECTOR_LOAD_BALANCE_PRIORITY_STRATEGY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.CONNECTOR_LOAD_BALANCE_RANDOM_STRATEGY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.CONNECTOR_LOAD_BALANCE_ROUND_ROBIN_STRATEGY;

public abstract class IoTDBSyncClientManager extends IoTDBClientManager implements Closeable {

  private static final Logger LOGGER = LoggerFactory.getLogger(IoTDBSyncClientManager.class);

  private static final PipeConfig PIPE_CONFIG = PipeConfig.getInstance();

  private final boolean useSSL;
  private final String trustStorePath;
  private final String trustStorePwd;

  protected final Map<TEndPoint, Pair<IoTDBSyncClient, Boolean>> endPoint2ClientAndStatus =
      new ConcurrentHashMap<>();
  private final Map<TEndPoint, String> endPoint2HandshakeErrorMessage = new ConcurrentHashMap<>();

  private volatile long lastCheckClientStatusTimestamp = 0L;

  private final LoadBalancer loadBalancer;

  protected IoTDBSyncClientManager(
      List<TEndPoint> endPoints,
      boolean useSSL,
      String trustStorePath,
      String trustStorePwd,
      /* The following parameters are used locally. */
      boolean useLeaderCache,
      String loadBalanceStrategy,
      /* The following parameters are used to handshake with the receiver. */
      UserEntity userEntity,
      String password,
      boolean shouldReceiverConvertOnTypeMismatch,
      String loadTsFileStrategy,
      boolean validateTsFile,
      boolean shouldMarkAsPipeRequest,
      final boolean skipIfNoPrivileges) {
    super(
        endPoints,
        useLeaderCache,
        userEntity,
        password,
        shouldReceiverConvertOnTypeMismatch,
        loadTsFileStrategy,
        validateTsFile,
        shouldMarkAsPipeRequest,
        skipIfNoPrivileges);

    this.useSSL = useSSL;
    this.trustStorePath = trustStorePath;
    this.trustStorePwd = trustStorePwd;

    for (final TEndPoint endPoint : endPoints) {
      endPoint2ClientAndStatus.put(endPoint, new Pair<>(null, false));
    }

    switch (loadBalanceStrategy) {
      case CONNECTOR_LOAD_BALANCE_ROUND_ROBIN_STRATEGY:
        loadBalancer = new RoundRobinLoadBalancer();
        break;
      case CONNECTOR_LOAD_BALANCE_RANDOM_STRATEGY:
        loadBalancer = new RandomLoadBalancer();
        break;
      case CONNECTOR_LOAD_BALANCE_PRIORITY_STRATEGY:
        loadBalancer = new PriorityLoadBalancer();
        break;
      default:
        LOGGER.warn(
            "Unknown load balance strategy: {}, use round-robin strategy instead.",
            loadBalanceStrategy);
        loadBalancer = new RoundRobinLoadBalancer();
    }
  }

  public void checkClientStatusAndTryReconstructIfNecessary() {
    if (System.currentTimeMillis() - lastCheckClientStatusTimestamp
        < PipeConfig.getInstance().getPipeCheckAllSyncClientLiveTimeIntervalMs()) {
      // Check whether any clients are available, if any client is available, return directly
      for (final Pair<IoTDBSyncClient, Boolean> clientAndStatus :
          endPoint2ClientAndStatus.values()) {
        if (Boolean.TRUE.equals(clientAndStatus.getRight()) && clientAndStatus.getLeft() != null) {
          return;
        }
      }
    }

    // Reconstruct all dead clients
    for (final Map.Entry<TEndPoint, Pair<IoTDBSyncClient, Boolean>> entry :
        endPoint2ClientAndStatus.entrySet()) {
      if (Boolean.TRUE.equals(entry.getValue().getRight()) && entry.getValue().getLeft() != null) {
        continue;
      }

      reconstructClient(entry.getKey());
    }

    // Check whether any clients are available
    for (final Pair<IoTDBSyncClient, Boolean> clientAndStatus : endPoint2ClientAndStatus.values()) {
      if (Boolean.TRUE.equals(clientAndStatus.getRight()) && clientAndStatus.getLeft() != null) {
        lastCheckClientStatusTimestamp = System.currentTimeMillis();
        return;
      }
    }

    // If all clients are not available, throw an exception
    final StringBuilder errorMessage =
        new StringBuilder(
            String.format(
                "All target servers %s are not available.", endPoint2ClientAndStatus.keySet()));
    for (final Map.Entry<TEndPoint, String> entry : endPoint2HandshakeErrorMessage.entrySet()) {
      errorMessage
          .append(" (")
          .append("host: ")
          .append(entry.getKey().getIp())
          .append(", port: ")
          .append(entry.getKey().getPort())
          .append(", because: ")
          .append(entry.getValue())
          .append(")");
    }
    throw new PipeConnectionException(errorMessage.toString());
  }

  protected void reconstructClient(TEndPoint endPoint) {
    endPoint2HandshakeErrorMessage.remove(endPoint);

    final Pair<IoTDBSyncClient, Boolean> clientAndStatus = endPoint2ClientAndStatus.get(endPoint);

    if (clientAndStatus.getLeft() != null) {
      try {
        clientAndStatus.getLeft().close();
      } catch (Exception e) {
        LOGGER.warn(
            "Failed to close client with target server ip: {}, port: {}, because: {}. Ignore it.",
            endPoint.getIp(),
            endPoint.getPort(),
            e.getMessage());
      }
    }

    // It is necessary to ensure that the client is initialized successfully and not null. If false
    // is returned, it means that the initialization is not successful and the handshake operation
    // is not performed.
    if (initClientAndStatus(clientAndStatus, endPoint)) {
      sendHandshakeReq(clientAndStatus);
    }
  }

  private boolean initClientAndStatus(
      final Pair<IoTDBSyncClient, Boolean> clientAndStatus, final TEndPoint endPoint) {
    try {
      clientAndStatus.setLeft(
          new IoTDBSyncClient(
              new ThriftClientProperty.Builder()
                  .setConnectionTimeoutMs(PIPE_CONFIG.getPipeConnectorHandshakeTimeoutMs())
                  .setRpcThriftCompressionEnabled(
                      PIPE_CONFIG.isPipeConnectorRPCThriftCompressionEnabled())
                  .build(),
              endPoint.getIp(),
              endPoint.getPort(),
              useSSL,
              trustStorePath,
              trustStorePwd));
      return true;
    } catch (Exception e) {
      endPoint2HandshakeErrorMessage.put(endPoint, e.getMessage());
      PipeLogger.log(
          LOGGER::warn,
          e,
          "Failed to initialize client with target server ip: %s, port: %s, because %s",
          endPoint.getIp(),
          endPoint.getPort(),
          e.getMessage());
      return false;
    }
  }

  public void sendHandshakeReq(final Pair<IoTDBSyncClient, Boolean> clientAndStatus) {
    final IoTDBSyncClient client = clientAndStatus.getLeft();
    try {
      final HashMap<String, String> params = new HashMap<>();
      params.put(
          PipeTransferHandshakeConstant.HANDSHAKE_KEY_TIME_PRECISION,
          CommonDescriptor.getInstance().getConfig().getTimestampPrecision());
      params.put(PipeTransferHandshakeConstant.HANDSHAKE_KEY_CLUSTER_ID, getClusterId());
      params.put(
          PipeTransferHandshakeConstant.HANDSHAKE_KEY_CONVERT_ON_TYPE_MISMATCH,
          Boolean.toString(shouldReceiverConvertOnTypeMismatch));
      params.put(
          PipeTransferHandshakeConstant.HANDSHAKE_KEY_LOAD_TSFILE_STRATEGY, loadTsFileStrategy);
      params.put(
          PipeTransferHandshakeConstant.HANDSHAKE_KEY_USER_ID,
          String.valueOf(userEntity.getUserId()));
      params.put(PipeTransferHandshakeConstant.HANDSHAKE_KEY_USERNAME, userEntity.getUsername());
      params.put(
          PipeTransferHandshakeConstant.HANDSHAKE_KEY_CLI_HOSTNAME, userEntity.getCliHostname());
      params.put(PipeTransferHandshakeConstant.HANDSHAKE_KEY_PASSWORD, password);
      params.put(
          PipeTransferHandshakeConstant.HANDSHAKE_KEY_VALIDATE_TSFILE,
          Boolean.toString(validateTsFile));
      params.put(
          PipeTransferHandshakeConstant.HANDSHAKE_KEY_MARK_AS_PIPE_REQUEST,
          Boolean.toString(shouldMarkAsPipeRequest));
      params.put(
          PipeTransferHandshakeConstant.HANDSHAKE_KEY_SKIP_IF,
          Boolean.toString(skipIfNoPrivileges));

      // Try to handshake by PipeTransferHandshakeV2Req.
      TPipeTransferResp resp = client.pipeTransfer(buildHandshakeV2Req(params));
      // Receiver may be an old version, so we need to retry to handshake by
      // PipeTransferHandshakeV1Req.
      if (resp.getStatus().getCode() == TSStatusCode.PIPE_TYPE_ERROR.getStatusCode()) {
        LOGGER.warn(
            "Handshake error with target server ip: {}, port: {}, because: {}. "
                + "Retry to handshake by PipeTransferHandshakeV1Req.",
            client.getIpAddress(),
            client.getPort(),
            resp.getStatus());
        supportModsIfIsDataNodeReceiver = false;
        resp = client.pipeTransfer(buildHandshakeV1Req());
      }

      if (resp.getStatus().getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        LOGGER.warn(
            "Handshake error with target server ip: {}, port: {}, because: {}.",
            client.getIpAddress(),
            client.getPort(),
            resp.getStatus());
        endPoint2HandshakeErrorMessage.put(client.getEndPoint(), resp.getStatus().getMessage());
      } else {
        clientAndStatus.setRight(true);
        client.setTimeout(CONNECTION_TIMEOUT_MS.get());
        LOGGER.info(
            "Handshake success. Target server ip: {}, port: {}",
            client.getIpAddress(),
            client.getPort());
      }
    } catch (Exception e) {
      LOGGER.warn(
          "Handshake error with target server ip: {}, port: {}, because: {}.",
          client.getIpAddress(),
          client.getPort(),
          e.getMessage(),
          e);
      endPoint2HandshakeErrorMessage.put(client.getEndPoint(), e.getMessage());
    }
  }

  protected abstract PipeTransferHandshakeV1Req buildHandshakeV1Req() throws IOException;

  protected abstract PipeTransferHandshakeV2Req buildHandshakeV2Req(Map<String, String> params)
      throws IOException;

  protected abstract String getClusterId();

  public Pair<IoTDBSyncClient, Boolean> getClient() {
    return loadBalancer.getClient();
  }

  @Override
  public void close() {
    for (final Map.Entry<TEndPoint, Pair<IoTDBSyncClient, Boolean>> entry :
        endPoint2ClientAndStatus.entrySet()) {
      final TEndPoint endPoint = entry.getKey();
      final Pair<IoTDBSyncClient, Boolean> clientAndStatus = entry.getValue();

      if (clientAndStatus == null) {
        continue;
      }

      try {
        if (clientAndStatus.getLeft() != null) {
          clientAndStatus.getLeft().close();
        }
        LOGGER.info("Client {}:{} closed.", endPoint.getIp(), endPoint.getPort());
      } catch (Exception e) {
        LOGGER.warn(
            "Failed to close client {}:{}, because: {}.",
            endPoint.getIp(),
            endPoint.getPort(),
            e.getMessage(),
            e);
      } finally {
        clientAndStatus.setRight(false);
      }
    }
  }

  /////////////////////// Strategies for load balance //////////////////////////

  private interface LoadBalancer {
    Pair<IoTDBSyncClient, Boolean> getClient();
  }

  private class RoundRobinLoadBalancer implements LoadBalancer {
    @Override
    public Pair<IoTDBSyncClient, Boolean> getClient() {
      final int clientSize = endPointList.size();
      // Round-robin, find the next alive client
      for (int tryCount = 0; tryCount < clientSize; ++tryCount) {
        final int clientIndex = (int) (currentClientIndex++ % clientSize);
        final Pair<IoTDBSyncClient, Boolean> clientAndStatus =
            endPoint2ClientAndStatus.get(endPointList.get(clientIndex));
        if (Boolean.TRUE.equals(clientAndStatus.getRight()) && clientAndStatus.getLeft() != null) {
          return clientAndStatus;
        }
      }

      throw new PipeConnectionException(
          "All clients are dead, please check the connection to the receiver.");
    }
  }

  private class RandomLoadBalancer implements LoadBalancer {
    @Override
    public Pair<IoTDBSyncClient, Boolean> getClient() {
      final int clientSize = endPointList.size();
      final int clientIndex = (int) (Math.random() * clientSize);
      final Pair<IoTDBSyncClient, Boolean> clientAndStatus =
          endPoint2ClientAndStatus.get(endPointList.get(clientIndex));
      if (Boolean.TRUE.equals(clientAndStatus.getRight()) && clientAndStatus.getLeft() != null) {
        return clientAndStatus;
      }

      // Random, find the next alive client
      for (int tryCount = 0; tryCount < clientSize - 1; ++tryCount) {
        final int nextClientIndex = (clientIndex + tryCount + 1) % clientSize;
        final Pair<IoTDBSyncClient, Boolean> nextClientAndStatus =
            endPoint2ClientAndStatus.get(endPointList.get(nextClientIndex));
        if (Boolean.TRUE.equals(nextClientAndStatus.getRight())
            && clientAndStatus.getLeft() != null) {
          return nextClientAndStatus;
        }
      }

      throw new PipeConnectionException(
          "All clients are dead, please check the connection to the receiver.");
    }
  }

  private class PriorityLoadBalancer implements LoadBalancer {
    @Override
    public Pair<IoTDBSyncClient, Boolean> getClient() {
      // Priority, find the first alive client
      for (final TEndPoint endPoint : endPointList) {
        final Pair<IoTDBSyncClient, Boolean> clientAndStatus =
            endPoint2ClientAndStatus.get(endPoint);
        if (Boolean.TRUE.equals(clientAndStatus.getRight()) && clientAndStatus.getLeft() != null) {
          return clientAndStatus;
        }
      }

      throw new PipeConnectionException(
          "All clients are dead, please check the connection to the receiver.");
    }
  }
}
