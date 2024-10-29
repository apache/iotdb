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

package org.apache.iotdb.db.pipe.connector.client;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.commons.client.ClientPoolFactory;
import org.apache.iotdb.commons.client.IClientManager;
import org.apache.iotdb.commons.client.async.AsyncPipeDataTransferServiceClient;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.pipe.config.PipeConfig;
import org.apache.iotdb.commons.pipe.connector.client.IoTDBClientManager;
import org.apache.iotdb.commons.pipe.connector.payload.thrift.common.PipeTransferHandshakeConstant;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.pipe.connector.payload.evolvable.request.PipeTransferDataNodeHandshakeV1Req;
import org.apache.iotdb.db.pipe.connector.payload.evolvable.request.PipeTransferDataNodeHandshakeV2Req;
import org.apache.iotdb.pipe.api.exception.PipeConnectionException;
import org.apache.iotdb.pipe.api.exception.PipeException;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.service.rpc.thrift.TPipeTransferResp;

import org.apache.thrift.async.AsyncMethodCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Base64;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.iotdb.commons.pipe.config.constant.PipeConnectorConstant.CONNECTOR_LOAD_BALANCE_PRIORITY_STRATEGY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeConnectorConstant.CONNECTOR_LOAD_BALANCE_RANDOM_STRATEGY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeConnectorConstant.CONNECTOR_LOAD_BALANCE_ROUND_ROBIN_STRATEGY;

public class IoTDBDataNodeAsyncClientManager extends IoTDBClientManager
    implements IoTDBDataNodeCacheLeaderClientManager {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(IoTDBDataNodeAsyncClientManager.class);

  private final Set<TEndPoint> endPointSet;

  private static final Map<String, Integer> RECEIVER_ATTRIBUTES_REF_COUNT =
      new ConcurrentHashMap<>();
  private final String receiverAttributes;

  // receiverAttributes -> IClientManager<TEndPoint, AsyncPipeDataTransferServiceClient>
  private static final Map<String, IClientManager<TEndPoint, AsyncPipeDataTransferServiceClient>>
      ASYNC_PIPE_DATA_TRANSFER_CLIENT_MANAGER_HOLDER = new ConcurrentHashMap<>();
  private final IClientManager<TEndPoint, AsyncPipeDataTransferServiceClient> endPoint2Client;

  private final LoadBalancer loadBalancer;

  public IoTDBDataNodeAsyncClientManager(
      List<TEndPoint> endPoints,
      /* The following parameters are used locally. */
      boolean useLeaderCache,
      String loadBalanceStrategy,
      /* The following parameters are used to handshake with the receiver. */
      String username,
      String password,
      boolean shouldReceiverConvertOnTypeMismatch,
      String loadTsFileStrategy) {
    super(
        endPoints,
        useLeaderCache,
        username,
        password,
        shouldReceiverConvertOnTypeMismatch,
        loadTsFileStrategy);

    endPointSet = new HashSet<>(endPoints);

    receiverAttributes =
        String.format(
            "%s-%s-%s",
            Base64.getEncoder().encodeToString((username + ":" + password).getBytes()),
            shouldReceiverConvertOnTypeMismatch,
            loadTsFileStrategy);
    synchronized (IoTDBDataNodeAsyncClientManager.class) {
      if (!ASYNC_PIPE_DATA_TRANSFER_CLIENT_MANAGER_HOLDER.containsKey(receiverAttributes)) {
        ASYNC_PIPE_DATA_TRANSFER_CLIENT_MANAGER_HOLDER.putIfAbsent(
            receiverAttributes,
            new IClientManager.Factory<TEndPoint, AsyncPipeDataTransferServiceClient>()
                .createClientManager(
                    new ClientPoolFactory.AsyncPipeDataTransferServiceClientPoolFactory()));
      }
      endPoint2Client = ASYNC_PIPE_DATA_TRANSFER_CLIENT_MANAGER_HOLDER.get(receiverAttributes);

      RECEIVER_ATTRIBUTES_REF_COUNT.compute(
          receiverAttributes, (attributes, refCount) -> refCount == null ? 1 : refCount + 1);
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

  public AsyncPipeDataTransferServiceClient borrowClient() throws Exception {
    return loadBalancer.borrowClient();
  }

  public AsyncPipeDataTransferServiceClient borrowClient(String deviceId) throws Exception {
    if (!useLeaderCache || Objects.isNull(deviceId)) {
      return borrowClient();
    }

    return borrowClient(LEADER_CACHE_MANAGER.getLeaderEndPoint(deviceId));
  }

  public AsyncPipeDataTransferServiceClient borrowClient(TEndPoint endPoint) throws Exception {
    if (!useLeaderCache || Objects.isNull(endPoint)) {
      return borrowClient();
    }

    try {
      final AsyncPipeDataTransferServiceClient client = endPoint2Client.borrowClient(endPoint);
      if (handshakeIfNecessary(endPoint, client)) {
        return client;
      }
    } catch (Exception e) {
      LOGGER.warn(
          "failed to borrow client {}:{} for cached leader.",
          endPoint.getIp(),
          endPoint.getPort(),
          e);
    }

    return borrowClient();
  }

  /**
   * Handshake with the target if necessary.
   *
   * @param client client to handshake
   * @return {@code true} if the handshake is already finished, {@code false} if the handshake is
   *     not finished yet and finished in this method
   * @throws Exception if an error occurs.
   */
  private boolean handshakeIfNecessary(
      TEndPoint targetNodeUrl, AsyncPipeDataTransferServiceClient client) throws Exception {
    if (client.isHandshakeFinished()) {
      return true;
    }

    final AtomicBoolean isHandshakeFinished = new AtomicBoolean(false);
    final AtomicReference<TPipeTransferResp> resp = new AtomicReference<>();
    final AtomicReference<Exception> exception = new AtomicReference<>();

    final AsyncMethodCallback<TPipeTransferResp> callback =
        new AsyncMethodCallback<TPipeTransferResp>() {
          @Override
          public void onComplete(TPipeTransferResp response) {
            resp.set(response);

            if (response.getStatus().getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
              LOGGER.warn(
                  "Handshake error with receiver {}:{}, code: {}, message: {}.",
                  targetNodeUrl.getIp(),
                  targetNodeUrl.getPort(),
                  response.getStatus().getCode(),
                  response.getStatus().getMessage());
              exception.set(
                  new PipeConnectionException(
                      String.format(
                          "Handshake error with receiver %s:%s, code: %d, message: %s.",
                          targetNodeUrl.getIp(),
                          targetNodeUrl.getPort(),
                          response.getStatus().getCode(),
                          response.getStatus().getMessage())));
            } else {
              LOGGER.info(
                  "Handshake successfully with receiver {}:{}.",
                  targetNodeUrl.getIp(),
                  targetNodeUrl.getPort());
              client.markHandshakeFinished();
            }

            isHandshakeFinished.set(true);
          }

          @Override
          public void onError(Exception e) {
            LOGGER.warn(
                "Handshake error with receiver {}:{}.",
                targetNodeUrl.getIp(),
                targetNodeUrl.getPort(),
                e);
            exception.set(e);

            isHandshakeFinished.set(true);
          }
        };

    try {
      client.setShouldReturnSelf(false);
      // Try to handshake by PipeTransferHandshakeV2Req.
      final HashMap<String, String> params = new HashMap<>();
      params.put(
          PipeTransferHandshakeConstant.HANDSHAKE_KEY_CLUSTER_ID,
          IoTDBDescriptor.getInstance().getConfig().getClusterId());
      params.put(
          PipeTransferHandshakeConstant.HANDSHAKE_KEY_TIME_PRECISION,
          CommonDescriptor.getInstance().getConfig().getTimestampPrecision());
      params.put(
          PipeTransferHandshakeConstant.HANDSHAKE_KEY_CONVERT_ON_TYPE_MISMATCH,
          Boolean.toString(shouldReceiverConvertOnTypeMismatch));
      params.put(
          PipeTransferHandshakeConstant.HANDSHAKE_KEY_LOAD_TSFILE_STRATEGY, loadTsFileStrategy);
      params.put(PipeTransferHandshakeConstant.HANDSHAKE_KEY_USERNAME, username);
      params.put(PipeTransferHandshakeConstant.HANDSHAKE_KEY_PASSWORD, password);

      client.setTimeoutDynamically(PipeConfig.getInstance().getPipeConnectorHandshakeTimeoutMs());
      client.pipeTransfer(PipeTransferDataNodeHandshakeV2Req.toTPipeTransferReq(params), callback);
      waitHandshakeFinished(isHandshakeFinished);

      // Retry to handshake by PipeTransferHandshakeV1Req.
      if (resp.get() != null
          && resp.get().getStatus().getCode() == TSStatusCode.PIPE_TYPE_ERROR.getStatusCode()) {
        LOGGER.info(
            "Handshake error by PipeTransferHandshakeV2Req with receiver {}:{} "
                + "retry to handshake by PipeTransferHandshakeV1Req.",
            targetNodeUrl.getIp(),
            targetNodeUrl.getPort());

        supportModsIfIsDataNodeReceiver = false;
        isHandshakeFinished.set(false);
        resp.set(null);
        exception.set(null);

        client.setTimeoutDynamically(PipeConfig.getInstance().getPipeConnectorHandshakeTimeoutMs());
        client.pipeTransfer(
            PipeTransferDataNodeHandshakeV1Req.toTPipeTransferReq(
                CommonDescriptor.getInstance().getConfig().getTimestampPrecision()),
            callback);
        waitHandshakeFinished(isHandshakeFinished);
      }
      if (exception.get() != null) {
        throw new PipeConnectionException("Failed to handshake.", exception.get());
      }
    } finally {
      client.setShouldReturnSelf(true);
      client.returnSelf();
    }

    return false;
  }

  private void waitHandshakeFinished(AtomicBoolean isHandshakeFinished) {
    try {
      while (!isHandshakeFinished.get()) {
        Thread.sleep(10);
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new PipeException("Interrupted while waiting for handshake response.", e);
    }
  }

  public void updateLeaderCache(String deviceId, TEndPoint endPoint) {
    if (!useLeaderCache || deviceId == null || endPoint == null) {
      return;
    }

    if (!endPointSet.contains(endPoint)) {
      endPointList.add(endPoint);
      endPointSet.add(endPoint);
    }

    LEADER_CACHE_MANAGER.updateLeaderEndPoint(deviceId, endPoint);
  }

  public void close() {
    synchronized (IoTDBDataNodeAsyncClientManager.class) {
      RECEIVER_ATTRIBUTES_REF_COUNT.computeIfPresent(
          receiverAttributes,
          (attributes, refCount) -> {
            if (refCount <= 1) {
              final IClientManager<TEndPoint, AsyncPipeDataTransferServiceClient> clientManager =
                  ASYNC_PIPE_DATA_TRANSFER_CLIENT_MANAGER_HOLDER.remove(receiverAttributes);
              if (clientManager != null) {
                try {
                  clientManager.close();
                } catch (Exception e) {
                  LOGGER.warn("Failed to close client manager.", e);
                }
              }
              return null;
            }
            return refCount - 1;
          });
    }
  }

  /////////////////////// Strategies for load balance //////////////////////////

  private interface LoadBalancer {
    AsyncPipeDataTransferServiceClient borrowClient() throws Exception;
  }

  private class RoundRobinLoadBalancer implements LoadBalancer {
    @Override
    public AsyncPipeDataTransferServiceClient borrowClient() throws Exception {
      final int clientSize = endPointList.size();
      while (true) {
        final TEndPoint targetNodeUrl = endPointList.get((int) (currentClientIndex++ % clientSize));
        final AsyncPipeDataTransferServiceClient client =
            endPoint2Client.borrowClient(targetNodeUrl);
        if (handshakeIfNecessary(targetNodeUrl, client)) {
          return client;
        }
      }
    }
  }

  private class RandomLoadBalancer implements LoadBalancer {
    @Override
    public AsyncPipeDataTransferServiceClient borrowClient() throws Exception {
      final int clientSize = endPointList.size();
      while (true) {
        final TEndPoint targetNodeUrl = endPointList.get((int) (Math.random() * clientSize));
        final AsyncPipeDataTransferServiceClient client =
            endPoint2Client.borrowClient(targetNodeUrl);
        if (handshakeIfNecessary(targetNodeUrl, client)) {
          return client;
        }
      }
    }
  }

  private class PriorityLoadBalancer implements LoadBalancer {
    @Override
    public AsyncPipeDataTransferServiceClient borrowClient() throws Exception {
      while (true) {
        for (final TEndPoint targetNodeUrl : endPointList) {
          final AsyncPipeDataTransferServiceClient client =
              endPoint2Client.borrowClient(targetNodeUrl);
          if (handshakeIfNecessary(targetNodeUrl, client)) {
            return client;
          }
        }
      }
    }
  }
}
