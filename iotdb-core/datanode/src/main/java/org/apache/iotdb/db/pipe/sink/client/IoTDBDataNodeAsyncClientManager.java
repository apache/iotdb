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

package org.apache.iotdb.db.pipe.sink.client;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.commons.audit.UserEntity;
import org.apache.iotdb.commons.client.ClientPoolFactory;
import org.apache.iotdb.commons.client.IClientManager;
import org.apache.iotdb.commons.client.ThriftClient;
import org.apache.iotdb.commons.client.async.AsyncPipeDataTransferServiceClient;
import org.apache.iotdb.commons.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.commons.concurrent.ThreadName;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.pipe.config.PipeConfig;
import org.apache.iotdb.commons.pipe.resource.log.PipeLogger;
import org.apache.iotdb.commons.pipe.sink.client.IoTDBClientManager;
import org.apache.iotdb.commons.pipe.sink.payload.thrift.common.PipeTransferHandshakeConstant;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.i18n.DataNodePipeMessages;
import org.apache.iotdb.db.pipe.sink.payload.evolvable.request.PipeTransferDataNodeHandshakeV1Req;
import org.apache.iotdb.db.pipe.sink.payload.evolvable.request.PipeTransferDataNodeHandshakeV2Req;
import org.apache.iotdb.pipe.api.exception.PipeConnectionException;
import org.apache.iotdb.pipe.api.exception.PipeException;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.service.rpc.thrift.TPipeTransferResp;

import org.apache.thrift.TException;
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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.CONNECTOR_LOAD_BALANCE_PRIORITY_STRATEGY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.CONNECTOR_LOAD_BALANCE_RANDOM_STRATEGY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.CONNECTOR_LOAD_BALANCE_ROUND_ROBIN_STRATEGY;

public class IoTDBDataNodeAsyncClientManager extends IoTDBClientManager
    implements IoTDBDataNodeCacheLeaderClientManager {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(IoTDBDataNodeAsyncClientManager.class);

  private final Set<TEndPoint> endPointSet;

  private static final Map<String, Integer> CLIENT_RESOURCE_REF_COUNT = new ConcurrentHashMap<>();
  private final String receiverAttributes;
  private final String clientResourceKey;

  // clientResourceKey -> IClientManager<TEndPoint, AsyncPipeDataTransferServiceClient>
  private static final Map<String, IClientManager<TEndPoint, AsyncPipeDataTransferServiceClient>>
      ASYNC_PIPE_DATA_TRANSFER_CLIENT_MANAGER_HOLDER = new ConcurrentHashMap<>();
  private static final Map<String, ExecutorService> TS_FILE_ASYNC_EXECUTOR_HOLDER =
      new ConcurrentHashMap<>();
  private static final AtomicInteger id = new AtomicInteger(0);

  private final IClientManager<TEndPoint, AsyncPipeDataTransferServiceClient> endPoint2Client;
  private ExecutorService executor;

  private final LoadBalancer loadBalancer;

  private volatile boolean isClosed = false;

  private final Map<TEndPoint, Long> unhealthyEndPointMap = new ConcurrentHashMap<>();

  public IoTDBDataNodeAsyncClientManager(
      final List<TEndPoint> endPoints,
      /* The following parameters are used locally. */
      final boolean useLeaderCache,
      final String loadBalanceStrategy,
      /* The following parameters are used to handshake with the receiver. */
      final UserEntity userEntity,
      final String password,
      final boolean shouldReceiverConvertOnTypeMismatch,
      final String loadTsFileStrategy,
      final boolean validateTsFile,
      final boolean shouldMarkAsPipeRequest,
      final boolean isTSFileUsed,
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

    endPointSet = new HashSet<>(endPoints);

    receiverAttributes =
        String.format(
            "%s-%s-%s-%s-%s-%s-%s",
            Base64.getEncoder()
                .encodeToString((userEntity.getUsername() + ":" + password).getBytes()),
            shouldReceiverConvertOnTypeMismatch,
            loadTsFileStrategy,
            validateTsFile,
            shouldMarkAsPipeRequest,
            isTSFileUsed,
            skipIfNoPrivileges);
    clientResourceKey = generateClientResourceKey(receiverAttributes, endPoints);
    synchronized (IoTDBDataNodeAsyncClientManager.class) {
      if (!ASYNC_PIPE_DATA_TRANSFER_CLIENT_MANAGER_HOLDER.containsKey(clientResourceKey)) {
        ASYNC_PIPE_DATA_TRANSFER_CLIENT_MANAGER_HOLDER.putIfAbsent(
            clientResourceKey,
            new IClientManager.Factory<TEndPoint, AsyncPipeDataTransferServiceClient>()
                .createClientManager(
                    isTSFileUsed
                        ? new ClientPoolFactory
                            .AsyncPipeTsFileDataTransferServiceClientPoolFactory()
                        : new ClientPoolFactory.AsyncPipeDataTransferServiceClientPoolFactory()));
      }
      endPoint2Client = ASYNC_PIPE_DATA_TRANSFER_CLIENT_MANAGER_HOLDER.get(clientResourceKey);

      if (isTSFileUsed) {
        if (!TS_FILE_ASYNC_EXECUTOR_HOLDER.containsKey(clientResourceKey)) {
          TS_FILE_ASYNC_EXECUTOR_HOLDER.putIfAbsent(
              clientResourceKey,
              IoTDBThreadPoolFactory.newFixedThreadPool(
                  PipeConfig.getInstance().getPipeRealTimeQueueMaxWaitingTsFileSize(),
                  ThreadName.PIPE_TSFILE_ASYNC_SEND_POOL.getName() + "-" + id.getAndIncrement()));
        }
        executor = TS_FILE_ASYNC_EXECUTOR_HOLDER.get(clientResourceKey);
      }

      CLIENT_RESOURCE_REF_COUNT.compute(
          clientResourceKey, (attributes, refCount) -> refCount == null ? 1 : refCount + 1);
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
            DataNodePipeMessages.UNKNOWN_LOAD_BALANCE_STRATEGY_USE_ROUND_ROBIN,
            loadBalanceStrategy);
        loadBalancer = new RoundRobinLoadBalancer();
    }
  }

  public AsyncPipeDataTransferServiceClient borrowClient() throws Exception {
    return loadBalancer.borrowClient();
  }

  public AsyncPipeDataTransferServiceClient borrowClient(final String deviceId) throws Exception {
    if (!useLeaderCache || Objects.isNull(deviceId)) {
      return borrowClient();
    }

    return borrowClient(LEADER_CACHE_MANAGER.getLeaderEndPoint(deviceId));
  }

  public AsyncPipeDataTransferServiceClient borrowClient(final TEndPoint endPoint)
      throws Exception {
    if (!useLeaderCache || Objects.isNull(endPoint) || isUnhealthy(endPoint)) {
      return borrowClient();
    }

    try {
      final AsyncPipeDataTransferServiceClient client = endPoint2Client.borrowClient(endPoint);
      if (handshakeIfNecessary(endPoint, client)) {
        return client;
      }
    } catch (final Exception e) {
      PipeLogger.log(
          ignored ->
              LOGGER.warn(
                  DataNodePipeMessages.FAILED_TO_BORROW_CLIENT_FOR_CACHED_LEADER,
                  endPoint.getIp(),
                  endPoint.getPort(),
                  e),
          e,
          DataNodePipeMessages.FAILED_TO_BORROW_CLIENT_FOR_CACHED_LEADER,
          endPoint.getIp(),
          endPoint.getPort());
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
      final TEndPoint targetNodeUrl, final AsyncPipeDataTransferServiceClient client)
      throws Exception {
    if (client.isHandshakeFinished()) {
      return true;
    }

    final AtomicBoolean isHandshakeFinished = new AtomicBoolean(false);
    final AtomicReference<TPipeTransferResp> resp = new AtomicReference<>();
    final AtomicReference<Exception> exception = new AtomicReference<>();

    final AsyncMethodCallback<TPipeTransferResp> callback =
        new AsyncMethodCallback<TPipeTransferResp>() {
          @Override
          public void onComplete(final TPipeTransferResp response) {
            resp.set(response);

            if (response.getStatus().getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
              PipeLogger.log(
                  LOGGER::warn,
                  DataNodePipeMessages.HANDSHAKE_ERROR_WITH_RECEIVER,
                  targetNodeUrl.getIp(),
                  targetNodeUrl.getPort(),
                  response.getStatus().getCode(),
                  response.getStatus().getMessage());
              exception.set(
                  new PipeConnectionException(
                      String.format(
                          DataNodePipeMessages
                              .PIPE_EXCEPTION_HANDSHAKE_ERROR_WITH_RECEIVER_S_S_CODE_D_MESSAGE_S_4ED82649,
                          targetNodeUrl.getIp(),
                          targetNodeUrl.getPort(),
                          response.getStatus().getCode(),
                          response.getStatus().getMessage())));
            } else {
              LOGGER.info(
                  DataNodePipeMessages.HANDSHAKE_SUCCESSFULLY_WITH_RECEIVER,
                  targetNodeUrl.getIp(),
                  targetNodeUrl.getPort());
              client.markHandshakeFinished();
            }

            isHandshakeFinished.set(true);
            synchronized (isHandshakeFinished) {
              isHandshakeFinished.notifyAll();
            }
          }

          @Override
          public void onError(final Exception e) {
            ThriftClient.resolveException(e, client);
            PipeLogger.log(
                LOGGER::warn,
                e,
                DataNodePipeMessages.HANDSHAKE_ERROR_WITH_RECEIVER_1,
                targetNodeUrl.getIp(),
                targetNodeUrl.getPort());
            exception.set(e);

            isHandshakeFinished.set(true);
            synchronized (isHandshakeFinished) {
              isHandshakeFinished.notifyAll();
            }
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

      client.setTimeoutDynamically(PipeConfig.getInstance().getPipeSinkHandshakeTimeoutMs());
      client.pipeTransfer(PipeTransferDataNodeHandshakeV2Req.toTPipeTransferReq(params), callback);
      waitHandshakeFinished(isHandshakeFinished);

      // Retry to handshake by PipeTransferHandshakeV1Req.
      if (resp.get() != null
          && resp.get().getStatus().getCode() == TSStatusCode.PIPE_TYPE_ERROR.getStatusCode()) {
        PipeLogger.log(
            LOGGER::warn,
            DataNodePipeMessages.HANDSHAKE_ERROR_BY_HANDSHAKE_V2_RETRY_WITH_V1,
            targetNodeUrl.getIp(),
            targetNodeUrl.getPort());

        supportModsIfIsDataNodeReceiver = false;
        isHandshakeFinished.set(false);
        resp.set(null);
        exception.set(null);

        client.setTimeoutDynamically(PipeConfig.getInstance().getPipeSinkHandshakeTimeoutMs());
        client.pipeTransfer(
            PipeTransferDataNodeHandshakeV1Req.toTPipeTransferReq(
                CommonDescriptor.getInstance().getConfig().getTimestampPrecision()),
            callback);
        waitHandshakeFinished(isHandshakeFinished);
      }
      if (exception.get() != null) {
        markUnhealthy(targetNodeUrl);
        throw new PipeConnectionException(
            DataNodePipeMessages.FAILED_TO_HANDSHAKE, exception.get());
      } else {
        markHealthy(targetNodeUrl);
      }
    } catch (TException e) {
      client.resetMethodStateIfStopped();
      markUnhealthy(targetNodeUrl);
      throw e;
    } finally {
      if (isClosed) {
        try {
          client.close();
          client.invalidateAll();
        } catch (final Exception e) {
          PipeLogger.log(
              ignored ->
                  LOGGER.warn(
                      DataNodePipeMessages.FAILED_TO_CLOSE_CLIENT_AFTER_HANDSHAKE_FAILURE,
                      targetNodeUrl.getIp(),
                      targetNodeUrl.getPort(),
                      e),
              e,
              DataNodePipeMessages.FAILED_TO_CLOSE_CLIENT_AFTER_HANDSHAKE_FAILURE,
              targetNodeUrl.getIp(),
              targetNodeUrl.getPort());
        }
      }
      client.setShouldReturnSelf(true);
      client.returnSelf();
    }

    return false;
  }

  private void waitHandshakeFinished(final AtomicBoolean isHandshakeFinished) {
    try {
      while (!isHandshakeFinished.get()) {
        if (isClosed) {
          throw new PipeConnectionException(
              DataNodePipeMessages.TIMED_OUT_WHEN_WAITING_FOR_CLIENT_HANDSHAKE);
        }
        synchronized (isHandshakeFinished) {
          isHandshakeFinished.wait(1);
        }
      }
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new PipeException(
          DataNodePipeMessages.INTERRUPTED_WHILE_WAITING_FOR_HANDSHAKE_RESPONSE, e);
    }
  }

  public void updateLeaderCache(final String deviceId, final TEndPoint endPoint) {
    if (!useLeaderCache || deviceId == null || endPoint == null) {
      return;
    }

    if (!endPointSet.contains(endPoint)) {
      endPointList.add(endPoint);
      endPointSet.add(endPoint);
    }

    LEADER_CACHE_MANAGER.updateLeaderEndPoint(deviceId, endPoint);
  }

  public ExecutorService getExecutor() {
    return executor;
  }

  public void close() {
    isClosed = true;
    synchronized (IoTDBDataNodeAsyncClientManager.class) {
      CLIENT_RESOURCE_REF_COUNT.computeIfPresent(
          clientResourceKey,
          (attributes, refCount) -> {
            if (refCount <= 1) {
              final IClientManager<TEndPoint, AsyncPipeDataTransferServiceClient> clientManager =
                  ASYNC_PIPE_DATA_TRANSFER_CLIENT_MANAGER_HOLDER.remove(clientResourceKey);
              if (clientManager != null) {
                try {
                  clientManager.close();
                  LOGGER.info(
                      DataNodePipeMessages
                          .CLOSED_ASYNCPIPEDATATRANSFERSERVICECLIENTMANAGER_FOR_RECEIVER_ATTRIBUTES,
                      clientResourceKey);
                } catch (final Exception e) {
                  LOGGER.warn(
                      DataNodePipeMessages
                          .FAILED_TO_CLOSE_ASYNCPIPEDATATRANSFERSERVICECLIENTMANAGER_FOR_RECEIVER_ATTRIBUTE,
                      clientResourceKey,
                      e);
                }
              }

              final ExecutorService executor =
                  TS_FILE_ASYNC_EXECUTOR_HOLDER.remove(clientResourceKey);
              if (executor != null) {
                try {
                  executor.shutdown();
                  LOGGER.info(DataNodePipeMessages.SUCCESSFULLY_SHUTDOWN_EXECUTOR, executor);
                } catch (final Exception e) {
                  LOGGER.warn(DataNodePipeMessages.FAILED_TO_SHUTDOWN_EXECUTOR, executor);
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
      long n = 0;
      while (true) {
        final TEndPoint targetNodeUrl = endPointList.get((int) (currentClientIndex++ % clientSize));
        if (isUnhealthy(targetNodeUrl) && n < clientSize) {
          n++;
          continue;
        }

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
      long n = 0;

      while (true) {
        final TEndPoint targetNodeUrl = endPointList.get((int) (Math.random() * clientSize));
        if (isUnhealthy(targetNodeUrl) && n < clientSize) {
          n++;
          continue;
        }

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
      final int clientSize = endPointList.size();
      long n = 0;
      while (true) {
        for (final TEndPoint targetNodeUrl : endPointList) {
          if (isUnhealthy(targetNodeUrl) && n < clientSize) {
            n++;
            continue;
          }

          final AsyncPipeDataTransferServiceClient client =
              endPoint2Client.borrowClient(targetNodeUrl);
          if (handshakeIfNecessary(targetNodeUrl, client)) {
            return client;
          }
        }
      }
    }
  }

  private boolean isUnhealthy(TEndPoint endPoint) {
    Long downTime = unhealthyEndPointMap.get(endPoint);
    if (downTime == null) {
      return false;
    }
    if (System.currentTimeMillis() - downTime
        > PipeConfig.getInstance().getPipeCheckAllSyncClientLiveTimeIntervalMs()) {
      markHealthy(endPoint);
      return false;
    }
    return true;
  }

  private void markUnhealthy(TEndPoint endPoint) {
    unhealthyEndPointMap.put(endPoint, System.currentTimeMillis());
  }

  private void markHealthy(TEndPoint endPoint) {
    unhealthyEndPointMap.remove(endPoint);
  }

  private static String generateClientResourceKey(
      final String receiverAttributes, final List<TEndPoint> endPoints) {
    return String.format(
        "%s-%s",
        receiverAttributes,
        endPoints.stream()
            .map(endPoint -> String.format("%s:%s", endPoint.getIp(), endPoint.getPort()))
            .distinct()
            .sorted()
            .collect(Collectors.joining(",", "[", "]")));
  }
}
