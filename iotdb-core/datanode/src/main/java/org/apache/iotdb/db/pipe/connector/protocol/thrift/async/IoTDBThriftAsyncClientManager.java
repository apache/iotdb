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

package org.apache.iotdb.db.pipe.connector.protocol.thrift.async;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.commons.client.ClientPoolFactory;
import org.apache.iotdb.commons.client.IClientManager;
import org.apache.iotdb.commons.client.async.AsyncPipeDataTransferServiceClient;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.db.pipe.connector.payload.evolvable.request.PipeTransferHandshakeReq;
import org.apache.iotdb.db.pipe.connector.protocol.thrift.IoTDBThriftClientManager;
import org.apache.iotdb.pipe.api.exception.PipeConnectionException;
import org.apache.iotdb.pipe.api.exception.PipeException;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.service.rpc.thrift.TPipeTransferResp;

import org.apache.thrift.async.AsyncMethodCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

public class IoTDBThriftAsyncClientManager extends IoTDBThriftClientManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(IoTDBThriftAsyncClientManager.class);

  private final Set<TEndPoint> endPointSet;

  private static final AtomicReference<
          IClientManager<TEndPoint, AsyncPipeDataTransferServiceClient>>
      ASYNC_PIPE_DATA_TRANSFER_CLIENT_MANAGER_HOLDER = new AtomicReference<>();
  private final IClientManager<TEndPoint, AsyncPipeDataTransferServiceClient> endPoint2Client;

  public IoTDBThriftAsyncClientManager(List<TEndPoint> endPoints, boolean useLeaderCache) {
    super(endPoints, useLeaderCache);

    endPointSet = new HashSet<>(endPoints);

    if (ASYNC_PIPE_DATA_TRANSFER_CLIENT_MANAGER_HOLDER.get() == null) {
      synchronized (IoTDBThriftAsyncConnector.class) {
        if (ASYNC_PIPE_DATA_TRANSFER_CLIENT_MANAGER_HOLDER.get() == null) {
          ASYNC_PIPE_DATA_TRANSFER_CLIENT_MANAGER_HOLDER.set(
              new IClientManager.Factory<TEndPoint, AsyncPipeDataTransferServiceClient>()
                  .createClientManager(
                      new ClientPoolFactory.AsyncPipeDataTransferServiceClientPoolFactory()));
        }
      }
    }
    endPoint2Client = ASYNC_PIPE_DATA_TRANSFER_CLIENT_MANAGER_HOLDER.get();
  }

  public AsyncPipeDataTransferServiceClient borrowClient() throws Exception {
    final int clientSize = endPointList.size();
    while (true) {
      final TEndPoint targetNodeUrl = endPointList.get((int) (currentClientIndex++ % clientSize));
      final AsyncPipeDataTransferServiceClient client = endPoint2Client.borrowClient(targetNodeUrl);
      if (handshakeIfNecessary(targetNodeUrl, client)) {
        return client;
      }
    }
  }

  public AsyncPipeDataTransferServiceClient borrowClient(String deviceId) throws Exception {
    if (!useLeaderCache) {
      return borrowClient();
    }

    final TEndPoint endPoint = leaderCacheManager.getLeaderEndPoint(deviceId);
    if (endPoint == null) {
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
   * @return true if the handshake is already finished, false if the handshake is not finished yet
   *     and finished in this method
   * @throws Exception if an error occurs.
   */
  private boolean handshakeIfNecessary(
      TEndPoint targetNodeUrl, AsyncPipeDataTransferServiceClient client) throws Exception {
    if (client.isHandshakeFinished()) {
      return true;
    }

    final AtomicBoolean isHandshakeFinished = new AtomicBoolean(false);
    final AtomicReference<Exception> exception = new AtomicReference<>();

    client.pipeTransfer(
        PipeTransferHandshakeReq.toTPipeTransferReq(
            CommonDescriptor.getInstance().getConfig().getTimestampPrecision()),
        new AsyncMethodCallback<TPipeTransferResp>() {
          @Override
          public void onComplete(TPipeTransferResp response) {
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
        });

    try {
      while (!isHandshakeFinished.get()) {
        Thread.sleep(10);
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new PipeException("Interrupted while waiting for handshake response.", e);
    }

    if (exception.get() != null) {
      throw new PipeConnectionException("Failed to handshake.", exception.get());
    }

    return false;
  }

  public void updateLeaderCache(String deviceId, TEndPoint endPoint) {
    if (!useLeaderCache) {
      return;
    }

    if (!endPointSet.contains(endPoint)) {
      endPointList.add(endPoint);
      endPointSet.add(endPoint);
    }

    leaderCacheManager.updateLeaderEndPoint(deviceId, endPoint);
  }
}
