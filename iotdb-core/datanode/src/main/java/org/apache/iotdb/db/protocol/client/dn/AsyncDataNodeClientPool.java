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

package org.apache.iotdb.db.protocol.client.dn;

import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.commons.client.ClientPoolFactory;
import org.apache.iotdb.commons.client.IClientManager;
import org.apache.iotdb.commons.client.async.AsyncDataNodeInternalServiceClient;
import org.apache.iotdb.commons.client.exception.ClientManagerException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/** Asynchronously send RPC requests to DataNodes. See queryengine.thrift for more details. */
public class AsyncDataNodeClientPool {

  private static final Logger LOGGER = LoggerFactory.getLogger(AsyncDataNodeClientPool.class);

  private final IClientManager<TEndPoint, AsyncDataNodeInternalServiceClient> clientManager;

  private static final int MAX_RETRY_NUM = 6;

  private AsyncDataNodeClientPool() {
    clientManager =
        new IClientManager.Factory<TEndPoint, AsyncDataNodeInternalServiceClient>()
            .createClientManager(
                new ClientPoolFactory.AsyncDataNodeInternalServiceClientPoolFactory());
  }

  /**
   * Send asynchronous requests to the specified DataNodes with default retry num
   *
   * <p>Notice: The DataNodes that failed to receive the requests will be reconnected
   *
   * @param clientHandler <RequestType, ResponseType> which will also contain the result
   * @param timeoutInMs timeout in milliseconds
   */
  public void sendAsyncRequestToDataNodeWithRetryAndTimeoutInMs(
      AsyncClientHandler<?, ?> clientHandler, long timeoutInMs) {
    sendAsyncRequest(clientHandler, MAX_RETRY_NUM, timeoutInMs);
  }

  /**
   * Send asynchronous requests to the specified DataNodes with default retry num
   *
   * <p>Notice: The DataNodes that failed to receive the requests will be reconnected
   *
   * @param clientHandler <RequestType, ResponseType> which will also contain the result
   */
  public void sendAsyncRequestToDataNodeWithRetry(AsyncClientHandler<?, ?> clientHandler) {
    sendAsyncRequest(clientHandler, MAX_RETRY_NUM, null);
  }

  public void sendAsyncRequestToDataNode(AsyncClientHandler<?, ?> clientHandler) {
    sendAsyncRequest(clientHandler, 1, null);
  }

  private void sendAsyncRequest(
      AsyncClientHandler<?, ?> clientHandler, int retryNum, Long timeoutInMs) {
    if (clientHandler.getRequestIndices().isEmpty()) {
      return;
    }

    DataNodeRequestType requestType = clientHandler.getRequestType();
    for (int retry = 0; retry < retryNum; retry++) {
      // Always Reset CountDownLatch first
      clientHandler.resetCountDownLatch();

      // Send requests to all targetDataNodes
      for (int requestId : clientHandler.getRequestIndices()) {
        TDataNodeLocation targetDataNode = clientHandler.getDataNodeLocation(requestId);
        sendAsyncRequestToDataNode(clientHandler, requestId, targetDataNode, retry);
      }

      // Wait for this batch of asynchronous RPC requests finish
      try {
        if (timeoutInMs == null) {
          clientHandler.getCountDownLatch().await();
        } else {
          if (!clientHandler.getCountDownLatch().await(timeoutInMs, TimeUnit.MILLISECONDS)) {
            LOGGER.warn(
                "Timeout during {} on ConfigNode. Retry: {}/{}", requestType, retry, retryNum);
          }
        }
      } catch (InterruptedException e) {
        LOGGER.error(
            "Interrupted during {} on ConfigNode. Retry: {}/{}", requestType, retry, retryNum);
        Thread.currentThread().interrupt();
      }

      // Check if there is a DataNode that fails to execute the request, and retry if there exists
      if (clientHandler.getRequestIndices().isEmpty()) {
        return;
      }
    }

    if (!clientHandler.getRequestIndices().isEmpty()) {
      LOGGER.warn(
          "Failed to {} on ConfigNode after {} retries, requestIndices: {}",
          requestType,
          retryNum,
          clientHandler.getRequestIndices());
    }
  }

  private void sendAsyncRequestToDataNode(
      AsyncClientHandler<?, ?> clientHandler,
      int requestId,
      TDataNodeLocation targetDataNode,
      int retryCount) {

    try {
      AsyncDataNodeInternalServiceClient client;
      client = clientManager.borrowClient(targetDataNode.getInternalEndPoint());
      Object req = clientHandler.getRequest(requestId);
      AbstractAsyncRPCHandler<?> handler =
          clientHandler.createAsyncRPCHandler(requestId, targetDataNode);
      AsyncTSStatusRPCHandler defaultHandler = (AsyncTSStatusRPCHandler) handler;

      switch (clientHandler.getRequestType()) {
        case TEST_CONNECTION:
          client.testConnection(defaultHandler);
          break;
        default:
          LOGGER.error(
              "Unexpected DataNode Request Type: {} when sendAsyncRequestToDataNode",
              clientHandler.getRequestType());
      }
    } catch (Exception e) {
      LOGGER.warn(
          "{} failed on DataNode {}, because {}, retrying {}...",
          clientHandler.getRequestType(),
          targetDataNode.getInternalEndPoint(),
          e.getMessage(),
          retryCount);
    }
  }

  /**
   * Always call this interface when a DataNode is restarted or removed.
   *
   * @param endPoint The specific DataNode
   */
  public void resetClient(TEndPoint endPoint) {
    clientManager.clear(endPoint);
  }

  public AsyncDataNodeInternalServiceClient getAsyncClient(TDataNodeLocation targetDataNode)
      throws ClientManagerException {
    return clientManager.borrowClient(targetDataNode.getInternalEndPoint());
  }

  // TODO: Is the ClientPool must be a singleton?
  private static class ClientPoolHolder {

    private static final AsyncDataNodeClientPool INSTANCE = new AsyncDataNodeClientPool();

    private ClientPoolHolder() {
      // Empty constructor
    }
  }

  public static AsyncDataNodeClientPool getInstance() {
    return ClientPoolHolder.INSTANCE;
  }
}
