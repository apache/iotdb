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

package org.apache.iotdb.db.protocol.client.cn;

import org.apache.iotdb.common.rpc.thrift.TConfigNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.commons.client.ClientPoolFactory;
import org.apache.iotdb.commons.client.IClientManager;
import org.apache.iotdb.commons.client.async.AsyncConfigNodeInternalServiceClient;
import org.apache.iotdb.commons.client.exception.ClientManagerException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/** Asynchronously send RPC requests to ConfigNodes. See queryengine.thrift for more details. */
public class AsyncConfigNodeClientPool {

  private static final Logger LOGGER = LoggerFactory.getLogger(AsyncConfigNodeClientPool.class);

  private final IClientManager<TEndPoint, AsyncConfigNodeInternalServiceClient> clientManager;

  private static final int MAX_RETRY_NUM = 6;

  private AsyncConfigNodeClientPool() {
    clientManager =
        new IClientManager.Factory<TEndPoint, AsyncConfigNodeInternalServiceClient>()
            .createClientManager(
                new ClientPoolFactory.AsyncConfigNodeInternalServiceClientPoolFactory());
  }

  /**
   * Send asynchronous requests to the specified ConfigNodes with default retry num
   *
   * <p>Notice: The ConfigNodes that failed to receive the requests will be reconnected
   *
   * @param clientHandler <RequestType, ResponseType> which will also contain the result
   * @param timeoutInMs timeout in milliseconds
   */
  public void sendAsyncRequestToConfigNodeWithRetryAndTimeoutInMs(
      AsyncConfigNodeClientHandler<?, ?> clientHandler, long timeoutInMs) {
    sendAsyncRequest(clientHandler, MAX_RETRY_NUM, timeoutInMs);
  }

  /**
   * Send asynchronous requests to the specified ConfigNodes with default retry num
   *
   * <p>Notice: The ConfigNodes that failed to receive the requests will be reconnected
   *
   * @param clientHandler <RequestType, ResponseType> which will also contain the result
   */
  public void sendAsyncRequestToConfigNodeWithRetry(
      AsyncConfigNodeClientHandler<?, ?> clientHandler) {
    sendAsyncRequest(clientHandler, MAX_RETRY_NUM, null);
  }

  public void sendAsyncRequestToConfigNode(AsyncConfigNodeClientHandler<?, ?> clientHandler) {
    sendAsyncRequest(clientHandler, 1, null);
  }

  private void sendAsyncRequest(
      AsyncConfigNodeClientHandler<?, ?> clientHandler, int retryNum, Long timeoutInMs) {
    if (clientHandler.getRequestIndices().isEmpty()) {
      return;
    }

    ConfigNodeRequestType requestType = clientHandler.getRequestType();
    for (int retry = 0; retry < retryNum; retry++) {
      // Always Reset CountDownLatch first
      clientHandler.resetCountDownLatch();

      // Send requests to all targetConfigNodes
      for (int requestId : clientHandler.getRequestIndices()) {
        TConfigNodeLocation targetConfigNode = clientHandler.getConfigNodeLocation(requestId);
        sendAsyncRequestToConfigNode(clientHandler, requestId, targetConfigNode, retry);
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

      // Check if there is a ConfigNode that fails to execute the request, and retry if there exists
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

  private void sendAsyncRequestToConfigNode(
      AsyncConfigNodeClientHandler<?, ?> clientHandler,
      int requestId,
      TConfigNodeLocation targetConfigNode,
      int retryCount) {

    try {
      AsyncConfigNodeInternalServiceClient client;
      client = clientManager.borrowClient(targetConfigNode.getInternalEndPoint());
      Object req = clientHandler.getRequest(requestId);
      AbstractAsyncRPCHandler2<?> handler =
          clientHandler.createAsyncRPCHandler(requestId, targetConfigNode);
      AsyncTSStatusRPCHandler2 defaultHandler = (AsyncTSStatusRPCHandler2) handler;

      switch (clientHandler.getRequestType()) {
          //                case SUBMIT_TEST_CONNECTION_TASK:
          //                    client.submitTestConnectionTask((TNodeLocations) req,
          // (SubmitTestConnectionTaskToConfigNodeRPCHandler) handler);
          //                    break;
        case TEST_CONNECTION:
          client.testConnection(defaultHandler);
          break;
        default:
          LOGGER.error(
              "Unexpected ConfigNode Request Type: {} when sendAsyncRequestToConfigNode",
              clientHandler.getRequestType());
      }
    } catch (Exception e) {
      LOGGER.warn(
          "{} failed on ConfigNode {}, because {}, retrying {}...",
          clientHandler.getRequestType(),
          targetConfigNode.getInternalEndPoint(),
          e.getMessage(),
          retryCount);
    }
  }

  /**
   * Always call this interface when a ConfigNode is restarted or removed.
   *
   * @param endPoint The specific ConfigNode
   */
  public void resetClient(TEndPoint endPoint) {
    clientManager.clear(endPoint);
  }

  public AsyncConfigNodeInternalServiceClient getAsyncClient(TConfigNodeLocation targetConfigNode)
      throws ClientManagerException {
    return clientManager.borrowClient(targetConfigNode.getInternalEndPoint());
  }

  // TODO: Is the ClientPool must be a singleton?
  private static class ClientPoolHolder {

    private static final AsyncConfigNodeClientPool INSTANCE = new AsyncConfigNodeClientPool();

    private ClientPoolHolder() {
      // Empty constructor
    }
  }

  public static AsyncConfigNodeClientPool getInstance() {
    return ClientPoolHolder.INSTANCE;
  }
}
