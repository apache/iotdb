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

package org.apache.iotdb.commons.client.request;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.commons.client.IClientManager;
import org.apache.iotdb.commons.client.exception.ClientManagerException;
import org.apache.iotdb.commons.utils.function.CheckedTriConsumer;

import com.google.common.collect.ImmutableMap;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.concurrent.TimeUnit;

/** Asynchronously send RPC requests to Nodes. See queryengine.thrift for more details. */
public abstract class AsyncRequestManager<RequestType, NodeLocation, Client> {

  private static final Logger LOGGER = LoggerFactory.getLogger(AsyncRequestManager.class);

  protected IClientManager<TEndPoint, Client> clientManager;

  protected ImmutableMap<
          RequestType,
          CheckedTriConsumer<
              Object, Client, AsyncRequestRPCHandler<?, RequestType, NodeLocation>, TException>>
      actionMap;

  protected ImmutableMap.Builder<
          RequestType,
          CheckedTriConsumer<
              Object, Client, AsyncRequestRPCHandler<?, RequestType, NodeLocation>, TException>>
      actionMapBuilder;

  private static final int MAX_RETRY_NUM = 6;

  protected AsyncRequestManager() {
    initClientManager();
    actionMapBuilder = ImmutableMap.builder();
    initActionMapBuilder();
    this.actionMap = this.actionMapBuilder.build();
    checkActionMapCompleteness();
  }

  protected abstract void initClientManager();

  protected abstract void initActionMapBuilder();

  protected void checkActionMapCompleteness() {
    // No check by default
  }
  ;

  /**
   * Send asynchronous requests to the specified Nodes with default retry num
   *
   * <p>Notice: The Nodes that failed to receive the requests will be reconnected
   *
   * @param requestContext <RequestType, ResponseType> which will also contain the result
   * @param timeoutInMs timeout in milliseconds
   */
  public void sendAsyncRequestToNodeWithRetryAndTimeoutInMs(
      final AsyncRequestContext<?, ?, RequestType, NodeLocation> requestContext,
      final long timeoutInMs) {
    sendAsyncRequest(requestContext, MAX_RETRY_NUM, timeoutInMs);
  }

  /**
   * Send asynchronous requests to the specified Nodes with default retry num
   *
   * <p>Notice: The Nodes that failed to receive the requests will be reconnected
   *
   * @param requestContext <RequestType, ResponseType> which will also contain the result
   */
  public final void sendAsyncRequestWithRetry(
      final AsyncRequestContext<?, ?, RequestType, NodeLocation> requestContext) {
    sendAsyncRequest(requestContext, MAX_RETRY_NUM, null);
  }

  /**
   * Send asynchronous requests to the specified Nodes with default retry num
   *
   * <p>Notice: The Nodes that failed to receive the requests will be reconnected
   *
   * @param requestContext <RequestType, ResponseType> which will also contain the result
   */
  public final void sendAsyncRequestWithTimeoutInMs(
      final AsyncRequestContext<?, ?, RequestType, NodeLocation> requestContext,
      final long timeoutInMs) {
    sendAsyncRequest(requestContext, MAX_RETRY_NUM, timeoutInMs);
  }

  public final void sendAsyncRequest(
      final AsyncRequestContext<?, ?, RequestType, NodeLocation> requestContext) {
    sendAsyncRequest(requestContext, 1, null);
  }

  public void sendAsyncRequest(
      final AsyncRequestContext<?, ?, RequestType, NodeLocation> requestContext,
      final int retryNum,
      final Long timeoutInMs) {
    if (requestContext.getRequestIndices().isEmpty()) {
      return;
    }

    final RequestType requestType = requestContext.getRequestType();
    for (int retry = 0; retry < retryNum; retry++) {
      // Always Reset CountDownLatch first
      requestContext.resetCountDownLatch();

      // Send requests to all targetNodes
      final int finalRetry = retry;
      requestContext
          .getNodeLocationMap()
          .forEach(
              (requestId, nodeLocation) ->
                  sendAsyncRequest(requestContext, requestId, nodeLocation, finalRetry));

      // Wait for this batch of asynchronous RPC requests finish
      try {
        if (timeoutInMs == null) {
          requestContext.getCountDownLatch().await();
        } else {
          if (!requestContext.getCountDownLatch().await(timeoutInMs, TimeUnit.MILLISECONDS)) {
            LOGGER.warn("Timeout during {}. Retry: {}/{}", requestType, retry, retryNum);
          }
        }
      } catch (final InterruptedException e) {
        LOGGER.error("Interrupted during {}. Retry: {}/{}", requestType, retry, retryNum);
        Thread.currentThread().interrupt();
      }

      // Check if there is a Node that fails to execute the request, and retry if there exists
      if (requestContext.getRequestIndices().isEmpty()) {
        return;
      }
    }

    if (!requestContext.getRequestIndices().isEmpty()) {
      LOGGER.warn(
          "Failed to {} after {} retries, requestIndices: {}",
          requestType,
          retryNum,
          requestContext.getRequestIndices());
    }
  }

  protected void sendAsyncRequest(
      AsyncRequestContext<?, ?, RequestType, NodeLocation> requestContext,
      int requestId,
      NodeLocation targetNode,
      int retryCount) {
    try {
      if (!actionMap.containsKey(requestContext.getRequestType())) {
        throw new UnsupportedOperationException(
            "unsupported request type "
                + requestContext.getRequestType()
                + ", please set it in AsyncRequestManager::initActionMapBuilder()");
      }
      Client client = clientManager.borrowClient(nodeLocationToEndPoint(targetNode));
      adjustClientTimeoutIfNecessary(requestContext.getRequestType(), client);
      Object req = requestContext.getRequest(requestId);
      AsyncRequestRPCHandler<?, RequestType, NodeLocation> handler =
          buildHandler(requestContext, requestId, targetNode);
      Objects.requireNonNull(actionMap.get(requestContext.getRequestType()))
          .accept(req, client, handler);
    } catch (Exception e) {
      LOGGER.warn(
          "{} failed on Node {}, because {}, retrying {}...",
          requestContext.getRequestType(),
          nodeLocationToEndPoint(targetNode),
          e.getMessage(),
          retryCount);
    }
  }

  protected void adjustClientTimeoutIfNecessary(RequestType type, Client client) {
    // In default, no need to do this
  }

  protected abstract TEndPoint nodeLocationToEndPoint(NodeLocation location);

  protected abstract AsyncRequestRPCHandler<?, RequestType, NodeLocation> buildHandler(
      AsyncRequestContext<?, ?, RequestType, NodeLocation> requestContext,
      int requestId,
      NodeLocation targetNode);

  /**
   * Always call this interface when a Node is restarted or removed.
   *
   * @param endPoint The specific Node
   */
  public void resetClient(TEndPoint endPoint) {
    clientManager.clear(endPoint);
  }

  public Client getAsyncClient(NodeLocation targetNode) throws ClientManagerException {
    return clientManager.borrowClient(nodeLocationToEndPoint(targetNode));
  }
}
