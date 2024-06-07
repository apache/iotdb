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

package org.apache.iotdb.commons.client.gg;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TFlushReq;
import org.apache.iotdb.common.rpc.thrift.TNodeLocations;
import org.apache.iotdb.common.rpc.thrift.TSetSpaceQuotaReq;
import org.apache.iotdb.common.rpc.thrift.TSetTTLReq;
import org.apache.iotdb.common.rpc.thrift.TSetThrottleQuotaReq;
import org.apache.iotdb.commons.client.ClientPoolFactory;
import org.apache.iotdb.commons.client.IClientManager;
import org.apache.iotdb.commons.client.exception.ClientManagerException;
import org.apache.iotdb.mpp.rpc.thrift.TActiveTriggerInstanceReq;
import org.apache.iotdb.mpp.rpc.thrift.TAlterViewReq;
import org.apache.iotdb.mpp.rpc.thrift.TCheckSchemaRegionUsingTemplateReq;
import org.apache.iotdb.mpp.rpc.thrift.TCheckTimeSeriesExistenceReq;
import org.apache.iotdb.mpp.rpc.thrift.TConstructSchemaBlackListReq;
import org.apache.iotdb.mpp.rpc.thrift.TConstructSchemaBlackListWithTemplateReq;
import org.apache.iotdb.mpp.rpc.thrift.TConstructViewSchemaBlackListReq;
import org.apache.iotdb.mpp.rpc.thrift.TCountPathsUsingTemplateReq;
import org.apache.iotdb.mpp.rpc.thrift.TCreateDataRegionReq;
import org.apache.iotdb.mpp.rpc.thrift.TCreateFunctionInstanceReq;
import org.apache.iotdb.mpp.rpc.thrift.TCreatePipePluginInstanceReq;
import org.apache.iotdb.mpp.rpc.thrift.TCreateSchemaRegionReq;
import org.apache.iotdb.mpp.rpc.thrift.TCreateTriggerInstanceReq;
import org.apache.iotdb.mpp.rpc.thrift.TDeactivateTemplateReq;
import org.apache.iotdb.mpp.rpc.thrift.TDeleteDataForDeleteSchemaReq;
import org.apache.iotdb.mpp.rpc.thrift.TDeleteTimeSeriesReq;
import org.apache.iotdb.mpp.rpc.thrift.TDeleteViewSchemaReq;
import org.apache.iotdb.mpp.rpc.thrift.TDropFunctionInstanceReq;
import org.apache.iotdb.mpp.rpc.thrift.TDropPipePluginInstanceReq;
import org.apache.iotdb.mpp.rpc.thrift.TDropTriggerInstanceReq;
import org.apache.iotdb.mpp.rpc.thrift.TFetchSchemaBlackListReq;
import org.apache.iotdb.mpp.rpc.thrift.TInactiveTriggerInstanceReq;
import org.apache.iotdb.mpp.rpc.thrift.TInvalidateMatchedSchemaCacheReq;
import org.apache.iotdb.mpp.rpc.thrift.TPipeHeartbeatReq;
import org.apache.iotdb.mpp.rpc.thrift.TPushConsumerGroupMetaReq;
import org.apache.iotdb.mpp.rpc.thrift.TPushMultiPipeMetaReq;
import org.apache.iotdb.mpp.rpc.thrift.TPushMultiTopicMetaReq;
import org.apache.iotdb.mpp.rpc.thrift.TPushPipeMetaReq;
import org.apache.iotdb.mpp.rpc.thrift.TPushSingleConsumerGroupMetaReq;
import org.apache.iotdb.mpp.rpc.thrift.TPushSinglePipeMetaReq;
import org.apache.iotdb.mpp.rpc.thrift.TPushSingleTopicMetaReq;
import org.apache.iotdb.mpp.rpc.thrift.TPushTopicMetaReq;
import org.apache.iotdb.mpp.rpc.thrift.TRegionLeaderChangeReq;
import org.apache.iotdb.mpp.rpc.thrift.TRegionRouteReq;
import org.apache.iotdb.mpp.rpc.thrift.TResetPeerListReq;
import org.apache.iotdb.mpp.rpc.thrift.TRollbackSchemaBlackListReq;
import org.apache.iotdb.mpp.rpc.thrift.TRollbackSchemaBlackListWithTemplateReq;
import org.apache.iotdb.mpp.rpc.thrift.TRollbackViewSchemaBlackListReq;
import org.apache.iotdb.mpp.rpc.thrift.TUpdateTemplateReq;
import org.apache.iotdb.mpp.rpc.thrift.TUpdateTriggerLocationReq;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/** Asynchronously send RPC requests to Nodes. See queryengine.thrift for more details. */
public abstract class AsyncRequestSender<RequestType, NodeLocation, Client> {

  private static final Logger LOGGER = LoggerFactory.getLogger(AsyncRequestSender.class);

  protected IClientManager<TEndPoint, Client> clientManager;

  private static final int MAX_RETRY_NUM = 6;

  protected AsyncRequestSender() {
    initClientManager();
  }

  protected abstract void initClientManager();

  /**
   * Send asynchronous requests to the specified Nodes with default retry num
   *
   * <p>Notice: The Nodes that failed to receive the requests will be reconnected
   *
   * @param requestContext <RequestType, ResponseType> which will also contain the result
   * @param timeoutInMs timeout in milliseconds
   */
  public void sendAsyncRequestToNodeWithRetryAndTimeoutInMs(
          AsyncRequestContext<?,?,RequestType,NodeLocation> requestContext, long timeoutInMs) {
    sendAsyncRequest(requestContext, MAX_RETRY_NUM, timeoutInMs);
  }

  /**
   * Send asynchronous requests to the specified Nodes with default retry num
   *
   * <p>Notice: The Nodes that failed to receive the requests will be reconnected
   *
   * @param requestContext <RequestType, ResponseType> which will also contain the result
   */
  public final void sendAsyncRequestToNodeWithRetry(AsyncRequestContext<?,?,RequestType,NodeLocation> requestContext) {
    sendAsyncRequest(requestContext, MAX_RETRY_NUM, null);
  }

  public final void sendAsyncRequestToNode(AsyncRequestContext<?,?,RequestType,NodeLocation> requestContext) {
    sendAsyncRequest(requestContext, 1, null);
  }

  private final void sendAsyncRequest(
          AsyncRequestContext<?,?,RequestType,NodeLocation> requestContext, int retryNum, Long timeoutInMs) {
    if (requestContext.getRequestIndices().isEmpty()) {
      return;
    }

    RequestType requestType = requestContext.getRequestType();
    for (int retry = 0; retry < retryNum; retry++) {
      // Always Reset CountDownLatch first
      requestContext.resetCountDownLatch();

      // Send requests to all targetNodes
      for (int requestId : requestContext.getRequestIndices()) {
        NodeLocation targetNode = requestContext.getNodeLocation(requestId);
        sendAsyncRequestToNode(requestContext, requestId, targetNode, retry);
      }

      // Wait for this batch of asynchronous RPC requests finish
      try {
        if (timeoutInMs == null) {
          requestContext.getCountDownLatch().await();
        } else {
          if (!requestContext.getCountDownLatch().await(timeoutInMs, TimeUnit.MILLISECONDS)) {
            LOGGER.warn(
                "Timeout during {} on ConfigNode. Retry: {}/{}", requestType, retry, retryNum);
          }
        }
      } catch (InterruptedException e) {
        LOGGER.error(
            "Interrupted during {} on ConfigNode. Retry: {}/{}", requestType, retry, retryNum);
        Thread.currentThread().interrupt();
      }

      // Check if there is a Node that fails to execute the request, and retry if there exists
      if (requestContext.getRequestIndices().isEmpty()) {
        return;
      }
    }

    if (!requestContext.getRequestIndices().isEmpty()) {
      LOGGER.warn(
          "Failed to {} on ConfigNode after {} retries, requestIndices: {}",
          requestType,
          retryNum,
          requestContext.getRequestIndices());
    }
  }
  
  protected abstract TEndPoint nodeLocationToEndPoint(NodeLocation location);

  abstract protected void sendAsyncRequestToNode(
      AsyncRequestContext<?,?,RequestType,NodeLocation> requestContext,
      int requestId,
      NodeLocation targetNode,
      int retryCount);

  /**
   * Always call this interface when a Node is restarted or removed.
   *
   * @param endPoint The specific Node
   */
  public void resetClient(TEndPoint endPoint) {
    clientManager.clear(endPoint);
  }
  

  public Client getAsyncClient(NodeLocation targetNode)
      throws ClientManagerException {
    return clientManager.borrowClient(nodeLocationToEndPoint(targetNode));
  }
}
