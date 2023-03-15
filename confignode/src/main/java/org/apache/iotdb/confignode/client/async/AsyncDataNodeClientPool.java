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

package org.apache.iotdb.confignode.client.async;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TFlushReq;
import org.apache.iotdb.common.rpc.thrift.TSetTTLReq;
import org.apache.iotdb.commons.client.ClientPoolFactory;
import org.apache.iotdb.commons.client.IClientManager;
import org.apache.iotdb.commons.client.async.AsyncDataNodeInternalServiceClient;
import org.apache.iotdb.commons.client.exception.ClientManagerException;
import org.apache.iotdb.confignode.client.DataNodeRequestType;
import org.apache.iotdb.confignode.client.async.handlers.AsyncClientHandler;
import org.apache.iotdb.confignode.client.async.handlers.rpc.AsyncTSStatusRPCHandler;
import org.apache.iotdb.confignode.client.async.handlers.rpc.CountPathsUsingTemplateRPCHandler;
import org.apache.iotdb.confignode.client.async.handlers.rpc.DeleteSchemaRPCHandler;
import org.apache.iotdb.confignode.client.async.handlers.rpc.FetchSchemaBlackListRPCHandler;
import org.apache.iotdb.mpp.rpc.thrift.TActiveTriggerInstanceReq;
import org.apache.iotdb.mpp.rpc.thrift.TConstructSchemaBlackListReq;
import org.apache.iotdb.mpp.rpc.thrift.TConstructSchemaBlackListWithTemplateReq;
import org.apache.iotdb.mpp.rpc.thrift.TCountPathsUsingTemplateReq;
import org.apache.iotdb.mpp.rpc.thrift.TCreateDataRegionReq;
import org.apache.iotdb.mpp.rpc.thrift.TCreateFunctionInstanceReq;
import org.apache.iotdb.mpp.rpc.thrift.TCreatePipeOnDataNodeReq;
import org.apache.iotdb.mpp.rpc.thrift.TCreateSchemaRegionReq;
import org.apache.iotdb.mpp.rpc.thrift.TCreateTriggerInstanceReq;
import org.apache.iotdb.mpp.rpc.thrift.TDeactivateTemplateReq;
import org.apache.iotdb.mpp.rpc.thrift.TDeleteDataForDeleteSchemaReq;
import org.apache.iotdb.mpp.rpc.thrift.TDeleteTimeSeriesReq;
import org.apache.iotdb.mpp.rpc.thrift.TDropFunctionInstanceReq;
import org.apache.iotdb.mpp.rpc.thrift.TDropTriggerInstanceReq;
import org.apache.iotdb.mpp.rpc.thrift.TFetchSchemaBlackListReq;
import org.apache.iotdb.mpp.rpc.thrift.TInactiveTriggerInstanceReq;
import org.apache.iotdb.mpp.rpc.thrift.TInvalidateMatchedSchemaCacheReq;
import org.apache.iotdb.mpp.rpc.thrift.TOperatePipeOnDataNodeReq;
import org.apache.iotdb.mpp.rpc.thrift.TRegionLeaderChangeReq;
import org.apache.iotdb.mpp.rpc.thrift.TRegionRouteReq;
import org.apache.iotdb.mpp.rpc.thrift.TRollbackSchemaBlackListReq;
import org.apache.iotdb.mpp.rpc.thrift.TRollbackSchemaBlackListWithTemplateReq;
import org.apache.iotdb.mpp.rpc.thrift.TUpdateConfigNodeGroupReq;
import org.apache.iotdb.mpp.rpc.thrift.TUpdateTemplateReq;
import org.apache.iotdb.mpp.rpc.thrift.TUpdateTriggerLocationReq;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Asynchronously send RPC requests to DataNodes. See mpp.thrift for more details. */
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
   */
  public void sendAsyncRequestToDataNodeWithRetry(AsyncClientHandler<?, ?> clientHandler) {
    sendAsyncRequest(clientHandler, MAX_RETRY_NUM);
  }

  public void sendAsyncRequestToDataNodeWithRetry(
      AsyncClientHandler<?, ?> clientHandler, int retryNum) {
    sendAsyncRequest(clientHandler, retryNum);
  }

  private void sendAsyncRequest(AsyncClientHandler<?, ?> clientHandler, int retryNum) {
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
        clientHandler.getCountDownLatch().await();
      } catch (InterruptedException e) {
        LOGGER.error("Interrupted during {} on ConfigNode", requestType);
        Thread.currentThread().interrupt();
      }

      // Check if there is a DataNode that fails to execute the request, and retry if there exists
      if (clientHandler.getRequestIndices().isEmpty()) {
        return;
      }
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

      switch (clientHandler.getRequestType()) {
        case SET_TTL:
          client.setTTL(
              (TSetTTLReq) clientHandler.getRequest(requestId),
              (AsyncTSStatusRPCHandler)
                  clientHandler.createAsyncRPCHandler(requestId, targetDataNode));
          break;
        case CREATE_DATA_REGION:
          client.createDataRegion(
              (TCreateDataRegionReq) clientHandler.getRequest(requestId),
              (AsyncTSStatusRPCHandler)
                  clientHandler.createAsyncRPCHandler(requestId, targetDataNode));
          break;
        case DELETE_REGION:
          client.deleteRegion(
              (TConsensusGroupId) clientHandler.getRequest(requestId),
              (AsyncTSStatusRPCHandler)
                  clientHandler.createAsyncRPCHandler(requestId, targetDataNode));
          break;
        case CREATE_SCHEMA_REGION:
          client.createSchemaRegion(
              (TCreateSchemaRegionReq) clientHandler.getRequest(requestId),
              (AsyncTSStatusRPCHandler)
                  clientHandler.createAsyncRPCHandler(requestId, targetDataNode));
          break;
        case CREATE_FUNCTION:
          client.createFunction(
              (TCreateFunctionInstanceReq) clientHandler.getRequest(requestId),
              (AsyncTSStatusRPCHandler)
                  clientHandler.createAsyncRPCHandler(requestId, targetDataNode));
          break;
        case DROP_FUNCTION:
          client.dropFunction(
              (TDropFunctionInstanceReq) clientHandler.getRequest(requestId),
              (AsyncTSStatusRPCHandler)
                  clientHandler.createAsyncRPCHandler(requestId, targetDataNode));
          break;
        case CREATE_TRIGGER_INSTANCE:
          client.createTriggerInstance(
              (TCreateTriggerInstanceReq) clientHandler.getRequest(requestId),
              (AsyncTSStatusRPCHandler)
                  clientHandler.createAsyncRPCHandler(requestId, targetDataNode));
          break;
        case DROP_TRIGGER_INSTANCE:
          client.dropTriggerInstance(
              (TDropTriggerInstanceReq) clientHandler.getRequest(requestId),
              (AsyncTSStatusRPCHandler)
                  clientHandler.createAsyncRPCHandler(requestId, targetDataNode));
          break;
        case ACTIVE_TRIGGER_INSTANCE:
          client.activeTriggerInstance(
              (TActiveTriggerInstanceReq) clientHandler.getRequest(requestId),
              (AsyncTSStatusRPCHandler)
                  clientHandler.createAsyncRPCHandler(requestId, targetDataNode));
          break;
        case INACTIVE_TRIGGER_INSTANCE:
          client.inactiveTriggerInstance(
              (TInactiveTriggerInstanceReq) clientHandler.getRequest(requestId),
              (AsyncTSStatusRPCHandler)
                  clientHandler.createAsyncRPCHandler(requestId, targetDataNode));
          break;
        case UPDATE_TRIGGER_LOCATION:
          client.updateTriggerLocation(
              (TUpdateTriggerLocationReq) clientHandler.getRequest(requestId),
              (AsyncTSStatusRPCHandler)
                  clientHandler.createAsyncRPCHandler(requestId, targetDataNode));
          break;
        case MERGE:
        case FULL_MERGE:
          client.merge(
              (AsyncTSStatusRPCHandler)
                  clientHandler.createAsyncRPCHandler(requestId, targetDataNode));
          break;
        case FLUSH:
          client.flush(
              (TFlushReq) clientHandler.getRequest(requestId),
              (AsyncTSStatusRPCHandler)
                  clientHandler.createAsyncRPCHandler(requestId, targetDataNode));
          break;
        case CLEAR_CACHE:
          client.clearCache(
              (AsyncTSStatusRPCHandler)
                  clientHandler.createAsyncRPCHandler(requestId, targetDataNode));
          break;
        case LOAD_CONFIGURATION:
          client.loadConfiguration(
              (AsyncTSStatusRPCHandler)
                  clientHandler.createAsyncRPCHandler(requestId, targetDataNode));
          break;
        case SET_SYSTEM_STATUS:
          client.setSystemStatus(
              (String) clientHandler.getRequest(requestId),
              (AsyncTSStatusRPCHandler)
                  clientHandler.createAsyncRPCHandler(requestId, targetDataNode));
          break;
        case UPDATE_REGION_ROUTE_MAP:
          client.updateRegionCache(
              (TRegionRouteReq) clientHandler.getRequest(requestId),
              (AsyncTSStatusRPCHandler)
                  clientHandler.createAsyncRPCHandler(requestId, targetDataNode));
          break;
        case CHANGE_REGION_LEADER:
          client.changeRegionLeader(
              (TRegionLeaderChangeReq) clientHandler.getRequest(requestId),
              (AsyncTSStatusRPCHandler)
                  clientHandler.createAsyncRPCHandler(requestId, targetDataNode));
          break;
        case BROADCAST_LATEST_CONFIG_NODE_GROUP:
          client.updateConfigNodeGroup(
              (TUpdateConfigNodeGroupReq) clientHandler.getRequest(requestId),
              (AsyncTSStatusRPCHandler)
                  clientHandler.createAsyncRPCHandler(requestId, targetDataNode));
          break;
        case CONSTRUCT_SCHEMA_BLACK_LIST:
          client.constructSchemaBlackList(
              (TConstructSchemaBlackListReq) clientHandler.getRequest(requestId),
              (DeleteSchemaRPCHandler)
                  clientHandler.createAsyncRPCHandler(requestId, targetDataNode));
          break;
        case ROLLBACK_SCHEMA_BLACK_LIST:
          client.rollbackSchemaBlackList(
              (TRollbackSchemaBlackListReq) clientHandler.getRequest(requestId),
              (DeleteSchemaRPCHandler)
                  clientHandler.createAsyncRPCHandler(requestId, targetDataNode));
          break;
        case FETCH_SCHEMA_BLACK_LIST:
          client.fetchSchemaBlackList(
              (TFetchSchemaBlackListReq) clientHandler.getRequest(requestId),
              (FetchSchemaBlackListRPCHandler)
                  clientHandler.createAsyncRPCHandler(requestId, targetDataNode));
          break;
        case INVALIDATE_MATCHED_SCHEMA_CACHE:
          client.invalidateMatchedSchemaCache(
              (TInvalidateMatchedSchemaCacheReq) clientHandler.getRequest(requestId),
              (AsyncTSStatusRPCHandler)
                  clientHandler.createAsyncRPCHandler(requestId, targetDataNode));
          break;
        case DELETE_DATA_FOR_DELETE_SCHEMA:
          client.deleteDataForDeleteSchema(
              (TDeleteDataForDeleteSchemaReq) clientHandler.getRequest(requestId),
              (DeleteSchemaRPCHandler)
                  clientHandler.createAsyncRPCHandler(requestId, targetDataNode));
          break;
        case DELETE_TIMESERIES:
          client.deleteTimeSeries(
              (TDeleteTimeSeriesReq) clientHandler.getRequest(requestId),
              (DeleteSchemaRPCHandler)
                  clientHandler.createAsyncRPCHandler(requestId, targetDataNode));
          break;
        case PRE_CREATE_PIPE:
          client.createPipeOnDataNode(
              (TCreatePipeOnDataNodeReq) clientHandler.getRequest(requestId),
              (AsyncTSStatusRPCHandler)
                  clientHandler.createAsyncRPCHandler(requestId, targetDataNode));
          break;
        case OPERATE_PIPE:
          client.operatePipeOnDataNode(
              (TOperatePipeOnDataNodeReq) clientHandler.getRequest(requestId),
              (AsyncTSStatusRPCHandler)
                  clientHandler.createAsyncRPCHandler(requestId, targetDataNode));
          break;
        case ROLLBACK_OPERATE_PIPE:
          client.operatePipeOnDataNodeForRollback(
              (TOperatePipeOnDataNodeReq) clientHandler.getRequest(requestId),
              (AsyncTSStatusRPCHandler)
                  clientHandler.createAsyncRPCHandler(requestId, targetDataNode));
          break;
        case CONSTRUCT_SCHEMA_BLACK_LIST_WITH_TEMPLATE:
          client.constructSchemaBlackListWithTemplate(
              (TConstructSchemaBlackListWithTemplateReq) clientHandler.getRequest(requestId),
              (DeleteSchemaRPCHandler)
                  clientHandler.createAsyncRPCHandler(requestId, targetDataNode));
          break;
        case ROLLBACK_SCHEMA_BLACK_LIST_WITH_TEMPLATE:
          client.rollbackSchemaBlackListWithTemplate(
              (TRollbackSchemaBlackListWithTemplateReq) clientHandler.getRequest(requestId),
              (DeleteSchemaRPCHandler)
                  clientHandler.createAsyncRPCHandler(requestId, targetDataNode));
          break;
        case DEACTIVATE_TEMPLATE:
          client.deactivateTemplate(
              (TDeactivateTemplateReq) clientHandler.getRequest(requestId),
              (DeleteSchemaRPCHandler)
                  clientHandler.createAsyncRPCHandler(requestId, targetDataNode));
          break;
        case UPDATE_TEMPLATE:
          client.updateTemplate(
              (TUpdateTemplateReq) clientHandler.getRequest(requestId),
              (AsyncTSStatusRPCHandler)
                  clientHandler.createAsyncRPCHandler(requestId, targetDataNode));
          break;
        case COUNT_PATHS_USING_TEMPLATE:
          client.countPathsUsingTemplate(
              (TCountPathsUsingTemplateReq) clientHandler.getRequest(requestId),
              (CountPathsUsingTemplateRPCHandler)
                  clientHandler.createAsyncRPCHandler(requestId, targetDataNode));
          break;
        case KILL_QUERY_INSTANCE:
          client.killQueryInstance(
              (String) clientHandler.getRequest(requestId),
              (AsyncTSStatusRPCHandler)
                  clientHandler.createAsyncRPCHandler(requestId, targetDataNode));
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
