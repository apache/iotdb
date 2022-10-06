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

import org.apache.iotdb.common.rpc.thrift.TConfigNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TFlushReq;
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.common.rpc.thrift.TSetTTLReq;
import org.apache.iotdb.commons.client.ClientPoolFactory;
import org.apache.iotdb.commons.client.IClientManager;
import org.apache.iotdb.commons.client.async.AsyncDataNodeInternalServiceClient;
import org.apache.iotdb.confignode.client.DataNodeRequestType;
import org.apache.iotdb.confignode.client.async.handlers.AbstractAsyncRPCHandler;
import org.apache.iotdb.confignode.client.async.handlers.AsyncClientHandler;
import org.apache.iotdb.confignode.client.async.handlers.AsyncTSStatusRPCHandler;
import org.apache.iotdb.confignode.client.async.handlers.ConstructSchemaBlackListHandler;
import org.apache.iotdb.confignode.client.async.handlers.CreateRegionHandler;
import org.apache.iotdb.confignode.client.async.handlers.DeleteDataForDeleteTimeSeriesHandler;
import org.apache.iotdb.confignode.client.async.handlers.DeleteTimeSeriesHandler;
import org.apache.iotdb.confignode.client.async.handlers.FetchSchemaBlackListHandler;
import org.apache.iotdb.confignode.client.async.handlers.InvalidateMatchedSchemaCacheHandler;
import org.apache.iotdb.confignode.consensus.request.write.region.CreateRegionGroupsPlan;
import org.apache.iotdb.mpp.rpc.thrift.TActiveTriggerInstanceReq;
import org.apache.iotdb.mpp.rpc.thrift.TConstructSchemaBlackListReq;
import org.apache.iotdb.mpp.rpc.thrift.TCreateDataRegionReq;
import org.apache.iotdb.mpp.rpc.thrift.TCreateFunctionRequest;
import org.apache.iotdb.mpp.rpc.thrift.TCreateSchemaRegionReq;
import org.apache.iotdb.mpp.rpc.thrift.TCreateTriggerInstanceReq;
import org.apache.iotdb.mpp.rpc.thrift.TDeleteDataForDeleteTimeSeriesReq;
import org.apache.iotdb.mpp.rpc.thrift.TDeleteTimeSeriesReq;
import org.apache.iotdb.mpp.rpc.thrift.TDropFunctionRequest;
import org.apache.iotdb.mpp.rpc.thrift.TDropTriggerInstanceReq;
import org.apache.iotdb.mpp.rpc.thrift.TFetchSchemaBlackListReq;
import org.apache.iotdb.mpp.rpc.thrift.TFetchSchemaBlackListResp;
import org.apache.iotdb.mpp.rpc.thrift.TInactiveTriggerInstanceReq;
import org.apache.iotdb.mpp.rpc.thrift.TInvalidateMatchedSchemaCacheReq;
import org.apache.iotdb.mpp.rpc.thrift.TRegionRouteReq;
import org.apache.iotdb.mpp.rpc.thrift.TRollbackSchemaBlackListReq;
import org.apache.iotdb.mpp.rpc.thrift.TUpdateConfigNodeGroupReq;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;

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
   * Send asynchronous requests to the specific DataNodes, and reconnect the DataNode that failed to
   * receive the requests
   */
  public void sendAsyncRequestToDataNodeWithRetry(AsyncClientHandler<?, ?> clientHandler) {
    if (clientHandler.getRequestIndices().isEmpty()) {
      return;
    }

    DataNodeRequestType requestType = clientHandler.getRequestType();
    for (int retry = 0; retry < MAX_RETRY_NUM; retry++) {
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
        case CREATE_SCHEMA_REGION:
          client.createSchemaRegion(
              (TCreateSchemaRegionReq) clientHandler.getRequest(requestId),
              (AsyncTSStatusRPCHandler)
                  clientHandler.createAsyncRPCHandler(requestId, targetDataNode));
          break;
        case CREATE_FUNCTION:
          client.createFunction(
              (TCreateFunctionRequest) clientHandler.getRequest(requestId),
              (AsyncTSStatusRPCHandler)
                  clientHandler.createAsyncRPCHandler(requestId, targetDataNode));
          break;
        case DROP_FUNCTION:
          client.dropFunction(
              (TDropFunctionRequest) clientHandler.getRequest(requestId),
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
        case BROADCAST_LATEST_CONFIG_NODE_GROUP:
          client.updateConfigNodeGroup(
              (TUpdateConfigNodeGroupReq) clientHandler.getRequest(requestId),
              (AsyncTSStatusRPCHandler)
                  clientHandler.createAsyncRPCHandler(requestId, targetDataNode));
          break;
        case CONSTRUCT_SCHEMA_BLACK_LIST:
          client.constructSchemaBlackList(
              (TConstructSchemaBlackListReq) req, (ConstructSchemaBlackListHandler) handler);
          break;
        case ROLLBACK_SCHEMA_BLACK_LIST:
          client.rollbackSchemaBlackList(
              (TRollbackSchemaBlackListReq) clientHandler.getRequest(requestId),
              (AsyncTSStatusRPCHandler)
                  clientHandler.createAsyncRPCHandler(requestId, targetDataNode));
          break;
        case FETCH_SCHEMA_BLACK_LIST:
          FetchSchemaBlackListHandler fetchSchemaBlackListHandler =
              new FetchSchemaBlackListHandler(
                  requestType,
                  targetDataNode,
                  dataNodeLocationMap,
                  (Map<Integer, TFetchSchemaBlackListResp>) responseMap,
                  countDownLatch);
          client.fetchSchemaBlackList(
              (TFetchSchemaBlackListReq) req, (FetchSchemaBlackListHandler) handler);
          break;
        case INVALIDATE_MATCHED_SCHEMA_CACHE:
          client.invalidateMatchedSchemaCache(
              (TInvalidateMatchedSchemaCacheReq) req,
              (InvalidateMatchedSchemaCacheHandler) handler);
          break;
        case DELETE_DATA_FOR_DELETE_TIMESERIES:
          client.deleteDataForDeleteTimeSeries(
              (TDeleteDataForDeleteTimeSeriesReq) req,
              (DeleteDataForDeleteTimeSeriesHandler) handler);
          break;
        case DELETE_TIMESERIES:
          client.deleteTimeSeries((TDeleteTimeSeriesReq) req, (DeleteTimeSeriesHandler) handler);
          break;
        default:
          LOGGER.error(
              "Unexpected DataNode Request Type: {} when sendAsyncRequestToDataNode", requestType);
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
   * Execute CreateRegionGroupsPlan asynchronously
   *
   * @param ttlMap Map<StorageGroupName, TTL>
   * @return Those RegionReplicas that failed to create
   */
  public Map<TConsensusGroupId, TRegionReplicaSet> createRegionGroups(
      CreateRegionGroupsPlan createRegionGroupsPlan, Map<String, Long> ttlMap) {
    // Because different requests will be sent to the same node when createRegions,
    // so for CreateRegions use Map<index, TDataNodeLocation>
    Map<Integer, TDataNodeLocation> dataNodeLocationMap = new ConcurrentHashMap<>();
    int index = 0;
    // Count the DataNodes to be sent
    for (List<TRegionReplicaSet> regionReplicaSets :
        createRegionGroupsPlan.getRegionGroupMap().values()) {
      for (TRegionReplicaSet regionReplicaSet : regionReplicaSets) {
        for (TDataNodeLocation dataNodeLocation : regionReplicaSet.getDataNodeLocations()) {
          dataNodeLocationMap.put(index++, dataNodeLocation);
        }
      }
    }
    if (dataNodeLocationMap.isEmpty()) {
      return new HashMap<>();
    }
    for (int retry = 0; retry < MAX_RETRY_NUM; retry++) {
      index = 0;
      CountDownLatch countDownLatch = new CountDownLatch(dataNodeLocationMap.size());
      for (Map.Entry<String, List<TRegionReplicaSet>> entry :
          createRegionGroupsPlan.getRegionGroupMap().entrySet()) {
        // Enumerate each RegionReplicaSet
        for (TRegionReplicaSet regionReplicaSet : entry.getValue()) {
          // Enumerate each Region
          for (TDataNodeLocation targetDataNode : regionReplicaSet.getDataNodeLocations()) {
            if (dataNodeLocationMap.containsKey(index)) {
              AbstractAsyncRPCHandler handler;
              switch (regionReplicaSet.getRegionId().getType()) {
                case SchemaRegion:
                  handler =
                      new CreateRegionHandler(
                          countDownLatch,
                          DataNodeRequestType.CREATE_SCHEMA_REGION,
                          regionReplicaSet.regionId,
                          targetDataNode,
                          dataNodeLocationMap,
                          index++);
                  sendAsyncRequestToDataNode(
                      targetDataNode,
                      genCreateSchemaRegionReq(entry.getKey(), regionReplicaSet),
                      handler,
                      retry);
                  break;
                case DataRegion:
                  handler =
                      new CreateRegionHandler(
                          countDownLatch,
                          DataNodeRequestType.CREATE_DATA_REGION,
                          regionReplicaSet.regionId,
                          targetDataNode,
                          dataNodeLocationMap,
                          index++);
                  sendAsyncRequestToDataNode(
                      targetDataNode,
                      genCreateDataRegionReq(
                          entry.getKey(), regionReplicaSet, ttlMap.get(entry.getKey())),
                      handler,
                      retry);
                  break;
                default:
                  break;
              }
            } else {
              index++;
            }
          }
        }
      }
      try {
        countDownLatch.await();
      } catch (InterruptedException e) {
        LOGGER.error("Interrupted during createRegions on ConfigNode");
      }
      // Check if there is a node that fails to send the request, and
      // retry if there is one
      if (dataNodeLocationMap.isEmpty()) {
        break;
      }
    }

    // Filter RegionGroups that weren't created successfully
    index = 0;
    Map<TConsensusGroupId, TRegionReplicaSet> failedRegions = new HashMap<>();
    for (List<TRegionReplicaSet> regionReplicaSets :
        createRegionGroupsPlan.getRegionGroupMap().values()) {
      for (TRegionReplicaSet regionReplicaSet : regionReplicaSets) {
        for (TDataNodeLocation dataNodeLocation : regionReplicaSet.getDataNodeLocations()) {
          if (dataNodeLocationMap.containsKey(index)) {
            failedRegions
                .computeIfAbsent(
                    regionReplicaSet.getRegionId(),
                    empty -> new TRegionReplicaSet().setRegionId(regionReplicaSet.getRegionId()))
                .addToDataNodeLocations(dataNodeLocation);
          }
          index += 1;
        }
      }
    }
    return failedRegions;
  }

  private TCreateSchemaRegionReq genCreateSchemaRegionReq(
      String storageGroup, TRegionReplicaSet regionReplicaSet) {
    TCreateSchemaRegionReq req = new TCreateSchemaRegionReq();
    req.setStorageGroup(storageGroup);
    req.setRegionReplicaSet(regionReplicaSet);
    return req;
  }

  private TCreateDataRegionReq genCreateDataRegionReq(
      String storageGroup, TRegionReplicaSet regionReplicaSet, long TTL) {
    TCreateDataRegionReq req = new TCreateDataRegionReq();
    req.setStorageGroup(storageGroup);
    req.setRegionReplicaSet(regionReplicaSet);
    req.setTtl(TTL);
    return req;
  }

  /**
   * notify all DataNodes when the capacity of the ConfigNodeGroup is expanded or reduced
   *
   * @param registeredDataNodeLocationMap Map<Integer, TDataNodeLocation>
   * @param registeredConfigNodes List<TConfigNodeLocation>
   */
  public void broadCastTheLatestConfigNodeGroup(
      Map<Integer, TDataNodeLocation> registeredDataNodeLocationMap,
      List<TConfigNodeLocation> registeredConfigNodes) {
    if (registeredDataNodeLocationMap != null) {
      TUpdateConfigNodeGroupReq updateConfigNodeGroupReq =
          new TUpdateConfigNodeGroupReq(registeredConfigNodes);
      LOGGER.info("Begin to broadcast the latest configNodeGroup: {}", registeredConfigNodes);
      sendAsyncRequestToDataNodeWithRetry(
          updateConfigNodeGroupReq,
          registeredDataNodeLocationMap,
          DataNodeRequestType.BROADCAST_LATEST_CONFIG_NODE_GROUP);
      LOGGER.info("Broadcast the latest configNodeGroup finished.");
    }
  }

  /**
   * Always call this interface when a DataNode is restarted or removed
   *
   * @param endPoint The specific DataNode
   */
  public void resetClient(TEndPoint endPoint) {
    clientManager.clear(endPoint);
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
