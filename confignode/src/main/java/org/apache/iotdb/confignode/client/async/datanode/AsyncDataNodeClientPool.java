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
package org.apache.iotdb.confignode.client.async.datanode;

import org.apache.iotdb.common.rpc.thrift.TConfigNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TFlushReq;
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.common.rpc.thrift.TSetTTLReq;
import org.apache.iotdb.commons.client.ClientPoolFactory;
import org.apache.iotdb.commons.client.IClientManager;
import org.apache.iotdb.commons.client.async.AsyncDataNodeInternalServiceClient;
import org.apache.iotdb.confignode.client.DataNodeRequestType;
import org.apache.iotdb.confignode.client.async.handlers.AbstractRetryHandler;
import org.apache.iotdb.confignode.client.async.handlers.ClearCacheHandler;
import org.apache.iotdb.confignode.client.async.handlers.CreateRegionHandler;
import org.apache.iotdb.confignode.client.async.handlers.FlushHandler;
import org.apache.iotdb.confignode.client.async.handlers.FunctionManagementHandler;
import org.apache.iotdb.confignode.client.async.handlers.LoadConfigurationHandler;
import org.apache.iotdb.confignode.client.async.handlers.MergeHandler;
import org.apache.iotdb.confignode.client.async.handlers.SetTTLHandler;
import org.apache.iotdb.confignode.client.async.handlers.UpdateConfigNodeGroupHandler;
import org.apache.iotdb.confignode.client.async.handlers.UpdateRegionRouteMapHandler;
import org.apache.iotdb.confignode.consensus.request.write.CreateRegionGroupsPlan;
import org.apache.iotdb.mpp.rpc.thrift.TCreateDataRegionReq;
import org.apache.iotdb.mpp.rpc.thrift.TCreateFunctionRequest;
import org.apache.iotdb.mpp.rpc.thrift.TCreateSchemaRegionReq;
import org.apache.iotdb.mpp.rpc.thrift.TDropFunctionRequest;
import org.apache.iotdb.mpp.rpc.thrift.TRegionRouteReq;
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

  private final int retryNum = 6;

  private AsyncDataNodeClientPool() {
    clientManager =
        new IClientManager.Factory<TEndPoint, AsyncDataNodeInternalServiceClient>()
            .createClientManager(
                new ClientPoolFactory.AsyncDataNodeInternalServiceClientPoolFactory());
  }

  /**
   * Send asynchronize requests to the specific DataNodes, and reconnect the DataNode that failed to
   * receive the requests
   *
   * @param req request
   * @param dataNodeLocationMap Map<DataNodeId, TDataNodeLocation>
   * @param requestType DataNodeRequestType
   * @param dataNodeResponseStatus response list.Used by CREATE_FUNCTION,DROP_FUNCTION and FLUSH
   */
  public void sendAsyncRequestToDataNodeWithRetry(
      Object req,
      Map<Integer, TDataNodeLocation> dataNodeLocationMap,
      DataNodeRequestType requestType,
      List<TSStatus> dataNodeResponseStatus) {
    if (dataNodeLocationMap.isEmpty()) {
      return;
    }
    for (int retry = 0; retry < retryNum; retry++) {
      CountDownLatch countDownLatch = new CountDownLatch(dataNodeLocationMap.size());
      for (TDataNodeLocation targetDataNode : dataNodeLocationMap.values()) {
        AbstractRetryHandler handler;
        switch (requestType) {
          case SET_TTL:
            handler =
                new SetTTLHandler(countDownLatch, requestType, targetDataNode, dataNodeLocationMap);
            break;
          case CREATE_FUNCTION:
          case DROP_FUNCTION:
            handler =
                new FunctionManagementHandler(
                    countDownLatch,
                    requestType,
                    targetDataNode,
                    dataNodeLocationMap,
                    dataNodeResponseStatus);
            break;
          case FULL_MERGE:
          case MERGE:
            handler =
                new MergeHandler(
                    countDownLatch,
                    requestType,
                    targetDataNode,
                    dataNodeLocationMap,
                    dataNodeResponseStatus);
            break;
          case FLUSH:
            handler =
                new FlushHandler(
                    countDownLatch,
                    requestType,
                    targetDataNode,
                    dataNodeLocationMap,
                    dataNodeResponseStatus);
            break;
          case CLEAR_CACHE:
            handler =
                new ClearCacheHandler(
                    countDownLatch,
                    requestType,
                    targetDataNode,
                    dataNodeLocationMap,
                    dataNodeResponseStatus);
            break;
          case LOAD_CONFIGURATION:
            handler =
                new LoadConfigurationHandler(
                    countDownLatch,
                    requestType,
                    targetDataNode,
                    dataNodeLocationMap,
                    dataNodeResponseStatus);
            break;
          case UPDATE_REGION_ROUTE_MAP:
            handler =
                new UpdateRegionRouteMapHandler(
                    countDownLatch, requestType, targetDataNode, dataNodeLocationMap);
            break;
          case BROADCAST_LATEST_CONFIG_NODE_GROUP:
            handler =
                new UpdateConfigNodeGroupHandler(
                    countDownLatch, requestType, targetDataNode, dataNodeLocationMap);
            break;
          default:
            return;
        }
        sendAsyncRequestToDataNode(targetDataNode, req, handler, retry);
      }
      try {
        countDownLatch.await();
      } catch (InterruptedException e) {
        LOGGER.error("Interrupted during {} on ConfigNode", requestType);
      }
      // Check if there is a node that fails to send the request, and retry if there is one
      if (dataNodeLocationMap.isEmpty()) {
        break;
      }
    }
  }

  public void sendAsyncRequestToDataNode(
      TDataNodeLocation dataNodeLocation,
      Object req,
      AbstractRetryHandler handler,
      int retryCount) {
    AsyncDataNodeInternalServiceClient client;
    try {
      client = clientManager.borrowClient(dataNodeLocation.getInternalEndPoint());
      switch (handler.getDataNodeRequestType()) {
        case SET_TTL:
          client.setTTL((TSetTTLReq) req, (SetTTLHandler) handler);
          break;
        case CREATE_DATA_REGIONS:
          client.createDataRegion((TCreateDataRegionReq) req, (CreateRegionHandler) handler);
          break;
        case CREATE_SCHEMA_REGIONS:
          client.createSchemaRegion((TCreateSchemaRegionReq) req, (CreateRegionHandler) handler);
          break;
        case CREATE_FUNCTION:
          client.createFunction((TCreateFunctionRequest) req, (FunctionManagementHandler) handler);
          break;
        case DROP_FUNCTION:
          client.dropFunction((TDropFunctionRequest) req, (FunctionManagementHandler) handler);
          break;
        case MERGE:
        case FULL_MERGE:
          client.merge((MergeHandler) handler);
          break;
        case FLUSH:
          client.flush((TFlushReq) req, (FlushHandler) handler);
          break;
        case CLEAR_CACHE:
          client.clearCache((ClearCacheHandler) handler);
          break;
        case LOAD_CONFIGURATION:
          client.loadConfiguration((LoadConfigurationHandler) handler);
          break;
        case UPDATE_REGION_ROUTE_MAP:
          client.updateRegionCache((TRegionRouteReq) req, (UpdateRegionRouteMapHandler) handler);
          break;
        case BROADCAST_LATEST_CONFIG_NODE_GROUP:
          client.updateConfigNodeGroup(
              (TUpdateConfigNodeGroupReq) req, (UpdateConfigNodeGroupHandler) handler);
          break;
        default:
          LOGGER.error(
              "Unexpected DataNode Request Type: {} when sendAsyncRequestToDataNode",
              handler.getDataNodeRequestType());
      }
    } catch (Exception e) {
      LOGGER.warn(
          "{} failed on ConfigNode {}, because {}, retrying {}...",
          handler.getDataNodeRequestType(),
          dataNodeLocation.getInternalEndPoint(),
          e.getMessage(),
          retryCount);
    }
  }

  /**
   * Execute CreateRegionGroupsPlan asynchronously
   *
   * @param ttlMap Map<StorageGroupName, TTL>
   * @return Those RegionGroups that failed to create
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
    for (int retry = 0; retry < retryNum; retry++) {
      index = 0;
      CountDownLatch countDownLatch = new CountDownLatch(dataNodeLocationMap.size());
      for (Map.Entry<String, List<TRegionReplicaSet>> entry :
          createRegionGroupsPlan.getRegionGroupMap().entrySet()) {
        // Enumerate each RegionReplicaSet
        for (TRegionReplicaSet regionReplicaSet : entry.getValue()) {
          // Enumerate each Region
          for (TDataNodeLocation targetDataNode : regionReplicaSet.getDataNodeLocations()) {
            if (dataNodeLocationMap.containsKey(index)) {
              AbstractRetryHandler handler;
              switch (regionReplicaSet.getRegionId().getType()) {
                case SchemaRegion:
                  handler =
                      new CreateRegionHandler(
                          countDownLatch,
                          DataNodeRequestType.CREATE_SCHEMA_REGIONS,
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
                          DataNodeRequestType.CREATE_DATA_REGIONS,
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
          DataNodeRequestType.BROADCAST_LATEST_CONFIG_NODE_GROUP,
          null);
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
