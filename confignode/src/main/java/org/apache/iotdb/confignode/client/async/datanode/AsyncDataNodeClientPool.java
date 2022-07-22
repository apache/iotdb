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

import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TFlushReq;
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.client.IClientManager;
import org.apache.iotdb.commons.client.async.AsyncDataNodeInternalServiceClient;
import org.apache.iotdb.confignode.client.ConfigNodeClientPoolFactory;
import org.apache.iotdb.confignode.client.DataNodeRequestType;
import org.apache.iotdb.confignode.client.async.handlers.AbstractRetryHandler;
import org.apache.iotdb.confignode.client.async.handlers.CreateRegionHandler;
import org.apache.iotdb.confignode.client.async.handlers.DataNodeHeartbeatHandler;
import org.apache.iotdb.confignode.client.async.handlers.FlushHandler;
import org.apache.iotdb.confignode.client.async.handlers.FunctionManagementHandler;
import org.apache.iotdb.confignode.client.async.handlers.SetTTLHandler;
import org.apache.iotdb.confignode.client.async.handlers.UpdateRegionRouteMapHandler;
import org.apache.iotdb.confignode.consensus.request.write.CreateRegionGroupsPlan;
import org.apache.iotdb.mpp.rpc.thrift.TCreateDataRegionReq;
import org.apache.iotdb.mpp.rpc.thrift.TCreateFunctionRequest;
import org.apache.iotdb.mpp.rpc.thrift.TCreateSchemaRegionReq;
import org.apache.iotdb.mpp.rpc.thrift.TDropFunctionRequest;
import org.apache.iotdb.mpp.rpc.thrift.THeartbeatReq;
import org.apache.iotdb.mpp.rpc.thrift.TRegionRouteReq;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
                new ConfigNodeClientPoolFactory.AsyncDataNodeInternalServiceClientPoolFactory());
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
      AbstractRetryHandler handler;
      for (TDataNodeLocation targetDataNode : dataNodeLocationMap.values()) {
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
          case FLUSH:
            handler =
                new FlushHandler(
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
        case FLUSH:
          client.flush((TFlushReq) req, (FlushHandler) handler);
          break;
        case UPDATE_REGION_ROUTE_MAP:
          client.updateRegionCache((TRegionRouteReq) req, (UpdateRegionRouteMapHandler) handler);
          break;
        default:
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
   * Execute CreateRegionsReq asynchronously
   *
   * @param createRegionGroupsPlan CreateRegionsReq
   * @param ttlMap Map<StorageGroupName, TTL>
   */
  public void createRegions(
      CreateRegionGroupsPlan createRegionGroupsPlan, Map<String, Long> ttlMap) {
    // Map<index, TDataNodeLocation>
    Map<Integer, TDataNodeLocation> dataNodeLocations = new ConcurrentHashMap<>();
    int index = 0;
    // Count the datanodes to be sent
    for (List<TRegionReplicaSet> regionReplicaSets :
        createRegionGroupsPlan.getRegionGroupMap().values()) {
      for (TRegionReplicaSet regionReplicaSet : regionReplicaSets) {
        for (TDataNodeLocation dataNodeLocation : regionReplicaSet.getDataNodeLocations()) {
          dataNodeLocations.put(index++, dataNodeLocation);
        }
      }
    }
    if (dataNodeLocations.isEmpty()) {
      return;
    }
    for (int retry = 0; retry < retryNum; retry++) {
      index = 0;
      CountDownLatch countDownLatch = new CountDownLatch(dataNodeLocations.size());
      AbstractRetryHandler handler;
      for (Map.Entry<String, List<TRegionReplicaSet>> entry :
          createRegionGroupsPlan.getRegionGroupMap().entrySet()) {
        // Enumerate each RegionReplicaSet
        for (TRegionReplicaSet regionReplicaSet : entry.getValue()) {
          // Enumerate each Region
          for (TDataNodeLocation targetDataNode : regionReplicaSet.getDataNodeLocations()) {
            if (dataNodeLocations.containsKey(index)) {
              switch (regionReplicaSet.getRegionId().getType()) {
                case SchemaRegion:
                  handler =
                      new CreateRegionHandler(
                          countDownLatch,
                          DataNodeRequestType.CREATE_SCHEMA_REGIONS,
                          regionReplicaSet.regionId,
                          targetDataNode,
                          dataNodeLocations,
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
                          dataNodeLocations,
                          index++);
                  sendAsyncRequestToDataNode(
                      targetDataNode,
                      genCreateDataRegionReq(
                          entry.getKey(), regionReplicaSet, ttlMap.get(entry.getKey())),
                      handler,
                      retry);
                  break;
                default:
                  return;
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
      if (dataNodeLocations.isEmpty()) {
        break;
      }
    }
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
   * Only used in LoadManager
   *
   * @param endPoint The specific DataNode
   */
  public void getDataNodeHeartBeat(
      TEndPoint endPoint, THeartbeatReq req, DataNodeHeartbeatHandler handler) {
    // TODO: Add a retry logic
    AsyncDataNodeInternalServiceClient client;
    try {
      client = clientManager.borrowClient(endPoint);
      client.getDataNodeHeartBeat(req, handler);
    } catch (Exception e) {
      LOGGER.error("Asking DataNode: {}, for heartbeat failed", endPoint, e);
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
