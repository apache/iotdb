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
package org.apache.iotdb.confignode.client;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupType;
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TFlushReq;
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.common.rpc.thrift.TSetTTLReq;
import org.apache.iotdb.commons.client.IClientManager;
import org.apache.iotdb.commons.client.async.AsyncDataNodeInternalServiceClient;
import org.apache.iotdb.confignode.client.handlers.AbstractRetryHandler;
import org.apache.iotdb.confignode.client.handlers.CreateRegionHandler;
import org.apache.iotdb.confignode.client.handlers.DataNodeHeartbeatHandler;
import org.apache.iotdb.confignode.client.handlers.FlushHandler;
import org.apache.iotdb.confignode.client.handlers.FunctionManagementHandler;
import org.apache.iotdb.confignode.client.handlers.SetTTLHandler;
import org.apache.iotdb.confignode.client.handlers.UpdateRegionRouteMapHandler;
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
import java.util.concurrent.atomic.AtomicInteger;

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
   * When the request fails, reconnect the DataNode that failed the request
   *
   * @param req request
   * @param handlerMap Map<index, Handler>
   * @param dataNodeLocations ConcurrentHashMap<index, TDataNodeLocation> The specific DataNodes
   */
  public void sendAsyncRequestToDataNodeWithRetry(
      Object req,
      Object handlerMap,
      ConcurrentHashMap<Integer, TDataNodeLocation> dataNodeLocations) {
    CountDownLatch countDownLatch = new CountDownLatch(dataNodeLocations.size());
    if (dataNodeLocations.isEmpty()) {
      return;
    }
    for (int retry = 0; retry < retryNum; retry++) {
      final int retryCount = retry;
      final AbstractRetryHandler[] handler = new AbstractRetryHandler[1];
      final CountDownLatch retryCountDownLatch = countDownLatch;
      dataNodeLocations.forEach(
          (index, dataNodeLocation) -> {
            handler[0] = ((Map<Integer, AbstractRetryHandler>) handlerMap).get(index);
            // If it is not the first request, then prove that this operation is a retry.
            // The count of countDownLatch needs to be updated
            if (retryCount != 0) {
              handler[0].setCountDownLatch(retryCountDownLatch);
            }
            // send request
            sendAsyncRequestToDataNode(dataNodeLocation, req, handler[0], retryCount);
          });
      try {
        handler[0].getCountDownLatch().await();
      } catch (InterruptedException e) {
        LOGGER.error("Interrupted during {} on ConfigNode", handler[0].getDataNodeRequestType());
      }
      // Check if there is a node that fails to send the request, and retry if there is one
      if (!handler[0].getDataNodeLocations().isEmpty()) {
        countDownLatch = new CountDownLatch(handler[0].getDataNodeLocations().size());
      } else {
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
        case setTTL:
          client.setTTL((TSetTTLReq) req, (SetTTLHandler) handler);
          break;
        case createRegions:
          TConsensusGroupType regionType =
              ((CreateRegionHandler) handler).getConsensusGroupId().getType();
          if (regionType == TConsensusGroupType.SchemaRegion) {
            client.createSchemaRegion(
                (TCreateSchemaRegionReq) ((Map<Integer, Object>) req).get(handler.getIndex()),
                (CreateRegionHandler) handler);
          } else if (regionType == TConsensusGroupType.DataRegion) {
            client.createDataRegion(
                (TCreateDataRegionReq) ((Map<Integer, Object>) req).get(handler.getIndex()),
                (CreateRegionHandler) handler);
          }
          break;
        case createFunction:
          client.createFunction((TCreateFunctionRequest) req, (FunctionManagementHandler) handler);
          break;
        case dropFunction:
          client.dropFunction((TDropFunctionRequest) req, (FunctionManagementHandler) handler);
          break;
        case flush:
          client.flush((TFlushReq) req, (FlushHandler) handler);
          break;
        case updateRegionRouteMap:
          client.updateRegionCache((TRegionRouteReq) req, (UpdateRegionRouteMapHandler) handler);
          break;
        default:
          return;
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

    // Number of regions to be created
    int regionNum = 0;
    // Assign an independent index to each Region
    for (Map.Entry<String, List<TRegionReplicaSet>> entry :
        createRegionGroupsPlan.getRegionGroupMap().entrySet()) {
      for (TRegionReplicaSet regionReplicaSet : entry.getValue()) {
        regionNum += regionReplicaSet.getDataNodeLocationsSize();
      }
    }
    Map<Integer, CreateRegionHandler> handlerMap = new ConcurrentHashMap<>();
    ConcurrentHashMap<Integer, TDataNodeLocation> dataNodeLocations = new ConcurrentHashMap<>();
    Map<Integer, Object> req = new ConcurrentHashMap<>();
    AtomicInteger index = new AtomicInteger();
    CountDownLatch latch = new CountDownLatch(regionNum);
    createRegionGroupsPlan
        .getRegionGroupMap()
        .forEach(
            (storageGroup, regionReplicaSets) -> {
              // Enumerate each RegionReplicaSet
              regionReplicaSets.forEach(
                  regionReplicaSet -> {
                    // Enumerate each Region
                    regionReplicaSet
                        .getDataNodeLocations()
                        .forEach(
                            dataNodeLocation -> {
                              handlerMap.put(
                                  index.get(),
                                  new CreateRegionHandler(
                                      index.get(),
                                      latch,
                                      regionReplicaSet.getRegionId(),
                                      dataNodeLocation,
                                      dataNodeLocations));

                              switch (regionReplicaSet.getRegionId().getType()) {
                                case SchemaRegion:
                                  req.put(
                                      index.get(),
                                      genCreateSchemaRegionReq(storageGroup, regionReplicaSet));
                                  break;
                                case DataRegion:
                                  req.put(
                                      index.get(),
                                      genCreateDataRegionReq(
                                          storageGroup,
                                          regionReplicaSet,
                                          ttlMap.get(storageGroup)));
                              }
                              dataNodeLocations.put(index.getAndIncrement(), dataNodeLocation);
                            });
                  });
            });
    sendAsyncRequestToDataNodeWithRetry(req, handlerMap, dataNodeLocations);
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
   * @param endPoint
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
