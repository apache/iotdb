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

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TFlushReq;
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.commons.client.IClientManager;
import org.apache.iotdb.commons.client.async.AsyncDataNodeInternalServiceClient;
import org.apache.iotdb.confignode.client.handlers.CreateRegionHandler;
import org.apache.iotdb.confignode.client.handlers.FlushHandler;
import org.apache.iotdb.confignode.client.handlers.FunctionManagementHandler;
import org.apache.iotdb.confignode.client.handlers.HeartbeatHandler;
import org.apache.iotdb.confignode.consensus.request.write.CreateRegionsReq;
import org.apache.iotdb.mpp.rpc.thrift.TCreateDataRegionReq;
import org.apache.iotdb.mpp.rpc.thrift.TCreateFunctionRequest;
import org.apache.iotdb.mpp.rpc.thrift.TCreateSchemaRegionReq;
import org.apache.iotdb.mpp.rpc.thrift.TDropFunctionRequest;
import org.apache.iotdb.mpp.rpc.thrift.THeartbeatReq;

import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.BitSet;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.CountDownLatch;

/** Asynchronously send RPC requests to DataNodes. See mpp.thrift for more details. */
public class AsyncDataNodeClientPool {

  private static final Logger LOGGER = LoggerFactory.getLogger(AsyncDataNodeClientPool.class);

  private final IClientManager<TEndPoint, AsyncDataNodeInternalServiceClient> clientManager;

  private AsyncDataNodeClientPool() {
    clientManager =
        new IClientManager.Factory<TEndPoint, AsyncDataNodeInternalServiceClient>()
            .createClientManager(
                new ConfigNodeClientPoolFactory.AsyncDataNodeInternalServiceClientPoolFactory());
  }

  /**
   * Execute CreateRegionsReq asynchronously
   *
   * @param createRegionsReq CreateRegionsReq
   * @param ttlMap Map<StorageGroupName, TTL>
   */
  public void createRegions(CreateRegionsReq createRegionsReq, Map<String, Long> ttlMap) {

    // Index of each Region
    int index = 0;
    // Number of regions to be created
    int regionNum = 0;
    // Map<TConsensusGroupId, Map<DataNodeId, index>>
    Map<TConsensusGroupId, Map<Integer, Integer>> indexMap = new TreeMap<>();

    // Assign an independent index to each Region
    for (Map.Entry<String, List<TRegionReplicaSet>> entry :
        createRegionsReq.getRegionMap().entrySet()) {
      for (TRegionReplicaSet regionReplicaSet : entry.getValue()) {
        regionNum += regionReplicaSet.getDataNodeLocationsSize();
        for (TDataNodeLocation dataNodeLocation : regionReplicaSet.getDataNodeLocations()) {
          indexMap
              .computeIfAbsent(regionReplicaSet.getRegionId(), idMap -> new TreeMap<>())
              .put(dataNodeLocation.getDataNodeId(), index);
          index += 1;
        }
      }
    }

    BitSet bitSet = new BitSet(regionNum);
    for (int retry = 0; retry < 3; retry++) {
      CountDownLatch latch = new CountDownLatch(regionNum - bitSet.cardinality());
      createRegionsReq
          .getRegionMap()
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
                                // Skip those created successfully
                                if (!bitSet.get(
                                    indexMap
                                        .get(regionReplicaSet.getRegionId())
                                        .get(dataNodeLocation.getDataNodeId()))) {
                                  TEndPoint endPoint = dataNodeLocation.getInternalEndPoint();
                                  CreateRegionHandler handler =
                                      new CreateRegionHandler(
                                          indexMap
                                              .get(regionReplicaSet.getRegionId())
                                              .get(dataNodeLocation.getDataNodeId()),
                                          bitSet,
                                          latch,
                                          regionReplicaSet.getRegionId(),
                                          dataNodeLocation);

                                  switch (regionReplicaSet.getRegionId().getType()) {
                                    case SchemaRegion:
                                      createSchemaRegion(
                                          endPoint,
                                          genCreateSchemaRegionReq(storageGroup, regionReplicaSet),
                                          handler);
                                      break;
                                    case DataRegion:
                                      createDataRegion(
                                          endPoint,
                                          genCreateDataRegionReq(
                                              storageGroup,
                                              regionReplicaSet,
                                              ttlMap.get(storageGroup)),
                                          handler);
                                  }
                                }
                              });
                    });
              });

      try {
        // Waiting until this batch of create requests done
        latch.await();
      } catch (InterruptedException e) {
        LOGGER.error("ClusterSchemaManager was interrupted during create Regions on DataNodes", e);
      }

      if (bitSet.cardinality() == regionNum) {
        // Break if all creations success
        break;
      }
    }

    if (bitSet.cardinality() < regionNum) {
      LOGGER.error(
          "Failed to create some SchemaRegions or DataRegions on DataNodes. Please check former logs.");
    }
  }

  private TCreateSchemaRegionReq genCreateSchemaRegionReq(
      String storageGroup, TRegionReplicaSet regionReplicaSet) {
    TCreateSchemaRegionReq req = new TCreateSchemaRegionReq();
    req.setStorageGroup(storageGroup);
    req.setRegionReplicaSet(regionReplicaSet);
    return req;
  }

  /**
   * Create a SchemaRegion on specific DataNode
   *
   * @param endPoint The specific DataNode
   */
  private void createSchemaRegion(
      TEndPoint endPoint, TCreateSchemaRegionReq req, CreateRegionHandler handler) {
    AsyncDataNodeInternalServiceClient client;
    try {
      client = clientManager.borrowClient(endPoint);
      client.createSchemaRegion(req, handler);
    } catch (IOException e) {
      LOGGER.error("Can't connect to DataNode {}", endPoint, e);
    } catch (TException e) {
      LOGGER.error("Create SchemaRegion on DataNode {} failed", endPoint, e);
    }
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
   * Create a DataRegion on specific DataNode
   *
   * @param endPoint The specific DataNode
   */
  public void createDataRegion(
      TEndPoint endPoint, TCreateDataRegionReq req, CreateRegionHandler handler) {
    AsyncDataNodeInternalServiceClient client;
    try {
      client = clientManager.borrowClient(endPoint);
      client.createDataRegion(req, handler);
    } catch (IOException e) {
      LOGGER.error("Can't connect to DataNode {}", endPoint, e);
    } catch (TException e) {
      LOGGER.error("Create DataRegion on DataNode {} failed", endPoint, e);
    }
  }

  /**
   * Only used in LoadManager
   *
   * @param endPoint The specific DataNode
   */
  public void getHeartBeat(TEndPoint endPoint, THeartbeatReq req, HeartbeatHandler handler) {
    AsyncDataNodeInternalServiceClient client;
    try {
      client = clientManager.borrowClient(endPoint);
      client.getHeartBeat(req, handler);
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

  /**
   * Only used in UDFManager
   *
   * @param endPoint The specific DataNode
   */
  public void createFunction(
      TEndPoint endPoint, TCreateFunctionRequest request, FunctionManagementHandler handler) {
    try {
      clientManager.borrowClient(endPoint).createFunction(request, handler);
    } catch (Exception e) {
      LOGGER.error("Failed to asking DataNode to create function: {}", endPoint, e);
    }
  }

  /**
   * Only used in UDFManager
   *
   * @param endPoint The specific DataNode
   */
  public void dropFunction(
      TEndPoint endPoint, TDropFunctionRequest request, FunctionManagementHandler handler) {
    try {
      clientManager.borrowClient(endPoint).dropFunction(request, handler);
    } catch (Exception e) {
      LOGGER.error("Failed to asking DataNode to create function: {}", endPoint, e);
    }
  }

  /**
   * Flush on specific DataNode
   *
   * @param endPoint The specific DataNode
   */
  public void flush(TEndPoint endPoint, TFlushReq flushReq, FlushHandler handler) {
    for (int retry = 0; retry < 3; retry++) {
      try {
        clientManager.borrowClient(endPoint).flush(flushReq, handler);
        return;
      } catch (Exception e) {
        LOGGER.error("Failed to asking DataNode to flush: {}", endPoint, e);
      }
    }
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
