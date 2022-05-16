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
package org.apache.iotdb.confignode.manager.load;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TConsensusGroupType;
import org.apache.iotdb.common.rpc.thrift.TDataNodeInfo;
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.THeartbeatReq;
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.confignode.client.AsyncDataNodeClientPool;
import org.apache.iotdb.confignode.client.handlers.CreateRegionHandler;
import org.apache.iotdb.confignode.client.handlers.HeartbeatHandler;
import org.apache.iotdb.confignode.conf.ConfigNodeDescriptor;
import org.apache.iotdb.confignode.consensus.request.write.CreateRegionsReq;
import org.apache.iotdb.confignode.exception.NotEnoughDataNodeException;
import org.apache.iotdb.confignode.manager.ClusterSchemaManager;
import org.apache.iotdb.confignode.manager.ConsensusManager;
import org.apache.iotdb.confignode.manager.Manager;
import org.apache.iotdb.confignode.manager.NodeManager;
import org.apache.iotdb.confignode.manager.PartitionManager;
import org.apache.iotdb.confignode.manager.load.allocator.CopySetRegionAllocator;
import org.apache.iotdb.confignode.manager.load.allocator.IRegionAllocator;
import org.apache.iotdb.confignode.manager.load.heartbeat.HeartbeatCache;
import org.apache.iotdb.confignode.rpc.thrift.TStorageGroupSchema;
import org.apache.iotdb.mpp.rpc.thrift.TCreateDataRegionReq;
import org.apache.iotdb.mpp.rpc.thrift.TCreateSchemaRegionReq;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.BitSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * The LoadManager at ConfigNodeGroup-Leader is active. It proactively implements the cluster
 * dynamic load balancing policy and passively accepts the PartitionTable expansion request.
 */
public class LoadManager implements Runnable {

  private static final Logger LOGGER = LoggerFactory.getLogger(LoadManager.class);

  private final long heartbeatInterval =
      ConfigNodeDescriptor.getInstance().getConf().getHeartbeatInterval();

  private final Manager configManager;

  private final HeartbeatCache heartbeatCache;

  private final IRegionAllocator regionAllocator;

  // TODO: Interfaces for active, interrupt and reset LoadBalancer

  public LoadManager(Manager configManager) {
    this.configManager = configManager;
    this.heartbeatCache = new HeartbeatCache();
    this.regionAllocator = new CopySetRegionAllocator();
  }

  /**
   * Allocate and create one Region on DataNode for each StorageGroup.
   *
   * @param storageGroups List<StorageGroupName>
   * @param consensusGroupType TConsensusGroupType of Region to be allocated
   */
  public void allocateAndCreateRegions(
      List<String> storageGroups, TConsensusGroupType consensusGroupType)
      throws NotEnoughDataNodeException {
    CreateRegionsReq createRegionsReq = null;

    // TODO: use procedure to protect create Regions process
    try {
      createRegionsReq = allocateRegions(storageGroups, consensusGroupType);
      createRegionsOnDataNodes(createRegionsReq);
    } catch (MetadataException e) {
      LOGGER.error("Meet error when create Regions", e);
    }

    getConsensusManager().write(createRegionsReq);
  }

  private CreateRegionsReq allocateRegions(
      List<String> storageGroups, TConsensusGroupType consensusGroupType)
      throws NotEnoughDataNodeException, MetadataException {
    CreateRegionsReq createRegionsReq = new CreateRegionsReq();

    List<TDataNodeInfo> onlineDataNodes = getNodeManager().getOnlineDataNodes(-1);
    List<TRegionReplicaSet> allocatedRegions = getPartitionManager().getAllocatedRegions();

    for (String storageGroup : storageGroups) {
      TStorageGroupSchema storageGroupSchema =
          getClusterSchemaManager().getStorageGroupSchemaByName(storageGroup);
      int replicationFactor =
          consensusGroupType == TConsensusGroupType.SchemaRegion
              ? storageGroupSchema.getSchemaReplicationFactor()
              : storageGroupSchema.getDataReplicationFactor();

      if (onlineDataNodes.size() < replicationFactor) {
        throw new NotEnoughDataNodeException();
      }

      TRegionReplicaSet newRegion =
          regionAllocator.allocateRegion(
              onlineDataNodes,
              allocatedRegions,
              replicationFactor,
              new TConsensusGroupId(
                  consensusGroupType, getPartitionManager().generateNextRegionGroupId()));
      createRegionsReq.addRegion(storageGroup, newRegion);
    }

    return createRegionsReq;
  }

  private void createRegionsOnDataNodes(CreateRegionsReq createRegionsReq)
      throws MetadataException {
    // Index of each Region
    int index = 0;
    // Number of regions to be created
    int regionNum = 0;
    Map<String, Map<Integer, Integer>> indexMap = new HashMap<>();
    Map<String, Long> ttlMap = new HashMap<>();
    for (Map.Entry<String, TRegionReplicaSet> entry : createRegionsReq.getRegionMap().entrySet()) {
      regionNum += entry.getValue().getDataNodeLocationsSize();
      ttlMap.put(
          entry.getKey(),
          getClusterSchemaManager().getStorageGroupSchemaByName(entry.getKey()).getTTL());
      for (TDataNodeLocation dataNodeLocation : entry.getValue().getDataNodeLocations()) {
        indexMap
            .computeIfAbsent(entry.getKey(), sg -> new HashMap<>())
            .put(dataNodeLocation.getDataNodeId(), index);
        index += 1;
      }
    }

    BitSet bitSet = new BitSet(regionNum);

    for (int retry = 0; retry < 3; retry++) {
      CountDownLatch latch = new CountDownLatch(regionNum - bitSet.cardinality());

      createRegionsReq
          .getRegionMap()
          .forEach(
              (storageGroup, regionReplicaSet) -> {
                // Enumerate each Region
                regionReplicaSet
                    .getDataNodeLocations()
                    .forEach(
                        dataNodeLocation -> {
                          // Skip those created successfully
                          if (!bitSet.get(
                              indexMap.get(storageGroup).get(dataNodeLocation.getDataNodeId()))) {
                            TEndPoint endPoint = dataNodeLocation.getInternalEndPoint();
                            CreateRegionHandler handler =
                                new CreateRegionHandler(
                                    indexMap
                                        .get(storageGroup)
                                        .get(dataNodeLocation.getDataNodeId()),
                                    bitSet,
                                    latch,
                                    regionReplicaSet.getRegionId(),
                                    dataNodeLocation);

                            switch (regionReplicaSet.getRegionId().getType()) {
                              case SchemaRegion:
                                AsyncDataNodeClientPool.getInstance()
                                    .createSchemaRegion(
                                        endPoint,
                                        genCreateSchemaRegionReq(storageGroup, regionReplicaSet),
                                        handler);
                                break;
                              case DataRegion:
                                AsyncDataNodeClientPool.getInstance()
                                    .createDataRegion(
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

      try {
        latch.await();
      } catch (InterruptedException e) {
        LOGGER.error("ClusterSchemaManager was interrupted during create Regions on DataNodes", e);
      }

      if (bitSet.cardinality() == regionNum) {
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

  private TCreateDataRegionReq genCreateDataRegionReq(
      String storageGroup, TRegionReplicaSet regionReplicaSet, long TTL) {
    TCreateDataRegionReq req = new TCreateDataRegionReq();
    req.setStorageGroup(storageGroup);
    req.setRegionReplicaSet(regionReplicaSet);
    req.setTtl(TTL);
    return req;
  }

  private ConsensusManager getConsensusManager() {
    return configManager.getConsensusManager();
  }

  private NodeManager getNodeManager() {
    return configManager.getNodeManager();
  }

  private ClusterSchemaManager getClusterSchemaManager() {
    return configManager.getClusterSchemaManager();
  }

  private PartitionManager getPartitionManager() {
    return configManager.getPartitionManager();
  }

  private THeartbeatReq genHeartbeatReq() {
    return new THeartbeatReq(System.currentTimeMillis());
  }

  @Override
  public void run() {
    int balanceCount = 0;
    while (true) {
      try {

        if (getConsensusManager().isLeader()) {
          // Ask DataNode for heartbeat in every heartbeat interval
          List<TDataNodeInfo> onlineDataNodes = getNodeManager().getOnlineDataNodes(-1);
          for (TDataNodeInfo dataNodeInfo : onlineDataNodes) {
            HeartbeatHandler handler =
                new HeartbeatHandler(dataNodeInfo.getLocation().getDataNodeId(), heartbeatCache);
            AsyncDataNodeClientPool.getInstance()
                .getHeartBeat(
                    dataNodeInfo.getLocation().getInternalEndPoint(), genHeartbeatReq(), handler);
          }

          balanceCount += 1;
          // TODO: Adjust load balancing period
          if (balanceCount == 10) {
            doLoadBalancing();
            balanceCount = 0;
          }
        } else {
          heartbeatCache.discardAllCache();
        }

        TimeUnit.MILLISECONDS.sleep(heartbeatInterval);
      } catch (InterruptedException e) {
        LOGGER.error("Heartbeat thread has been interrupted, stopping ConfigNode...", e);
        System.exit(-1);
      }
    }
  }

  /** Load balancing */
  private void doLoadBalancing() {}
}
