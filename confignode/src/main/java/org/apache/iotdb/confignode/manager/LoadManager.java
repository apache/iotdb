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
package org.apache.iotdb.confignode.manager;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TConsensusGroupType;
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.confignode.client.AsyncDataNodeClientPool;
import org.apache.iotdb.confignode.client.handlers.InitRegionHandler;
import org.apache.iotdb.confignode.conf.ConfigNodeDescriptor;
import org.apache.iotdb.confignode.consensus.request.write.CreateRegionsReq;
import org.apache.iotdb.confignode.exception.NotEnoughDataNodeException;
import org.apache.iotdb.confignode.manager.allocator.CopySetRegionAllocator;
import org.apache.iotdb.confignode.manager.allocator.IRegionAllocator;
import org.apache.iotdb.mpp.rpc.thrift.TCreateDataRegionReq;
import org.apache.iotdb.mpp.rpc.thrift.TCreateSchemaRegionReq;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.BitSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

/**
 * The LoadManager at ConfigNodeGroup-Leader is active. It proactively implements the cluster
 * dynamic load balancing policy and passively accepts the PartitionTable expansion request.
 */
public class LoadManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(LoadManager.class);

  private static final int schemaReplicationFactor =
      ConfigNodeDescriptor.getInstance().getConf().getSchemaReplicationFactor();
  private static final int dataReplicationFactor =
      ConfigNodeDescriptor.getInstance().getConf().getDataReplicationFactor();

  private final Manager configManager;

  private final IRegionAllocator regionAllocator;

  // TODO: Interfaces for active, interrupt and reset LoadBalancer

  public LoadManager(Manager configManager) {
    this.configManager = configManager;
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
    CreateRegionsReq createRegionsReq = allocateRegions(storageGroups, consensusGroupType);

    // TODO: use procedure to protect create Regions process
    try {
      createRegionsOnDataNodes(createRegionsReq);
    } catch (Exception e) {
      LOGGER.error("Meet error when create Regions", e);
    }

    getConsensusManager().write(createRegionsReq);
  }

  private CreateRegionsReq allocateRegions(
      List<String> storageGroups, TConsensusGroupType consensusGroupType)
      throws NotEnoughDataNodeException {
    CreateRegionsReq createRegionsReq = new CreateRegionsReq();

    int replicationFactor =
        consensusGroupType.equals(TConsensusGroupType.SchemaRegion)
            ? schemaReplicationFactor
            : dataReplicationFactor;
    List<TDataNodeLocation> onlineDataNodes = getNodeManager().getOnlineDataNodes();
    List<TRegionReplicaSet> allocatedRegions = getPartitionManager().getAllocatedRegions();

    if (onlineDataNodes.size() < replicationFactor) {
      throw new NotEnoughDataNodeException();
    }

    for (String storageGroup : storageGroups) {
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
    int index = 0;
    int regionNum = 0;
    Map<String, Integer> indexMap = new HashMap<>();
    Map<String, Long> ttlMap = new HashMap<>();
    for (Map.Entry<String, TRegionReplicaSet> entry : createRegionsReq.getRegionMap().entrySet()) {
      indexMap.put(entry.getKey(), index);
      ttlMap.put(
          entry.getKey(),
          getClusterSchemaManager().getStorageGroupSchemaByName(entry.getKey()).getTTL());
      index += 1;
      regionNum += entry.getValue().getDataNodeLocationsSize();
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
                          if (!bitSet.get(indexMap.get(storageGroup))) {
                            TEndPoint endPoint = dataNodeLocation.getInternalEndPoint();
                            InitRegionHandler handler =
                                new InitRegionHandler(
                                    indexMap.get(storageGroup),
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
}
