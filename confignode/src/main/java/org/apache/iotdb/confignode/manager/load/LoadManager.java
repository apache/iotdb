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
import org.apache.iotdb.common.rpc.thrift.THeartbeatReq;
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.confignode.client.AsyncDataNodeClientPool;
import org.apache.iotdb.confignode.client.handlers.HeartbeatHandler;
import org.apache.iotdb.confignode.conf.ConfigNodeDescriptor;
import org.apache.iotdb.confignode.consensus.request.write.CreateRegionsReq;
import org.apache.iotdb.confignode.exception.NotEnoughDataNodeException;
import org.apache.iotdb.confignode.manager.ClusterSchemaManager;
import org.apache.iotdb.confignode.manager.ConsensusManager;
import org.apache.iotdb.confignode.manager.Manager;
import org.apache.iotdb.confignode.manager.NodeManager;
import org.apache.iotdb.confignode.manager.load.balancer.RegionBalancer;
import org.apache.iotdb.confignode.manager.load.heartbeat.HeartbeatCache;
import org.apache.iotdb.confignode.rpc.thrift.TStorageGroupSchema;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

/**
 * The LoadManager at ConfigNodeGroup-Leader is active. It proactively implements the cluster
 * dynamic load balancing policy and passively accepts the PartitionTable expansion request.
 */
public class LoadManager implements Runnable {

  private static final Logger LOGGER = LoggerFactory.getLogger(LoadManager.class);

  private final Manager configManager;

  private final long heartbeatInterval =
      ConfigNodeDescriptor.getInstance().getConf().getHeartbeatInterval();
  private final HeartbeatCache heartbeatCache;

  private final RegionBalancer regionBalancer;

  private final Map<TConsensusGroupId, TRegionReplicaSet> replicaScoreMap;

  // TODO: Interfaces for active, interrupt and reset LoadBalancer

  public LoadManager(Manager configManager) {
    this.configManager = configManager;
    this.heartbeatCache = new HeartbeatCache();

    this.regionBalancer = new RegionBalancer(configManager);

    this.replicaScoreMap = new TreeMap<>();
  }

  /**
   * Allocate and create one Region for each StorageGroup. TODO: Use procedure to protect create
   * Regions process
   *
   * @param storageGroups List<StorageGroupName>
   * @param consensusGroupType TConsensusGroupType of Region to be allocated
   */
  public void initializeRegions(List<String> storageGroups, TConsensusGroupType consensusGroupType)
      throws NotEnoughDataNodeException {
    CreateRegionsReq createRegionsReq = null;

    try {
      createRegionsReq =
          regionBalancer.genRegionsAllocationPlan(storageGroups, consensusGroupType, 1);
      createRegionsOnDataNodes(createRegionsReq);
    } catch (MetadataException e) {
      LOGGER.error("Meet error when create Regions", e);
    }

    getConsensusManager().write(createRegionsReq);
  }

  private void createRegionsOnDataNodes(CreateRegionsReq createRegionsReq)
      throws MetadataException {
    Map<String, Long> ttlMap = new HashMap<>();
    for (String storageGroup : createRegionsReq.getRegionMap().keySet()) {
      ttlMap.put(
          storageGroup,
          getClusterSchemaManager().getStorageGroupSchemaByName(storageGroup).getTTL());
    }
    AsyncDataNodeClientPool.getInstance().createRegions(createRegionsReq, ttlMap);
  }

  private THeartbeatReq genHeartbeatReq() {
    return new THeartbeatReq(System.currentTimeMillis());
  }

  private void regionExpansion() {
    // Currently, we simply expand the number of regions held by each storage group to
    // 50% of the total CPU cores to facilitate performance testing of multiple regions

    int totalCoreNum = 0;
    List<TDataNodeInfo> dataNodeInfos = getNodeManager().getOnlineDataNodes(-1);
    for (TDataNodeInfo dataNodeInfo : dataNodeInfos) {
      totalCoreNum += dataNodeInfo.getCpuCoreNum();
    }

    List<String> storageGroups = getClusterSchemaManager().getStorageGroupNames();
    for (String storageGroup : storageGroups) {
      try {
        TStorageGroupSchema storageGroupSchema =
            getClusterSchemaManager().getStorageGroupSchemaByName(storageGroup);
        int totalReplicaNum =
            storageGroupSchema.getSchemaReplicationFactor()
                    * storageGroupSchema.getSchemaRegionGroupIdsSize()
                + storageGroupSchema.getDataReplicationFactor()
                    * storageGroupSchema.getDataRegionGroupIdsSize();

        if (totalReplicaNum < totalCoreNum * 0.5) {
          // Allocate more Regions
          CreateRegionsReq createRegionsReq;
          if (storageGroupSchema.getSchemaRegionGroupIdsSize() * 5
              > storageGroupSchema.getDataRegionGroupIdsSize()) {
            // TODO: Find an optimal SchemaRegion:DataRegion rate
            // Currently, we just assume that it's 1:5
            int regionNum =
                Math.min(
                    (totalCoreNum - totalReplicaNum)
                        / storageGroupSchema.getDataReplicationFactor(),
                    storageGroupSchema.getSchemaRegionGroupIdsSize() * 5
                        - storageGroupSchema.getDataRegionGroupIdsSize());
            createRegionsReq =
                regionBalancer.genRegionsAllocationPlan(
                    Collections.singletonList(storageGroup),
                    TConsensusGroupType.DataRegion,
                    regionNum);
          } else {
            createRegionsReq =
                regionBalancer.genRegionsAllocationPlan(
                    Collections.singletonList(storageGroup), TConsensusGroupType.SchemaRegion, 1);
          }

          // TODO: use procedure to protect this
          createRegionsOnDataNodes(createRegionsReq);
        }
      } catch (MetadataException e) {
        LOGGER.warn("Meet error when doing regionExpansion", e);
      } catch (NotEnoughDataNodeException ignore) {
        // The LoadManager will expand Regions automatically after there are enough DataNodes.
      }
    }
  }

  private void doLoadBalancing() {
    regionExpansion();
    // TODO: update replicaScoreMap
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

  private ConsensusManager getConsensusManager() {
    return configManager.getConsensusManager();
  }

  private NodeManager getNodeManager() {
    return configManager.getNodeManager();
  }

  private ClusterSchemaManager getClusterSchemaManager() {
    return configManager.getClusterSchemaManager();
  }
}
