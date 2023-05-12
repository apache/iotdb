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

package org.apache.iotdb.confignode.manager.load.cache.route;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.confignode.conf.ConfigNodeConfig;
import org.apache.iotdb.confignode.conf.ConfigNodeDescriptor;
import org.apache.iotdb.consensus.ConsensusFactory;
import org.apache.iotdb.tsfile.utils.Pair;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class RegionRouteCache {

  private static final Logger LOGGER = LoggerFactory.getLogger(RegionRouteCache.class);

  private static final ConfigNodeConfig CONF = ConfigNodeDescriptor.getInstance().getConf();
  private static final String SCHEMA_REGION_CONSENSUS_PROTOCOL_CLASS =
      CONF.getSchemaRegionConsensusProtocolClass();
  private static final String DATA_REGION_CONSENSUS_PROTOCOL_CLASS =
      CONF.getDataRegionConsensusProtocolClass();

  public static final int unReadyLeaderId = -1;
  public static final TRegionReplicaSet unReadyRegionPriority = new TRegionReplicaSet();

  private final String consensusProtocolClass;

  // Pair<Timestamp, LeaderDataNodeId>, where
  // the left value stands for sampling timestamp
  // and the right value stands for the index of DataNode that leader resides.
  private final AtomicReference<Pair<Long, Integer>> leaderSample;
  private final AtomicInteger leaderId;
  private final AtomicReference<TRegionReplicaSet> regionPriority;

  private final TConsensusGroupId consensusGroupId;

  public RegionRouteCache(TConsensusGroupId consensusGroupId) {
    this.consensusGroupId = consensusGroupId;
    switch (consensusGroupId.getType()) {
      case SchemaRegion:
        this.consensusProtocolClass = SCHEMA_REGION_CONSENSUS_PROTOCOL_CLASS;
        break;
      case DataRegion:
      default:
        this.consensusProtocolClass = DATA_REGION_CONSENSUS_PROTOCOL_CLASS;
        break;
    }

    this.leaderSample = new AtomicReference<>(new Pair<>(0L, -1));
    this.leaderId = new AtomicInteger(-1);
    this.regionPriority = new AtomicReference<>(new TRegionReplicaSet());
  }

  /**
   * Cache the latest leader of a RegionGroup.
   *
   * @param leaderSample the latest leader of a RegionGroup
   */
  public synchronized void cacheLeaderSample(Pair<Long, Integer> leaderSample) {
    switch (consensusProtocolClass) {
      case ConsensusFactory.SIMPLE_CONSENSUS:
      case ConsensusFactory.RATIS_CONSENSUS:
        // The leader of simple and ratis consensus is self-elected
        if (leaderSample.getLeft() > this.leaderSample.get().getLeft()) {
          this.leaderSample.set(leaderSample);
        }
        break;
      case ConsensusFactory.IOT_CONSENSUS:
      default:
        // The leader of other consensus protocol is selected by ConfigNode-leader.
    }
  }

  /**
   * Invoking periodically in the Cluster-LoadStatistics-Service to update leaderId and compare with
   * the previous leader, in order to detect whether the RegionGroup's leader has changed.
   *
   * @return True if the leader has changed recently(compare with the leaderId), false otherwise
   */
  public boolean periodicUpdate() {
    switch (consensusProtocolClass) {
      case ConsensusFactory.SIMPLE_CONSENSUS:
      case ConsensusFactory.RATIS_CONSENSUS:
        // The leader of simple and ratis consensus is self-elected
        if (leaderSample.get().getRight() != leaderId.get()) {
          leaderId.set(leaderSample.get().getRight());
          return true;
        }
        return false;
      case ConsensusFactory.IOT_CONSENSUS:
      default:
        // The leader of iot consensus protocol is selected by ConfigNode-leader.
        // The leaderId is initialized to -1, in this case return ture will trigger the leader
        // selection.
        return leaderId.get() == -1;
    }
  }

  /**
   * Force update the specified RegionGroup's leader.
   *
   * @param leaderId Leader DataNodeId
   */
  public void forceUpdateRegionLeader(int leaderId) {
    this.leaderId.set(leaderId);
  }

  /**
   * Force update the specified RegionGroup's priority.
   *
   * @param regionPriority Region route priority
   */
  public void forceUpdateRegionPriority(TRegionReplicaSet regionPriority) {
    this.regionPriority.set(regionPriority);
  }

  public boolean isRegionGroupUnready() {
    return unReadyLeaderId == leaderId.get() || unReadyRegionPriority.equals(regionPriority.get());
  }

  public int getLeaderId() {
    return leaderId.get();
  }

  public TRegionReplicaSet getRegionPriority() {
    return regionPriority.get();
  }
}
