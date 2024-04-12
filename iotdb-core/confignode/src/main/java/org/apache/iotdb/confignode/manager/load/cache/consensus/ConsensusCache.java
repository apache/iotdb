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

package org.apache.iotdb.confignode.manager.load.cache.consensus;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.confignode.conf.ConfigNodeConfig;
import org.apache.iotdb.confignode.conf.ConfigNodeDescriptor;
import org.apache.iotdb.confignode.manager.load.cache.AbstractLoadCache;
import org.apache.iotdb.consensus.ConsensusFactory;

public class ConsensusCache extends AbstractLoadCache {

  private static final ConfigNodeConfig CONF = ConfigNodeDescriptor.getInstance().getConf();
  private static final String SCHEMA_REGION_CONSENSUS_PROTOCOL_CLASS =
      CONF.getSchemaRegionConsensusProtocolClass();
  private static final String DATA_REGION_CONSENSUS_PROTOCOL_CLASS =
      CONF.getDataRegionConsensusProtocolClass();

  public static final int UN_READY_LEADER_ID = -1;
  public static final TRegionReplicaSet UN_READY_REGION_PRIORITY = new TRegionReplicaSet();

  private final String consensusProtocolClass;

  public ConsensusCache(TConsensusGroupId consensusGroupId) {
    switch (consensusGroupId.getType()) {
      case SchemaRegion:
        this.consensusProtocolClass = SCHEMA_REGION_CONSENSUS_PROTOCOL_CLASS;
        break;
      case DataRegion:
      default:
        this.consensusProtocolClass = DATA_REGION_CONSENSUS_PROTOCOL_CLASS;
        break;
    }
  }

  /**
   * Cache the latest consensus sample of a RegionGroup.
   *
   * @param sample the latest heartbeat sample
   */
  public synchronized void cacheConsensusSample(ConsensusHeartbeatSample sample) {
    switch (consensusProtocolClass) {
      case ConsensusFactory.RATIS_CONSENSUS:
        // The leader of ratis consensus is self-elected
        if (sample.getSampleLogicalTimestamp()
            > this.lastConsensusSample.get().getSampleLogicalTimestamp()) {
          this.lastConsensusSample.set(sample);
        }
        break;
      case ConsensusFactory.SIMPLE_CONSENSUS:
      case ConsensusFactory.IOT_CONSENSUS:
      default:
        // The leader of other consensus protocol is selected by ConfigNode-leader.
    }
  }

  public void updateCurrentStatistics() {
    int currentLeader = lastConsensusSample.get().getLeaderId();
    if (currentLeader != UN_READY_LEADER_ID) {
      currentStatistics.set(
          new ConsensusStatistics(System.nanoTime(), lastConsensusSample.get().getLeaderId()));
    }
  }

  public ConsensusStatistics getCurrentStatistics() {
    return (ConsensusStatistics) currentStatistics.get();
  }

  /**
   * Force update the specified RegionGroup's leader.
   *
   * @param leaderId Leader DataNodeId
   */
  public void forceUpdateRegionLeader(int leaderId) {
    cacheConsensusSample(
        new ConsensusHeartbeatSample(
            lastConsensusSample.get().getSampleLogicalTimestamp() + 1L, leaderId));
    this.leaderId.set(leaderId);
  }

  public boolean isRegionGroupUnready() {
    return UN_READY_LEADER_ID == leaderId.get()
        || UN_READY_REGION_PRIORITY.equals(regionPriority.get());
  }

  public int getLeaderId() {
    return leaderId.get();
  }
}
