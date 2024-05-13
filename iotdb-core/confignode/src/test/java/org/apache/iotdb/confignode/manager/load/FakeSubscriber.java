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
import org.apache.iotdb.confignode.manager.load.cache.consensus.ConsensusGroupStatistics;
import org.apache.iotdb.confignode.manager.load.cache.node.NodeStatistics;
import org.apache.iotdb.confignode.manager.load.cache.region.RegionGroupStatistics;
import org.apache.iotdb.confignode.manager.load.subscriber.ConsensusGroupStatisticsChangeEvent;
import org.apache.iotdb.confignode.manager.load.subscriber.IClusterStatusSubscriber;
import org.apache.iotdb.confignode.manager.load.subscriber.NodeStatisticsChangeEvent;
import org.apache.iotdb.confignode.manager.load.subscriber.RegionGroupStatisticsChangeEvent;

import org.apache.tsfile.utils.Pair;

import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.Semaphore;

public class FakeSubscriber implements IClusterStatusSubscriber {

  private final Semaphore nodeSemaphore;
  private final Map<Integer, Pair<NodeStatistics, NodeStatistics>> differentNodeStatisticsMap;
  private final Semaphore regionGroupSemaphore;
  private final Map<TConsensusGroupId, Pair<RegionGroupStatistics, RegionGroupStatistics>>
      differentRegionGroupStatisticsMap;
  private final Semaphore consensusGroupSemaphore;
  private final Map<TConsensusGroupId, Pair<ConsensusGroupStatistics, ConsensusGroupStatistics>>
      differentConsensusGroupStatisticsMap;

  public FakeSubscriber(
      Semaphore nodeSemaphore, Semaphore regionGroupSemaphore, Semaphore consensusGroupSemaphore) {
    this.nodeSemaphore = nodeSemaphore;
    this.regionGroupSemaphore = regionGroupSemaphore;
    this.consensusGroupSemaphore = consensusGroupSemaphore;
    differentNodeStatisticsMap = new TreeMap<>();
    differentRegionGroupStatisticsMap = new TreeMap<>();
    differentConsensusGroupStatisticsMap = new TreeMap<>();
  }

  @Override
  public void onNodeStatisticsChanged(NodeStatisticsChangeEvent event) {
    differentNodeStatisticsMap.clear();
    differentNodeStatisticsMap.putAll(event.getDifferentNodeStatisticsMap());
    nodeSemaphore.release();
  }

  @Override
  public void onRegionGroupStatisticsChanged(RegionGroupStatisticsChangeEvent event) {
    differentRegionGroupStatisticsMap.clear();
    differentRegionGroupStatisticsMap.putAll(event.getDifferentRegionGroupStatisticsMap());
    regionGroupSemaphore.release();
  }

  @Override
  public void onConsensusGroupStatisticsChanged(ConsensusGroupStatisticsChangeEvent event) {
    differentConsensusGroupStatisticsMap.clear();
    differentConsensusGroupStatisticsMap.putAll(event.getDifferentConsensusGroupStatisticsMap());
    consensusGroupSemaphore.release();
  }

  public Map<Integer, Pair<NodeStatistics, NodeStatistics>> getDifferentNodeStatisticsMap() {
    return differentNodeStatisticsMap;
  }

  public Map<TConsensusGroupId, Pair<RegionGroupStatistics, RegionGroupStatistics>>
      getDifferentRegionGroupStatisticsMap() {
    return differentRegionGroupStatisticsMap;
  }

  public Map<TConsensusGroupId, Pair<ConsensusGroupStatistics, ConsensusGroupStatistics>>
      getDifferentConsensusGroupStatisticsMap() {
    return differentConsensusGroupStatisticsMap;
  }
}
