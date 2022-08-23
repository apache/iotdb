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
package org.apache.iotdb.confignode.manager.load.heartbeat;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class RegionGroupCache implements IRegionGroupCache {

  private static final Logger LOGGER = LoggerFactory.getLogger(RegionGroupCache.class);

  // TODO: This class might be split into SchemaRegionGroupCache and DataRegionGroupCache

  private static final int maximumWindowSize = 100;

  private final TConsensusGroupId consensusGroupId;

  // Map<DataNodeId(where a RegionReplica resides), LinkedList<RegionHeartbeatSample>>
  private final Map<Integer, LinkedList<RegionHeartbeatSample>> slidingWindow;

  // Indicates the version of the statistics
  private final AtomicLong versionTimestamp;
  // The DataNode where the leader resides
  private final AtomicInteger leaderDataNodeId;

  public RegionGroupCache(TConsensusGroupId consensusGroupId) {
    this.consensusGroupId = consensusGroupId;

    this.slidingWindow = new ConcurrentHashMap<>();

    this.versionTimestamp = new AtomicLong(0);
    this.leaderDataNodeId = new AtomicInteger(-1);
  }

  @Override
  public void cacheHeartbeatSample(RegionHeartbeatSample newHeartbeatSample) {
    slidingWindow.putIfAbsent(newHeartbeatSample.getBelongedDataNodeId(), new LinkedList<>());
    synchronized (slidingWindow.get(newHeartbeatSample.getBelongedDataNodeId())) {
      LinkedList<RegionHeartbeatSample> samples =
          slidingWindow.get(newHeartbeatSample.getBelongedDataNodeId());

      // Only sequential HeartbeatSamples are accepted.
      // And un-sequential HeartbeatSamples will be discarded.
      if (samples.size() == 0
          || samples.getLast().getSendTimestamp() < newHeartbeatSample.getSendTimestamp()) {
        samples.add(newHeartbeatSample);
      }

      if (samples.size() > maximumWindowSize) {
        samples.removeFirst();
      }
    }
  }

  @Override
  public boolean updateLoadStatistic() {
    long updateVersion = Long.MIN_VALUE;
    int updateLeaderDataNodeId = -1;
    int originLeaderDataNodeId = leaderDataNodeId.get();

    synchronized (slidingWindow) {
      for (LinkedList<RegionHeartbeatSample> samples : slidingWindow.values()) {
        if (samples.size() > 0) {
          RegionHeartbeatSample lastSample = samples.getLast();
          if (lastSample.getSendTimestamp() > updateVersion && lastSample.isLeader()) {
            updateVersion = lastSample.getSendTimestamp();
            updateLeaderDataNodeId = lastSample.getBelongedDataNodeId();
          }
        }
      }
    }

    if (updateVersion > versionTimestamp.get()) {
      // Only update when the leadership information is latest
      versionTimestamp.set(updateVersion);
      leaderDataNodeId.set(updateLeaderDataNodeId);
    }

    return originLeaderDataNodeId != leaderDataNodeId.get();
  }

  @Override
  public int getLeaderDataNodeId() {
    return leaderDataNodeId.get();
  }

  @Override
  public TConsensusGroupId getConsensusGroupId() {
    return consensusGroupId;
  }
}
