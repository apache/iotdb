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
package org.apache.iotdb.confignode.client.async.handlers.heartbeat;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.commons.cluster.RegionStatus;
import org.apache.iotdb.confignode.manager.load.balancer.RouteBalancer;
import org.apache.iotdb.confignode.manager.node.heartbeat.DataNodeHeartbeatCache;
import org.apache.iotdb.confignode.manager.node.heartbeat.NodeHeartbeatSample;
import org.apache.iotdb.confignode.manager.partition.heartbeat.RegionGroupCache;
import org.apache.iotdb.confignode.manager.partition.heartbeat.RegionHeartbeatSample;
import org.apache.iotdb.mpp.rpc.thrift.THeartbeatResp;
import org.apache.iotdb.tsfile.utils.Pair;

import org.apache.thrift.async.AsyncMethodCallback;

import java.util.Map;

public class DataNodeHeartbeatHandler implements AsyncMethodCallback<THeartbeatResp> {

  // Update DataNodeHeartbeatCache when success
  private final TDataNodeLocation dataNodeLocation;
  private final DataNodeHeartbeatCache dataNodeHeartbeatCache;
  private final Map<TConsensusGroupId, RegionGroupCache> regionGroupCacheMap;
  private final RouteBalancer routeBalancer;

  public DataNodeHeartbeatHandler(
      TDataNodeLocation dataNodeLocation,
      DataNodeHeartbeatCache dataNodeHeartbeatCache,
      Map<TConsensusGroupId, RegionGroupCache> regionGroupCacheMap,
      RouteBalancer routeBalancer) {
    this.dataNodeLocation = dataNodeLocation;
    this.dataNodeHeartbeatCache = dataNodeHeartbeatCache;
    this.regionGroupCacheMap = regionGroupCacheMap;
    this.routeBalancer = routeBalancer;
  }

  @Override
  public void onComplete(THeartbeatResp heartbeatResp) {
    long receiveTime = System.currentTimeMillis();

    // Update NodeCache
    dataNodeHeartbeatCache.cacheHeartbeatSample(
        new NodeHeartbeatSample(heartbeatResp, receiveTime));

    // Update RegionGroupCache And leaderCache
    heartbeatResp
        .getJudgedLeaders()
        .forEach(
            (regionGroupId, isLeader) -> {
              regionGroupCacheMap
                  .computeIfAbsent(regionGroupId, empty -> new RegionGroupCache(regionGroupId))
                  .cacheHeartbeatSample(
                      dataNodeLocation.getDataNodeId(),
                      new RegionHeartbeatSample(
                          heartbeatResp.getHeartbeatTimestamp(),
                          receiveTime,
                          // Region will inherit DataNode's status
                          RegionStatus.parse(heartbeatResp.getStatus())));

              if (isLeader) {
                routeBalancer.cacheLeaderSample(
                    regionGroupId,
                    new Pair<>(
                        heartbeatResp.getHeartbeatTimestamp(), dataNodeLocation.getDataNodeId()));
              }
            });
  }

  @Override
  public void onError(Exception e) {
    // Do nothing
  }
}
