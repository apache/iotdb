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
package org.apache.iotdb.confignode.client.handlers;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.confignode.manager.load.heartbeat.DataNodeHeartbeatCache;
import org.apache.iotdb.confignode.manager.load.heartbeat.HeartbeatPackage;
import org.apache.iotdb.confignode.manager.load.heartbeat.IRegionGroupCache;
import org.apache.iotdb.confignode.manager.load.heartbeat.RegionGroupCache;
import org.apache.iotdb.mpp.rpc.thrift.THeartbeatResp;

import org.apache.thrift.async.AsyncMethodCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class DataNodeHeartbeatHandler implements AsyncMethodCallback<THeartbeatResp> {

  private static final Logger LOGGER = LoggerFactory.getLogger(DataNodeHeartbeatHandler.class);

  // Update DataNodeHeartbeatCache when success
  private final TDataNodeLocation dataNodeLocation;
  private final DataNodeHeartbeatCache dataNodeHeartbeatCache;
  private final Map<TConsensusGroupId, IRegionGroupCache> regionGroupCacheMap;

  public DataNodeHeartbeatHandler(
      TDataNodeLocation dataNodeLocation,
      DataNodeHeartbeatCache dataNodeHeartbeatCache,
      Map<TConsensusGroupId, IRegionGroupCache> regionGroupCacheMap) {
    this.dataNodeLocation = dataNodeLocation;
    this.dataNodeHeartbeatCache = dataNodeHeartbeatCache;
    this.regionGroupCacheMap = regionGroupCacheMap;
  }

  @Override
  public void onComplete(THeartbeatResp heartbeatResp) {
    dataNodeHeartbeatCache.cacheHeartBeat(
        new HeartbeatPackage(heartbeatResp.getHeartbeatTimestamp(), System.currentTimeMillis()));

    if (heartbeatResp.isSetJudgedLeaders()) {
      heartbeatResp
          .getJudgedLeaders()
          .forEach(
              (consensusGroupId, isLeader) -> {
                if (isLeader) {
                  regionGroupCacheMap
                      .computeIfAbsent(consensusGroupId, empty -> new RegionGroupCache())
                      .updateLeader(
                          heartbeatResp.getHeartbeatTimestamp(), dataNodeLocation.getDataNodeId());
                }
              });
    }
  }

  @Override
  public void onError(Exception e) {
    LOGGER.warn(
        "Heartbeat error on DataNode: {id={}, internalEndPoint={}}",
        dataNodeLocation.getDataNodeId(),
        dataNodeLocation.getInternalEndPoint(),
        e);
  }
}
