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

import org.apache.iotdb.commons.cluster.RegionStatus;
import org.apache.iotdb.confignode.manager.load.cache.LoadCache;
import org.apache.iotdb.confignode.manager.load.cache.node.NodeHeartbeatSample;
import org.apache.iotdb.confignode.manager.load.cache.region.RegionHeartbeatSample;
import org.apache.iotdb.confignode.manager.pipe.runtime.PipeRuntimeCoordinator;
import org.apache.iotdb.mpp.rpc.thrift.THeartbeatResp;
import org.apache.iotdb.tsfile.utils.Pair;

import org.apache.thrift.async.AsyncMethodCallback;

import java.util.Map;
import java.util.function.Consumer;

public class DataNodeHeartbeatHandler implements AsyncMethodCallback<THeartbeatResp> {

  private final int nodeId;

  private final LoadCache loadCache;

  private final Map<Integer, Long> deviceNum;
  private final Map<Integer, Long> timeSeriesNum;
  private final Map<Integer, Long> regionDisk;

  private final Consumer<Map<Integer, Long>> schemaQuotaRespProcess;

  private final PipeRuntimeCoordinator pipeRuntimeCoordinator;

  public DataNodeHeartbeatHandler(
      int nodeId,
      LoadCache loadCache,
      Map<Integer, Long> deviceNum,
      Map<Integer, Long> timeSeriesNum,
      Map<Integer, Long> regionDisk,
      Consumer<Map<Integer, Long>> schemaQuotaRespProcess,
      PipeRuntimeCoordinator pipeRuntimeCoordinator) {

    this.nodeId = nodeId;
    this.loadCache = loadCache;
    this.deviceNum = deviceNum;
    this.timeSeriesNum = timeSeriesNum;
    this.regionDisk = regionDisk;
    this.schemaQuotaRespProcess = schemaQuotaRespProcess;
    this.pipeRuntimeCoordinator = pipeRuntimeCoordinator;
  }

  @Override
  public void onComplete(THeartbeatResp heartbeatResp) {
    long receiveTime = System.currentTimeMillis();

    // Update NodeCache
    loadCache.cacheDataNodeHeartbeatSample(
        nodeId, new NodeHeartbeatSample(heartbeatResp, receiveTime));

    heartbeatResp
        .getJudgedLeaders()
        .forEach(
            (regionGroupId, isLeader) -> {
              // Update RegionGroupCache
              loadCache.cacheRegionHeartbeatSample(
                  regionGroupId,
                  nodeId,
                  new RegionHeartbeatSample(
                      heartbeatResp.getHeartbeatTimestamp(),
                      receiveTime,
                      // Region will inherit DataNode's status
                      RegionStatus.parse(heartbeatResp.getStatus())));

              if (Boolean.TRUE.equals(isLeader)) {
                // Update leaderCache
                loadCache.cacheLeaderSample(
                    regionGroupId, new Pair<>(heartbeatResp.getHeartbeatTimestamp(), nodeId));
              }
            });

    if (heartbeatResp.getRegionDeviceNumMap() != null) {
      deviceNum.putAll(heartbeatResp.getRegionDeviceNumMap());
    }
    if (heartbeatResp.getRegionTimeSeriesNumMap() != null) {
      timeSeriesNum.putAll(heartbeatResp.getRegionTimeSeriesNumMap());
    }
    if (heartbeatResp.getRegionDisk() != null) {
      regionDisk.putAll(heartbeatResp.getRegionDisk());
    }
    if (heartbeatResp.getSchemaLimitLevel() != null) {
      switch (heartbeatResp.getSchemaLimitLevel()) {
        case DEVICE:
          schemaQuotaRespProcess.accept(heartbeatResp.getRegionDeviceNumMap());
          break;
        case TIMESERIES:
          schemaQuotaRespProcess.accept(heartbeatResp.getRegionTimeSeriesNumMap());
          break;
        default:
          break;
      }
    }
    if (heartbeatResp.getPipeMetaList() != null) {
      pipeRuntimeCoordinator.parseHeartbeat(nodeId, heartbeatResp.getPipeMetaList());
    }
  }

  @Override
  public void onError(Exception e) {
    // Do nothing
  }
}
