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

import org.apache.iotdb.commons.client.ThriftClient;
import org.apache.iotdb.commons.cluster.NodeStatus;
import org.apache.iotdb.commons.cluster.NodeType;
import org.apache.iotdb.commons.cluster.RegionStatus;
import org.apache.iotdb.confignode.manager.load.cache.LoadCache;
import org.apache.iotdb.confignode.manager.load.cache.node.NodeHeartbeatSample;
import org.apache.iotdb.confignode.manager.load.cache.region.RegionHeartbeatSample;
import org.apache.iotdb.confignode.manager.pipe.coordinator.runtime.PipeRuntimeCoordinator;
import org.apache.iotdb.mpp.rpc.thrift.TDataNodeHeartbeatResp;
import org.apache.iotdb.tsfile.utils.Pair;

import org.apache.thrift.async.AsyncMethodCallback;

import java.util.Map;
import java.util.function.Consumer;

public class DataNodeHeartbeatHandler implements AsyncMethodCallback<TDataNodeHeartbeatResp> {

  private final int nodeId;

  private final LoadCache loadCache;

  private final Map<Integer, Long> deviceNum;
  private final Map<Integer, Long> timeSeriesNum;
  private final Map<Integer, Long> regionDisk;

  private final Consumer<Map<Integer, Long>> seriesUsageRespProcess;
  private final Consumer<Map<Integer, Long>> deviceUsageRespProcess;

  private final PipeRuntimeCoordinator pipeRuntimeCoordinator;

  public DataNodeHeartbeatHandler(
      int nodeId,
      LoadCache loadCache,
      Map<Integer, Long> deviceNum,
      Map<Integer, Long> timeSeriesNum,
      Map<Integer, Long> regionDisk,
      Consumer<Map<Integer, Long>> seriesUsageRespProcess,
      Consumer<Map<Integer, Long>> deviceUsageRespProcess,
      PipeRuntimeCoordinator pipeRuntimeCoordinator) {

    this.nodeId = nodeId;
    this.loadCache = loadCache;
    this.deviceNum = deviceNum;
    this.timeSeriesNum = timeSeriesNum;
    this.regionDisk = regionDisk;
    this.seriesUsageRespProcess = seriesUsageRespProcess;
    this.deviceUsageRespProcess = deviceUsageRespProcess;
    this.pipeRuntimeCoordinator = pipeRuntimeCoordinator;
  }

  @Override
  public void onComplete(TDataNodeHeartbeatResp heartbeatResp) {
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

    if (heartbeatResp.getRegionDeviceUsageMap() != null) {
      deviceNum.putAll(heartbeatResp.getRegionDeviceUsageMap());
      deviceUsageRespProcess.accept(heartbeatResp.getRegionDeviceUsageMap());
    }
    if (heartbeatResp.getRegionSeriesUsageMap() != null) {
      timeSeriesNum.putAll(heartbeatResp.getRegionSeriesUsageMap());
      seriesUsageRespProcess.accept(heartbeatResp.getRegionSeriesUsageMap());
    }
    if (heartbeatResp.getRegionDisk() != null) {
      regionDisk.putAll(heartbeatResp.getRegionDisk());
    }
    if (heartbeatResp.getPipeMetaList() != null) {
      pipeRuntimeCoordinator.parseHeartbeat(nodeId, heartbeatResp.getPipeMetaList());
    }
  }

  @Override
  public void onError(Exception e) {
    if (ThriftClient.isConnectionBroken(e)) {
      loadCache.forceUpdateNodeCache(
          NodeType.DataNode, nodeId, NodeHeartbeatSample.generateDefaultSample(NodeStatus.Unknown));
    }
  }
}
