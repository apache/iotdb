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
import org.apache.iotdb.common.rpc.thrift.TConsensusGroupType;
import org.apache.iotdb.commons.client.ThriftClient;
import org.apache.iotdb.commons.cluster.NodeStatus;
import org.apache.iotdb.commons.cluster.NodeType;
import org.apache.iotdb.commons.cluster.RegionStatus;
import org.apache.iotdb.confignode.conf.ConfigNodeConfig;
import org.apache.iotdb.confignode.conf.ConfigNodeDescriptor;
import org.apache.iotdb.confignode.manager.load.LoadManager;
import org.apache.iotdb.confignode.manager.load.cache.consensus.ConsensusGroupHeartbeatSample;
import org.apache.iotdb.confignode.manager.load.cache.node.NodeHeartbeatSample;
import org.apache.iotdb.confignode.manager.load.cache.region.RegionHeartbeatSample;
import org.apache.iotdb.confignode.manager.pipe.coordinator.runtime.PipeRuntimeCoordinator;
import org.apache.iotdb.consensus.ConsensusFactory;
import org.apache.iotdb.mpp.rpc.thrift.TDataNodeHeartbeatResp;

import org.apache.thrift.async.AsyncMethodCallback;

import java.util.Collections;
import java.util.Map;
import java.util.function.Consumer;

public class DataNodeHeartbeatHandler implements AsyncMethodCallback<TDataNodeHeartbeatResp> {

  private static final ConfigNodeConfig CONF = ConfigNodeDescriptor.getInstance().getConf();
  private static final boolean SCHEMA_REGION_SHOULD_CACHE_CONSENSUS_SAMPLE =
      ConsensusFactory.RATIS_CONSENSUS.equals(CONF.getSchemaRegionConsensusProtocolClass());
  private static final boolean DATA_REGION_SHOULD_CACHE_CONSENSUS_SAMPLE =
      ConsensusFactory.RATIS_CONSENSUS.equals(CONF.getDataRegionConsensusProtocolClass());

  private final int nodeId;

  private final LoadManager loadManager;

  private final Map<Integer, Long> deviceNum;
  private final Map<Integer, Long> timeSeriesNum;
  private final Map<Integer, Long> regionDisk;

  private final Consumer<Map<Integer, Long>> seriesUsageRespProcess;
  private final Consumer<Map<Integer, Long>> deviceUsageRespProcess;

  private final PipeRuntimeCoordinator pipeRuntimeCoordinator;

  public DataNodeHeartbeatHandler(
      int nodeId,
      LoadManager loadManager,
      Map<Integer, Long> deviceNum,
      Map<Integer, Long> timeSeriesNum,
      Map<Integer, Long> regionDisk,
      Consumer<Map<Integer, Long>> seriesUsageRespProcess,
      Consumer<Map<Integer, Long>> deviceUsageRespProcess,
      PipeRuntimeCoordinator pipeRuntimeCoordinator) {

    this.nodeId = nodeId;
    this.loadManager = loadManager;
    this.deviceNum = deviceNum;
    this.timeSeriesNum = timeSeriesNum;
    this.regionDisk = regionDisk;
    this.seriesUsageRespProcess = seriesUsageRespProcess;
    this.deviceUsageRespProcess = deviceUsageRespProcess;
    this.pipeRuntimeCoordinator = pipeRuntimeCoordinator;
  }

  @Override
  public void onComplete(TDataNodeHeartbeatResp heartbeatResp) {
    cacheNodeHeartbeatSample(heartbeatResp);
    cacheRegionGroupHeartbeatSamples(heartbeatResp);
    cacheUsageSamples(heartbeatResp);
    cachePipeHeartbeat(heartbeatResp);
    cacheConfirmedConfigNodeEndPoints(heartbeatResp);
    cacheRegionSizeSamples(heartbeatResp);
  }

  private void cacheNodeHeartbeatSample(TDataNodeHeartbeatResp heartbeatResp) {
    loadManager
        .getLoadCache()
        .cacheDataNodeHeartbeatSample(nodeId, new NodeHeartbeatSample(heartbeatResp));
  }

  private void cacheRegionGroupHeartbeatSamples(TDataNodeHeartbeatResp heartbeatResp) {
    RegionStatus regionStatus = RegionStatus.valueOf(heartbeatResp.getStatus());

    Map<TConsensusGroupId, Boolean> judgedLeaders =
        heartbeatResp.isSetJudgedLeaders()
            ? heartbeatResp.getJudgedLeaders()
            : Collections.emptyMap();
    judgedLeaders.forEach(
        (regionGroupId, isLeader) -> {
          cacheRegionHeartbeatSample(heartbeatResp, regionStatus, regionGroupId);
          cacheConsensusSampleIfNeeded(heartbeatResp, regionGroupId, isLeader);
        });
  }

  private void cacheRegionHeartbeatSample(
      TDataNodeHeartbeatResp heartbeatResp,
      RegionStatus dataNodeRegionStatus,
      TConsensusGroupId regionGroupId) {
    loadManager
        .getLoadCache()
        .cacheRegionHeartbeatSample(
            regionGroupId,
            nodeId,
            new RegionHeartbeatSample(
                heartbeatResp.getHeartbeatTimestamp(),
                getRegionHeartbeatStatus(regionGroupId, dataNodeRegionStatus)),
            false);
  }

  private RegionStatus getRegionHeartbeatStatus(
      TConsensusGroupId regionGroupId, RegionStatus dataNodeRegionStatus) {
    return dataNodeRegionStatus == RegionStatus.Removing
        ? loadManager.getLoadCache().getRegionCacheLastSampleStatus(regionGroupId, nodeId)
        : dataNodeRegionStatus;
  }

  private void cacheConsensusSampleIfNeeded(
      TDataNodeHeartbeatResp heartbeatResp, TConsensusGroupId regionGroupId, Boolean isLeader) {
    if (!Boolean.TRUE.equals(isLeader)
        || !shouldCacheConsensusSample(regionGroupId)
        || !hasConsensusLogicalTimestamp(heartbeatResp, regionGroupId)) {
      return;
    }

    loadManager
        .getLoadCache()
        .cacheConsensusSample(
            regionGroupId,
            new ConsensusGroupHeartbeatSample(
                heartbeatResp.getConsensusLogicalTimeMap().get(regionGroupId), nodeId));
  }

  private boolean shouldCacheConsensusSample(TConsensusGroupId regionGroupId) {
    return (TConsensusGroupType.SchemaRegion.equals(regionGroupId.getType())
            && SCHEMA_REGION_SHOULD_CACHE_CONSENSUS_SAMPLE)
        || (TConsensusGroupType.DataRegion.equals(regionGroupId.getType())
            && DATA_REGION_SHOULD_CACHE_CONSENSUS_SAMPLE);
  }

  private boolean hasConsensusLogicalTimestamp(
      TDataNodeHeartbeatResp heartbeatResp, TConsensusGroupId regionGroupId) {
    return heartbeatResp.isSetConsensusLogicalTimeMap()
        && heartbeatResp.getConsensusLogicalTimeMap().containsKey(regionGroupId);
  }

  private void cacheUsageSamples(TDataNodeHeartbeatResp heartbeatResp) {
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
  }

  private void cachePipeHeartbeat(TDataNodeHeartbeatResp heartbeatResp) {
    if (heartbeatResp.getPipeMetaList() != null) {
      pipeRuntimeCoordinator.parseHeartbeat(
          nodeId,
          heartbeatResp.getPipeMetaList(),
          heartbeatResp.getPipeCompletedList(),
          heartbeatResp.getPipeRemainingEventCountList(),
          heartbeatResp.getPipeRemainingTimeList(),
          heartbeatResp.getPipeDegradedStatusList());
    }
  }

  private void cacheConfirmedConfigNodeEndPoints(TDataNodeHeartbeatResp heartbeatResp) {
    if (heartbeatResp.isSetConfirmedConfigNodeEndPoints()) {
      loadManager
          .getLoadCache()
          .updateConfirmedConfigNodeEndPoints(
              nodeId, heartbeatResp.getConfirmedConfigNodeEndPoints());
    }
  }

  private void cacheRegionSizeSamples(TDataNodeHeartbeatResp heartbeatResp) {
    if (heartbeatResp.isSetRegionDisk()) {
      loadManager.getLoadCache().updateRegionSizeMap(nodeId, heartbeatResp.getRegionDisk());
    }
    if (heartbeatResp.isSetDataRegionRawDataSize()) {
      loadManager
          .getLoadCache()
          .updateRegionRawSizeMap(nodeId, heartbeatResp.getDataRegionRawDataSize());
    }
  }

  @Override
  public void onError(Exception e) {
    if (ThriftClient.isConnectionBroken(e)) {
      loadManager.forceUpdateNodeCache(
          NodeType.DataNode, nodeId, new NodeHeartbeatSample(NodeStatus.Unknown));
    }
    loadManager.getLoadCache().resetHeartbeatProcessing(nodeId);
  }
}
