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
import org.apache.iotdb.commons.cluster.NodeStatus;
import org.apache.iotdb.commons.cluster.NodeType;
import org.apache.iotdb.commons.cluster.RegionStatus;
import org.apache.iotdb.confignode.manager.lease.DataNodeContactTracker;
import org.apache.iotdb.confignode.manager.load.LoadManager;
import org.apache.iotdb.confignode.manager.load.cache.LoadCache;
import org.apache.iotdb.confignode.manager.pipe.coordinator.runtime.PipeRuntimeCoordinator;
import org.apache.iotdb.mpp.rpc.thrift.TDataNodeHeartbeatResp;

import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DataNodeHeartbeatHandlerTest {

  @Test
  public void testStoppedHeartbeatUpdatesNodeAndRegions() {
    int nodeId = 7;
    TConsensusGroupId dataRegionId = new TConsensusGroupId(TConsensusGroupType.DataRegion, 10);
    TConsensusGroupId schemaRegionId = new TConsensusGroupId(TConsensusGroupType.SchemaRegion, 20);
    Map<TConsensusGroupId, Boolean> judgedLeaders =
        Map.of(dataRegionId, false, schemaRegionId, false);
    LoadCache loadCache = new LoadCache();
    loadCache.createNodeHeartbeatCache(NodeType.DataNode, nodeId);
    judgedLeaders
        .keySet()
        .forEach(id -> loadCache.createRegionGroupHeartbeatCache("root.db", id, Set.of(nodeId)));
    LoadManager loadManager = mock(LoadManager.class);
    when(loadManager.getLoadCache()).thenReturn(loadCache);
    DataNodeHeartbeatHandler handler =
        new DataNodeHeartbeatHandler(
            nodeId,
            loadManager,
            new HashMap<>(),
            new HashMap<>(),
            new HashMap<>(),
            ignored -> {},
            ignored -> {},
            mock(PipeRuntimeCoordinator.class));

    try {
      handler.onComplete(
          new TDataNodeHeartbeatResp()
              .setHeartbeatTimestamp(System.nanoTime())
              .setStatus(NodeStatus.Running.getStatus())
              .setJudgedLeaders(judgedLeaders));
      loadCache.updateNodeStatistics();
      loadCache.updateRegionGroupStatistics();
      assertEquals(NodeStatus.Running, loadCache.getNodeStatus(nodeId));
      for (TConsensusGroupId regionId : judgedLeaders.keySet()) {
        assertEquals(RegionStatus.Running, loadCache.getRegionStatus(regionId, nodeId));
      }

      handler.onComplete(
          new TDataNodeHeartbeatResp()
              .setHeartbeatTimestamp(System.nanoTime())
              .setStatus(NodeStatus.Stopped.getStatus())
              .setJudgedLeaders(judgedLeaders));
      loadCache.updateNodeStatistics();
      loadCache.updateRegionGroupStatistics();
      assertEquals(NodeStatus.Stopped, loadCache.getNodeStatus(nodeId));
      for (TConsensusGroupId regionId : judgedLeaders.keySet()) {
        assertEquals(
            RegionStatus.Unknown, loadCache.getRegionCacheLastSampleStatus(regionId, nodeId));
        assertEquals(RegionStatus.Unknown, loadCache.getRegionStatus(regionId, nodeId));
      }
    } finally {
      DataNodeContactTracker.getInstance().removeDataNode(nodeId);
    }
  }
}
