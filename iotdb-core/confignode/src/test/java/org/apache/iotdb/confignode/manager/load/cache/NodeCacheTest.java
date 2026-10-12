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
package org.apache.iotdb.confignode.manager.load.cache;

import org.apache.iotdb.commons.cluster.NodeStatus;
import org.apache.iotdb.commons.cluster.NodeType;
import org.apache.iotdb.confignode.conf.ConfigNodeConfig;
import org.apache.iotdb.confignode.conf.ConfigNodeDescriptor;
import org.apache.iotdb.confignode.manager.load.cache.node.AINodeHeartbeatCache;
import org.apache.iotdb.confignode.manager.load.cache.node.BaseNodeCache;
import org.apache.iotdb.confignode.manager.load.cache.node.ConfigNodeHeartbeatCache;
import org.apache.iotdb.confignode.manager.load.cache.node.DataNodeHeartbeatCache;
import org.apache.iotdb.confignode.manager.load.cache.node.NodeHeartbeatSample;
import org.apache.iotdb.confignode.manager.load.cache.node.NodeStatistics;
import org.apache.iotdb.mpp.rpc.thrift.TDataNodeHeartbeatResp;

import org.junit.Assert;
import org.junit.Test;

import java.util.Map;

public class NodeCacheTest {

  @Test
  public void updateStatisticsTest() {
    // Test DataNode heartbeat cache
    DataNodeHeartbeatCache dataNodeHeartbeatCache = new DataNodeHeartbeatCache(1);
    long currentTime = System.nanoTime();
    dataNodeHeartbeatCache.cacheHeartbeatSample(
        new NodeHeartbeatSample(currentTime, NodeStatus.Running));
    dataNodeHeartbeatCache.updateNodeStatistics();
    Assert.assertEquals(NodeStatus.Running, dataNodeHeartbeatCache.getNodeStatus());
    Assert.assertEquals(0, dataNodeHeartbeatCache.getLoadScore());

    // Test ConfigNode heartbeat cache
    ConfigNodeHeartbeatCache configNodeHeartbeatCache = new ConfigNodeHeartbeatCache(2);
    currentTime = System.nanoTime();
    configNodeHeartbeatCache.cacheHeartbeatSample(
        new NodeHeartbeatSample(currentTime, NodeStatus.Running));
    configNodeHeartbeatCache.updateNodeStatistics();
    Assert.assertEquals(NodeStatus.Running, configNodeHeartbeatCache.getNodeStatus());
    Assert.assertEquals(0, configNodeHeartbeatCache.getLoadScore());
  }

  @Test
  public void testRemovingAINodeStatisticsSurviveHeartbeatTimeout() {
    ConfigNodeConfig config = ConfigNodeDescriptor.getInstance().getConf();
    String detector = config.getFailureDetector();
    long timeout = config.getFailureDetectorFixedThresholdInMs();
    config.setFailureDetector(IFailureDetector.FIXED_DETECTOR);
    config.setFailureDetectorFixedThresholdInMs(0);
    try {
      AINodeHeartbeatCache cache = new AINodeHeartbeatCache(3);
      cache.trySetNodeStatus(NodeStatus.Removing, null, false);
      cache.updateNodeStatistics();
      Assert.assertEquals(NodeStatus.Removing, cache.getNodeStatus());
      Assert.assertEquals(Long.MAX_VALUE, cache.getLoadScore());
    } finally {
      config.setFailureDetector(detector);
      config.setFailureDetectorFixedThresholdInMs(timeout);
    }
  }

  @Test
  public void stoppedStatusStickyAndRevivalTest() {
    for (BaseNodeCache cache :
        new BaseNodeCache[] {
          new DataNodeHeartbeatCache(1),
          new ConfigNodeHeartbeatCache(ConfigNodeHeartbeatCache.CURRENT_NODE_ID + 1),
          new AINodeHeartbeatCache(3)
        }) {
      String cacheType = cache.getClass().getSimpleName();
      cache.cacheHeartbeatSample(new NodeHeartbeatSample(NodeStatus.Stopped));
      cache.updateNodeStatistics();
      Assert.assertEquals(cacheType, NodeStatus.Stopped, cache.getNodeStatus());
      Assert.assertEquals(cacheType, Long.MAX_VALUE, cache.getLoadScore());

      // Unknown observations cannot undo a reported stop.
      cache.cacheHeartbeatSample(new NodeHeartbeatSample(NodeStatus.Unknown));
      cache.updateNodeStatistics();
      Assert.assertEquals(cacheType, NodeStatus.Stopped, cache.getNodeStatus());

      // A live heartbeat revives a stopped node.
      cache.cacheHeartbeatSample(new NodeHeartbeatSample(NodeStatus.Running));
      cache.updateNodeStatistics();
      Assert.assertEquals(cacheType, NodeStatus.Running, cache.getNodeStatus());
      Assert.assertEquals(cacheType, 0, cache.getLoadScore());

      // A stopped node can still enter removal.
      cache.cacheHeartbeatSample(new NodeHeartbeatSample(NodeStatus.Stopped));
      cache.updateNodeStatistics();
      Assert.assertEquals(cacheType, NodeStatus.Stopped, cache.getNodeStatus());
      cache.cacheHeartbeatSample(new NodeHeartbeatSample(NodeStatus.Removing));
      cache.updateNodeStatistics();
      Assert.assertEquals(cacheType, NodeStatus.Removing, cache.getNodeStatus());

      // Ordinary updates, including fresh Running, must not cancel removal.
      for (NodeStatus observed :
          new NodeStatus[] {NodeStatus.Stopped, NodeStatus.Unknown, NodeStatus.Running}) {
        cache.cacheHeartbeatSample(new NodeHeartbeatSample(observed));
        cache.updateNodeStatistics();
        Assert.assertEquals(cacheType, NodeStatus.Removing, cache.getNodeStatus());
        Assert.assertEquals(cacheType, Long.MAX_VALUE, cache.getLoadScore());
      }

      // Management restores the requested state explicitly, including offline states.
      for (NodeStatus restored :
          new NodeStatus[] {NodeStatus.Unknown, NodeStatus.Stopped, NodeStatus.Running}) {
        cache.trySetNodeStatus(NodeStatus.Removing, null, true);
        cache.trySetNodeStatus(restored, null, true);
        Assert.assertEquals(cacheType, restored, cache.getNodeStatus());
        cache.updateNodeStatistics();
        Assert.assertEquals(cacheType, restored, cache.getNodeStatus());
      }
    }
  }

  @Test
  public void testConnectionFailureIsNotUndoneByPreviousRunningHeartbeat() {
    for (BaseNodeCache cache :
        new BaseNodeCache[] {
          new DataNodeHeartbeatCache(1),
          new ConfigNodeHeartbeatCache(ConfigNodeHeartbeatCache.CURRENT_NODE_ID + 1),
          new AINodeHeartbeatCache(3)
        }) {
      cache.cacheHeartbeatSample(new NodeHeartbeatSample(NodeStatus.Running));
      cache.updateNodeStatistics();
      Assert.assertEquals(NodeStatus.Running, cache.getNodeStatus());

      cache.trySetNodeStatus(NodeStatus.Unknown, null, false);
      Assert.assertEquals(NodeStatus.Unknown, cache.getNodeStatus());
      Assert.assertEquals(Long.MAX_VALUE, cache.getLoadScore());
      cache.updateNodeStatistics();
      Assert.assertEquals(NodeStatus.Unknown, cache.getNodeStatus());
    }
  }

  @Test
  public void testExplicitStatusUpdateDoesNotRequireLiveHeartbeat() {
    ConfigNodeConfig config = ConfigNodeDescriptor.getInstance().getConf();
    String detector = config.getFailureDetector();
    long timeout = config.getFailureDetectorFixedThresholdInMs();
    config.setFailureDetector(IFailureDetector.FIXED_DETECTOR);
    config.setFailureDetectorFixedThresholdInMs(0);
    try {
      for (BaseNodeCache cache :
          new BaseNodeCache[] {
            new DataNodeHeartbeatCache(1),
            new ConfigNodeHeartbeatCache(ConfigNodeHeartbeatCache.CURRENT_NODE_ID + 1),
            new AINodeHeartbeatCache(3)
          }) {
        cache.cacheHeartbeatSample(new NodeHeartbeatSample(0, NodeStatus.Running));
        cache.updateNodeStatistics();
        Assert.assertEquals(NodeStatus.Unknown, cache.getNodeStatus());

        // A shutdown report is known information even when heartbeat detection says unavailable.
        cache.trySetNodeStatus(NodeStatus.Stopped, null, false);
        Assert.assertEquals(NodeStatus.Stopped, cache.getNodeStatus());
        Assert.assertEquals(Long.MAX_VALUE, cache.getLoadScore());
        cache.updateNodeStatistics();
        Assert.assertEquals(NodeStatus.Stopped, cache.getNodeStatus());

        cache.trySetNodeStatus(NodeStatus.Unknown, null, false);
        Assert.assertEquals(NodeStatus.Stopped, cache.getNodeStatus());
        cache.trySetNodeStatus(NodeStatus.Unknown, null, true);
        Assert.assertEquals(NodeStatus.Unknown, cache.getNodeStatus());
      }
    } finally {
      config.setFailureDetector(detector);
      config.setFailureDetectorFixedThresholdInMs(timeout);
    }
  }

  @Test
  public void statusReasonPropagationTest() {
    DataNodeHeartbeatCache dataNodeHeartbeatCache = new DataNodeHeartbeatCache(1);

    // A heartbeat response carrying a status reason (e.g. ReadOnly + DiskFull from the DataNode)
    // publishes the reason into the node statistics.
    TDataNodeHeartbeatResp heartbeatResp =
        new TDataNodeHeartbeatResp()
            .setHeartbeatTimestamp(System.nanoTime())
            .setStatus(NodeStatus.ReadOnly.getStatus())
            .setStatusReason(NodeStatus.DISK_FULL);
    dataNodeHeartbeatCache.cacheHeartbeatSample(new NodeHeartbeatSample(heartbeatResp));
    dataNodeHeartbeatCache.updateNodeStatistics();
    Assert.assertEquals(NodeStatus.ReadOnly, dataNodeHeartbeatCache.getNodeStatus());
    Assert.assertEquals(NodeStatus.DISK_FULL, dataNodeHeartbeatCache.getNodeStatusReason());

    // A heartbeat response without a status reason clears the previous reason.
    heartbeatResp =
        new TDataNodeHeartbeatResp()
            .setHeartbeatTimestamp(System.nanoTime())
            .setStatus(NodeStatus.Running.getStatus());
    dataNodeHeartbeatCache.cacheHeartbeatSample(new NodeHeartbeatSample(heartbeatResp));
    dataNodeHeartbeatCache.updateNodeStatistics();
    Assert.assertEquals(NodeStatus.Running, dataNodeHeartbeatCache.getNodeStatus());
    Assert.assertNull(dataNodeHeartbeatCache.getNodeStatusReason());

    // An Unknown decided by the failure detector (a stale heartbeat) never carries a reason. The
    // stale sample is accepted because the fresh cache has an empty sliding window.
    DataNodeHeartbeatCache staleCache = new DataNodeHeartbeatCache(3);
    staleCache.cacheHeartbeatSample(
        new NodeHeartbeatSample(System.nanoTime() - 60_000_000_000L, NodeStatus.ReadOnly));
    staleCache.updateNodeStatistics();
    Assert.assertEquals(NodeStatus.Unknown, staleCache.getNodeStatus());
    Assert.assertNull(staleCache.getNodeStatusReason());
  }

  @Test
  public void explicitStatusUpdatesKeepTheSuppliedReason() {
    DataNodeHeartbeatCache cache = new DataNodeHeartbeatCache(1);
    // ConfigNode must use the supplied reason, without inventing Manual or applying local priority.
    for (String reason : new String[] {NodeStatus.MANUAL, NodeStatus.DISK_FULL, null}) {
      cache.trySetNodeStatus(NodeStatus.ReadOnly, reason, false);
      Assert.assertEquals(NodeStatus.ReadOnly, cache.getNodeStatus());
      Assert.assertEquals(reason, cache.getNodeStatusReason());
      Assert.assertEquals(reason, ((NodeHeartbeatSample) cache.getLastSample()).getStatusReason());

      cache.updateNodeStatistics();
      Assert.assertEquals(NodeStatus.ReadOnly, cache.getNodeStatus());
      Assert.assertEquals(reason, cache.getNodeStatusReason());
    }

    // A rejected ReadOnly update cannot attach its reason to a node that remains Removing.
    cache.trySetNodeStatus(NodeStatus.Removing, null, true);
    cache.trySetNodeStatus(NodeStatus.ReadOnly, NodeStatus.DISK_FULL, false);
    Assert.assertEquals(NodeStatus.Removing, cache.getNodeStatus());
    Assert.assertNull(cache.getNodeStatusReason());
    cache.updateNodeStatistics();
    Assert.assertEquals(NodeStatus.Removing, cache.getNodeStatus());
    Assert.assertNull(cache.getNodeStatusReason());
  }

  @Test
  public void loadCacheSnapshotKeepsStatusAndReasonFromSameStatistics() {
    LoadCache loadCache = new LoadCache();
    loadCache.createNodeHeartbeatCache(NodeType.DataNode, 1);
    loadCache.cacheDataNodeHeartbeatSample(
        1,
        new NodeHeartbeatSample(
            new TDataNodeHeartbeatResp()
                .setHeartbeatTimestamp(System.nanoTime())
                .setStatus(NodeStatus.ReadOnly.getStatus())
                .setStatusReason(NodeStatus.MANUAL)));
    loadCache.updateNodeStatistics();

    // The snapshot used by SHOW CLUSTER reads status and reason from one statistics object.
    Map<Integer, NodeStatistics> snapshot = loadCache.getCurrentNodeStatisticsMap();
    Assert.assertEquals(1, snapshot.size());
    Assert.assertEquals(NodeStatus.ReadOnly, snapshot.get(1).getStatus());
    Assert.assertEquals(NodeStatus.MANUAL, snapshot.get(1).getStatusReason());
    Assert.assertEquals(NodeStatus.MANUAL, loadCache.getNodeStatusReason(1));

    // A missing cache yields no reason and Unknown status, without a fake merged string.
    Assert.assertNull(loadCache.getNodeStatusReason(2));
    Assert.assertEquals(NodeStatus.Unknown, loadCache.getNodeStatus(2));
  }
}
