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
package org.apache.iotdb.confignode.manager.node;

import org.apache.iotdb.commons.cluster.NodeStatus;
import org.apache.iotdb.confignode.manager.node.heartbeat.DataNodeHeartbeatCache;
import org.apache.iotdb.confignode.manager.node.heartbeat.NodeHeartbeatSample;
import org.apache.iotdb.mpp.rpc.thrift.THeartbeatResp;

import org.junit.Assert;
import org.junit.Test;

public class NodeCacheTest {

  @Test
  public void forceUpdateTest() {
    DataNodeHeartbeatCache dataNodeHeartbeatCache = new DataNodeHeartbeatCache();

    // Test default
    Assert.assertEquals(NodeStatus.Unknown, dataNodeHeartbeatCache.getNodeStatus());
    Assert.assertEquals(Long.MAX_VALUE, dataNodeHeartbeatCache.getLoadScore());

    // Test force update to RunningStatus
    long currentTime = System.currentTimeMillis() - 2000;
    dataNodeHeartbeatCache.forceUpdate(
        new NodeHeartbeatSample(
            new THeartbeatResp(currentTime, NodeStatus.Running.getStatus()), currentTime));
    Assert.assertEquals(NodeStatus.Running, dataNodeHeartbeatCache.getNodeStatus());
    Assert.assertEquals(0, dataNodeHeartbeatCache.getLoadScore());

    // Test force update to ReadOnlyStatus
    currentTime += 2000;
    dataNodeHeartbeatCache.forceUpdate(
        new NodeHeartbeatSample(
            new THeartbeatResp(currentTime, NodeStatus.ReadOnly.getStatus()), currentTime));
    Assert.assertEquals(NodeStatus.ReadOnly, dataNodeHeartbeatCache.getNodeStatus());
    Assert.assertEquals(Long.MAX_VALUE, dataNodeHeartbeatCache.getLoadScore());
  }

  @Test
  public void periodicUpdateTest() {
    DataNodeHeartbeatCache dataNodeHeartbeatCache = new DataNodeHeartbeatCache();
    long currentTime = System.currentTimeMillis();
    dataNodeHeartbeatCache.cacheHeartbeatSample(
        new NodeHeartbeatSample(
            new THeartbeatResp(currentTime, NodeStatus.Running.getStatus()), currentTime));
    Assert.assertTrue(dataNodeHeartbeatCache.periodicUpdate());
    Assert.assertEquals(NodeStatus.Running, dataNodeHeartbeatCache.getNodeStatus());
    Assert.assertEquals(0, dataNodeHeartbeatCache.getLoadScore());
  }
}
