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

package org.apache.iotdb.confignode.procedure.impl.schema;

import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.commons.cluster.NodeStatus;
import org.apache.iotdb.confignode.conf.ConfigNodeDescriptor;
import org.apache.iotdb.confignode.manager.ConfigManager;
import org.apache.iotdb.confignode.manager.load.LoadManager;
import org.apache.iotdb.confignode.manager.node.NodeManager;

import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SchemaUtilsTest {

  private static final int STOPPED_DATA_NODE_ID = 1;
  private static final int UNKNOWN_DATA_NODE_ID = 2;
  private static final int RUNNING_DATA_NODE_ID = 3;

  private static TDataNodeLocation locationOf(final int dataNodeId) {
    return new TDataNodeLocation(
        dataNodeId,
        new TEndPoint("127.0.0.1", dataNodeId * 1000),
        new TEndPoint("127.0.0.1", dataNodeId * 1000 + 1),
        new TEndPoint("127.0.0.1", dataNodeId * 1000 + 2),
        new TEndPoint("127.0.0.1", dataNodeId * 1000 + 3),
        new TEndPoint("127.0.0.1", dataNodeId * 1000 + 4));
  }

  @Test
  public void testFilterFencedDataNodeTreatsStoppedLikeUnknown() {
    final ConfigManager configManager = mock(ConfigManager.class);
    final NodeManager nodeManager = mock(NodeManager.class);
    final LoadManager loadManager = mock(LoadManager.class);
    when(configManager.getNodeManager()).thenReturn(nodeManager);
    when(configManager.getLoadManager()).thenReturn(loadManager);

    final Map<Integer, TDataNodeLocation> registered = new HashMap<>();
    registered.put(STOPPED_DATA_NODE_ID, locationOf(STOPPED_DATA_NODE_ID));
    registered.put(UNKNOWN_DATA_NODE_ID, locationOf(UNKNOWN_DATA_NODE_ID));
    registered.put(RUNNING_DATA_NODE_ID, locationOf(RUNNING_DATA_NODE_ID));
    when(nodeManager.getRegisteredDataNodeLocations()).thenReturn(registered);
    when(loadManager.getNodeStatus(STOPPED_DATA_NODE_ID)).thenReturn(NodeStatus.Stopped);
    when(loadManager.getNodeStatus(UNKNOWN_DATA_NODE_ID)).thenReturn(NodeStatus.Unknown);
    when(loadManager.getNodeStatus(RUNNING_DATA_NODE_ID)).thenReturn(NodeStatus.Running);

    // A negative fence threshold makes every never-contacted DataNode read as fenced, so the
    // status condition decides alone.
    final long originalFenceMs =
        ConfigNodeDescriptor.getInstance().getConf().getMetadataLeaseFenceMs();
    try {
      ConfigNodeDescriptor.getInstance().getConf().setMetadataLeaseFenceMs(-100_000L);

      final Map<Integer, TDataNodeLocation> filtered =
          SchemaUtils.filterFencedDataNode(configManager);

      // An additionally fenced Stopped node is skipped, exactly like an additionally fenced
      // Unknown node. A Running node is kept even when fenced.
      Assert.assertFalse(filtered.containsKey(STOPPED_DATA_NODE_ID));
      Assert.assertFalse(filtered.containsKey(UNKNOWN_DATA_NODE_ID));
      Assert.assertTrue(filtered.containsKey(RUNNING_DATA_NODE_ID));
    } finally {
      ConfigNodeDescriptor.getInstance().getConf().setMetadataLeaseFenceMs(originalFenceMs);
    }
  }
}
