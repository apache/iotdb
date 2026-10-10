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

package org.apache.iotdb.confignode.manager.subscription.runtime;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TConsensusGroupType;
import org.apache.iotdb.commons.cluster.NodeStatus;
import org.apache.iotdb.commons.subscription.meta.topic.TopicMeta;
import org.apache.iotdb.confignode.manager.ConfigManager;
import org.apache.iotdb.confignode.manager.ProcedureManager;
import org.apache.iotdb.confignode.manager.load.LoadManager;
import org.apache.iotdb.confignode.manager.load.cache.node.NodeStatistics;
import org.apache.iotdb.confignode.manager.load.subscriber.NodeStatisticsChangeEvent;
import org.apache.iotdb.confignode.manager.subscription.SubscriptionCoordinator;
import org.apache.iotdb.confignode.manager.subscription.SubscriptionManager;
import org.apache.iotdb.confignode.persistence.subscription.SubscriptionInfo;
import org.apache.iotdb.rpc.subscription.config.TopicConfig;

import org.apache.tsfile.utils.Pair;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class SubscriptionRuntimeCoordinatorTest {

  private static final int DATA_NODE_ID = 1;

  private final ConfigManager configManager = mock(ConfigManager.class);
  private final SubscriptionManager subscriptionManager = mock(SubscriptionManager.class);
  private final SubscriptionCoordinator subscriptionCoordinator =
      mock(SubscriptionCoordinator.class);
  private final SubscriptionInfo subscriptionInfo = mock(SubscriptionInfo.class);
  private final TopicMeta topicMeta = mock(TopicMeta.class);
  private final TopicConfig topicConfig = mock(TopicConfig.class);
  private final LoadManager loadManager = mock(LoadManager.class);
  private final ProcedureManager procedureManager = mock(ProcedureManager.class);

  private final SubscriptionRuntimeCoordinator coordinator =
      new SubscriptionRuntimeCoordinator(configManager);

  private void stubConsensusBasedTopic() {
    when(configManager.getSubscriptionManager()).thenReturn(subscriptionManager);
    when(subscriptionManager.getSubscriptionCoordinator()).thenReturn(subscriptionCoordinator);
    when(subscriptionCoordinator.getSubscriptionInfo()).thenReturn(subscriptionInfo);
    when(subscriptionInfo.getAllTopicMeta()).thenReturn(Collections.singletonList(topicMeta));
    when(topicMeta.getConfig()).thenReturn(topicConfig);
    when(topicConfig.isIncrementalMode()).thenReturn(true);

    when(configManager.getLoadManager()).thenReturn(loadManager);
    // A seeded DataRegion leader pair makes the refresh map non-empty.
    when(loadManager.getRegionLeaderMap())
        .thenReturn(
            Collections.singletonMap(
                new TConsensusGroupId(TConsensusGroupType.DataRegion, 1), DATA_NODE_ID));
    when(configManager.getProcedureManager()).thenReturn(procedureManager);
  }

  private NodeStatisticsChangeEvent eventOf(
      final NodeStatus oldStatus, final NodeStatus newStatus) {
    final Map<Integer, Pair<NodeStatistics, NodeStatistics>> map = new HashMap<>();
    map.put(DATA_NODE_ID, new Pair<>(new NodeStatistics(oldStatus), new NodeStatistics(newStatus)));
    return new NodeStatisticsChangeEvent(map);
  }

  @Test
  public void testStoppedStatusTriggersRuntimeRefresh() {
    stubConsensusBasedTopic();

    coordinator.handleNodeStatisticsChange(eventOf(NodeStatus.Running, NodeStatus.Stopped));

    // A Stopped node is handled like Unknown/Removing: its runtime leader pairs are refreshed.
    verify(procedureManager).subscriptionHandleLeaderChange(any(Map.class), anyLong());
  }

  @Test
  public void testUnknownStatusTriggersRuntimeRefresh() {
    stubConsensusBasedTopic();

    coordinator.handleNodeStatisticsChange(eventOf(NodeStatus.Running, NodeStatus.Unknown));

    verify(procedureManager).subscriptionHandleLeaderChange(any(Map.class), anyLong());
  }

  @Test
  public void testReadOnlyStatusDoesNotTriggerRuntimeRefresh() {
    stubConsensusBasedTopic();

    coordinator.handleNodeStatisticsChange(eventOf(NodeStatus.Running, NodeStatus.ReadOnly));

    // ReadOnly is not runtime-sensitive: the runtime leader pairs stay untouched.
    verify(procedureManager, never()).subscriptionHandleLeaderChange(any(Map.class), anyLong());
  }
}
