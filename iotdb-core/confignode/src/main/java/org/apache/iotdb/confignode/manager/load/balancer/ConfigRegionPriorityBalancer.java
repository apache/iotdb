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

package org.apache.iotdb.confignode.manager.load.balancer;

import org.apache.iotdb.common.rpc.thrift.TConfigNodeLocation;
import org.apache.iotdb.commons.cluster.NodeStatus;
import org.apache.iotdb.confignode.i18n.ManagerMessages;
import org.apache.iotdb.confignode.manager.IManager;
import org.apache.iotdb.confignode.manager.consensus.ConsensusManager;
import org.apache.iotdb.confignode.manager.load.cache.node.NodeStatistics;
import org.apache.iotdb.confignode.manager.load.subscriber.IClusterStatusSubscriber;
import org.apache.iotdb.confignode.manager.load.subscriber.NodeStatisticsChangeEvent;
import org.apache.iotdb.consensus.exception.ConsensusException;

import org.apache.tsfile.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.OptionalInt;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Reacts to {@link NodeStatisticsChangeEvent} on the ConfigNode leader and pushes Ratis peer
 * priorities for the ConfigRegion so candidacy in leader elections reflects the latest {@link
 * NodeStatus} of every ConfigNode (see {@link NodeStatus#priorityForStatus}).
 *
 * <p>Only fires when this ConfigNode is the ConfigRegion leader, since {@code setConfiguration}
 * must be applied through the leader. Filters events to ConfigNode peers (DataNode/AINode entries
 * are skipped) and accumulates only transitions whose priority bucket actually moves before issuing
 * a single batched reconfiguration call.
 */
public class ConfigRegionPriorityBalancer implements IClusterStatusSubscriber {

  private static final Logger LOGGER = LoggerFactory.getLogger(ConfigRegionPriorityBalancer.class);

  private final IManager configManager;

  public ConfigRegionPriorityBalancer(IManager configManager) {
    this.configManager = configManager;
  }

  @Override
  public void onNodeStatisticsChanged(NodeStatisticsChangeEvent event) {
    if (!configManager.getConsensusManager().isLeader()) {
      return;
    }
    Set<Integer> configNodeIds =
        configManager.getNodeManager().getRegisteredConfigNodes().stream()
            .map(TConfigNodeLocation::getConfigNodeId)
            .collect(Collectors.toSet());

    Map<Integer, Integer> desired = new HashMap<>();
    for (Map.Entry<Integer, Pair<NodeStatistics, NodeStatistics>> entry :
        event.getDifferentNodeStatisticsMap().entrySet()) {
      int nodeId = entry.getKey();
      if (!configNodeIds.contains(nodeId)) {
        continue;
      }
      NodeStatistics previous = entry.getValue().getLeft();
      NodeStatistics current = entry.getValue().getRight();
      if (current == null) {
        // Node disappeared from the cache; peer removal flows through addConfigNodePeer /
        // removeConfigNodePeer instead — no priority push to issue here.
        continue;
      }
      OptionalInt newPriority =
          NodeStatus.priorityForStatus(current.getStatus(), current.getStatusReason());
      if (!newPriority.isPresent()) {
        // Transient (Unknown / Removing / manual ReadOnly) — leave the priority as-is.
        continue;
      }
      OptionalInt oldPriority =
          previous == null
              ? OptionalInt.empty()
              : NodeStatus.priorityForStatus(previous.getStatus(), previous.getStatusReason());
      if (oldPriority.isPresent() && oldPriority.getAsInt() == newPriority.getAsInt()) {
        // The status moved but the priority bucket did not — no point churning the group config.
        continue;
      }
      desired.put(nodeId, newPriority.getAsInt());
    }
    if (desired.isEmpty()) {
      return;
    }
    try {
      configManager
          .getConsensusManager()
          .getConsensusImpl()
          .reconfigurePeerPriorities(ConsensusManager.DEFAULT_CONSENSUS_GROUP_ID, desired);
    } catch (ConsensusException e) {
      LOGGER.warn(ManagerMessages.RECONFIGURE_PEER_PRIORITIES_FAILED, desired, e);
    }
  }
}
