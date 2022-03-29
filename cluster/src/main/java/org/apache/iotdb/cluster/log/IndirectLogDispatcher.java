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

package org.apache.iotdb.cluster.log;

import org.apache.iotdb.cluster.config.ClusterDescriptor;
import org.apache.iotdb.cluster.query.manage.QueryCoordinator;
import org.apache.iotdb.cluster.rpc.thrift.AppendEntryRequest;
import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.cluster.server.member.RaftMember;
import org.apache.iotdb.cluster.server.monitor.NodeStatusManager;
import org.apache.iotdb.cluster.server.monitor.Timer.Statistic;
import org.apache.iotdb.cluster.utils.ClusterUtils;
import org.apache.iotdb.cluster.utils.WeightedList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;

/**
 * IndirectLogDispatcher sends entries only to a pre-selected subset of followers instead of all
 * followers and let the selected followers to broadcast the log to other followers.
 */
public class IndirectLogDispatcher extends LogDispatcher {

  private static final Logger logger = LoggerFactory.getLogger(IndirectLogDispatcher.class);

  private Map<Node, List<Node>> directToIndirectFollowerMap = new HashMap<>();

  public IndirectLogDispatcher(RaftMember member) {
    super(member);
    recalculateDirectFollowerMap();
    useBatchInLogCatchUp = false;
  }

  @Override
  LogDispatcher.DispatcherThread newDispatcherThread(
      Node node, BlockingQueue<SendLogRequest> logBlockingQueue) {
    return new DispatcherThread(node, logBlockingQueue);
  }

  @Override
  void createQueueAndBindingThreads() {
    for (Node node : member.getAllNodes()) {
      if (!ClusterUtils.isNodeEquals(node, member.getThisNode())) {
        nodesEnabled.put(node, false);
        nodesLogQueues.put(node, createQueueAndBindingThread(node));
      }
    }
  }

  @Override
  public void offer(SendLogRequest request) {
    super.offer(request);
    recalculateDirectFollowerMap();
  }

  @Override
  protected SendLogRequest transformRequest(Node node, SendLogRequest request) {
    SendLogRequest newRequest = new SendLogRequest(request);
    // copy the RPC request so each request can have different sub-receivers but the same log
    // binary and other fields
    newRequest.setAppendEntryRequest(new AppendEntryRequest(newRequest.getAppendEntryRequest()));
    newRequest.getAppendEntryRequest().setSubReceivers(directToIndirectFollowerMap.get(node));
    return newRequest;
  }

  public void recalculateDirectFollowerMap() {
    List<Node> allNodes = new ArrayList<>(member.getAllNodes());
    allNodes.removeIf(n -> ClusterUtils.isNodeEquals(n, member.getThisNode()));
    Collections.shuffle(allNodes);
    List<Node> orderedNodes = allNodes;

    nodesEnabled.clear();
    directToIndirectFollowerMap.clear();

    if (ClusterDescriptor.getInstance().getConfig().isOptimizeIndirectBroadcasting()) {
      QueryCoordinator instance = QueryCoordinator.getINSTANCE();
      orderedNodes = instance.reorderNodes(allNodes);
      long thisLoad =
          Statistic.RAFT_SENDER_SEND_LOG.getCnt() + Statistic.RAFT_RECEIVER_RELAY_LOG.getCnt() + 1;
      long minLoad =
          NodeStatusManager.getINSTANCE()
                  .getNodeStatus(orderedNodes.get(0), false)
                  .getStatus()
                  .fanoutRequestNum
              + 1;
      double loadFactor = 1.05;
      WeightedList<Node> firstLevelCandidates = new WeightedList<>();
      firstLevelCandidates.insert(
          orderedNodes.get(0),
          1.0
              / (NodeStatusManager.getINSTANCE()
                      .getNodeStatus(orderedNodes.get(0), false)
                      .getStatus()
                      .fanoutRequestNum
                  + 1));
      int firstLevelSize = 1;

      for (int i = 1, orderedNodesSize = orderedNodes.size(); i < orderedNodesSize; i++) {
        Node orderedNode = orderedNodes.get(i);
        long nodeLoad =
            NodeStatusManager.getINSTANCE()
                    .getNodeStatus(orderedNode, false)
                    .getStatus()
                    .fanoutRequestNum
                + 1;
        if (nodeLoad * 1.0 <= minLoad * loadFactor) {
          firstLevelCandidates.insert(orderedNode, 1.0 / nodeLoad);
        }
        if (nodeLoad > thisLoad) {
          firstLevelSize = (int) Math.max(firstLevelSize, nodeLoad / thisLoad);
        }
      }

      if (firstLevelSize > firstLevelCandidates.size()) {
        firstLevelSize = firstLevelCandidates.size();
      }

      List<Node> firstLevelNodes = firstLevelCandidates.select(firstLevelSize);

      Map<Node, List<Node>> secondLevelNodeMap = new HashMap<>();
      orderedNodes.removeAll(firstLevelNodes);
      for (int i = 0; i < orderedNodes.size(); i++) {
        Node firstLevelNode = firstLevelNodes.get(i % firstLevelSize);
        secondLevelNodeMap
            .computeIfAbsent(firstLevelNode, n -> new ArrayList<>())
            .add(orderedNodes.get(i));
      }

      for (Node firstLevelNode : firstLevelNodes) {
        directToIndirectFollowerMap.put(
            firstLevelNode,
            secondLevelNodeMap.getOrDefault(firstLevelNode, Collections.emptyList()));
        nodesEnabled.put(firstLevelNode, true);
      }

    } else {
      for (int i = 0, j = orderedNodes.size() - 1; i <= j; i++, j--) {
        if (i != j) {
          directToIndirectFollowerMap.put(
              orderedNodes.get(i), Collections.singletonList(orderedNodes.get(j)));
        } else {
          directToIndirectFollowerMap.put(orderedNodes.get(i), Collections.emptyList());
        }
        nodesEnabled.put(orderedNodes.get(i), true);
      }
    }

    logger.debug("New relay map: {}", directToIndirectFollowerMap);
  }
}
