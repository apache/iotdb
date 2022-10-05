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
import org.apache.iotdb.cluster.rpc.thrift.AppendEntryRequest;
import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.cluster.server.member.RaftMember;
import org.apache.iotdb.cluster.server.monitor.NodeStatus;
import org.apache.iotdb.cluster.server.monitor.NodeStatusManager;
import org.apache.iotdb.cluster.server.monitor.Timer.Statistic;
import org.apache.iotdb.cluster.utils.ClusterUtils;
import org.apache.iotdb.cluster.utils.WeightedList;
import org.apache.iotdb.commons.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.tsfile.utils.Pair;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.iotdb.cluster.server.monitor.Timer.Statistic.RAFT_RELAYED_LEVEL1_NUM;

/**
 * IndirectLogDispatcher sends entries only to a pre-selected subset of followers instead of all
 * followers and let the selected followers to broadcast the log to other followers.
 */
public class IndirectLogDispatcher extends LogDispatcher {

  private static final Logger logger = LoggerFactory.getLogger(IndirectLogDispatcher.class);

  private Map<Node, List<Node>> directToIndirectFollowerMap = new ConcurrentHashMap<>();
  private long dispatchedEntryNum;
  private int recalculateMapInterval = -1;
  private Random random = new Random();

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
    nodesEnabled = new HashMap<>();
    for (Node node : member.getAllNodes()) {
      if (!ClusterUtils.isNodeEquals(node, member.getThisNode())) {
        nodesEnabled.put(node, false);
        BlockingQueue<SendLogRequest> logBlockingQueue;
        logBlockingQueue =
            new ArrayBlockingQueue<>(
                ClusterDescriptor.getInstance().getConfig().getMaxNumOfLogsInMem());
        nodesLogQueuesList.add(new Pair<>(node, logBlockingQueue));
      }
    }

    for (int i = 0; i < bindingThreadNum; i++) {
      for (Pair<Node, BlockingQueue<SendLogRequest>> pair : nodesLogQueuesList) {
        executorServices
            .computeIfAbsent(
                pair.left,
                n ->
                    IoTDBThreadPoolFactory.newCachedThreadPool(
                        "LogDispatcher-"
                            + member.getName()
                            + "-"
                            + ClusterUtils.nodeToString(pair.left)))
            .submit(newDispatcherThread(pair.left, pair.right));
      }
    }
  }

  @Override
  public void offer(SendLogRequest request) {
    super.offer(request);
    dispatchedEntryNum++;
    if (recalculateMapInterval > 0 && dispatchedEntryNum % recalculateMapInterval == 0) {
      recalculateDirectFollowerMap();
    }
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

  private double getNodeWeight(Node node, double maxLatency) {
    NodeStatus status = NodeStatusManager.getINSTANCE().getNodeStatus(node, false);
    //    return 1.0
    //        / (status.getStatus().fanoutRequestNum + 1);
    double pow =
        Math.pow(
            ClusterDescriptor.getInstance().getConfig().getRelayWeightBase(),
            maxLatency / status.getSendEntryLatencyStatistic().getAvg());
    status.setRelayWeight(pow);
    return pow;
  }

  public void recalculateDirectFollowerMap() {
    List<Node> allNodes = new ArrayList<>(member.getAllNodes());
    allNodes.removeIf(n -> ClusterUtils.isNodeEquals(n, member.getThisNode()));
    Collections.shuffle(allNodes);
    List<Node> orderedNodes = allNodes;

    nodesEnabled.clear();
    directToIndirectFollowerMap.clear();

    double firstLevelSizeDouble =
        ClusterDescriptor.getInstance().getConfig().getRelayFirstLevelSize();
    int firstLevelSize = (int) firstLevelSizeDouble;
    double firstLevelRemain = firstLevelSizeDouble - firstLevelSize;
    if (random.nextDouble() < firstLevelRemain) {
      firstLevelSize++;
    }
    List<Node> firstLevelNodes;

    if (ClusterDescriptor.getInstance().getConfig().isOptimizeIndirectBroadcasting()) {
      long thisLoad = Statistic.getTotalFanout() + 1;
      double maxLatency =
          NodeStatusManager.getINSTANCE()
              .getNodeStatus(orderedNodes.get(0), false)
              .getSendEntryLatencyStatistic()
              .getAvg();
      for (int i = 1, orderedNodesSize = orderedNodes.size(); i < orderedNodesSize; i++) {
        maxLatency =
            Double.max(
                maxLatency,
                NodeStatusManager.getINSTANCE()
                    .getNodeStatus(orderedNodes.get(i), false)
                    .getSendEntryLatencyStatistic()
                    .getAvg());
      }

      WeightedList<Node> firstLevelCandidates = new WeightedList<>();
      firstLevelCandidates.insert(
          orderedNodes.get(0), getNodeWeight(orderedNodes.get(0), maxLatency));

      for (int i = 1, orderedNodesSize = orderedNodes.size(); i < orderedNodesSize; i++) {
        Node orderedNode = orderedNodes.get(i);
        long nodeLoad =
            NodeStatusManager.getINSTANCE()
                    .getNodeStatus(orderedNode, false)
                    .getStatus()
                    .fanoutRequestNum
                + 1;
        firstLevelCandidates.insert(orderedNode, getNodeWeight(orderedNode, maxLatency));
        if (nodeLoad > thisLoad) {
          firstLevelSize = (int) Math.max(firstLevelSize, nodeLoad / thisLoad);
        }
      }

      if (firstLevelSize > firstLevelCandidates.size()) {
        firstLevelSize = firstLevelCandidates.size();
      }

      firstLevelNodes = firstLevelCandidates.select(firstLevelSize);
    } else {
      firstLevelNodes = new ArrayList<>(orderedNodes.subList(0, firstLevelSize));
      if (firstLevelSize > orderedNodes.size()) {
        firstLevelSize = orderedNodes.size();
      }
    }

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
          firstLevelNode, secondLevelNodeMap.getOrDefault(firstLevelNode, Collections.emptyList()));
      nodesEnabled.put(firstLevelNode, true);
    }

    RAFT_RELAYED_LEVEL1_NUM.add(directToIndirectFollowerMap.size());
    logger.debug("New relay map: {}", directToIndirectFollowerMap);
  }

  public Map<Node, List<Node>> getDirectToIndirectFollowerMap() {
    return Collections.unmodifiableMap(directToIndirectFollowerMap);
  }
}
