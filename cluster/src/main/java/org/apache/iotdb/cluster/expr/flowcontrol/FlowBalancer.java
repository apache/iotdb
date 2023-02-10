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

package org.apache.iotdb.cluster.expr.flowcontrol;

import org.apache.iotdb.cluster.log.LogDispatcher;
import org.apache.iotdb.cluster.log.LogDispatcher.SendLogRequest;
import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.cluster.server.member.RaftMember;
import org.apache.iotdb.commons.concurrent.threadpool.ScheduledExecutorUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class FlowBalancer {

  private static final Logger logger = LoggerFactory.getLogger(FlowBalancer.class);
  private double maxFlow = 900_000_000;
  private double minFlow = 10_000_000;
  private int windowsToUse = 3;
  private int flowBalanceIntervalMS = 1000;
  private FlowMonitorManager flowMonitorManager = FlowMonitorManager.INSTANCE;
  private LogDispatcher logDispatcher;
  private RaftMember member;

  private ScheduledExecutorService scheduledExecutorService;

  public FlowBalancer(LogDispatcher logDispatcher, RaftMember member) {
    this.logDispatcher = logDispatcher;
    this.member = member;
  }

  public void start() {
    scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
    ScheduledExecutorUtil.safelyScheduleAtFixedRate(
        scheduledExecutorService,
        this::rebalance,
        flowBalanceIntervalMS,
        flowBalanceIntervalMS,
        TimeUnit.MILLISECONDS);
  }

  public void stop() {
    scheduledExecutorService.shutdownNow();
  }

  private void rebalance() {
    if (!member.isLeader()) {
      return;
    }

    List<Node> followers = new ArrayList<>(member.getAllNodes());
    followers.remove(member.getThisNode());

    int nodeNum = member.getAllNodes().size();
    int followerNum = nodeNum - 1;

    double thisNodeFlow = flowMonitorManager.averageFlow(member.getThisNode(), windowsToUse);
    double assumedFlow = thisNodeFlow * 1.1;
    logger.info("Flow of this node: {}", thisNodeFlow);
    Map<Node, BlockingQueue<SendLogRequest>> nodesLogQueuesMap =
        logDispatcher.getNodesLogQueuesMap();
    Map<Node, Double> nodesRate = logDispatcher.getNodesRate();

    // sort followers according to their queue length
    followers.sort(Comparator.comparing(node -> nodesLogQueuesMap.get(node).size()));
    if (assumedFlow * followerNum > maxFlow) {
      enterBurst(nodesRate, nodeNum, assumedFlow, followers);
    } else {
      exitBurst(followerNum, nodesRate, followers);
    }
    logDispatcher.updateRateLimiter();
  }

  private void enterBurst(
      Map<Node, Double> nodesRate, int nodeNum, double assumedFlow, List<Node> followers) {
    int followerNum = nodeNum - 1;
    int quorumFollowerNum = nodeNum / 2;
    double remainingFlow = maxFlow;
    double quorumMaxFlow = maxFlow / quorumFollowerNum;
    // distribute flow to quorum followers with the shortest queues
    double flowToQuorum = Math.min(assumedFlow, quorumMaxFlow);
    int i = 0;
    for (; i < quorumFollowerNum; i++) {
      Node node = followers.get(i);
      nodesRate.put(node, flowToQuorum);
      remainingFlow -= flowToQuorum;
    }
    double flowToRemaining = remainingFlow / (followerNum - quorumFollowerNum);
    if (flowToRemaining < minFlow) {
      flowToRemaining = minFlow;
    }
    for (; i < followerNum; i++) {
      Node node = followers.get(i);
      nodesRate.put(node, flowToRemaining);
    }
  }

  private void exitBurst(int followerNum, Map<Node, Double> nodesRate, List<Node> followers) {
    // lift flow limits
    for (int i = 0; i < followerNum; i++) {
      Node node = followers.get(i);
      nodesRate.put(node, maxFlow);
    }
  }
}
