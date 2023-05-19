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

package org.apache.iotdb.consensus.natraft.protocol.log.dispatch.flowcontrol;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.commons.concurrent.threadpool.ScheduledExecutorUtil;
import org.apache.iotdb.consensus.common.Peer;
import org.apache.iotdb.consensus.natraft.protocol.RaftConfig;
import org.apache.iotdb.consensus.natraft.protocol.RaftMember;
import org.apache.iotdb.consensus.natraft.protocol.RaftRole;
import org.apache.iotdb.consensus.natraft.protocol.log.dispatch.DispatcherGroup;
import org.apache.iotdb.consensus.natraft.protocol.log.dispatch.LogDispatcher;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class FlowBalancer {

  private static final Logger logger = LoggerFactory.getLogger(FlowBalancer.class);
  private double maxFlow;
  private double minFlow;
  private int windowsToUse;
  private double overestimateFactor;
  private int flowBalanceIntervalMS = 5000;
  private FlowMonitorManager flowMonitorManager = FlowMonitorManager.INSTANCE;
  private LogDispatcher logDispatcher;
  private RaftMember member;
  private ScheduledExecutorService scheduledExecutorService;
  private volatile boolean inBurst = false;
  private long burstStart;
  private long burstDuration;
  private RaftConfig config;

  public FlowBalancer(LogDispatcher logDispatcher, RaftMember member, RaftConfig config) {
    this.logDispatcher = logDispatcher;
    this.member = member;
    this.windowsToUse = config.getFollowerLoadBalanceWindowsToUse();
    this.overestimateFactor = config.getFollowerLoadBalanceOverestimateFactor();
    this.minFlow = config.getFlowControlMinFlow();
    this.maxFlow = config.getFlowControlMaxFlow();
    this.config = config;
  }

  public void start() {
    scheduledExecutorService =
        Executors.newSingleThreadScheduledExecutor(
            r -> new Thread(r, member.getName() + "-FlowBalancer"));
    ScheduledExecutorUtil.safelyScheduleAtFixedRate(
        scheduledExecutorService,
        this::rebalance,
        flowBalanceIntervalMS,
        flowBalanceIntervalMS,
        TimeUnit.MILLISECONDS);
  }

  public void stop() {
    scheduledExecutorService.shutdownNow();
    logger.info("{}: Total burst duration: {}ms", member.getName(), burstDuration);
  }

  private void rebalance() {
    if (!member.getStatus().getRole().equals(RaftRole.LEADER)) {
      return;
    }

    List<Peer> followers = new ArrayList<>(member.getAllNodes());
    followers.remove(member.getThisNode());

    int nodeNum = member.getAllNodes().size();
    int followerNum = nodeNum - 1;
    long flowMonitorWindowInterval = config.getFlowMonitorWindowInterval();

    List<FlowWindow> latestWindows =
        flowMonitorManager.getLatestWindows(member.getThisNode().getEndpoint(), windowsToUse);

    int burstWindowNum = 0;
    for (FlowWindow latestWindow : latestWindows) {
      double assumedFlow =
          latestWindow.sum * 1.0 * flowMonitorWindowInterval / 1000 * overestimateFactor;
      if (assumedFlow * followerNum > maxFlow) {
        burstWindowNum++;
      }
    }

    double assumedFlow =
        latestWindows.stream().mapToLong(w -> w.sum).sum()
            * 1.0
            / latestWindows.size()
            * (flowMonitorWindowInterval / 1000.0)
            * overestimateFactor;

    Map<Peer, DispatcherGroup> dispatcherGroupMap = logDispatcher.getDispatcherGroupMap();
    Map<Peer, Double> nodesRate = logDispatcher.getNodesRate();

    // sort followers according to their queue length
    followers.sort(Comparator.comparing(node -> dispatcherGroupMap.get(node).getQueueSize()));
    if (burstWindowNum > latestWindows.size() / 2 && !inBurst) {
      enterBurst(nodesRate, nodeNum, assumedFlow, followers);
      logDispatcher.updateRateLimiter();
      for (Entry<TEndPoint, FlowMonitor> entry : flowMonitorManager.getMonitorMap().entrySet()) {
        logger.info(
            "{}: Flow of {}: {}, {}, {}",
            member.getName(),
            entry.getKey(),
            entry.getValue().getLatestWindows(windowsToUse),
            entry.getValue().averageFlow(windowsToUse),
            inBurst);
      }
    } else if (burstWindowNum <= latestWindows.size() / 2 && inBurst) {
      exitBurst(followerNum, nodesRate, followers);
      logDispatcher.updateRateLimiter();
      for (Entry<TEndPoint, FlowMonitor> entry : flowMonitorManager.getMonitorMap().entrySet()) {
        logger.info(
            "{}: Flow of {}: {}, {}, {}",
            member.getName(),
            entry.getKey(),
            entry.getValue().getLatestWindows(windowsToUse),
            entry.getValue().averageFlow(windowsToUse),
            inBurst);
      }
    }
  }

  private void enterBurst(
      Map<Peer, Double> nodesRate, int nodeNum, double assumedFlow, List<Peer> followers) {
    int followerNum = nodeNum - 1;
    int quorumFollowerNum = nodeNum / 2;
    double remainingFlow = maxFlow;
    double quorumMaxFlow = maxFlow / quorumFollowerNum;
    // distribute flow to quorum followers with the shortest queues
    double flowToQuorum = Math.min(assumedFlow, quorumMaxFlow);
    int i = 0;
    for (; i < quorumFollowerNum; i++) {
      Peer node = followers.get(i);
      nodesRate.put(node, Double.MAX_VALUE);
      remainingFlow -= flowToQuorum;
    }
    double flowToRemaining = remainingFlow / (followerNum - quorumFollowerNum);
    logger.info(
        "{}: entering burst, quorum flow: {}, non-quorum flow: {}, max flow: {}, min flow: {}, assumed flow: {}, quorum max flow: {}",
        member.getName(),
        flowToQuorum,
        flowToRemaining,
        maxFlow,
        minFlow,
        assumedFlow,
        quorumMaxFlow);
    if (flowToRemaining < minFlow) {
      flowToRemaining = minFlow;
    }
    for (; i < followerNum; i++) {
      Peer node = followers.get(i);
      nodesRate.put(node, flowToRemaining);
    }
    inBurst = true;
    burstStart = System.currentTimeMillis();
  }

  private void exitBurst(int followerNum, Map<Peer, Double> nodesRate, List<Peer> followers) {
    logger.info("{}: exiting burst", member.getName());
    // lift flow limits
    for (int i = 0; i < followerNum; i++) {
      Peer node = followers.get(i);
      nodesRate.put(node, Double.MAX_VALUE);
    }
    inBurst = false;
    burstDuration += System.currentTimeMillis() - burstStart;
  }
}
