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

package org.apache.iotdb.consensus.natraft.protocol.log.dispatch;

import org.apache.iotdb.consensus.common.Peer;
import org.apache.iotdb.consensus.natraft.protocol.RaftConfig;
import org.apache.iotdb.consensus.natraft.protocol.RaftMember;
import org.apache.iotdb.consensus.natraft.protocol.log.VotingEntry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import static org.apache.iotdb.consensus.natraft.utils.NodeUtils.unionNodes;

/**
 * A LogDispatcher serves a raft leader by queuing logs that the leader wants to send to its
 * followers and send the logs in an ordered manner so that the followers will not wait for previous
 * logs for too long. For example: if the leader send 3 logs, log1, log2, log3, concurrently to
 * follower A, the actual reach order may be log3, log2, and log1. According to the protocol, log3
 * and log2 must halt until log1 reaches, as a result, the total delay may increase significantly.
 */
public class LogDispatcher implements ILogDispatcher {

  private static final Logger logger = LoggerFactory.getLogger(LogDispatcher.class);
  protected RaftMember member;
  protected RaftConfig config;
  protected List<Peer> allNodes;
  protected List<Peer> newNodes;
  protected Map<Peer, DispatcherGroup> dispatcherGroupMap = new HashMap<>();
  protected Map<Peer, Double> nodesRate = new HashMap<>();
  protected boolean queueOrdered;
  protected boolean enableCompressedDispatching;
  public int maxBindingThreadNum;
  public int minBindingThreadNum;
  public int maxBatchSize;

  public LogDispatcher(RaftMember member, RaftConfig config) {
    this.member = member;
    this.config = config;
    this.queueOrdered = !(config.isUseFollowerSlidingWindow() && config.isEnableWeakAcceptance());
    this.enableCompressedDispatching = config.isEnableCompressedDispatching();
    this.minBindingThreadNum = config.getMinDispatcherBindingThreadNum();
    this.maxBindingThreadNum = config.getMaxDispatcherBindingThreadNum();
    this.allNodes = member.getAllNodes();
    this.newNodes = member.getNewNodes();
    createDispatcherGroups(unionNodes(allNodes, newNodes));
    maxBatchSize = config.getLogNumInBatch();
  }

  public void updateRateLimiter() {
    logger.info("{}: TEndPoint rates: {}", member.getName(), nodesRate);
    for (Entry<Peer, Double> nodeDoubleEntry : nodesRate.entrySet()) {
      Peer peer = nodeDoubleEntry.getKey();
      Double rate = nodeDoubleEntry.getValue();
      dispatcherGroupMap.get(peer).updateRate(rate);
    }
  }

  void createDispatcherGroup(Peer node) {
    dispatcherGroupMap.computeIfAbsent(
        node,
        n -> new CursorBasedDispatcherGroup(n, this, maxBindingThreadNum, minBindingThreadNum));
  }

  void createDispatcherGroups(Collection<Peer> peers) {
    for (Peer node : peers) {
      if (!node.equals(member.getThisNode())) {
        createDispatcherGroup(node);
      }
    }
    updateRateLimiter();
  }

  public void offer(VotingEntry request) {

    for (Entry<Peer, DispatcherGroup> entry : dispatcherGroupMap.entrySet()) {
      DispatcherGroup dispatcherGroup = entry.getValue();
      if (!dispatcherGroup.isNodeEnabled()) {
        continue;
      }

      try {
        boolean addSucceeded = dispatcherGroup.add(request);

        if (!addSucceeded) {
          logger.debug(
              "Log queue[{}] of {} is full, ignore the request to this node",
              entry.getKey(),
              member.getName());
        }
      } catch (IllegalStateException e) {
        logger.debug(
            "Log queue[{}] of {} is full, ignore the request to this node",
            entry.getKey(),
            member.getName());
      }
    }
  }

  public void applyNewNodes() {
    allNodes = newNodes;
    newNodes = null;

    List<Peer> nodesToRemove = new ArrayList<>();
    for (Entry<Peer, DispatcherGroup> entry : dispatcherGroupMap.entrySet()) {
      if (!allNodes.contains(entry.getKey())) {
        nodesToRemove.add(entry.getKey());
      }
    }
    for (Peer peer : nodesToRemove) {
      DispatcherGroup removed = dispatcherGroupMap.remove(peer);
      removed.close();
    }
  }

  public Map<Peer, Double> getNodesRate() {
    return nodesRate;
  }

  public void setNewNodes(List<Peer> newNodes) {
    this.newNodes = newNodes;
    for (Peer newNode : newNodes) {
      if (!allNodes.contains(newNode)) {
        createDispatcherGroup(newNode);
      }
    }
  }

  public RaftConfig getConfig() {
    return config;
  }

  public RaftMember getMember() {
    return member;
  }

  public void stop() {
    dispatcherGroupMap.forEach((p, g) -> g.close());
  }

  public void wakeUp() {
    for (DispatcherGroup group : dispatcherGroupMap.values()) {
      group.wakeUp();
    }
  }

  @Override
  public String toString() {
    StringBuilder stringBuilder = new StringBuilder();
    for (Entry<Peer, DispatcherGroup> entry : dispatcherGroupMap.entrySet()) {
      stringBuilder.append(entry.getKey()).append("->").append(entry.getValue()).append(";");
    }
    return "LogDispatcher{" + stringBuilder + "}";
  }

  public void sortPeers(List<Peer> peers) {
    peers.sort(Comparator.comparing(dispatcherGroupMap::get));
  }
}
