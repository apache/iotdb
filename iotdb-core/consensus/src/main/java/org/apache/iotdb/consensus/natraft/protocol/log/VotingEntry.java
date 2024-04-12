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

package org.apache.iotdb.consensus.natraft.protocol.log;

import org.apache.iotdb.consensus.common.Peer;
import org.apache.iotdb.consensus.natraft.protocol.RaftConfig;
import org.apache.iotdb.consensus.raft.thrift.AppendEntryRequest;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class VotingEntry {

  protected Entry entry;
  // for NB-Raft
  protected Set<Peer> weaklyAcceptedNodes;
  private AppendEntryRequest appendEntryRequest;
  protected List<Peer> currNodes;
  protected List<Peer> newNodes;
  private boolean isStronglyAccepted;
  private boolean isWeaklyAccepted;
  private boolean notified;

  public VotingEntry(
      Entry entry,
      AppendEntryRequest appendEntryRequest,
      List<Peer> currNodes,
      List<Peer> newNodes,
      RaftConfig config) {
    this.entry = entry;
    if (config.isUseFollowerSlidingWindow()) {
      weaklyAcceptedNodes =
          new HashSet<>(currNodes.size() + (newNodes != null ? newNodes.size() : 0));
    }
    this.setAppendEntryRequest(appendEntryRequest);
    this.currNodes = currNodes;
    this.newNodes = newNodes;
  }

  public Entry getEntry() {
    return entry;
  }

  public void setEntry(Entry entry) {
    this.entry = entry;
  }

  public Set<Peer> getWeaklyAcceptedNodes() {
    return weaklyAcceptedNodes != null ? weaklyAcceptedNodes : Collections.emptySet();
  }

  public void addWeaklyAcceptedNodes(Peer node) {
    weaklyAcceptedNodes.add(node);
  }

  @Override
  public String toString() {
    return entry.toString();
  }

  public AppendEntryRequest getAppendEntryRequest() {
    return appendEntryRequest;
  }

  public void setAppendEntryRequest(AppendEntryRequest appendEntryRequest) {
    this.appendEntryRequest = appendEntryRequest;
  }

  public int currNodesQuorumNum() {
    return currNodes.size() / 2 + 1;
  }

  public int newNodesQuorumNum() {
    return newNodes != null ? newNodes.size() / 2 + 1 : 0;
  }

  public boolean isStronglyAccepted(Map<Peer, Long> stronglyAcceptedIndices) {
    if (isStronglyAccepted) {
      return true;
    }
    int currNodeQuorumNum = currNodesQuorumNum();
    int newNodeQuorumNum = newNodesQuorumNum();
    boolean stronglyAcceptedByCurrNodes =
        stronglyAcceptedNumByCurrNodes(stronglyAcceptedIndices) >= currNodeQuorumNum;
    boolean stronglyAcceptedByNewNodes =
        stronglyAcceptedNumByNewNodes(stronglyAcceptedIndices) >= newNodeQuorumNum;
    if (stronglyAcceptedByCurrNodes && stronglyAcceptedByNewNodes) {
      isStronglyAccepted = true;
    }
    return stronglyAcceptedByCurrNodes && stronglyAcceptedByNewNodes;
  }

  public boolean isWeaklyAccepted(Map<Peer, Long> stronglyAcceptedIndices) {
    if (isWeaklyAccepted) {
      return true;
    }
    int currNodeQuorumNum = currNodesQuorumNum();
    int newNodeQuorumNum = newNodesQuorumNum();
    int stronglyAcceptedNumByCurrNodes = stronglyAcceptedNumByCurrNodes(stronglyAcceptedIndices);
    int stronglyAcceptedNumByNewNodes = stronglyAcceptedNumByNewNodes(stronglyAcceptedIndices);
    int weaklyAcceptedNumByCurrNodes = weaklyAcceptedNumByCurrNodes(stronglyAcceptedIndices);
    int weaklyAcceptedNumByNewNodes = weaklyAcceptedNumByNewNodes(stronglyAcceptedIndices);
    if ((weaklyAcceptedNumByCurrNodes + stronglyAcceptedNumByCurrNodes) >= currNodeQuorumNum
        && (weaklyAcceptedNumByNewNodes + stronglyAcceptedNumByNewNodes) >= newNodeQuorumNum) {
      isWeaklyAccepted = true;
      return true;
    }
    return false;
  }

  public int stronglyAcceptedNumByCurrNodes(Map<Peer, Long> stronglyAcceptedIndices) {
    int num = 0;
    for (Peer node : currNodes) {
      if (stronglyAcceptedIndices.getOrDefault(node, -1L) >= entry.getCurrLogIndex()) {
        num++;
      }
    }
    return num;
  }

  public int stronglyAcceptedNumByNewNodes(Map<Peer, Long> stronglyAcceptedIndices) {
    if (!hasNewNodes()) {
      return 0;
    }
    int num = 0;
    for (Peer node : newNodes) {
      if (stronglyAcceptedIndices.getOrDefault(node, -1L) >= entry.getCurrLogIndex()) {
        num++;
      }
    }
    return num;
  }

  public int weaklyAcceptedNumByCurrNodes(Map<Peer, Long> stronglyAcceptedIndices) {
    int num = 0;
    for (Peer node : currNodes) {
      if (weaklyAcceptedNodes.contains(node)
          && stronglyAcceptedIndices.getOrDefault(node, -1L) < entry.getCurrLogIndex()) {
        num++;
      }
    }
    return num;
  }

  public int weaklyAcceptedNumByNewNodes(Map<Peer, Long> stronglyAcceptedIndices) {
    if (!hasNewNodes()) {
      return 0;
    }
    int num = 0;
    for (Peer node : currNodes) {
      if (weaklyAcceptedNodes.contains(node)
          && stronglyAcceptedIndices.getOrDefault(node, -1L) < entry.getCurrLogIndex()) {
        num++;
      }
    }
    return num;
  }

  public boolean hasNewNodes() {
    return newNodes != null;
  }

  public boolean isNotified() {
    return notified;
  }

  public void setNotified(boolean notified) {
    this.notified = notified;
  }
}
