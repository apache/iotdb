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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.iotdb.consensus.common.Peer;
import org.apache.iotdb.consensus.natraft.exception.LogExecutionException;
import org.apache.iotdb.consensus.natraft.protocol.RaftMember;
import org.apache.iotdb.consensus.natraft.protocol.log.VotingEntry;
import org.apache.iotdb.consensus.natraft.protocol.log.manager.RaftLogManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class VotingLogList {

  private static final Logger logger = LoggerFactory.getLogger(VotingLogList.class);
  private RaftMember member;
  private Map<Peer, Long> stronglyAcceptedIndices = new ConcurrentHashMap<>();
  private AtomicLong newCommitIndex = new AtomicLong(-1);
  private boolean enableWeakAcceptance = false;

  public VotingLogList(RaftMember member) {
    this.member = member;
    stronglyAcceptedIndices.put(member.getThisNode(), Long.MAX_VALUE);
  }

  private boolean tryCommit(VotingEntry entry) {
    RaftLogManager logManager = member.getLogManager();

    if (computeNewCommitIndex(entry)
        && logManager != null
        && newCommitIndex.get() > logManager.getCommitLogIndex()) {
      try {
        logManager.commitTo(newCommitIndex.get());
      } catch (LogExecutionException e) {
        logger.error("Fail to commit {}", newCommitIndex, e);
      }
      return true;
    } else {
      return false;
    }
  }

  public boolean computeNewCommitIndex(VotingEntry entry) {
    long currLogIndex = entry.getEntry().getCurrLogIndex();
    if (newCommitIndex.get() >= currLogIndex) {
      return false;
    }
    if (entry.isStronglyAccepted(stronglyAcceptedIndices)) {
      return currLogIndex > newCommitIndex.getAndUpdate(ov -> Math.max(ov, currLogIndex));
    } else {
      return false;
    }
  }

  /**
   * When an entry of index-term is strongly accepted by a node of acceptingNodeId, record the id in
   * all entries whose index <= the accepted entry. If any entry is accepted by a quorum, remove it
   * from the list.
   *
   * @return the lastly removed entry if any.
   */
  public void onStronglyAccept(VotingEntry entry, Peer acceptingNode) {
    logger.debug("{} is strongly accepted by {}", entry, acceptingNode);
    long currLogIndex = entry.getEntry().getCurrLogIndex();

    Long newIndex =
        stronglyAcceptedIndices.compute(
            acceptingNode,
            (nid, oldIndex) -> {
              if (oldIndex == null) {
                return currLogIndex;
              } else {
                if (currLogIndex > oldIndex) {
                  return currLogIndex;
                }
                return oldIndex;
              }
            });
    if (newIndex == currLogIndex) {
      tryCommit(entry);
    }
  }

  public String report() {
    return String.format(
        "Nodes accepted indices: %s, new commitIndex: %d",
        stronglyAcceptedIndices, newCommitIndex.get());
  }

  public AcceptedType computeAcceptedType(VotingEntry votingEntry) {
    if ((votingEntry.getEntry().getCurrLogIndex() == Long.MIN_VALUE)) {
      return AcceptedType.NOT_ACCEPTED;
    }

    if (newCommitIndex.get() >= votingEntry.getEntry().getCurrLogIndex()) {
      return AcceptedType.STRONGLY_ACCEPTED;
    }

    if (enableWeakAcceptance) {
      int currNodeQuorumNum = votingEntry.currNodesQuorumNum();
      int newNodeQuorumNum = votingEntry.newNodesQuorumNum();
      int stronglyAcceptedNumByCurrNodes = votingEntry.stronglyAcceptedNumByCurrNodes(
          stronglyAcceptedIndices);
      int stronglyAcceptedNumByNewNodes = votingEntry.stronglyAcceptedNumByNewNodes(
          stronglyAcceptedIndices);
      int weaklyAcceptedNumByCurrNodes = votingEntry.weaklyAcceptedNumByCurrNodes(
          stronglyAcceptedIndices);
      int weaklyAcceptedNumByNewNodes = votingEntry.weaklyAcceptedNumByNewNodes(
          stronglyAcceptedIndices);
      if ((weaklyAcceptedNumByCurrNodes + stronglyAcceptedNumByCurrNodes) >= currNodeQuorumNum &&
          (weaklyAcceptedNumByNewNodes + stronglyAcceptedNumByNewNodes) >= newNodeQuorumNum) {
        return AcceptedType.WEAKLY_ACCEPTED;
      }
    }

    return AcceptedType.NOT_ACCEPTED;
  }

  public void setEnableWeakAcceptance(boolean enableWeakAcceptance) {
    this.enableWeakAcceptance = enableWeakAcceptance;
  }

  public enum AcceptedType {
    NOT_ACCEPTED,
    STRONGLY_ACCEPTED,
    WEAKLY_ACCEPTED
  }
}
