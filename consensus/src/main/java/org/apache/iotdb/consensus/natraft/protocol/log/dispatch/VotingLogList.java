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

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.consensus.natraft.exception.LogExecutionException;
import org.apache.iotdb.consensus.natraft.protocol.RaftMember;
import org.apache.iotdb.consensus.natraft.protocol.log.VotingLog;
import org.apache.iotdb.consensus.natraft.protocol.log.manager.RaftLogManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public class VotingLogList {

  private static final Logger logger = LoggerFactory.getLogger(VotingLogList.class);
  private int quorumSize;
  private RaftMember member;
  private Map<TEndPoint, Long> stronglyAcceptedIndices = new ConcurrentHashMap<>();
  private AtomicLong newCommitIndex = new AtomicLong(-1);

  public VotingLogList(int quorumSize, RaftMember member) {
    this.quorumSize = quorumSize;
    this.member = member;
  }

  private boolean tryCommit() {
    RaftLogManager logManager = member.getLogManager();

    if (computeNewCommitIndex()
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

  public boolean computeNewCommitIndex() {
    List<Entry<TEndPoint, Long>> nodeIndices = new ArrayList<>(stronglyAcceptedIndices.entrySet());
    if (nodeIndices.size() < quorumSize) {
      return false;
    }
    nodeIndices.sort(Entry.comparingByValue());
    Long value = nodeIndices.get(nodeIndices.size() - quorumSize).getValue();
    long oldValue = newCommitIndex.getAndUpdate(oldV -> Math.max(value, oldV));
    return value > oldValue;
  }

  /**
   * When an entry of index-term is strongly accepted by a node of acceptingNodeId, record the id in
   * all entries whose index <= the accepted entry. If any entry is accepted by a quorum, remove it
   * from the list.
   *
   * @param index
   * @param term
   * @param acceptingNode
   * @return the lastly removed entry if any.
   */
  public void onStronglyAccept(long index, long term, TEndPoint acceptingNode) {
    logger.debug("{}-{} is strongly accepted by {}", index, term, acceptingNode);

    Long newIndex =
        stronglyAcceptedIndices.compute(
            acceptingNode,
            (nid, oldIndex) -> {
              if (oldIndex == null) {
                return index;
              } else {
                if (index > oldIndex) {
                  return index;
                }
                return oldIndex;
              }
            });
    if (newIndex == index) {
      tryCommit();
    }
  }

  public int totalAcceptedNodeNum(VotingLog log) {
    long index = log.getEntry().getCurrLogIndex();
    int num = log.getWeaklyAcceptedNodes().size();
    for (Entry<TEndPoint, Long> entry : stronglyAcceptedIndices.entrySet()) {
      if (entry.getValue() >= index) {
        num++;
      }
    }
    return num;
  }

  public String report() {
    return String.format(
        "Nodes accepted indices: %s, new commitIndex: %d",
        stronglyAcceptedIndices, newCommitIndex.get());
  }
}
