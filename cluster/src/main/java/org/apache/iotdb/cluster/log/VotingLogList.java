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

import org.apache.iotdb.cluster.exception.LogExecutionException;
import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.cluster.server.member.RaftMember;
import org.apache.iotdb.commons.concurrent.threadpool.ScheduledExecutorUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class VotingLogList {

  private static final Logger logger = LoggerFactory.getLogger(VotingLogList.class);

  private volatile long currTerm = -1;
  private int quorumSize;
  private RaftMember member;
  private Map<Integer, Long> stronglyAcceptedIndices = new ConcurrentHashMap<>();
  private ScheduledExecutorService service = Executors.newSingleThreadScheduledExecutor();

  public VotingLogList(int quorumSize, RaftMember member) {
    this.quorumSize = quorumSize;
    this.member = member;
    ScheduledExecutorUtil.safelyScheduleAtFixedRate(
        service,
        () -> {
          long newCommitIndex = computeNewCommitIndex();
          if (newCommitIndex > member.getLogManager().getCommitLogIndex()) {
            synchronized (member.getLogManager()) {
              try {
                member.getLogManager().commitTo(newCommitIndex);
              } catch (LogExecutionException e) {
                logger.error("Fail to commit {}", newCommitIndex, e);
              }
            }
          }
        },
        0,
        1,
        TimeUnit.MILLISECONDS);
  }

  private long computeNewCommitIndex() {
    List<Entry<Integer, Long>> nodeIndices = new ArrayList<>(stronglyAcceptedIndices.entrySet());
    if (nodeIndices.size() < quorumSize) {
      return -1;
    }
    nodeIndices.sort(Entry.comparingByValue());
    return nodeIndices.get(quorumSize - 1).getValue();
  }
  /**
   * When an entry of index-term is strongly accepted by a node of acceptingNodeId, record the id in
   * all entries whose index <= the accepted entry. If any entry is accepted by a quorum, remove it
   * from the list.
   *
   * @param index
   * @param term
   * @param acceptingNodeId
   * @param signature
   * @return the lastly removed entry if any.
   */
  public void onStronglyAccept(long index, long term, Node acceptingNode, ByteBuffer signature) {
    logger.debug("{}-{} is strongly accepted by {}", index, term, acceptingNode);

    stronglyAcceptedIndices.compute(
        acceptingNode.nodeIdentifier,
        (nid, idx) -> {
          if (idx == null) {
            return index;
          } else {
            return Math.max(index, idx);
          }
        });
  }

  public int totalAcceptedNodeNum(VotingLog log) {
    long index = log.getLog().getCurrLogIndex();
    int num = log.getWeaklyAcceptedNodeIds().size();
    for (Entry<Integer, Long> entry : stronglyAcceptedIndices.entrySet()) {
      if (entry.getValue() >= index) {
        num++;
      }
    }
    return num;
  }
}
