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

package org.apache.iotdb.consensus.natraft.utils;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.consensus.common.Peer;
import org.apache.iotdb.consensus.natraft.protocol.RaftRole;

import java.util.ArrayList;
import java.util.List;

/**
 * A node report collects the current runtime information of the local node, which contains: 1. The
 * MetaMemberReport of the meta member. 2. The DataMemberReports of each data member.
 */
@SuppressWarnings("java:S107") // reports need enough parameters
public class NodeReport {

  private TEndPoint thisNode;
  private List<RaftMemberReport> memberReports;

  public NodeReport(TEndPoint thisNode) {
    this.thisNode = thisNode;
    memberReports = new ArrayList<>();
  }

  public void setMemberReports(List<RaftMemberReport> memberReports) {
    this.memberReports = memberReports;
  }

  @Override
  public String toString() {
    StringBuilder stringBuilder = new StringBuilder();
    stringBuilder.append("Report of ").append(thisNode).append(System.lineSeparator());
    for (RaftMemberReport memberReport : memberReports) {
      stringBuilder.append(memberReport).append(System.lineSeparator());
    }
    return stringBuilder.toString();
  }

  /**
   * A RaftMemberReport contains the character, leader, term, last log term/index of a raft member.
   */
  public static class RaftMemberReport {
    RaftRole character;
    Peer leader;
    long term;
    long lastLogTerm;
    long lastLogIndex;
    long commitIndex;
    long commitTerm;
    long persistedIndex;
    boolean isReadOnly;
    long lastHeartbeatReceivedTime;
    long prevLastLogIndex;
    long maxAppliedLogIndex;

    public RaftMemberReport(
        RaftRole character,
        Peer leader,
        long term,
        long lastLogTerm,
        long lastLogIndex,
        long commitIndex,
        long commitTerm,
        long persistedIndex,
        boolean isReadOnly,
        long lastHeartbeatReceivedTime,
        long prevLastLogIndex,
        long maxAppliedLogIndex) {
      this.character = character;
      this.leader = leader;
      this.term = term;
      this.lastLogTerm = lastLogTerm;
      this.lastLogIndex = lastLogIndex;
      this.commitIndex = commitIndex;
      this.commitTerm = commitTerm;
      this.persistedIndex = persistedIndex;
      this.isReadOnly = isReadOnly;
      this.lastHeartbeatReceivedTime = lastHeartbeatReceivedTime;
      this.prevLastLogIndex = prevLastLogIndex;
      this.maxAppliedLogIndex = maxAppliedLogIndex;
    }

    @Override
    public String toString() {
      String transportCompressionReport = "";
      return "RaftReport {\n"
          + "character="
          + character
          + ", Leader="
          + leader
          + ", term="
          + term
          + ", lastLogTerm="
          + lastLogTerm
          + ", lastLogIndex="
          + lastLogIndex
          + ", commitIndex="
          + commitIndex
          + ", commitTerm="
          + commitTerm
          + ", persistedIndex="
          + persistedIndex
          + ", appliedLogIndex="
          + maxAppliedLogIndex
          + ", readOnly="
          + isReadOnly
          + ", lastHeartbeat="
          + (System.currentTimeMillis() - lastHeartbeatReceivedTime)
          + "ms ago"
          + ", logIncrement="
          + (lastLogIndex - prevLastLogIndex)
          + transportCompressionReport
          + ", \n timer: "
          + Timer.Statistic.getReport()
          + '}';
    }

    public RaftRole getCharacter() {
      return character;
    }

    public Peer getLeader() {
      return leader;
    }

    public long getTerm() {
      return term;
    }

    public long getLastLogTerm() {
      return lastLogTerm;
    }

    public long getLastLogIndex() {
      return lastLogIndex;
    }

    public long getCommitIndex() {
      return commitIndex;
    }

    public long getCommitTerm() {
      return commitTerm;
    }

    public boolean isReadOnly() {
      return isReadOnly;
    }

    public long getLastHeartbeatReceivedTime() {
      return lastHeartbeatReceivedTime;
    }

    public long getPrevLastLogIndex() {
      return prevLastLogIndex;
    }

    public long getMaxAppliedLogIndex() {
      return maxAppliedLogIndex;
    }
  }
}
