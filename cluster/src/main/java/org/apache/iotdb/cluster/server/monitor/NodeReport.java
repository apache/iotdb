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

package org.apache.iotdb.cluster.server.monitor;

import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.cluster.rpc.thrift.RaftNode;
import org.apache.iotdb.cluster.server.NodeCharacter;
import org.apache.iotdb.rpc.RpcStat;
import org.apache.iotdb.rpc.RpcTransportFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * A node report collects the current runtime information of the local node, which contains: 1. The
 * MetaMemberReport of the meta member. 2. The DataMemberReports of each data member.
 */
@SuppressWarnings("java:S107") // reports need enough parameters
public class NodeReport {

  private Node thisNode;
  private MetaMemberReport metaMemberReport;
  private List<DataMemberReport> dataMemberReportList;

  public NodeReport(Node thisNode) {
    this.thisNode = thisNode;
    dataMemberReportList = new ArrayList<>();
  }

  public void setMetaMemberReport(MetaMemberReport metaMemberReport) {
    this.metaMemberReport = metaMemberReport;
  }

  public void setDataMemberReportList(List<DataMemberReport> dataMemberReportList) {
    this.dataMemberReportList = dataMemberReportList;
  }

  @Override
  public String toString() {
    StringBuilder stringBuilder = new StringBuilder();
    stringBuilder.append("Report of ").append(thisNode).append(System.lineSeparator());
    stringBuilder.append(metaMemberReport).append(System.lineSeparator());
    for (DataMemberReport dataMemberReport : dataMemberReportList) {
      stringBuilder.append(dataMemberReport).append(System.lineSeparator());
    }
    return stringBuilder.toString();
  }

  /**
   * A RaftMemberReport contains the character, leader, term, last log term/index of a raft member.
   */
  static class RaftMemberReport {
    NodeCharacter character;
    Node leader;
    long term;
    long lastLogTerm;
    long lastLogIndex;
    long commitIndex;
    long commitTerm;
    boolean isReadOnly;
    long lastHeartbeatReceivedTime;
    long prevLastLogIndex;
    long maxAppliedLogIndex;

    RaftMemberReport(
        NodeCharacter character,
        Node leader,
        long term,
        long lastLogTerm,
        long lastLogIndex,
        long commitIndex,
        long commitTerm,
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
      this.isReadOnly = isReadOnly;
      this.lastHeartbeatReceivedTime = lastHeartbeatReceivedTime;
      this.prevLastLogIndex = prevLastLogIndex;
      this.maxAppliedLogIndex = maxAppliedLogIndex;
    }
  }

  /** MetaMemberReport has no additional fields currently. */
  public static class MetaMemberReport extends RaftMemberReport {

    public MetaMemberReport(
        NodeCharacter character,
        Node leader,
        long term,
        long lastLogTerm,
        long lastLogIndex,
        long commitIndex,
        long commitTerm,
        boolean isReadOnly,
        long lastHeartbeatReceivedTime,
        long prevLastLogIndex,
        long maxAppliedLogIndex) {
      super(
          character,
          leader,
          term,
          lastLogTerm,
          lastLogIndex,
          commitIndex,
          commitTerm,
          isReadOnly,
          lastHeartbeatReceivedTime,
          prevLastLogIndex,
          maxAppliedLogIndex);
    }

    @Override
    public String toString() {
      long readBytes = RpcStat.getReadBytes();
      long readCompressedBytes = RpcStat.getReadCompressedBytes();
      long writeBytes = RpcStat.getWriteBytes();
      long writeCompressedBytes = RpcStat.getWriteCompressedBytes();
      double readCompressionRatio = (double) readBytes / readCompressedBytes;
      double writeCompressionRatio = (double) writeBytes / writeCompressedBytes;
      String transportCompressionReport = "";
      if (RpcTransportFactory.isUseSnappy()) {
        transportCompressionReport =
            ", readBytes="
                + readBytes
                + "/"
                + readCompressedBytes
                + "("
                + readCompressionRatio
                + ")"
                + ", writeBytes="
                + writeBytes
                + "/"
                + writeCompressedBytes
                + "("
                + writeCompressionRatio
                + ")";
      }
      return "MetaMemberReport {\n"
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
          + Timer.getReport()
          + '}';
    }
  }

  /**
   * A DataMemberReport additionally contains the header, so it can be told which group this member
   * belongs to.
   */
  public static class DataMemberReport extends RaftMemberReport {
    RaftNode header;
    long headerLatency;

    public DataMemberReport(
        NodeCharacter character,
        Node leader,
        long term,
        long lastLogTerm,
        long lastLogIndex,
        long commitIndex,
        long commitTerm,
        RaftNode header,
        boolean isReadOnly,
        long headerLatency,
        long lastHeartbeatReceivedTime,
        long prevLastLogIndex,
        long maxAppliedLogIndex) {
      super(
          character,
          leader,
          term,
          lastLogTerm,
          lastLogIndex,
          commitIndex,
          commitTerm,
          isReadOnly,
          lastHeartbeatReceivedTime,
          prevLastLogIndex,
          maxAppliedLogIndex);
      this.header = header;
      this.headerLatency = headerLatency;
    }

    @Override
    public String toString() {
      return "DataMemberReport{"
          + "header="
          + header.getNode()
          + ", raftId="
          + header.getRaftId()
          + ", character="
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
          + ", appliedLogIndex="
          + maxAppliedLogIndex
          + ", readOnly="
          + isReadOnly
          + ", headerLatency="
          + headerLatency
          + "ns"
          + ", lastHeartbeat="
          + (System.currentTimeMillis() - lastHeartbeatReceivedTime)
          + "ms ago"
          + ", logIncrement="
          + (lastLogIndex - prevLastLogIndex)
          + '}';
    }
  }
}
