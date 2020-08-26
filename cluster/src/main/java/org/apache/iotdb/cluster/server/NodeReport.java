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

package org.apache.iotdb.cluster.server;

import java.util.ArrayList;
import java.util.List;
import org.apache.iotdb.cluster.rpc.thrift.Node;

/**
 * A node report collects the current runtime information of the local node, which contains:
 * 1. The MetaMemberReport of the meta member.
 * 2. The DataMemberReports of each data member.
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

  public void setMetaMemberReport(
      MetaMemberReport metaMemberReport) {
    this.metaMemberReport = metaMemberReport;
  }

  public void setDataMemberReportList(
      List<DataMemberReport> dataMemberReportList) {
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

    RaftMemberReport(NodeCharacter character, Node leader, long term, long lastLogTerm,
        long lastLogIndex, long commitIndex, long commitTerm, boolean isReadOnly,
        long lastHeartbeatReceivedTime) {
      this.character = character;
      this.leader = leader;
      this.term = term;
      this.lastLogTerm = lastLogTerm;
      this.lastLogIndex = lastLogIndex;
      this.commitIndex = commitIndex;
      this.commitTerm = commitTerm;
      this.isReadOnly = isReadOnly;
      this.lastHeartbeatReceivedTime = lastHeartbeatReceivedTime;
    }
  }

  /**
   * MetaMemberReport has no additional fields currently.
   */
  public static class MetaMemberReport extends RaftMemberReport {

    public MetaMemberReport(NodeCharacter character, Node leader, long term, long lastLogTerm,
        long lastLogIndex, long commitIndex, long commitTerm, boolean isReadOnly,
        long lastHeartbeatReceivedTime) {
      super(character, leader, term, lastLogTerm, lastLogIndex, commitIndex, commitTerm, isReadOnly,
          lastHeartbeatReceivedTime);
    }

    @Override
    public String toString() {
      return "MetaMemberReport{" +
          "character=" + character +
          ", Leader=" + leader +
          ", term=" + term +
          ", lastLogTerm=" + lastLogTerm +
          ", lastLogIndex=" + lastLogIndex +
          ", commitIndex=" + commitIndex +
          ", commitTerm=" + commitTerm +
          ", readOnly=" + isReadOnly +
          ", lastHeartbeat=" + (System.currentTimeMillis() - lastHeartbeatReceivedTime) + "ms ago" +
          '}';
    }
  }

  /**
   * A DataMemberReport additionally contains the header, so it can be told which group this
   * member belongs to.
   */
  public static class DataMemberReport extends RaftMemberReport {
    Node header;
    long headerLatency;

    public DataMemberReport(NodeCharacter character, Node leader, long term, long lastLogTerm,
        long lastLogIndex, long commitIndex, long commitTerm, Node header, boolean isReadOnly,
        long headerLatency,
        long lastHeartbeatReceivedTime) {
      super(character, leader, term, lastLogTerm, lastLogIndex, commitIndex, commitTerm, isReadOnly,
          lastHeartbeatReceivedTime);
      this.header = header;
      this.headerLatency = headerLatency;
    }

    @Override
    public String toString() {
      return "DataMemberReport{" +
          "header=" + header +
          ", character=" + character +
          ", Leader=" + leader +
          ", term=" + term +
          ", lastLogTerm=" + lastLogTerm +
          ", lastLogIndex=" + lastLogIndex +
          ", commitIndex=" + commitIndex +
          ", commitTerm=" + commitTerm +
          ", readOnly=" + isReadOnly +
          ", headerLatency=" + headerLatency + "ns" +
          ", lastHeartbeat=" + (System.currentTimeMillis() - lastHeartbeatReceivedTime) + "ms ago" +
          '}';
    }
  }
}
