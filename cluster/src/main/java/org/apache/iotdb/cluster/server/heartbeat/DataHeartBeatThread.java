/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at      http://www.apache.org/licenses/LICENSE-2.0  Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the License for the specific language governing permissions and limitations under the License.
 */

package org.apache.iotdb.cluster.server.heartbeat;

import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.cluster.rpc.thrift.RaftService.AsyncClient;
import org.apache.iotdb.cluster.server.member.DataGroupMember;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataHeartBeatThread extends HeartBeatThread {

  private static final Logger logger = LoggerFactory.getLogger(DataHeartBeatThread.class);
  private DataGroupMember dataGroupMember;

  public DataHeartBeatThread(DataGroupMember raftMember) {
    super(raftMember);
    this.dataGroupMember = raftMember;
  }

  @Override
  void sendHeartbeat(Node node, AsyncClient client) {
    request.setHeader(dataGroupMember.getHeader());
    super.sendHeartbeat(node, client);
  }

  @Override
  void startElection() {
    electionRequest.setHeader(dataGroupMember.getHeader());
    electionRequest.setLastLogIndex(dataGroupMember.getMetaGroupMember().getLogManager().getLastLogIndex());
    electionRequest.setLastLogTerm(dataGroupMember.getMetaGroupMember().getLogManager().getLastLogTerm());
    electionRequest.setDataLogLastIndex(dataGroupMember.getLogManager().getLastLogIndex());
    electionRequest.setDataLogLastTerm(dataGroupMember.getLogManager().getLastLogTerm());
    super.startElection();
  }
}
