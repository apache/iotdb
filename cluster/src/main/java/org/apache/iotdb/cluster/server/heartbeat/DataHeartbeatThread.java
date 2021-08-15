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

package org.apache.iotdb.cluster.server.heartbeat;

import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.cluster.server.member.DataGroupMember;

public class DataHeartbeatThread extends HeartbeatThread {

  private static final int MAX_ELECTIONS_TO_SKIP = 5;

  private DataGroupMember dataGroupMember;
  private int skippedElectionNumber = 0;

  public DataHeartbeatThread(DataGroupMember raftMember) {
    super(raftMember);
    this.dataGroupMember = raftMember;
  }

  @Override
  void sendHeartbeatSync(Node node) {
    request.setHeader(dataGroupMember.getHeader());
    super.sendHeartbeatSync(node);
  }

  @Override
  void sendHeartbeatAsync(Node node) {
    request.setHeader(dataGroupMember.getHeader());
    super.sendHeartbeatAsync(node);
  }

  /**
   * Different from the election of the meta group, the leader of a data group should have the
   * newest meta log to guarantee it will not receive the data of the slots that no longer belongs
   * to it. So the progress of meta logs is also examined.
   */
  @Override
  void startElection() {
    // skip first few elections to let the header have a larger chance to become the leader, so
    // possibly each node will only be one leader at the same time
    if (!dataGroupMember.getThisNode().equals(dataGroupMember.getHeader().getNode())
        && skippedElectionNumber < MAX_ELECTIONS_TO_SKIP
        && !hasHadLeader) {
      skippedElectionNumber++;
      return;
    }
    electionRequest.setHeader(dataGroupMember.getHeader());

    super.startElection();
  }
}
