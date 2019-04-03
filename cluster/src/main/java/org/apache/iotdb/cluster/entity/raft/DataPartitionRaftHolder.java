/**
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
package org.apache.iotdb.cluster.entity.raft;

import com.alipay.remoting.rpc.RpcServer;
import com.alipay.sofa.jraft.entity.PeerId;
import org.apache.iotdb.cluster.entity.data.DataPartitionHolder;

public class DataPartitionRaftHolder extends DataPartitionHolder {

  private String groupId;
  private PeerId serverId;
  private DataStateMachine fsm;

  public DataPartitionRaftHolder(String groupId, PeerId[] peerIds, PeerId serverId, RpcServer rpcServer, boolean startRpcServer) {
    this.groupId = groupId;
    this.serverId = serverId;
    fsm = new DataStateMachine(groupId, serverId);
    service = new RaftService(groupId, peerIds, serverId, rpcServer, fsm, startRpcServer);
  }

  public DataStateMachine getFsm() {
    return fsm;
  }

  public void setFsm(DataStateMachine fsm) {
    this.fsm = fsm;
  }

  public String getGroupId() {
    return groupId;
  }

  public void setGroupId(String groupId) {
    this.groupId = groupId;
  }

  public PeerId getServerId() {
    return serverId;
  }

  public void setServerId(PeerId serverId) {
    this.serverId = serverId;
  }
}
