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
import com.alipay.sofa.jraft.RaftGroupService;
import com.alipay.sofa.jraft.entity.PeerId;
import java.util.ArrayList;
import java.util.List;
import org.apache.iotdb.cluster.entity.service.IService;

public class RaftService implements IService {

  private List<PeerId> peerIdList;
  private PeerId leader;
  private RaftGroupService raftGroupService;

  public RaftService(String groupId, PeerId[] peerIds, PeerId serverId, RpcServer rpcServer) {
    this.peerIdList = new ArrayList<>(peerIds.length);
    for (int i = 0; i < peerIds.length; i++) {
      peerIdList.add(peerIds[i]);
    }

    raftGroupService = new RaftGroupService(groupId, serverId, null, rpcServer);
  }

  @Override
  public void init() {

  }

  @Override
  public void start() {
    raftGroupService.start();
  }

  @Override
  public void stop() {

  }

  public void saveSnapshot() {

  }

  public void loadSnapshot() {

  }

  public void onRevice(Object message) {

  }

  enum State {
    FOLLOWER, LEADER, CANIDIDATE;
  }
}
