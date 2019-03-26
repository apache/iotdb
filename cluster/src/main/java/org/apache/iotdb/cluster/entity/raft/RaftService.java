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
import com.alipay.sofa.jraft.Node;
import com.alipay.sofa.jraft.RaftGroupService;
import com.alipay.sofa.jraft.conf.Configuration;
import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.option.NodeOptions;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.iotdb.cluster.entity.service.IService;

public class RaftService implements IService {

  private List<PeerId> peerIdList;
  private Node node;
  private RaftGroupService raftGroupService;

  public RaftService(String groupId, PeerId[] peerIds, PeerId serverId, RpcServer rpcServer) {
    this.peerIdList = new ArrayList<>(peerIds.length);
    peerIdList.addAll(Arrays.asList(peerIds));
    raftGroupService = new RaftGroupService(groupId, serverId, null, rpcServer);
  }

  @Override
  public void init() {
    NodeOptions nodeOptions = new NodeOptions();
    nodeOptions.setDisableCli(false);
    final Configuration initConf = new Configuration();
    initConf.setPeers(peerIdList);
    nodeOptions.setInitialConf(initConf);
    raftGroupService.setNodeOptions(nodeOptions);
  }

  @Override
  public void start() {
    this.node = raftGroupService.start();
  }

  @Override
  public void stop() {
    raftGroupService.shutdown();
  }

  public RaftGroupService getRaftGroupService() {
    return raftGroupService;
  }

  public Node getNode() {
    return node;
  }

  public void setNode(Node node) {
    this.node = node;
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
