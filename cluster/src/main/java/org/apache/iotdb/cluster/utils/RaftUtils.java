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
package org.apache.iotdb.cluster.utils;

import com.alipay.sofa.jraft.RouteTable;
import com.alipay.sofa.jraft.conf.Configuration;
import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.option.CliOptions;
import com.alipay.sofa.jraft.rpc.impl.cli.BoltCliClientService;
import java.util.concurrent.TimeoutException;
import org.apache.iotdb.cluster.exception.RaftConnectionException;
import org.apache.iotdb.cluster.utils.hash.PhysicalNode;

public class RaftUtils {

  private RaftUtils() {
  }

  /**
   * Get leader node according to the group id
   *
   * @param groupId group id of raft group
   * @return PeerId of leader
   */
  public static PeerId getLeader(String groupId) throws RaftConnectionException {
    Configuration conf = getConfiguration(groupId);
    RouteTable.getInstance().updateConfiguration(groupId, conf);

    final BoltCliClientService cliClientService = new BoltCliClientService();
    cliClientService.init(new CliOptions());
    try {
      if (!RouteTable.getInstance().refreshLeader(cliClientService, groupId, 1000).isOk()) {
        throw new RaftConnectionException("Refresh leader failed");
      }
    } catch (InterruptedException | TimeoutException e) {
      throw new RaftConnectionException("Refresh leader failed");
    }
    return RouteTable.getInstance().selectLeader(groupId);
  }


  public static Configuration getConfiguration(String groupID) {
    return null;
  }

  public static PeerId convertPhysicalNode(PhysicalNode node) {
    return new PeerId(node.ip, node.port);
  }

  public static PhysicalNode convertPeerId(PeerId peer) {
    return new PhysicalNode(peer.getIp(), peer.getPort());
  }

  public static PeerId[] convertStringArrayToPeerIdArray(String[] nodes) {
    PeerId[] peerIds = new PeerId[nodes.length];
    for (int i = 0; i < nodes.length; i++) {
      peerIds[i] = PeerId.parsePeer(nodes[i]);
    }
    return peerIds;
  }

  public static int getIndexOfIpFromRaftNodeList(String ip, PeerId[] peerIds) {
    for (int i = 0; i < peerIds.length; i++) {
      if (peerIds[i].getIp().equals(ip)) {
        return i;
      }
    }
    return -1;
  }

  public static PhysicalNode[] convertPeerIdArrayToPhysicalNodeArray(PeerId[] peerIds) {
    PhysicalNode[] physicalNodes = new PhysicalNode[peerIds.length];
    for (int i = 0; i < peerIds.length; i++) {
      physicalNodes[i] = new PhysicalNode(peerIds[i].getIp(), peerIds[i].getPort());
    }
    return physicalNodes;
  }

  public static PeerId[] convertPhysicalNodeArrayToPeerIdArray(PhysicalNode[] physicalNodes) {
    PeerId[] peerIds = new PeerId[physicalNodes.length];
    for (int i = 0; i < physicalNodes.length; i++) {
      peerIds[i] = new PeerId(physicalNodes[i].getIp(), physicalNodes[i].getPort());
    }
    return peerIds;
  }

}
