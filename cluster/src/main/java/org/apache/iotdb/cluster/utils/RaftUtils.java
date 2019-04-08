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

import com.alipay.remoting.AsyncContext;
import com.alipay.remoting.exception.CodecException;
import com.alipay.remoting.serialization.SerializerManager;
import com.alipay.sofa.jraft.RouteTable;
import com.alipay.sofa.jraft.conf.Configuration;
import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.entity.Task;
import com.alipay.sofa.jraft.rpc.impl.cli.BoltCliClientService;
import com.alipay.sofa.jraft.util.Bits;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeoutException;
import org.apache.iotdb.cluster.callback.QPTask;
import org.apache.iotdb.cluster.config.ClusterConfig;
import org.apache.iotdb.cluster.entity.Server;
import org.apache.iotdb.cluster.entity.raft.DataPartitionRaftHolder;
import org.apache.iotdb.cluster.entity.raft.MetadataRaftHolder;
import org.apache.iotdb.cluster.entity.raft.RaftService;
import org.apache.iotdb.cluster.exception.RaftConnectionException;
import org.apache.iotdb.cluster.rpc.closure.ResponseClosure;
import org.apache.iotdb.cluster.rpc.request.BasicRequest;
import org.apache.iotdb.cluster.rpc.response.BasicResponse;
import org.apache.iotdb.cluster.utils.hash.PhysicalNode;
import org.apache.iotdb.cluster.utils.hash.Router;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RaftUtils {

  private static final Logger LOGGER = LoggerFactory.getLogger(RaftUtils.class);
  private static final Server server = Server.getInstance();
  private static final Router router = Router.getInstance();
  /**
   * The cache will be update in two case: 1. When @onLeaderStart() method of state machine is
   * called, the cache will be update. 2. When @getLeaderPeerID() in this class is called and cache
   * don't have the key, it's will get random peer and update. 3. When @redirected of BasicRequest
   * is true, the task will be retry and the cache will update.
   */
  private static final ConcurrentHashMap<String, PeerId> groupLeaderCache = new ConcurrentHashMap<>();

  private RaftUtils() {
  }

  /**
   * Get peer id to send request. If groupLeaderCache has the group id, then return leader id of the
   * group.Otherwise, random get a peer of the group.
   *
   * @return leader id
   */
  public static PeerId getLeaderPeerID(String groupId) {
    if (!groupLeaderCache.containsKey(groupId)) {
      PeerId randomPeerId = getRandomPeerID(groupId);
      groupLeaderCache.put(groupId, randomPeerId);
    }
    return groupLeaderCache.get(groupId);
  }

  /**
   * Get random peer id
   */
  public static PeerId getRandomPeerID(String groupId) {
    PeerId randomPeerId;
    if (groupId.equals(ClusterConfig.METADATA_GROUP_ID)) {
      RaftService service = (RaftService) server.getMetadataHolder().getService();
      List<PeerId> peerIdList = service.getPeerIdList();
      randomPeerId = peerIdList.get(getRandomInt(peerIdList.size()));
    } else {
      PhysicalNode[] physicalNodes = router.getNodesByGroupId(groupId);
      PhysicalNode node = physicalNodes[getRandomInt(physicalNodes.length)];
      randomPeerId = getPeerIDFrom(node);
    }
    return randomPeerId;
  }

  /**
   * Get random int from [0, bound).
   */
  public static int getRandomInt(int bound) {
    return ThreadLocalRandom.current().nextInt(bound);
  }

  public static PeerId getPeerIDFrom(PhysicalNode node) {
    return new PeerId(node.ip, node.port);
  }

  public static PhysicalNode getPhysicalNodeFrom(PeerId peer) {
    return new PhysicalNode(peer.getIp(), peer.getPort());
  }

  /**
   * @param nodes each node string is in the format of "ip:port:idx",
   */
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

  public static PhysicalNode[] getPhysicalNodeArrayFrom(PeerId[] peerIds) {
    PhysicalNode[] physicalNodes = new PhysicalNode[peerIds.length];
    for (int i = 0; i < peerIds.length; i++) {
      physicalNodes[i] = new PhysicalNode(peerIds[i].getIp(), peerIds[i].getPort());
    }
    return physicalNodes;
  }

  public static PeerId[] getPeerIdArrayFrom(PhysicalNode[] physicalNodes) {
    PeerId[] peerIds = new PeerId[physicalNodes.length];
    for (int i = 0; i < physicalNodes.length; i++) {
      peerIds[i] = getPeerIDFrom(physicalNodes[i]);
    }
    return peerIds;
  }

  /**
   * Update raft group leader
   *
   * @param groupId group id
   * @param peerId leader id
   */
  public static void updateRaftGroupLeader(String groupId, PeerId peerId) {
    groupLeaderCache.put(groupId, peerId);
    LOGGER.info("group leader cache:{}", groupLeaderCache);
  }

  /**
   * Execute raft task for local processor
   *
   * @param service raft service
   */
  public static boolean executeRaftTaskForLocalProcessor(RaftService service, QPTask qpTask,
      BasicResponse response) throws InterruptedException {
    BasicRequest request = qpTask.getRequest();

    Task task = new Task();
    ResponseClosure closure = new ResponseClosure(response, status -> {
      response.addResult(status.isOk());
      if (!status.isOk()) {
        response.setErrorMsg(status.getErrorMsg());
      }
      qpTask.run(response);
    });
    task.setDone(closure);
    try {
      task.setData(ByteBuffer
          .wrap(SerializerManager.getSerializer(SerializerManager.Hessian2).serialize(request)));
    } catch (CodecException e) {
      return false;
    }
    service.getNode().apply(task);
    qpTask.await();
    return qpTask.getResponse().isSuccess();
  }


  /**
   * Execute raft task for rpc processor
   *
   * @param service raft service
   */
  public static void executeRaftTaskForRpcProcessor(RaftService service, AsyncContext asyncContext,
      BasicRequest request, BasicResponse response) {
    final Task task = new Task();
    ResponseClosure closure = new ResponseClosure(response, status -> {
      response.addResult(status.isOk());
      if (!status.isOk()) {
        response.setErrorMsg(status.getErrorMsg());
      }
      asyncContext.sendResponse(response);
    });
    task.setDone(closure);
    try {
      task.setData(ByteBuffer
          .wrap(SerializerManager.getSerializer(SerializerManager.Hessian2)
              .serialize(request)));
      service.getNode().apply(task);
    } catch (final CodecException e) {
      response.setErrorMsg(e.getMessage());
      response.addResult(false);
      asyncContext.sendResponse(response);
    }
  }

  /**
   * Get data partition raft holder by group id
   */
  public static DataPartitionRaftHolder getDataPartitonRaftHolder(String groupId) {
    return (DataPartitionRaftHolder) server.getDataPartitionHolderMap().get(groupId);
  }

  /**
   * Get metadata raft holder
   */
  public static MetadataRaftHolder getMetadataRaftHolder() {
    return (MetadataRaftHolder) server.getMetadataHolder();
  }

  /**
   * Create a new raft request context by request id
   */
  public static byte[] createRaftRequestContext(int requestId) {
    final byte[] reqContext = new byte[4];
    Bits.putInt(reqContext, 0, requestId);
    return reqContext;
  }
}
