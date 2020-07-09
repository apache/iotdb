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

package org.apache.iotdb.cluster.server.service;

import java.util.Arrays;
import java.util.List;
import org.apache.iotdb.cluster.client.sync.SyncMetaClient;
import org.apache.iotdb.cluster.config.ClusterConstant;
import org.apache.iotdb.cluster.config.ClusterDescriptor;
import org.apache.iotdb.cluster.exception.AddSelfException;
import org.apache.iotdb.cluster.exception.LeaderUnknownException;
import org.apache.iotdb.cluster.exception.LogExecutionException;
import org.apache.iotdb.cluster.exception.PartitionTableUnavailableException;
import org.apache.iotdb.cluster.rpc.thrift.AddNodeResponse;
import org.apache.iotdb.cluster.rpc.thrift.AppendEntryRequest;
import org.apache.iotdb.cluster.rpc.thrift.CheckStatusResponse;
import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.cluster.rpc.thrift.SendSnapshotRequest;
import org.apache.iotdb.cluster.rpc.thrift.StartUpStatus;
import org.apache.iotdb.cluster.rpc.thrift.TNodeStatus;
import org.apache.iotdb.cluster.rpc.thrift.TSMetaService;
import org.apache.iotdb.cluster.server.NodeCharacter;
import org.apache.iotdb.cluster.server.Response;
import org.apache.iotdb.cluster.server.member.MetaGroupMember;
import org.apache.iotdb.cluster.utils.ClusterUtils;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MetaSyncService extends BaseSyncService implements TSMetaService.Iface {

  private static final Logger logger = LoggerFactory.getLogger(MetaSyncService.class);

  private MetaGroupMember metaGroupMember;

  public MetaSyncService(MetaGroupMember metaGroupMember) {
    super(metaGroupMember);
    this.metaGroupMember = metaGroupMember;
  }

  @Override
  public long appendEntry(AppendEntryRequest request) throws TException {
    if (metaGroupMember.getPartitionTable() == null) {
      // this node lacks information of the cluster and refuse to work
      logger.debug("This node is blind to the cluster and cannot accept logs");
      return Response.RESPONSE_PARTITION_TABLE_UNAVAILABLE;
    }

    return super.appendEntry(request);
  }

  @Override
  public AddNodeResponse addNode(Node node, StartUpStatus startUpStatus) throws TException {
    AddNodeResponse addNodeResponse;
    try {
      addNodeResponse = metaGroupMember.addNode(node, startUpStatus);
    } catch (AddSelfException | LogExecutionException e) {
      throw new TException(e);
    }
    if (addNodeResponse != null) {
      return addNodeResponse;
    }

    if (member.getCharacter() == NodeCharacter.FOLLOWER && member.getLeader() != null) {
      logger.info("Forward the join request of {} to leader {}", node, member.getLeader());
      addNodeResponse = forwardAddNode(node, startUpStatus);
      if (addNodeResponse != null) {
        return addNodeResponse;
      }
    }
    throw new TException(new LeaderUnknownException(member.getAllNodes()));
  }

  @Override
  public void sendSnapshot(SendSnapshotRequest request) throws TException {
    try {
      metaGroupMember.sendSnapshot(request);
    } catch (Exception e) {
      throw new TException(e);
    }
  }

  @Override
  public CheckStatusResponse checkStatus(StartUpStatus startUpStatus) {
    // check status of the new node
    long remotePartitionInterval = startUpStatus.getPartitionInterval();
    int remoteHashSalt = startUpStatus.getHashSalt();
    int remoteReplicationNum = startUpStatus.getReplicationNumber();
    List<Node> remoteSeedNodeList = startUpStatus.getSeedNodeList();
    long localPartitionInterval = IoTDBDescriptor.getInstance().getConfig()
        .getPartitionInterval();
    int localHashSalt = ClusterConstant.HASH_SALT;
    int localReplicationNum = ClusterDescriptor.getInstance().getConfig().getReplicationNum();
    boolean partitionIntervalEquals = true;
    boolean hashSaltEquals = true;
    boolean replicationNumEquals = true;
    boolean seedNodeListEquals = true;

    if (localPartitionInterval != remotePartitionInterval) {
      partitionIntervalEquals = false;
      logger.info("Remote partition interval conflicts with the leader's. Leader: {}, remote: {}",
          localPartitionInterval, remotePartitionInterval);
    }
    if (localHashSalt != remoteHashSalt) {
      hashSaltEquals = false;
      logger.info("Remote hash salt conflicts with the leader's. Leader: {}, remote: {}",
          localHashSalt, remoteHashSalt);
    }
    if (localReplicationNum != remoteReplicationNum) {
      replicationNumEquals = false;
      logger.info("Remote replication number conflicts with the leader's. Leader: {}, remote: {}",
          localReplicationNum, remoteReplicationNum);
    }
    if (!ClusterUtils
        .checkSeedNodes(false, (List<Node>) metaGroupMember.getAllNodes(), remoteSeedNodeList)) {
      seedNodeListEquals = false;
      if (logger.isInfoEnabled()) {
        logger.info("Remote seed node list conflicts with the leader's. Leader: {}, remote: {}",
            Arrays.toString(metaGroupMember.getAllNodes().toArray(new Node[0])),
            Arrays.toString(remoteSeedNodeList.toArray(new Node[0])));
      }
    }

    return new CheckStatusResponse(partitionIntervalEquals, hashSaltEquals,
        replicationNumEquals, seedNodeListEquals);
  }

  /**
   * Forward the join cluster request to the leader.
   *
   * @return true if the forwarding succeeds, false otherwise.
   */
  private AddNodeResponse forwardAddNode(Node node, StartUpStatus startUpStatus) {
    SyncMetaClient client =
        (SyncMetaClient) metaGroupMember.getSyncClient(metaGroupMember.getLeader());
    if (client != null) {
      try {
        AddNodeResponse response = client.addNode(node, startUpStatus);
        putBackSyncClient(client);
        return response;
      } catch (TException e) {
        logger.warn("Cannot connect to node {}", node, e);
      }
    }
    return null;
  }

  /**
   * Return the status of the node to the requester that will help the requester figure out the load
   * of the this node and how well it may perform for a specific query.
   *
   * @return
   */
  @Override
  public TNodeStatus queryNodeStatus() {
    return new TNodeStatus();
  }

  @Override
  public Node checkAlive() {
    return metaGroupMember.getThisNode();
  }

  @Override
  public long removeNode(Node node) throws TException {
    long result;
    try {
      result = metaGroupMember.removeNode(node);
    } catch (PartitionTableUnavailableException | LogExecutionException e) {
      throw new TException(e);
    }

    if (result != Response.RESPONSE_NULL) {
      return result;
    }

    if (metaGroupMember.getCharacter() == NodeCharacter.FOLLOWER && metaGroupMember.getLeader() != null) {
      logger.info("Forward the node removal request of {} to leader {}", node, metaGroupMember.getLeader());
      Long rst = forwardRemoveNode(node);
      if (rst != null) {
        return rst;
      }
    }
    throw new TException(new LeaderUnknownException(metaGroupMember.getAllNodes()));
  }

  /**
   * Forward a node removal request to the leader.
   *
   * @param node          the node to be removed
   * @return true if the request is successfully forwarded, false otherwise
   */
  private Long forwardRemoveNode(Node node) {
    SyncMetaClient client =
        (SyncMetaClient) metaGroupMember.getSyncClient(metaGroupMember.getLeader());
    if (client != null) {
      try {
        long result = client.removeNode(node);
        putBackSyncClient(client);
        return result;
      } catch (TException e) {
        logger.warn("Cannot connect to node {}", node, e);
      }
    }
    return null;
  }

  /**
   * Process a request that the local node is removed from the cluster. As a node is removed from
   * the cluster, it no longer receive heartbeats or logs and cannot know it has been removed, so we
   * must tell it directly.
   *
   */
  @Override
  public void exile() {
    metaGroupMember.applyRemoveNode(metaGroupMember.getThisNode());
  }
}
