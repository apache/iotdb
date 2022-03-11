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

import org.apache.iotdb.cluster.client.sync.SyncMetaClient;
import org.apache.iotdb.cluster.config.ClusterConstant;
import org.apache.iotdb.cluster.exception.AddSelfException;
import org.apache.iotdb.cluster.exception.CheckConsistencyException;
import org.apache.iotdb.cluster.exception.LeaderUnknownException;
import org.apache.iotdb.cluster.exception.LogExecutionException;
import org.apache.iotdb.cluster.exception.PartitionTableUnavailableException;
import org.apache.iotdb.cluster.log.logtypes.RemoveNodeLog;
import org.apache.iotdb.cluster.rpc.thrift.AddNodeResponse;
import org.apache.iotdb.cluster.rpc.thrift.AppendEntryRequest;
import org.apache.iotdb.cluster.rpc.thrift.AppendEntryResult;
import org.apache.iotdb.cluster.rpc.thrift.CheckStatusResponse;
import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.cluster.rpc.thrift.SendSnapshotRequest;
import org.apache.iotdb.cluster.rpc.thrift.StartUpStatus;
import org.apache.iotdb.cluster.rpc.thrift.TNodeStatus;
import org.apache.iotdb.cluster.rpc.thrift.TSMetaService;
import org.apache.iotdb.cluster.server.NodeCharacter;
import org.apache.iotdb.cluster.server.Response;
import org.apache.iotdb.cluster.server.member.MetaGroupMember;
import org.apache.iotdb.cluster.utils.ClientUtils;
import org.apache.iotdb.cluster.utils.ClusterUtils;

import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;

public class MetaSyncService extends BaseSyncService implements TSMetaService.Iface {

  private static final Logger logger = LoggerFactory.getLogger(MetaSyncService.class);

  private MetaGroupMember metaGroupMember;

  public MetaSyncService(MetaGroupMember metaGroupMember) {
    super(metaGroupMember);
    this.metaGroupMember = metaGroupMember;
  }

  // behavior of followers
  @Override
  public AppendEntryResult appendEntry(AppendEntryRequest request) throws TException {
    // if the metaGroupMember is not ready (e.g., as a follower the PartitionTable is loaded
    // locally, but the partition table is not verified), we do not handle the RPC requests.
    if (!metaGroupMember.isReady()) {
      // the only special case is that the leader will send an empty entry for letting followers
      // submit  previous log
      // at this time, the partitionTable has been loaded but is not verified. So the PRC is not
      // ready.
      if (metaGroupMember.getPartitionTable() == null) {
        // this node lacks information of the cluster and refuse to work
        logger.debug("This node is blind to the cluster and cannot accept logs, {}", request);
        return new AppendEntryResult(Response.RESPONSE_PARTITION_TABLE_UNAVAILABLE);
      } else {
        // do nothing because we consider if the partitionTable is loaded, then it is corrected.
      }
    }

    return super.appendEntry(request);
  }

  private static final String ERROR_MSG_META_NOT_READY = "The metadata not is not ready.";

  @Override
  public AddNodeResponse addNode(Node node, StartUpStatus startUpStatus) throws TException {
    AddNodeResponse addNodeResponse;
    if (!metaGroupMember.isReady()) {
      logger.debug(ERROR_MSG_META_NOT_READY);
      throw new TException(ERROR_MSG_META_NOT_READY);
    }

    try {
      addNodeResponse = metaGroupMember.addNode(node, startUpStatus);
    } catch (AddSelfException | LogExecutionException | CheckConsistencyException e) {
      throw new TException(e);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new TException(e);
    }
    if (addNodeResponse != null) {
      return addNodeResponse;
    }

    if (member.getCharacter() == NodeCharacter.FOLLOWER
        && member.getLeader() != null
        && !ClusterConstant.EMPTY_NODE.equals(member.getLeader())) {
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
    // even the meta engine is not ready, we still need to catch up.
    try {
      metaGroupMember.receiveSnapshot(request);
    } catch (Exception e) {
      throw new TException(e);
    }
  }

  @Override
  public CheckStatusResponse checkStatus(StartUpStatus startUpStatus) {
    // this method is called before the meta engine is ready.
    return ClusterUtils.checkStatus(startUpStatus, metaGroupMember.getStartUpStatus());
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
        return client.addNode(node, startUpStatus);
      } catch (TException e) {
        client.getInputProtocol().getTransport().close();
        logger.warn("Cannot connect to node {}", node, e);
      } finally {
        ClientUtils.putBackSyncClient(client);
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
  public ByteBuffer collectMigrationStatus() {
    return ClusterUtils.serializeMigrationStatus(metaGroupMember.collectMigrationStatus());
  }

  @Override
  public long removeNode(Node node) throws TException {
    if (!metaGroupMember.isReady()) {
      logger.debug(ERROR_MSG_META_NOT_READY);
      throw new TException(ERROR_MSG_META_NOT_READY);
    }

    long result;
    try {
      result = metaGroupMember.removeNode(node);
    } catch (PartitionTableUnavailableException
        | LogExecutionException
        | CheckConsistencyException e) {
      logger.error("Can not remove node {}", node, e);
      throw new TException(e);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      logger.error("Can not remove node {}", node, e);
      throw new TException(e);
    }

    if (result != Response.RESPONSE_NULL) {
      return result;
    }

    if (metaGroupMember.getCharacter() == NodeCharacter.FOLLOWER
        && metaGroupMember.getLeader() != null) {
      logger.info(
          "Forward the node removal request of {} to leader {}", node, metaGroupMember.getLeader());
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
   * @param node the node to be removed
   * @return true if the request is successfully forwarded, false otherwise
   */
  private Long forwardRemoveNode(Node node) {
    SyncMetaClient client =
        (SyncMetaClient) metaGroupMember.getSyncClient(metaGroupMember.getLeader());
    if (client != null) {
      try {
        return client.removeNode(node);
      } catch (TException e) {
        client.getInputProtocol().getTransport().close();
        logger.warn("Cannot connect to node {}", node, e);
      } finally {
        ClientUtils.putBackSyncClient(client);
      }
    }
    return null;
  }

  /**
   * Process a request that the local node is removed from the cluster. As a node is removed from
   * the cluster, it no longer receive heartbeats or logs and cannot know it has been removed, so we
   * must tell it directly.
   */
  @Override
  public void exile(ByteBuffer removeNodeLogBuffer) {
    logger.info("{}: start to exile.", name);
    removeNodeLogBuffer.get();
    RemoveNodeLog removeNodeLog = new RemoveNodeLog();
    removeNodeLog.deserialize(removeNodeLogBuffer);
    metaGroupMember.getPartitionTable().deserialize(removeNodeLog.getPartitionTable());
    metaGroupMember.applyRemoveNode(removeNodeLog);
  }

  @Override
  public void handshake(Node sender) {
    metaGroupMember.handleHandshake(sender);
  }

  @Override
  public void acknowledgeAppendEntry(AppendEntryResult ack) {
    metaGroupMember.acknowledgeAppendLog(ack);
  }
}
