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

import org.apache.iotdb.cluster.config.ClusterConstant;
import org.apache.iotdb.cluster.exception.AddSelfException;
import org.apache.iotdb.cluster.exception.CheckConsistencyException;
import org.apache.iotdb.cluster.exception.LeaderUnknownException;
import org.apache.iotdb.cluster.exception.LogExecutionException;
import org.apache.iotdb.cluster.exception.PartitionTableUnavailableException;
import org.apache.iotdb.cluster.log.logtypes.RemoveNodeLog;
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

import org.apache.thrift.TException;
import org.apache.thrift.async.AsyncMethodCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;

public class MetaAsyncService extends BaseAsyncService implements TSMetaService.AsyncIface {

  private static final Logger logger = LoggerFactory.getLogger(MetaAsyncService.class);

  private MetaGroupMember metaGroupMember;

  public MetaAsyncService(MetaGroupMember metaGroupMember) {
    super(metaGroupMember);
    this.metaGroupMember = metaGroupMember;
  }

  @Override
  public void appendEntry(AppendEntryRequest request, AsyncMethodCallback resultHandler) {
    if (metaGroupMember.getPartitionTable() == null) {
      // this node lacks information of the cluster and refuse to work
      logger.debug("This node is blind to the cluster and cannot accept logs");
      resultHandler.onComplete(Response.RESPONSE_PARTITION_TABLE_UNAVAILABLE);
      return;
    }

    super.appendEntry(request, resultHandler);
  }

  @Override
  public void addNode(
      Node node, StartUpStatus startUpStatus, AsyncMethodCallback<AddNodeResponse> resultHandler) {
    AddNodeResponse addNodeResponse = null;
    try {
      addNodeResponse = metaGroupMember.addNode(node, startUpStatus);
    } catch (AddSelfException | LogExecutionException | CheckConsistencyException e) {
      resultHandler.onError(e);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      resultHandler.onError(e);
    }
    if (addNodeResponse != null) {
      resultHandler.onComplete(addNodeResponse);
      return;
    }

    if (member.getCharacter() == NodeCharacter.FOLLOWER
        && member.getLeader() != null
        && !ClusterConstant.EMPTY_NODE.equals(member.getLeader())) {
      logger.info("Forward the join request of {} to leader {}", node, member.getLeader());
      if (forwardAddNode(node, startUpStatus, resultHandler)) {
        return;
      }
    }
    resultHandler.onError(new LeaderUnknownException(member.getAllNodes()));
  }

  @Override
  public void sendSnapshot(SendSnapshotRequest request, AsyncMethodCallback<Void> resultHandler) {
    try {
      metaGroupMember.receiveSnapshot(request);
    } catch (Exception e) {
      resultHandler.onError(e);
      return;
    }
    resultHandler.onComplete(null);
  }

  @Override
  public void checkStatus(
      StartUpStatus startUpStatus, AsyncMethodCallback<CheckStatusResponse> resultHandler) {
    CheckStatusResponse response =
        ClusterUtils.checkStatus(startUpStatus, metaGroupMember.getNewStartUpStatus());
    resultHandler.onComplete(response);
  }

  /**
   * Forward the join cluster request to the leader.
   *
   * @return true if the forwarding succeeds, false otherwise.
   */
  private boolean forwardAddNode(
      Node node, StartUpStatus startUpStatus, AsyncMethodCallback<AddNodeResponse> resultHandler) {
    TSMetaService.AsyncClient client =
        (TSMetaService.AsyncClient) metaGroupMember.getAsyncClient(metaGroupMember.getLeader());
    if (client != null) {
      try {
        client.addNode(node, startUpStatus, resultHandler);
        return true;
      } catch (TException e) {
        logger.warn("Cannot connect to node {}", node, e);
      }
    }
    return false;
  }

  /**
   * Return the status of the node to the requester that will help the requester figure out the load
   * of the this node and how well it may perform for a specific query.
   *
   * @param resultHandler
   */
  @Override
  public void queryNodeStatus(AsyncMethodCallback<TNodeStatus> resultHandler) {
    resultHandler.onComplete(new TNodeStatus());
  }

  @Override
  public void checkAlive(AsyncMethodCallback<Node> resultHandler) {
    resultHandler.onComplete(metaGroupMember.getThisNode());
  }

  @Override
  public void collectMigrationStatus(AsyncMethodCallback<ByteBuffer> resultHandler) {
    resultHandler.onComplete(
        ClusterUtils.serializeMigrationStatus(metaGroupMember.collectMigrationStatus()));
  }

  @Override
  public void removeNode(Node node, AsyncMethodCallback<Long> resultHandler) {
    long result;
    try {
      result = metaGroupMember.removeNode(node);
    } catch (PartitionTableUnavailableException
        | LogExecutionException
        | CheckConsistencyException e) {
      logger.error("Can not remove node {}", node, e);
      resultHandler.onError(e);
      return;
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      logger.error("Can not remove node {}", node, e);
      resultHandler.onError(e);
      return;
    }

    if (result != Response.RESPONSE_NULL) {
      resultHandler.onComplete(result);
      return;
    }

    if (metaGroupMember.getCharacter() == NodeCharacter.FOLLOWER
        && metaGroupMember.getLeader() != null) {
      logger.info(
          "Forward the node removal request of {} to leader {}", node, metaGroupMember.getLeader());
      if (forwardRemoveNode(node, resultHandler)) {
        return;
      }
    }
    resultHandler.onError(new LeaderUnknownException(metaGroupMember.getAllNodes()));
  }

  /**
   * Forward a node removal request to the leader.
   *
   * @param node the node to be removed
   * @param resultHandler
   * @return true if the request is successfully forwarded, false otherwise
   */
  private boolean forwardRemoveNode(Node node, AsyncMethodCallback<Long> resultHandler) {
    TSMetaService.AsyncClient client =
        (TSMetaService.AsyncClient) metaGroupMember.getAsyncClient(metaGroupMember.getLeader());
    if (client != null) {
      try {
        client.removeNode(node, resultHandler);
        return true;
      } catch (TException e) {
        logger.warn("Cannot connect to node {}", node, e);
      }
    }
    return false;
  }

  /**
   * Process a request that the local node is removed from the cluster. As a node is removed from
   * the cluster, it no longer receive heartbeats or logs and cannot know it has been removed, so we
   * must tell it directly.
   *
   * @param resultHandler
   */
  @Override
  public void exile(ByteBuffer removeNodeLogBuffer, AsyncMethodCallback<Void> resultHandler) {
    logger.info("{}: start to exile.", name);
    removeNodeLogBuffer.get();
    RemoveNodeLog removeNodeLog = new RemoveNodeLog();
    removeNodeLog.deserialize(removeNodeLogBuffer);
    metaGroupMember.getPartitionTable().deserialize(removeNodeLog.getPartitionTable());
    metaGroupMember.applyRemoveNode(removeNodeLog);
    resultHandler.onComplete(null);
  }

  @Override
  public void handshake(Node sender, AsyncMethodCallback<Void> resultHandler) {
    metaGroupMember.handleHandshake(sender);
    resultHandler.onComplete(null);
  }
}
