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

package org.apache.iotdb.cluster.server.handlers.caller;

import org.apache.iotdb.cluster.config.ClusterDescriptor;
import org.apache.iotdb.cluster.log.Log;
import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.cluster.server.member.RaftMember;

import org.apache.thrift.async.AsyncMethodCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.iotdb.cluster.server.Response.RESPONSE_AGREE;

/**
 * AppendGroupEntryHandler checks if the log is successfully appended by the quorum or some node has
 * rejected it for some reason when one node has finished the AppendEntryRequest. The target of the
 * log is the data groups, the consistency can be reached as long as quorum data groups agree, even
 * if the actually agreed nodes can be less than quorum, because the same nodes may say "yes" for
 * multiple groups.
 */
public class AppendGroupEntryHandler implements AsyncMethodCallback<Long> {

  private static final Logger logger = LoggerFactory.getLogger(AppendGroupEntryHandler.class);

  private RaftMember member;
  private Log log;
  // the number of nodes that accept the log in each group
  // to succeed, each number should reach zero
  // for example: assuming there are 4 nodes and 3 replicas, then the initial array will be:
  // [2, 2, 2, 2]. And if node0 accepted the log, as node0 is in group 2,3,0, the array will be
  // [1, 2, 1, 1].
  private int[] groupReceivedCounter;
  // the index of the node which the request sends log to, if the node accepts the log, all
  // groups' counters the node is in should decrease
  private int receiverNodeIndex;
  private Node receiverNode;
  // store the flag of leadership lost and the new leader's term
  private AtomicBoolean leaderShipStale;
  private AtomicLong newLeaderTerm;
  private int replicationNum = ClusterDescriptor.getInstance().getConfig().getReplicationNum();

  private AtomicInteger erroredNodeNum = new AtomicInteger(0);

  public AppendGroupEntryHandler(
      int[] groupReceivedCounter,
      int receiverNodeIndex,
      Node receiverNode,
      AtomicBoolean leaderShipStale,
      Log log,
      AtomicLong newLeaderTerm,
      RaftMember member) {
    this.groupReceivedCounter = groupReceivedCounter;
    this.receiverNodeIndex = receiverNodeIndex;
    this.receiverNode = receiverNode;
    this.leaderShipStale = leaderShipStale;
    this.log = log;
    this.newLeaderTerm = newLeaderTerm;
    this.member = member;
  }

  @Override
  public void onComplete(Long response) {
    if (leaderShipStale.get()) {
      // someone has rejected this log because the leadership is stale
      return;
    }

    long resp = response;

    if (resp == RESPONSE_AGREE) {
      processAgreement();
    } else if (resp > 0) {
      // a response > 0 is the term fo the follower
      synchronized (groupReceivedCounter) {
        // the leader ship is stale, abort and wait for the new leader's heartbeat
        long previousNewTerm = newLeaderTerm.get();
        if (previousNewTerm < resp) {
          newLeaderTerm.set(resp);
        }
        leaderShipStale.set(true);
        groupReceivedCounter.notifyAll();
      }
    }
    // rejected because the follower's logs are stale or the follower has no cluster info, just
    // wait for the heartbeat to handle
  }

  /**
   * Decrease all related counters of the receiver node. See the field "groupReceivedCounter" for an
   * example. If all counters reach 0, wake the waiting thread to welcome the success.
   */
  private void processAgreement() {
    synchronized (groupReceivedCounter) {
      logger.debug("{}: Node {} has accepted log {}", member.getName(), receiverNode, log);
      // this node is contained in REPLICATION_NUM groups, decrease the counters of these groups
      for (int i = 0; i < replicationNum; i++) {
        int nodeIndex = receiverNodeIndex - i;
        if (nodeIndex < 0) {
          nodeIndex += groupReceivedCounter.length;
        }
        groupReceivedCounter[nodeIndex]--;
      }

      // examine if all groups has agreed
      boolean allAgreed = true;
      for (int remaining : groupReceivedCounter) {
        if (remaining > 0) {
          allAgreed = false;
          break;
        }
      }
      if (allAgreed) {
        // wake up the parent thread to welcome the new node
        groupReceivedCounter.notifyAll();
      }
    }
  }

  @Override
  public void onError(Exception exception) {
    logger.error(
        "{}: Cannot send the add node request to node {}",
        member.getName(),
        receiverNode,
        exception);
    if (erroredNodeNum.incrementAndGet() >= replicationNum / 2) {
      synchronized (groupReceivedCounter) {
        logger.error(
            "{}: Over half of the nodes failed, the request is rejected", member.getName());
        groupReceivedCounter.notifyAll();
      }
    }
  }
}
