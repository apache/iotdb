/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at      http://www.apache.org/licenses/LICENSE-2.0  Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the License for the specific language governing permissions and limitations under the License.
 */

package org.apache.iotdb.cluster.server.handlers.caller;

import static org.apache.iotdb.cluster.server.Response.RESPONSE_AGREE;
import static org.apache.iotdb.cluster.server.Response.RESPONSE_LOG_MISMATCH;
import static org.apache.iotdb.cluster.server.Response.RESPONSE_PARTITION_TABLE_UNAVAILABLE;
import static org.apache.iotdb.cluster.server.member.MetaGroupMember.REPLICATION_NUM;

import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.iotdb.cluster.log.Log;
import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.cluster.rpc.thrift.RaftService.AsyncClient.appendEntry_call;
import org.apache.iotdb.cluster.server.NodeCharacter;
import org.apache.iotdb.cluster.server.member.RaftMember;
import org.apache.thrift.TException;
import org.apache.thrift.async.AsyncMethodCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * AppendGroupEntryHandler checks if the log is successfully appended by the quorum or some node has
 * rejected it for some reason when one node has finished the AppendEntryRequest.
 * The target of the log is the data groups, the consistency can be reached as long as quorum
 * data groups agree, even if the actually agreed nodes can be less than quorum, because the same
 * nodes may say "yes" for multiple groups.
 */
public class AppendGroupEntryHandler implements AsyncMethodCallback<appendEntry_call> {

  private static final Logger logger = LoggerFactory.getLogger(AppendGroupEntryHandler.class);

  private Log log;
  private int[] groupRemainings;
  private int headerNodeIndex;
  private Node headerNode;
  private AtomicBoolean leaderShipStale;
  private RaftMember raftMember;

  public AppendGroupEntryHandler(int[] groupRemainings, int headerNodeIndex,
      Node headerNode, AtomicBoolean leaderShipStale, Log log, RaftMember raftMember) {
    this.groupRemainings = groupRemainings;
    this.headerNodeIndex = headerNodeIndex;
    this.headerNode = headerNode;
    this.leaderShipStale = leaderShipStale;
    this.log = log;
    this.raftMember = raftMember;
  }

  @Override
  public void onComplete(appendEntry_call response) {
    if (leaderShipStale.get()) {
      // someone has rejected this log because the leadership is stale
      return;
    }

    long resp;
    try {
      resp = response.getResult();
    } catch (TException e) {
      onError(e);
      return;
    }

    synchronized (groupRemainings) {
      if (resp == RESPONSE_AGREE) {
        logger.debug("Node {} has accepted log {}", headerNode, log);
        // this node is contained in REPLICATION_NUM groups, minus the counter for those nodes
        int startIndex = headerNodeIndex;
        for (int i = 0; i < REPLICATION_NUM; i++) {
          int nodeIndex = headerNodeIndex - i;
          if (nodeIndex < 0) {
            nodeIndex += groupRemainings.length;
          }
          groupRemainings[nodeIndex] --;
        }

        // examine if all groups has agreed
        boolean allAgreed = true;
        for (int remaining : groupRemainings) {
          if (remaining > 0) {
            allAgreed = false;
            break;
          }
        }
        if (allAgreed) {
          // wake up the parent thread to receive welcome the new node
          groupRemainings.notifyAll();
        }
      } else if (resp != RESPONSE_LOG_MISMATCH && resp != RESPONSE_PARTITION_TABLE_UNAVAILABLE) {
        // the leader ship is stale, wait for the new leader's heartbeat
        raftMember.retireFromLeader(resp, headerNode);
        leaderShipStale.set(true);
        groupRemainings.notifyAll();
      }
      // rejected because the follower's logs are stale or the follower has no cluster info, just
      // wait for the heartbeat to handle


    }
  }

  @Override
  public void onError(Exception exception) {
    synchronized (groupRemainings) {
      logger.error("Cannot send the add node request to node {}", headerNode, exception);
      groupRemainings.notifyAll();
    }
  }
}
