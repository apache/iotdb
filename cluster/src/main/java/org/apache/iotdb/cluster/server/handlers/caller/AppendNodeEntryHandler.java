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

import static org.apache.iotdb.cluster.server.Response.RESPONSE_AGREE;
import static org.apache.iotdb.cluster.server.Response.RESPONSE_LOG_MISMATCH;
import static org.apache.iotdb.cluster.server.Response.RESPONSE_PARTITION_TABLE_UNAVAILABLE;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.iotdb.cluster.log.Log;
import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.cluster.rpc.thrift.RaftService.AsyncClient.appendEntry_call;
import org.apache.iotdb.cluster.server.member.RaftMember;
import org.apache.thrift.TException;
import org.apache.thrift.async.AsyncMethodCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * AppendNodeEntryHandler checks if the log is successfully appended by the quorum or some node has
 * rejected it for some reason when one node has finished the AppendEntryRequest.
 * The target of the log is the single nodes, it requires the quorum of the nodes to reach
 * consistency.
 */
public class AppendNodeEntryHandler implements AsyncMethodCallback<appendEntry_call> {

  private static final Logger logger = LoggerFactory.getLogger(AppendNodeEntryHandler.class);

  private RaftMember raftMember;
  private Log log;
  private AtomicInteger quorum;
  private AtomicBoolean leaderShipStale;
  private Node follower;

  public AppendNodeEntryHandler(RaftMember raftMember) {
    this.raftMember = raftMember;
  }

  @Override
  public void onComplete(appendEntry_call response) {
    try {
      if (leaderShipStale.get()) {
        // someone has rejected this log because the leadership is stale
        return;
      }
      long resp = response.getResult();
      synchronized (quorum) {
        if (resp == RESPONSE_AGREE) {
          int remaining = quorum.decrementAndGet();
          logger.debug("Received an agreement from {} for {}, remaining votes to succeed: {}",
              follower, log, remaining);
          if (remaining == 0) {
            logger.debug("Log {} is accepted by the quorum", log);
            quorum.notifyAll();
          }
        } else if (resp != RESPONSE_LOG_MISMATCH && resp != RESPONSE_PARTITION_TABLE_UNAVAILABLE) {
          // the leader ship is stale, wait for the new leader's heartbeat
          leaderShipStale.set(true);
          raftMember.retireFromLeader(resp, follower);
          quorum.notifyAll();
        }
        // rejected because the follower's logs are stale or the follower has no cluster info, just
        // wait for the heartbeat to handle
      }
    } catch (TException e) {
      onError(e);
    }
  }

  @Override
  public void onError(Exception exception) {
    logger.warn("Cannot append log {} to {}", log, follower, exception);
  }

  public Log getLog() {
    return log;
  }

  public void setLog(Log log) {
    this.log = log;
  }

  public AtomicInteger getQuorum() {
    return quorum;
  }

  public void setQuorum(AtomicInteger quorum) {
    this.quorum = quorum;
  }

  public AtomicBoolean getLeaderShipStale() {
    return leaderShipStale;
  }

  public void setLeaderShipStale(AtomicBoolean leaderShipStale) {
    this.leaderShipStale = leaderShipStale;
  }

  public Node getFollower() {
    return follower;
  }

  public void setFollower(Node follower) {
    this.follower = follower;
  }
}
