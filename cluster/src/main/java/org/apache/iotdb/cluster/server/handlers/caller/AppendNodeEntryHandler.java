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
import org.apache.iotdb.cluster.server.monitor.Peer;
import org.apache.iotdb.cluster.server.monitor.Timer;
import org.apache.iotdb.cluster.server.monitor.Timer.Statistic;

import org.apache.thrift.async.AsyncMethodCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.ConnectException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.iotdb.cluster.server.Response.RESPONSE_AGREE;

/**
 * AppendNodeEntryHandler checks if the log is successfully appended by the quorum or some node has
 * rejected it for some reason when one node has finished the AppendEntryRequest. The target of the
 * log is the single nodes, it requires the agreement from the quorum of the nodes to reach
 * consistency.
 */
public class AppendNodeEntryHandler implements AsyncMethodCallback<Long> {

  private static final Logger logger = LoggerFactory.getLogger(AppendNodeEntryHandler.class);

  private RaftMember member;
  private AtomicLong receiverTerm;
  private Log log;
  private AtomicInteger voteCounter;
  private AtomicBoolean leaderShipStale;
  private Node receiver;
  private Peer peer;
  // initialized as the quorum size, and decrease by 1 each time when we receive a rejection or
  // an exception, upon decreased to zero, the request will be early-aborted
  private int failedDecreasingCounter;

  // nano start time when the send begins
  private long sendStart = Long.MIN_VALUE;

  public AppendNodeEntryHandler() {
    if (Timer.ENABLE_INSTRUMENTING
        && ClusterDescriptor.getInstance().getConfig().isUseAsyncServer()) {
      sendStart = System.nanoTime();
    }
  }

  @Override
  public void onComplete(Long response) {
    if (Timer.ENABLE_INSTRUMENTING) {
      Statistic.RAFT_SENDER_SEND_LOG_ASYNC.calOperationCostTimeFromStart(sendStart);
    }
    if (voteCounter.get() == Integer.MAX_VALUE) {
      // the request already failed
      return;
    }
    logger.debug("{}: Append response {} from {}", member.getName(), response, receiver);
    if (leaderShipStale.get()) {
      // someone has rejected this log because the leadership is stale
      return;
    }
    long resp = response;
    synchronized (voteCounter) {
      if (resp == RESPONSE_AGREE) {
        int remaining = voteCounter.decrementAndGet();
        logger.debug(
            "{}: Received an agreement from {} for {}, remaining votes to succeed: {}",
            member.getName(),
            receiver,
            log,
            remaining);
        if (remaining == 0) {
          logger.debug(
              "{}: Log [{}] {} is accepted by the quorum",
              member.getName(),
              log.getCurrLogIndex(),
              log);
          voteCounter.notifyAll();
        }
        peer.setMatchIndex(Math.max(log.getCurrLogIndex(), peer.getMatchIndex()));
      } else if (resp > 0) {
        // a response > 0 is the follower's term
        // the leader ship is stale, wait for the new leader's heartbeat
        long prevReceiverTerm = receiverTerm.get();
        logger.debug(
            "{}: Received a rejection from {} because term is stale: {}/{}",
            member.getName(),
            receiver,
            prevReceiverTerm,
            resp);
        if (resp > prevReceiverTerm) {
          receiverTerm.set(resp);
        }
        leaderShipStale.set(true);
        voteCounter.notifyAll();
      } else {
        // e.g., Response.RESPONSE_LOG_MISMATCH
        logger.debug(
            "{}: The log {} is rejected by {} because: {}", member.getName(), log, receiver, resp);
        onFail();
      }
      // rejected because the receiver's logs are stale or the receiver has no cluster info, just
      // wait for the heartbeat to handle
    }
  }

  @Override
  public void onError(Exception exception) {
    if (exception instanceof ConnectException) {
      logger.warn(
          "{}: Cannot append log {}: cannot connect to {}: {}",
          member.getName(),
          log,
          receiver,
          exception.getMessage());
    } else {
      logger.warn("{}: Cannot append log {} to {}", member.getName(), log, receiver, exception);
    }
    onFail();
  }

  private void onFail() {
    synchronized (voteCounter) {
      failedDecreasingCounter--;
      if (failedDecreasingCounter <= 0) {
        // quorum members have failed, there is no need to wait for others
        voteCounter.set(Integer.MAX_VALUE);
        voteCounter.notifyAll();
      }
    }
  }

  public void setLog(Log log) {
    this.log = log;
  }

  public void setMember(RaftMember member) {
    this.member = member;
  }

  public void setVoteCounter(AtomicInteger voteCounter) {
    this.voteCounter = voteCounter;
    this.failedDecreasingCounter =
        ClusterDescriptor.getInstance().getConfig().getReplicationNum() - voteCounter.get();
  }

  public void setLeaderShipStale(AtomicBoolean leaderShipStale) {
    this.leaderShipStale = leaderShipStale;
  }

  public void setPeer(Peer peer) {
    this.peer = peer;
  }

  public void setReceiver(Node follower) {
    this.receiver = follower;
  }

  public void setReceiverTerm(AtomicLong receiverTerm) {
    this.receiverTerm = receiverTerm;
  }
}
