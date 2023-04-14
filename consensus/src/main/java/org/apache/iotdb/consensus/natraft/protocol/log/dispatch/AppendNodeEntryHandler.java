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
package org.apache.iotdb.consensus.natraft.protocol.log.dispatch;

import org.apache.iotdb.commons.consensus.ConsensusGroupId;
import org.apache.iotdb.consensus.common.Peer;
import org.apache.iotdb.consensus.natraft.protocol.RaftMember;
import org.apache.iotdb.consensus.natraft.protocol.log.VotingEntry;
import org.apache.iotdb.consensus.natraft.utils.Timer.Statistic;
import org.apache.iotdb.consensus.raft.thrift.AppendEntryResult;

import org.apache.thrift.TApplicationException;
import org.apache.thrift.async.AsyncMethodCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.ConnectException;

import static org.apache.iotdb.consensus.natraft.utils.Response.RESPONSE_AGREE;
import static org.apache.iotdb.consensus.natraft.utils.Response.RESPONSE_LOG_MISMATCH;
import static org.apache.iotdb.consensus.natraft.utils.Response.RESPONSE_OUT_OF_WINDOW;
import static org.apache.iotdb.consensus.natraft.utils.Response.RESPONSE_STRONG_ACCEPT;
import static org.apache.iotdb.consensus.natraft.utils.Response.RESPONSE_WEAK_ACCEPT;

/**
 * AppendNodeEntryHandler checks if the log is successfully appended by the quorum or some node has
 * rejected it for some reason when one node has finished the AppendEntryRequest. The target of the
 * log is the single nodes, it requires the agreement from the quorum of the nodes to reach
 * consistency.
 */
public class AppendNodeEntryHandler implements AsyncMethodCallback<AppendEntryResult> {

  private static final Logger logger = LoggerFactory.getLogger(AppendNodeEntryHandler.class);

  protected RaftMember member;
  protected VotingEntry votingEntry;
  protected Peer directReceiver;

  public AppendNodeEntryHandler() {}

  @Override
  public void onComplete(AppendEntryResult response) {
    Peer trueReceiver =
        response.isSetReceiver()
            ? new Peer(
                ConsensusGroupId.Factory.createFromTConsensusGroupId(response.groupId),
                response.receiverId,
                response.receiver)
            : directReceiver;

    logger.debug(
        "{}: Append response {} from {} for log {}",
        member.getName(),
        response,
        trueReceiver,
        votingEntry);

    long resp = response.status;

    if (resp == RESPONSE_STRONG_ACCEPT || resp == RESPONSE_AGREE) {
      member.getVotingLogList().onStronglyAccept(votingEntry, trueReceiver);
      member.getStatus().getPeerMap().get(trueReceiver).setMatchIndex(response.lastLogIndex);
      synchronized (votingEntry.getEntry()) {
        votingEntry.getEntry().notifyAll();
      }
    } else if (resp > 0) {
      // a response > 0 is the follower's term
      // the leadership is stale, wait for the new leader's heartbeat
      logger.debug(
          "{}: Received a rejection from {} because term is stale: {}, log: {}",
          member.getName(),
          trueReceiver,
          resp,
          votingEntry);
      member.stepDown(resp, null);
      synchronized (votingEntry.getEntry()) {
        votingEntry.getEntry().notifyAll();
      }
    } else if (resp == RESPONSE_WEAK_ACCEPT) {
      votingEntry.getEntry().acceptedTime = System.nanoTime();
      Statistic.RAFT_SENDER_LOG_FROM_CREATE_TO_ACCEPT.add(
          votingEntry.getEntry().acceptedTime - votingEntry.getEntry().createTime);
      synchronized (votingEntry.getEntry()) {
        votingEntry.addWeaklyAcceptedNodes(trueReceiver);
        votingEntry.getEntry().notifyAll();
      }
    } else {
      // e.g., Response.RESPONSE_LOG_MISMATCH
      if (resp == RESPONSE_LOG_MISMATCH || resp == RESPONSE_OUT_OF_WINDOW) {
        logger.debug(
            "{}: The log {} is rejected by {} because: {}",
            member.getName(),
            votingEntry,
            trueReceiver,
            resp);
      } else {
        logger.warn(
            "{}: The log {} is rejected by {} because: {}",
            member.getName(),
            votingEntry,
            trueReceiver,
            resp);
      }
    }
    // rejected because the receiver's logs are stale or the receiver has no cluster info, just
    // wait for the heartbeat to handle
  }

  @Override
  public void onError(Exception exception) {
    if (exception instanceof TApplicationException) {
      if (exception.getMessage().contains("No such member")) {
        logger.debug(exception.getMessage());
      }
      return;
    }
    if (exception instanceof ConnectException) {
      logger.debug(
          "{}: Cannot append log {}: cannot connect to {}: {}",
          member.getName(),
          votingEntry,
          directReceiver,
          exception.getMessage());
    } else {
      logger.warn(
          "{}: Cannot append log {} to {}",
          member.getName(),
          votingEntry,
          directReceiver,
          exception);
    }
  }

  public void setVotingEntry(VotingEntry votingEntry) {
    this.votingEntry = votingEntry;
  }

  public void setMember(RaftMember member) {
    this.member = member;
  }

  public void setDirectReceiver(Peer follower) {
    this.directReceiver = follower;
  }
}
