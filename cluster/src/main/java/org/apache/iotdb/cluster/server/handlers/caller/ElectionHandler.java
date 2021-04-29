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

import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.cluster.server.member.RaftMember;

import org.apache.thrift.async.AsyncMethodCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.ConnectException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.iotdb.cluster.server.Response.RESPONSE_AGREE;
import static org.apache.iotdb.cluster.server.Response.RESPONSE_LEADER_STILL_ONLINE;
import static org.apache.iotdb.cluster.server.Response.RESPONSE_NODE_IS_NOT_IN_GROUP;

/**
 * ElectionHandler checks the result from a voter and decides whether the election goes on, succeeds
 * or fails.
 */
public class ElectionHandler implements AsyncMethodCallback<Long> {

  private static final Logger logger = LoggerFactory.getLogger(ElectionHandler.class);

  private RaftMember raftMember;
  private String memberName;
  private Node voter;
  private long currTerm;
  private AtomicInteger requiredVoteNum;
  private AtomicBoolean terminated;
  // when set to true, the elector wins the election
  private AtomicBoolean electionValid;
  private AtomicInteger failingVoteCounter;

  public ElectionHandler(
      RaftMember raftMember,
      Node voter,
      long currTerm,
      AtomicInteger requiredVoteNum,
      AtomicBoolean terminated,
      AtomicBoolean electionValid,
      AtomicInteger failingVoteCounter) {
    this.raftMember = raftMember;
    this.voter = voter;
    this.currTerm = currTerm;
    this.requiredVoteNum = requiredVoteNum;
    this.terminated = terminated;
    this.electionValid = electionValid;
    this.memberName = raftMember.getName();
    this.failingVoteCounter = failingVoteCounter;
  }

  @Override
  public void onComplete(Long resp) {
    long voterResp = resp;
    synchronized (raftMember.getTerm()) {
      if (terminated.get()) {
        // a voter has rejected this election, which means the term or the log id falls behind
        // this node is not able to be the leader
        logger.info(
            "{}: Terminated election received a election response {} from {}",
            memberName,
            voterResp,
            voter);
        return;
      }

      if (voterResp == RESPONSE_AGREE) {
        long remaining = requiredVoteNum.decrementAndGet();
        logger.info(
            "{}: Received a grant vote from {}, remaining votes to succeed: {}",
            memberName,
            voter,
            remaining);
        if (remaining == 0) {
          // the election is valid
          electionValid.set(true);
          terminated.set(true);
          raftMember.getTerm().notifyAll();
          raftMember.onElectionWins();
          logger.info("{}: Election {} is won", memberName, currTerm);
        }
        // still need more votes
      } else if (voterResp != RESPONSE_LEADER_STILL_ONLINE) {
        if (voterResp < currTerm) {
          // the rejection from a node with a smaller term means the log of this node falls behind
          logger.info("{}: Election {} rejected: code {}", memberName, currTerm, voterResp);
          onFail();
        } else if (voterResp == RESPONSE_NODE_IS_NOT_IN_GROUP) {
          logger.info("{}: This node has removed from the group", memberName);
          onFail();
        } else {
          // the election is rejected by a node with a bigger term, update current term to it
          logger.info(
              "{}: Election {} rejected from {}: The term of this node is no bigger than {}",
              memberName,
              currTerm,
              voter,
              voterResp);
          raftMember.stepDown(voterResp, false);
          // the election is rejected
          terminated.set(true);
          raftMember.getTerm().notifyAll();
        }
      }
    }
  }

  @Override
  public void onError(Exception exception) {
    if (exception instanceof ConnectException) {
      logger.warn("{}: Cannot connect to {}: {}", memberName, voter, exception.getMessage());
    } else {
      logger.warn("{}: A voter {} encountered an error:", memberName, voter, exception);
    }
    onFail();
  }

  private void onFail() {
    int failingVoteRemaining = failingVoteCounter.decrementAndGet();
    if (failingVoteRemaining <= 0) {
      synchronized (raftMember.getTerm()) {
        // wake up heartbeat thread to start the next election
        raftMember.getTerm().notifyAll();
      }
    }
  }
}
