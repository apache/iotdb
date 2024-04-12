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

package org.apache.iotdb.consensus.natraft.protocol.heartbeat;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.commons.consensus.ConsensusGroupId.Factory;
import org.apache.iotdb.consensus.common.Peer;
import org.apache.iotdb.consensus.natraft.protocol.RaftMember;
import org.apache.iotdb.consensus.natraft.protocol.RaftRole;
import org.apache.iotdb.consensus.natraft.utils.Response;
import org.apache.iotdb.consensus.raft.thrift.ElectionRequest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

public class ElectionReqHandler {

  private static final Logger logger = LoggerFactory.getLogger(ElectionReqHandler.class);
  private RaftMember member;

  public ElectionReqHandler(RaftMember member) {
    this.member = member;
  }

  public long processElectionRequest(ElectionRequest electionRequest) {
    TEndPoint candidate = electionRequest.getElector();
    Peer peer =
        new Peer(
            Factory.createFromTConsensusGroupId(electionRequest.groupId),
            electionRequest.electorId,
            candidate);
    // check if the node is in the group
    if (!member.containsNode(peer)) {
      logger.info(
          "{}: the elector {} is not in the data group {}, so reject this election.",
          member.getName(),
          peer,
          member.getAllNodes());
      return Response.RESPONSE_NODE_IS_NOT_IN_GROUP;
    }

    if (logger.isDebugEnabled()) {
      logger.debug("{}: start to handle request from elector {}", member.getName(), peer);
    }

    long currentTerm = member.getStatus().getTerm().get();
    long response =
        checkElectorTerm(currentTerm, electionRequest.getTerm(), electionRequest.getElector());
    if (response != Response.RESPONSE_AGREE) {
      return response;
    }

    // compare the log progress of the elector with this node
    response = checkElectorLogProgress(electionRequest, peer);
    logger.info(
        "{} sending response {} to the elector {}",
        member.getName(),
        response,
        electionRequest.getElector());
    return response;
  }

  private long checkElectorTerm(long currentTerm, long electorTerm, TEndPoint candidate) {
    if (electorTerm < currentTerm) {
      // the elector has a smaller term thus the request is invalid
      logger.info(
          "{} sending localTerm {} to the elector {} because it's term {} is smaller.",
          member.getName(),
          currentTerm,
          candidate,
          electorTerm);
      return currentTerm;
    }
    if (currentTerm == electorTerm
        && member.getStatus().getVoteFor() != null
        && !Objects.equals(member.getStatus().getVoteFor(), candidate)) {
      // this node has voted in this round, but not for the elector, as one node cannot vote
      // twice, reject the request
      logger.info(
          "{} sending rejection to the elector {} because member already has voted {} in this term {}.",
          member.getName(),
          candidate,
          member.getStatus().getVoteFor(),
          currentTerm);
      return Response.RESPONSE_REJECT;
    }
    if (electorTerm > currentTerm) {
      // the elector has a larger term, this node should update its term first
      logger.info(
          "{} received an election from elector {} which has bigger term {} than localTerm {}, raftMember should step down first and then continue to decide whether to grant it's vote by log status.",
          member.getName(),
          candidate,
          electorTerm,
          currentTerm);
      member.stepDown(electorTerm, null);
    }
    return Response.RESPONSE_AGREE;
  }

  /**
   * Verify the validity of an ElectionRequest, and make itself a follower of the elector if the
   * request is valid.
   *
   * @return Response.RESPONSE_AGREE if the elector is valid or the local term if the elector has a
   *     smaller term or Response.RESPONSE_LOG_MISMATCH if the elector has older logs.
   */
  long checkElectorLogProgress(ElectionRequest electionRequest, Peer peer) {

    long thatTerm = electionRequest.getTerm();
    long thatLastLogIndex = electionRequest.getLastLogIndex();
    long thatLastLogTerm = electionRequest.getLastLogTerm();

    // check the log progress of the elector
    long resp = checkLogProgress(thatLastLogIndex, thatLastLogTerm);
    if (resp == Response.RESPONSE_AGREE) {
      logger.info(
          "{} accepted an election request, term:{}/{}, logIndex:{}/{}, logTerm:{}/{}",
          member.getName(),
          thatTerm,
          member.getStatus().getTerm().get(),
          thatLastLogIndex,
          member.getLogManager().getLastLogIndex(),
          thatLastLogTerm,
          member.getLogManager().getLastLogTerm());
      member.getStatus().setRole(RaftRole.FOLLOWER);
      member.getHeartbeatThread().setLastHeartbeatReceivedTime(System.currentTimeMillis());
      member.getStatus().setVoteFor(peer);
      member.updateHardState(thatTerm, member.getStatus().getVoteFor());
    } else {
      logger.info(
          "{} rejected an election request, term:{}/{}, logIndex:{}/{}, logTerm:{}/{}",
          member.getName(),
          thatTerm,
          member.getStatus().getTerm().get(),
          thatLastLogIndex,
          member.getLogManager().getLastLogIndex(),
          thatLastLogTerm,
          member.getLogManager().getLastLogTerm());
    }
    return resp;
  }

  /**
   * Reject the election if the lastLogTerm of the candidate equals to the voter's but its
   * lastLogIndex is smaller than the voter's Otherwise accept the election.
   *
   * @return Response.RESPONSE_AGREE if the elector is valid or the local term if the elector has a
   *     smaller term or Response.RESPONSE_LOG_MISMATCH if the elector has older logs.
   */
  long checkLogProgress(long lastLogIndex, long lastLogTerm) {
    long response;
    synchronized (member.getLogManager()) {
      if (member.getLogManager().isLogUpToDate(lastLogTerm, lastLogIndex)) {
        response = Response.RESPONSE_AGREE;
      } else {
        response = Response.RESPONSE_LOG_MISMATCH;
      }
    }
    return response;
  }
}
