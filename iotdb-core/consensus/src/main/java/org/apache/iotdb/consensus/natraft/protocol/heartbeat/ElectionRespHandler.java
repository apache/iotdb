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

import org.apache.iotdb.consensus.common.Peer;
import org.apache.iotdb.consensus.natraft.protocol.RaftMember;

import org.apache.thrift.TApplicationException;
import org.apache.thrift.async.AsyncMethodCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.ConnectException;

import static org.apache.iotdb.consensus.natraft.utils.Response.RESPONSE_AGREE;
import static org.apache.iotdb.consensus.natraft.utils.Response.RESPONSE_LEADER_STILL_ONLINE;
import static org.apache.iotdb.consensus.natraft.utils.Response.RESPONSE_NODE_IS_NOT_IN_GROUP;

/**
 * ElectionHandler checks the result from a voter and decides whether the election goes on, succeeds
 * or fails.
 */
public class ElectionRespHandler implements AsyncMethodCallback<Long> {

  private static final Logger logger = LoggerFactory.getLogger(ElectionRespHandler.class);

  private RaftMember raftMember;
  private String memberName;
  private Peer voter;
  private long currTerm;
  private ElectionState electionState;

  public ElectionRespHandler(
      RaftMember raftMember, Peer voter, long currTerm, ElectionState electionState) {
    this.raftMember = raftMember;
    this.voter = voter;
    this.currTerm = currTerm;
    this.electionState = electionState;
    this.memberName = raftMember.getName();
  }

  @Override
  public void onComplete(Long resp) {
    long voterResp = resp;

    if (electionState.isAccepted() || electionState.isRejected()) {
      return;
    }

    if (voterResp == RESPONSE_AGREE) {
      electionState.onAccept(voter);
      logger.info("{}: Received a grant vote from {}", memberName, voter);
      if (electionState.isAccepted()) {
        // the election is valid
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
        raftMember.stepDown(voterResp, null);
        // the election is rejected
        electionState.setRejected(true);
      }
    }
  }

  @Override
  public void onError(Exception exception) {
    if (exception instanceof ConnectException) {
      logger.debug("{}: Cannot connect to {}: {}", memberName, voter, exception.getMessage());
    } else if (exception instanceof TApplicationException
        && exception.getMessage().contains("No such member")) {
      logger.debug("{}: voter {} not ready: {}", memberName, voter, exception.getMessage());
    } else {
      logger.warn("{}: A voter {} encountered an error:", memberName, voter, exception);
    }
    onFail();
  }

  private void onFail() {
    electionState.onReject(voter);
  }
}
