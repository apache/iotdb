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

import org.apache.iotdb.commons.consensus.ConsensusGroupId;
import org.apache.iotdb.consensus.common.Peer;
import org.apache.iotdb.consensus.natraft.protocol.PeerInfo;
import org.apache.iotdb.consensus.natraft.protocol.RaftMember;
import org.apache.iotdb.consensus.raft.thrift.HeartBeatResponse;

import org.apache.thrift.TApplicationException;
import org.apache.thrift.async.AsyncMethodCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.ConnectException;

import static org.apache.iotdb.consensus.natraft.utils.Response.RESPONSE_AGREE;

/**
 * HeartbeatHandler checks the response of a heartbeat and decides whether to start a catch-up or
 * give up the leadership due to the term is stale.
 */
public class HeartbeatRespHandler implements AsyncMethodCallback<HeartBeatResponse> {

  private static final Logger logger = LoggerFactory.getLogger(HeartbeatRespHandler.class);

  private RaftMember localMember;
  private String memberName;
  private Peer receiver;

  public HeartbeatRespHandler(RaftMember localMember, Peer receiver) {
    this.localMember = localMember;
    this.receiver = receiver;
    this.memberName = localMember.getName();
  }

  @Override
  public void onComplete(HeartBeatResponse resp) {
    long followerTerm = resp.getTerm();
    logger.debug(
        "{}: Received a heartbeat response {} for last log index {} from {}",
        memberName,
        followerTerm,
        resp.getLastLogIndex(),
        resp.follower);
    if (followerTerm == RESPONSE_AGREE) {
      // current leadership is still valid
      handleNormalHeartbeatResponse(resp);
    } else {
      // current leadership is invalid because the follower has a larger term
      long currTerm = localMember.getStatus().getTerm().get();
      if (currTerm < followerTerm) {
        logger.info(
            "{}: Losing leadership because current term {} is smaller than {}",
            memberName,
            currTerm,
            followerTerm);
        localMember.stepDown(followerTerm, null);
      }
    }
  }

  private void handleNormalHeartbeatResponse(HeartBeatResponse resp) {

    // check the necessity of performing a catch-up
    Peer peer =
        new Peer(
            ConsensusGroupId.Factory.createFromTConsensusGroupId(resp.groupId),
            resp.followerId,
            resp.follower);
    long lastLogIdx = resp.getLastLogIndex();
    long lastLogTerm = resp.getLastLogTerm();
    long localLastLogIdx = localMember.getLogManager().getLastLogIndex();
    long localLastLogTerm = localMember.getLogManager().getLastLogTerm();
    if (logger.isTraceEnabled()) {
      logger.trace(
          "{}: Node {} is still alive, log index: {}/{}, log term: {}/{}",
          memberName,
          peer,
          lastLogIdx,
          localLastLogIdx,
          lastLogTerm,
          localLastLogTerm);
    }

    PeerInfo peerInfo = localMember.getStatus().getPeerMap().get(peer);
    if (!localMember.getLogManager().isLogUpToDate(lastLogTerm, lastLogIdx)
        || (lastLogIdx == localLastLogIdx
            && !localMember.getLogManager().matchTerm(lastLogTerm, lastLogIdx))) {
      // the follower is not up-to-date
      if (lastLogIdx == -1 || lastLogIdx < peerInfo.getMatchIndex()) {
        // maybe the follower has restarted, so we need to find its match index again, because
        // some logs may be lost due to restart
        peerInfo.setMatchIndex(lastLogIdx);
      }

      // only start a catch-up when the follower's lastLogIndex remains stall and unchanged for 5
      // heartbeats. If the follower is installing snapshot currently, we reset the counter.
      if (lastLogIdx == peerInfo.getLastHeartBeatIndex() && !resp.isInstallingSnapshot()) {
        // the follower's lastLogIndex is unchanged, increase inconsistent counter
        int inconsistentNum = peerInfo.incInconsistentHeartbeatNum();
        if (inconsistentNum >= 10) {
          logger.info(
              "{}: catching up node {}, index-term: {}-{}/{}-{}, peer match index {}",
              memberName,
              peer,
              lastLogIdx,
              lastLogTerm,
              localLastLogIdx,
              localLastLogTerm,
              peerInfo.getMatchIndex());
          localMember.catchUp(peer, lastLogIdx);
        }
      } else {
        // the follower's lastLogIndex is changed, which means the follower is not down yet, we
        // reset the counter to see if it can eventually catch up by itself
        peerInfo.resetInconsistentHeartbeatNum();
      }
    } else {
      // the follower is up-to-date
      peerInfo.setMatchIndex(Math.max(peerInfo.getMatchIndex(), lastLogIdx));
      peerInfo.resetInconsistentHeartbeatNum();
    }
    peerInfo.setLastHeartBeatIndex(lastLogIdx);
  }

  @Override
  public void onError(Exception exception) {
    if (exception instanceof ConnectException) {
      logger.debug("{}: Cannot connect to {}: {}", memberName, receiver, exception.getMessage());
    } else if (exception instanceof TApplicationException
        && exception.getMessage().contains("No such member")) {
      logger.debug("{}: node {} not ready: {}", memberName, receiver, exception.getMessage());
    } else {
      logger.error(
          "{}: Heart beat error, receiver {}, {}", memberName, receiver, exception.getMessage());
    }
  }
}
