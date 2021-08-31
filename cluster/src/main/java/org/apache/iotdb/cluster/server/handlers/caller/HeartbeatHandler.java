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

import org.apache.iotdb.cluster.rpc.thrift.HeartBeatResponse;
import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.cluster.server.member.RaftMember;
import org.apache.iotdb.cluster.server.monitor.Peer;

import org.apache.thrift.async.AsyncMethodCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.ConnectException;

import static org.apache.iotdb.cluster.server.Response.RESPONSE_AGREE;

/**
 * HeartbeatHandler checks the response of a heartbeat and decides whether to start a catch-up or
 * give up the leadership due to the term is stale.
 */
public class HeartbeatHandler implements AsyncMethodCallback<HeartBeatResponse> {

  private static final Logger logger = LoggerFactory.getLogger(HeartbeatHandler.class);

  private RaftMember localMember;
  private String memberName;
  private Node receiver;

  public HeartbeatHandler(RaftMember localMember, Node receiver) {
    this.localMember = localMember;
    this.receiver = receiver;
    this.memberName = localMember.getName();
  }

  @Override
  public void onComplete(HeartBeatResponse resp) {
    long followerTerm = resp.getTerm();
    if (logger.isDebugEnabled()) {
      logger.debug("{}: Received a heartbeat response {}", memberName, followerTerm);
    }
    if (followerTerm == RESPONSE_AGREE) {
      // current leadership is still valid
      handleNormalHeartbeatResponse(resp);
    } else {
      // current leadership is invalid because the follower has a larger term
      synchronized (localMember.getTerm()) {
        long currTerm = localMember.getTerm().get();
        if (currTerm < followerTerm) {
          logger.info(
              "{}: Losing leadership because current term {} is smaller than {}",
              memberName,
              currTerm,
              followerTerm);
          localMember.stepDown(followerTerm, false);
        }
      }
    }
  }

  private void handleNormalHeartbeatResponse(HeartBeatResponse resp) {
    // additional process depending on member type
    localMember.processValidHeartbeatResp(resp, receiver);

    // check the necessity of performing a catch up
    Node follower = resp.getFollower();
    long lastLogIdx = resp.getLastLogIndex();
    long lastLogTerm = resp.getLastLogTerm();
    long localLastLogIdx = localMember.getLogManager().getLastLogIndex();
    long localLastLogTerm = localMember.getLogManager().getLastLogTerm();
    if (logger.isDebugEnabled()) {
      logger.debug(
          "{}: Node {} is still alive, log index: {}/{}, log term: {}/{}",
          memberName,
          follower,
          lastLogIdx,
          localLastLogIdx,
          lastLogTerm,
          localLastLogTerm);
    }

    Peer peer =
        localMember
            .getPeerMap()
            .computeIfAbsent(
                follower, k -> new Peer(localMember.getLogManager().getLastLogIndex()));
    if (!localMember.getLogManager().isLogUpToDate(lastLogTerm, lastLogIdx)
        || !localMember.getLogManager().matchTerm(lastLogTerm, lastLogIdx)) {
      // the follower is not up-to-date
      if (lastLogIdx == -1) {
        // maybe the follower has restarted, so we need to find its match index again, because
        // some logs may be lost due to restart
        peer.setMatchIndex(-1);
      }

      // only start a catch up when the follower's lastLogIndex remains stall and unchanged for 3
      // heartbeats
      if (lastLogIdx == peer.getLastHeartBeatIndex()) {
        // the follower's lastLogIndex is unchanged, increase inconsistent counter
        int inconsistentNum = peer.incInconsistentHeartbeatNum();
        if (inconsistentNum >= 5) {
          logger.info(
              "{}: catching up node {}, index-term: {}-{}/{}-{}, peer match index {}",
              memberName,
              follower,
              lastLogIdx,
              lastLogTerm,
              localLastLogIdx,
              localLastLogTerm,
              peer.getMatchIndex());
          localMember.catchUp(follower, lastLogIdx);
        }
      } else {
        // the follower's lastLogIndex is changed, which means the follower is not down yet, we
        // reset the counter to see if it can eventually catch up by itself
        peer.resetInconsistentHeartbeatNum();
      }
    } else {
      // the follower is up-to-date
      peer.setMatchIndex(Math.max(peer.getMatchIndex(), lastLogIdx));
      peer.resetInconsistentHeartbeatNum();
    }
    peer.setLastHeartBeatIndex(lastLogIdx);
  }

  @Override
  public void onError(Exception exception) {
    if (exception instanceof ConnectException) {
      logger.warn("{}: Cannot connect to {}: {}", memberName, receiver, exception.getMessage());
    } else {
      logger.error(
          "{}: Heart beat error, receiver {}, {}", memberName, receiver, exception.getMessage());
    }
  }
}
