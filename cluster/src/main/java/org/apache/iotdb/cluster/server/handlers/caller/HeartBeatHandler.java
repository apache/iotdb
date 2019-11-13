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

import static org.apache.iotdb.cluster.server.member.RaftMember.RESPONSE_AGREE;

import org.apache.iotdb.cluster.rpc.thrift.HeartBeatResponse;
import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.cluster.rpc.thrift.RaftService.AsyncClient.sendHeartBeat_call;
import org.apache.iotdb.cluster.server.NodeCharacter;
import org.apache.iotdb.cluster.server.member.RaftMember;
import org.apache.thrift.TException;
import org.apache.thrift.async.AsyncMethodCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * HeartBeatHandler check the response of a heartbeat and decide whether to start a catch-up or
 * give up the leadership due to the term is stale.
 */
public class HeartBeatHandler implements AsyncMethodCallback<sendHeartBeat_call> {

  private static final Logger logger = LoggerFactory.getLogger(HeartBeatHandler.class);

  private RaftMember raftMember;
  private Node receiver;

  public HeartBeatHandler(RaftMember raftMember, Node node) {
    this.raftMember = raftMember;
    this.receiver = node;
  }

  @Override
  public void onComplete(sendHeartBeat_call resp) {
    logger.debug("Received a heartbeat response");
    HeartBeatResponse response;
    try {
      response = resp.getResult();
    } catch (TException e) {
      onError(e);
      return;
    }

    long followerTerm = response.getTerm();
    if (followerTerm == RESPONSE_AGREE) {
      // current leadership is still valid
      raftMember.processValidHeartbeatResp(response, receiver);
    } else {
      // current leadership is invalid because the follower has a larger term
      synchronized (raftMember.getTerm()) {
        long currTerm = raftMember.getTerm().get();
        if (currTerm < followerTerm) {
          logger.info("Losing leadership because current term {} is smaller than {}", currTerm,
              followerTerm);
          raftMember.getTerm().set(followerTerm);
          raftMember.setCharacter(NodeCharacter.FOLLOWER);
          raftMember.setLeader(null);
          raftMember.setLastHeartBeatReceivedTime(System.currentTimeMillis());
        }
      }
    }
  }

  @Override
  public void onError(Exception exception) {
    logger.error("Heart beat error, receiver {}, {}", receiver, exception.getMessage());
  }
}
