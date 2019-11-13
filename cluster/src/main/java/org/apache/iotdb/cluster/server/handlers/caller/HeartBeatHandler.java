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
import org.apache.iotdb.cluster.rpc.thrift.RaftService.AsyncClient.sendHeartBeat_call;
import org.apache.iotdb.cluster.server.NodeCharacter;
import org.apache.iotdb.cluster.server.RaftServer;
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

  private RaftServer raftServer;
  private Node receiver;

  public HeartBeatHandler(RaftServer raftServer, Node node) {
    this.raftServer = raftServer;
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
    if (followerTerm == RaftServer.RESPONSE_AGREE) {
      // register the id of the node
      if (response.isSetFolloweIdentifier()) {
        raftServer.registerNodeIdentifier(receiver, response.getFolloweIdentifier());
      }
      // record the requirement of node list of the follower
      if (response.isRequireNodeList()) {
        raftServer.addBlindNode(receiver);
      }

      // current leadership is still valid
      Node follower = response.getFollower();
      long lastLogIdx = response.getLastLogIndex();
      long lastLogTerm = response.getLastLogTerm();
      long localLastLogIdx = raftServer.getLogManager().getLastLogIndex();
      long localLastLogTerm = raftServer.getLogManager().getLastLogTerm();
      logger.debug("Node {} is still alive, log index: {}/{}, log term: {}/{}", follower, lastLogIdx
          ,localLastLogIdx, lastLogTerm, localLastLogTerm);

      if (localLastLogIdx > lastLogIdx ||
          lastLogIdx == localLastLogIdx && localLastLogTerm > lastLogTerm) {
        raftServer.catchUp(follower, lastLogIdx);
      }
    } else {
      // current leadership is invalid because the follower has a larger term
      synchronized (raftServer.getTerm()) {
        long currTerm = raftServer.getTerm().get();
        if (currTerm < followerTerm) {
          logger.info("Losing leadership because current term {} is smaller than {}", currTerm,
              followerTerm);
          raftServer.getTerm().set(followerTerm);
          raftServer.setCharacter(NodeCharacter.FOLLOWER);
          raftServer.setLeader(null);
          raftServer.setLastHeartBeatReceivedTime(System.currentTimeMillis());
        }
      }
    }
  }

  @Override
  public void onError(Exception exception) {
    logger.error("Heart beat error, receiver {}, {}", receiver, exception.getMessage());
  }
}
