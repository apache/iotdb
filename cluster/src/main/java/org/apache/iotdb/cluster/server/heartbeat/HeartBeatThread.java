/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at      http://www.apache.org/licenses/LICENSE-2.0  Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the License for the specific language governing permissions and limitations under the License.
 */

package org.apache.iotdb.cluster.server.heartbeat;

import static org.apache.iotdb.cluster.server.RaftServer.CONNECTION_TIME_OUT_MS;

import java.util.Collection;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.iotdb.cluster.config.ClusterConstant;
import org.apache.iotdb.cluster.rpc.thrift.ElectionRequest;
import org.apache.iotdb.cluster.rpc.thrift.HeartBeatRequest;
import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.cluster.rpc.thrift.RaftService.AsyncClient;
import org.apache.iotdb.cluster.server.NodeCharacter;
import org.apache.iotdb.cluster.server.member.RaftMember;
import org.apache.iotdb.cluster.server.handlers.caller.ElectionHandler;
import org.apache.iotdb.cluster.server.handlers.caller.HeartBeatHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * HeartBeatThread takes the responsibility to send heartbeats (when this node is a leader),
 * check if the leader is still online (when this node is a follower) or start elections (when
 * this node is a elector).
 */
public class HeartBeatThread implements Runnable {

  private static final Logger logger = LoggerFactory.getLogger(HeartBeatThread.class);

  private RaftMember raftMember;
  private String memberName;
  HeartBeatRequest request = new HeartBeatRequest();
  ElectionRequest electionRequest = new ElectionRequest();

  private Random random = new Random();

  HeartBeatThread(RaftMember raftMember) {
    this.raftMember = raftMember;
    memberName = raftMember.getName();
  }

  @Override
  public void run() {
    logger.info("Heartbeat thread starts...");
    while (!Thread.interrupted()) {
      try {
        switch (raftMember.getCharacter()) {
          case LEADER:
            // send heartbeats to the followers
            sendHeartBeats();
            Thread.sleep(ClusterConstant.HEART_BEAT_INTERVAL_MS);
            break;
          case FOLLOWER:
            // check if heartbeat times out
            long heartBeatInterval = System.currentTimeMillis() - raftMember
                .getLastHeartBeatReceivedTime();
            if (heartBeatInterval >= CONNECTION_TIME_OUT_MS) {
              // the leader is considered dead, an election will be started in the next loop
              logger.debug("{}: The leader {} timed out", memberName, raftMember.getLeader());
              raftMember.setCharacter(NodeCharacter.ELECTOR);
              raftMember.setLeader(null);
            } else {
              logger.debug("{}: Heartbeat is still valid", memberName);
              Thread.sleep(CONNECTION_TIME_OUT_MS);
            }
            break;
          case ELECTOR:
          default:
            logger.info("Start elections");
            startElections();
            break;
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        break;
      } catch (Exception e) {
        logger.error("{}: Unexpected heartbeat exception:", memberName, e);
      }
    }

    logger.info("{}: Heartbeat thread exits", memberName);
  }

  private void sendHeartBeats() {
    synchronized (raftMember.getTerm()) {
      request.setTerm(raftMember.getTerm().get());
      request.setLeader(raftMember.getThisNode());
      request.setCommitLogIndex(raftMember.getLogManager().getCommitLogIndex());

      sendHeartBeats(raftMember.getAllNodes());
    }
  }

  private void sendHeartBeats(Collection<Node> nodes) {
    if (logger.isDebugEnabled()) {
      logger.debug("{}: Send heartbeat to {} followers", memberName, nodes.size());
    }
    for (Node node : nodes) {
      if (raftMember.getCharacter() != NodeCharacter.LEADER) {
        // if the character changes, abort the remaining heart beats
        return;
      }

      AsyncClient client = raftMember.connectNode(node);
      if (client != null) {
        sendHeartbeat(node, client);
      }
    }
  }

  void sendHeartbeat(Node node, AsyncClient client) {
    try {
      logger.debug("{}: Sending heartbeat to {}", memberName, node);
      client.sendHeartBeat(request, new HeartBeatHandler(raftMember, node));
    } catch (Exception e) {
      logger.warn("{}: Cannot send heart beat to node {}", memberName, node, e);
    }
  }

  // start elections until this node becomes a leader or a follower
  private void startElections() throws InterruptedException {
    if (raftMember.getAllNodes().size() == 1) {
      // single node cluster, this node is always the leader
      raftMember.setCharacter(NodeCharacter.LEADER);
      raftMember.setLeader(raftMember.getThisNode());
      logger.info("{}: Winning the election because the node is the only node.", memberName);
    }

    // the election goes on until this node becomes a follower or a leader
    while (raftMember.getCharacter() == NodeCharacter.ELECTOR) {
      startElection();
      long electionWait = ClusterConstant.ELECTION_LEAST_TIME_OUT_MS
          + Math.abs(random.nextLong() % ClusterConstant.ELECTION_RANDOM_TIME_OUT_MS);
      logger.info("{}: Sleep {}ms until next election", memberName, electionWait);
      Thread.sleep(electionWait);
    }
    raftMember.setLastHeartBeatReceivedTime(System.currentTimeMillis());
  }

  // start one round of election
  void startElection() {
    synchronized (raftMember.getTerm()) {
      long nextTerm = raftMember.getTerm().incrementAndGet();
      int quorumNum = raftMember.getAllNodes().size() / 2;
      logger.info("{}: Election {} starts, quorum: {}", memberName, nextTerm, quorumNum);
      AtomicBoolean electionTerminated = new AtomicBoolean(false);
      AtomicBoolean electionValid = new AtomicBoolean(false);
      AtomicInteger quorum = new AtomicInteger(quorumNum);

      electionRequest.setTerm(nextTerm);
      electionRequest.setElector(raftMember.getThisNode());

      requestVote(raftMember.getAllNodes(), electionRequest, nextTerm, quorum,
          electionTerminated, electionValid);

      try {
        logger.info("{}: Wait for {}ms until election time out", memberName,
            CONNECTION_TIME_OUT_MS);
        raftMember.getTerm().wait(CONNECTION_TIME_OUT_MS);
      } catch (InterruptedException e) {
        logger.info("{}: Election {} times out", memberName, nextTerm);
        Thread.currentThread().interrupt();
      }

      electionTerminated.set(true);
      if (electionValid.get()) {
        logger.info("{}: Election {} accepted", memberName, nextTerm);
        raftMember.setCharacter(NodeCharacter.LEADER);
        raftMember.setLeader(raftMember.getThisNode());
      }
    }
  }

  // request votes from given nodes
  private void requestVote(Collection<Node> nodes, ElectionRequest request, long nextTerm,
      AtomicInteger quorum, AtomicBoolean electionTerminated, AtomicBoolean electionValid) {
    for (Node node : nodes) {
      AsyncClient client = raftMember.connectNode(node);
      if (client != null) {
        logger.info("{}: Requesting a vote from {}", memberName, node);
        ElectionHandler handler = new ElectionHandler(raftMember, node, nextTerm, quorum,
            electionTerminated, electionValid);
        try {
          client.startElection(request, handler);
        } catch (Exception e) {
          logger.error("{}: Cannot request a vote from {}", memberName, node, e);
        }
      }
    }
  }
}
