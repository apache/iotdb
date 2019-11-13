/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at      http://www.apache.org/licenses/LICENSE-2.0  Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the License for the specific language governing permissions and limitations under the License.
 */

package org.apache.iotdb.cluster.server.heartbeat;

import static org.apache.iotdb.cluster.server.RaftServer.CONNECTION_TIME_OUT_MS;

import java.util.Collection;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
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
  private static final long HEART_BEAT_INTERVAL_MS = 1000L;
  // a failed election will restart in 5s~10s
  private static final long ELECTION_LEAST_TIME_OUT_MS = 5 * 1000L;
  private static final long ELECTION_RANDOM_TIME_OUT_MS = 5 * 1000L;

  private RaftMember raftMember;
  HeartBeatRequest request = new HeartBeatRequest();
  private ElectionRequest electionRequest = new ElectionRequest();

  private Random random = new Random();

  public HeartBeatThread() {
  }

  public HeartBeatThread(RaftMember raftMember) {
    this.raftMember = raftMember;
  }

  @Override
  public void run() {
    logger.info("Heartbeat thread starts...");
    while (!Thread.interrupted()) {
      try {
        switch (raftMember.getCharacter()) {
          case LEADER:
            // send heartbeats to the followers
            logger.debug("Send heartbeat to the followers");
            sendHeartBeats();
            Thread.sleep(HEART_BEAT_INTERVAL_MS);
            break;
          case FOLLOWER:
            // check if heartbeat times out
            long heartBeatInterval = System.currentTimeMillis() - raftMember
                .getLastHeartBeatReceivedTime();
            if (heartBeatInterval >= CONNECTION_TIME_OUT_MS) {
              // the leader is considered dead, an election will be started in the next loop
              logger.debug("The leader {} timed out", raftMember.getLeader());
              raftMember.setCharacter(NodeCharacter.ELECTOR);
              raftMember.setLeader(null);
            } else {
              logger.debug("Heartbeat is still valid");
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
        logger.error("Unexpected heartbeat exception:", e);
      }
    }

    logger.info("Heart beat thread exits");
  }

  private void sendHeartBeats() {
    synchronized (raftMember.getTerm()) {
      request.setTerm(raftMember.getTerm().get());
      request.setCommitLogIndex(raftMember.getLogManager().getLastLogTerm());
      request.setLeader(raftMember.getThisNode());

      sendHeartBeats(raftMember.getAllNodes());
    }
  }

  private void sendHeartBeats(Collection<Node> nodes) {
    for (Node node : nodes) {
      if (raftMember.getCharacter() != NodeCharacter.LEADER) {
        // if the character changes, abort the remaining heart beats
        return;
      }

      AsyncClient client = raftMember.connectNode(node);
      if (client == null) {
        return;
      }
      sendHeartbeat(node, client);
    }
  }

  void sendHeartbeat(Node node, AsyncClient client) {

    try {
      client.sendHeartBeat(request, new HeartBeatHandler(raftMember, node));
    } catch (Exception e) {
      logger.warn("Cannot send heart beat to node {}", node, e);
    }
  }

  // start elections until this node becomes a leader or a follower
  private void startElections() {

    // the election goes on until this node becomes a follower or a leader
    while (raftMember.getCharacter() == NodeCharacter.ELECTOR) {
      startElection();
      long electionWait = ELECTION_LEAST_TIME_OUT_MS + Math.abs(random.nextLong() % ELECTION_RANDOM_TIME_OUT_MS);
      try {
        logger.info("Sleep {}ms until next election", electionWait);
        Thread.sleep(electionWait);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        logger.error("Election is unexpectedly interrupted:", e);
      }
    }
    raftMember.setLastHeartBeatReceivedTime(System.currentTimeMillis());
  }

  // start one round of election
  private void startElection() {
    synchronized (raftMember.getTerm()) {
      long nextTerm = raftMember.getTerm().incrementAndGet();
      int quorumNum = raftMember.getAllNodes().size() / 2;
      logger.info("Election {} starts, quorum: {}", nextTerm, quorumNum);
      AtomicBoolean electionTerminated = new AtomicBoolean(false);
      AtomicBoolean electionValid = new AtomicBoolean(false);
      AtomicInteger quorum = new AtomicInteger(quorumNum);

      electionRequest.setTerm(nextTerm);
      electionRequest.setLastLogTerm(raftMember.getLogManager().getLastLogTerm());
      electionRequest.setLastLogIndex(raftMember.getLogManager().getCommitLogIndex());

      requestVote(raftMember.getAllNodes(), electionRequest, nextTerm, quorum,
          electionTerminated, electionValid);

      try {
        logger.info("Wait for {}ms until election time out", CONNECTION_TIME_OUT_MS);
        raftMember.getTerm().wait(CONNECTION_TIME_OUT_MS);
      } catch (InterruptedException e) {
        logger.info("Election {} times out", nextTerm);
        Thread.currentThread().interrupt();
      }

      electionTerminated.set(true);
      if (electionValid.get()) {
        logger.info("Election {} accepted", nextTerm);
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
        logger.info("Requesting a vote from {}", node);
        ElectionHandler handler = new ElectionHandler(raftMember, node, nextTerm, quorum,
            electionTerminated, electionValid);
        try {
          client.startElection(request, handler);
        } catch (Exception e) {
          logger.error("Cannot request a vote from {}", node, e);
        }
      }
    }
  }
}
