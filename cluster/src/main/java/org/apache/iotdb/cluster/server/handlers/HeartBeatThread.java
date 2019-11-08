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
package org.apache.iotdb.cluster.server.handlers;

import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.iotdb.cluster.rpc.thrift.ElectionRequest;
import org.apache.iotdb.cluster.rpc.thrift.HeartBeatRequest;
import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.cluster.rpc.thrift.RaftService.AsyncClient;
import org.apache.iotdb.cluster.server.NodeCharacter;
import org.apache.iotdb.cluster.server.NodeStatus;
import org.apache.iotdb.cluster.server.RaftServer;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * HeartBeatThread takes the responsibility to send heartbeats (when this node is a leader), or
 * start elections (when this node is a follower)
 */
public class HeartBeatThread implements Runnable {

  private static final Logger logger = LoggerFactory.getLogger(HeartBeatThread.class);
  private static final long HEART_BEAT_INTERVAL_MS = 1000L;
  private static final long ELECTION_LEAST_TIME_OUT_MS = 5 * 1000L;
  private static final long ELECTION_RANDOM_TIME_OUT_MS = 5 * 1000L;

  private RaftServer raftServer;
  private HeartBeatRequest request = new HeartBeatRequest();

  private Random random = new Random();

  public HeartBeatThread(RaftServer raftServer) {
    this.raftServer = raftServer;
  }

  @Override
  public void run() {
    try {
      logger.info("Heartbeat thread starts...");
      while (!Thread.interrupted()) {
        switch (raftServer.getCharacter()) {
          case LEADER:
            // send heart beats to the followers
            sendHeartBeats();
            Thread.sleep(HEART_BEAT_INTERVAL_MS);
            break;
          case FOLLOWER:
            // check if heart beat times out
            long heartBeatInterval = System.currentTimeMillis() - raftServer
                .getLastHeartBeatReceivedTime();
            if (heartBeatInterval >= RaftServer.CONNECTION_TIME_OUT_MS) {
              // the leader is considered dead, an election will be started in the next loop
              raftServer.setCharacter(NodeCharacter.ELECTOR);
              raftServer.setLeader(null);
            } else {
              Thread.sleep(RaftServer.CONNECTION_TIME_OUT_MS);
            }
            break;
          case ELECTOR:
          default:
            startElections();
            break;
        }
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    } catch (Exception e) {
      logger.error("Unexpected heartbeat exception:", e);
    }
    logger.info("Heart beat thread exits");
  }

  private void sendHeartBeats() {
    synchronized (raftServer.getTerm()) {
      request.setTerm(raftServer.getTerm().get());
      request.setCommitLogIndex(raftServer.getLogManager().getLastLogTerm());
      request.setLeader(raftServer.getThisNode());

      sendHeartBeats(raftServer.getAllNodes());
    }
  }

  private void sendHeartBeats(Set<Node> nodes) {
    for (Node node : nodes) {
      if (raftServer.getCharacter() != NodeCharacter.LEADER) {
        // if the character changes, abort the remaining heart beats
        return;
      }

      AsyncClient client = raftServer.connectNode(node);
      if (client == null) {
        continue;
      }
      try {
        client.sendHeartBeat(request, new HeartBeatHandler(raftServer, node));
      } catch (TException e) {
        raftServer.disConnectNode(node);
        logger.warn("Cannot send heart beat to node {}", node, e);
      }
    }
  }

  // start elections until this node becomes a leader or a follower
  private void startElections() {

    // the election goes on until this node becomes a follower or a leader
    while (raftServer.getCharacter() == NodeCharacter.ELECTOR) {
      if (!raftServer.isLogFallsBehind()) {
        startElection();
      }
    }
    raftServer.setLastHeartBeatReceivedTime(System.currentTimeMillis());
  }

  // start one round of election
  private void startElection() {
    synchronized (raftServer.getTerm()) {
      long nextTerm = raftServer.getTerm().incrementAndGet();
      // different elections are handled by different handlers, which avoids previous votes
      // interfere the current election
      int quorumNum = raftServer.getAllNodes().size() / 2;
      logger.info("Election {} starts, quorum: {}", nextTerm, quorumNum);
      AtomicBoolean electionTerminated = new AtomicBoolean(false);
      AtomicBoolean electionValid = new AtomicBoolean(false);
      AtomicInteger quorum = new AtomicInteger(quorumNum);

      ElectionRequest electionRequest = new ElectionRequest();
      electionRequest.setTerm(nextTerm);
      electionRequest.setLastLogTerm(raftServer.getLogManager().getLastLogTerm());
      electionRequest.setLastLogIndex(raftServer.getLogManager().getCommitLogIndex());

      requestVote(raftServer.getAllNodes(), electionRequest, nextTerm, quorum, electionTerminated, electionValid);

      long timeOut = ELECTION_LEAST_TIME_OUT_MS + Math.abs(random.nextLong() % ELECTION_RANDOM_TIME_OUT_MS);
      try {
        logger.info("Wait for {}ms until next election", timeOut);
        raftServer.getTerm().wait(timeOut);
      } catch (InterruptedException e) {
        logger.info("Election {} times out", nextTerm);
        Thread.currentThread().interrupt();
      }

      electionTerminated.set(true);
      if (electionValid.get()) {
        logger.info("Election {} accepted", nextTerm);
        raftServer.setCharacter(NodeCharacter.LEADER);
        raftServer.setLeader(raftServer.getThisNode());
        raftServer.setNodeStatus(NodeStatus.JOINED);
      }
    }
  }

  // request a vote from given nodes
  private void requestVote(Set<Node> nodes, ElectionRequest request, long nextTerm,
      AtomicInteger quorum, AtomicBoolean electionTerminated, AtomicBoolean electionValid) {
    for (Node node : nodes) {
      logger.info("Requesting a vote from {}", node);
      AsyncClient client = raftServer.connectNode(node);
      if (client != null) {
        ElectionHandler handler = new ElectionHandler(raftServer, node, nextTerm, quorum, electionTerminated,
            electionValid);
        try {
          client.startElection(request, handler);
        } catch (TException e) {
          logger.error("Cannot request a vote from {}", node, e);
          raftServer.disConnectNode(node);
        }
      }
    }
  }
}
