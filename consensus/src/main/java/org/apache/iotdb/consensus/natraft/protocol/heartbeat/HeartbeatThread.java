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

import org.apache.iotdb.commons.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.consensus.common.Peer;
import org.apache.iotdb.consensus.natraft.client.AsyncRaftServiceClient;
import org.apache.iotdb.consensus.natraft.protocol.RaftConfig;
import org.apache.iotdb.consensus.natraft.protocol.RaftMember;
import org.apache.iotdb.consensus.natraft.protocol.RaftRole;
import org.apache.iotdb.consensus.natraft.utils.NodeUtils;
import org.apache.iotdb.consensus.raft.thrift.ElectionRequest;
import org.apache.iotdb.consensus.raft.thrift.HeartBeatRequest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * HeartbeatThread takes the responsibility to send heartbeats (when this node is a leader), check
 * if the leader is still online (when this node is a follower) or start elections (when this node
 * is an elector).
 */
public class HeartbeatThread implements Runnable {

  private static final Logger logger = LoggerFactory.getLogger(HeartbeatThread.class);

  private RaftMember localMember;
  private String memberName;
  HeartBeatRequest request = new HeartBeatRequest();
  ElectionRequest electionRequest = new ElectionRequest();
  /**
   * when this node is a follower, this records the unix-epoch timestamp when the last heartbeat
   * arrived, and is reported in the timed member report to show how long the leader has been
   * offline.
   */
  volatile long lastHeartbeatReceivedTime;

  private Random random = new Random();
  boolean hasHadLeader = false;
  private final Object heartBeatWaitObject = new Object();
  private Object electionWaitObject;
  /**
   * the single thread pool that runs the heartbeat thread, which send heartbeats to the follower
   * when this node is a leader, or start elections when this node is an elector.
   */
  protected ExecutorService heartBeatService;

  protected RaftConfig config;

  public HeartbeatThread(RaftMember localMember, RaftConfig config) {
    this.localMember = localMember;
    memberName = localMember.getName();
    heartBeatService =
        IoTDBThreadPoolFactory.newSingleThreadScheduledExecutor(
            localMember.getName() + "-Heartbeat");
    this.config = config;
  }

  public void start() {
    heartBeatService.submit(this);
  }

  public void stop() {
    heartBeatService.shutdownNow();
    try {
      heartBeatService.awaitTermination(60, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      logger.error("Unexpected interruption when waiting for heartbeat to end", e);
    }
  }

  @Override
  public void run() {
    logger.info("{}: Heartbeat thread starts...", memberName);
    // sleep random time to reduce first election conflicts
    long electionWait = getElectionRandomWaitMs();
    try {
      logger.info("{}: Sleep {}ms before first election", memberName, electionWait);
      Thread.sleep(electionWait);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
    while (!Thread.interrupted()) {
      try {
        switch (localMember.getRole()) {
          case LEADER:
            // send heartbeats to the followers
            sendHeartbeats();
            synchronized (heartBeatWaitObject) {
              heartBeatWaitObject.wait(config.getHeartbeatIntervalMs());
            }
            hasHadLeader = true;
            break;
          case FOLLOWER:
            // check if heartbeat times out
            long heartbeatInterval = System.currentTimeMillis() - lastHeartbeatReceivedTime;

            long randomElectionTimeout = config.getElectionTimeoutMs() + getElectionRandomWaitMs();
            if (heartbeatInterval >= randomElectionTimeout) {
              // the leader is considered dead, an election will be started in the next loop
              logger.info(
                  "{}: The leader {} timed out", memberName, localMember.getStatus().getLeader());
              localMember.getStatus().setRole(RaftRole.CANDIDATE);
              localMember.setLeader(null);
            } else {
              logger.trace(
                  "{}: Heartbeat from leader {} is still valid",
                  memberName,
                  localMember.getStatus().getLeader());
              synchronized (heartBeatWaitObject) {
                // we sleep to next possible heartbeat timeout point
                long leastWaitTime =
                    lastHeartbeatReceivedTime + randomElectionTimeout - System.currentTimeMillis();
                heartBeatWaitObject.wait(leastWaitTime);
              }
            }
            hasHadLeader = true;
            break;
          case CANDIDATE:
          default:
            onElectionsStart();
            startElections();
            onElectionsEnd();
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

  protected void onElectionsStart() {
    logger.info("{}: Start elections", memberName);
  }

  protected void onElectionsEnd() {
    logger.info("{}: End elections", memberName);
  }

  /** Send each node (except the local node) in the group of the member a heartbeat. */
  protected void sendHeartbeats() {
    try {
      localMember.getLogManager().getLock().readLock().lock();
      request.setTerm(localMember.getStatus().getTerm().get());
      request.setLeader(localMember.getThisNode().getEndpoint());
      request.setLeaderId(localMember.getThisNode().getNodeId());
      request.setCommitLogIndex(localMember.getLogManager().getCommitLogIndex());
      request.setCommitLogTerm(localMember.getLogManager().getCommitLogTerm());
      request.setGroupId(localMember.getRaftGroupId().convertToTConsensusGroupId());
    } finally {
      localMember.getLogManager().getLock().readLock().unlock();
    }
    sendHeartbeats(localMember.getAllNodes());
  }

  /** Send each node (except the local node) in list a heartbeat. */
  @SuppressWarnings("java:S2445")
  private void sendHeartbeats(Collection<Peer> nodes) {
    logger.debug(
        "{}: Send heartbeat to {} followers, commit log index = {}",
        memberName,
        nodes.size() - 1,
        request.getCommitLogIndex());
    for (Peer node : nodes) {
      if (node.equals(localMember.getThisNode())) {
        continue;
      }
      if (Thread.currentThread().isInterrupted()) {
        Thread.currentThread().interrupt();
        return;
      }

      if (localMember.getRole() != RaftRole.LEADER) {
        // if the character changes, abort the remaining heartbeats
        logger.warn("The leadership of node {} is ended.", localMember.getThisNode());
        return;
      }

      sendHeartbeatAsync(node);
    }
  }

  /**
   * Send a heartbeat to "node" through "client".
   *
   * @param node
   */
  void sendHeartbeatAsync(Peer node) {
    AsyncRaftServiceClient client = localMember.getHeartbeatClient(node.getEndpoint());
    if (client != null) {
      // connecting to the local node results in a null
      try {
        logger.debug("{}: Sending heartbeat to {}", memberName, node);
        client.sendHeartbeat(request, new HeartbeatRespHandler(localMember, node));
      } catch (Exception e) {
        logger.warn("{}: Cannot send heart beat to node {}", memberName, node, e);
      }
    }
  }

  /**
   * Start elections until this node becomes a leader or a follower.
   *
   * @throws InterruptedException
   */
  private void startElections() throws InterruptedException {
    if (localMember.getAllNodes().size() == 1) {
      // single node group, this node is always the leader
      localMember.getStatus().setRole(RaftRole.LEADER);
      localMember.getStatus().getLeader().set(localMember.getThisNode());
      logger.info("{}: Winning the election because the node is the only node.", memberName);
    }

    // the election goes on until this node becomes a follower or a leader
    while (localMember.getRole() == RaftRole.CANDIDATE) {
      startElection();
      if (localMember.getRole() == RaftRole.CANDIDATE) {
        // sleep random time to reduce election conflicts
        long electionWait = getElectionRandomWaitMs();
        logger.info("{}: Sleep {}ms until next election", memberName, electionWait);
        Thread.sleep(electionWait);
      }
    }
    // take the election request as the first heartbeat
    setLastHeartbeatReceivedTime(System.currentTimeMillis());
  }

  /**
   * Start one round of election. Increase the local term, ask for vote from each of the nodes in
   * the group and become the leader if at least half of them agree.
   */
  @SuppressWarnings({"java:S2274"})
  // enable timeout
  void startElection() {
    if (localMember.isStopped()) {
      logger.info("{}: Skip election because this node has stopped.", memberName);
      return;
    }

    long nextTerm;
    try {
      localMember.getLogManager().getLock().writeLock().lock();

      nextTerm = localMember.getStatus().getTerm().incrementAndGet();
      localMember.getStatus().setVoteFor(localMember.getThisNode());
      localMember.updateHardState(nextTerm, this.localMember.getStatus().getVoteFor());
      // erase the log index, so it can be updated in the next heartbeat
      electionRequest.unsetLastLogIndex();
    } finally {
      localMember.getLogManager().getLock().writeLock().unlock();
    }

    // the number of votes needed to become a leader,
    // currNodeQuorumNum should be equal to localMember.getAllNodes().size() / 2 + 1,
    // but since it doesn’t need to vote for itself here, it directly decreases 1
    logger.info("{}: Election {} starts", memberName, nextTerm);
    // NOTICE, failingVoteCounter should be equal to currNodeQuorumNum + 1

    electionRequest.setTerm(nextTerm);
    electionRequest.setElector(localMember.getThisNode().getEndpoint());
    electionRequest.setElectorId(localMember.getThisNode().getNodeId());
    electionRequest.setLastLogTerm(localMember.getLogManager().getLastLogTerm());
    electionRequest.setLastLogIndex(localMember.getLogManager().getLastLogIndex());
    electionRequest.setGroupId(localMember.getRaftGroupId().convertToTConsensusGroupId());

    ElectionState electionState =
        new ElectionState(localMember.getAllNodes(), localMember.getNewNodes());

    requestVote(electionState, electionRequest, nextTerm);

    try {
      logger.info(
          "{}: Wait for {}ms until election time out", memberName, config.getElectionTimeoutMs());
      synchronized (electionState) {
        electionWaitObject = electionState;
        electionState.wait(config.getElectionTimeoutMs());
      }
      electionWaitObject = null;
    } catch (InterruptedException e) {
      logger.info("{}: Unexpected interruption when waiting the result of election", memberName);
      Thread.currentThread().interrupt();
    }

    // if the election times out, the remaining votes do not matter
    if (electionState.isAccepted()) {
      logger.info("{}: Election {} accepted", memberName, nextTerm);
      localMember.getStatus().setRole(RaftRole.LEADER);
      localMember.getStatus().getLeader().set(localMember.getThisNode());
    }
  }

  /**
   * Request a vote from each of the "currNodes". Each for vote will decrease the counter "quorum"
   * and when it reaches 0, the flag "electionValid" and "electionTerminated" will be set to true.
   * Any against vote will set the flag "electionTerminated" to true and ends the election.
   */
  @SuppressWarnings("java:S2445")
  private void requestVote(ElectionState electionState, ElectionRequest request, long nextTerm) {

    Collection<Peer> peers =
        NodeUtils.unionNodes(electionState.getCurrNodes(), electionState.getNewNodes());
    // avoid concurrent modification
    for (Peer node : peers) {
      if (node.equals(localMember.getThisNode())) {
        continue;
      }

      ElectionRespHandler handler =
          new ElectionRespHandler(localMember, node, nextTerm, electionState);
      requestVoteAsync(node, handler, request);
    }
  }

  private void requestVoteAsync(Peer node, ElectionRespHandler handler, ElectionRequest request) {
    AsyncRaftServiceClient client = localMember.getHeartbeatClient(node.getEndpoint());
    if (client != null) {
      logger.info("{}: Requesting a vote from {}", memberName, node);
      try {
        client.startElection(request, handler);
      } catch (Exception e) {
        logger.error("{}: Cannot request a vote from {}", memberName, node, e);
      }
    }
  }

  private long getElectionRandomWaitMs() {
    return Math.abs(random.nextLong() % config.getElectionMaxWaitMs());
  }

  public void notifyHeartbeat() {
    synchronized (heartBeatWaitObject) {
      heartBeatWaitObject.notifyAll();
    }
  }

  public void setLastHeartbeatReceivedTime(long currentTimeMillis) {
    lastHeartbeatReceivedTime = currentTimeMillis;
  }

  public Object getElectionWaitObject() {
    return electionWaitObject;
  }

  public long getLastHeartbeatReceivedTime() {
    return lastHeartbeatReceivedTime;
  }
}
