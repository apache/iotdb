package org.apache.iotdb.consensus.natraft.protocol.heartbeat;

import org.apache.iotdb.consensus.natraft.protocol.RaftMember;
import org.apache.iotdb.consensus.natraft.protocol.RaftRole;
import org.apache.iotdb.consensus.natraft.protocol.Response;
import org.apache.iotdb.consensus.raft.thrift.HeartBeatRequest;
import org.apache.iotdb.consensus.raft.thrift.HeartBeatResponse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HeartbeatReqHandler {

  private static final Logger logger = LoggerFactory.getLogger(HeartbeatReqHandler.class);
  private RaftMember member;

  public HeartbeatReqHandler(RaftMember member) {
    this.member = member;
  }

  /**
   * Process the HeartBeatRequest from the leader. If the term of the leader is smaller than the
   * local term, reject the request by telling it the newest term. Else if the local logs are
   * consistent with the leader's, commit them. Else help the leader find the last matched log. Also
   * update the leadership, heartbeat timer and term of the local node.
   */
  public HeartBeatResponse processHeartbeatRequest(HeartBeatRequest request) {
    logger.trace("{} received a heartbeat", member.getName());
    long thisTerm = member.getStatus().getTerm().get();
    long leaderTerm = request.getTerm();
    HeartBeatResponse response = new HeartBeatResponse();

    if (leaderTerm < thisTerm) {
      // a leader with a term lower than this node is invalid, send it the local term to inform
      // it to resign
      response.setTerm(thisTerm);
      if (logger.isDebugEnabled()) {
        logger.debug(
            "{} received a heartbeat from a stale leader {}",
            member.getName(),
            request.getLeader());
      }
    } else if (!(leaderTerm == thisTerm && member.getStatus().getLeader().get() != null)) {
      // try updating local term or leader
      try {
        member.getLogManager().getLock().writeLock().lock();
        member.stepDown(leaderTerm, request.leader);
        member.getStatus().getLeader().set(request.getLeader());
        if (member.getStatus().getRole() != RaftRole.FOLLOWER) {
          // interrupt current election
          Object electionWaitObject = member.getHeartbeatThread().getElectionWaitObject();
          if (electionWaitObject != null) {
            synchronized (electionWaitObject) {
              electionWaitObject.notifyAll();
            }
          }
        }
      } finally {
        member.getLogManager().getLock().writeLock().unlock();
      }

      response.setTerm(Response.RESPONSE_AGREE);
      // tell the leader who I am in case of catch-up
      response.setFollower(member.getThisNode());
      // tell the leader the local log progress, so it may decide whether to perform a catch-up
      response.setLastLogIndex(member.getLogManager().getLastLogIndex());
      response.setLastLogTerm(member.getLogManager().getLastLogTerm());
      response.setCommitIndex(member.getLogManager().getCommitLogIndex());

      // if the snapshot apply lock is held, it means that a snapshot is installing now.
      boolean isFree = member.getSnapshotApplyLock().tryLock();
      if (isFree) {
        member.getSnapshotApplyLock().unlock();
      }
      response.setInstallingSnapshot(!isFree);
      if (logger.isDebugEnabled()) {
        logger.debug(
            "{}: log commit log index = {}, max have applied commit index = {}",
            member.getName(),
            member.getLogManager().getCommitLogIndex(),
            member.getLogManager().getAppliedIndex());
      }

      member.tryUpdateCommitIndex(
          leaderTerm, request.getCommitLogIndex(), request.getCommitLogTerm());

      if (logger.isTraceEnabled()) {
        logger.trace(
            "{} received heartbeat from a valid leader {}", member.getName(), request.getLeader());
      }
    }
    return response;
  }
}
