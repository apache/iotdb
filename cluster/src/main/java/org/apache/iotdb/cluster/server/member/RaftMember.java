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

package org.apache.iotdb.cluster.server.member;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.iotdb.cluster.client.async.ClientPool;
import org.apache.iotdb.cluster.client.sync.SyncClientAdaptor;
import org.apache.iotdb.cluster.config.ClusterConfig;
import org.apache.iotdb.cluster.config.ClusterDescriptor;
import org.apache.iotdb.cluster.exception.LeaderUnknownException;
import org.apache.iotdb.cluster.exception.LogExecutionException;
import org.apache.iotdb.cluster.exception.UnknownLogTypeException;
import org.apache.iotdb.cluster.log.HardState;
import org.apache.iotdb.cluster.log.Log;
import org.apache.iotdb.cluster.log.LogParser;
import org.apache.iotdb.cluster.log.catchup.CatchUpTask;
import org.apache.iotdb.cluster.log.logtypes.PhysicalPlanLog;
import org.apache.iotdb.cluster.log.manage.RaftLogManager;
import org.apache.iotdb.cluster.rpc.thrift.AppendEntriesRequest;
import org.apache.iotdb.cluster.rpc.thrift.AppendEntryRequest;
import org.apache.iotdb.cluster.rpc.thrift.ElectionRequest;
import org.apache.iotdb.cluster.rpc.thrift.ExecutNonQueryReq;
import org.apache.iotdb.cluster.rpc.thrift.HeartBeatRequest;
import org.apache.iotdb.cluster.rpc.thrift.HeartBeatResponse;
import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.cluster.rpc.thrift.RaftService;
import org.apache.iotdb.cluster.rpc.thrift.RaftService.AsyncClient;
import org.apache.iotdb.cluster.server.NodeCharacter;
import org.apache.iotdb.cluster.server.Peer;
import org.apache.iotdb.cluster.server.RaftServer;
import org.apache.iotdb.cluster.server.Response;
import org.apache.iotdb.cluster.server.handlers.caller.AppendNodeEntryHandler;
import org.apache.iotdb.cluster.server.handlers.caller.GenericHandler;
import org.apache.iotdb.cluster.utils.StatusUtils;
import org.apache.iotdb.db.exception.IoTDBException;
import org.apache.iotdb.db.exception.metadata.PathAlreadyExistException;
import org.apache.iotdb.db.exception.metadata.PathNotExistException;
import org.apache.iotdb.db.exception.metadata.StorageGroupAlreadySetException;
import org.apache.iotdb.db.exception.metadata.StorageGroupNotSetException;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.utils.TestOnly;
import org.apache.iotdb.service.rpc.thrift.TSStatus;
import org.apache.thrift.TException;
import org.apache.thrift.async.AsyncMethodCallback;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * RaftMember process the common raft logic like leader election, log appending, catch-up and so
 * on.
 */
@SuppressWarnings("java:S3077") // reference volatile is enough
public abstract class RaftMember implements RaftService.AsyncIface {

  private static long waitLeaderTimeMs = 60 * 1000L;
  private static final Logger logger = LoggerFactory.getLogger(RaftMember.class);

  ClusterConfig config = ClusterDescriptor.getInstance().getConfig();
  // the name of the member, to distinguish several members from the logs
  String name;
  // to choose nodes to join cluster request randomly
  Random random = new Random();

  protected Node thisNode;
  // the nodes known by this node
  protected List<Node> allNodes;
  protected Map<Node, Peer> peerMap;

  // the current term of the node, this object also works as lock of some transactions of the
  // member like elections
  AtomicLong term = new AtomicLong(0);
  volatile NodeCharacter character = NodeCharacter.ELECTOR;
  volatile Node leader;
  volatile Node voteFor;
  final Object waitLeaderCondition = new Object();
  volatile long lastHeartbeatReceivedTime;

  // the raft logs are all stored and maintained in the log manager
  RaftLogManager logManager;

  // the single thread pool that runs the heartbeat thread
  ExecutorService heartBeatService;
  // when the header of the group is removed from the cluster, the members of the group should no
  // longer accept writes, but they still can be read candidates for weak consistency reads and
  // provide snapshots for the new holders
  volatile boolean readOnly = false;
  // the thread pool that runs catch-up tasks
  private ExecutorService catchUpService;
  // lastCatchUpResponseTime records when is the latest response of each node's catch-up. There
  // should be only one catch-up task for each node to avoid duplication, but the task may
  // time out and in that case, the next catch up should be enabled.
  private Map<Node, Long> lastCatchUpResponseTime = new ConcurrentHashMap<>();
  // the pool that provides reusable clients to connect to other RaftMembers. It will be initialized
  // according to the implementation of the subclasses
  private ClientPool clientPool;
  // when the commit progress is updated by a heart beat, this object is notified so that we may
  // know if this node is synchronized with the leader
  private Object syncLock = new Object();

  public RaftMember() {
  }

  RaftMember(String name, ClientPool pool) {
    this.name = name;
    this.clientPool = pool;
  }

  /**
   * The maximum time to wait if there is no leader in the group, after which a
   * LeadNotFoundException will be thrown.
   */
  public static long getWaitLeaderTimeMs() {
    return waitLeaderTimeMs;
  }

  public static void setWaitLeaderTimeMs(long waitLeaderTimeMs) {
    RaftMember.waitLeaderTimeMs = waitLeaderTimeMs;
  }

  /**
   * Start the heartbeat thread and the catch-up thread pool. Calling the method twice does not
   * induce side effects.
   *
   * @throws TTransportException
   */
  public void start() {
    if (heartBeatService != null) {
      return;
    }

    heartBeatService =
        Executors.newSingleThreadScheduledExecutor(r -> new Thread(r,
            name + "-HeartbeatThread@" + System.currentTimeMillis()));
    catchUpService = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
    logger.info("{} started", name);
  }

  public RaftLogManager getLogManager() {
    return logManager;
  }

  /**
   * Stop the heartbeat thread and the catch-up thread pool. Calling the method twice does not
   * induce side effects.
   *
   * @throws TTransportException
   */
  public void stop() {
    closeLogManager();
    if (heartBeatService == null) {
      return;
    }

    heartBeatService.shutdownNow();
    catchUpService.shutdownNow();
    try {
      heartBeatService.awaitTermination(10, TimeUnit.SECONDS);
      catchUpService.awaitTermination(10, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      logger.error("Unexpected interruption when waiting for heartBeatService and catchUpService "
          + "to end", e);
    }
    catchUpService = null;
    heartBeatService = null;
    logger.info("{} heartbeats stopped", name);
  }

  /**
   * Process the HeartBeatRequest from the leader. If the term of the leader is smaller than the
   * local term, turn it down and tell it the newest term. ELse if the local logs catch up the
   * leader's, commit them. Else help the leader find the last match log. Also update the
   * leadership, heartbeat timer and term of the local node.
   *
   * @param request
   * @param resultHandler
   */
  @Override
  public void sendHeartbeat(HeartBeatRequest request, AsyncMethodCallback resultHandler) {
    logger.trace("{} received a heartbeat", name);
    synchronized (term) {
      long thisTerm = term.get();
      long leaderTerm = request.getTerm();
      HeartBeatResponse response = new HeartBeatResponse();

      if (leaderTerm < thisTerm) {
        // a leader with term lower than this node is invalid, send it the local term to inform this
        response.setTerm(thisTerm);
        if (logger.isTraceEnabled()) {
          logger.trace("{} received a heartbeat from a stale leader {}", name, request.getLeader());
        }
      } else {

        // interrupt election

        stepDown(leaderTerm);
        setLeader(request.getLeader());
        if (character != NodeCharacter.FOLLOWER) {
          term.notifyAll();
        }

        // the heartbeat comes from a valid leader, process it with the sub-class logic
        processValidHeartbeatReq(request, response);

        response.setTerm(Response.RESPONSE_AGREE);
        // tell the leader who I am in case of catch-up
        response.setFollower(thisNode);
        synchronized (logManager) {
          response.setLastLogIndex(logManager.getLastLogIndex());
          response.setLastLogTerm(logManager.getLastLogTerm());

          // The term of the last log needs to be the same with leader's term in order to preserve
          // safety, otherwise it may come from an invalid leader and is not committed
          if (logManager.maybeCommit(request.getCommitLogIndex(), request.getCommitLogTerm())) {
            logger.info("{}: Committing to {}-{}, localCommit: {}-{}, localLast: {}-{}", name,
                request.getCommitLogIndex(),
                request.getCommitLogTerm(), logManager.getCommitLogIndex(),
                logManager.getCommitLogTerm(), logManager.getLastLogIndex(),
                logManager.getLastLogTerm());
            synchronized (syncLock) {
              syncLock.notifyAll();
            }
          } else if (logManager.getCommitLogIndex() < request.getCommitLogIndex()) {
            logger
                .info("{}: Inconsistent log found, leader: {}-{}, local: {}-{}, last: {}-{}", name,
                    request.getCommitLogIndex(), request.getCommitLogTerm(),
                    logManager.getCommitLogIndex(), logManager.getCommitLogTerm(),
                    logManager.getLastLogIndex(), logManager.getLastLogTerm());
          }

        }
        // if the log is not consistent, the commitment will be blocked until the leader makes the
        // node catch up

        if (logger.isTraceEnabled()) {
          logger.trace("{} received heartbeat from a valid leader {}", name, request.getLeader());
        }
      }
      resultHandler.onComplete(response);
    }
  }

  /**
   * Process an ElectionRequest. If the request comes from the last leader, agree with it. Else
   * decide whether to accept by examining the log status of the elector.
   *
   * @param electionRequest
   * @param resultHandler
   */
  @Override
  public void startElection(ElectionRequest electionRequest, AsyncMethodCallback resultHandler) {
    synchronized (term) {
      long currentTerm = term.get();
      if (electionRequest.getTerm() < currentTerm) {
        logger.info("{} sending localTerm {} to the elector {} because it's term {} is smaller.",
            name,
            currentTerm,
            electionRequest.getElector(), electionRequest.getTerm());
        resultHandler.onComplete(currentTerm);
        return;
      }
      if (currentTerm == electionRequest.getTerm() && voteFor != null && !Objects
          .equals(voteFor, electionRequest.getElector())) {
        logger.info(
            "{} sending rejection to the elector {} because member already has voted {} in this term {}.",
            name,
            electionRequest.getElector(), voteFor, currentTerm);
        resultHandler.onComplete(Response.RESPONSE_REJECT);
        return;
      }
      if (electionRequest.getTerm() > currentTerm) {
        logger.info(
            "{} received an election from elector {} which has bigger term {} than localTerm {}, raftMember should step down first and then continue to decide whether to grant it's vote by log status.",
            name,
            electionRequest.getElector(), electionRequest.getTerm(), currentTerm);
        stepDown(electionRequest.getTerm());
      }

      // check the log status of the elector
      long response = processElectionRequest(electionRequest);
      logger.info("{} sending response {} to the elector {}", name, response,
          electionRequest.getElector());
      resultHandler.onComplete(response);
    }
  }

  /**
   * Check the term of the AppendEntryRequest. The term checked is the term of the leader, not the
   * term of the log. A new leader can still send logs of old leaders.
   *
   * @param request
   * @param resultHandler if the term is illegal, the "resultHandler" will be invoked so the caller
   *                      does not need to invoke it again
   * @return true if the term is legal, false otherwise
   */
  private boolean checkRequestTerm(AppendEntryRequest request, AsyncMethodCallback resultHandler) {
    long leaderTerm = request.getTerm();
    long localTerm;

    synchronized (term) {
      // if the request comes before the heartbeat arrives, the local term may be smaller than the
      // leader term
      localTerm = term.get();
      if (leaderTerm < localTerm) {
        logger.debug("{} rejected the AppendEntryRequest for term: {}/{}", name, leaderTerm,
            localTerm);
        resultHandler.onComplete(localTerm);
        return false;
      } else {
        if (leaderTerm > localTerm) {
          stepDown(leaderTerm);
        } else {
          lastHeartbeatReceivedTime = System.currentTimeMillis();
        }
        setLeader(request.getLeader());
        if (character != NodeCharacter.FOLLOWER) {
          term.notifyAll();
        }
      }
    }
    logger.debug("{} accepted the AppendEntryRequest for term: {}", name, localTerm);
    return true;
  }

  /**
   * Find the local previous log of "log". If such log is found, discard all local logs behind it
   * and append "log" to it. Otherwise report a log mismatch.
   *
   * @param log
   * @return Response.RESPONSE_AGREE when the log is successfully appended or Response
   * .RESPONSE_LOG_MISMATCH if the previous log of "log" is not found.
   */
  private long appendEntry(long prevLogIndex, long prevLogTerm, long leaderCommit, Log log) {
    long resp;
    synchronized (logManager) {
      long success = logManager.maybeAppend(prevLogIndex, prevLogTerm, leaderCommit, log);
      if (success != -1) {
        if (logger.isDebugEnabled()) {
          logger.debug("{} append a new log {}", name, log);
        }
        resp = Response.RESPONSE_AGREE;
      } else {
        // the incoming log points to an illegal position, reject it
        resp = Response.RESPONSE_LOG_MISMATCH;
      }
    }
    return resp;
  }

  /**
   * Process an AppendEntryRequest. First check the term of the leader, then parse the log and
   * finally see if we can find a position to append the log.
   *
   * @param request
   * @param resultHandler
   */
  @Override
  public void appendEntry(AppendEntryRequest request, AsyncMethodCallback resultHandler) {
    logger.debug("{} received an AppendEntryRequest", name);
    // the term checked here is that of the leader, not that of the log
    if (!checkRequestTerm(request, resultHandler)) {
      return;
    }

    try {
      Log log = LogParser.getINSTANCE().parse(request.entry);
      resultHandler.onComplete(
          appendEntry(request.prevLogIndex, request.prevLogTerm, request.leaderCommit, log));
      logger.debug("{} AppendEntryRequest of {} completed", name, log);
    } catch (UnknownLogTypeException e) {
      resultHandler.onError(e);
    }
  }

  @Override
  public void appendEntries(AppendEntriesRequest request, AsyncMethodCallback resultHandler) {
    logger.debug("{} received an AppendEntriesRequest", name);

    // the term checked here is that of the leader, not that of the log
    if (!checkRequestTerm(request, resultHandler)) {
      return;
    }

    try {
      long response;
      List<Log> logs = new ArrayList<>();
      for (ByteBuffer buffer : request.getEntries()) {
        Log log = LogParser.getINSTANCE().parse(buffer);
        logs.add(log);
      }

      response = appendEntries(request.prevLogIndex, request.prevLogTerm, request.leaderCommit,
          logs);
      resultHandler.onComplete(response);
      logger.debug("{} AppendEntriesRequest of log size {} completed", name,
          request.getEntries().size());
    } catch (UnknownLogTypeException e) {
      resultHandler.onError(e);
    }
  }

  /**
   * Find the local previous log of "log". If such log is found, discard all local logs behind it
   * and append "log" to it. Otherwise report a log mismatch.
   *
   * @param logs append logs
   * @return Response.RESPONSE_AGREE when the log is successfully appended or Response
   * .RESPONSE_LOG_MISMATCH if the previous log of "log" is not found.
   */
  private long appendEntries(long prevLogIndex, long prevLogTerm, long leaderCommit,
      List<Log> logs) {
    if (logs.isEmpty()) {
      return Response.RESPONSE_AGREE;
    }

    long resp;
    synchronized (logManager) {
      resp = logManager.maybeAppend(prevLogIndex, prevLogTerm, leaderCommit, logs);
      if (resp != -1) {
        if (logger.isDebugEnabled()) {
          logger.debug("{} append a new log list {}, commit to {}", name, logs, leaderCommit);
        }
        resp = Response.RESPONSE_AGREE;
      } else {
        // the incoming log points to an illegal position, reject it
        resp = Response.RESPONSE_LOG_MISMATCH;
      }
    }
    return resp;
  }

  /**
   * Check the term of the AppendEntryRequest. The term checked is the term of the leader, not the
   * term of the log. A new leader can still send logs of old leaders.
   *
   * @param request
   * @param resultHandler if the term is illegal, the "resultHandler" will be invoked so the caller
   *                      does not need to invoke it again
   * @return true if the term is legal, false otherwise
   */
  private boolean checkRequestTerm(AppendEntriesRequest request,
      AsyncMethodCallback resultHandler) {
    long leaderTerm = request.getTerm();
    long localTerm;

    synchronized (term) {
      // if the request comes before the heartbeat arrives, the local term may be smaller than the
      // leader term
      localTerm = term.get();
      if (leaderTerm < localTerm) {
        logger.debug("{} rejected the AppendEntriesRequest for term: {}/{}", name, leaderTerm,
            localTerm);
        resultHandler.onComplete(localTerm);
        return false;
      } else {
        if (leaderTerm > localTerm) {
          stepDown(leaderTerm);
        } else {
          lastHeartbeatReceivedTime = System.currentTimeMillis();
        }
        setLeader(request.getLeader());
        if (character != NodeCharacter.FOLLOWER) {
          term.notifyAll();
        }
      }
    }
    logger.debug("{} accepted the AppendEntryRequest for term: {}", name, localTerm);
    return true;
  }

  /**
   * Send the given log to all the followers and decide the result by how many followers return a
   * success.
   *
   * @param log
   * @param requiredQuorum the number of votes needed to make the log valid, when requiredQuorum <=
   *                       0, half of the cluster size will be used.
   * @return an AppendLogResult
   */
  protected AppendLogResult sendLogToFollowers(Log log, int requiredQuorum) {
    if (requiredQuorum <= 0) {
      return sendLogToFollowers(log, new AtomicInteger(allNodes.size() / 2));
    } else {
      return sendLogToFollowers(log, new AtomicInteger(requiredQuorum));
    }
  }

  /**
   * Send the log to each follower. Every time a follower returns a success, "voteCounter" is
   * decreased by 1 and when it counts to 0, return an OK. If any follower returns a higher term
   * than the local term, retire from leader and return a LEADERSHIP_STALE. If "voteCounter" is
   * still positive after a certain time, return TIME_OUT.
   *
   * @param log
   * @param voteCounter a decreasing vote counter
   * @return an AppendLogResult indicating a success or a failure and why
   */
  @SuppressWarnings({"java:S2445", "java:S2274"})
  private AppendLogResult sendLogToFollowers(Log log, AtomicInteger voteCounter) {
    if (allNodes.size() == 1) {
      // single node group, does not need the agreement of others
      return AppendLogResult.OK;
    }

    logger.debug("{} sending a log to followers: {}", name, log);

    AtomicBoolean leaderShipStale = new AtomicBoolean(false);
    AtomicLong newLeaderTerm = new AtomicLong(term.get());

    AppendEntryRequest request = new AppendEntryRequest();
    request.setTerm(term.get());
    request.setEntry(log.serialize());
    request.setLeader(getThisNode());
    // don't need lock because even it's larger than the commitIndex when appending this log to logManager,
    // the follower can handle the larger commitIndex with no effect
    request.setLeaderCommit(logManager.getCommitLogIndex());
    request.setPrevLogIndex(log.getCurrLogIndex() - 1);
    try {
      request.setPrevLogTerm(logManager.getTerm(log.getCurrLogIndex() - 1));
    } catch (Exception e) {
      logger.error("getTerm failed for newly append entries", e);
    }
    if (getHeader() != null) {
      // data groups use header to find a particular DataGroupMember
      request.setHeader(getHeader());
    }

    synchronized (voteCounter) {
      // synchronized: avoid concurrent modification
      synchronized (allNodes) {
        for (Node node : allNodes) {
          sendLogToFollower(log, voteCounter, node, leaderShipStale, newLeaderTerm, request);
        }
      }

      try {
        voteCounter.wait(RaftServer.getConnectionTimeoutInMS());
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        logger.warn("Unexpected interruption when sending a log", e);
      }
    }

    // some node has a larger term than the local node, this node is no longer a valid leader
    if (leaderShipStale.get()) {
      stepDown(newLeaderTerm.get());
      return AppendLogResult.LEADERSHIP_STALE;
    }

    // cannot get enough agreements within a certain amount of time
    if (voteCounter.get() > 0) {
      return AppendLogResult.TIME_OUT;
    }

    return AppendLogResult.OK;
  }

  private void sendLogToFollower(Log log, AtomicInteger voteCounter, Node node,
      AtomicBoolean leaderShipStale, AtomicLong newLeaderTerm, AppendEntryRequest request) {
    if (node.equals(thisNode)) {
      return;
    }
    Peer peer = peerMap.computeIfAbsent(node, k -> new Peer(logManager.getLastLogIndex()));
    if (!peer.isCatchUp()) {
      logger.warn("{} can't append log to node {} because it needs catchUp", name, node);
    } else {
      AsyncClient client = connectNode(node);
      if (client != null) {
        AppendNodeEntryHandler handler = new AppendNodeEntryHandler();
        handler.setReceiver(node);
        handler.setVoteCounter(voteCounter);
        handler.setLeaderShipStale(leaderShipStale);
        handler.setLog(log);
        handler.setPeer(peer);
        handler.setReceiverTerm(newLeaderTerm);
        try {
          client.appendEntry(request, handler);
          logger.debug("{} sending a log to {}: {}", name, node, log);
        } catch (Exception e) {
          logger.warn("{} cannot append log to node {}", name, node, e);
        }
      }
    }
  }

  /**
   * Get an asynchronous thrift client to the given node.
   *
   * @param node
   * @return an asynchronous thrift client or null if the caller tries to connect the local node.
   */
  public AsyncClient connectNode(Node node) {
    if (node == null) {
      return null;
    }

    AsyncClient client = null;
    try {
      client = clientPool.getClient(node);
    } catch (IOException e) {
      logger.warn("{} cannot connect to node {}", name, node, e);
    }
    return client;
  }

  public NodeCharacter getCharacter() {
    return character;
  }

  public void setCharacter(NodeCharacter character) {
    if (!Objects.equals(character, this.character)) {
      logger.info("{} has become a {}", name, character);
      this.character = character;
    }
  }

  public AtomicLong getTerm() {
    return term;
  }

  public void setTerm(AtomicLong term) {
    this.term = term;
  }

  public long getLastHeartbeatReceivedTime() {
    return lastHeartbeatReceivedTime;
  }

  public void setLastHeartbeatReceivedTime(long lastHeartbeatReceivedTime) {
    this.lastHeartbeatReceivedTime = lastHeartbeatReceivedTime;
  }

  public Node getLeader() {
    return leader;
  }

  public void setLeader(Node leader) {
    if (!Objects.equals(leader, this.leader)) {
      if (leader == null) {
        logger.info("{} has been set to null in term {}", getName(), term.get());
      } else if (!Objects.equals(leader, this.thisNode)) {
        logger.info("{} has become a follower of {} in term {}", getName(), leader, term.get());
      }
      synchronized (waitLeaderCondition) {
        this.leader = leader;
        if (leader != null) {
          waitLeaderCondition.notifyAll();
        }
      }
    }
  }

  public Node getVoteFor() {
    return voteFor;
  }

  public void setVoteFor(Node voteFor) {
    if (!Objects.equals(voteFor, this.voteFor)) {
      logger.info("{} has update it's voteFor to {}", getName(), voteFor);
      this.voteFor = voteFor;
    }
  }

  public Node getThisNode() {
    return thisNode;
  }

  public void setThisNode(Node thisNode) {
    this.thisNode = thisNode;
    allNodes.add(thisNode);
  }

  public Collection<Node> getAllNodes() {
    return allNodes;
  }

  public Map<Node, Long> getLastCatchUpResponseTime() {
    return lastCatchUpResponseTime;
  }

  /**
   * Sub-classes will add their own process of HeartBeatResponse in this method.
   *
   * @param response
   * @param receiver
   */
  public void processValidHeartbeatResp(HeartBeatResponse response, Node receiver) {

  }

  /**
   * The actions performed when the node wins in an election (becoming a leader).
   */
  public void onElectionWins() {

  }

  /**
   * Sub-classes will add their own process of HeartBeatRequest in this method.
   *
   * @param request
   * @param response
   */
  void processValidHeartbeatReq(HeartBeatRequest request, HeartBeatResponse response) {

  }

  /**
   * If "newTerm" is larger than the local term, give up the leadership, become a follower and reset
   * heartbeat timer.
   *
   * @param newTerm
   */
  public void stepDown(long newTerm) {
    synchronized (term) {
      long currTerm = term.get();
      // confirm that the heartbeat of the new leader hasn't come
      if (currTerm < newTerm) {
        logger.info("{} has update it's term to {}", getName(), newTerm);
        term.set(newTerm);
        setVoteFor(null);
        setLeader(null);
        updateHardState(newTerm, getVoteFor());
      }
      setCharacter(NodeCharacter.FOLLOWER);
      lastHeartbeatReceivedTime = System.currentTimeMillis();
    }
  }

  /**
   * Verify the validity of an ElectionRequest, and make itself a follower of the elector if the
   * request is valid.
   *
   * @param electionRequest
   * @return Response.RESPONSE_AGREE if the elector is valid or the local term if the elector has a
   * smaller term or Response.RESPONSE_LOG_MISMATCH if the elector has older logs.
   */
  long processElectionRequest(ElectionRequest electionRequest) {

    long thatTerm = electionRequest.getTerm();
    long thatLastLogIndex = electionRequest.getLastLogIndex();
    long thatLastLogTerm = electionRequest.getLastLogTerm();

    long resp = verifyElector(thatLastLogIndex,
        thatLastLogTerm);
    if (resp == Response.RESPONSE_AGREE) {
      logger.info("{} accepted an election request, term:{}/{}, logIndex:{}/{}, logTerm:{}/{}",
          name, thatTerm, term.get(), thatLastLogIndex, logManager.getLastLogIndex(),
          thatLastLogTerm,
          logManager.getLastLogTerm());
      setCharacter(NodeCharacter.FOLLOWER);
      lastHeartbeatReceivedTime = System.currentTimeMillis();
      setVoteFor(electionRequest.getElector());
      updateHardState(thatTerm, getVoteFor());
    } else {
      logger.info("{} rejected an election request, term:{}/{}, logIndex:{}/{}, logTerm:{}/{}",
          name, thatTerm, term.get(), thatLastLogIndex, logManager.getLastLogIndex(),
          thatLastLogTerm,
          logManager.getLastLogTerm());
    }
    return resp;
  }

  /**
   * Reject the election if one of the three holds: 1. the lastLogTerm of the candidate is smaller
   * than the voter's 2. the lastLogTerm of the candidate equals to the voter's but its lastLogIndex
   * is smaller than the voter's Otherwise accept the election.
   *
   * @param lastLogIndex
   * @param lastLogTerm
   * @return Response.RESPONSE_AGREE if the elector is valid or the local term if the elector has a
   * smaller term or Response.RESPONSE_LOG_MISMATCH if the elector has older logs.
   */
  long verifyElector(long lastLogIndex, long lastLogTerm) {
    long response;
    synchronized (logManager) {
      if (logManager.isLogUpToDate(lastLogTerm, lastLogIndex)) {
        response = Response.RESPONSE_AGREE;
      } else {
        response = Response.RESPONSE_LOG_MISMATCH;
      }
    }
    return response;
  }

  /**
   * Update the followers' log by sending logs whose index >= followerLastMatchedLogIndex to the
   * follower. If some of the logs are not in memory, also send the snapshot.
   * <br>notice that if a part of data is in the snapshot, then it is not in the logs</>
   *
   * @param follower
   */
  public void catchUp(Node follower) {
    // for one follower, there is at most one ongoing catch-up
    synchronized (catchUpService) {
      // check if the last catch-up is still ongoing
      Long lastCatchupResp = lastCatchUpResponseTime.get(follower);
      if (lastCatchupResp != null
          && System.currentTimeMillis() - lastCatchupResp < RaftServer.getConnectionTimeoutInMS()) {
        logger.debug("{}: last catch up of {} is ongoing", name, follower);
        return;
      } else {
        // record the start of the catch-up
        lastCatchUpResponseTime.put(follower, System.currentTimeMillis());
      }
    }

    AsyncClient client = connectNode(follower);
    if (client != null) {
      catchUpService.submit(new CatchUpTask(follower, peerMap.get(follower), this));
    } else {
      lastCatchUpResponseTime.remove(follower);
      logger.warn("{}: Catch-up failed: node {} is currently unavailable", name, follower);
    }
  }

  public String getName() {
    return name;
  }

  /**
   * @return the header of the data raft group or null if this is in a meta group.
   */
  public Node getHeader() {
    return null;
  }

  /**
   * Forward a non-query plan to a node using the default client.
   *
   * @param plan   a non-query plan
   * @param node   cannot be the local node
   * @param header must be set for data group communication, set to null for meta group
   *               communication
   * @return a TSStatus indicating if the forwarding is successful.
   */
  TSStatus forwardPlan(PhysicalPlan plan, Node node, Node header) {
    if (node == null || node.equals(thisNode)) {
      logger.debug("{}: plan {} has no where to be forwarded", name, plan);
      return StatusUtils.NO_LEADER;
    }

    logger.debug("{}: Forward {} to node {}", name, plan, node);

    AsyncClient client = connectNode(node);
    if (client != null) {
      return forwardPlan(plan, client, node, header);
    }
    return StatusUtils.TIME_OUT;
  }

  /**
   * Forward a non-query plan to "receiver" using "client".
   *
   * @param plan     a non-query plan
   * @param client
   * @param receiver
   * @param header   to determine which DataGroupMember of "receiver" will process the request.
   * @return a TSStatus indicating if the forwarding is successful.
   */
  TSStatus forwardPlan(PhysicalPlan plan, AsyncClient client, Node receiver, Node header) {

    try {
      TSStatus tsStatus = SyncClientAdaptor.executeNonQuery(client, plan, header, receiver);
      return tsStatus == null ? StatusUtils.TIME_OUT : tsStatus;
    } catch (IOException | TException e) {
      TSStatus status = StatusUtils.INTERNAL_ERROR.deepCopy();
      status.setMessage(e.getMessage());
      logger
          .error("{}: encountered an error when forwarding {} to {}", name, plan, receiver, e);
      return status;
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      return StatusUtils.TIME_OUT;
    }
  }

  /**
   * Create a log for "plan" and append it locally and to all followers. Only the group leader can
   * call this method. Will commit the log locally and send it to followers
   *
   * @param plan
   * @return OK if over half of the followers accept the log or null if the leadership is lost
   * during the appending
   */
  TSStatus processPlanLocally(PhysicalPlan plan) {
    logger.debug("{}: Processing plan {}", name, plan);
    if (readOnly) {
      return StatusUtils.NODE_READ_ONLY;
    }

    PhysicalPlanLog log = new PhysicalPlanLog();
    // assign term and index to the new log and append it
    synchronized (logManager) {
      log.setCurrLogTerm(getTerm().get());
      log.setPreviousLogIndex(logManager.getLastLogIndex());
      log.setPreviousLogTerm(logManager.getLastLogTerm());
      log.setCurrLogIndex(logManager.getLastLogIndex() + 1);

      log.setPlan(plan);
      logManager.append(log);
    }

    try {
      if (appendLogInGroup(log)) {
        return StatusUtils.OK;
      }
    } catch (LogExecutionException e) {
      TSStatus tsStatus = StatusUtils.EXECUTE_STATEMENT_ERROR.deepCopy();
      Throwable cause = e.getCause();
      if (cause instanceof IoTDBException) {
        tsStatus.setCode(((IoTDBException) cause).getErrorCode());
      }
      if (!(cause instanceof PathNotExistException) &&
          !(cause instanceof StorageGroupNotSetException) &&
          !(cause instanceof PathAlreadyExistException) &&
          !(cause instanceof StorageGroupAlreadySetException)) {
        logger.debug("{} cannot be executed because {}", plan, cause);
      }
      tsStatus.setMessage(cause.getClass().getName() + ":" + cause.getMessage());
      return tsStatus;
    }
    return null;
  }

  /**
   * Append a log to all followers in the group until half of them accept the log or the leadership
   * is lost.
   *
   * @param log
   * @return true if the log is accepted by the quorum of the group, false otherwise
   */
  protected boolean appendLogInGroup(Log log) throws LogExecutionException {
    int retryTime = 0;
    while (true) {
      logger.debug("{}: Send log {} to other nodes, retry times: {}", name, log, retryTime);
      AppendLogResult result = sendLogToFollowers(log, allNodes.size() / 2);
      switch (result) {
        case OK:
          logger.debug("{}: log {} is accepted", name, log);
          synchronized (logManager) {
            logManager.commitTo(log.getCurrLogIndex(), false);
          }
          return true;
        case TIME_OUT:
          logger.debug("{}: log {} timed out, retrying...", name, log);
          retryTime++;
          break;
        case LEADERSHIP_STALE:
          // abort the appending, the new leader will fix the local logs by catch-up
        default:
          return false;
      }
    }
  }

  /**
   * If the node is not a leader, the request will be sent to the leader or reports an error if
   * there is no leader. Otherwise execute the plan locally (whether to send it to followers depends
   * on the type of the plan).
   *
   * @param request
   * @param resultHandler
   */
  public void executeNonQueryPlan(ExecutNonQueryReq request,
      AsyncMethodCallback<TSStatus> resultHandler) {
    if (character != NodeCharacter.LEADER) {
      // forward the plan to the leader
      AsyncClient client = connectNode(leader);
      if (client != null) {
        try {
          client.executeNonQueryPlan(request, resultHandler);
        } catch (TException e) {
          resultHandler.onError(e);
        }
      } else {
        resultHandler.onComplete(StatusUtils.NO_LEADER);
      }
      return;
    }
    try {
      // process the plan locally
      PhysicalPlan plan = PhysicalPlan.Factory.create(request.planBytes);
      logger.debug("{}: Received a plan {}", name, plan);
      resultHandler.onComplete(executeNonQuery(plan));
    } catch (Exception e) {
      resultHandler.onError(e);
    }
  }

  /**
   * Request and check the leader's commitId to see whether this node has caught up. If not, wait
   * until this node catches up.
   *
   * @return true if the node has caught up, false otherwise
   */
  public boolean syncLeader() {

    if (character == NodeCharacter.LEADER) {
      return true;
    }
    waitLeader();
    if (leader == null) {
      // the leader has not been elected, we must assume the node falls behind
      logger.warn("{}: No leader is found when synchronizing", name);
      return false;
    }
    if (character == NodeCharacter.LEADER) {
      return true;
    }
    logger.debug("{}: try synchronizing with the leader {}", name, leader);
    long startTime = System.currentTimeMillis();
    long waitedTime = 0;
    AtomicReference<Long> commitIdResult = new AtomicReference<>(Long.MAX_VALUE);
    while (waitedTime < RaftServer.getSyncLeaderMaxWaitMs()) {
      AsyncClient client = connectNode(leader);
      if (client == null) {
        // cannot connect to the leader
        logger.warn("{}: No leader is found when synchronizing", name);
        return false;
      }
      try {
        synchronized (commitIdResult) {
          client.requestCommitIndex(getHeader(), new GenericHandler<>(leader, commitIdResult));
          commitIdResult.wait(RaftServer.getSyncLeaderMaxWaitMs());
        }
        long leaderCommitId = commitIdResult.get();
        long localCommitId = logManager.getCommitLogIndex();
        logger.debug("{}: synchronizing commitIndex {}/{}", name, localCommitId, leaderCommitId);
        if (leaderCommitId <= localCommitId) {
          // before the response comes, the leader may commit new logs and the localCommitId may be
          // updated by catching up, so it is possible that localCommitId > leaderCommitId at
          // this time
          // this node has caught up
          if (logger.isDebugEnabled()) {
            waitedTime = System.currentTimeMillis() - startTime;
            logger.debug("{}: synchronized with the leader after {}ms", name, waitedTime);
          }
          return true;
        }
        // wait for next heartbeat to catch up
        // the local node will not perform a commit here according to the leaderCommitId because
        // the node may have some inconsistent logs with the leader
        waitedTime = System.currentTimeMillis() - startTime;
        synchronized (syncLock) {
          syncLock.wait(RaftServer.getHeartBeatIntervalMs());
        }
      } catch (TException e) {
        logger.error("{}: Cannot request commit index from {}", name, leader, e);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        logger.error("{}: Cannot request commit index from {}", name, leader, e);
      }
    }
    logger.warn("{}: Failed to synchronize with the leader after {}ms", name, waitedTime);
    return false;
  }

  /**
   * Execute a non-query plan.
   *
   * @param plan a non-query plan.
   * @return A TSStatus indicating the execution result.
   */
  abstract TSStatus executeNonQuery(PhysicalPlan plan);

  /**
   * Tell the requester the current commit index if the local node is the leader of the group headed
   * by header. Or forward it to the leader. Otherwise report an error.
   *
   * @param header        to determine the DataGroupMember in data groups
   * @param resultHandler
   */
  @Override
  public void requestCommitIndex(Node header, AsyncMethodCallback<Long> resultHandler) {
    if (character == NodeCharacter.LEADER) {
      resultHandler.onComplete(logManager.getCommitLogIndex());
      return;
    }
    waitLeader();
    AsyncClient client = connectNode(leader);
    if (client == null) {
      resultHandler.onError(new LeaderUnknownException(getAllNodes()));
      return;
    }
    try {
      client.requestCommitIndex(header, resultHandler);
    } catch (TException e) {
      resultHandler.onError(e);
    }
  }

  /**
   * An ftp-like interface that is used for a node to pull chunks of files like TsFiles. Once the
   * file is totally read, it will be removed.
   *
   * @param filePath
   * @param offset
   * @param length
   * @param resultHandler
   */
  @Override
  public void readFile(String filePath, long offset, int length,
      AsyncMethodCallback<ByteBuffer> resultHandler) {
    File file = new File(filePath);
    if (!file.exists()) {
      resultHandler.onComplete(ByteBuffer.allocate(0));
      return;
    }

    boolean fileExhausted = false;
    try (BufferedInputStream bufferedInputStream =
        new BufferedInputStream(new FileInputStream(file))) {
      skipExactly(bufferedInputStream, offset);
      byte[] bytes = new byte[length];
      ByteBuffer result = ByteBuffer.wrap(bytes);
      int len = bufferedInputStream.read(bytes);
      result.limit(Math.max(len, 0));
      fileExhausted = bufferedInputStream.available() <= 0;

      resultHandler.onComplete(result);
    } catch (IOException e) {
      resultHandler.onError(e);
    }

    if (fileExhausted) {
      try {
        Files.delete(file.toPath());
      } catch (IOException e) {
        logger.warn("Cannot delete an exhausted file {}", filePath, e);
      }
    }
  }

  private void skipExactly(InputStream stream, long byteToSkip) throws IOException {
    while (byteToSkip > 0) {
      byteToSkip -= stream.skip(byteToSkip);
    }
  }

  public void updateHardState(long currentTerm, Node voteFor) {
    HardState state = logManager.getHardState();
    state.setCurrentTerm(currentTerm);
    state.setVoteFor(voteFor);
    logManager.updateHardState(state);
  }

  public void setReadOnly() {
    synchronized (logManager) {
      readOnly = true;
    }
  }

  public void setAllNodes(List<Node> allNodes) {
    this.allNodes = allNodes;
  }

  public void initPeerMap() {
    peerMap = new ConcurrentHashMap<>();
    for (Node entry : allNodes) {
      peerMap.computeIfAbsent(entry, k -> new Peer(logManager.getLastLogIndex()));
    }
  }

  public Map<Node, Peer> getPeerMap() {
    return peerMap;
  }

  @TestOnly
  public void setLogManager(RaftLogManager logManager) {
    if (this.logManager != null) {
      this.logManager.close();
    }
    this.logManager = logManager;
  }

  public void closeLogManager() {
    if (logManager != null) {
      logManager.close();
    }
  }

  enum AppendLogResult {
    OK, TIME_OUT, LEADERSHIP_STALE
  }

  @Override
  public void matchTerm(long index, long term, Node header,
      AsyncMethodCallback<Boolean> resultHandler) {
    resultHandler.onComplete(logManager.matchTerm(term, index));
  }

  void waitLeader() {
    long startTime = System.currentTimeMillis();
    while (leader == null) {
      synchronized (waitLeaderCondition) {
        try {
          waitLeaderCondition.wait(10);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          logger.error("Unexpected interruption when waiting for a leader to be elected", e);
        }
      }
      long consumedTime = System.currentTimeMillis() - startTime;
      if (consumedTime >= getWaitLeaderTimeMs()) {
        break;
      }
    }
  }
}
