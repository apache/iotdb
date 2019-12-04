/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at      http://www.apache.org/licenses/LICENSE-2.0  Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the License for the specific language governing permissions and limitations under the License.
 */

package org.apache.iotdb.cluster.server.member;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.iotdb.cluster.client.ClientPool;
import org.apache.iotdb.cluster.config.ClusterConfig;
import org.apache.iotdb.cluster.config.ClusterConstant;
import org.apache.iotdb.cluster.config.ClusterDescriptor;
import org.apache.iotdb.cluster.exception.LeaderUnknownException;
import org.apache.iotdb.cluster.exception.UnknownLogTypeException;
import org.apache.iotdb.cluster.log.Log;
import org.apache.iotdb.cluster.log.LogManager;
import org.apache.iotdb.cluster.log.LogParser;
import org.apache.iotdb.cluster.log.Snapshot;
import org.apache.iotdb.cluster.log.catchup.LogCatchUpTask;
import org.apache.iotdb.cluster.log.catchup.SnapshotCatchUpTask;
import org.apache.iotdb.cluster.log.logs.PhysicalPlanLog;
import org.apache.iotdb.cluster.partition.PartitionGroup;
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
import org.apache.iotdb.cluster.server.Response;
import org.apache.iotdb.cluster.server.handlers.caller.AppendNodeEntryHandler;
import org.apache.iotdb.cluster.server.handlers.caller.RequestCommitIdHandler;
import org.apache.iotdb.cluster.server.handlers.forwarder.ForwardPlanHandler;
import org.apache.iotdb.cluster.utils.StatusUtils;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.qp.QueryProcessor;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.service.rpc.thrift.TSStatus;
import org.apache.thrift.TException;
import org.apache.thrift.async.AsyncMethodCallback;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class RaftMember implements RaftService.AsyncIface {

  ClusterConfig config = ClusterDescriptor.getINSTANCE().getConfig();
  private static final Logger logger = LoggerFactory.getLogger(RaftMember.class);

  static final int PULL_SNAPSHOT_RETRY_INTERVAL = 5 * 1000;

  String name;

  Random random = new Random();
  Node thisNode;
  List<Node> allNodes;

  volatile NodeCharacter character = NodeCharacter.ELECTOR;
  AtomicLong term = new AtomicLong(0);
  volatile Node leader;
  volatile long lastHeartBeatReceivedTime;

  LogManager logManager;

  ExecutorService heartBeatService;
  private ExecutorService catchUpService;

  private Map<Node, Long> lastCatchUpResponseTime = new ConcurrentHashMap<>();

  QueryProcessor queryProcessor;
  private ClientPool clientPool;
  // when the commit progress is updated by a heart beat, this object is notified so that we may
  // know if this node is synchronized with the leader
  private Object syncLock = new Object();

  RaftMember(String name, ClientPool pool) {
    this.name = name;
    this.clientPool = pool;
  }

  public void start() throws TTransportException {
    if (heartBeatService != null) {
      return;
    }

    heartBeatService =
        Executors.newSingleThreadScheduledExecutor(r -> new Thread(r, name + "-HeartBeatThread"));
    catchUpService = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
  }

  public LogManager getLogManager() {
    return logManager;
  }

  void initLogManager() {

  }

  public void stop() {
    if (heartBeatService == null) {
      return;
    }

    heartBeatService.shutdownNow();
    catchUpService.shutdownNow();
    catchUpService = null;
    heartBeatService = null;
  }

  @Override
  public void sendHeartBeat(HeartBeatRequest request, AsyncMethodCallback resultHandler) {
    logger.debug("{} received a heartbeat", name);
    synchronized (term) {
      long thisTerm = term.get();
      long leaderTerm = request.getTerm();
      HeartBeatResponse response = new HeartBeatResponse();

      if (leaderTerm < thisTerm) {
        // the leader is invalid
        response.setTerm(thisTerm);
        if (logger.isDebugEnabled()) {
          logger.debug("{} received a heartbeat from a stale leader {}", name, request.getLeader());
        }
      } else {
        processValidHeartbeatReq(request, response, leaderTerm);

        response.setTerm(Response.RESPONSE_AGREE);
        response.setFollower(thisNode);
        response.setLastLogIndex(logManager.getLastLogIndex());
        response.setLastLogTerm(logManager.getLastLogTerm());

        synchronized (syncLock) {
          logManager.commitLog(request.getCommitLogIndex());
          syncLock.notifyAll();
        }
        term.set(leaderTerm);
        setLeader(request.getLeader());
        if (character != NodeCharacter.FOLLOWER) {
          setCharacter(NodeCharacter.FOLLOWER);
        }
        setLastHeartBeatReceivedTime(System.currentTimeMillis());
        if (logger.isDebugEnabled()) {
          logger.debug("{} received heartbeat from a valid leader {}", name, request.getLeader());
        }
      }
      resultHandler.onComplete(response);
    }
  }

  @Override
  public void startElection(ElectionRequest electionRequest, AsyncMethodCallback resultHandler) {
    synchronized (term) {
      if (electionRequest.getElector().equals(leader)) {
        resultHandler.onComplete(Response.RESPONSE_AGREE);
        return;
      }

      long thisTerm = term.get();
      if (character != NodeCharacter.ELECTOR) {
        // only elector votes
        resultHandler.onComplete(Response.RESPONSE_LEADER_STILL_ONLINE);
        return;
      }
      long response = processElectionRequest(electionRequest);
      logger.info("{} sending response {} to the elector {}", name, response, electionRequest.getElector());
      resultHandler.onComplete(response);
    }
  }

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
      } else if (leaderTerm > localTerm) {
        term.set(leaderTerm);
        localTerm = leaderTerm;
        if (character != NodeCharacter.FOLLOWER) {
          setCharacter(NodeCharacter.FOLLOWER);
        }
      }
    }
    logger.debug("{} accepted the AppendEntryRequest for term: {}",name, localTerm);
    return true;
  }

  long appendEntry(Log log) throws QueryProcessException {
    long resp;
    synchronized (logManager) {
      Log lastLog = logManager.getLastLog();
      long previousLogIndex = log.getPreviousLogIndex();
      long previousLogTerm = log.getPreviousLogTerm();

      if (logManager.getLastLogIndex() == previousLogIndex && logManager.getLastLogTerm() == previousLogTerm) {
        // the incoming log points to the local last log, append it
        logManager.appendLog(log);
        if (logger.isDebugEnabled()) {
          logger.debug("{} append a new log {}, new term:{}, new index:{}", name, log, term.get(),
              logManager.getLastLogIndex());
        }
        resp = Response.RESPONSE_AGREE;
      } else if (lastLog != null && lastLog.getPreviousLogIndex() == previousLogIndex
          && lastLog.getPreviousLogTerm() <= previousLogTerm) {
        // the incoming log points to the previous log of the local last log, and its term is
        // bigger than or equals to the local last log's, replace the local last log with it
        logManager.replaceLastLog(log);
        logger.debug("{} replaced the last log with {}, new term:{}, new index:{}", name, log,
            log.getCurrLogTerm(), log.getCurrLogIndex());
        resp = Response.RESPONSE_AGREE;
      } else {
        long lastPrevLogTerm = lastLog == null ? -1 : lastLog.getPreviousLogTerm();
        long lastPrevLogId = lastLog == null ? -1 : lastLog.getPreviousLogIndex();
        // the incoming log points to an illegal position, reject it
        logger.debug("{} cannot append the log because the last log does not match, "
                + "local:term[{}],index[{}],previousTerm[{}],previousIndex[{}], "
                + "request:term[{}],index[{}],previousTerm[{}],previousIndex[{}]",
            name,
            logManager.getLastLogTerm(), logManager.getLastLogIndex(),
            lastPrevLogTerm, lastPrevLogId,
            log.getCurrLogTerm(), log.getPreviousLogIndex(),
            previousLogTerm, previousLogIndex);
        resp = Response.RESPONSE_LOG_MISMATCH;
      }
    }
    return resp;
  }

  @Override
  public void appendEntry(AppendEntryRequest request, AsyncMethodCallback resultHandler) {
    logger.debug("{} received an AppendEntryRequest", name);
    if (!checkRequestTerm(request, resultHandler)) {
      return;
    }

    try {
      Log log = LogParser.getINSTANCE().parse(request.entry);
      resultHandler.onComplete(appendEntry(log));
    } catch (UnknownLogTypeException | QueryProcessException e) {
      resultHandler.onError(e);
    }
  }

  @Override
  public void appendEntries(AppendEntriesRequest request, AsyncMethodCallback resultHandler) {
    //TODO-Cluster: implement
  }

  /**
   * Send the given log to all the followers and decide the result according to the specified
   * quorum.
   * @param log
   * @param requiredQuorum the number of votes needed to make the log valid, when requiredQuorum
   *                       < 0, half of the cluster size will be used.
   * @return an AppendLogResult
   */
  private AppendLogResult sendLogToFollowers(Log log, int requiredQuorum) {
    if (requiredQuorum < 0) {
      return sendLogToFollowers(log, new AtomicInteger(allNodes.size() / 2));
    } else {
      return sendLogToFollowers(log ,new AtomicInteger(requiredQuorum));
    }
  }

  // synchronized: logs are serialized
  private synchronized AppendLogResult sendLogToFollowers(Log log, AtomicInteger quorum) {
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
    if (getHeader() != null) {
      request.setHeader(getHeader());
    }

    synchronized (quorum) {
      // synchronized: avoid concurrent modification
      synchronized (allNodes) {
        for (Node node : allNodes) {
          AsyncClient client = connectNode(node);
          if (client != null) {
            AppendNodeEntryHandler handler = new AppendNodeEntryHandler();
            handler.setReceiver(node);
            handler.setQuorum(quorum);
            handler.setLeaderShipStale(leaderShipStale);
            handler.setLog(log);
            handler.setReceiverTerm(newLeaderTerm);
            try {
              client.appendEntry(request, handler);
            } catch (Exception e) {
              logger.warn("{} cannot append log to node {}", name, node, e);
            }
          }
        }
      }

      try {
        quorum.wait(ClusterConstant.CONNECTION_TIME_OUT_MS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }

    if (leaderShipStale.get()) {
      retireFromLeader(newLeaderTerm.get());
      return AppendLogResult.LEADERSHIP_STALE;
    }

    if (quorum.get() > 0) {
      return AppendLogResult.TIME_OUT;
    }

    return AppendLogResult.OK;
  }

  enum AppendLogResult {
    OK, TIME_OUT, LEADERSHIP_STALE
  }

  public AsyncClient connectNode(Node node) {
    if (node == null || node.equals(thisNode)) {
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

  void setThisNode(Node thisNode) {
    this.thisNode = thisNode;
    allNodes.add(thisNode);
  }

  public void setLastCatchUpResponseTime(
      Map<Node, Long> lastCatchUpResponseTime) {
    this.lastCatchUpResponseTime = lastCatchUpResponseTime;
  }

  public void setCharacter(NodeCharacter character) {
    logger.info("{} has become a {}", name, character);
    this.character = character;
  }

  public NodeCharacter getCharacter() {
    return character;
  }

  public AtomicLong getTerm() {
    return term;
  }

  public long getLastHeartBeatReceivedTime() {
    return lastHeartBeatReceivedTime;
  }

  public Node getLeader() {
    return leader;
  }

  public void setTerm(AtomicLong term) {
    this.term = term;
  }

  public void setLastHeartBeatReceivedTime(long lastHeartBeatReceivedTime) {
    this.lastHeartBeatReceivedTime = lastHeartBeatReceivedTime;
  }

  public void setLeader(Node leader) {
    this.leader = leader;
  }

  public Node getThisNode() {
    return thisNode;
  }

  public Collection<Node> getAllNodes() {
    return allNodes;
  }

  public Map<Node, Long> getLastCatchUpResponseTime() {
    return lastCatchUpResponseTime;
  }


  public void processValidHeartbeatResp(HeartBeatResponse response, Node receiver) {

  }

  /**
   * The actions performed when the node wins in an election (becoming a leader).
   */
  public void onElectionWins() {

  }
  void processValidHeartbeatReq(HeartBeatRequest request, HeartBeatResponse response,
      long leaderTerm) {

  }

  public void retireFromLeader(long newTerm) {
    synchronized (term) {
      long currTerm = term.get();
      // confirm that the heartbeat of the new leader hasn't come
      if (currTerm < newTerm) {
        term.set(newTerm);
        setCharacter(NodeCharacter.FOLLOWER);
        setLeader(null);
        setLastHeartBeatReceivedTime(System.currentTimeMillis());
      }
    }
  }

  long processElectionRequest(ElectionRequest electionRequest) {
    // reject the election if one of the four holds:
    // 1. the term of the candidate is no bigger than the voter's
    // 2. the lastLogIndex of the candidate is smaller than the voter's
    // 3. the lastLogIndex of the candidate equals to the voter's but its lastLogTerm is
    // smaller than the voter's
    long thatTerm = electionRequest.getTerm();
    long thatLastLogId = electionRequest.getLastLogIndex();
    long thatLastLogTerm = electionRequest.getLastLogTerm();
    logger.info("{} received an election request, term:{}, metaLastLogId:{}, metaLastLogTerm:{}",
        name, thatTerm,
        thatLastLogId, thatLastLogTerm);

    long lastLogIndex = logManager.getLastLogIndex();
    long lastLogTerm = logManager.getLastLogTerm();

    synchronized (term) {
      long thisTerm = term.get();
      long resp = verifyElector(thisTerm, lastLogIndex, lastLogTerm, thatTerm, thatLastLogId, thatLastLogTerm);
      if (resp == Response.RESPONSE_AGREE) {
        term.set(thatTerm);
        setCharacter(NodeCharacter.FOLLOWER);
        lastHeartBeatReceivedTime = System.currentTimeMillis();
        leader = electionRequest.getElector();
        // interrupt election
        term.notifyAll();
      }
      return resp;
    }
  }

  long verifyElector(long thisTerm, long thisLastLogIndex, long thisLastLogTerm,
      long thatTerm, long thatLastLogId, long thatLastLogTerm) {
    long response;
    if (thatTerm <= thisTerm
        || thatLastLogTerm < thisLastLogTerm
        || (thatLastLogTerm == thisLastLogTerm && thatLastLogId < thisLastLogIndex)) {
      logger.debug("{} rejected an election request, term:{}/{}, logIndex:{}/{}, logTerm:{}/{}",
          name, thatTerm, thisTerm, thatLastLogId, thisLastLogIndex, thatLastLogTerm,
          thisLastLogTerm);
      response = Response.RESPONSE_LOG_MISMATCH;
    } else {
      logger.debug("{} accepted an election request, term:{}/{}, logIndex:{}/{}, logTerm:{}/{}",
          name, thatTerm, thisTerm, thatLastLogId, thisLastLogIndex, thatLastLogTerm,
          thisLastLogTerm);
      response = Response.RESPONSE_AGREE;
    }
    return response;
  }

  /**
   * Update the followers' log by sending logs whose index >= followerLastLogIndex to the follower.
   * If some of the logs are not in memory, also send the snapshot.
   * @param follower
   * @param followerLastLogIndex
   */
  public void catchUp(Node follower, long followerLastLogIndex) {
    // for one follower, there is at most one ongoing catch-up
    synchronized (follower) {
      // check if the last catch-up is still ongoing
      Long lastCatchupResp = lastCatchUpResponseTime.get(follower);
      if (lastCatchupResp != null
          && System.currentTimeMillis() - lastCatchupResp < ClusterConstant.CONNECTION_TIME_OUT_MS) {
        logger.debug("{}: last catch up of {} is ongoing", name, follower);
        return;
      } else {
        // record the start of the catch-up
        lastCatchUpResponseTime.put(follower, System.currentTimeMillis());
      }
    }
    if (followerLastLogIndex == -1) {
      // if the follower does not have any logs, send from the first one
      followerLastLogIndex = 0;
    }

    AsyncClient client = connectNode(follower);
    if (client != null) {
      List<Log> logs;
      boolean allLogsValid;
      Snapshot snapshot = null;
      synchronized (logManager) {
        allLogsValid = logManager.logValid(followerLastLogIndex);
        logs = logManager.getLogs(followerLastLogIndex, logManager.getLastLogIndex());
        if (!allLogsValid) {
          snapshot = logManager.getSnapshot();
        }
      }

      if (allLogsValid) {
        if (logger.isDebugEnabled()) {
          logger.debug("{} makes {} catch up with {} cached logs", name, follower, logs.size());
        }
        catchUpService.submit(new LogCatchUpTask(logs, follower, this));
      } else {
        logger.debug("{}: Logs in {} are too old, catch up with snapshot", name, follower);
        catchUpService.submit(new SnapshotCatchUpTask(logs, snapshot, follower, this));
      }
    } else {
      lastCatchUpResponseTime.remove(follower);
      logger.warn("{}: Catch-up failed: node {} is currently unavailable", name, follower);
    }
  }

  public String getName() {
    return name;
  }

  TSStatus forwardPlan(PhysicalPlan plan, PartitionGroup group) {
    for (Node node : group) {
      TSStatus status = forwardPlan(plan, node);
      if (status != StatusUtils.TIME_OUT) {
        return status;
      }
    }
    return StatusUtils.TIME_OUT;
  }

  /**
   *
   * @return the header of the data raft group or null if this is in a meta group.
   */
  public Node getHeader() {
    return null;
  }

  TSStatus forwardPlan(PhysicalPlan plan, Node node) {
    if (character != NodeCharacter.FOLLOWER || leader == null) {
      return StatusUtils.NO_LEADER;
    }

    logger.info("{}: Forward {} to leader {}", name, plan, leader);

    AsyncClient client = connectNode(leader);
    if (client != null) {
      ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
      DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream);
      try {
        plan.serializeTo(dataOutputStream);
        AtomicReference<TSStatus> status = new AtomicReference<>();
        ExecutNonQueryReq req = new ExecutNonQueryReq();
        req.setPlanBytes(byteArrayOutputStream.toByteArray());
        if (getHeader() != null) {
          req.setHeader(getHeader());
        }
        synchronized (status) {
          client.executeNonQueryPlan(req, new ForwardPlanHandler(status, plan, node));
          status.wait(ClusterConstant.CONNECTION_TIME_OUT_MS);
        }
        return status.get() == null ? StatusUtils.TIME_OUT : status.get();
      } catch (IOException | TException e) {
        TSStatus status = StatusUtils.INTERNAL_ERROR.deepCopy();
        status.getStatusType().setMessage(e.getMessage());
        return status;
      } catch (InterruptedException e) {
        return StatusUtils.TIME_OUT;
      }
    }
    return StatusUtils.TIME_OUT;
  }

  TSStatus processPlanLocally(PhysicalPlan plan) {
    synchronized (logManager) {
      PhysicalPlanLog log = new PhysicalPlanLog();
      log.setCurrLogTerm(getTerm().get());
      log.setPreviousLogIndex(logManager.getLastLogIndex());
      log.setPreviousLogTerm(logManager.getLastLogTerm());
      log.setCurrLogIndex(logManager.getLastLogIndex() + 1);

      log.setPlan(plan);
      logManager.appendLog(log);

      logger.debug("{}: Send plan {} to other nodes", name, plan);
      AppendLogResult result = sendLogToFollowers(log, allNodes.size() - 1);

      switch (result) {
        case OK:
          logger.debug("{}: Plan {} is accepted", name, plan);
          try {
            logManager.commitLog(log);
          } catch (QueryProcessException e) {
            logger.info("{}: The log {} is not successfully applied, reverting", name, log, e);
            logManager.removeLastLog();
            TSStatus status = StatusUtils.EXECUTE_STATEMENT_ERROR.deepCopy();
            status.getStatusType().setMessage(e.getMessage());
            return status;
          }
          return StatusUtils.OK;
        case TIME_OUT:
          logger.debug("{}: Plan {} timed out", name, plan);
          logManager.removeLastLog();
          return StatusUtils.TIME_OUT;
        case LEADERSHIP_STALE:
        default:
          logManager.removeLastLog();
      }
    }
    return null;
  }

  public void executeNonQueryPlan(ExecutNonQueryReq request,
      AsyncMethodCallback<TSStatus> resultHandler) {
    if (character != NodeCharacter.LEADER) {
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
      PhysicalPlan plan = PhysicalPlan.Factory.create(request.planBytes);
      logger.debug("{}: Received a plan {}", name, plan);
      resultHandler.onComplete(executeNonQuery(plan));
    } catch (Exception e) {
      resultHandler.onError(e);
    }
  }

  /**
   * Request and check the leader's commitId to see whether this node has caught up, if not wait
   * until this node catches up.
   * @return true if the node has caught up, false otherwise
   */
  boolean syncLeader() {
    if (character == NodeCharacter.LEADER) {
      return true;
    }
    if (leader == null) {
      return false;
    }
    logger.debug("{}: try synchronizing with leader {}", name, leader);
    long startTime = System.currentTimeMillis();
    long waitedTime = 0;
    AtomicLong commitIdResult = new AtomicLong(Long.MAX_VALUE);
    while (waitedTime < ClusterConstant.SYNC_LEADER_MAX_WAIT_MS) {
      AsyncClient client = connectNode(leader);
      if (client == null) {
        return false;
      }
      try {
        synchronized (commitIdResult) {
          client.requestCommitIndex(getHeader(), new RequestCommitIdHandler(leader, commitIdResult));
          commitIdResult.wait(ClusterConstant.SYNC_LEADER_MAX_WAIT_MS);
        }
        long leaderCommitId = commitIdResult.get();
        long localCommitId = logManager.getCommitLogIndex();
        logger.debug("{}: synchronizing commitIndex {}/{}", name, localCommitId, leaderCommitId);
        if (leaderCommitId <= localCommitId) {
          // this node has caught up
          return true;
        }
        // wait for next heartbeat to catch up
        waitedTime = System.currentTimeMillis() - startTime;
        Thread.sleep(ClusterConstant.HEART_BEAT_INTERVAL_MS);
      } catch (TException | InterruptedException e) {
        logger.error("{}: Cannot request commit index from {}", name, leader, e);
      }
    }
    return false;
  }

  abstract TSStatus executeNonQuery(PhysicalPlan plan);

  @Override
  public void requestCommitIndex(Node header, AsyncMethodCallback<Long> resultHandler) {
    if (character == NodeCharacter.LEADER) {
      resultHandler.onComplete(logManager.getCommitLogIndex());
      return;
    }
    AsyncClient client = connectNode(leader);
    if (client == null) {
      resultHandler.onError(new LeaderUnknownException());
      return;
    }
    try {
      client.requestCommitIndex(header, resultHandler);
    } catch (TException e) {
      resultHandler.onError(e);
    }
  }
}
