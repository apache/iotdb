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
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
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
import org.apache.iotdb.cluster.config.ClusterDescriptor;
import org.apache.iotdb.cluster.exception.LeaderUnknownException;
import org.apache.iotdb.cluster.exception.UnknownLogTypeException;
import org.apache.iotdb.cluster.log.Log;
import org.apache.iotdb.cluster.log.LogManager;
import org.apache.iotdb.cluster.log.LogParser;
import org.apache.iotdb.cluster.log.Snapshot;
import org.apache.iotdb.cluster.log.catchup.LogCatchUpTask;
import org.apache.iotdb.cluster.log.catchup.SnapshotCatchUpTask;
import org.apache.iotdb.cluster.log.logtypes.PhysicalPlanLog;
import org.apache.iotdb.cluster.rpc.thrift.AppendEntriesRequest;
import org.apache.iotdb.cluster.rpc.thrift.AppendEntryRequest;
import org.apache.iotdb.cluster.rpc.thrift.ElectionRequest;
import org.apache.iotdb.cluster.rpc.thrift.ExecutNonQueryReq;
import org.apache.iotdb.cluster.rpc.thrift.HeartbeatRequest;
import org.apache.iotdb.cluster.rpc.thrift.HeartbeatResponse;
import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.cluster.rpc.thrift.RaftService;
import org.apache.iotdb.cluster.rpc.thrift.RaftService.AsyncClient;
import org.apache.iotdb.cluster.server.NodeCharacter;
import org.apache.iotdb.cluster.server.RaftServer;
import org.apache.iotdb.cluster.server.Response;
import org.apache.iotdb.cluster.server.handlers.caller.AppendNodeEntryHandler;
import org.apache.iotdb.cluster.server.handlers.caller.GenericHandler;
import org.apache.iotdb.cluster.server.handlers.forwarder.ForwardPlanHandler;
import org.apache.iotdb.cluster.utils.StatusUtils;
import org.apache.iotdb.db.exception.metadata.PathAlreadyExistException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
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
public abstract class RaftMember implements RaftService.AsyncIface {

  ClusterConfig config = ClusterDescriptor.getINSTANCE().getConfig();
  private static final Logger logger = LoggerFactory.getLogger(RaftMember.class);

  // the name of the member, to distinguish several members from the logs
  String name;

  // to choose nodes to join cluster request randomly
  Random random = new Random();
  protected Node thisNode;
  // the nodes known by this node
  protected volatile List<Node> allNodes;

  AtomicLong term = new AtomicLong(0);
  volatile NodeCharacter character = NodeCharacter.ELECTOR;
  volatile Node leader;
  volatile long lastHeartbeatReceivedTime;

  // the raft logs are all stored and maintained in the log manager
  LogManager logManager;

  // the single thread pool that runs the heartbeat thread
  ExecutorService heartBeatService;

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

  // when the header of the group is removed from the cluster, the members of the group should no
  // longer accept writes, but they still can be read candidates for weak consistency reads and
  // provide snapshots for the new holders
  volatile boolean readOnly = false;

  public RaftMember() {
  }

  RaftMember(String name, ClientPool pool) {
    this.name = name;
    this.clientPool = pool;
  }

  /**
   * Start the heartbeat thread and the catch-up thread pool. Calling the method twice does not
   * induce side effects.
   *
   * @throws TTransportException
   */
  public void start() throws TTransportException {
    if (heartBeatService != null) {
      return;
    }

    heartBeatService =
        Executors.newSingleThreadScheduledExecutor(r -> new Thread(r,
            name + "-HeartbeatThread@" + System.currentTimeMillis()));
    catchUpService = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
    logger.info("{} started", name);
  }

  public LogManager getLogManager() {
    return logManager;
  }

  /**
   * Stop the heartbeat thread and the catch-up thread pool. Calling the method twice does not
   * induce side effects.
   *
   * @throws TTransportException
   */
  public void stop() {
    if (heartBeatService == null) {
      return;
    }

    heartBeatService.shutdownNow();
    catchUpService.shutdownNow();
    catchUpService = null;
    heartBeatService = null;
    logger.info("{} stopped", name);
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
  public void sendHeartbeat(HeartbeatRequest request, AsyncMethodCallback resultHandler) {
    logger.trace("{} received a heartbeat", name);
    synchronized (term) {
      long thisTerm = term.get();
      long leaderTerm = request.getTerm();
      HeartbeatResponse response = new HeartbeatResponse();

      if (leaderTerm < thisTerm) {
        // a leader with term lower than this node is invalid, send it the local term to inform this
        response.setTerm(thisTerm);
        if (logger.isTraceEnabled()) {
          logger.trace("{} received a heartbeat from a stale leader {}", name, request.getLeader());
        }
      } else {
        // the heartbeat comes from a valid leader, process it with the sub-class logic
        processValidHeartbeatReq(request, response);

        response.setTerm(Response.RESPONSE_AGREE);
        // tell the leader who I am in case of catch-up
        response.setFollower(thisNode);
        // TODO-CLuster: the log being sent should be chosen wisely instead of the last log, so that
        //  the leader would be able to find the last match log
        response.setLastLogIndex(logManager.getLastLogIndex());
        response.setLastLogTerm(logManager.getLastLogTerm());

        // The term of the last log needs to be the same with leader's term in order to preserve
        // safety, otherwise it may come from an invalid leader and is not committed
        if (logManager.getLastLogTerm() == leaderTerm) {
          synchronized (syncLock) {
            logManager.commitLog(request.getCommitLogIndex());
            syncLock.notifyAll();
          }
        }
        // if the log is not consistent, the commitment will be blocked until the leader makes the
        // node catch up

        term.set(leaderTerm);
        setLeader(request.getLeader());
        if (character != NodeCharacter.FOLLOWER) {
          setCharacter(NodeCharacter.FOLLOWER);
        }
        setLastHeartbeatReceivedTime(System.currentTimeMillis());
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
      if (electionRequest.getElector().equals(leader)) {
        // always agree with the last leader
        resultHandler.onComplete(Response.RESPONSE_AGREE);
        return;
      }

      // check the log status of the elector
      long response = processElectionRequest(electionRequest);
      logger.info("{} sending response {} to the elector {}", name, response,
          electionRequest.getElector());
      resultHandler.onComplete(response);
    }
  }

  /**
   * Check the term of the AppednEntryRequest. The term checked is the term of the leader, not the
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
      } else if (leaderTerm > localTerm) {
        term.set(leaderTerm);
        localTerm = leaderTerm;
        if (character != NodeCharacter.FOLLOWER) {
          setCharacter(NodeCharacter.FOLLOWER);
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
  private long appendEntry(Log log) {
    long resp;
    synchronized (logManager) {
      // TODO-Cluster: instead of the last log, we should find the log that matches the previous
      //  log of "log", discard all logs behind it and then append the new log. If such a matched
      //  log cannot be found, the follower should help the leader position the last matched log.
      Log lastLog = logManager.getLastLog();
      long previousLogIndex = log.getPreviousLogIndex();
      long previousLogTerm = log.getPreviousLogTerm();

      if (logManager.getLastLogIndex() == previousLogIndex
          && logManager.getLastLogTerm() == previousLogTerm) {
        // the incoming log points to the local last log, append it
        logManager.appendLog(log);
        if (logger.isDebugEnabled()) {
          logger.debug("{} append a new log {}", name, log);
        }
        resp = Response.RESPONSE_AGREE;
      } else if (lastLog != null && lastLog.getPreviousLogIndex() == previousLogIndex
          && lastLog.getCurrLogTerm() <= log.getCurrLogTerm()) {
        /* <pre>
                                       +------+
                                     .'|      | new coming log
                                   .'  +------+
                                 .'
        +--------+     +--------+      +------+
        |        |-----|        |------|      | so called latest log   (local)
        +--------+     +--------+      +------+
        </pre>
        */
        // the incoming log points to the previous log of the local last log, and its term is
        // bigger than or equals to the local last log's, replace the local last log with it
        // because the local latest log is invalid.
        logManager.replaceLastLog(log);
        logger.debug("{} replaced the last log with {}", name, log);
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
            log.getCurrLogTerm(), log.getCurrLogIndex(),
            previousLogTerm, previousLogIndex);
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
      resultHandler.onComplete(appendEntry(log));
      logger.debug("{} AppendEntryRequest of {} completed", name, log);
    } catch (UnknownLogTypeException e) {
      resultHandler.onError(e);
    }
  }

  @Override
  public void appendEntries(AppendEntriesRequest request, AsyncMethodCallback resultHandler) {
    //TODO-Cluster#354: implement
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
    if (getHeader() != null) {
      // data groups use header to find a particular DataGroupMember
      request.setHeader(getHeader());
    }

    synchronized (voteCounter) {
      // synchronized: avoid concurrent modification
      synchronized (allNodes) {
        for (Node node : allNodes) {
          AsyncClient client = connectNode(node);
          if (client != null) {
            AppendNodeEntryHandler handler = new AppendNodeEntryHandler();
            handler.setReceiver(node);
            handler.setVoteCounter(voteCounter);
            handler.setLeaderShipStale(leaderShipStale);
            handler.setLog(log);
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

      try {
        voteCounter.wait(RaftServer.connectionTimeoutInMS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        logger.warn("Unexpected interruption when sending a log", e);
      }
    }

    // some node has a larger term than the local node, this node is no longer a valid leader
    if (leaderShipStale.get()) {
      retireFromLeader(newLeaderTerm.get());
      return AppendLogResult.LEADERSHIP_STALE;
    }

    // cannot get enough agreements within a certain amount of time
    if (voteCounter.get() > 0) {
      return AppendLogResult.TIME_OUT;
    }

    return AppendLogResult.OK;
  }

  enum AppendLogResult {
    OK, TIME_OUT, LEADERSHIP_STALE
  }

  /**
   * Get an asynchronous thrift client to the given node.
   *
   * @param node
   * @return an asynchronous thrift client or null if the caller tries to connect the local node.
   */
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

  public void setThisNode(Node thisNode) {
    this.thisNode = thisNode;
    allNodes.add(thisNode);
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

  public long getLastHeartbeatReceivedTime() {
    return lastHeartbeatReceivedTime;
  }

  public Node getLeader() {
    return leader;
  }

  public void setTerm(AtomicLong term) {
    this.term = term;
  }

  public void setLastHeartbeatReceivedTime(long lastHeartbeatReceivedTime) {
    this.lastHeartbeatReceivedTime = lastHeartbeatReceivedTime;
  }

  public void setLeader(Node leader) {
    if (!Objects.equals(leader, this.leader)) {
      logger.info("{} has become a follower of {}", getName(), leader);
      this.leader = leader;
    }
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


    /**
     * Sub-classes will add their own process of HeartBeatResponse in this method.
     * @param response
     * @param receiver
     */
    public void processValidHeartbeatResp (HeartbeatResponse response, Node receiver){

    }

    /**
     * The actions performed when the node wins in an election (becoming a leader).
     */
    public void onElectionWins () {

    }


      /**
       * Sub-classes will add their own process of HeartBeatRequest in this method.
       * @param request
       * @param response
       */
      void processValidHeartbeatReq (HeartbeatRequest request, HeartbeatResponse response){

      }

      /**
       * If "newTerm" is larger than the local term, give up the leadership, become a follower and
       * reset heartbeat timer.
       * @param newTerm
       */
      public void retireFromLeader ( long newTerm){
        synchronized (term) {
          long currTerm = term.get();
          // confirm that the heartbeat of the new leader hasn't come
          if (currTerm < newTerm) {
            term.set(newTerm);
            setCharacter(NodeCharacter.FOLLOWER);
            setLeader(null);
            setLastHeartbeatReceivedTime(System.currentTimeMillis());
          }
        }
      }

      /**
       * Verify the validity of an ElectionRequest, and make itself a follower of the elector if the
       * request is valid.
       * @param electionRequest
       * @return Response.RESPONSE_AGREE if the elector is valid or the local term if the elector has
       *   a smaller term or Response.RESPONSE_LOG_MISMATCH if the elector has older logs.
       */
      long processElectionRequest (ElectionRequest electionRequest){

        long thatTerm = electionRequest.getTerm();
        long thatLastLogId = electionRequest.getLastLogIndex();
        long thatLastLogTerm = electionRequest.getLastLogTerm();
        logger
            .info("{} received an election request, term:{}, metaLastLogId:{}, metaLastLogTerm:{}",
                name, thatTerm,
                thatLastLogId, thatLastLogTerm);

        long lastLogIndex = logManager.getLastLogIndex();
        long lastLogTerm = logManager.getLastLogTerm();

        synchronized (term) {
          long thisTerm = term.get();
          long resp = verifyElector(thisTerm, lastLogIndex, lastLogTerm, thatTerm, thatLastLogId,
              thatLastLogTerm);
          if (resp == Response.RESPONSE_AGREE) {
            term.set(thatTerm);
            setCharacter(NodeCharacter.FOLLOWER);
            lastHeartbeatReceivedTime = System.currentTimeMillis();
            leader = electionRequest.getElector();
            // interrupt election
            term.notifyAll();
          }
          return resp;
        }
      }

      /**
       *  Reject the election if one of the four holds:
       *   1. the term of the candidate is no bigger than the voter's
       *   2. the lastLogTerm of the candidate is smaller than the voter's
       *   3. the lastLogTerm of the candidate equals to the voter's but its lastLogIndex is
       *      smaller than the voter's
       *   Otherwise accept the election.
       * @param thisTerm
       * @param thisLastLogIndex
       * @param thisLastLogTerm
       * @param thatTerm
       * @param thatLastLogId
       * @param thatLastLogTerm
       * @return Response.RESPONSE_AGREE if the elector is valid or the local term if the elector has
       * a smaller term or Response.RESPONSE_LOG_MISMATCH if the elector has older logs.
       */
      long verifyElector ( long thisTerm, long thisLastLogIndex, long thisLastLogTerm,
      long thatTerm, long thatLastLogId, long thatLastLogTerm){
        long response;
        if (thatTerm <= thisTerm) {
          response = thisTerm;
          logger.debug("{} rejected an election request, term:{}/{}",
              name, thatTerm, thisTerm);
        } else if (thatLastLogTerm < thisLastLogTerm
            || (thatLastLogTerm == thisLastLogTerm && thatLastLogId < thisLastLogIndex)) {
          logger.debug("{} rejected an election request, logIndex:{}/{}, logTerm:{}/{}",
              name, thatLastLogId, thisLastLogIndex, thatLastLogTerm, thisLastLogTerm);
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
       * Update the followers' log by sending logs whose index >= followerLastMatchedLogIndex to the
       * follower. If some of the logs are not in memory, also send the snapshot.
       * <br>notice that if a part of data is in the snapshot, then it is not in the logs</>
       *
       * @param follower
       * @param followerLastLogIndex
       */
      public void catchUp (Node follower,long followerLastLogIndex){
        // TODO-Cluster: use lastMatchLogIndex instead of lastLogIndex
        // for one follower, there is at most one ongoing catch-up
        synchronized (follower) {
          // check if the last catch-up is still ongoing
          Long lastCatchupResp = lastCatchUpResponseTime.get(follower);
          if (lastCatchupResp != null
              && System.currentTimeMillis() - lastCatchupResp < RaftServer.connectionTimeoutInMS) {
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
            // check if the very first log has been snapshot
            allLogsValid = logManager.logValid(followerLastLogIndex);
            logs = logManager.getLogs(followerLastLogIndex, Long.MAX_VALUE);
            if (!allLogsValid) {
              // if the first log has been snapshot, the snapshot should also be sent to the
              // follower, otherwise some data will be missing
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

      public String getName () {
        return name;
      }

      /**
       * @return the header of the data raft group or null if this is in a meta group.
       */
      public Node getHeader () {
        return null;
      }

      /**
       <<<<<<< HEAD
       * Forward a plan to a node using the default client.
       *
       * @param plan
       * @param node
      =======
       * Forward a non-query plan to a node using the default client.
       * @param plan a non-query plan
       * @param node cannot be the local node
      >>>>>>> 378da6117d05a7e38d05fb46cb20b3023434c08c
       * @param header must be set for data group communication, set to null for meta group
       *               communication
       * @return a TSStatus indicating if the forwarding is successful.
       */
      TSStatus forwardPlan (PhysicalPlan plan, Node node, Node header){
        if (node == thisNode || node == null) {
          logger.debug("{}: plan {} has no where to be forwarded", name, plan);
          return StatusUtils.NO_LEADER;
        }

        logger.info("{}: Forward {} to node {}", name, plan, node);

        AsyncClient client = connectNode(node);
        if (client != null) {
          return forwardPlan(plan, client, node, header);
        }
        return StatusUtils.TIME_OUT;
      }

      /**
       * Forward a non-query plan to "receiver" using "client".
       * @param plan a non-query plan
       * @param client
       * @param receiver
       * @param header to determine which DataGroupMember of "receiver" will process the request.
       * @return a TSStatus indicating if the forwarding is successful.
       */
      TSStatus forwardPlan (PhysicalPlan plan, AsyncClient client, Node receiver, Node header){
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream);
        try {
          plan.serializeTo(dataOutputStream);
          AtomicReference<TSStatus> status = new AtomicReference<>();
          ExecutNonQueryReq req = new ExecutNonQueryReq();
          req.setPlanBytes(byteArrayOutputStream.toByteArray());
          if (header != null) {
            req.setHeader(header);
          }
          synchronized (status) {
            client.executeNonQueryPlan(req, new ForwardPlanHandler(status, plan, receiver));
            status.wait(RaftServer.connectionTimeoutInMS);
          }
          return status.get() == null ? StatusUtils.TIME_OUT : status.get();
        } catch (IOException | TException e) {
          TSStatus status = StatusUtils.INTERNAL_ERROR.deepCopy();
          status.setMessage(e.getMessage());
          logger
              .error("{}: encountered an error when forwarding {} to {}", name, plan, receiver, e);
          return status;
        } catch (InterruptedException e) {
          return StatusUtils.TIME_OUT;
        }
      }

      /**
       <<<<<<< HEAD
       * Only the group leader can call this method. Will commit the log locally and send it to
       * followers
       *
       =======
       * Create a log for "plan" and append it locally and to all followers.
       * Only the group leader can call this method.
       * Will commit the log locally and send it to followers
       >>>>>>> 378da6117d05a7e38d05fb46cb20b3023434c08c
       * @param plan
       * @return OK if over half of the followers accept the log or null if the leadership is lost
       * during the appending
       */
      TSStatus processPlanLocally (PhysicalPlan plan){
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
          logManager.appendLog(log);
        }

        if (appendLogInGroup(log)) {
          return StatusUtils.OK;
        }
        return null;
      }

      /**
       <<<<<<< HEAD
       * if the node is not a leader, will send it to the leader. Otherwise do it locally (whether to
       * send it to followers depends on the implementation of executeNonQuery()).
       *
       =======
       * Append a log to all followers in the group until half of them accept the log or the
       * leadership is lost.
       * @param log
       * @return true if the log is accepted by the quorum of the group, false otherwise
       */
      protected boolean appendLogInGroup (Log log){
        int retryTime = 0;
        retry:
        while (true) {
          logger.debug("{}: Send log {} to other nodes, retry times: {}", name, log, retryTime);
          AppendLogResult result = sendLogToFollowers(log, allNodes.size() / 2);
          switch (result) {
            case OK:
              logger.debug("{}: log {} is accepted", name, log);
              logManager.commitLog(log.getCurrLogIndex());
              return true;
            case TIME_OUT:
              logger.debug("{}: log {} timed out, retrying...", name, log);
              retryTime++;
              break;
            case LEADERSHIP_STALE:
              // abort the appending, the new leader will fix the local logs by catch-up
            default:
              break retry;
          }
        }
        return false;
      }

      /**
       * If the node is not a leader, the request will be sent to the leader or reports an error if
       * there is no leader.
       * Otherwise execute the plan locally (whether to send it to followers depends on the
       * type of the plan).
       >>>>>>> 378da6117d05a7e38d05fb46cb20b3023434c08c
       * @param request
       * @param resultHandler
       */
      public void executeNonQueryPlan (ExecutNonQueryReq request,
          AsyncMethodCallback < TSStatus > resultHandler){
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
      public boolean syncLeader () {
        if (character == NodeCharacter.LEADER) {
          return true;
        }
        if (leader == null) {
          // the leader has not been elected, we must assume the node falls behind
          return false;
        }
        logger.debug("{}: try synchronizing with the leader {}", name, leader);
        long startTime = System.currentTimeMillis();
        long waitedTime = 0;
        AtomicReference<Long> commitIdResult = new AtomicReference<>(Long.MAX_VALUE);
        while (waitedTime < RaftServer.syncLeaderMaxWaitMs) {
          AsyncClient client = connectNode(leader);
          if (client == null) {
            // cannot connect to the leader
            return false;
          }
          try {
            synchronized (commitIdResult) {
              client.requestCommitIndex(getHeader(), new GenericHandler<>(leader, commitIdResult));
              commitIdResult.wait(RaftServer.syncLeaderMaxWaitMs);
            }
            long leaderCommitId = commitIdResult.get();
            long localCommitId = logManager.getCommitLogIndex();
            logger
                .debug("{}: synchronizing commitIndex {}/{}", name, localCommitId, leaderCommitId);
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
              syncLock.wait(RaftServer.heartBeatIntervalMs);
            }
          } catch (TException | InterruptedException e) {
            logger.error("{}: Cannot request commit index from {}", name, leader, e);
          }
        }
        return false;
      }

      /**
       * Execute a non-query plan.
       * @param plan a non-query plan.
       * @return A TSStatus indicating the execution result.
       */
      abstract TSStatus executeNonQuery (PhysicalPlan plan);

      /**
       * Tell the requester the current commit index if the local node is the leader of the group
       * headed by header. Or forward it to the leader. Otherwise report an error.
       * @param header to determine the DataGroupMember in data groups
       * @param resultHandler
       */
      @Override
      public void requestCommitIndex (Node header, AsyncMethodCallback < Long > resultHandler){
        if (character == NodeCharacter.LEADER) {
          resultHandler.onComplete(logManager.getCommitLogIndex());
          return;
        }
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
       * An ftp-like interface that is used for a node to pull chunks of files like TsFiles.
       * @param filePath
       * @param offset
       * @param length
       * @param header to determine the DataGroupMember in data groups
       * @param resultHandler
       */
      @Override
      public void readFile (String filePath,long offset, int length, Node header,
          AsyncMethodCallback < ByteBuffer > resultHandler){
        try (BufferedInputStream bufferedInputStream =
            new BufferedInputStream(new FileInputStream(filePath))) {
          bufferedInputStream.skip(offset);
          byte[] bytes = new byte[length];
          ByteBuffer result = ByteBuffer.wrap(bytes);
          int len = bufferedInputStream.read(bytes);
          result.limit(Math.max(len, 0));

          resultHandler.onComplete(result);
        } catch (IOException e) {
          resultHandler.onError(e);
        }
      }

      public void setReadOnly () {
        synchronized (logManager) {
          readOnly = true;
        }
      }

      public void setAllNodes (List < Node > allNodes) {
        this.allNodes = allNodes;
      }
    }
