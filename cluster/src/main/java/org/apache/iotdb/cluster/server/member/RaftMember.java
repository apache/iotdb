/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at      http://www.apache.org/licenses/LICENSE-2.0  Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the License for the specific language governing permissions and limitations under the License.
 */

package org.apache.iotdb.cluster.server.member;

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
import org.apache.iotdb.cluster.config.ClusterConfig;
import org.apache.iotdb.cluster.config.ClusterDescriptor;
import org.apache.iotdb.cluster.exception.UnknownLogTypeException;
import org.apache.iotdb.cluster.log.Log;
import org.apache.iotdb.cluster.log.LogManager;
import org.apache.iotdb.cluster.log.LogParser;
import org.apache.iotdb.cluster.log.Snapshot;
import org.apache.iotdb.cluster.rpc.thrift.AppendEntriesRequest;
import org.apache.iotdb.cluster.rpc.thrift.AppendEntryRequest;
import org.apache.iotdb.cluster.rpc.thrift.ElectionRequest;
import org.apache.iotdb.cluster.rpc.thrift.HeartBeatRequest;
import org.apache.iotdb.cluster.rpc.thrift.HeartBeatResponse;
import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.cluster.rpc.thrift.RaftService;
import org.apache.iotdb.cluster.rpc.thrift.RaftService.AsyncClient;
import org.apache.iotdb.cluster.server.LogCatchUpTask;
import org.apache.iotdb.cluster.server.NodeCharacter;
import org.apache.iotdb.cluster.server.Response;
import org.apache.iotdb.cluster.server.handlers.caller.AppendNodeEntryHandler;
import org.apache.iotdb.db.qp.QueryProcessor;
import org.apache.thrift.async.AsyncMethodCallback;
import org.apache.thrift.transport.TNonblockingSocket;
import org.apache.thrift.transport.TNonblockingTransport;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class RaftMember implements RaftService.AsyncIface {

  ClusterConfig config = ClusterDescriptor.getINSTANCE().getConfig();
  private static final Logger logger = LoggerFactory.getLogger(RaftMember.class);

  static final int CONNECTION_TIME_OUT_MS = 20 * 1000;

  Random random = new Random();
  Node thisNode;
  List<Node> allNodes;

  NodeCharacter character = NodeCharacter.ELECTOR;
  AtomicLong term = new AtomicLong(0);
  Node leader;
  private long lastHeartBeatReceivedTime;

  LogManager logManager;

  ExecutorService heartBeatService;
  private ExecutorService catchUpService;

  private Map<Node, Long> lastCatchUpResponseTime = new ConcurrentHashMap<>();

  QueryProcessor queryProcessor;

  public void start() throws TTransportException {
    if (heartBeatService != null) {
      return;
    }

    heartBeatService =
        Executors.newSingleThreadScheduledExecutor(r -> new Thread(r, "HeartBeatThread"));
    catchUpService = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
  }

  abstract void initLogManager();

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
    logger.debug("Received a heartbeat");
    synchronized (term) {
      long thisTerm = term.get();
      long leaderTerm = request.getTerm();
      HeartBeatResponse response = new HeartBeatResponse();

      if (leaderTerm < thisTerm) {
        // the leader is invalid
        response.setTerm(thisTerm);
        if (logger.isDebugEnabled()) {
          logger.debug("Received a heartbeat from a stale leader {}", request.getLeader());
        }
      } else {
        processValidHeartbeatReq(request, response, leaderTerm);
      }
      resultHandler.onComplete(response);
    }
  }

  @Override
  public void startElection(ElectionRequest electionRequest, AsyncMethodCallback resultHandler) {
    long thatTerm = electionRequest.getTerm();
    long thatLastLogId = electionRequest.getLastLogIndex();
    long thatLastLogTerm = electionRequest.getLastLogTerm();
    logger.info("Received an election request, term:{}, lastLogId:{}, lastLogTerm:{}", thatTerm,
        thatLastLogId, thatLastLogTerm);



    synchronized (term) {
      long response;
      long thisTerm = term.get();
      if (character != NodeCharacter.ELECTOR) {
        // only elector votes
        resultHandler.onComplete(thisTerm);
        return;
      }
      // reject the election if one of the four holds:
      // 1. the term of the candidate is no bigger than the voter's
      // 2. the lastLogIndex of the candidate is smaller than the voter's
      // 3. the lastLogIndex of the candidate equals to the voter's but its lastLogTerm is
      // smaller than the voter's
      long lastLogIndex = logManager.getLastLogIndex();
      long lastLogTerm = logManager.getLastLogTerm();
      if (thatTerm <= thisTerm
          || thatLastLogId < lastLogIndex
          || (thatLastLogId == lastLogIndex && thatLastLogTerm < lastLogTerm)) {
        logger.debug("Rejected an election request, term:{}/{}, logIndex:{}/{}, logTerm:{}/{}",
            thatTerm, thisTerm, thatLastLogId, lastLogIndex, thatLastLogTerm, lastLogTerm);
        response = thisTerm;
      } else {
        logger.debug("Accepted an election request, term:{}/{}, logIndex:{}/{}, logTerm:{}/{}",
            thatTerm, thisTerm, thatLastLogId, lastLogIndex, thatLastLogTerm, lastLogTerm);
        term.set(thatTerm);
        response = Response.RESPONSE_AGREE;
        setCharacter(NodeCharacter.FOLLOWER);
        lastHeartBeatReceivedTime = System.currentTimeMillis();
        leader = null;
      }
      logger.info("Sending response to the elector");
      resultHandler.onComplete(response);
    }
  }

  @Override
  public void appendEntry(AppendEntryRequest request, AsyncMethodCallback resultHandler) {

    logger.debug("Received an AppendEntryRequest");
    long leaderTerm = request.getTerm();
    long localTerm;

    synchronized (term) {
      // if the request comes before the heartbeat arrives, the local term may be smaller than the
      // leader term
      localTerm = term.get();
      if (leaderTerm < localTerm) {
        logger.debug("Rejected the AppendEntryRequest for term: {}/{}", leaderTerm, localTerm);
        resultHandler.onComplete(localTerm);
        return;
      } else if (leaderTerm > localTerm) {
        term.set(leaderTerm);
        localTerm = leaderTerm;
        if (character != NodeCharacter.FOLLOWER) {
          setCharacter(NodeCharacter.FOLLOWER);
        }
      }
    }

    logger.debug("Accepted the AppendEntryRequest for term: {}", localTerm);
    try {
      Log log = LogParser.getINSTANCE().parse(request.entry);
      synchronized (logManager) {
        Log lastLog = logManager.getLastLog();
        long previousLogIndex = log.getPreviousLogIndex();
        long previousLogTerm = log.getPreviousLogTerm();

        if (lastLog == null ||
            lastLog.getCurrLogIndex() == previousLogIndex && lastLog.getCurrLogTerm() == previousLogTerm) {
          // the incoming log points to the local last log, append it
          logManager.appendLog(log);
          resultHandler.onComplete(Response.RESPONSE_AGREE);
          logger.debug("Append a new log {}, new term:{}, new index:{}", log, localTerm,
              logManager.getLastLogIndex());
        } else if (lastLog.getPreviousLogIndex() == previousLogIndex
            && lastLog.getPreviousLogTerm() <= previousLogTerm) {
          // the incoming log points to the previous log of the local last log, and its term is
          // bigger than or equals to the local last log's, replace the local last log with it
          logManager.replaceLastLog(log);
          resultHandler.onComplete(Response.RESPONSE_AGREE);
          logger.debug("Replaced the last log with {}, new term:{}, new index:{}", log,
              previousLogTerm, previousLogIndex);
        } else {
          // the incoming log points to an illegal position, reject it
          resultHandler.onComplete(Response.RESPONSE_LOG_MISMATCH);
          logger.debug("Cannot append the log because the last log does not match, "
                  + "local:term[{}],index[{}],previousTerm[{}],previousIndex[{}], "
                  + "request:term[{}],index[{}],previousTerm[{}],previousIndex[{}]",
              lastLog.getCurrLogTerm(), lastLog.getCurrLogIndex(),
              lastLog.getPreviousLogTerm(), lastLog.getPreviousLogIndex(),
              log.getCurrLogTerm(), log.getPreviousLogIndex(),
              previousLogTerm, previousLogIndex);
        }
      }
    } catch (UnknownLogTypeException e) {
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
  AppendLogResult sendLogToFollowers(Log log, int requiredQuorum) {
    if (requiredQuorum < 0) {
      return sendLogToFollowers(log, new AtomicInteger(allNodes.size() / 2));
    } else {
      return sendLogToFollowers(log ,new AtomicInteger(requiredQuorum));
    }
  }

  // synchronized: logs are serialized
  private synchronized AppendLogResult sendLogToFollowers(Log log, AtomicInteger quorum) {
    logger.debug("Sending a log to followers: {}", log);

    AtomicBoolean leaderShipStale = new AtomicBoolean(false);

    AppendEntryRequest request = new AppendEntryRequest();
    request.setTerm(term.get());
    request.setEntry(log.serialize());

    // synchronized: avoid concurrent modification
    synchronized (allNodes) {
      for (Node node : allNodes) {
        AsyncClient client = connectNode(node);
        if (client != null) {
          AppendNodeEntryHandler handler = new AppendNodeEntryHandler(this);
          handler.setFollower(node);
          handler.setQuorum(quorum);
          handler.setLeaderShipStale(leaderShipStale);
          handler.setLog(log);
          try {
            client.appendEntry(request, handler);
          } catch (Exception e) {
            logger.warn("Cannot append log to node {}", node, e);
          }
        }
      }
    }

    synchronized (quorum) {
      if (quorum.get() != 0 && !leaderShipStale.get()) {
        try {
          quorum.wait(CONNECTION_TIME_OUT_MS);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          return AppendLogResult.TIME_OUT;
        }
      }
    }

    if (leaderShipStale.get()) {
      return AppendLogResult.LEADERSHIP_STALE;
    }
    return AppendLogResult.OK;
  }

  enum AppendLogResult {
    OK, TIME_OUT, LEADERSHIP_STALE
  }

  /**
   * Update the followers' log by sending logs whose index >= followerLastLogIndex to the follower.
   * If some of the logs are not in memory, also send the snapshot.
   * @param follower
   * @param followerLastLogIndex
   */
  private void catchUp(Node follower, long followerLastLogIndex) {
    // for one follower, there is at most one ongoing catch-up
    synchronized (follower) {
      // check if the last catch-up is still ongoing
      Long lastCatchupResp = lastCatchUpResponseTime.get(follower);
      if (lastCatchupResp != null
          && System.currentTimeMillis() - lastCatchupResp < CONNECTION_TIME_OUT_MS) {
        logger.debug("Last catch up of {} is ongoing", follower);
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
          logger.debug("Make {} catch up with {} cached logs", follower, logs.size());
        }
        catchUpService.submit(new LogCatchUpTask(logs, follower, this));
      } else {
        logger.debug("Logs in {} are too old, catch up with snapshot", follower);
        // TODO-Cluster doCatchUp(logs, node, snapshot);
      }
    } else {
      lastCatchUpResponseTime.remove(follower);
      logger.warn("Catch-up failed: node {} is currently unavailable", follower);
    }
  }

  abstract AsyncClient getAsyncClient(TNonblockingTransport transport);

  public AsyncClient connectNode(Node node) {
    if (node.equals(thisNode)) {
      return null;
    }

    AsyncClient client = null;
    try {
      client = getAsyncClient(new TNonblockingSocket(node.getIp(), node.getPort(),
          CONNECTION_TIME_OUT_MS));
    } catch (IOException e) {
      logger.warn("Cannot connect to node {}", node, e);
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
    logger.info("This node has become a {}", character);
    this.character = character;
  }

  public NodeCharacter getCharacter() {
    return character;
  }

  public AtomicLong getTerm() {
    return term;
  }

  public LogManager getLogManager() {
    return logManager;
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

  public void setLogManager(LogManager logManager) {
    this.logManager = logManager;
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
    Node follower = response.getFollower();
    long lastLogIdx = response.getLastLogIndex();
    long lastLogTerm = response.getLastLogTerm();
    long localLastLogIdx = getLogManager().getLastLogIndex();
    long localLastLogTerm = getLogManager().getLastLogTerm();
    logger.debug("Node {} is still alive, log index: {}/{}, log term: {}/{}", follower, lastLogIdx
        ,localLastLogIdx, lastLogTerm, localLastLogTerm);

    if (localLastLogIdx > lastLogIdx ||
        lastLogIdx == localLastLogIdx && localLastLogTerm > lastLogTerm) {
      catchUp(follower, lastLogIdx);
    }
  }

  /**
   * The actions performed when the node wins in an election (becoming a leader).
   */
  public void onElectionWins() {

  }

  void processValidHeartbeatReq(HeartBeatRequest request, HeartBeatResponse response,
      long leaderTerm) {

    response.setTerm(Response.RESPONSE_AGREE);
    response.setFollower(thisNode);
    response.setLastLogIndex(logManager.getLastLogIndex());
    response.setLastLogTerm(logManager.getLastLogTerm());

    synchronized (logManager) {
      logManager.commitLog(request.getCommitLogIndex());
    }
    term.set(leaderTerm);
    setLeader(request.getLeader());
    if (character != NodeCharacter.FOLLOWER) {
      setCharacter(NodeCharacter.FOLLOWER);
    }
    setLastHeartBeatReceivedTime(System.currentTimeMillis());
    if (logger.isDebugEnabled()) {
      logger.debug("Received heartbeat from a valid leader {}", request.getLeader());
    }
  }

  public void retireFromLeader(long newTerm, Node follower) {
    synchronized (term) {
      long currTerm = term.get();
      logger.debug("Received a rejection from {} because term is stale: {}/{}", follower,
          currTerm, newTerm);
      // confirm that the heartbeat of the new leader hasn't come
      if (currTerm < newTerm) {
        term.set(newTerm);
        setCharacter(NodeCharacter.FOLLOWER);
        setLeader(null);
      }
    }
  }
}
