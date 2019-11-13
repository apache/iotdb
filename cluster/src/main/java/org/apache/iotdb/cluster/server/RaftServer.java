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

package org.apache.iotdb.cluster.server;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.Set;
import java.util.SortedSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.iotdb.cluster.config.ClusterConfig;
import org.apache.iotdb.cluster.config.ClusterDescriptor;
import org.apache.iotdb.cluster.exception.AddSelfException;
import org.apache.iotdb.cluster.exception.LeaderUnknownException;
import org.apache.iotdb.cluster.exception.RequestTimeOutException;
import org.apache.iotdb.cluster.exception.UnknownLogTypeException;
import org.apache.iotdb.cluster.log.Log;
import org.apache.iotdb.cluster.log.LogApplier;
import org.apache.iotdb.cluster.log.LogManager;
import org.apache.iotdb.cluster.log.LogParser;
import org.apache.iotdb.cluster.log.MemoryLogManager;
import org.apache.iotdb.cluster.log.Snapshot;
import org.apache.iotdb.cluster.log.meta.AddNodeLog;
import org.apache.iotdb.cluster.rpc.thrift.AppendEntriesRequest;
import org.apache.iotdb.cluster.rpc.thrift.AppendEntryRequest;
import org.apache.iotdb.cluster.rpc.thrift.ElectionRequest;
import org.apache.iotdb.cluster.rpc.thrift.HeartBeatRequest;
import org.apache.iotdb.cluster.rpc.thrift.HeartBeatResponse;
import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.cluster.rpc.thrift.RaftService;
import org.apache.iotdb.cluster.rpc.thrift.RaftService.AsyncClient;
import org.apache.iotdb.cluster.rpc.thrift.RaftService.AsyncProcessor;
import org.apache.iotdb.cluster.rpc.thrift.TSMetaService;
import org.apache.iotdb.cluster.server.handlers.caller.AppendEntryHandler;
import org.apache.iotdb.cluster.server.handlers.forwarder.ForwardAddNodeHandler;
import org.apache.thrift.TException;
import org.apache.thrift.async.AsyncMethodCallback;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.server.THsHaServer;
import org.apache.thrift.server.THsHaServer.Args;
import org.apache.thrift.server.TServer;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TNonblockingServerSocket;
import org.apache.thrift.transport.TNonblockingServerTransport;
import org.apache.thrift.transport.TNonblockingSocket;
import org.apache.thrift.transport.TNonblockingTransport;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class RaftServer implements RaftService.AsyncIface, LogApplier {

  private static final Logger logger = LoggerFactory.getLogger(RaftService.class);
  private static final String NODE_IDENTIFIER_NAME = "node_identifier";
  static final int CONNECTION_TIME_OUT_MS = 20 * 1000;

  static final long RESPONSE_UNSET = 0;
  public static final long RESPONSE_AGREE = -1;
  public static final long RESPONSE_LOG_MISMATCH = -2;
  public static final long RESPONSE_REJECT = -3;
  public static final long RESPONSE_CLUSTER_UNKNOWN = -4;
  static final long RESPONSE_IDENTIFIER_CONFLICT = -5;

  Random random = new Random();

  private ClusterConfig config = ClusterDescriptor.getINSTANCE().getConfig();
  private TNonblockingServerTransport socket;
  private TServer poolServer;
  Node thisNode;

  TProtocolFactory protocolFactory = config.isRpcThriftCompressionEnabled() ?
      new TCompactProtocol.Factory() : new TBinaryProtocol.Factory();

  Set<Node> allNodes = new HashSet<>();
  Map<Integer, Node> idNodeMap = null;
  // blind nodes are nodes that does not know the nodes in the cluster
  private Set<Node> blindNodes = new HashSet<>();
  private Map<Node, Integer> idConflictNodes = new HashMap<>();

  private NodeCharacter character = NodeCharacter.ELECTOR;
  private AtomicLong term = new AtomicLong(0);
  private Node leader;
  private long lastHeartBeatReceivedTime;

  private LogManager logManager;

  ExecutorService heartBeatService;
  private ExecutorService clientService;
  private ExecutorService catchUpService;

  private Map<Node, Long> lastCatchUpResponseTime = new ConcurrentHashMap<>();

  RaftServer() {
    this.thisNode = new Node();
    this.thisNode.setIp(config.getLocalIP());
    this.thisNode.setPort(config.getLocalMetaPort());
    allNodes.add(thisNode);

    loadIdentifier();
  }

  // if the identifier file does not exist, a new identifier will be generated
  private void loadIdentifier() {
    File file = new File(NODE_IDENTIFIER_NAME);
    Integer nodeId = null;
    if (file.exists()) {
      try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
        nodeId = Integer.parseInt(reader.readLine());
      } catch (Exception e) {
        logger.warn("Cannot read the identifier from file, generating a new one");
      }
    }
    if (nodeId != null) {
      setNodeIdentifier(nodeId);
      return;
    }

    setNodeIdentifier(genNodeIdentifier());
  }

  int genNodeIdentifier() {
    return Objects.hash(thisNode.getIp(), thisNode.getPort(), System.currentTimeMillis());
  }

  void setNodeIdentifier(int identifier) {
    logger.info("The identifier of this node has been set to {}", identifier);
    thisNode.setNodeIdentifier(identifier);
    try (BufferedWriter writer = new BufferedWriter(new FileWriter(NODE_IDENTIFIER_NAME))) {
      writer.write(String.valueOf(identifier));
    } catch (IOException e) {
      logger.error("Cannot save the node identifier");
    }
  }

  public void start() throws TTransportException {
    if (clientService != null) {
      return;
    }

    addSeedNodes();
    initLogManager();
    establishServer();

    heartBeatService =
        Executors.newSingleThreadScheduledExecutor(r -> new Thread(r, "HeartBeatThread"));
    catchUpService = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
  }

  private void initLogManager() {
    this.logManager = new MemoryLogManager(this);
  }

  void stop() {
    if (clientService == null) {
      return;
    }

    poolServer.stop();
    socket.close();
    clientService.shutdownNow();
    heartBeatService.shutdownNow();
    catchUpService.shutdownNow();
    socket = null;
    poolServer = null;
    catchUpService = null;
  }

  private void addSeedNodes() {
    List<String> seedUrls = config.getSeedNodeUrls();
    for (String seedUrl : seedUrls) {
      String[] split = seedUrl.split(":");
      if (split.length != 2) {
        logger.warn("Bad seed url: {}", seedUrl);
        continue;
      }
      String ip = split[0];
      // TODO-Cluster: check ip format
      try {
        int port = Integer.parseInt(split[1]);
        if (!ip.equals(thisNode.ip) || port != thisNode.port) {
          Node seedNode = new Node();
          seedNode.setIp(ip);
          seedNode.setPort(port);
          allNodes.add(seedNode);
        }
      } catch (NumberFormatException e) {
        logger.warn("Bad seed url: {}", seedUrl);
      }
    }
  }

  AsyncClient connectNode(Node node) {
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

  abstract AsyncClient getAsyncClient(TNonblockingTransport transport);

  abstract AsyncProcessor getProcessor();

  private void establishServer() throws TTransportException {
    logger.info("Cluster node {} begins to set up", thisNode);

    socket = new TNonblockingServerSocket(new InetSocketAddress(config.getLocalIP(),
        config.getLocalMetaPort()), CONNECTION_TIME_OUT_MS);
    Args poolArgs =
        new THsHaServer.Args(socket).maxWorkerThreads(config.getMaxConcurrentClientNum())
            .minWorkerThreads(1);

    poolArgs.executorService(new ThreadPoolExecutor(poolArgs.minWorkerThreads,
        poolArgs.maxWorkerThreads, poolArgs.getStopTimeoutVal(), poolArgs.getStopTimeoutUnit(),
        new SynchronousQueue<>(), new ThreadFactory() {
      private AtomicLong threadIndex = new AtomicLong(0);
      @Override
      public Thread newThread(Runnable r) {
        return new Thread(r, "IoTDBClusterClientThread-" + threadIndex.incrementAndGet());
      }
    }));
    poolArgs.processor(getProcessor());
    poolArgs.protocolFactory(protocolFactory);
    poolArgs.transportFactory(new TFramedTransport.Factory());

    poolServer = new THsHaServer(poolArgs);
    clientService = Executors.newSingleThreadExecutor(r -> new Thread(r, "ClientServiceThread"));
    clientService.submit(() -> poolServer.serve());

    logger.info("Cluster node {} is up", thisNode);
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
        processLegalHeartbeat(request, response, leaderTerm);
      }
      resultHandler.onComplete(response);
    }
  }

  private void processLegalHeartbeat(HeartBeatRequest request, HeartBeatResponse response,
      long leaderTerm) {
    response.setTerm(RESPONSE_AGREE);
    response.setFollower(thisNode);
    response.setLastLogIndex(logManager.getLastLogIndex());
    response.setLastLogTerm(logManager.getLastLogTerm());

    if (request.isRequireIdentifier()) {
      // the leader wants to know who the node is
      if (request.isRegenerateIdentifier()) {
        // the previously sent id conflicted, generate a new one
        setNodeIdentifier(genNodeIdentifier());
      }
      response.setFolloweIdentifier(thisNode.getNodeIdentifier());
    }

    if (!allNodesIdKnown()) {
      // this node is blind to the cluster
      if (request.isSetNodeSet()) {
        // if the leader has sent the node set then accept it
        allNodes = request.getNodeSet();
        initIdNodeMap();
        for (Node node : allNodes) {
          idNodeMap.put(node.getNodeIdentifier(), node);
        }
      } else {
        // require the node list
        response.setRequireNodeList(true);
      }
    }

    synchronized (logManager) {
      logManager.commitLog(request.getCommitLogIndex());
    }
    term.set(leaderTerm);
    leader = request.getLeader();
    if (character != NodeCharacter.FOLLOWER) {
      setCharacter(NodeCharacter.FOLLOWER);
    }
    lastHeartBeatReceivedTime = System.currentTimeMillis();
    if (logger.isDebugEnabled()) {
      logger.debug("Received heartbeat from a valid leader {}", request.getLeader());
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
        response = RESPONSE_AGREE;
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
    if (idNodeMap == null) {
      // this node lacks information of the cluster and refuse to work
      logger.debug("This node is blind to the cluster and cannot accept logs");
      resultHandler.onComplete(RESPONSE_CLUSTER_UNKNOWN);
      return;
    }

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
          resultHandler.onComplete(RESPONSE_AGREE);
          logger.debug("Append a new log {}, new term:{}, new index:{}", log, localTerm,
              logManager.getLastLogIndex());
        } else if (lastLog.getPreviousLogIndex() == previousLogIndex
            && lastLog.getPreviousLogTerm() < previousLogTerm) {
          // the incoming log points to the previous log of the local last log, and its term is
          // bigger than then local last log's, replace the local last log with it
          logManager.replaceLastLog(log);
          resultHandler.onComplete(RESPONSE_AGREE);
          logger.debug("Replaced the last log with {}, new term:{}, new index:{}", log,
              previousLogTerm, previousLogIndex);
        } else {
          // the incoming log points to an illegal position, reject it
          resultHandler.onComplete(RESPONSE_LOG_MISMATCH);
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
  private AppendLogResult sendLogToFollowers(Log log, int requiredQuorum) {
    if (requiredQuorum < 0) {
      return sendLogToFollowers(log, new AtomicInteger(idNodeMap.size() / 2));
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
    synchronized (idNodeMap) {
      for (Node node : idNodeMap.values()) {
        AsyncClient client = connectNode(node);
        if (client != null) {
          AppendEntryHandler handler = new AppendEntryHandler(this);
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
  public void catchUp(Node follower, long followerLastLogIndex) {
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

  @Override
  public void addNode(Node node, AsyncMethodCallback resultHandler) {
    if (!allNodesIdKnown()) {
      logger.info("Cannot add node now because not all nodes' id are known");
      logger.debug("Known nodes: {}, all nodes: {}", idNodeMap, allNodes);
      resultHandler.onComplete((int) RESPONSE_CLUSTER_UNKNOWN);
      return;
    }

    logger.info("A node {} wants to join this cluster", node);
    if (node == thisNode) {
      resultHandler.onError(new AddSelfException());
      return;
    }

    if (character == NodeCharacter.LEADER) {
      if (idNodeMap.containsKey(node.getNodeIdentifier())) {
        logger.debug("Node {} is already in the cluster", node);
        resultHandler.onComplete((int) RESPONSE_AGREE);
        return;
      }

      // node adding must be serialized
      synchronized (logManager) {
        AddNodeLog addNodeLog = new AddNodeLog();
        addNodeLog.setPreviousLogIndex(logManager.getLastLogIndex());
        addNodeLog.setPreviousLogTerm(logManager.getLastLogTerm());
        addNodeLog.setCurrLogIndex(logManager.getLastLogIndex() + 1);
        addNodeLog.setCurrLogTerm(getTerm().get());

        addNodeLog.setIp(node.getIp());
        addNodeLog.setPort(node.getPort());
        addNodeLog.setNodeIdentifier(node.getNodeIdentifier());

        logManager.appendLog(addNodeLog);

        logger.info("Send the join request of {} to other nodes", node);
        // adding a node requires strong consistency, -2 for this node and the new node
        AppendLogResult result = sendLogToFollowers(addNodeLog, idNodeMap.size() - 2);

        switch (result) {
          case OK:
            logger.info("Join request of {} is accepted", node);
            resultHandler.onComplete((int) RESPONSE_AGREE);
            logManager.commitLog(logManager.getLastLogIndex());
            return;
          case TIME_OUT:
            logger.info("Join request of {} timed out", node);
            resultHandler.onError(new RequestTimeOutException(addNodeLog));
            logManager.removeLastLog();
            return;
          case LEADERSHIP_STALE:
          default:
            logManager.removeLastLog();
            // if the leader is found, forward to it
        }
      }
    }
    if (character == NodeCharacter.FOLLOWER && leader != null) {
      logger.info("Forward the join request of {} to leader {}", node, leader);
      if (forwardAddNode(node, resultHandler)) {
        return;
      }
    }
    resultHandler.onError(new LeaderUnknownException());
  }

  /**
   * Forward the join cluster request to the leader.
   * @param node
   * @param resultHandler
   * @return true if the forwarding succeeds, false otherwise.
   */
  private boolean forwardAddNode(Node node, AsyncMethodCallback resultHandler) {
    TSMetaService.AsyncClient client = (TSMetaService.AsyncClient) connectNode(leader);
    if (client != null) {
      try {
        client.addNode(node, new ForwardAddNodeHandler(resultHandler));
        return true;
      } catch (TException e) {
        logger.warn("Cannot connect to node {}", node, e);
      }
    }
    return false;
  }

  NodeCharacter getCharacter() {
    return character;
  }

  public AtomicLong getTerm() {
    return term;
  }

  public LogManager getLogManager() {
    return logManager;
  }

  long getLastHeartBeatReceivedTime() {
    return lastHeartBeatReceivedTime;
  }

  public Node getLeader() {
    return leader;
  }

  public void setCharacter(NodeCharacter character) {
    logger.info("This node has become a {}", character);
    this.character = character;
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

  Set<Node> getAllNodes() {
    return allNodes;
  }

  Map<Node, Long> getLastCatchUpResponseTime() {
    return lastCatchUpResponseTime;
  }

  public Map<Integer, Node> getIdNodeMap() {
    return idNodeMap;
  }

  Map<Node, Integer> getIdConflictNodes() {
    return idConflictNodes;
  }

  public void setIdNodeMap(
      Map<Integer, Node> idNodeMap) {
    this.idNodeMap = idNodeMap;
  }

  /**
   * Register the identifier for the node if it does not conflict with other nodes.
   * @param node
   * @param identifier
   */
  public void registerNodeIdentifier(Node node, int identifier) {
    synchronized (idNodeMap) {
      if (idNodeMap.containsKey(identifier)) {
        return;
      }
      node.setNodeIdentifier(identifier);
      logger.info("Node {} registered with id {}", node, identifier);
      idNodeMap.put(identifier, node);
      idConflictNodes.remove(node);
    }
  }

  /**
   * When a node requires node list in its heartbeat response, add it into blindNodes so in the
   * heartbeat the node list will be sent to the node.
   * @param node
   */
  public void addBlindNode(Node node) {
    logger.debug("Node {} requires the node list", node);
    blindNodes.add(node);
  }

  /**
   *
   * @param node
   * @return whether a node wants the node list.
   */
  boolean isNodeBlind(Node node) {
    return blindNodes.contains(node);
  }

  /**
   * Remove the node from the blindNodes when the node list is sent.
   * @param node
   */
  void removeBlindNode(Node node) {
    blindNodes.remove(node);
  }
  /**
   * idNodeMap is initialized when the first leader wins or the follower receives the node list
   * from the leader or a node recovers
   */
  public void initIdNodeMap() {
    idNodeMap = new HashMap<>();
    idNodeMap.put(thisNode.getNodeIdentifier(), thisNode);
  }

  /**
   *
   * @return Whether all nodes' identifier is known.
   */
  boolean allNodesIdKnown() {
    return idNodeMap != null && idNodeMap.size() == allNodes.size();
  }

}
