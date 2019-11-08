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
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.iotdb.cluster.config.ClusterConfig;
import org.apache.iotdb.cluster.config.ClusterDescriptor;
import org.apache.iotdb.cluster.exception.UnknownLogTypeException;
import org.apache.iotdb.cluster.log.Log;
import org.apache.iotdb.cluster.log.LogApplier;
import org.apache.iotdb.cluster.log.LogManager;
import org.apache.iotdb.cluster.log.LogParser;
import org.apache.iotdb.cluster.log.MemoryLogManager;
import org.apache.iotdb.cluster.rpc.thrift.AppendEntriesRequest;
import org.apache.iotdb.cluster.rpc.thrift.AppendEntryRequest;
import org.apache.iotdb.cluster.rpc.thrift.ElectionRequest;
import org.apache.iotdb.cluster.rpc.thrift.HeartBeatRequest;
import org.apache.iotdb.cluster.rpc.thrift.HeartBeatResponse;
import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.cluster.rpc.thrift.RaftService;
import org.apache.iotdb.cluster.rpc.thrift.RaftService.AsyncClient;
import org.apache.iotdb.cluster.rpc.thrift.RaftService.AsyncClient.Factory;
import org.apache.iotdb.cluster.rpc.thrift.RaftService.AsyncProcessor;
import org.apache.iotdb.cluster.server.handlers.AppendEntryHandler;
import org.apache.iotdb.cluster.server.handlers.HeartBeatThread;
import org.apache.thrift.TException;
import org.apache.thrift.async.AsyncMethodCallback;
import org.apache.thrift.async.TAsyncClientManager;
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
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class RaftServer implements RaftService.AsyncIface, LogApplier {

  private static final Logger logger = LoggerFactory.getLogger(RaftService.class);
  public static final int CONNECTION_TIME_OUT_MS = 30 * 1000;
  private static final String NODES_FILE_NAME = "nodes";

  public static final long AGREE = -1;
  public static final long LOG_MISMATCH = -2;

  private Random random = new Random();

  private ClusterConfig config = ClusterDescriptor.getINSTANCE().getConfig();
  private TNonblockingServerTransport socket;
  private TServer poolServer;
  Node thisNode;

  private AsyncClient.Factory clientFactory;

  private List<Node> seedNodes = new ArrayList<>();
  Set<Node> allNodes = new HashSet<>();

  Map<Node, TTransport> nodeTransportMap = new ConcurrentHashMap<>();
  Map<Node, AsyncClient> nodeClientMap = new ConcurrentHashMap<>();

  private NodeStatus nodeStatus = NodeStatus.STARTING_UP;
  NodeCharacter character = NodeCharacter.ELECTOR;

  private AtomicLong term = new AtomicLong(0);

  LogManager logManager;

  // if the log of this node falls behind, there is no point for this node to start election anymore
  private boolean logFallsBehind = false;

  private long lastHeartBeatReceivedTime;
  Node leader;

  private ExecutorService heartBeatService;
  private ExecutorService clientService;

  RaftServer() {
    this.thisNode = new Node();
    this.thisNode.setIp(config.getLocalIP());
    this.thisNode.setPort(config.getLocalMetaPort());
    initLogManager();
  }

  /**
   * load the non-seed nodes from a local record
   */
  private void loadNodes() {
    File nonSeedFile = new File(NODES_FILE_NAME);
    if (!nonSeedFile.exists()) {
      logger.info("No non-seed file found");
      return;
    }
    try (BufferedReader reader = new BufferedReader(new FileReader(NODES_FILE_NAME))) {
      String line;
      while ((line = reader.readLine()) != null) {
        loadNode(line);
      }
      logger.info("Load {} non-seed nodes: {}", allNodes.size(), allNodes);
    } catch (IOException e) {
      logger.error("Cannot read non seeds");
    }
  }

  private void loadNode(String url) {
    String[] split = url.split(":");
    if (split.length != 2) {
      logger.warn("Incorrect seed url: {}", url);
      return;
    }
    // TODO: check url format
    String ip = split[0];
    try {
      int port = Integer.parseInt(split[1]);
      Node node = new Node();
      node.setIp(ip);
      node.setPort(port);
      allNodes.add(node);
    } catch (NumberFormatException e) {
      logger.warn("Incorrect seed url: {}", url);
    }
  }

  synchronized void saveNodes() {
    try (BufferedWriter writer = new BufferedWriter(new FileWriter(NODES_FILE_NAME))){
      for (Node node : allNodes) {
        writer.write(node.ip + ":" + node.port);
        writer.newLine();
      }
    } catch (IOException e) {
      logger.error("Cannot save the non-seed nodes", e);
    }
  }

  public void start() throws TTransportException, IOException {
    if (clientService != null) {
      return;
    }

    establishServer();
    connectToNodes();
    nodeStatus = NodeStatus.ALONE;
    heartBeatService =
        Executors.newSingleThreadScheduledExecutor(r -> new Thread(r, "HeartBeatThread"));
  }

  private void initLogManager() {
    this.logManager = new MemoryLogManager(this);
  }

  public void stop() {
    if (clientService == null) {
      return;
    }

    for (TTransport transport : nodeTransportMap.values()) {
      transport.close();
    }
    nodeTransportMap.clear();
    nodeClientMap.clear();

    poolServer.stop();
    socket.close();
    clientService.shutdownNow();
    heartBeatService.shutdownNow();
    socket = null;
    poolServer = null;
  }

  /**
   * this node itself is a seed node, and it is going to build the initial cluster with other seed
   * nodes
   */
  public void buildCluster() {
    // just establish the heart beat thread and it will do the remaining
    heartBeatService.submit(new HeartBeatThread(this));
  }

  private void connectToNodes() {
    List<String> seedUrls = config.getSeedNodeUrls();
    for (String seedUrl : seedUrls) {
      String[] split = seedUrl.split(":");
      String ip = split[0];
      int port = Integer.parseInt(split[1]);
      if (!ip.equals(thisNode.ip) || port != thisNode.port) {
        Node seedNode = new Node();
        seedNode.setIp(ip);
        seedNode.setPort(port);
        seedNodes.add(seedNode);
      }
    }
    allNodes.addAll(seedNodes);

    for (Node seedNode : allNodes) {
      // pre-connect the nodes
      connectNode(seedNode);
    }
  }

  public synchronized AsyncClient connectNode(Node node) {
    if (node.equals(thisNode)) {
      return null;
    }
    AsyncClient client = nodeClientMap.get(node);
    if (client != null) {
      return client;
    }

    try {
      logger.info("Establish a new connection to {}", node);
      TNonblockingTransport transport = new TNonblockingSocket(node.getIp(), node.getPort(),
          CONNECTION_TIME_OUT_MS);
      client = clientFactory.getAsyncClient(transport);
      nodeClientMap.put(node, client);
      nodeTransportMap.put(node, transport);
    } catch (IOException e) {
      logger.warn("Cannot connect to seed node {}", node, e);
    }
    return client;
  }


  private void establishServer() throws TTransportException, IOException {
    logger.info("Cluster node {} begins to set up", thisNode);
    TProtocolFactory protocolFactory = config.isRpcThriftCompressionEnabled() ?
        new TCompactProtocol.Factory() : new TBinaryProtocol.Factory();

    socket = new TNonblockingServerSocket(new InetSocketAddress(config.getLocalIP(),
        config.getLocalMetaPort()), CONNECTION_TIME_OUT_MS);
    Args poolArgs =
        new THsHaServer.Args(socket).maxWorkerThreads(config.getMaxConcurrentClientNum())
            .minWorkerThreads(1);

    poolArgs.executorService(new ThreadPoolExecutor(poolArgs.minWorkerThreads,
        poolArgs.maxWorkerThreads, poolArgs.getStopTimeoutVal(), poolArgs.getStopTimeoutUnit(),
        new SynchronousQueue<>(), r -> new Thread(r, "IoTDBClusterClientThread")));
    poolArgs.processor(new AsyncProcessor<>(this));
    poolArgs.protocolFactory(protocolFactory);
    poolArgs.transportFactory(new TFramedTransport.Factory());

    poolServer = new THsHaServer(poolArgs);
    clientService = Executors.newSingleThreadExecutor(r -> new Thread(r, "ClientServiceThread"));
    clientService.submit(()->poolServer.serve());

    clientFactory = new Factory(new TAsyncClientManager(), protocolFactory);
    logger.info("Cluster node {} is up", thisNode);
  }


  @Override
  public void sendHeartBeat(HeartBeatRequest request, AsyncMethodCallback resultHandler) {
    logger.info("Received a heartbeat");
    synchronized (term) {
      long thisTerm = term.get();
      long leaderTerm = request.getTerm();
      HeartBeatResponse response = new HeartBeatResponse();

      if (leaderTerm < thisTerm) {
        // the leader is invalid
        response.setTerm(thisTerm);
        if (logger.isDebugEnabled()) {
          logger.debug("Received heartbeat from a stale leader {}", request.getLeader());
        }
      } else {
        response.setTerm(AGREE);
        response.setFollower(thisNode);
        response.setLastLogIndex(logManager.getLastLogIndex());
        response.setLastLogTerm(logManager.getLastLogTerm());
        term.set(leaderTerm);
        leader = request.getLeader();
        nodeStatus = NodeStatus.JOINED;
        character = NodeCharacter.FOLLOWER;
        lastHeartBeatReceivedTime = System.currentTimeMillis();
        if (logger.isDebugEnabled()) {
          logger.debug("Received heartbeat from a valid leader {}", request.getLeader());
        }
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
      // reject the election if one of the four holds:
      // 1. the term of the candidate is no bigger than the voter's
      // 2. the lastLogIndex of the candidate is smaller than the voter's
      // 3. the lastLogIndex of the candidate equals to the voter's but its lastLogTerm is
      // smaller than the voter's
      long lastLogIndex = logManager.getLastLogIndex();
      long lastLogTerm = logManager.getLastLogTerm();
      if (thatTerm <= thisTerm || thatLastLogId < lastLogIndex
          || (thatLastLogId == lastLogIndex && thatLastLogTerm < lastLogTerm)) {
        response = thisTerm;
      } else {
        term.set(thatTerm);
        response = AGREE;
        character = NodeCharacter.FOLLOWER;
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
        resultHandler.onComplete(localTerm);
        return;
      } else if (leaderTerm > localTerm) {
        term.set(leaderTerm);
        localTerm = leaderTerm;
        character = NodeCharacter.FOLLOWER;
      }
    }

    try {
      Log log = LogParser.getINSTANCE().parse(request.entry);
      synchronized (logManager) {
        long localLastLogIndex = logManager.getLastLogIndex();
        long localLastLogTerm = logManager.getLastLogTerm();
        long lastLogIndex = log.getPreviousLogIndex();
        long lastLogTerm = log.getPreviousLogTerm();
        if (lastLogIndex == localLastLogIndex && lastLogTerm == localLastLogTerm) {
          logManager.appendLog(log, localTerm);
          resultHandler.onComplete(AGREE);
          // as the log is updated, this node gets a chance to compete for the leader
          logFallsBehind = false;
          logger.debug("Append a new log {}, term:{}, index:{}", log, localTerm, lastLogIndex + 1);
        } else if (lastLogIndex == localLastLogIndex - 1 && lastLogTerm > localLastLogTerm) {
          logManager.replaceLastLog(log, localTerm);
          resultHandler.onComplete(AGREE);
          logger.debug("Replaced a stale log {}, term:{}, index:{}", log, localTerm, lastLogIndex);
        } else {
          resultHandler.onComplete(LOG_MISMATCH);
          logger.debug("Cannot append the log because the last log does not match, local:term-{},"
                  + "index-{}, request:term-{},index-{}", localLastLogTerm, localLastLogIndex,
              lastLogTerm, lastLogIndex);
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

  AppendLogResult sendLogToFollowers(Log log) {
    logger.debug("Sending a log to followers: {}", log);

    AtomicInteger quorum = new AtomicInteger(allNodes.size() / 2);
    AtomicBoolean leaderShipStale = new AtomicBoolean(false);

    AppendEntryRequest request = new AppendEntryRequest();
    request.setTerm(term.get());
    request.setEntry(log.serialize());

    for (Node node : allNodes) {
      AsyncClient client = connectNode(node);
      if (client != null) {
        AppendEntryHandler handler = new AppendEntryHandler(this);
        handler.setFollower(node);
        handler.setQuorum(quorum);
        handler.setLeaderShipStale(leaderShipStale);
        handler.setLog(log);
        try {
          client.appendEntry(request, handler);
        } catch (TException e) {
          nodeClientMap.remove(node);
          nodeTransportMap.remove(node).close();
          logger.warn("Cannot append log to node {}", node, e);
        }
      }
    }
    synchronized (quorum) {
      if (quorum.get() == 0 || !leaderShipStale.get()) {
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

  public void disConnectNode(Node node) {
    nodeClientMap.remove(node);
    nodeTransportMap.remove(node).close();
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

  public boolean isLogFallsBehind() {
    return logFallsBehind;
  }

  public long getLastHeartBeatReceivedTime() {
    return lastHeartBeatReceivedTime;
  }

  public Node getLeader() {
    return leader;
  }

  public void setCharacter(NodeCharacter character) {
    this.character = character;
  }

  public void setTerm(AtomicLong term) {
    this.term = term;
  }

  public void setLogManager(LogManager logManager) {
    this.logManager = logManager;
  }

  public void setLogFallsBehind(boolean logFallsBehind) {
    this.logFallsBehind = logFallsBehind;
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

  public Set<Node> getAllNodes() {
    return allNodes;
  }

  public NodeStatus getNodeStatus() {
    return nodeStatus;
  }

  public void setNodeStatus(NodeStatus nodeStatus) {
    this.nodeStatus = nodeStatus;
  }
}
