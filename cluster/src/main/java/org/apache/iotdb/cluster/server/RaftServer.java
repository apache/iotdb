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
import org.apache.iotdb.cluster.log.Log;
import org.apache.iotdb.cluster.log.LogApplier;
import org.apache.iotdb.cluster.log.LogManager;
import org.apache.iotdb.cluster.log.MemoryLogManager;
import org.apache.iotdb.cluster.rpc.thrift.AppendEntryRequest;
import org.apache.iotdb.cluster.rpc.thrift.ElectionRequest;
import org.apache.iotdb.cluster.rpc.thrift.HeartBeatRequest;
import org.apache.iotdb.cluster.rpc.thrift.HeartBeatResponse;
import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.cluster.rpc.thrift.RaftService;
import org.apache.iotdb.cluster.rpc.thrift.RaftService.AsyncClient;
import org.apache.iotdb.cluster.rpc.thrift.RaftService.AsyncClient.Factory;
import org.apache.iotdb.cluster.rpc.thrift.RaftService.AsyncClient.appendEntry_call;
import org.apache.iotdb.cluster.rpc.thrift.RaftService.AsyncClient.sendHeartBeat_call;
import org.apache.iotdb.cluster.rpc.thrift.RaftService.AsyncClient.startElection_call;
import org.apache.iotdb.cluster.rpc.thrift.RaftService.AsyncProcessor;
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
  private static final long ELECTION_LEAST_TIME_OUT_MS = 5 * 1000L;
  private static final long ELECTION_RANDOM_TIME_OUT_MS = 5 * 1000L;
  private static final int CONNECTION_TIME_OUT_MS = 30 * 1000;
  private static final String NON_SEED_FILE_NAME = "non-seeds";

  static final long AGREE = -1;
  private static final long LOG_INDEX_MISMATCH = -2;

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
    File nonSeedFile = new File(NON_SEED_FILE_NAME);
    if (!nonSeedFile.exists()) {
      logger.info("No non-seed file found");
      return;
    }
    try (BufferedReader reader = new BufferedReader(new FileReader(NON_SEED_FILE_NAME))) {
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
    try (BufferedWriter writer = new BufferedWriter(new FileWriter(NON_SEED_FILE_NAME))){
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
    heartBeatService.submit(new HeartBeatThread());
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

  synchronized AsyncClient connectNode(Node node) {
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

  AppendLogResult sendLogToFollowers(Log log) {
    logger.debug("Sending a log to followers: {}", log);

    AtomicInteger quorum = new AtomicInteger(allNodes.size() / 2);
    AtomicBoolean leaderShipStale = new AtomicBoolean(false);

    AppendEntryRequest request = new AppendEntryRequest();
    request.setLeader(thisNode);
    request.setTerm(term.get());
    request.setEntry(log.serialize());
    request.setPreviousLogIndex(logManager.getLastLogIndex());
    request.setPreviousLogTerm(logManager.getLastLogTerm());


    for (Node node : allNodes) {
      AsyncClient client = connectNode(node);
      if (client != null) {
        AppendEntryHandler handler = new AppendEntryHandler();
        handler.follower = node;
        handler.quorum = quorum;
        handler.leaderShipStale = leaderShipStale;
        handler.log = log;
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

  class AppendEntryHandler implements AsyncMethodCallback<appendEntry_call> {

    private Log log;
    private AtomicInteger quorum;
    private AtomicBoolean leaderShipStale;
    private Node follower;

    @Override
    public void onComplete(appendEntry_call response) {
      try {
        if (leaderShipStale.get()) {
          // someone has rejected this log because the leadership is stale
          return;
        }
        long resp = response.getResult();
        synchronized (quorum) {
          if (resp == AGREE) {
            int remaining = quorum.decrementAndGet();
            logger.debug("Received an agreement from {} for {}, remaining to succeed: {}", follower,
                log, remaining);
            if (remaining == 0) {
              quorum.notifyAll();
            }
          } else if (resp != LOG_INDEX_MISMATCH) {
            // the leader ship is stale, wait for the new leader's heartbeat
            synchronized (term) {
              long currTerm = term.get();
              leaderShipStale.set(true);
              // confirm that the heartbeat of the new leader hasn't come
              if (currTerm < resp) {
                term.set(resp);
                character = NodeCharacter.FOLLOWER;
                leader = null;
              }
            }
            quorum.notifyAll();
          }
          // if the follower's logs are stale, just wait for the heartbeat to handle
        }
      } catch (TException e) {
        onError(e);
      }
    }

    @Override
    public void onError(Exception exception) {
      logger.warn("Cannot append log {} to {}", log, follower, exception);
    }
  }

  /**
   * HeartBeatThread takes the responsibility to send heartbeats (when this node is a leader), or
   * start elections (when this node is a follower)
   */
  private class HeartBeatThread implements Runnable {

    private static final long HEART_BEAT_INTERVAL_MS = 1000L;

    HeartBeatRequest request = new HeartBeatRequest();

    @Override
    public void run() {
      try {
        logger.info("Heartbeat thread starts...");
        while (!Thread.interrupted()) {
          switch (character) {
            case LEADER:
              // send heart beats to the followers
              sendHeartBeats();
              Thread.sleep(HEART_BEAT_INTERVAL_MS);
              break;
            case FOLLOWER:
              // check if heart beat times out
              long heartBeatInterval = System.currentTimeMillis() - lastHeartBeatReceivedTime;
              if (heartBeatInterval >= CONNECTION_TIME_OUT_MS) {
                // the leader is considered dead, an election will be started in the next loop
                character = NodeCharacter.ELECTOR;
                leader = null;
              } else {
                Thread.sleep(CONNECTION_TIME_OUT_MS);
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
      synchronized (term) {
        request.setTerm(term.get());
        request.setCommitLogIndex(logManager.getLastLogTerm());
        request.setLeader(thisNode);

        sendHeartBeats(allNodes);
      }
    }

    private void sendHeartBeats(Set<Node> nodes) {
      for (Node node : nodes) {
        if (character != NodeCharacter.LEADER) {
          // if the character changes, abort the remaining heart beats
          return;
        }

        AsyncClient client = connectNode(node);
        if (client == null) {
          continue;
        }
        try {
          client.sendHeartBeat(request, new HeartBeatHandler(node));
        } catch (TException e) {
          nodeClientMap.remove(node);
          nodeTransportMap.remove(node).close();
          logger.warn("Cannot send heart beat to node {}", node, e);
        }
      }
    }

    // start elections until this node becomes a leader or a follower
    private void startElections() {

      // the election goes on until this node becomes a follower or a leader
      while (character == NodeCharacter.ELECTOR) {
        if (!logFallsBehind) {
          startElection();
        }
      }
      lastHeartBeatReceivedTime = System.currentTimeMillis();
    }

    // start one round of election
    private void startElection() {
      synchronized (term) {
        long nextTerm = term.incrementAndGet();
        // different elections are handled by different handlers, which avoids previous votes
        // interfere the current election
        int quorumNum = allNodes.size() / 2;
        logger.info("Election {} starts, quorum: {}", term, quorumNum);
        AtomicBoolean electionTerminated = new AtomicBoolean(false);
        AtomicBoolean electionValid = new AtomicBoolean(false);
        AtomicInteger quorum = new AtomicInteger(quorumNum);

        ElectionRequest electionRequest = new ElectionRequest();
        electionRequest.setTerm(nextTerm);
        electionRequest.setLastLogTerm(logManager.getLastLogTerm());
        electionRequest.setLastLogIndex(logManager.getCommitLogIndex());

        requestVote(allNodes, electionRequest, nextTerm, quorum, electionTerminated, electionValid);

        long timeOut =
            ELECTION_LEAST_TIME_OUT_MS + Math.abs(random.nextLong() % ELECTION_RANDOM_TIME_OUT_MS);
        try {
          logger.info("Wait for {}ms until next election", timeOut);
          term.wait(timeOut);
        } catch (InterruptedException e) {
          logger.info("Election {} times out", nextTerm);
          Thread.currentThread().interrupt();
        }

        electionTerminated.set(true);
        if (electionValid.get()) {
          logger.info("Election {} accepted", nextTerm);
          character = NodeCharacter.LEADER;
          leader = thisNode;
          nodeStatus = NodeStatus.JOINED;
        }
      }
    }

    // request a vote from given nodes
    private void requestVote(Set<Node> nodes, ElectionRequest request, long nextTerm,
        AtomicInteger quorum, AtomicBoolean electionTerminated, AtomicBoolean electionValid) {
      for (Node node : nodes) {
        logger.info("Requesting a vote from {}", node);
        AsyncClient client = connectNode(node);
        if (client != null) {
          ElectionHandler handler = new ElectionHandler(node, nextTerm, quorum, electionTerminated,
              electionValid);
          try {
            client.startElection(request, handler);
          } catch (TException e) {
            logger.error("Cannot request a vote from {}", node, e);
            nodeClientMap.remove(node);
            nodeTransportMap.remove(node).close();
          }
        }
      }
    }
  }

  class HeartBeatHandler implements AsyncMethodCallback<sendHeartBeat_call> {

    private Node receiver;

    HeartBeatHandler(Node node) {
      this.receiver = node;
    }

    @Override
    public void onComplete(sendHeartBeat_call resp) {
      logger.debug("Received a heartbeat response");
      HeartBeatResponse response;
      try {
        response = resp.getResult();
      } catch (TException e) {
        onError(e);
        return;
      }

      long followerTerm = response.getTerm();
      if (followerTerm == AGREE) {
        // current leadership is still valid
        Node follower = response.getFollower();
        long lastLogIdx = response.getLastLogIndex();
        logger.debug("Node {} is still alive, log index: {}", follower, lastLogIdx);
        if (RaftServer.this.logManager.getLastLogIndex() > lastLogIdx) {
          handleCatchUp(follower, lastLogIdx);
        }
      } else {
        // current leadership is invalid because the follower has a larger term
        synchronized (term) {
          long currTerm = term.get();
          if (currTerm < followerTerm) {
            logger.info("Losing leadership because current term {} is smaller than {}", currTerm,
                followerTerm);
            term.set(followerTerm);
            character = NodeCharacter.FOLLOWER;
            leader = null;
            lastHeartBeatReceivedTime = System.currentTimeMillis();
          }
        }
      }
    }

    @Override
    public void onError(Exception exception) {
      logger.error("Heart beat error, receiver {}", receiver, exception);
      // reset the connection
      nodeClientMap.remove(receiver);
      nodeTransportMap.remove(receiver).close();
    }

  }

  abstract void handleCatchUp(Node follower, long followerLastLogIndex);

  class ElectionHandler implements AsyncMethodCallback<startElection_call> {

    private Node voter;
    private long currTerm;
    private AtomicInteger quorum;
    private AtomicBoolean terminated;
    private AtomicBoolean electionValid;

    ElectionHandler(Node voter, long currTerm, AtomicInteger quorum,
        AtomicBoolean terminated, AtomicBoolean electionValid) {
      this.voter = voter;
      this.currTerm = currTerm;
      this.quorum = quorum;
      this.terminated = terminated;
      this.electionValid = electionValid;
    }

    @Override
    public void onComplete(startElection_call resp) {
      logger.info("Received an election response");
      long voterTerm;
      try {
        voterTerm = resp.getResult();
      } catch (TException e) {
        onError(e);
        return;
      }

      logger.info("Election response term {}", voterTerm);
      synchronized (term) {
        if (terminated.get()) {
          // a voter has rejected this election, which means the term or the log id falls behind
          // this node is not able to be the leader
          return;
        }

        if (voterTerm == AGREE) {
          long remaining = quorum.decrementAndGet();
          logger.info("Received a for vote, reaming votes to succeed: {}", remaining);
          if (remaining == 0) {
            // the election is valid
            electionValid.set(true);
            terminated.set(true);
            term.notifyAll();
          }
          // still need more votes
        } else {
          // the election is rejected
          terminated.set(true);
          if (voterTerm < currTerm) {
            // the rejection from a node with a smaller term means the log of this node falls behind
            logFallsBehind = true;
            logger.info("Election {} rejected: The node has stale logs and should not start "
                + "elections again", currTerm);
          } else {
            // the election is rejected by a node with a bigger term, update current term to it
            term.set(voterTerm);
            logger.info("Election {} rejected: The term of this node is no bigger than {}",
                voterTerm, currTerm);
          }
          term.notifyAll();
        }
      }
    }

    @Override
    public void onError(Exception exception) {
      logger.warn("A voter {} encountered an error:", voter, exception);
      // reset the connection
      nodeClientMap.remove(voter);
      nodeTransportMap.remove(voter).close();
    }
  }
}
