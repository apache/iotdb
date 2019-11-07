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

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
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
import org.apache.iotdb.cluster.rpc.thrift.ElectionRequest;
import org.apache.iotdb.cluster.rpc.thrift.HeartBeatRequest;
import org.apache.iotdb.cluster.rpc.thrift.HeartBeatResponse;
import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.cluster.rpc.thrift.RaftService;
import org.apache.iotdb.cluster.rpc.thrift.RaftService.AsyncClient;
import org.apache.iotdb.cluster.rpc.thrift.RaftService.AsyncClient.Factory;
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

public abstract class RaftServer implements RaftService.AsyncIface {

  private static final Logger logger = LoggerFactory.getLogger(RaftService.class);
  private static final long ELECTION_LEAST_TIME_OUT_MS = 5 * 1000L;
  private static final long ELECTION_RANDOM_TIME_OUT_MS = 5 * 1000L;
  private static final int CONNECTION_TIME_OUT_MS = 30 * 1000;

  private static final long TERM_AGREE = -1;
  private static final long CATCH_UP_THRESHOLD = 10;

  private Random random = new Random();

  private ClusterConfig config = ClusterDescriptor.getINSTANCE().getConfig();
  private TNonblockingServerTransport socket;
  private TServer poolServer;
  private Node thisNode;

  private AsyncClient.Factory clientFactory;

  private List<Node> seedNodes = new ArrayList<>();
  private List<Node> nonSeedNodes = new ArrayList<>();
  private Map<Node, TTransport> nodeTransportMap = new ConcurrentHashMap<>();
  private Map<Node, AsyncClient> nodeClientMap = new ConcurrentHashMap<>();

  private NodeStatus nodeStatus = NodeStatus.STARTING_UP;
  private NodeCharacter character = NodeCharacter.ELECTOR;

  private AtomicLong term = new AtomicLong(0);
  private long lastLogIndex = -1;
  private long lastLogTerm = -1;
  private long commitLogIndex = -1;
  // if the log of this node falls behind, there is no point for this node to start election anymore
  private boolean logFallsBehind = false;

  private long lastHeartBeatReceivedTime;
  private Node leader;

  private ExecutorService heartBeatService;
  private ExecutorService clientService;

  RaftServer() {
    this.thisNode = new Node();
    this.thisNode.setIp(config.getLocalIP());
    this.thisNode.setPort(config.getLocalMetaPort());
  }

  public void start() throws TTransportException, IOException {
    if (clientService != null) {
      return;
    }

    establishServer();
    connectToSeeds();
    nodeStatus = NodeStatus.ALONE;
    heartBeatService =
        Executors.newSingleThreadScheduledExecutor(r -> new Thread(r, "HeartBeatThread"));
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

  private void connectToSeeds() {
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

    for (Node seedNode : seedNodes) {
      // pre-connect the nodes
      connectNode(seedNode);
    }
  }

  private synchronized AsyncClient connectNode(Node node) {
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
        response.setTerm(TERM_AGREE);
        response.setFollower(thisNode);
        response.setLastLogIndex(lastLogIndex);
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
      // reject the election if one of the three holds:
      // 1. the term of the candidate is no bigger than the voter's
      // 2. the lastLogIndex of the candidate is smaller than the voter's
      // 3. the lastLogIndex of the candidate equals to the voter's but its lastLogTerm is
      // smaller than the voter's
      if (thatTerm <= thisTerm || thatLastLogId < lastLogIndex
          || (thatLastLogId == lastLogIndex && thatLastLogTerm < lastLogTerm)) {
        response = thisTerm;
      } else {
        term.set(thatTerm);
        response = TERM_AGREE;
        character = NodeCharacter.FOLLOWER;
        leader = null;
      }
      logger.info("Sending response to the elector");
      resultHandler.onComplete(response);
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
        request.setCommitLogIndex(commitLogIndex);
        request.setLeader(thisNode);

        sendHeartBeats(seedNodes);
        sendHeartBeats(nonSeedNodes);
      }
    }

    private void sendHeartBeats(List<Node> nodes) {
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
          logger.error("Cannot send heart beat to node {}", node, e);
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
        int quorumNum = (seedNodes.size() + nonSeedNodes.size()) / 2;
        logger.info("Election {} starts, quorum: {}", term, quorumNum);
        AtomicBoolean electionTerminated = new AtomicBoolean(false);
        AtomicBoolean electionValid = new AtomicBoolean(false);
        AtomicInteger quorum = new AtomicInteger(quorumNum);

        ElectionRequest electionRequest = new ElectionRequest();
        electionRequest.setTerm(nextTerm);
        electionRequest.setLastLogTerm(lastLogTerm);
        electionRequest.setLastLogIndex(lastLogIndex);

        requestVote(seedNodes, electionRequest, nextTerm, quorum, electionTerminated, electionValid);
        requestVote(nonSeedNodes, electionRequest, nextTerm, quorum, electionTerminated, electionValid);

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
    private void requestVote(List<Node> nodes, ElectionRequest request, long nextTerm,
        AtomicInteger quorum, AtomicBoolean electionTerminated, AtomicBoolean electionValid) {
      for (Node node : nodes) {
        logger.info("Requesting a vote from {}", node);
        AsyncClient client = connectNode(node);
        ElectionHandler handler = new ElectionHandler(node, nextTerm, quorum, electionTerminated,
            electionValid);
        if (client != null) {
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
      if (followerTerm == TERM_AGREE) {
        // current leadership is still valid
        Node follower = response.getFollower();
        long lastLogIdx = response.getLastLogIndex();
        logger.debug("Node {} is still alive, log index: {}", follower, lastLogIdx);
        if (RaftServer.this.lastLogIndex - lastLogIdx >= CATCH_UP_THRESHOLD) {
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

        if (voterTerm == TERM_AGREE) {
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
