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

package org.apache.iotdb.cluster;

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
import java.util.concurrent.atomic.AtomicLong;
import org.apache.iotdb.cluster.config.ClusterConfig;
import org.apache.iotdb.cluster.config.ClusterDescriptor;
import org.apache.iotdb.cluster.rpc.thrift.AppendEntriesRequest;
import org.apache.iotdb.cluster.rpc.thrift.AppendEntryRequest;
import org.apache.iotdb.cluster.rpc.thrift.ElectionRequest;
import org.apache.iotdb.cluster.rpc.thrift.HeartBeatRequest;
import org.apache.iotdb.cluster.rpc.thrift.HeartBeatResponse;
import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.cluster.rpc.thrift.TSMetaService;
import org.apache.iotdb.cluster.rpc.thrift.TSMetaService.AsyncClient;
import org.apache.iotdb.cluster.rpc.thrift.TSMetaService.AsyncClient.Factory;
import org.apache.iotdb.cluster.rpc.thrift.TSMetaService.AsyncProcessor;
import org.apache.thrift.TException;
import org.apache.thrift.async.AsyncMethodCallback;
import org.apache.thrift.async.TAsyncClientManager;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.server.TThreadPoolServer.Args;
import org.apache.thrift.transport.TNonblockingSocket;
import org.apache.thrift.transport.TNonblockingTransport;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * MetaCluster manages cluster metadata, such as what nodes are in the cluster and data partition.
 */
public class MetaClusterServer implements TSMetaService.AsyncIface {

  private static final Logger logger = LoggerFactory.getLogger(MetaClusterServer.class);
  private static final long ELECTION_LEAST_TIME_OUT_MS = 30 * 1000L;
  private static final long ELECTION_RANDOM_TIME_OUT_MS = 30 * 1000L;
  private static final long TERM_AGREE = -1;
  private static final long CATCH_UP_THRESHOLD = 10;

  private Random random = new Random();

  private ClusterConfig config = ClusterDescriptor.getINSTANCE().getConfig();
  private TServerSocket socket;
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

  // handlers
  private HeartBeatHandler heartBeatHandler = new HeartBeatHandler();
  private ElectionHandler electionHandler;

  private ExecutorService heartBeatService;

  public MetaClusterServer() {
    this.thisNode = new Node();
    this.thisNode.setIp(config.getLocalIP());
    this.thisNode.setPort(config.getLocalMetaPort());
  }

  public void start() throws TTransportException, IOException {
    if (poolServer != null) {
      return;
    }

    establishServer();
    connectToSeeds();
    nodeStatus = NodeStatus.ALONE;
    heartBeatService =
        Executors.newSingleThreadScheduledExecutor(r -> new Thread(r, "HeartBeatThread"));
  }

  public void stop() {
    if (poolServer == null) {
      return;
    }

    for (TTransport transport : nodeTransportMap.values()) {
      transport.close();
    }
    nodeTransportMap.clear();
    nodeClientMap.clear();

    heartBeatService.shutdownNow();
    socket.close();
    poolServer.stop();
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
      TNonblockingTransport transport = new TNonblockingSocket(node.getIp(), node.getPort());
      client = clientFactory.getAsyncClient(transport);
      nodeClientMap.put(node, client);
      nodeTransportMap.put(node, transport);
      logger.info("Connected to node {}", node);
    } catch (IOException e) {
      logger.warn("Cannot connect to seed node {}", node, e);
    }
    return client;
  }


  private void establishServer() throws TTransportException, IOException {
    logger.info("Cluster node {} begins to set up", thisNode);
    TProtocolFactory protocolFactory = config.isRpcThriftCompressionEnabled() ?
        new TCompactProtocol.Factory() : new TBinaryProtocol.Factory();

    socket = new TServerSocket(new InetSocketAddress(config.getLocalIP(),
        config.getLocalMetaPort()));
    Args poolArgs =
        new TThreadPoolServer.Args(socket).maxWorkerThreads(config.getMaxConcurrentClientNum())
            .minWorkerThreads(1);

    poolArgs.executorService = new ThreadPoolExecutor(poolArgs.minWorkerThreads,
        poolArgs.maxWorkerThreads, poolArgs.stopTimeoutVal, poolArgs.stopTimeoutUnit,
        new SynchronousQueue<>(), r -> new Thread(r, "IoTDBClusterClientThread"));
    poolArgs.processor(new AsyncProcessor<>(this));
    poolArgs.protocolFactory(protocolFactory);
    poolServer = new TThreadPoolServer(poolArgs);
    poolServer.serve();

    clientFactory = new Factory(new TAsyncClientManager(), protocolFactory);
    logger.info("Cluster node {} is up", thisNode);
  }


  @Override
  public void sendHeartBeat(HeartBeatRequest request, AsyncMethodCallback resultHandler) {
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
      resultHandler.onComplete(response);
    }
  }

  @Override
  public void appendMetadataEntry(AppendEntryRequest request, AsyncMethodCallback resultHandler) {

  }

  @Override
  public void appendMetadataEntries(AppendEntriesRequest request, AsyncMethodCallback resultHandler) {

  }

  @Override
  public void addNode(Node node, AsyncMethodCallback resultHandler) {

  }

  /**
   * HeartBeatThread takes the responsibility to send heartbeats (when this node is a leader), or
   * start elections (when this node is a follower)
   */
  private class HeartBeatThread implements Runnable {

    private static final long HEART_BEAT_INTERVAL_MS = 1000L;
    private static final long HEART_BEAT_TIME_OUT_MS = 30 * 1000L;

    HeartBeatRequest request = new HeartBeatRequest();

    @Override
    public void run() {
      try {
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
              if (heartBeatInterval >= HEART_BEAT_TIME_OUT_MS) {
                // the leader is considered dead, an election will be started in the next loop
                character = NodeCharacter.ELECTOR;
                leader = null;
              } else {
                Thread.sleep(HEART_BEAT_TIME_OUT_MS);
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
      for (Node seedNode : nodes) {
        if (character != NodeCharacter.LEADER) {
          // if the character changes, abort the remaining heart beats
          return;
        }

        AsyncClient client = connectNode(seedNode);
        if (client == null) {
          continue;
        }
        try {
          client.sendHeartBeat(request, heartBeatHandler);
        } catch (TException e) {
          nodeClientMap.remove(seedNode);
          nodeTransportMap.remove(seedNode).close();
          logger.error("Cannot send heart beat to node {}", seedNode, e);
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
        Long nextTerm = term.incrementAndGet();
        // different elections are handled by different handlers, which avoids previous votes
        // interfere the current election
        electionHandler = new ElectionHandler();
        electionHandler.currTerm = nextTerm;
        electionHandler.quorum = (seedNodes.size() + nonSeedNodes.size()) / 2;
        logger.info("Election {} starts, quorum: {}", term, electionHandler.quorum);

        ElectionRequest electionRequest = new ElectionRequest();
        electionRequest.setTerm(nextTerm);
        electionRequest.setLastLogTerm(lastLogTerm);
        electionRequest.setLastLogIndex(lastLogIndex);

        requestVote(seedNodes, electionRequest);
        requestVote(nonSeedNodes, electionRequest);

        long timeOut =
            ELECTION_LEAST_TIME_OUT_MS + Math.abs(random.nextLong() % ELECTION_RANDOM_TIME_OUT_MS);
        try {
          nextTerm.wait(timeOut);
        } catch (InterruptedException e) {
          logger.info("Election {} times out", nextTerm);
          Thread.currentThread().interrupt();
        }

        electionHandler.terminated = true;
        if (electionHandler.electionValid) {
          logger.info("Election {} accepted", nextTerm);
          character = NodeCharacter.LEADER;
          leader = thisNode;
          nodeStatus = NodeStatus.JOINED;
        }
      }
    }

    // request a vote from given nodes
    private void requestVote(List<Node> nodes, ElectionRequest request) {
      for (Node node : nodes) {
        AsyncClient client = connectNode(node);
        if (client != null) {
          try {
            client.startElection(request, electionHandler);
          } catch (TException e) {
            logger.error("Cannot request a vote from {}", node, e);
            nodeClientMap.remove(node);
            nodeTransportMap.remove(node).close();
          }
        }
      }
    }
  }

  class HeartBeatHandler implements AsyncMethodCallback<HeartBeatResponse> {

    @Override
    public void onComplete(HeartBeatResponse response) {
      long followerTerm = response.getTerm();
      if (followerTerm == TERM_AGREE) {
        // current leadership is still valid
        Node follower = response.getFollower();
        long lastLogIdx = response.getLastLogIndex();
        logger.debug("Node {} is still alive, log index: {}", follower, lastLogIdx);
        if (MetaClusterServer.this.lastLogIndex - lastLogIdx >= CATCH_UP_THRESHOLD) {
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
      logger.error("Heart beat error", exception);
    }

    private void handleCatchUp(Node follower, long followerLastLogIndex) {
      logger.debug("Log of {} is stale [Pos:{}/{}], try to help it catch up", follower,
          followerLastLogIndex, lastLogIndex);
    }
  }

  class ElectionHandler implements AsyncMethodCallback<Long> {

    private int quorum;
    private boolean electionValid = false;
    private Long currTerm;
    private boolean terminated = false;

    @Override
    public void onComplete(Long response) {
      synchronized (currTerm) {
        if (!electionValid || terminated) {
          // a voter has rejected this election, which means the term or the log id falls behind
          // this node is not able to be the leader
          return;
        }

        if (response == TERM_AGREE) {
          if (--quorum == 0) {
            // the election is valid
            electionValid = true;
            currTerm.notifyAll();
            terminated = true;
          }
          // still need more votes
        } else {
          // the election is rejected
          terminated = true;
          if (response < currTerm) {
            // the rejection from a node with a smaller term means the log of this node falls behind
            logFallsBehind = true;
            logger.info("Election {} rejected: The node has stale logs and should not start "
                + "elections again", currTerm);
          } else {
            // the election is rejected by a node with a bigger term, update current term to it
            term.set(response);
            logger.info("Election {} rejected: The term of this node is no bigger than {}",
                response, currTerm);
          }
          currTerm.notifyAll();
        }
      }

    }

    @Override
    public void onError(Exception exception) {
      logger.warn("A voter encountered an error:", exception);
    }

  }
}
