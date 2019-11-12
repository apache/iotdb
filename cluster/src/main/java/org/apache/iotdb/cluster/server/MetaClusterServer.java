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
import java.util.concurrent.atomic.AtomicLong;
import org.apache.iotdb.cluster.log.Log;
import org.apache.iotdb.cluster.log.meta.AddNodeLog;
import org.apache.iotdb.cluster.log.meta.PhysicalPlanLog;
import org.apache.iotdb.cluster.partition.PartitionTable;
import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.cluster.rpc.thrift.RaftService;
import org.apache.iotdb.cluster.rpc.thrift.TSMetaService;
import org.apache.iotdb.cluster.rpc.thrift.TSMetaService.AsyncClient;
import org.apache.iotdb.cluster.rpc.thrift.TSMetaService.AsyncProcessor;
import org.apache.iotdb.cluster.server.handlers.caller.JoinClusterHandler;
import org.apache.iotdb.db.exception.ProcessorException;
import org.apache.iotdb.db.qp.QueryProcessor;
import org.apache.iotdb.db.qp.executor.QueryProcessExecutor;
import org.apache.iotdb.db.service.IoTDB;
import org.apache.thrift.TException;
import org.apache.thrift.async.TAsyncClientManager;
import org.apache.thrift.transport.TNonblockingTransport;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * MetaCluster manages cluster metadata, such as what nodes are in the cluster and data partition.
 */
public class MetaClusterServer extends RaftServer implements TSMetaService.AsyncIface {

  private static final Logger logger = LoggerFactory.getLogger(MetaClusterServer.class);
  private static final int DEFAULT_JOIN_RETRY = 10;
  private static final String NODES_FILE_NAME = "nodes";

  private AsyncClient.Factory clientFactory;
  private IoTDB ioTDB;
  private QueryProcessor queryProcessor = new QueryProcessor(new QueryProcessExecutor());
  private PartitionTable partitionTable;

  public MetaClusterServer() throws IOException {
    super();
    clientFactory = new AsyncClient.Factory(new TAsyncClientManager(), protocolFactory);
    loadNodes();
    buildPartitionTable();
  }

  /**
   * Use the initial nodes to build a partition table. As the logs catch up, the partitionTable
   * will eventually be consistent with the leader's.
   */
  private void buildPartitionTable() {
    //TODO-Cluster: implement
  }

  @Override
  public void start() throws TTransportException {
    super.start();
    ioTDB = new IoTDB();
    ioTDB.active();
  }

  @Override
  void stop() {
    super.stop();
    ioTDB.stop();
    ioTDB = null;
  }

  @Override
  RaftService.AsyncClient getAsyncClient(TNonblockingTransport transport) {
    return clientFactory.getAsyncClient(transport);
  }

  @Override
  AsyncProcessor getProcessor() {
    return new AsyncProcessor(this);
  }

  /**
   * load the nodes from a local file
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
      logger.info("Load {} nodes: {}", allNodes.size(), allNodes);
    } catch (IOException e) {
      logger.error("Cannot load nodes", e);
    }
  }

  private void loadNode(String url) {
    String[] split = url.split(":");
    if (split.length != 2) {
      logger.warn("Incorrect node url: {}", url);
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
      logger.warn("Incorrect node url: {}", url);
    }
  }

  private synchronized void saveNodes() {
    try (BufferedWriter writer = new BufferedWriter(new FileWriter(NODES_FILE_NAME))){
      for (Node node : allNodes) {
        writer.write(node.ip + ":" + node.port);
        writer.newLine();
      }
    } catch (IOException e) {
      logger.error("Cannot save the nodes", e);
    }
  }



  @Override
  public void apply(Log log) {
    if (log instanceof AddNodeLog) {
      AddNodeLog addNodeLog = (AddNodeLog) log;
      Node newNode = new Node();
      newNode.setIp(addNodeLog.getIp());
      newNode.setPort(addNodeLog.getPort());
      synchronized (allNodes) {
        allNodes.add(newNode);
        saveNodes();
        // update the partition table
        PartitionTable oldPartition = partitionTable;
        partitionTable = partitionTable.addNode(newNode);
      }
    } if (log instanceof PhysicalPlanLog) {
      PhysicalPlanLog physicalPlanLog = (PhysicalPlanLog) log;
      try {
        queryProcessor.getExecutor().processNonQuery(physicalPlanLog.getPlan());
      } catch (ProcessorException e) {
        logger.error("Log {} cannot be applied:", e);
      }
    } else {
      // TODO-Cluster support more types of logs
      logger.error("Unsupported log: {}", log);
    }
  }

  /**
   * Compare the old partition table and the new one, then issue data transfer if necessary.
   * When data transfer is issued, the data flow is unidirectional, which means only the senders
   * will actively send the data or the receivers will actively pull the data (this is to be
   * discussed).
   * @param oldTable
   * @param newTable
   */
  private void repartition(PartitionTable oldTable, PartitionTable newTable) {
    // TODO-Cluster: implement
  }

  /**
   * This node itself is a seed node, and it is going to build the initial cluster with other seed
   * nodes
   */
  public void buildCluster() {
    // just establish the heart beat thread and it will do the remaining
    heartBeatService.submit(new HeartBeatThread(this));
  }

  /**
   * This node is a node seed node and wants to join an established cluster
   */
  public void joinCluster() {
    int retry = DEFAULT_JOIN_RETRY;
    Node[] nodes = allNodes.toArray(new Node[0]);
    JoinClusterHandler handler = new JoinClusterHandler();

    AtomicLong response = new AtomicLong(RESPONSE_UNSET);
    handler.setResponse(response);

    while (retry > 0) {
      // randomly pick up a node to try
      Node node = nodes[random.nextInt(nodes.length)];
      try {
        if (joinCluster(node, response, handler)) {
          logger.info("Joined a cluster, starting the heartbeat thread");
          setCharacter(NodeCharacter.FOLLOWER);
          setLastHeartBeatReceivedTime(System.currentTimeMillis());
          heartBeatService.submit(new HeartBeatThread(this));
          return;
        }
      } catch (TException e) {
        logger.warn("Cannot join the cluster from {}, because:", node, e);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        logger.warn("Cannot join the cluster from {}, because time out after {}ms",
            node, CONNECTION_TIME_OUT_MS);
      }
      // start next try
      retry--;
    }
    // all tries failed
    logger.error("Cannot join the cluster after {} retries", DEFAULT_JOIN_RETRY);
    stop();
  }

  private boolean joinCluster(Node node, AtomicLong response, JoinClusterHandler handler)
      throws TException, InterruptedException {
    AsyncClient client = (AsyncClient) connectNode(node);
    if (client != null) {
      response.set(RESPONSE_UNSET);
      handler.setContact(node);

      client.addNode(thisNode, handler);

      synchronized (response) {
        if (response.get() == RESPONSE_UNSET) {
          response.wait(CONNECTION_TIME_OUT_MS);
        }
      }
      long resp = response.get();
      if (resp == RESPONSE_AGREE) {
        logger.info("Node {} admitted this node into the cluster", node);
        return true;
      }
      logger.warn("Joining the cluster is rejected by {} for response {}", node, resp);
      return false;
    }
    return false;
  }
}
