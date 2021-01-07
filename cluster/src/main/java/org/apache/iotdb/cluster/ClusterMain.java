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
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.iotdb.cluster.client.async.AsyncMetaClient;
import org.apache.iotdb.cluster.client.sync.SyncClientAdaptor;
import org.apache.iotdb.cluster.config.ClusterConfig;
import org.apache.iotdb.cluster.config.ClusterDescriptor;
import org.apache.iotdb.cluster.exception.ConfigInconsistentException;
import org.apache.iotdb.cluster.exception.StartUpCheckFailureException;
import org.apache.iotdb.cluster.partition.slot.SlotPartitionTable;
import org.apache.iotdb.cluster.partition.slot.SlotStrategy;
import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.cluster.server.MetaClusterServer;
import org.apache.iotdb.cluster.server.Response;
import org.apache.iotdb.cluster.utils.ClusterUtils;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.StartupException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.thrift.TException;
import org.apache.thrift.async.TAsyncClientManager;
import org.apache.thrift.protocol.TBinaryProtocol.Factory;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClusterMain {

  private static final Logger logger = LoggerFactory.getLogger(ClusterMain.class);

  // establish the cluster as a seed
  private static final String MODE_START = "-s";
  // join an established cluster
  private static final String MODE_ADD = "-a";
  // send a request to remove a node, more arguments: ip-of-removed-node
  // metaport-of-removed-node
  private static final String MODE_REMOVE = "-r";
  // the separator between the cluster configuration and the single-server configuration
  private static final String SERVER_CONF_SEPARATOR = "-sc";
  private static MetaClusterServer metaServer;

  public static void main(String[] args) {
    if (args.length < 1) {
      logger.error("Usage: <-s|-a|-r> [-internal_meta_port <internal meta port>] "
          + "[-internal_data_port <internal data port>] "
          + "[-cluster_rpc_port <cluster rpc port>] "
          + "[-seed_nodes <node1:meta_port:data_port:cluster_rpc_port,"
          +               "node2:meta_port:data_port:cluster_rpc_port,"
          +           "...,noden:meta_port:data_port:cluster_rpc_port>] "
          + "[-sc] "
          + "[-rpc_port <rpc port>]");
      return;
    }
    String mode = args[0];
    if (args.length > 1) {
      String[] params = Arrays.copyOfRange(args, 1, args.length);
      replaceDefaultPrams(params);
    }

    // params check
    if (!checkConfig()) {
      return;
    }

    IoTDBDescriptor.getInstance().getConfig().setSyncEnable(false);
    IoTDBDescriptor.getInstance().getConfig().setAutoCreateSchemaEnabled(false);
    logger.info("Running mode {}", mode);
    if (MODE_START.equals(mode)) {
      try {
        metaServer = new MetaClusterServer();
        startServerCheck();
        preStartCustomize();
        metaServer.start();
        metaServer.buildCluster();
      } catch (TTransportException | StartupException | QueryProcessException |
          StartUpCheckFailureException | ConfigInconsistentException e) {
        metaServer.stop();
        logger.error("Fail to start meta server", e);
      }
    } else if (MODE_ADD.equals(mode)) {
      try {
        metaServer = new MetaClusterServer();
        preStartCustomize();
        metaServer.start();
        metaServer.joinCluster();
      } catch (TTransportException | StartupException | QueryProcessException | StartUpCheckFailureException | ConfigInconsistentException e) {
        metaServer.stop();
        logger.error("Fail to join cluster", e);
      }
    } else if (MODE_REMOVE.equals(mode)) {
      try {
        doRemoveNode(args);
      } catch (IOException e) {
        logger.error("Fail to remove node in cluster", e);
      }
    } else {
      logger.error("Unrecognized mode {}", mode);
    }
  }

  private static void startServerCheck() throws StartupException {
    ClusterConfig config = ClusterDescriptor.getInstance().getConfig();
    // check the initial replicateNum and refuse to start when the replicateNum <= 0
    if (config.getReplicationNum() <= 0) {
      String message = String.format("ReplicateNum should be greater than 0 instead of %d.",
          config.getReplicationNum());
      throw new StartupException(metaServer.getMember().getName(), message);
    }
    // check the initial cluster size and refuse to start when the size < quorum
    int quorum = config.getReplicationNum() / 2 + 1;
    if (config.getSeedNodeUrls().size() < quorum) {
      String message = String.format("Seed number less than quorum, seed number: %s, quorum: "
              + "%s.",
          config.getSeedNodeUrls().size(), quorum);
      throw new StartupException(metaServer.getMember().getName(), message);
    }
    // assert not duplicated nodes
    Set<Node> seedNodes = new HashSet<>();
    for (String url : config.getSeedNodeUrls()) {
      Node node = ClusterUtils.parseNode(url);
      if (seedNodes.contains(node)) {
        String message = String.format(
            "SeedNodes must not repeat each other. SeedNodes: %s", config.getSeedNodeUrls());
        throw new StartupException(metaServer.getMember().getName(), message);
      }
      seedNodes.add(node);
    }

    // assert this node is in all nodes when restart
    if (!metaServer.getMember().getAllNodes().isEmpty()) {
      if (!metaServer.getMember().getAllNodes().contains(metaServer.getMember().getThisNode())) {
        String message = String.format(
            "All nodes in partitionTables must contains local node in start-server mode. "
                + "LocalNode: %s, AllNodes: %s",
            metaServer.getMember().getThisNode(), metaServer.getMember().getAllNodes());
        throw new StartupException(metaServer.getMember().getName(), message);
      } else {
        return;
      }
    }

    // assert this node is in seed nodes list
    Node localNode = new Node();
    localNode.setIp(config.getClusterRpcIp()).setMetaPort(config.getInternalMetaPort())
        .setDataPort(config.getInternalDataPort()).setClientPort(config.getClusterRpcPort());
    if (!seedNodes.contains(localNode)) {
      String message = String.format(
          "SeedNodes must contains local node in start-server mode. LocalNode: %s ,SeedNodes: %s",
          localNode.toString(), config.getSeedNodeUrls());
      throw new StartupException(metaServer.getMember().getName(), message);
    }
  }

  private static void replaceDefaultPrams(String[] args) {
    int index;
    String[] clusterParams;
    String[] serverParams = null;
    for (index = 0; index < args.length; index++) {
      //find where -sc is
      if (SERVER_CONF_SEPARATOR.equals(args[index])) {
        break;
      }
    }
    //parameters from 0 to "-sc" are for clusters
    clusterParams = Arrays.copyOfRange(args, 0, index);

    if (index < args.length) {
      serverParams = Arrays.copyOfRange(args, index + 1, args.length);
    }

    if (clusterParams.length > 0) {
      // replace the cluster default conf params
      ClusterDescriptor.getInstance().replaceProps(clusterParams);
    }

    if (serverParams != null && serverParams.length > 0) {
      // replace the server default conf params
      IoTDBDescriptor.getInstance().replaceProps(serverParams);
    }
  }

  /**
   * check the configuration is legal or not
   */
  private static boolean checkConfig() {
    // 0. first replace all hostname with ip
    try {
      ClusterDescriptor.getInstance().replaceHostnameWithIp();
    } catch (Exception e) {
      logger.error("replace hostname with ip failed, {}", e.getMessage());
      return false;
    }

    // 1. check the cluster_rpc_ip and seed_nodes consistent or not
    // when clusterRpcIp is 127.0.0.1, the entire cluster must be start locally
    ClusterConfig config = ClusterDescriptor.getInstance().getConfig();
    String localhostIp = "127.0.0.1";
    String configClusterRpcIp = config.getClusterRpcIp();
    List<String> seedNodes = config.getSeedNodeUrls();
    boolean isLocalCluster = localhostIp.equals(configClusterRpcIp);
    for (String seedNodeIP : seedNodes) {
      if ((isLocalCluster && !seedNodeIP.contains(localhostIp)) ||
          (!isLocalCluster && seedNodeIP.contains(localhostIp))) {
        logger.error(
            "cluster_rpc_ip={} and seed_nodes={} should be consistent, both use local ip or real ip please",
            configClusterRpcIp, seedNodes);
        return false;
      }
    }
    return true;
  }

  private static void doRemoveNode(String[] args) throws IOException {
    if (args.length != 3) {
      logger.error("Usage: -r <ip> <metaPort>");
      return;
    }
    String ip = args[1];
    int metaPort = Integer.parseInt(args[2]);
    ClusterConfig config = ClusterDescriptor.getInstance().getConfig();
    TProtocolFactory factory = config
        .isRpcThriftCompressionEnabled() ? new TCompactProtocol.Factory() : new Factory();
    Node nodeToRemove = new Node();
    nodeToRemove.setIp(ip).setMetaPort(metaPort);
    // try sending the request to each seed node
    for (String url : config.getSeedNodeUrls()) {
      Node node = ClusterUtils.parseNode(url);
      if (node == null) {
        continue;
      }
      AsyncMetaClient client = new AsyncMetaClient(factory, new TAsyncClientManager(), node, null);
      Long response = null;
      try {
        logger.info("Start removing node {} with the help of node {}", nodeToRemove, node);
        response = SyncClientAdaptor.removeNode(client, nodeToRemove);
      } catch (TException e) {
        logger.warn("Cannot send remove node request through {}, try next node", node);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        logger.warn("Cannot send remove node request through {}, try next node", node);
      }
      if (response != null) {
        handleNodeRemovalResp(response, nodeToRemove);
        return;
      }
    }
  }

  private static void handleNodeRemovalResp(Long response, Node nodeToRemove) {
    if (response == Response.RESPONSE_AGREE) {
      logger.info("Node {} is successfully removed", nodeToRemove);
    } else if (response == Response.RESPONSE_CLUSTER_TOO_SMALL) {
      logger.error("Cluster size is too small, cannot remove any node");
    } else if (response == Response.RESPONSE_REJECT) {
      logger.error("Node {} is not found in the cluster, please check", nodeToRemove);
    } else {
      logger.error("Unexpected response {}", response);
    }
  }

  public static MetaClusterServer getMetaServer() {
    return metaServer;
  }

  /**
   * Developers may perform pre-start customizations here for debugging or experiments.
   */
  @SuppressWarnings("java:S125") // leaving examples
  private static void preStartCustomize() {
    // customize data distribution
    // The given example tries to divide storage groups like "root.sg_1", "root.sg_2"... into k
    // nodes evenly, and use default strategy for other groups
    SlotPartitionTable.setSlotStrategy(new SlotStrategy() {
      SlotStrategy defaultStrategy = new SlotStrategy.DefaultStrategy();
      int k = 3;
      @Override
      public int calculateSlotByTime(String storageGroupName, long timestamp, int maxSlotNum) {
        int sgSerialNum = extractSerialNumInSGName(storageGroupName) % k;
        if (sgSerialNum > 0) {
          return maxSlotNum / k * sgSerialNum;
        } else {
          return defaultStrategy.calculateSlotByTime(storageGroupName, timestamp, maxSlotNum);
        }
      }

      @Override
      public int calculateSlotByPartitionNum(String storageGroupName, long partitionId,
          int maxSlotNum) {
        int sgSerialNum = extractSerialNumInSGName(storageGroupName) % k;
        if (sgSerialNum > 0) {
          return maxSlotNum / k * sgSerialNum;
        } else {
          return defaultStrategy
              .calculateSlotByPartitionNum(storageGroupName, partitionId, maxSlotNum);
        }
      }

      private int extractSerialNumInSGName(String storageGroupName) {
        String[] s = storageGroupName.split("_");
        if (s.length != 2) {
          return -1;
        }
        try {
          return Integer.parseInt(s[1]);
        } catch (NumberFormatException e) {
          return -1;
        }
      }
    });
  }
}
