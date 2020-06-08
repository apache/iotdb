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
import java.util.List;
import org.apache.iotdb.cluster.client.async.MetaClient;
import org.apache.iotdb.cluster.client.sync.SyncClientAdaptor;
import org.apache.iotdb.cluster.config.ClusterConfig;
import org.apache.iotdb.cluster.config.ClusterDescriptor;
import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.cluster.server.MetaClusterServer;
import org.apache.iotdb.cluster.server.Response;
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
      logger.error("Usage: <start mode>");
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
    logger.info("Running mode {}", mode);
    try {

      if (MODE_START.equals(mode)) {
        metaServer = new MetaClusterServer();
        ClusterConfig config = ClusterDescriptor.getInstance().getConfig();
        // check the initial cluster size and refuse to start when the size < quorum
        int quorum = config.getReplicationNum() / 2 + 1;
        if (config.getSeedNodeUrls().size() < quorum) {
          String message = String.format("Seed number less than quorum, seed number: %s, quorum: "
                  + "%s.",
              config.getSeedNodeUrls().size(), quorum);
          throw new StartupException(metaServer.getMember().getName(), message);
        }
        metaServer.start();
        metaServer.buildCluster();
      } else if (MODE_ADD.equals(mode)) {
        metaServer = new MetaClusterServer();
        metaServer.start();
        if (!metaServer.joinCluster()) {
          metaServer.stop();
        }
      } else if (MODE_REMOVE.equals(mode)) {
        doRemoveNode(args);
      } else {
        logger.error("Unrecognized mode {}", mode);
      }
    } catch (IOException | TTransportException | StartupException | QueryProcessException e) {
      logger.error("Fail to start meta server", e);
    }
  }

  private static void replaceDefaultPrams(String[] args) {
    int index;
    String[] clusterParams;
    String[] serverParams = null;
    for (index = 0; index < args.length; index++) {
      if (SERVER_CONF_SEPARATOR.equals(args[index])) {
        break;
      }
    }
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

    // 1. check the LOCAL_IP and SEED_NODES consistent or not
    ClusterConfig config = ClusterDescriptor.getInstance().getConfig();
    String localIP = "127.0.0.1";
    String configLocalIP = config.getLocalIP();
    List<String> seedNodes = config.getSeedNodeUrls();
    boolean isLocalIP = localIP.equals(configLocalIP);
    for (String seedNodeIP : seedNodes) {
      if ((isLocalIP && !seedNodeIP.contains(localIP)) ||
          (!isLocalIP && seedNodeIP.contains(localIP))) {
        logger.error(
            "LOCAL_IP={} and SEED_NODES={} should be consistent, both use local ip or real ip please",
            configLocalIP, seedNodes);
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
      String[] splits = url.split(":");
      Node node = new Node();
      node.setIp(splits[0]).setMetaPort(Integer.parseInt(splits[1]));
      MetaClient client = new MetaClient(factory, new TAsyncClientManager(), node, null);

      try {
        logger.info("Start removing node {} with the help of node {}", nodeToRemove, node);
        Long response = SyncClientAdaptor.removeNode(client, nodeToRemove);
        if (response != null) {
          if (response == Response.RESPONSE_AGREE) {
            logger.info("Node {} is successfully removed", nodeToRemove);
            return;
          } else if (response == Response.RESPONSE_CLUSTER_TOO_SMALL) {
            logger.error("Cluster size is too small, cannot remove any node");
            return;
          } else if (response == Response.RESPONSE_REJECT) {
            logger.error("Node {} is not found in the cluster, please check", nodeToRemove);
            return;
          }
        }
      } catch (TException e) {
        logger.warn("Cannot send remove node request through {}, try next node", node);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        logger.warn("Cannot send remove node request through {}, try next node", node);
      }
    }
  }

  public static MetaClusterServer getMetaServer() {
    return metaServer;
  }
}
