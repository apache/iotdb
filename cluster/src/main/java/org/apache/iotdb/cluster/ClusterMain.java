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
import java.util.concurrent.atomic.AtomicReference;
import org.apache.iotdb.cluster.client.MetaClient;
import org.apache.iotdb.cluster.config.ClusterConfig;
import org.apache.iotdb.cluster.config.ClusterDescriptor;
import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.cluster.server.MetaClusterServer;
import org.apache.iotdb.cluster.server.RaftServer;
import org.apache.iotdb.cluster.server.Response;
import org.apache.iotdb.cluster.server.handlers.caller.GenericHandler;
import org.apache.iotdb.db.exception.StartupException;
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
  // join an established cluster
  private static final String MODE_REMOVE = "-r";

  public static MetaClusterServer metaServer;

  public static void main(String[] args) {
    if (args.length < 1) {
      logger.error("Usage: <start mode>");
      return;
    }
    String mode = args[0];

    logger.info("Running mode {}", mode);
    try {
      if (MODE_START.equals(mode)) {
        metaServer = new MetaClusterServer();
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
    } catch (IOException | TTransportException | StartupException e) {
      logger.error("Fail to start meta server", e);
    }
  }

  private static void doRemoveNode(String[] args) throws IOException {
    if (args.length != 3) {
      logger.error("Usage: -r <ip> <metaPort>");
      return;
    }
    String ip = args[1];
    int metaPort = Integer.parseInt(args[2]);
    ClusterConfig config = ClusterDescriptor.getINSTANCE().getConfig();
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

      AtomicReference<Long> responseRef = new AtomicReference<>();
      GenericHandler handler = new GenericHandler(node, responseRef);
      try {
        synchronized (responseRef) {
          client.removeNode(nodeToRemove, handler);
          responseRef.wait(RaftServer.connectionTimeoutInMS);
        }
        Long response = responseRef.get();
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
      } catch (TException | InterruptedException e) {
        logger.warn("Cannot send remove node request through {}, try next node", node);
      }
    }
  }
}
