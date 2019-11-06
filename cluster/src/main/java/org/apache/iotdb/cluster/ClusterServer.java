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

import java.net.InetSocketAddress;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import org.apache.iotdb.cluster.config.ClusterConfig;
import org.apache.iotdb.cluster.config.ClusterDescriptor;
import org.apache.iotdb.cluster.rpc.thrift.AppendEntriesRequest;
import org.apache.iotdb.cluster.rpc.thrift.AppendEntryRequest;
import org.apache.iotdb.cluster.rpc.thrift.HeartBeatRequest;
import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.cluster.rpc.thrift.TSIService;
import org.apache.iotdb.cluster.rpc.thrift.TSIService.AsyncClient;
import org.apache.iotdb.cluster.rpc.thrift.TSIService.AsyncProcessor;
import org.apache.iotdb.cluster.rpc.thrift.VoteRequest;
import org.apache.iotdb.db.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.db.concurrent.ThreadName;
import org.apache.thrift.async.AsyncMethodCallback;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.server.TThreadPoolServer.Args;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClusterServer implements TSIService.AsyncIface {

  private static final Logger logger = LoggerFactory.getLogger(ClusterServer.class);

  private ClusterConfig config = ClusterDescriptor.getINSTANCE().getConfig();
  private TServerSocket socket;
  private TServer poolServer;
  private Node thisNode;

  private AsyncClient[] seedNodes;

  private NodeStatus nodeStatus = NodeStatus.STARTING_UP;

  public ClusterServer(String ip, int port) {
    this.thisNode = new Node();
  }

  public void start() throws TTransportException {
    if (poolServer != null) {
      return;
    }

    establishServer();
    nodeStatus = NodeStatus.ALONE;
  }

  public void stop() {
    if (poolServer == null) {
      return;
    }

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

  }


  private void establishServer() throws TTransportException {
    logger.info("Cluster node {} begins to set up", thisNode);
    TProtocolFactory protocolFactory;
    if(config.isRpcThriftCompressionEnabled()) {
      protocolFactory = new TCompactProtocol.Factory();
    }
    else {
      protocolFactory = new TBinaryProtocol.Factory();
    }

    socket = new TServerSocket(new InetSocketAddress(config.getLocalIP(),
        config.getLocalPort()));
    Args poolArgs =
        new TThreadPoolServer.Args(socket).maxWorkerThreads(config.getMaxConcurrentClientNum())
            .minWorkerThreads(1);

    poolArgs.executorService = new ThreadPoolExecutor(poolArgs.minWorkerThreads,
        poolArgs.maxWorkerThreads, poolArgs.stopTimeoutVal, poolArgs.stopTimeoutUnit,
        new SynchronousQueue<>(), r -> new Thread(r, "IoTDBClusterClientThread"));
    poolArgs.executorService = IoTDBThreadPoolFactory.createThriftRpcClientThreadPool(poolArgs,
        ThreadName.JDBC_CLIENT.getName());
    poolArgs.processor(new AsyncProcessor<>(this));
    poolArgs.protocolFactory(protocolFactory);
    poolServer = new TThreadPoolServer(poolArgs);
    poolServer.serve();
    logger.info("Cluster node {} is up", thisNode);
  }


  @Override
  public void sendHeartBeat(HeartBeatRequest request, AsyncMethodCallback resultHandler) {

  }

  @Override
  public void startVote(VoteRequest voteRequest, AsyncMethodCallback resultHandler) {

  }

  @Override
  public void appendMetadataEntry(AppendEntryRequest request, AsyncMethodCallback resultHandler) {

  }

  @Override
  public void appendDataEntry(AppendEntryRequest request, AsyncMethodCallback resultHandler) {

  }

  @Override
  public void appendMetadataEntries(AppendEntriesRequest request, AsyncMethodCallback resultHandler) {

  }

  @Override
  public void appendDataEntries(AppendEntriesRequest request, AsyncMethodCallback resultHandler) {

  }

  @Override
  public void addNode(Node node, AsyncMethodCallback resultHandler) {

  }
}
