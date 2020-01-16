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

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.iotdb.cluster.config.ClusterConfig;
import org.apache.iotdb.cluster.config.ClusterDescriptor;
import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.cluster.rpc.thrift.RaftService;
import org.apache.iotdb.cluster.rpc.thrift.RaftService.AsyncProcessor;
import org.apache.iotdb.db.exception.StartupException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.server.THsHaServer;
import org.apache.thrift.server.THsHaServer.Args;
import org.apache.thrift.server.TServer;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TNonblockingServerSocket;
import org.apache.thrift.transport.TNonblockingServerTransport;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * RaftServer works as a broker that sends the requests to the proper RaftMembers to process.
 */
public abstract class RaftServer implements RaftService.AsyncIface {

  private static final Logger logger = LoggerFactory.getLogger(RaftService.class);
  public static int connectionTimeoutInMS = ClusterDescriptor.getINSTANCE().getConfig().getConnectionTimeoutInMS();
  public static int syncLeaderMaxWaitMs = 20 * 1000;
  public static long heartBeatIntervalMs = 3000L;

  ClusterConfig config = ClusterDescriptor.getINSTANCE().getConfig();
  private TNonblockingServerTransport socket;
  private TServer poolServer;
  Node thisNode;

  TProtocolFactory protocolFactory = config.isRpcThriftCompressionEnabled() ?
      new TCompactProtocol.Factory() : new TBinaryProtocol.Factory();

  private ExecutorService clientService;

  RaftServer() {
    thisNode = new Node();
    thisNode.setIp(config.getLocalIP());
    thisNode.setMetaPort(config.getLocalMetaPort());
    thisNode.setDataPort(config.getLocalDataPort());
  }

  RaftServer(Node thisNode) {
    this.thisNode = thisNode;
  }

  public void start() throws TTransportException, StartupException {
    if (poolServer != null) {
      return;
    }

    establishServer();
  }

  public void stop() {
    if (poolServer == null) {
      return;
    }

    poolServer.stop();
    socket.close();
    clientService.shutdownNow();
    socket = null;
    poolServer = null;
  }

  abstract AsyncProcessor getProcessor();

  abstract TNonblockingServerSocket getServerSocket() throws TTransportException;

  abstract String getClientThreadPrefix();

  abstract String getServerClientName();

  private void establishServer() throws TTransportException {
    logger.info("Cluster node {} begins to set up", thisNode);

    socket = getServerSocket();
    Args poolArgs =
        new THsHaServer.Args(socket).maxWorkerThreads(config.getMaxConcurrentClientNum())
            .minWorkerThreads(1);

    poolArgs.executorService(new ThreadPoolExecutor(poolArgs.minWorkerThreads,
        poolArgs.maxWorkerThreads, poolArgs.getStopTimeoutVal(), poolArgs.getStopTimeoutUnit(),
        new SynchronousQueue<>(), new ThreadFactory() {
      private AtomicLong threadIndex = new AtomicLong(0);
      @Override
      public Thread newThread(Runnable r) {
        return new Thread(r, getClientThreadPrefix() + threadIndex.incrementAndGet());
      }
    }));
    poolArgs.processor(getProcessor());
    poolArgs.protocolFactory(protocolFactory);
    poolArgs.transportFactory(new TFramedTransport.Factory());

    poolServer = new THsHaServer(poolArgs);
    clientService = Executors.newSingleThreadExecutor(r -> new Thread(r, getServerClientName()));
    clientService.submit(() -> poolServer.serve());

    logger.info("Cluster node {} is up", thisNode);
  }
}
