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

package org.apache.iotdb.cluster.server.heartbeat;

import org.apache.iotdb.cluster.config.ClusterConfig;
import org.apache.iotdb.cluster.config.ClusterDescriptor;
import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.cluster.utils.ClusterUtils;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.StartupException;
import org.apache.iotdb.db.utils.CommonUtils;
import org.apache.iotdb.rpc.RpcTransportFactory;

import org.apache.thrift.TProcessor;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.server.THsHaServer;
import org.apache.thrift.server.THsHaServer.Args;
import org.apache.thrift.server.TServer;
import org.apache.thrift.transport.TNonblockingServerTransport;
import org.apache.thrift.transport.TServerTransport;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicLong;

/**
 * HeartbeatServer works as a broker (network and protocol layer) that sends and receive the
 * heartbeat requests to the proper RaftMembers to process.
 */
public abstract class HeartbeatServer {

  private static final Logger logger = LoggerFactory.getLogger(HeartbeatServer.class);
  private static int connectionTimeoutInMS =
      ClusterDescriptor.getInstance().getConfig().getConnectionTimeoutInMS();

  ClusterConfig config = ClusterDescriptor.getInstance().getConfig();
  /** the heartbeat socket poolServer will listen to */
  private TServerTransport heartbeatSocket;

  /** the heartbeat RPC processing server */
  private TServer heartbeatPoolServer;

  Node thisNode;

  private TProtocolFactory heartbeatProtocolFactory =
      config.isRpcThriftCompressionEnabled()
          ? new TCompactProtocol.Factory()
          : new TBinaryProtocol.Factory();

  /** This thread pool is to run the thrift server (heartbeatPoolServer above) */
  private ExecutorService heartbeatClientService;

  HeartbeatServer() {
    thisNode = new Node();
    thisNode.setInternalIp(config.getInternalIp());
    thisNode.setMetaPort(config.getInternalMetaPort());
    thisNode.setDataPort(config.getInternalDataPort());
    thisNode.setClientIp(IoTDBDescriptor.getInstance().getConfig().getRpcAddress());
  }

  HeartbeatServer(Node thisNode) {
    this.thisNode = thisNode;
  }

  public static int getConnectionTimeoutInMS() {
    return connectionTimeoutInMS;
  }

  /**
   * Establish a thrift server with the configurations in ClusterConfig to listen to and respond to
   * thrift RPCs. Calling the method twice does not induce side effects.
   *
   * @throws TTransportException
   */
  @SuppressWarnings("java:S1130") // thrown in override method
  public void start() throws TTransportException, StartupException {
    if (heartbeatPoolServer != null) {
      return;
    }

    establishHeartbeatServer();
  }

  /**
   * Stop the thrift server, close the socket and interrupt all in progress RPCs. Calling the method
   * twice does not induce side effects.
   */
  public void stop() {
    if (heartbeatPoolServer == null) {
      return;
    }

    heartbeatPoolServer.stop();
    heartbeatSocket.close();
    heartbeatClientService.shutdownNow();
    heartbeatSocket = null;
    heartbeatPoolServer = null;
  }

  /**
   * An AsyncProcessor that contains the extended interfaces of a non-abstract subclass of
   * RaftService (DataHeartbeatService or MetaHeartbeatService).
   *
   * @return the TProcessor
   */
  abstract TProcessor getProcessor();

  /**
   * A socket that will be used to establish a thrift server to listen to RPC heartbeat requests.
   * DataHeartServer and MetaHeartServer use different port, so this is to be determined.
   *
   * @return TServerTransport
   * @throws TTransportException
   */
  abstract TServerTransport getHeartbeatServerSocket() throws TTransportException;

  /**
   * Each thrift RPC request will be processed in a separate thread and this will return the name
   * prefix of such threads. This is used to fast distinguish DataHeartbeatServer and
   * MetaHeartbeatServer in the logs for the sake of debug.
   *
   * @return name prefix of RPC processing threads.
   */
  abstract String getClientThreadPrefix();

  /**
   * The thrift server will be run in a separate thread, and this will be its name. It help you
   * locate the desired logs quickly when debugging.
   *
   * @return The name of the thread running the thrift server.
   */
  abstract String getServerClientName();

  private TServer getSyncHeartbeatServer() throws TTransportException {
    heartbeatSocket = getHeartbeatServerSocket();
    return ClusterUtils.createTThreadPoolServer(
        heartbeatSocket, getClientThreadPrefix(), getProcessor(), heartbeatProtocolFactory);
  }

  private TServer getAsyncHeartbeatServer() throws TTransportException {
    heartbeatSocket = getHeartbeatServerSocket();
    int maxConcurrentClientNum =
        Math.max(CommonUtils.getCpuCores(), config.getMaxConcurrentClientNum());
    Args poolArgs =
        new Args((TNonblockingServerTransport) heartbeatSocket)
            .maxWorkerThreads(maxConcurrentClientNum)
            .minWorkerThreads(CommonUtils.getCpuCores());

    poolArgs.executorService(
        new ThreadPoolExecutor(
            poolArgs.minWorkerThreads,
            poolArgs.maxWorkerThreads,
            poolArgs.getStopTimeoutVal(),
            poolArgs.getStopTimeoutUnit(),
            new SynchronousQueue<>(),
            new ThreadFactory() {
              private AtomicLong threadIndex = new AtomicLong(0);

              @Override
              public Thread newThread(Runnable r) {
                return new Thread(r, getClientThreadPrefix() + threadIndex.incrementAndGet());
              }
            }));
    poolArgs.processor(getProcessor());
    poolArgs.protocolFactory(heartbeatProtocolFactory);
    // async service requires FramedTransport
    poolArgs.transportFactory(RpcTransportFactory.INSTANCE);

    return new THsHaServer(poolArgs);
  }

  private void establishHeartbeatServer() throws TTransportException {
    logger.info("Cluster node's heartbeat {} begins to set up", thisNode);

    if (ClusterDescriptor.getInstance().getConfig().isUseAsyncServer()) {
      heartbeatPoolServer = getAsyncHeartbeatServer();
    } else {
      heartbeatPoolServer = getSyncHeartbeatServer();
    }

    heartbeatClientService =
        Executors.newSingleThreadExecutor(r -> new Thread(r, getServerClientName()));
    heartbeatClientService.submit(() -> heartbeatPoolServer.serve());

    logger.info("[{}] Cluster node's heartbeat {} is up", getServerClientName(), thisNode);
  }
}
