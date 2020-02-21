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
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.iotdb.cluster.client.DataClient;
import org.apache.iotdb.cluster.config.ClusterConfig;
import org.apache.iotdb.cluster.config.ClusterDescriptor;
import org.apache.iotdb.cluster.query.ClusterQueryParser;
import org.apache.iotdb.cluster.query.RemoteQueryContext;
import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.cluster.server.handlers.caller.GenericHandler;
import org.apache.iotdb.cluster.server.member.MetaGroupMember;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.service.TSServiceImpl;
import org.apache.iotdb.service.rpc.thrift.TSIService.AsyncClient;
import org.apache.iotdb.service.rpc.thrift.TSIService.Processor;
import org.apache.iotdb.service.rpc.thrift.TSStatus;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.server.ServerContext;
import org.apache.thrift.server.THsHaServer;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TServerEventHandler;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TNonblockingServerSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClientServer extends TSServiceImpl {

  private static final Logger logger = LoggerFactory.getLogger(ClientServer.class);
  private MetaGroupMember metaGroupMember;

  private ExecutorService serverService;
  private TServer poolServer;
  private Map<Long, RemoteQueryContext> queryContextMap = new HashMap<>();

  public ClientServer(MetaGroupMember metaGroupMember) {
    this.metaGroupMember = metaGroupMember;
    this.processor = new ClusterQueryParser(metaGroupMember);
  }

  public void start() throws TTransportException {
    if (serverService != null) {
      return;
    }

    serverService = Executors.newSingleThreadExecutor(r -> new Thread(r,
        "ClusterClientServer"));
    ClusterConfig config = ClusterDescriptor.getINSTANCE().getConfig();

    TProtocolFactory protocolFactory;
    if(ClusterDescriptor.getINSTANCE().getConfig().isRpcThriftCompressionEnabled()) {
      protocolFactory = new TCompactProtocol.Factory();
    }
    else {
      protocolFactory = new TBinaryProtocol.Factory();
    }
    TNonblockingServerSocket serverTransport = new TNonblockingServerSocket(new InetSocketAddress(config.getLocalIP(),
        config.getLocalClientPort()));
    THsHaServer.Args poolArgs =
        new THsHaServer.Args(serverTransport).maxWorkerThreads(IoTDBDescriptor.
        getInstance().getConfig().getRpcMaxConcurrentClientNum()).minWorkerThreads(1);
    poolArgs.executorService(new ThreadPoolExecutor(poolArgs.minWorkerThreads,
        poolArgs.maxWorkerThreads, poolArgs.getStopTimeoutVal(), poolArgs.getStopTimeoutUnit(),
        new SynchronousQueue<>(), new ThreadFactory() {
      private AtomicLong threadIndex = new AtomicLong(0);
      @Override
      public Thread newThread(Runnable r) {
        return new Thread(r, "ClusterClient" + threadIndex.incrementAndGet());
      }
    }));
    poolArgs.processor(new Processor<>(this));
    poolArgs.protocolFactory(protocolFactory);
    poolArgs.transportFactory(new TFramedTransport.Factory());

    poolServer = new THsHaServer(poolArgs);
    poolServer.setServerEventHandler(new EventHandler());

    serverService.submit(() -> poolServer.serve());
    logger.info("Client service is set up");
  }

  public void stop() {
    if (serverService == null) {
      return;
    }

    poolServer.stop();
    serverService.shutdownNow();
  }


  @Override
  protected TSStatus executePlan(PhysicalPlan plan) {
    return metaGroupMember.executeNonQuery(plan);
  }

  class EventHandler implements TServerEventHandler {

    @Override
    public void preServe() {
      // do nothing
    }

    @Override
    public ServerContext createContext(TProtocol input, TProtocol output) {
      return null;
    }

    @Override
    public void deleteContext(ServerContext serverContext, TProtocol input, TProtocol output) {
      ClientServer.this.handleClientExit();
    }

    @Override
    public void processContext(ServerContext serverContext, TTransport inputTransport,
        TTransport outputTransport) {
      // do nothing
    }
  }

  @Override
  public TSDataType getSeriesType(String pathStr) throws MetadataException {
    return metaGroupMember.getSeriesType(pathStr);
  }

  @Override
  protected QueryContext genQueryContext(long queryId) {
    RemoteQueryContext context = new RemoteQueryContext(queryId);
    queryContextMap.put(queryId, context);
    return context;
  }

  @Override
  protected void releaseQueryResource(long queryId) throws StorageEngineException {
    super.releaseQueryResource(queryId);
    RemoteQueryContext context = queryContextMap.get(queryId);
    if (context != null) {
      // release the resources in every queried node
      for (Entry<Node, Set<Node>> headerEntry : context.getQueriedNodesMap().entrySet()) {
        Node header = headerEntry.getKey();
        Set<Node> queriedNodes = headerEntry.getValue();

        for (Node queriedNode : queriedNodes) {
          GenericHandler handler = new GenericHandler(queriedNode, new AtomicReference());
          try {
            DataClient client = metaGroupMember.getDataClient(queriedNode);
            client.endQuery(header, metaGroupMember.getThisNode(), queryId, handler);
          } catch (IOException | TException e) {
            logger.error("Cannot end query {} in {}", queryId, queriedNode);
          }
        }
      }
    }
  }
}
