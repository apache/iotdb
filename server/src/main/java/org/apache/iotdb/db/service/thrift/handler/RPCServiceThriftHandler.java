/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.iotdb.db.service.thrift.handler;

import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.query.control.SessionManager;
import org.apache.iotdb.db.query.control.clientsession.ClientSession;
import org.apache.iotdb.db.service.metrics.MetricService;
import org.apache.iotdb.db.service.thrift.impl.TSServiceImpl;
import org.apache.iotdb.external.api.thrift.JudgableServerContext;
import org.apache.iotdb.external.api.thrift.ServerContextFactory;
import org.apache.iotdb.rpc.TElasticFramedTransport;

import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.server.ServerContext;
import org.apache.thrift.server.TServerEventHandler;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.concurrent.atomic.AtomicLong;

public class RPCServiceThriftHandler implements TServerEventHandler {
  private static final Logger logger = LoggerFactory.getLogger(RPCServiceThriftHandler.class);
  private TSServiceImpl serviceImpl;
  private AtomicLong thriftConnectionNumber = new AtomicLong(0);

  private ServerContextFactory factory = null;

  public RPCServiceThriftHandler(TSServiceImpl serviceImpl) {
    this.serviceImpl = serviceImpl;
    MetricService.getInstance()
        .addMetricSet(new RPCServiceThriftHandlerMetrics(thriftConnectionNumber));
    String factoryClass =
        IoTDBDescriptor.getInstance()
            .getConfig()
            .getCustomizedProperties()
            .getProperty("rpc_service_thrift_handler_context_class");
    if (factoryClass != null) {
      try {
        factory = (ServerContextFactory) Class.forName(factoryClass).newInstance();
      } catch (Exception e) {
        logger.warn(
            "configuration announced ServerContextFactory {}, but it is not found in classpath",
            factoryClass);
        factory = null;
      }
    }
  }

  @Override
  public ServerContext createContext(TProtocol in, TProtocol out) {
    logger.info("创建了连接");
    thriftConnectionNumber.incrementAndGet();
    Socket socket =
        ((TSocket) ((TElasticFramedTransport) in.getTransport()).getSocket()).getSocket();
    logger.info(
        "in local: {}:{}, remote: {}, default: {}",
        socket.getLocalSocketAddress(),
        socket.getLocalPort(),
        socket.getRemoteSocketAddress(),
        socket.getPort());
    socket = ((TSocket) ((TElasticFramedTransport) out.getTransport()).getSocket()).getSocket();
    logger.info(
        "out local: {}:{}, remote: {}, default: {}",
        socket.getLocalSocketAddress(),
        socket.getLocalPort(),
        socket.getRemoteSocketAddress(),
        socket.getPort());
    getSessionManager()
        .registerSession(new ClientSession((InetSocketAddress) socket.getRemoteSocketAddress()));
    if (factory != null) {
      JudgableServerContext context = factory.newServerContext();
      // TODO
      return context;
    }

    return null;
  }

  @Override
  public void deleteContext(ServerContext arg0, TProtocol arg1, TProtocol arg2) {
    logger.info("移除了连接");
    // release query resources.
    serviceImpl.handleClientExit();
    thriftConnectionNumber.decrementAndGet();
    getSessionManager().removeCurrSession();
  }

  @Override
  public void preServe() {
    // nothing
  }

  @Override
  public void processContext(ServerContext arg0, TTransport arg1, TTransport arg2) {
    // nothing
  }

  /**
   * get the SessionManager Instance. <br>
   * in v0.13, Cluster mode uses different SessionManager instance...
   *
   * @return
   */
  protected SessionManager getSessionManager() {
    return SessionManager.getInstance();
  }
}
