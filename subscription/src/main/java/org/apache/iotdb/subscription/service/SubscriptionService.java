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

package org.apache.iotdb.subscription.service;

import org.apache.iotdb.subscription.consumer.Consumer;
import org.apache.iotdb.subscription.rpc.thrift.ISubscriptionRPCService;
import org.apache.iotdb.subscription.service.thrift.handler.SubscriptionServiceThriftHandler;

import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class SubscriptionService {
  private static final Logger logger = LoggerFactory.getLogger(SubscriptionService.class);
  private final String localHost;
  private final Integer localPort;

  private SubscriptionServiceThriftHandler handler;

  private ISubscriptionRPCService.Processor processor;
  private TServer server;

  public SubscriptionService(String localHost, Integer localPort) {
    this.localHost = localHost;
    this.localPort = localPort;
  }

  public abstract void registerConsumer(Consumer consumer);

  public abstract Consumer getConsumer();

  public void start() {
    try {
      initTProcessor();
      initThriftService();
    } catch (Exception e) {
      logger.error("Failed to start Subscription service", e);
    }
  }

  private void initTProcessor() {
    this.handler = new SubscriptionServiceThriftHandler(getConsumer());
    this.processor = new ISubscriptionRPCService.Processor(handler);
  }

  private void initThriftService() {
    try {
      TServerTransport serverTransport = new TServerSocket(this.localPort);
      new Thread(
              () -> {
                startThriftServer(serverTransport);
              })
          .start();
    } catch (Exception e) {
      logger.error("Failed to start Subscription thrift server", e);
    }
  }

  private void startThriftServer(TServerTransport serverTransport) {
    if (server == null) {
      server =
          new TThreadPoolServer(
              new TThreadPoolServer.Args(serverTransport)
                  .protocolFactory(new TBinaryProtocol.Factory())
                  .processor(processor));
    }
    server.serve();
    logger.info("Subscription thrift server started at {}:{}", localHost, localPort);
  }

  public void stop() {
    if (server != null && server.isServing()) {
      server.setShouldStop(true);
      server.stop();
    }
  }
}
