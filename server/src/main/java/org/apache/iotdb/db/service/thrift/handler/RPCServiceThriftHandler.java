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

import org.apache.iotdb.db.service.metrics.MetricService;
import org.apache.iotdb.db.service.thrift.impl.TSServiceImpl;

import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.server.ServerContext;
import org.apache.thrift.server.TServerEventHandler;
import org.apache.thrift.transport.TTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicLong;

public class RPCServiceThriftHandler extends BaseServerContextHandler
    implements TServerEventHandler {
  private static final Logger logger = LoggerFactory.getLogger(RPCServiceThriftHandler.class);
  private TSServiceImpl serviceImpl;
  private AtomicLong thriftConnectionNumber = new AtomicLong(0);

  public RPCServiceThriftHandler(TSServiceImpl serviceImpl) {
    super(logger);
    this.serviceImpl = serviceImpl;
    MetricService.getInstance()
        .addMetricSet(new RPCServiceThriftHandlerMetrics(thriftConnectionNumber));
  }

  @Override
  public ServerContext createContext(TProtocol in, TProtocol out) {
    thriftConnectionNumber.incrementAndGet();
    return super.createContext(in, out);
  }

  @Override
  public void deleteContext(ServerContext arg0, TProtocol arg1, TProtocol arg2) {
    // release query resources.
    serviceImpl.handleClientExit();
    thriftConnectionNumber.decrementAndGet();
    super.deleteContext(arg0, arg1, arg2);
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
}
