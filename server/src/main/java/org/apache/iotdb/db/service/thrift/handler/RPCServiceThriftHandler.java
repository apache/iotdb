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

import org.apache.iotdb.commons.service.metric.MetricService;
import org.apache.iotdb.db.service.thrift.impl.IClientRPCServiceWithHandler;

import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.server.ServerContext;
import org.apache.thrift.server.TServerEventHandler;
import org.apache.thrift.transport.TTransport;

import java.util.concurrent.atomic.AtomicLong;

public class RPCServiceThriftHandler extends BaseServerContextHandler
    implements TServerEventHandler {

  private final AtomicLong thriftConnectionNumber = new AtomicLong(0);
  private final IClientRPCServiceWithHandler eventHandler;

  public RPCServiceThriftHandler(IClientRPCServiceWithHandler eventHandler) {
    this.eventHandler = eventHandler;
    MetricService.getInstance()
        .addMetricSet(new RPCServiceThriftHandlerMetrics(thriftConnectionNumber));
  }

  @Override
  public ServerContext createContext(TProtocol in, TProtocol out) {
    thriftConnectionNumber.incrementAndGet();
    return super.createContext(in, out);
  }

  @Override
  public void deleteContext(ServerContext arg0, TProtocol in, TProtocol out) {
    // release query resources.
    eventHandler.handleClientExit();
    thriftConnectionNumber.decrementAndGet();
    super.deleteContext(arg0, in, out);
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
