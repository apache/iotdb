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

package org.apache.iotdb.confignode.service.thrift;

import org.apache.iotdb.commons.service.metric.MetricService;

import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.server.ServerContext;
import org.apache.thrift.server.TServerEventHandler;
import org.apache.thrift.transport.TTransport;

import java.util.concurrent.atomic.AtomicLong;

public class ConfigNodeRPCServiceHandler implements TServerEventHandler {
  private final AtomicLong thriftConnectionNumber = new AtomicLong(0);

  public ConfigNodeRPCServiceHandler() {
    MetricService.getInstance()
        .addMetricSet(new ConfigNodeRPCServiceHandlerMetrics(thriftConnectionNumber));
  }

  @Override
  public ServerContext createContext(TProtocol input, TProtocol output) {
    thriftConnectionNumber.incrementAndGet();
    return null;
  }

  @Override
  public void deleteContext(ServerContext serverContext, TProtocol input, TProtocol output) {
    thriftConnectionNumber.decrementAndGet();
  }

  @Override
  public void preServe() {
    // nothing
  }

  @Override
  public void processContext(ServerContext serverContext, TTransport input, TTransport output) {
    // nothing
  }
}
