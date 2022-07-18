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
 *
 */
package org.apache.iotdb.db.sync.transport.server;

import org.apache.iotdb.db.service.metrics.MetricsService;
import org.apache.iotdb.db.service.metrics.enums.Metric;
import org.apache.iotdb.db.service.metrics.enums.Tag;
import org.apache.iotdb.metrics.utils.MetricLevel;

import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.server.ServerContext;
import org.apache.thrift.server.TServerEventHandler;
import org.apache.thrift.transport.TTransport;

public class TransportServerThriftHandler implements TServerEventHandler {

  private TransportServiceImpl serviceImpl;

  public TransportServerThriftHandler(TransportServiceImpl serviceImpl) {
    this.serviceImpl = serviceImpl;
  }

  @Override
  public void preServe() {}

  @Override
  public ServerContext createContext(TProtocol input, TProtocol output) {
    MetricsService.getInstance()
        .getMetricManager()
        .getOrCreateGauge(
            Metric.THRIFT_CONNECTIONS.toString(),
            MetricLevel.CORE,
            Tag.NAME.toString(),
            "Transport")
        .incr(1L);
    return null;
  }

  @Override
  public void deleteContext(ServerContext serverContext, TProtocol input, TProtocol output) {
    // release query resources.
    serviceImpl.handleClientExit();
    MetricsService.getInstance()
        .getMetricManager()
        .getOrCreateGauge(
            Metric.THRIFT_CONNECTIONS.toString(),
            MetricLevel.CORE,
            Tag.NAME.toString(),
            "Transport")
        .decr(1L);
  }

  @Override
  public void processContext(
      ServerContext serverContext, TTransport inputTransport, TTransport outputTransport) {}
}
