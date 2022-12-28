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

package org.apache.iotdb.db.service.thrift.handler;

import org.apache.iotdb.db.service.thrift.impl.InfluxDBServiceImpl;

import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.server.ServerContext;
import org.apache.thrift.server.TServerEventHandler;
import org.apache.thrift.transport.TTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class InfluxDBServiceThriftHandler extends BaseServerContextHandler
    implements TServerEventHandler {
  private final InfluxDBServiceImpl influxDBServiceImpl;
  private static final Logger logger = LoggerFactory.getLogger(InfluxDBServiceThriftHandler.class);

  public InfluxDBServiceThriftHandler(InfluxDBServiceImpl influxDBServiceImpl) {
    this.influxDBServiceImpl = influxDBServiceImpl;
  }

  @Override
  public void preServe() {
    // nothing
  }

  @Override
  public ServerContext createContext(TProtocol in, TProtocol out) {
    // nothing
    return super.createContext(in, out);
  }

  @Override
  public void deleteContext(
      ServerContext serverContext, TProtocol tProtocol, TProtocol tProtocol1) {
    // release resources.
    influxDBServiceImpl.handleClientExit();
    super.deleteContext(serverContext, tProtocol, tProtocol1);
  }

  @Override
  public void processContext(
      ServerContext serverContext, TTransport tTransport, TTransport tTransport1) {
    // nothing
  }
}
