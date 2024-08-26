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

package org.apache.iotdb.db.protocol.thrift.handler;

import org.apache.iotdb.db.protocol.thrift.impl.IAINodeRPCServiceWithHandler;

import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.server.ServerContext;
import org.apache.thrift.server.TServerEventHandler;
import org.apache.thrift.transport.TTransport;

import java.util.concurrent.atomic.AtomicLong;

public class AINodeRPCServiceThriftHandler implements TServerEventHandler {

  private final AtomicLong thriftConnectionNumber = new AtomicLong(0);
  private final IAINodeRPCServiceWithHandler eventHandler;

  public AINodeRPCServiceThriftHandler(IAINodeRPCServiceWithHandler eventHandler) {
    this.eventHandler = eventHandler;
  }

  @Override
  public ServerContext createContext(TProtocol in, TProtocol out) {
    thriftConnectionNumber.incrementAndGet();
    return null;
  }

  @Override
  public void deleteContext(ServerContext arg0, TProtocol in, TProtocol out) {
    thriftConnectionNumber.decrementAndGet();
    eventHandler.handleExit();
  }

  @Override
  public void preServe() {
    // do nothing
  }

  @Override
  public void processContext(
      ServerContext serverContext, TTransport tTransport, TTransport tTransport1) {
    // do nothing
  }
}
