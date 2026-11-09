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

package org.apache.iotdb.db.protocol.thrift.handler;

import org.apache.iotdb.commons.service.NoopServerContext;
import org.apache.iotdb.commons.service.metric.MetricService;
import org.apache.iotdb.db.i18n.DataNodeMiscMessages;
import org.apache.iotdb.db.protocol.session.IClientSession;
import org.apache.iotdb.db.protocol.session.SessionManager;
import org.apache.iotdb.db.protocol.thrift.impl.IClientRPCServiceWithHandler;

import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.server.ServerContext;
import org.apache.thrift.server.TServerEventHandler;
import org.apache.thrift.transport.TTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicLong;

public class InternalServiceThriftHandler implements TServerEventHandler {

  private static final Logger LOGGER = LoggerFactory.getLogger(InternalServiceThriftHandler.class);

  private final AtomicLong thriftConnectionNumber = new AtomicLong(0);
  private final IClientRPCServiceWithHandler eventHandler;

  public InternalServiceThriftHandler(IClientRPCServiceWithHandler eventHandler) {
    this.eventHandler = eventHandler;
    MetricService.getInstance()
        .addMetricSet(new InternalServiceThriftHandlerMetrics(thriftConnectionNumber));
  }

  @Override
  public void preServe() {
    // do nothing
  }

  @Override
  public ServerContext createContext(TProtocol tProtocol, TProtocol tProtocol1) {
    thriftConnectionNumber.incrementAndGet();
    return NoopServerContext.INSTANCE;
  }

  @Override
  public void deleteContext(
      ServerContext serverContext, TProtocol tProtocol, TProtocol tProtocol1) {
    try {
      IClientSession session = SessionManager.getInstance().getCurrSession();
      if (session != null) {
        eventHandler.handleClientExit();
      }
    } catch (RuntimeException e) {
      LOGGER.warn(
          DataNodeMiscMessages
              .LOG_FAILED_TO_CLEAN_UP_INTERNAL_CLIENT_SESSION_AFTER_CONNECTION_CLOSED_9BD1700E,
          e);
    } finally {
      SessionManager.getInstance().removeCurrSession();
      thriftConnectionNumber.decrementAndGet();
    }
  }

  @Override
  public void processContext(
      ServerContext serverContext, TTransport tTransport, TTransport tTransport1) {
    // do nothing
  }
}
