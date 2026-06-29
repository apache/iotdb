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
import org.apache.iotdb.db.i18n.DataNodeMiscMessages;
import org.apache.iotdb.db.protocol.session.ClientSession;
import org.apache.iotdb.db.protocol.session.SessionManager;
import org.apache.iotdb.external.api.thrift.JudgableServerContext;
import org.apache.iotdb.external.api.thrift.ServerContextFactory;
import org.apache.iotdb.rpc.TElasticFramedTransport;

import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.server.ServerContext;
import org.apache.thrift.transport.TSocket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.Socket;
import java.util.ServiceLoader;

public class BaseServerContextHandler {
  private static ServerContextFactory factory = null;
  private static final Logger logger = LoggerFactory.getLogger(BaseServerContextHandler.class);

  static {
    ServiceLoader<ServerContextFactory> contextFactoryLoader =
        ServiceLoader.load(ServerContextFactory.class);
    for (ServerContextFactory loader : contextFactoryLoader) {
      if (factory != null) {
        // it means there is more than one implementation.
        logger.warn(DataNodeMiscMessages.MULTIPLE_SERVER_CONTEXT_FACTORY);
      }
      logger.info(DataNodeMiscMessages.SET_SERVER_CONTEXT_FACTORY, loader.getClass().getName());
      factory = loader;
    }
  }

  public BaseServerContextHandler() {
    // empty constructor
  }

  public ServerContext createContext(TProtocol in, TProtocol out) {
    Socket socket =
        ((TSocket) ((TElasticFramedTransport) out.getTransport()).getSocket()).getSocket();
    JudgableServerContext context = null;
    getSessionManager().registerSession(new ClientSession(socket));
    if (factory != null) {
      context = factory.newServerContext(out, socket);
      if (context != null && !context.whenConnect()) {
        return context;
      }
    }
    return context == null ? NoopServerContext.INSTANCE : context;
  }

  public void deleteContext(ServerContext context, TProtocol in, TProtocol out) {
    getSessionManager().removeCurrSession();

    if (context instanceof JudgableServerContext) {
      ((JudgableServerContext) context).whenDisconnect();
    }
  }

  protected SessionManager getSessionManager() {
    return SessionManager.getInstance();
  }
}
