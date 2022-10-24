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

import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.query.control.SessionManager;
import org.apache.iotdb.db.query.control.clientsession.ClientSession;
import org.apache.iotdb.external.api.thrift.JudgableServerContext;
import org.apache.iotdb.external.api.thrift.ServerContextFactory;
import org.apache.iotdb.rpc.TElasticFramedTransport;

import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.server.ServerContext;
import org.apache.thrift.transport.TSocket;
import org.slf4j.Logger;

import java.net.Socket;

public class BaseServerContextHandler {
  private ServerContextFactory factory = () -> (JudgableServerContext) () -> true;

  public BaseServerContextHandler(Logger logger) {
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

  public ServerContext createContext(TProtocol in, TProtocol out) {
    Socket socket =
        ((TSocket) ((TElasticFramedTransport) out.getTransport()).getSocket()).getSocket();
    getSessionManager().registerSession(new ClientSession(socket));
    if (factory != null) {
      JudgableServerContext context = factory.newServerContext();
      // TODO
      return context;
    }

    return null;
  }

  public void deleteContext(ServerContext arg0, TProtocol arg1, TProtocol arg2) {
    getSessionManager().removeCurrSession();
  }

  protected SessionManager getSessionManager() {
    return SessionManager.getInstance();
  }
}
